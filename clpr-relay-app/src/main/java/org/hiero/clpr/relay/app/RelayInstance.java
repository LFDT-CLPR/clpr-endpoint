// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.app;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.logging.api.Logger;
import com.swirlds.logging.api.Loggers;
import com.swirlds.metrics.api.LongGauge;
import com.swirlds.metrics.api.Metrics;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.hiero.clpr.relay.core.ClprChannelOps;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.RelayProtocol;
import org.hiero.clpr.relay.core.metrics.SimpleMetrics;
import org.hiero.clpr.relay.grpc.client.ClprEndpointClient;
import org.hiero.clpr.relay.grpc.server.ClprGrpcServer;
import org.hiero.clpr.relay.grpc.server.ClprSyncHandler;
import org.hiero.clpr.relay.grpc.server.GetLedgerConfigurationHandler;

/**
 * A running CLPR relay instance. Encapsulates all relay components and provides lifecycle methods
 * for starting and stopping the relay programmatically.
 *
 * <p>The instance is a three-tier hierarchy that mirrors {@link RelayConfig}:
 * <ol>
 *   <li>a {@link LocalNetworkAdapter} per {@code localNetworks} entry (shared RPC/gas clients),</li>
 *   <li>a {@link ClprServiceHandler} per {@code clprServices} entry (one {@code ClprService}
 *       deployment; owns discovery + its channel handlers), each of which owns</li>
 *   <li>a {@link ClprChannelHandler} per channel (all channel-scoped services and the
 *       worker threads it spawns).</li>
 * </ol>
 *
 * <p>The remaining components are relay-global: the inbound gRPC server, the outbound peer client,
 * and the metrics registry. The gRPC {@code ClprSyncHandler} reaches channel-scoped state by resolving
 * the owning {@link ClprChannelHandler} ({@link org.hiero.clpr.relay.core.ClprChannelOps}) for a channel
 * id — via a {@code ChannelLookup} — across every {@link ClprServiceHandler}.
 *
 * <p>This class lets integration tests and embedding applications drive a full relay from code
 * without going through {@link ClprRelayMain#main()}.
 */
public final class RelayInstance {

    private static final Logger log = Loggers.getLogger(RelayInstance.class);

    private final Map<String, LocalNetworkAdapter> localNetworks;
    private final Map<String, ClprServiceHandler> clprServices;
    private final ClprGrpcServer grpcServer;
    private final ClprEndpointClient clprEndpointClient;
    private final Metrics metrics;

    private volatile boolean running;

    /**
     * Options controlling which subsystems {@link #start(StartOptions)} brings up.
     *
     * @param outboundSync when {@code false}, no channel starts its outbound sync loop, so the
     *     relay never initiates outbound syncs — only its inbound gRPC server and on-chain state
     *     listeners run. Used by single-side / inbound-only tests (and any inbound-only deployment).
     */
    public record StartOptions(boolean outboundSync) {
        /** Full relay: inbound server, state listeners, and the outbound sync loops. */
        public static StartOptions full() {
            return new StartOptions(true);
        }

        /** Inbound-only: server + state listeners, no outbound sync loops. */
        public static StartOptions inboundOnly() {
            return new StartOptions(false);
        }
    }

    private RelayInstance(
            final Map<String, LocalNetworkAdapter> localNetworks,
            final Map<String, ClprServiceHandler> clprServices,
            final ClprGrpcServer grpcServer,
            final ClprEndpointClient clprEndpointClient,
            final Metrics metrics) {
        this.localNetworks = localNetworks;
        this.clprServices = clprServices;
        this.grpcServer = grpcServer;
        this.clprEndpointClient = clprEndpointClient;
        this.metrics = metrics;
    }

    /** Returns the metrics registry for this relay instance. */
    public Metrics metrics() {
        return metrics;
    }

    /**
     * Build a {@link RelayInstance} from the given configuration without starting it.
     *
     * @param config the relay configuration
     * @return a new, not-yet-started relay instance
     */
    public static RelayInstance build(final RelayConfig config) {
        return build(config, new SimpleMetrics(), "");
    }

    /**
     * Build a {@link RelayInstance} carrying a log-context label without starting it.
     *
     * @param config       the relay configuration
     * @param instanceName free-form label stamped onto the relay's worker-loop log context under
     *                     the {@code relay} key; a blank value adds no context entry
     * @return a new, not-yet-started relay instance
     */
    public static RelayInstance build(final RelayConfig config, final String instanceName) {
        return build(config, new SimpleMetrics(), instanceName);
    }

    /**
     * Build a {@link RelayInstance} using the supplied metrics registry.
     *
     * @param config  the relay configuration
     * @param metrics the registry submodules will record into
     * @return a new, not-yet-started relay instance
     */
    public static RelayInstance build(final RelayConfig config, final Metrics metrics) {
        return build(config, metrics, "");
    }

    /**
     * Terminal build: supplied metrics registry and log-context label.
     *
     * @param config       the relay configuration
     * @param metrics      the registry submodules will record into
     * @param instanceName free-form label stamped onto the relay's worker-loop log context under
     *                     the {@code relay} key; a blank value adds no context entry
     * @return a new, not-yet-started relay instance
     */
    public static RelayInstance build(final RelayConfig config, final Metrics metrics, final String instanceName) {

        // 1. One shared adapter per local network (RPC client, gas strategy, CometBFT client).
        //    A repeated localNetworks id is ignored (warned), keeping the first entry.
        final var dedupedNetworks = dedupeLocalNetworks(config.localNetworks());
        final var localNetworks = new HashMap<String, LocalNetworkAdapter>(dedupedNetworks.size());
        for (final var network : dedupedNetworks) {
            localNetworks.put(network.id(), LocalNetworkAdapter.create(network, metrics));
        }

        // 2. The (initially empty) service-handler map. The gRPC glue below resolves a channel
        //    handler by iterating this live map, so handlers registered later — configured services'
        //    predefined channels at start(), or channels discovered on-chain at runtime — are
        //    served without rewiring.
        final var clprServices = new ConcurrentHashMap<String, ClprServiceHandler>(
                config.clprServices().size());
        final Function<Bytes, Optional<ClprChannelHandler>> channelLookup =
                channelId -> findChannelHandler(clprServices, channelId);

        // 3. Outbound peer client (shared by every channel's sync loop).
        final var clprEndpointClient = new ClprEndpointClient(config.grpc().maxMessageSize());

        // Two-phase sync (responder side): resolves the per-channel collaborators by channel id, and
        // builds a bundle ad hoc for the peer's request via createBundle. The peer's held manifest
        // version arrives in the request, so no local per-channel manifest-version cache is needed.
        final var syncHandler = new ClprSyncHandler(
                channelId -> channelLookup.apply(channelId).map(ClprChannelOps.class::cast), metrics, instanceName);

        // Route getLedgerConfiguration by the request's service_address. An empty selector preserves
        // the prior behavior of serving the single/primary deployment (the first available channel's
        // provider). Resolved live from the channel topology so runtime-discovered channels are
        // served immediately; the handler maps a null result to the right gRPC status (UNIMPLEMENTED
        // when nothing is registered yet, NOT_FOUND for an unknown non-empty address).
        final GetLedgerConfigurationHandler.Resolver ledgerConfigResolver = serviceAddress -> {
            final boolean anyDeployment = serviceAddress.length() == 0;
            final Bytes wanted = anyDeployment ? null : normalizeServiceAddress(serviceAddress);
            return clprServices.values().stream()
                    .filter(s -> anyDeployment
                            || normalizeServiceAddress(hexToBytes(s.serviceAddress()))
                                    .equals(wanted))
                    .flatMap(s -> s.channelHandlers().stream())
                    .map(ClprChannelHandler::ledgerConfigProvider)
                    .findFirst()
                    .orElse(null);
        };
        final var getLedgerConfigurationHandler =
                new GetLedgerConfigurationHandler(ledgerConfigResolver, CommitmentLevel.FINALIZED);

        final var grpcServer = new ClprGrpcServer(
                config.grpc().port(),
                config.grpc().maxMessageSize(),
                syncHandler,
                getLedgerConfigurationHandler,
                instanceName);

        // 5. Protocol-version gauge + aggregate per-loop failure gauges (computed at scrape time
        //    from the live channel handlers).
        final var protocolVersionGauge = metrics.getOrCreate(new LongGauge.Config("relay", "protocol.version")
                .withDescription("CLPR protocol version implemented by this relay"));
        protocolVersionGauge.set(RelayProtocol.PROTOCOL_VERSION);
        registerFailureGauges(metrics, clprServices);

        // 6. One service handler per configured ClprService deployment. A ClprService is unique
        //    within its local network; a repeated (localNetwork, serviceAddress) is ignored (warned).
        //    The map is keyed by that composite so the same address on a different network stays
        //    distinct; it is only ever iterated (never looked up by key).
        for (final var serviceConfig : dedupeClprServices(config.clprServices())) {
            clprServices.put(
                    serviceKey(serviceConfig.localNetwork(), serviceConfig.serviceAddress()),
                    ClprServiceHandler.create(
                            serviceConfig,
                            localNetworks,
                            metrics,
                            config.backoff(),
                            instanceName,
                            clprEndpointClient,
                            config.peerProofTypes()));
        }

        return new RelayInstance(localNetworks, clprServices, grpcServer, clprEndpointClient, metrics);
    }

    /**
     * Start all services: the gRPC server, then every {@link ClprServiceHandler} (which registers its
     * predefined channels and launches discovery).
     */
    public void start() {
        start(StartOptions.full());
    }

    /**
     * Start the relay's subsystems according to {@code options}. The inbound gRPC server always
     * starts; each channel's on-chain state listener always starts; a channel's outbound sync
     * loop starts only when {@link StartOptions#outboundSync()} is {@code true}.
     *
     * @param options which subsystems to bring up
     */
    public synchronized void start(final StartOptions options) {
        grpcServer.start();
        for (final var service : clprServices.values()) {
            service.start(options);
        }
        running = true;
    }

    /**
     * Stop all services gracefully, in reverse order of startup.
     */
    public synchronized void stop() {
        running = false;
        for (final var service : clprServices.values()) {
            service.stop();
        }
        // Close outbound peer channels only after the sync loops have been signalled to stop, so no
        // loop is mid-RPC when the channels shut down. Releases the pooled gRPC channels that would
        // otherwise be leaked (and reported by grpc-java's orphan detector) when this relay is
        // discarded.
        clprEndpointClient.close();
        grpcServer.stop();
        // Stop the per-account submitter worker threads only after both bundle producers — the
        // outbound sync loops (service.stop()) and the inbound gRPC handler (grpcServer.stop()) — have
        // been signalled to stop, so nothing enqueues onto a submitter whose worker is being torn down.
        for (final var network : localNetworks.values()) {
            network.close();
        }
    }

    /**
     * Returns the gRPC port, or {@code -1} if not started.
     *
     * @return the gRPC port
     */
    public int grpcPort() {
        return grpcServer.port();
    }

    /**
     * Returns whether this instance is currently running.
     *
     * @return {@code true} if {@link #start()} has been called and {@link #stop()} has not
     */
    public boolean isRunning() {
        return running && grpcServer.isRunning();
    }

    /**
     * De-duplicate the configured local networks by {@code id}: a repeated id keeps the first entry
     * and logs a warning for each ignored duplicate.
     */
    static List<RelayConfig.LocalNetworkConfig> dedupeLocalNetworks(
            final List<RelayConfig.LocalNetworkConfig> networks) {
        final var seen = new HashSet<String>();
        final var unique = new ArrayList<RelayConfig.LocalNetworkConfig>(networks.size());
        for (final var network : networks) {
            if (!seen.add(network.id())) {
                log.warn("Duplicate localNetwork id '{}' — ignoring the duplicate entry", network.id());
                continue;
            }
            unique.add(network);
        }
        return unique;
    }

    /** Composite key identifying a ClprService deployment: unique per {@code (localNetwork, serviceAddress)}. */
    private static String serviceKey(final String localNetwork, final String serviceAddress) {
        return localNetwork + "|" + serviceAddress;
    }

    /**
     * De-duplicate the configured services by {@code (localNetwork, serviceAddress)}: a ClprService is
     * unique within its local network, so a repeated pair keeps the first entry and logs a warning for
     * each ignored duplicate. The same {@code serviceAddress} on a <em>different</em> local network is
     * a distinct deployment and is kept. Values are used as provided (no case normalisation).
     */
    static List<RelayConfig.ClprServiceConfig> dedupeClprServices(final List<RelayConfig.ClprServiceConfig> services) {
        final var seen = new HashSet<String>();
        final var unique = new ArrayList<RelayConfig.ClprServiceConfig>(services.size());
        for (final var service : services) {
            if (!seen.add(serviceKey(service.localNetwork(), service.serviceAddress()))) {
                log.warn(
                        "Duplicate ClprService for localNetwork '{}' serviceAddress {} — ignoring the duplicate entry",
                        service.localNetwork(),
                        service.serviceAddress());
                continue;
            }
            unique.add(service);
        }
        return unique;
    }

    /** Resolve a channel handler by id across every service handler. */
    private static Optional<ClprChannelHandler> findChannelHandler(
            final Map<String, ClprServiceHandler> clprServices, final Bytes channelId) {
        for (final var service : clprServices.values()) {
            final var handler = service.channelHandler(channelId);
            if (handler.isPresent()) {
                return handler;
            }
        }
        return Optional.empty();
    }

    /**
     * Register the aggregate per-loop failure gauges ({@code sync.channels.failing} /
     * {@code sync.channels.max_consecutive_failures} and the {@code evm.listener.*} pair),
     * computed at scrape time from every live channel handler's {@code FailState}.
     */
    private static void registerFailureGauges(
            final Metrics metrics, final Map<String, ClprServiceHandler> clprServices) {
        final LongGauge syncFailing = metrics.getOrCreate(new LongGauge.Config("sync", "channels.failing")
                .withDescription("Channels whose sync loop is currently in a failing (backing-off) state"));
        final LongGauge syncMax = metrics.getOrCreate(new LongGauge.Config("sync", "channels.max_consecutive_failures")
                .withDescription("Maximum consecutive sync-cycle failures across all channels"));
        final LongGauge listenerFailing = metrics.getOrCreate(new LongGauge.Config("evm.listener", "channels.failing")
                .withDescription("Channels whose poll loop is currently in a failing (backing-off) state"));
        final LongGauge listenerMax =
                metrics.getOrCreate(new LongGauge.Config("evm.listener", "channels.max_consecutive_failures")
                        .withDescription("Maximum consecutive poll failures across all channels"));
        metrics.addUpdater(() -> {
            long syncFailingCount = 0;
            long syncMaxStreak = 0;
            long listenerFailingCount = 0;
            long listenerMaxStreak = 0;
            for (final var service : clprServices.values()) {
                for (final var handler : service.channelHandlers()) {
                    final int syncStreak = handler.syncFailState().consecutiveFailures();
                    if (syncStreak > 0) {
                        syncFailingCount++;
                    }
                    if (syncStreak > syncMaxStreak) {
                        syncMaxStreak = syncStreak;
                    }
                    final int listenerStreak =
                            handler.manifestRefreshFailState().consecutiveFailures();
                    if (listenerStreak > 0) {
                        listenerFailingCount++;
                    }
                    if (listenerStreak > listenerMaxStreak) {
                        listenerMaxStreak = listenerStreak;
                    }
                }
            }
            syncFailing.set(syncFailingCount);
            syncMax.set(syncMaxStreak);
            listenerFailing.set(listenerFailingCount);
            listenerMax.set(listenerMaxStreak);
        });
    }

    /**
     * Decode a hex string (with or without {@code 0x} prefix) into a {@link Bytes} instance.
     *
     * @param hex the hex-encoded string
     * @return the decoded bytes
     * @throws IllegalArgumentException if the string has an odd number of hex digits
     */
    static Bytes hexToBytes(final String hex) {
        final var clean = hex.startsWith("0x") ? hex.substring(2) : hex;
        if (clean.length() % 2 != 0) {
            throw new IllegalArgumentException("Hex string has odd length: " + hex);
        }
        final var bytes = new byte[clean.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(clean.substring(i * 2, i * 2 + 2), 16);
        }
        return Bytes.wrap(bytes);
    }

    /**
     * Returns the canonical 20-byte form of a ClprService address, so that two representations
     * that differ only by leading-zero padding (e.g. a bare 20-byte address versus the same
     * address padded to a 32-byte word) compare equal. Keys {@code service_address} lookups
     * consistently whether the address comes from configuration or from an inbound request.
     */
    static Bytes normalizeServiceAddress(final Bytes raw) {
        final byte[] in = raw.toByteArray();
        final byte[] out = new byte[20];
        final int copy = Math.min(in.length, 20);
        System.arraycopy(in, in.length - copy, out, 20 - copy, copy);
        return Bytes.wrap(out);
    }
}
