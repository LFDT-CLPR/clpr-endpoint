// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.app;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.logging.api.Logger;
import com.swirlds.logging.api.Loggers;
import com.swirlds.metrics.api.Metrics;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.PeerEndpointTlsRegistry;
import org.hiero.clpr.relay.core.metrics.LabeledCounter;
import org.hiero.clpr.relay.evm.EthSigner;
import org.hiero.clpr.relay.evm.EvmContractStateReader;
import org.hiero.clpr.relay.grpc.client.ClprEndpointClient;
import org.hiero.clpr.relay.sync.ChannelSyncTask;
import org.jspecify.annotations.Nullable;

/**
 * Manages one {@code ClprService} contract deployment (a {@code (localNetwork, serviceAddress)}
 * pair) and the {@link ClprChannelHandler}s for every channel it serves. Channels arrive
 * two ways:
 *
 * <ul>
 *   <li><b>predefined</b> — declared by id in {@link RelayConfig.ClprServiceConfig#predefinedChannels()};
 *       registered when the service handler starts;</li>
 *   <li><b>discovered</b> — surfaced on-chain by the {@link ChannelDiscoveryTask} (present only
 *       when {@code discoverChannels} is enabled) after the relay is already running.</li>
 * </ul>
 *
 * <p>Both paths funnel through {@link #addChannel(Bytes, ProofType)}, which resolves the
 * channel's signing key, builds its {@link ClprChannelHandler}, and — when the service is
 * already running — starts it immediately.
 */
public final class ClprServiceHandler {

    private static final Logger log = Loggers.getLogger(ClprServiceHandler.class);

    /**
     * Default proof lag for a service whose local network runs CometBFT (Sei): a Sei bundle anchors
     * to the signed header at H+1 and the validator set at H+2, so the state read must lag the head
     * by two already-committed blocks. All other proof types read at the head (lag 0).
     */
    static final int DEFAULT_COMETBFT_PROOF_LAG_BLOCKS = 2;

    private final LocalNetworkAdapter network;
    private final String serviceAddress;
    private final RelayConfig.ClprServiceConfig config;
    private final Metrics metrics;
    private final RelayConfig.BackoffConfig backoff;
    private final String instanceName;
    private final ClprEndpointClient clprEndpointClient;
    private final PeerEndpointTlsRegistry tlsRegistry;

    // Resolved once for this service: the commitment level, sync cadence, and proof lag applied to
    // every channel it serves, plus the peer proof-type resolver and the normalised per-channel
    // signing-key overrides.
    private final CommitmentLevel commitmentLevel;
    private final long syncIntervalMs;
    private final int proofLagBlocks;
    private final Function<String, ProofType> peerProofTypeResolver;
    private final Map<String, String> perChannelSigningKeys;

    // Reused for predefined-channel peerProofType resolution and shared with the discovery task.
    private final EvmContractStateReader serviceStateReader;

    @Nullable
    private final ChannelDiscoveryTask discoveryTask;

    private final Map<Bytes, ClprChannelHandler> channelHandlers = new ConcurrentHashMap<>();

    private volatile boolean running;
    private volatile boolean outboundSync;

    private ClprServiceHandler(
            final LocalNetworkAdapter network,
            final RelayConfig.ClprServiceConfig config,
            final Metrics metrics,
            final RelayConfig.BackoffConfig backoff,
            final String instanceName,
            final ClprEndpointClient clprEndpointClient,
            final Map<String, ProofType> peerProofTypes,
            final PeerEndpointTlsRegistry tlsRegistry) {
        this.network = network;
        this.serviceAddress = config.serviceAddress();
        this.config = config;
        this.metrics = metrics;
        this.backoff = backoff;
        this.instanceName = instanceName;
        this.clprEndpointClient = clprEndpointClient;
        this.tlsRegistry = tlsRegistry;

        this.commitmentLevel = CommitmentLevel.LATEST;
        this.syncIntervalMs = ChannelSyncTask.DEFAULT_INTERVAL_MS;
        this.proofLagBlocks = network.proofType() == ProofType.CometBFT ? DEFAULT_COMETBFT_PROOF_LAG_BLOCKS : 0;
        this.peerProofTypeResolver = buildPeerProofTypeResolver(network.proofType(), peerProofTypes);
        this.perChannelSigningKeys = normaliseKeys(config.perChannelSigningPrivateKeyHex());
        this.serviceStateReader = new EvmContractStateReader(
                network.rpcClient(),
                serviceAddress,
                new LabeledCounter(
                        "evm.manifest",
                        "read.failed",
                        "Endpoint-manifest read failures (scope=local|peer, reason=rpc_error|decode_error)",
                        metrics));

        this.discoveryTask = config.discoverChannels() ? buildDiscoveryTask() : null;
    }

    /**
     * Build a service handler for one {@code ClprService} deployment.
     *
     * @param config                the service configuration
     * @param localNetworkAdapters  shared clients keyed by local-network id
     * @param metrics               metrics registry
     * @param backoff               per-loop failure-backoff policy
     * @param instanceName          worker-loop log-context label
     * @param clprEndpointClient    outbound gRPC client shared with the sync loops
     * @param peerProofTypes        CAIP-2 {@code chainId → ProofType} map used to resolve peers
     * @param tlsRegistry           global TLS registry updated whenever a channel's peer endpoint
     *                              manifest changes, so the mTLS trust manager stays current
     * @return a not-yet-started service handler
     * @throws IllegalArgumentException if {@code config.localNetwork()} is unknown
     */
    public static ClprServiceHandler create(
            final RelayConfig.ClprServiceConfig config,
            final Map<String, LocalNetworkAdapter> localNetworkAdapters,
            final Metrics metrics,
            final RelayConfig.BackoffConfig backoff,
            final String instanceName,
            final ClprEndpointClient clprEndpointClient,
            final Map<String, ProofType> peerProofTypes,
            final PeerEndpointTlsRegistry tlsRegistry) {
        final var network = localNetworkAdapters.get(config.localNetwork());
        if (network == null) {
            throw new IllegalArgumentException("clprServices['" + config.serviceAddress() + "'].localNetwork='"
                    + config.localNetwork() + "' does not reference a known localNetworks id");
        }
        return new ClprServiceHandler(
                network, config, metrics, backoff, instanceName, clprEndpointClient, peerProofTypes, tlsRegistry);
    }

    /**
     * Bring the service online: register every predefined channel, then start the discovery
     * poller (if enabled). Channels registered while running start their worker loops
     * immediately, honouring {@code options.outboundSync()}.
     *
     * @param options which subsystems to bring up
     */
    public synchronized void start(final RelayInstance.StartOptions options) {
        running = true;
        outboundSync = options.outboundSync();
        for (final var idHex : config.predefinedChannels()) {
            registerPredefinedChannel(idHex);
        }
        if (discoveryTask != null) {
            discoveryTask.start();
        }
    }

    /** Stop discovery and all channel handlers. */
    public synchronized void stop() {
        running = false;
        if (discoveryTask != null) {
            discoveryTask.stop();
        }
        for (final var handler : channelHandlers.values()) {
            handler.stop();
        }
    }

    /**
     * Register a channel, building and (when running) starting its {@link ClprChannelHandler}.
     * Idempotent per channel id and thread-safe against concurrent callers (discovery poller,
     * predefined registration) and against {@link #stop()}.
     *
     * @param channelId  the 32-byte channel identifier
     * @param peerProofType the peer's already-resolved proof format
     */
    public synchronized void addChannel(final Bytes channelId, final ProofType peerProofType) {
        if (channelHandlers.containsKey(channelId)) {
            log.info("Channel {} already registered on service {} — ignoring", channelId, serviceAddress);
            return;
        }
        final String signingKey = resolveSigningKey(channelId);
        if (signingKey == null || signingKey.isBlank()) {
            log.error(
                    "Cannot register channel {} on service {}: no signing key (neither a per-channel"
                            + " override nor a service default was configured)",
                    channelId,
                    serviceAddress);
            return;
        }
        final EthSigner signer;
        try {
            signer = new EthSigner(signingKey);
        } catch (final RuntimeException e) {
            log.error(
                    "Cannot register channel {} on service {}: invalid signing key ({})",
                    channelId,
                    serviceAddress,
                    e.getMessage());
            return;
        }

        final ClprChannelHandler handler;
        try {
            handler = ClprChannelHandler.create(
                    network,
                    serviceAddress,
                    channelId,
                    peerProofType,
                    signer,
                    commitmentLevel,
                    syncIntervalMs,
                    proofLagBlocks,
                    metrics,
                    backoff,
                    instanceName,
                    clprEndpointClient,
                    tlsRegistry);
        } catch (final RuntimeException e) {
            // A bootstrap read (protocol version / ledger config / roster) failed. Leave nothing
            // behind so discovery retries on its next cycle and a predefined channel is not
            // recorded as registered-yet-inert.
            log.error("Failed to register channel {} on service {}: {}", channelId, serviceAddress, e.getMessage());
            return;
        }

        channelHandlers.put(channelId, handler);
        if (running) {
            handler.start(outboundSync);
        }
    }

    /** Resolve a predefined channel's peer proof type and register it. */
    private void registerPredefinedChannel(final String idHex) {
        final Bytes channelId;
        try {
            channelId = RelayInstance.hexToBytes(idHex);
            if (channelId.length() != 32) {
                throw new RuntimeException("Channel ID must be exactly 32 bytes");
            }
        } catch (final RuntimeException e) {
            log.error(
                    "Skipping predefined channel '{}' on service {}: invalid channelId ({})",
                    idHex,
                    serviceAddress,
                    e.getMessage());
            return;
        }
        final ProofType peerProofType = resolvePeerProofType(channelId);
        if (peerProofType == null) {
            log.warn(
                    "Skipping predefined channel {} on service {}: peer proof type is not resolvable"
                            + " (unmapped, non-EVM peer this relay does not serve)",
                    channelId,
                    serviceAddress);
            return;
        }
        addChannel(channelId, peerProofType);
    }

    /**
     * Resolve a channel's peer proof type from its on-chain peer {@code chainId}. Fails open on a
     * read error (a null chainId falls back to a servable default); returns {@code null} for a peer
     * this relay cannot serve.
     */
    @Nullable
    private ProofType resolvePeerProofType(final Bytes channelId) {
        String peerChainId = null;
        try {
            peerChainId = serviceStateReader
                    .readChannelState(channelId, commitmentLevel.toBlockTag())
                    .map(com.hedera.hapi.node.state.clpr.ClprChannel::chainId)
                    .orElse(null);
        } catch (final RuntimeException e) {
            log.warn(
                    "Could not read on-chain state for {} on service {} ({}) — resolving peer proof type fail-open",
                    channelId,
                    serviceAddress,
                    e.getMessage());
        }
        return peerProofTypeResolver.apply(peerChainId);
    }

    private ChannelDiscoveryTask buildDiscoveryTask() {
        return new ChannelDiscoveryTask(
                serviceAddress,
                network.rpcClient(),
                serviceStateReader,
                network.config().evm().pollIntervalMs(),
                commitmentLevel.toBlockTag(),
                config.discoveryStartBlock(),
                backoff.baseMs(),
                backoff.capMs(),
                channelHandlers::containsKey,
                peerProofTypeResolver,
                this::addChannel,
                instanceName);
    }

    /**
     * Resolve a discovered/predefined channel's peer proof type from its on-chain peer
     * {@code chainId}: an explicit mapping wins; an EVM ({@code eip155:}) peer (or an unread chainId,
     * fail-open) falls back to this network's own proof type; any other unmapped peer is unservable
     * ({@code null}).
     */
    private static Function<String, ProofType> buildPeerProofTypeResolver(
            final ProofType localProofType, final Map<String, ProofType> peerProofTypes) {
        return peerChainId -> {
            if (peerChainId == null || peerChainId.isBlank()) {
                return localProofType;
            }
            final var mapped = peerProofTypes.get(peerChainId);
            if (mapped != null) {
                return mapped;
            }
            return peerChainId.startsWith("eip155:") ? localProofType : null;
        };
    }

    /** Per-channel override key, else the service default. */
    @Nullable
    private String resolveSigningKey(final Bytes channelId) {
        final var override = perChannelSigningKeys.get(normaliseHex(channelId.toHex()));
        return override != null ? override : config.defaultSigningPrivateKeyHex();
    }

    private static Map<String, String> normaliseKeys(final Map<String, String> raw) {
        final var out = new HashMap<String, String>(raw.size());
        raw.forEach((k, v) -> out.put(normaliseHex(k), v));
        return out;
    }

    /** Normalise a hex id/key for stable map lookups: strip any {@code 0x} prefix and lowercase. */
    static String normaliseHex(final String hex) {
        final var stripped = hex.startsWith("0x") || hex.startsWith("0X") ? hex.substring(2) : hex;
        return stripped.toLowerCase(Locale.ROOT);
    }

    /** The service contract address this handler manages. */
    public String serviceAddress() {
        return serviceAddress;
    }

    /** Look up a live channel handler by id. */
    public Optional<ClprChannelHandler> channelHandler(final Bytes channelId) {
        return Optional.ofNullable(channelHandlers.get(channelId));
    }

    /** A live view of the channel handlers this service manages. */
    public Collection<ClprChannelHandler> channelHandlers() {
        return channelHandlers.values();
    }
}
