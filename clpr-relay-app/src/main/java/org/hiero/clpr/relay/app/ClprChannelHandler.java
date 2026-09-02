// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.app;

import com.hedera.hapi.node.state.clpr.ClprEndpointManifest;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.context.Context;
import com.swirlds.logging.api.Logger;
import com.swirlds.logging.api.Loggers;
import com.swirlds.metrics.api.Metrics;
import java.time.Duration;
import java.util.function.UnaryOperator;
import org.hiero.clpr.relay.core.BundleConstructor;
import org.hiero.clpr.relay.core.BundlePayloadCodec;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.FailState;
import org.hiero.clpr.relay.core.HieroProofCodec;
import org.hiero.clpr.relay.core.LedgerConfigurationPayloadProvider;
import org.hiero.clpr.relay.core.PeerEndpointCache;
import org.hiero.clpr.relay.core.PeerEndpointTlsRegistry;
import org.hiero.clpr.relay.core.PeerManifestVersionCache;
import org.hiero.clpr.relay.core.RelayProtocol;
import org.hiero.clpr.relay.core.TransactionSubmitter;
import org.hiero.clpr.relay.core.metrics.LabeledCounter;
import org.hiero.clpr.relay.evm.EthSigner;
import org.hiero.clpr.relay.evm.EthereumPosBundleConstructor;
import org.hiero.clpr.relay.evm.EthereumPosLedgerConfigurationProvider;
import org.hiero.clpr.relay.evm.EthereumPosProofCodec;
import org.hiero.clpr.relay.evm.EvmChannelStateChangeTask;
import org.hiero.clpr.relay.evm.EvmContractStateReader;
import org.hiero.clpr.relay.evm.EvmQbftLedgerConfigurationProvider;
import org.hiero.clpr.relay.evm.QbftBundleConstructor;
import org.hiero.clpr.relay.evm.QbftProofCodec;
import org.hiero.clpr.relay.evm.beacon.BeaconApiClient;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.sei.SeiBundleConstructor;
import org.hiero.clpr.relay.evm.sei.SeiLedgerConfigurationProvider;
import org.hiero.clpr.relay.evm.sei.SeiProofCodec;
import org.hiero.clpr.relay.grpc.client.ClprEndpointClient;
import org.hiero.clpr.relay.grpc.server.ThrottleEnforcer;
import org.hiero.clpr.relay.sync.ChannelSyncTask;
import org.hiero.clpr.relay.sync.PeerSelector;
import org.jspecify.annotations.Nullable;

/**
 * The channel-scoped unit of the relay. One handler owns every component tied to a single CLPR
 * channel: the contract state reader, transaction submitter, bundle constructor, ledger-config
 * provider, peer endpoint cache, inbound throttle enforcer, inbound proof codec, and the two worker
 * loops it spawns — the outbound {@link ChannelSyncTask} and the on-chain
 * {@link EvmChannelStateChangeTask}. Handlers are created and managed by a
 * {@link ClprServiceHandler}, one per {@code ClprService} deployment.
 *
 * <p>Construction ({@link #create}) performs the blocking JSON-RPC reads needed to bring the
 * channel online — on-chain protocol-version validation, the ledger configuration (for the
 * inbound throttles), and the peer endpoint roster. Any failure aborts construction so no
 * half-wired handler is ever published; the caller (predefined wiring or discovery) simply retries.
 * The handler spawns no threads until {@link #start(boolean)} is called.
 */
public final class ClprChannelHandler {

    private static final Logger log = Loggers.getLogger(ClprChannelHandler.class);

    private final Bytes channelId;
    private final CommitmentLevel commitmentLevel;

    private final EvmContractStateReader stateReader;
    private final TransactionSubmitter txSubmitter;
    private final BundleConstructor bundleConstructor;
    private final LedgerConfigurationPayloadProvider ledgerConfigProvider;
    private final PeerEndpointCache peerCache;
    private final ThrottleEnforcer throttleEnforcer;
    private final BundlePayloadCodec inboundCodec;

    private final ChannelSyncTask syncTask;
    private final EvmChannelStateChangeTask stateChangeTask;
    private final String instanceName;

    // Handles to the child threads this handler spawns in start(); null until started.
    @Nullable
    private volatile Thread syncThread;

    @Nullable
    private volatile Thread stateChangeThread;

    private volatile boolean started;

    private ClprChannelHandler(
            final Bytes channelId,
            final CommitmentLevel commitmentLevel,
            final EvmContractStateReader stateReader,
            final TransactionSubmitter txSubmitter,
            final BundleConstructor bundleConstructor,
            final LedgerConfigurationPayloadProvider ledgerConfigProvider,
            final PeerEndpointCache peerCache,
            final ThrottleEnforcer throttleEnforcer,
            final BundlePayloadCodec inboundCodec,
            final ChannelSyncTask syncTask,
            final EvmChannelStateChangeTask stateChangeTask,
            final String instanceName) {
        this.channelId = channelId;
        this.commitmentLevel = commitmentLevel;
        this.stateReader = stateReader;
        this.txSubmitter = txSubmitter;
        this.bundleConstructor = bundleConstructor;
        this.ledgerConfigProvider = ledgerConfigProvider;
        this.peerCache = peerCache;
        this.throttleEnforcer = throttleEnforcer;
        this.inboundCodec = inboundCodec;
        this.syncTask = syncTask;
        this.stateChangeTask = stateChangeTask;
        this.instanceName = instanceName;
    }

    /**
     * Build a fully-wired channel handler, performing the blocking bootstrap reads described in
     * the class javadoc. Does not start any thread.
     *
     * @param network            shared clients for the channel's local network
     * @param serviceAddress     hex address of the {@code ClprService} contract for this channel
     * @param channelId       the 32-byte channel identifier
     * @param peerProofType      the peer's proof format (selects the inbound {@link BundlePayloadCodec})
     * @param signer             the ECDSA signer resolved for this channel (EVM tx + submission)
     * @param commitmentLevel    commitment level used for on-chain reads
     * @param syncIntervalMs     outbound sync cadence, in milliseconds
     * @param proofLagBlocks     blocks the state read lags behind the commitment head
     * @param metrics            metrics registry
     * @param backoff            per-loop failure-backoff policy
     * @param instanceName       worker-loop log-context label; a blank value adds no context entry
     * @param submitterDecorator wraps the per-account submitter's channel view
     *                           ({@link UnaryOperator#identity()} in production)
     * @param clprEndpointClient outbound gRPC client used by the sync loop
     * @param peerManifestVersions global cache of the endpoint-manifest version each peer last
     *                           reported holding, consumed by the QBFT bundle constructor
     * @param tlsRegistry        global TLS registry updated alongside the peer cache on every
     *                           manifest change and at bootstrap
     * @return a not-yet-started channel handler
     */
    public static ClprChannelHandler create(
            final LocalNetworkAdapter network,
            final String serviceAddress,
            final Bytes channelId,
            final ProofType peerProofType,
            final EthSigner signer,
            final CommitmentLevel commitmentLevel,
            final long syncIntervalMs,
            final int proofLagBlocks,
            final Metrics metrics,
            final RelayConfig.BackoffConfig backoff,
            final String instanceName,
            final UnaryOperator<TransactionSubmitter> submitterDecorator,
            final ClprEndpointClient clprEndpointClient,
            final PeerManifestVersionCache peerManifestVersions,
            final PeerEndpointTlsRegistry tlsRegistry) {

        final var evmParams = network.config().evm();
        final var rpcClient = network.rpcClient();

        final var stateReader = new EvmContractStateReader(
                rpcClient,
                serviceAddress,
                new LabeledCounter(
                        "evm.manifest",
                        "read.failed",
                        "Endpoint-manifest read failures (scope=local|peer, reason=rpc_error|decode_error)",
                        metrics));

        // Share the per-account submitter across every channel/service on this network that signs
        // from the same key. It fully serialises submission (at most one tx per account in flight, so no
        // nonce tracking is needed) and runs the gas-free eth_call preview per request. submitterDecorator
        // is identity in production; the integration suite injects the CLPRSTUB re-encoder here, so the
        // re-encode runs first and the submitter's preview simulates the exact bytes it will submit
        // against the StubClprVerifier.
        final TransactionSubmitter txSubmitter =
                submitterDecorator.apply(network.accountSubmitterFor(signer).forContract(serviceAddress));

        final var clprServiceAddress = Address.fromHexString(serviceAddress);

        final BundleConstructor bundleConstructor;
        final LedgerConfigurationPayloadProvider ledgerConfigProvider;
        switch (network.proofType()) {
            case QBFT -> {
                final var qbft = network.config().qbft();
                bundleConstructor = new QbftBundleConstructor(
                        clprServiceAddress,
                        qbft.epochLength(),
                        qbft.maxEpochBlockHeadersPerBundle(),
                        qbft.maxMessagesPerBundle(),
                        rpcClient,
                        stateReader,
                        peerManifestVersions);
                ledgerConfigProvider = new EvmQbftLedgerConfigurationProvider(
                        rpcClient, stateReader, clprServiceAddress, qbft.epochLength());
            }
            case CometBFT -> {
                final var cometBft = network.config().cometBft();
                final var cometBftRpcClient = network.cometBftRpcClient()
                        .orElseThrow(() -> new IllegalStateException(
                                "CometBFT network '" + network.id() + "' has no CometBFT RPC client"));
                bundleConstructor = new SeiBundleConstructor(
                        clprServiceAddress,
                        cometBft.maxMessagesPerBundle(),
                        cometBft.maxPriorValidatorSetUpdates(),
                        cometBftRpcClient,
                        stateReader,
                        peerManifestVersions);
                ledgerConfigProvider = new SeiLedgerConfigurationProvider(rpcClient, stateReader, cometBftRpcClient);
            }
            case EthereumPOS -> {
                final var beacon = network.config().beacon();
                final var beaconClient = new BeaconApiClient(
                        beacon.beaconApiUrl(), beacon.maxRetries(), Duration.ofMillis(beacon.requestTimeoutMs()));
                bundleConstructor = new EthereumPosBundleConstructor(
                        clprServiceAddress,
                        beacon.maxMessagesPerBundle(),
                        rpcClient,
                        beaconClient,
                        stateReader,
                        peerManifestVersions);

                ledgerConfigProvider = new EthereumPosLedgerConfigurationProvider(rpcClient, stateReader, beaconClient);
            }
            default ->
                throw new UnsupportedOperationException(
                        "Local network proof type " + network.proofType() + " is not supported");
        }

        // Bootstrap reads: on-chain protocol version + throttles (from the ledger configuration) and
        // the peer endpoint manifest. These block and may throw; doing them here means a failed read
        // aborts construction rather than leaving a half-wired handler registered.
        final var ledgerConfig = stateReader.readLedgerConfiguration(commitmentLevel);
        validateProtocolVersion(serviceAddress, ledgerConfig.protocolVersion());

        final var throttles = ledgerConfig.throttles();
        final int maxMessagePerBundle = throttles != null ? throttles.maxMessagesPerBundle() : Integer.MAX_VALUE;
        final int maxMessagePayloadBytes = throttles != null ? throttles.maxMessagePayloadBytes() : Integer.MAX_VALUE;
        final long maxSyncBytes = throttles != null ? throttles.maxSyncBytes() : Integer.MAX_VALUE;
        final var throttleEnforcer = new ThrottleEnforcer(maxSyncBytes, maxMessagePerBundle, maxMessagePayloadBytes);

        final var peerCache = new PeerEndpointCache();
        final ClprEndpointManifest manifest = stateReader.readPeerEndpointManifest(channelId, commitmentLevel);
        if (manifest.endpoints().isEmpty()) {
            log.warn(
                    "Empty peer endpoint manifest for channel {}. Peer cache is empty; sync will not proceed"
                            + " until the peer endpoint manifest is populated.",
                    channelId);
        } else {
            // The endpoint manifest is the peer source of truth: its endpoints carry the same
            // on-chain TLS certificates the legacy roster did, so it feeds both the peer cache and
            // the mTLS trust registry in one read.
            peerCache.replaceAll(manifest.endpoints());
            tlsRegistry.update(channelId, manifest.endpoints());
            log.info(
                    "Loaded {} peer endpoint(s) from manifest for channel {}",
                    manifest.endpoints().size(),
                    channelId);
        }

        final var inboundCodec = codecFor(peerProofType, channelId);

        final var stateChangeTask = new EvmChannelStateChangeTask(
                channelId,
                commitmentLevel,
                rpcClient,
                stateReader,
                bundleConstructor,
                peerCache,
                tlsRegistry,
                metrics,
                evmParams.pollIntervalMs(),
                backoff.baseMs(),
                backoff.capMs(),
                proofLagBlocks);

        final var peerSelector = new PeerSelector(peerCache);
        final var syncTask = new ChannelSyncTask(
                channelId,
                commitmentLevel,
                stateReader,
                bundleConstructor,
                inboundCodec,
                txSubmitter,
                peerSelector,
                clprEndpointClient,
                peerManifestVersions,
                metrics,
                backoff.baseMs(),
                backoff.capMs(),
                syncIntervalMs);

        return new ClprChannelHandler(
                channelId,
                commitmentLevel,
                stateReader,
                txSubmitter,
                bundleConstructor,
                ledgerConfigProvider,
                peerCache,
                throttleEnforcer,
                inboundCodec,
                syncTask,
                stateChangeTask,
                instanceName);
    }

    /**
     * Launch this channel's worker loops. The on-chain state-change poller always starts; the
     * outbound sync loop starts only when {@code outboundSync} is {@code true} (inbound-only relays
     * pass {@code false}). Idempotent: a second call is a no-op.
     *
     * @param outboundSync whether to start the outbound sync loop
     */
    public synchronized void start(final boolean outboundSync) {
        if (started) {
            return;
        }
        started = true;
        stateChangeThread = spawn(" clpr-state-change", stateChangeTask::run);
        if (outboundSync) {
            syncThread = spawn(" clpr-sync-task", syncTask::run);
        }
        log.info("Started channel {} (outboundSync={})", channelId, outboundSync);
    }

    /** Signal both worker loops to stop and interrupt any in-flight wait. */
    public synchronized void stop() {
        syncTask.stop();
        stateChangeTask.stop();
    }

    private Thread spawn(final String threadSuffix, final Runnable body) {
        return Thread.ofVirtual().name(instanceName + threadSuffix).start(() -> {
            // Tag every log line on this loop's thread with the channel id (and relay label), so
            // individual messages need not repeat it. One-shot virtual thread, so the context entry
            // is reclaimed when it terminates.
            if (!instanceName.isBlank()) {
                Context.getThreadLocalContext().add("relay", instanceName);
            }
            Context.getThreadLocalContext().add("conn", channelId.toHex());
            body.run();
        });
    }

    /** Select the inbound {@link BundlePayloadCodec} implementation for a peer's proof format. */
    private static BundlePayloadCodec codecFor(final ProofType peerProofType, final Bytes channelId) {
        return switch (peerProofType) {
            case QBFT -> new QbftProofCodec(channelId);
            case Hiero -> new HieroProofCodec();
            case CometBFT -> new SeiProofCodec(channelId);
            case EthereumPOS -> new EthereumPosProofCodec();
        };
    }

    /**
     * Abort the channel when the on-chain {@code protocolVersion} does not match
     * {@link RelayProtocol#PROTOCOL_VERSION}.
     */
    private static void validateProtocolVersion(final String serviceAddress, final int onChainVersion) {
        log.info(
                "service={} on-chain protocolVersion={}, relay protocolVersion={}",
                serviceAddress,
                onChainVersion,
                RelayProtocol.PROTOCOL_VERSION);
        if (onChainVersion != RelayProtocol.PROTOCOL_VERSION) {
            log.error(
                    "protocolVersion mismatch: service={}, contract={}, relay={} — refusing to register channel",
                    serviceAddress,
                    onChainVersion,
                    RelayProtocol.PROTOCOL_VERSION);
            RelayProtocol.validateProtocolVersion(onChainVersion);
        }
    }

    // --- Accessors consumed by the gRPC glue (ClprSyncHandler / discoverEndpoints) and metrics. ---

    /** The channel identifier this handler manages. */
    public Bytes channelId() {
        return channelId;
    }

    /** The commitment level used for this channel's on-chain reads. */
    public CommitmentLevel commitmentLevel() {
        return commitmentLevel;
    }

    /** The per-channel contract state reader. */
    public ContractStateReader stateReader() {
        return stateReader;
    }

    /** The per-channel transaction submitter (enqueues onto the shared per-account submitter). */
    public TransactionSubmitter txSubmitter() {
        return txSubmitter;
    }

    /** The per-channel bundle constructor / cached-proof supplier. */
    public BundleConstructor bundleConstructor() {
        return bundleConstructor;
    }

    /** The per-channel ledger-configuration payload provider. */
    public LedgerConfigurationPayloadProvider ledgerConfigProvider() {
        return ledgerConfigProvider;
    }

    /** The per-channel peer endpoint cache (seeded from the on-ledger roster). */
    public PeerEndpointCache peerCache() {
        return peerCache;
    }

    /** The per-channel inbound throttle enforcer. */
    public ThrottleEnforcer throttleEnforcer() {
        return throttleEnforcer;
    }

    /** The inbound proof codec for the peer's proof format. */
    public BundlePayloadCodec inboundCodec() {
        return inboundCodec;
    }

    /** The outbound sync loop's failure state (for aggregate metrics). */
    public FailState syncFailState() {
        return syncTask.failState();
    }

    /** The state-change poll loop's failure state (for aggregate metrics). */
    public FailState stateChangeFailState() {
        return stateChangeTask.failState();
    }
}
