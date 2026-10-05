// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.app;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprEndpointManifest;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.context.Context;
import com.swirlds.logging.api.Logger;
import com.swirlds.logging.api.Loggers;
import com.swirlds.metrics.api.Metrics;
import java.time.Duration;
import java.util.function.LongSupplier;
import org.hiero.clpr.relay.core.BundleConstructor;
import org.hiero.clpr.relay.core.BundlePayloadCodec;
import org.hiero.clpr.relay.core.BundleRequestConstructor;
import org.hiero.clpr.relay.core.CachedBundleConstructor;
import org.hiero.clpr.relay.core.ClprChannelOps;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.FailState;
import org.hiero.clpr.relay.core.HieroProofCodec;
import org.hiero.clpr.relay.core.InboundBundleQueue;
import org.hiero.clpr.relay.core.LedgerConfigurationPayloadProvider;
import org.hiero.clpr.relay.core.ParsedBundle;
import org.hiero.clpr.relay.core.PeerEndpointCache;
import org.hiero.clpr.relay.core.PeerEndpointTlsRegistry;
import org.hiero.clpr.relay.core.RelayProtocol;
import org.hiero.clpr.relay.core.StubBundleConstructor;
import org.hiero.clpr.relay.core.StubProofCodec;
import org.hiero.clpr.relay.core.ThrottleEnforcer;
import org.hiero.clpr.relay.core.metrics.LabeledCounter;
import org.hiero.clpr.relay.evm.AccountTransactionSubmitter;
import org.hiero.clpr.relay.evm.EndpointManifestRefreshTask;
import org.hiero.clpr.relay.evm.EthSigner;
import org.hiero.clpr.relay.evm.EthereumPosBundleConstructor;
import org.hiero.clpr.relay.evm.EthereumPosLedgerConfigurationProvider;
import org.hiero.clpr.relay.evm.EthereumPosProofCodec;
import org.hiero.clpr.relay.evm.EvmContractStateReader;
import org.hiero.clpr.relay.evm.beacon.BeaconApiClient;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.qbft.QbftBundleConstructor;
import org.hiero.clpr.relay.evm.qbft.QbftLedgerConfigurationProvider;
import org.hiero.clpr.relay.evm.qbft.QbftProofCodec;
import org.hiero.clpr.relay.evm.sei.SeiBundleConstructor;
import org.hiero.clpr.relay.evm.sei.SeiLedgerConfigurationProvider;
import org.hiero.clpr.relay.evm.sei.SeiProofCodec;
import org.hiero.clpr.relay.grpc.client.ClprEndpointClient;
import org.hiero.clpr.relay.sync.ChannelSyncTask;
import org.hiero.clpr.relay.sync.PeerSelector;
import org.jspecify.annotations.Nullable;

/**
 * The channel-scoped unit of the relay. One handler owns every component tied to a single CLPR
 * channel: the contract state reader, transaction submitter, bundle constructor, ledger-config
 * provider, peer endpoint cache, inbound throttle enforcer, inbound proof codec, and the two worker
 * loops it spawns — the outbound {@link ChannelSyncTask} and the on-chain
 * {@link EndpointManifestRefreshTask}. Handlers are created and managed by a
 * {@link ClprServiceHandler}, one per {@code ClprService} deployment.
 *
 * <p>Construction ({@link #create}) performs the blocking JSON-RPC reads needed to bring the
 * channel online — on-chain protocol-version validation, the ledger configuration (for the
 * inbound throttles), and the peer endpoint roster. Any failure aborts construction so no
 * half-wired handler is ever published; the caller (predefined wiring or discovery) simply retries.
 * The handler spawns no threads until {@link #start(boolean)} is called.
 */
public final class ClprChannelHandler implements ClprChannelOps {

    private static final Logger log = Loggers.getLogger(ClprChannelHandler.class);

    private final Bytes channelId;
    private final String serviceAddress;
    private final CommitmentLevel commitmentLevel;

    private final EvmContractStateReader stateReader;
    private final AccountTransactionSubmitter txSubmitter;
    private final BundleConstructor bundleConstructor;
    private final LedgerConfigurationPayloadProvider ledgerConfigProvider;
    private final PeerEndpointCache peerCache;
    private final ThrottleEnforcer throttleEnforcer;
    private final BundlePayloadCodec inboundCodec;

    private final Metrics metrics;
    private final LabeledCounter bundleEnqueued;
    private final RelayConfig.BackoffConfig backoffConfig;
    private final ClprEndpointClient clprEndpointClient;
    private final PeerEndpointTlsRegistry tlsRegistry;
    private final RelayConfig.CommonEvmParams evmParams;
    private final long syncIntervalMs;

    @Nullable
    private ChannelSyncTask syncTask;

    @Nullable
    private EndpointManifestRefreshTask manifestRefreshTask;

    private final String instanceName;

    private volatile boolean started;

    private ClprChannelHandler(
            final Bytes channelId,
            final String serviceAddress,
            final CommitmentLevel commitmentLevel,
            final EvmContractStateReader stateReader,
            final AccountTransactionSubmitter txSubmitter,
            final BundleConstructor bundleConstructor,
            final LedgerConfigurationPayloadProvider ledgerConfigProvider,
            final PeerEndpointCache peerCache,
            final ThrottleEnforcer throttleEnforcer,
            final BundlePayloadCodec inboundCodec,
            final Metrics metrics,
            final RelayConfig.BackoffConfig backoffConfig,
            final ClprEndpointClient clprEndpointClient,
            final PeerEndpointTlsRegistry tlsRegistry,
            final RelayConfig.CommonEvmParams evmParams,
            final long syncIntervalMs,
            final String instanceName) {
        this.channelId = channelId;
        this.serviceAddress = serviceAddress;
        this.commitmentLevel = commitmentLevel;
        this.stateReader = stateReader;
        this.txSubmitter = txSubmitter;
        this.bundleConstructor = bundleConstructor;
        this.ledgerConfigProvider = ledgerConfigProvider;
        this.peerCache = peerCache;
        this.throttleEnforcer = throttleEnforcer;
        this.inboundCodec = inboundCodec;
        this.metrics = metrics;
        this.bundleEnqueued = new LabeledCounter(
                "sync",
                "bundle.enqueued",
                "Inbound peer bundles accepted onto the submit queue (passed parse + throttle)",
                metrics);
        this.backoffConfig = backoffConfig;
        this.clprEndpointClient = clprEndpointClient;
        this.tlsRegistry = tlsRegistry;
        this.evmParams = evmParams;
        this.syncIntervalMs = syncIntervalMs;
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
     * @param clprEndpointClient outbound gRPC client used by the sync loop
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
            final ClprEndpointClient clprEndpointClient,
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
        // nonce tracking is needed) and runs the gas-free eth_call preview per request.
        final AccountTransactionSubmitter txSubmitter = network.accountSubmitterFor(signer);

        final var clprServiceAddress = Address.fromHexString(serviceAddress);

        // The head read the decorator uses to key the cache must match the height each constructor
        // proves against: QBFT proves against the commitment head (no lag), Sei backs off proofLagBlocks.
        final BundleConstructor rawBundleConstructor;
        // Null for proof types that read at a commitment tag rather than a numeric height (Stub):
        // those are left unwrapped by CachedBundleConstructor below.
        final @Nullable LongSupplier localChainHeight;
        final LedgerConfigurationPayloadProvider ledgerConfigProvider;
        switch (network.proofType()) {
            case QBFT -> {
                final var qbft = network.config().qbft();
                rawBundleConstructor = new QbftBundleConstructor(
                        clprServiceAddress,
                        qbft.epochLength(),
                        qbft.maxEpochBlockHeadersPerBundle(),
                        qbft.maxMessagesPerBundle(),
                        rpcClient,
                        stateReader,
                        commitmentLevel,
                        qbft.maxBundleSize());
                localChainHeight = () -> rpcClient
                        .ethGetBlockHeaderByNumber(commitmentLevel.toBlockTag())
                        .number()
                        .longValueExact();
                ledgerConfigProvider = new QbftLedgerConfigurationProvider(
                        rpcClient, stateReader, clprServiceAddress, qbft.epochLength());
            }
            case CometBFT -> {
                final var cometBft = network.config().cometBft();
                final var cometBftRpcClient = network.cometBftRpcClient()
                        .orElseThrow(() -> new IllegalStateException(
                                "CometBFT network '" + network.id() + "' has no CometBFT RPC client"));
                rawBundleConstructor = new SeiBundleConstructor(
                        clprServiceAddress,
                        cometBft.maxMessagesPerBundle(),
                        cometBft.maxPriorValidatorSetUpdates(),
                        cometBftRpcClient,
                        rpcClient,
                        stateReader,
                        commitmentLevel,
                        proofLagBlocks,
                        cometBft.maxBundleSize());
                localChainHeight = () -> rpcClient
                                .ethGetBlockHeaderByNumber(commitmentLevel.toBlockTag())
                                .number()
                                .longValueExact()
                        - proofLagBlocks;
                ledgerConfigProvider = new SeiLedgerConfigurationProvider(rpcClient, stateReader, cometBftRpcClient);
            }
            case Stub -> {
                // Test-only local network: build CLPRSTUB bundles from real channel state for the
                // StubClprVerifier, exercising the full two-phase sync + submission path without a real
                // proof. The stub reads at a commitment tag and reports no numeric height, so it is
                // deliberately left unwrapped by CachedBundleConstructor (its height sentinel would
                // otherwise collide across chain heights and serve stale bundles).
                rawBundleConstructor = new StubBundleConstructor(stateReader, commitmentLevel);
                localChainHeight = null;
                // getLedgerConfiguration has no meaning for a stub network (the sync path never calls
                // it; it is exposed only on the info plane), so fail loudly if a tool ever asks.
                ledgerConfigProvider = commitment -> {
                    throw new UnsupportedOperationException(
                            "getLedgerConfiguration is not supported for a Stub local network");
                };
            }
            case EthereumPOS -> {
                final var beacon = network.config().beacon();
                final var beaconClient = new BeaconApiClient(
                        beacon.beaconApiUrl(), beacon.maxRetries(), Duration.ofMillis(beacon.requestTimeoutMs()));
                rawBundleConstructor = new EthereumPosBundleConstructor(
                        clprServiceAddress,
                        beacon.maxMessagesPerBundle(),
                        rpcClient,
                        beaconClient,
                        stateReader,
                        commitmentLevel);
                // The bundle proves against the beacon-attested (finalized) execution block, which lags
                // head and is derived inside createBundle from the light-client update — a cheap head
                // probe can't predict it, so the (expensive) build is left uncached, like Stub, rather
                // than risking a stale cache hit. Caching keyed on the attested block is a future option.
                localChainHeight = null;
                ledgerConfigProvider = new EthereumPosLedgerConfigurationProvider(rpcClient, stateReader, beaconClient);
            }
            default ->
                throw new UnsupportedOperationException(
                        "Local network proof type " + network.proofType() + " is not supported");
        }
        // Memoize built bundles so a burst of identical peer requests at the same height reuses one
        // build. Skipped for constructors that report no numeric height (Stub), which must not be cached.
        final BundleConstructor bundleConstructor = localChainHeight == null
                ? rawBundleConstructor
                : new CachedBundleConstructor(rawBundleConstructor, localChainHeight);

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

        return new ClprChannelHandler(
                channelId,
                serviceAddress,
                commitmentLevel,
                stateReader,
                txSubmitter,
                bundleConstructor,
                ledgerConfigProvider,
                peerCache,
                throttleEnforcer,
                inboundCodec,
                metrics,
                backoff,
                clprEndpointClient,
                tlsRegistry,
                evmParams,
                syncIntervalMs,
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
        manifestRefreshTask = new EndpointManifestRefreshTask(
                channelId,
                stateReader,
                commitmentLevel,
                peerCache,
                tlsRegistry,
                metrics,
                evmParams.pollIntervalMs(),
                backoffConfig.baseMs(),
                backoffConfig.capMs());
        spawn(" clpr-manifest-refresh", manifestRefreshTask::run);

        if (outboundSync) {
            syncTask = new ChannelSyncTask(
                    this,
                    commitmentLevel,
                    new PeerSelector(peerCache),
                    clprEndpointClient,
                    metrics,
                    backoffConfig.baseMs(),
                    backoffConfig.capMs(),
                    syncIntervalMs);
            spawn(" clpr-sync-task", syncTask::run);
        }

        log.info("Started channel {} (outboundSync={})", channelId, outboundSync);
    }

    /** Signal both worker loops to stop and interrupt any in-flight wait. */
    public synchronized void stop() {
        // syncTask is only spawned for outbound (full-duplex) channels; manifestRefreshTask only after
        // start(). Guard both so an inbound-only or never-started handler tears down cleanly rather than
        // NPEing (which would also strand the manifest poller against a torn-down node).
        if (syncTask != null) {
            syncTask.stop();
        }
        if (manifestRefreshTask != null) {
            manifestRefreshTask.stop();
        }
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
            case Stub -> new StubProofCodec();
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

    // --- ClprChannelOps + accessors consumed by the gRPC glue (ClprSyncHandler via ChannelLookup) and metrics. ---

    /** The channel identifier this handler manages. */
    public Bytes channelId() {
        return channelId;
    }

    /** The commitment level used for this channel's on-chain reads. */
    public CommitmentLevel commitmentLevel() {
        return commitmentLevel;
    }

    /** The per-channel bundle constructor / cached-proof supplier. */
    public BundleConstructor bundleConstructor() {
        return bundleConstructor;
    }

    @Override
    public BundleRequestConstructor bundleRequestConstructor() {
        return () -> {
            final var channelStateOpt = stateReader.readChannelState(channelId, "pending");
            if (channelStateOpt.isEmpty()) {
                return BundleRequest.newBuilder()
                        .currentReceivedMessageId(0L)
                        .currentStatus(ClprChannelStatus.PENDING)
                        .build();
            }
            final var channel = channelStateOpt.get();

            return BundleRequest.newBuilder()
                    .currentReceivedMessageId(channel.receivedMessageId())
                    .currentStatus(channel.status())
                    .currentTrustAnchorId(channel.trustAnchorId())
                    // The version of the peer's endpoint manifest we currently hold, cached on-chain in the
                    // Channel struct — so the peer re-proves its manifest only when it has advanced past this.
                    .currentEndpointManifestVersion(channel.endpointManifestVersion())
                    .build();
        };
    }

    @Override
    public InboundBundleQueue inboundBundleQueue() {
        return (rawBundleBytes, peer) -> {
            if (rawBundleBytes.length() == 0) {
                log.warn("Ignoring empty bundle from {}", peer);
                return;
            }

            final var currentStateOpt = stateReader.readChannelState(channelId, "pending");
            if (currentStateOpt.isEmpty()) {
                log.warn("Ignoring incoming bundle from {} - channel not yet initialized", peer);
                return;
            }
            final var currentState = currentStateOpt.get();

            if (ClprChannelStatus.CLOSED.equals(currentState.status())) {
                log.warn("Ignoring incoming bundle from {} - channel is closed", peer);
                return;
            }

            final ParsedBundle parsedBundle;
            try {
                parsedBundle = inboundCodec.parseBundle(rawBundleBytes, currentState.receivedMessageId());
            } catch (Exception e) {
                log.warn("Parsing bundle from {} failed: {}", peer, e);
                return;
            }

            if (!throttleEnforcer.shouldAccept(parsedBundle.rawBundleBytes(), parsedBundle.messages())) {
                log.info("bundle was rejected before adding to the queue due to throttles (size limits)");
                return;
            }

            // Accepted onto the submit queue (parse + throttle passed). Labels mirror the submitter's
            // channel_id/peer so bundle.enqueued → bundle.submissions/reverted/skipped can be joined.
            final String peerLabel = peer != null ? peer.accountId().toHex() : "anonymous";
            bundleEnqueued.increment("channel_id", channelId.toHex(), "peer", peerLabel);
            txSubmitter.submit(serviceAddress, channelId, parsedBundle, peer);
        };
    }

    /** The per-channel ledger-configuration payload provider. */
    // TODO: this should live on ClprServiceHandler and be parametrized with proofType
    public LedgerConfigurationPayloadProvider ledgerConfigProvider() {
        return ledgerConfigProvider;
    }

    @Override
    public ContractStateReader contractStateReader() {
        return stateReader;
    }

    @Override
    public PeerEndpointCache peerEndpointCache() {
        return peerCache;
    }

    /** The outbound sync loop's failure state (for aggregate metrics). */
    public FailState syncFailState() {
        return syncTask.failState();
    }

    /** The state-change poll loop's failure state (for aggregate metrics). */
    public FailState manifestRefreshFailState() {
        return manifestRefreshTask.failState();
    }
}
