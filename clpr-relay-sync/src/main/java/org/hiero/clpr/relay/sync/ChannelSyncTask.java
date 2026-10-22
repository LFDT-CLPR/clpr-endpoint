// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.sync;

import com.hedera.hapi.node.state.clpr.BundleResponse;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.logging.api.Logger;
import com.swirlds.logging.api.Loggers;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.Metrics;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.hiero.clpr.relay.core.ClprChannelOps;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.FailState;
import org.hiero.clpr.relay.core.Peers;
import org.hiero.clpr.relay.core.metrics.LabeledCounter;
import org.hiero.clpr.relay.core.metrics.MetricLabels;
import org.hiero.clpr.relay.grpc.client.ClprEndpointClient;
import org.jspecify.annotations.Nullable;

/**
 * Per-channel sync loop intended to run on a virtual thread.
 *
 * <p>Each cycle reads the current channel state from the chain, checks whether there are
 * outbound messages to deliver, fetches a cached proof, selects a peer, and
 * dispatches the sync request via {@link ClprEndpointClient}. Between cycles the task sleeps for a
 * configurable interval derived from the throttle settings.
 */
public class ChannelSyncTask implements Runnable {
    /** Default sync interval when not explicitly configured (ms). */
    public static final long DEFAULT_INTERVAL_MS = 1_000L;

    private static final Logger log = Loggers.getLogger(ChannelSyncTask.class);
    // Unique ID for this channel. Must have a matching ID on the source & destination ledgers
    private final ClprChannelOps clprChannel;
    // EVM chains have different commitment levels with different "finality" guarantees
    private final CommitmentLevel commitmentLevel;
    // Determines which remote peer to sync with
    private final PeerSelector peerSelector;
    // Used to talk with the remote peer
    private final ClprEndpointClient clprEndpointClient;

    // interval between sync cycles (ms) based on config
    private final long syncIntervalMs;
    // Hex label for this channel — computed once, reused in all labeled metrics.
    private final String channelIdLabel;
    private final Counter cyclesTotal;
    private final LabeledCounter cyclesFailed;
    private final Counter noPeerSkips;
    private final LabeledCounter peerSyncSuccess;
    private final LabeledCounter peerSyncErrors;
    private final LabeledCounter exchangesStarted;
    private final LabeledCounter exchangesFailed;
    private final LabeledCounter exchangesFailedAfterSend;
    private final FailState failState;
    private volatile boolean running = true;

    @Nullable
    private volatile Thread thread;

    /**
     * Creates a new {@code ChannelSyncTask}.
     *
     * @param commitmentLevel the commitment level used when reading on-chain state
     * @param peerSelector selects the remote peer to sync with
     * @param clprEndpointClient gRPC client used to send sync payloads to peers
     * @param metrics the metrics registry
     * @param backoffBaseMs first-failure backoff in ms (doubled per consecutive failure)
     * @param backoffCapMs ceiling on the per-failure backoff in ms
     * @param syncIntervalMs     sync intervals for running channel tasks
     */
    public ChannelSyncTask(
            final ClprChannelOps clprChannel,
            final CommitmentLevel commitmentLevel,
            final PeerSelector peerSelector,
            final ClprEndpointClient clprEndpointClient,
            final Metrics metrics,
            final long backoffBaseMs,
            final long backoffCapMs,
            final long syncIntervalMs) {
        this.clprChannel = clprChannel;
        this.commitmentLevel = commitmentLevel;
        this.peerSelector = peerSelector;
        this.clprEndpointClient = clprEndpointClient;
        this.syncIntervalMs = syncIntervalMs;

        this.channelIdLabel = HexFormat.of().formatHex(clprChannel.channelId().toByteArray());
        this.cyclesTotal = metrics.getOrCreate(
                new Counter.Config("sync", MetricLabels.labeled("cycles.total", "channel_id", channelIdLabel))
                        .withDescription("Total sync cycles attempted"));
        this.cyclesFailed = new LabeledCounter("sync", "cycles.failed", "Sync cycles that threw an exception", metrics);
        this.noPeerSkips = metrics.getOrCreate(
                new Counter.Config("sync", MetricLabels.labeled("no_peer_skips", "channel_id", channelIdLabel))
                        .withDescription("Cycles that skipped syncing because no peer was available"));

        this.peerSyncSuccess =
                new LabeledCounter("sync", "outbound.peer.success", "Successful outbound sync RPCs by peer", metrics);
        this.peerSyncErrors =
                new LabeledCounter("sync", "outbound.peer.errors", "Failed outbound sync RPCs by peer", metrics);
        this.exchangesStarted = new LabeledCounter(
                "sync",
                "exchanges.started",
                "Two-phase sync exchanges started (role=initiator dialed a peer | responder received a phase-1 request)",
                metrics);
        this.exchangesFailed = new LabeledCounter(
                "sync", "exchanges.failed", "Two-phase sync exchanges that failed before completing", metrics);
        this.exchangesFailedAfterSend = new LabeledCounter(
                "sync",
                "exchanges.failed_after_send",
                "Two-phase exchanges that failed after we had already sent a non-empty bundle (possible non-reciprocity)",
                metrics);

        this.failState = new FailState(backoffBaseMs, backoffCapMs);
    }

    /**
     * Run the sync loop on a virtual thread until {@link #stop()} or the channel transitions to
     * {@link ClprChannelStatus#CLOSED}.
     *
     * <p>Any unchecked exception from a cycle is caught and routed through {@link FailState}: the
     * loop logs at ERROR, backs off (bounded, capped — never the tight sync interval), and continues,
     * so a channel never dies silently; the first clean cycle resets the backoff. An interrupt —
     * from a cadence sleep or mid-backoff — breaks the loop.
     */
    public void run() {
        this.thread = Thread.currentThread();
        while (running) {
            try {
                executeSyncCycle();
                final int cleared = failState.onSuccess();
                if (cleared > 0) {
                    log.info("recovered after {} consecutive failure(s)", cleared);
                }
                if (running) {
                    // Wait for the next sync interval derived from settings.
                    Thread.sleep(syncIntervalMs);
                } else {
                    return;
                }
            } catch (final InterruptedException e) {
                // Shut down
                Thread.currentThread().interrupt();
                break;
            } catch (final Exception e) {
                if (!running) {
                    // Shutting down: a cycle that fails because resources were torn down mid-flight
                    // is expected teardown, not a fault. Exit quietly rather than logging at ERROR.
                    break;
                }
                cyclesFailed.increment("channel_id", channelIdLabel, "reason", classifySyncError(e));
                final var d = failState.onFailure(e);
                if (d.logFullDetail()) {
                    log.error("failure #{} fp={}: {}", e, d.consecutiveFailures(), d.fingerprint(), e.getMessage());
                } else {
                    log.error("failure #{} fp={} (repeat)", d.consecutiveFailures(), d.fingerprint());
                }
                try {
                    //noinspection BusyWait — periodic poll cadence;
                    Thread.sleep(d.backoffMs());
                } catch (final InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /**
     * Execute a single sync cycle.
     *
     * <p>Package-private for testing.
     */
    void executeSyncCycle() {
        cyclesTotal.increment();

        // 1. Read channel state. Empty means the chain has no Channel record for this id
        //    (PENDING commit-only phase, or never registered). Per spec §3.1.3 that is
        //    "no messaging, no syncing" — idle this cycle and re-check next tick.
        final var channelOpt = clprChannel
                .contractStateReader()
                .readChannelState(clprChannel.channelId(), commitmentLevel.toBlockTag());
        if (channelOpt.isEmpty()) {
            log.debug("no Channel record yet — idling cycle");
            return;
        }
        final var channel = channelOpt.get();
        final var status = channel.status();
        // Per-cycle steady-state diagnostic — kept at DEBUG so prod logs aren't flooded.
        log.debug(
                "SyncCycle status={} next={} acked={} recv={}",
                status,
                channel.nextMessageId(),
                channel.ackedMessageId(),
                channel.receivedMessageId());

        // 2. CLOSED → terminate the task; any other live status (ACTIVE / PAUSED / CLOSING /
        //    DRAINED) keeps syncing, because the spec's §4.2 / §4.5 auto-transitions out of those
        //    states are driven by continued bundle traffic — short-circuiting outbound sync on
        //    PAUSED/CLOSING/DRAINED produces symmetric stalemates (D-state-2/3/4). PENDING is not
        //    expected on a stored record (the chain never persists PENDING Channels), but if it
        //    ever appears we idle rather than race forward.
        switch (status) {
            case CLOSED -> {
                log.info("CLOSED, stopping");
                // TODO: move the stop logic to parent (ClprChannelHandler)
                // ClprChannelHandler should detect when the channel closes and when it should cease
                // all its operations (including ChannelSyncTask, but also others).
                running = false;
                return;
            }
            case PENDING -> {
                log.warn("unexpected PENDING status from chain — idling");
                return;
            }
            default -> {
                /* ACTIVE / PAUSED / CLOSING / DRAINED → fall through and keep syncing. */
            }
        }

        // 3. Select a peer to run the two-phase exchange with.
        var peerOpt = peerSelector.selectPeer();
        if (peerOpt.isEmpty()) {
            noPeerSkips.increment();
            log.debug("no peer available");
            return;
        }
        final var peer = peerOpt.get();
        final var serviceEndpoint = peer.serviceEndpoint();
        if (serviceEndpoint == null) {
            log.warn("Selected peer has no service endpoint: {}", peer);
            peerSelector.recordFailure(peer);
            return;
        }
        final String peerAddress = serviceEndpoint.ipAddress() + ":" + serviceEndpoint.port();
        log.debug("selected peer {}", peerAddress);

        // 4. Phase 1: send our request (what we want next, our status/trust anchor). The peer replies
        //    with a bundle answering it PLUS its own request; the phase-2 factory answers that request
        //    with a bundle built ad hoc via createBundle. The peer does not answer our phase-2 message.
        final var phase1 = ClprSyncPayload.newBuilder()
                .channelId(clprChannel.channelId())
                .bundleRequest(clprChannel.bundleRequestConstructor().createBundleRequest())
                .build();
        // Tracks whether we hand the stream a non-empty phase-2 bundle: if the exchange then fails, we
        // may have delivered messages the peer never acked back (a possible non-reciprocity — see catch).
        final AtomicBoolean sentBundle = new AtomicBoolean(false);
        final Function<ClprSyncPayload, ClprSyncPayload> followUp = peerReply -> {
            // Consume the peer's answer to OUR request the moment its phase-1 reply arrives — before we
            // build our own phase-2 answer — so a phase-2 build or transport failure never drops an
            // inbound bundle we already hold. (The old unary design had no coupling between our inbound
            // delivery and our outbound leg.) The codec replay-trims already-accepted messages, so a
            // stale message can't sink it as a replay; offer() hands it to the serial submitter, whose
            // outcome is metered inside AccountTransactionSubmitter. An empty payload is a no-op.
            if (peerReply.hasBundleResponse()) {
                final Bytes inbound = peerReply.bundleResponseOrThrow().bundlePayload();
                if (inbound.length() > 0) {
                    try {
                        clprChannel.inboundBundleQueue().offer(inbound, peer);
                    } catch (final RuntimeException e) {
                        // Decouple the two directions: offer() reaches the serial submitter, so a full queue
                        // or a submitter fault surfaces here. It must not stop us building and sending our own
                        // phase-2 bundle (nor fail the whole cycle) — log and carry on.
                        log.warn("failed to enqueue the peer's inbound bundle; continuing with phase 2", e);
                    }
                }
            }

            final var b = ClprSyncPayload.newBuilder().channelId(clprChannel.channelId());
            if (peerReply.hasBundleRequest()) {
                final var built = clprChannel
                        .bundleConstructor()
                        .createBundle(clprChannel.channelId(), peerReply.bundleRequestOrThrow());
                final Bytes outbound = built.bundlePayload();
                b.bundleResponse(
                        BundleResponse.newBuilder().bundlePayload(outbound).build());
                if (outbound.length() > 0) {
                    sentBundle.set(true);
                }
            }
            return b.build();
        };

        try {
            exchangesStarted.increment("role", "initiator", "channel_id", channelIdLabel);
            // followUp (above) already enqueued the peer's answer to our request when its phase-1 reply
            // arrived, so sync()'s return value is not needed here: the inbound delivery is done,
            // decoupled from whether our phase-2 leg completes.
            clprEndpointClient.sync(serviceEndpoint.ipAddress(), serviceEndpoint.port(), phase1, followUp);
            // The RPC completed, so the peer is reachable: credit its health now.
            peerSelector.recordSuccess(peer);
            peerSyncSuccess.increment("channel_id", channelIdLabel, "peer", Peers.peerLabel(peer));
        } catch (final Exception e) {
            if (!running) {
                // Shutting down: the peer client may already be closed or the RPC cancelled
                // mid-flight. Expected teardown — don't log it or record it as a peer failure.
                return;
            }
            cyclesFailed.increment("channel_id", channelIdLabel, "reason", classifySyncError(e));
            exchangesFailed.increment("role", "initiator", "channel_id", channelIdLabel);
            if (sentBundle.get()) {
                // We handed the peer a non-empty bundle before the exchange broke — it may hold our
                // messages while we never learned it did. Surfaced separately as a reciprocity signal.
                exchangesFailedAfterSend.increment(
                        "role", "initiator", "channel_id", channelIdLabel, "peer", Peers.peerLabel(peer));
            }
            peerSelector.recordFailure(peer);
            peerSyncErrors.increment("channel_id", channelIdLabel, "peer", Peers.peerLabel(peer));
            log.warn("Sync failed for channel {}", e, channelIdLabel);
        }
    }

    /**
     * Classifies a sync-cycle exception into a short reason label for the
     * {@code sync.cycles.failed} metric.
     *
     * <p>{@code clpr-relay-sync} does not depend on gRPC directly, so we identify
     * {@code StatusRuntimeException} and {@code StatusException} by class name rather
     * than by an {@code instanceof} check.
     */
    private static String classifySyncError(final Exception e) {
        final String cn = e.getClass().getName();
        if (cn.contains("StatusRuntimeException") || cn.contains("StatusException")) {
            return "grpc_error";
        }
        if (e instanceof IllegalArgumentException) {
            return "parse_error";
        }
        return "runtime_error";
    }

    /**
     * Stop the sync loop after the current cycle completes.
     * Running this method guarantees the loop exits; the interrupt only shortens the wait.
     */
    public void stop() {
        running = false;
        final var t = thread;
        if (t != null) {
            t.interrupt();
        }
    }

    /**
     * Returns {@code true} if the sync loop is still running.
     *
     * @return whether the task is running
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Returns the channel identifier managed by this task.
     *
     * @return the channel ID
     */
    public Bytes channelId() {
        return clprChannel.channelId();
    }

    /**
     * Returns this loop's failure state machine.
     *
     * @return the {@link FailState}
     */
    public FailState failState() {
        return failState;
    }
}
