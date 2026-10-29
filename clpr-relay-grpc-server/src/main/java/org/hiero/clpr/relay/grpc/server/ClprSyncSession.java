// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.grpc.server;

import com.hedera.hapi.node.state.clpr.BundleResponse;
import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.context.Context;
import com.swirlds.logging.api.Logger;
import com.swirlds.logging.api.Loggers;
import java.util.HexFormat;
import java.util.concurrent.Flow;
import org.hiero.clpr.relay.core.ChannelLookup;
import org.hiero.clpr.relay.core.metrics.LabeledCounter;
import org.jspecify.annotations.Nullable;

/**
 * Stateful handler for a single two-phase {@code sync} bidirectional-streaming session (responder
 * side). One instance is created per inbound stream. It drives a fixed <b>two-phase</b> exchange:
 *
 * <ol>
 *   <li><b>Phase 1</b> — the peer sends its {@link ClprSyncPayload} carrying a {@code bundle_request}
 *       (what it wants next). We build a bundle answering that request ({@code bundle_response}) and
 *       attach <em>our own</em> {@code bundle_request} (from our current chain state), sending exactly
 *       one response back.</li>
 *   <li><b>Phase 2</b> — the peer sends a follow-up carrying a {@code bundle_response} that answers our
 *       phase-1 request. We submit it to the local chain (only if it targets the same channel we
 *       authorized in phase 1), emit a terminal message (both fields absent), then complete the stream.</li>
 * </ol>
 *
 * <p>A terminal message received in phase 1 (both fields absent) closes the stream cleanly with no
 * exchange. Other deviations from the expected order (a message after the exchange completed, or the
 * client completing/erroring early) are logged and drive the session to a terminal state.
 *
 * <p><b>Thread-safety:</b> reactive-streams already serializes signals on a single subscriber, but the
 * transitions are {@code synchronized} so any future timeout handling on another thread sees a
 * consistent phase.
 */
public class ClprSyncSession implements Pipeline<ClprSyncPayload> {

    private static final Logger log = Loggers.getLogger(ClprSyncSession.class);

    private enum Phase {
        AWAITING_REQUEST,
        AWAITING_FOLLOW_UP,
        DONE
    }

    private final Pipeline<? super ClprSyncPayload> responses;
    private final ChannelLookup channelLookup;
    private final String instanceName;
    private final LabeledCounter exchangesStarted;
    private final LabeledCounter exchangesFailed;
    private final LabeledCounter exchangesFailedAfterSend;
    private final LabeledCounter syncErrors;

    private Phase phase = Phase.AWAITING_REQUEST;

    /** The phase-1 channel id label; {@code null} until the first request is seen. */
    @Nullable
    private String channelIdLabel;

    /**
     * The channel this session committed to serving in phase 1; {@code null} until then. The phase-2
     * follow-up must target the same channel — see {@link #handleFollowUp}.
     */
    @Nullable
    private Bytes phase1ChannelId;

    /** Whether we sent the peer a non-empty phase-1 bundle_response (for the non-reciprocity metric). */
    private boolean sentBundle = false;

    public ClprSyncSession(
            final Pipeline<? super ClprSyncPayload> responses,
            final ChannelLookup channelLookup,
            final String instanceName,
            final LabeledCounter exchangesStarted,
            final LabeledCounter exchangesFailed,
            final LabeledCounter exchangesFailedAfterSend,
            final LabeledCounter syncErrors) {
        this.responses = responses;
        this.channelLookup = channelLookup;
        this.instanceName = instanceName;
        this.exchangesStarted = exchangesStarted;
        this.exchangesFailed = exchangesFailed;
        this.exchangesFailedAfterSend = exchangesFailedAfterSend;
        this.syncErrors = syncErrors;
    }

    @Override
    public void onSubscribe(final Flow.Subscription subscription) {
        // PBJ's bidi-streaming pipeline drives delivery from its own source subscription (requesting
        // unbounded) and does not subscribe this handler, so onNext is invoked for every inbound message
        // without us requesting. Request unbounded defensively in case a future PBJ version does subscribe
        // us — never per-message back-pressure, which would strand a stream on a null subscription.
        subscription.request(Long.MAX_VALUE);
    }

    @Override
    public synchronized void onNext(final ClprSyncPayload message) {
        // A terminal message carries neither field. Per the sync protocol it closes the stream.
        final boolean isTerminal = !message.hasBundleRequest() && !message.hasBundleResponse();
        switch (phase) {
            case AWAITING_REQUEST -> {
                if (isTerminal) {
                    // The peer opened the stream but sent a terminal message instead of a request: there is
                    // no exchange to run. Recognize it and close cleanly rather than replying and stranding
                    // the stream in AWAITING_FOLLOW_UP waiting for a phase 2 that will never come.
                    log.debug("sync: peer sent a terminal message before any request; closing the stream");
                    phase = Phase.DONE;
                    responses.onComplete();
                    return;
                }
                responses.onNext(handleRequest(message));
                phase = Phase.AWAITING_FOLLOW_UP;
            }
            case AWAITING_FOLLOW_UP -> {
                // A terminal follow-up means the peer has no bundle for us; handleFollowUp already no-ops on
                // an absent bundle_response, so it falls through to the same completion (we still emit our
                // own terminal and complete the stream).
                handleFollowUp(message);
                phase = Phase.DONE;
                responses.onNext(ClprSyncPayload.newBuilder().build());
                responses.onComplete();
            }
            case DONE ->
                log.warn("sync: received an unexpected message after the two-phase exchange completed; ignoring");
        }
    }

    @Override
    public synchronized void onError(final Throwable throwable) {
        log.warn("sync: client stream errored during phase {}", throwable, phase);
        if (phase != Phase.DONE) {
            recordExchangeFailure();
        }
        phase = Phase.DONE;
    }

    @Override
    public synchronized void onComplete() {
        if (phase != Phase.DONE) {
            log.warn("sync: client completed the stream early during phase {}", phase);
            recordExchangeFailure();
        }
        phase = Phase.DONE;
    }

    /**
     * Count a failed inbound exchange — and, if we had already sent the peer a non-empty phase-1 bundle,
     * count it separately as a possible non-reciprocity (we may have delivered messages the peer never
     * acked back). No-op if no phase-1 request was ever received (no exchange was started to fail).
     */
    private void recordExchangeFailure() {
        if (channelIdLabel == null) {
            return;
        }
        exchangesFailed.increment("role", "responder", "channel_id", channelIdLabel);
        if (sentBundle) {
            exchangesFailedAfterSend.increment("role", "responder", "channel_id", channelIdLabel, "peer", "anonymous");
        }
    }

    /**
     * Phase 1: answer the peer's request with a bundle and attach our own request. If the peer sent no
     * request there is nothing to build a bundle from, so we reply with our request only.
     */
    private ClprSyncPayload handleRequest(final ClprSyncPayload request) {
        final Bytes channelId = request.channelId();

        final var clprChannelOpt = channelLookup.findById(channelId);
        if (clprChannelOpt.isEmpty()) {
            log.warn("sync: unknown channel; ignoring it");
            return ClprSyncPayload.newBuilder().channelId(channelId).build();
        }
        final var clprChannel = clprChannelOpt.get();

        final String channelIdLabel = HexFormat.of().formatHex(channelId.toByteArray());
        try (var _ = Context.getThreadLocalContext().add("relay", instanceName);
                var _ = Context.getThreadLocalContext().add("conn", channelIdLabel)) {
            this.channelIdLabel = channelIdLabel;
            exchangesStarted.increment("role", "responder", "channel_id", channelIdLabel);

            // Commit to this channel for the rest of the exchange: phase 2 must target the same one.
            this.phase1ChannelId = channelId;

            final var reply = ClprSyncPayload.newBuilder().channelId(channelId);
            if (request.hasBundleResponse()) {
                log.warn("sync: unexpected bundle_response in the phase-1 request; ignoring it");
            }
            if (request.hasBundleRequest()) {
                try {
                    final var built =
                            clprChannel.bundleConstructor().createBundle(channelId, request.bundleRequestOrThrow());
                    final Bytes payload = built.bundlePayload();
                    reply.bundleResponse(
                            BundleResponse.newBuilder().bundlePayload(payload).build());
                    if (payload.length() > 0) {
                        sentBundle = true;
                    }
                } catch (final Exception e) {
                    syncErrors.increment("channel_id", channelIdLabel, "reason", "build_error");
                    log.warn("sync: failed to build a bundle for the peer's request", e);
                }
            } else {
                log.warn("sync: phase-1 request carried no bundle_request; replying with our request only");
            }
            // Always attach our own request so the peer can serve us in phase 2. A failure building it
            // (e.g. an on-chain state read) must not abort the exchange: reply with what we have so the
            // initiator still receives our phase-1 bundle and does not count us as a failed peer.
            try {
                reply.bundleRequest(clprChannel.bundleRequestConstructor().createBundleRequest());
            } catch (final Exception e) {
                syncErrors.increment("channel_id", channelIdLabel, "reason", "build_error");
                log.warn("sync: failed to build our own bundle request; replying without it", e);
            }
            return reply.build();
        } catch (final Exception e) {
            syncErrors.increment("channel_id", channelIdLabel, "reason", "build_error");
            log.warn("sync: failed to prepare the phase-1 reply; replying with channel id only", e);
            return ClprSyncPayload.newBuilder().channelId(channelId).build();
        }
    }

    /**
     * Phase 2: the peer's follow-up answers our phase-1 request. Parse it, apply the per-channel
     * throttle, and submit it to the local chain.
     */
    private void handleFollowUp(final ClprSyncPayload followUp) {
        final Bytes channelId = followUp.channelId();

        // The phase-2 follow-up must target the same channel we served in phase 1: a peer that requested
        // channel A must not be able to submit a bundle against channel B in the same exchange. Reject any
        // mismatch (also covers a follow-up arriving when phase 1 never committed to a channel).
        if (phase1ChannelId == null || !phase1ChannelId.equals(channelId)) {
            log.warn(
                    "sync: phase-2 follow-up channel {} does not match the phase-1 channel; ignoring it",
                    HexFormat.of().formatHex(channelId.toByteArray()));
            return;
        }

        final var clprChannelOpt = channelLookup.findById(channelId);
        if (clprChannelOpt.isEmpty()) {
            log.warn("sync: unknown channel; ignoring it");
            return;
        }
        final var clprChannel = clprChannelOpt.get();

        final String channelIdLabel = HexFormat.of().formatHex(channelId.toByteArray());
        try (var _ = Context.getThreadLocalContext().add("relay", instanceName);
                var _ = Context.getThreadLocalContext().add("conn", channelIdLabel)) {
            if (!followUp.hasBundleResponse()) {
                return;
            }
            // The bundle_response is validated downstream rather than here: the per-channel codec
            // replay-trims anything at or below our received-message frontier, and the on-chain verifier
            // (reached via the submitter's eth_call preview) rejects any bundle whose proof or message range
            // does not line up — so a stale, replayed, or mismatched response is dropped, never advancing
            // state, without this session having to parse the opaque payload. The inbound transport does not
            // identify the dialer, so the source peer is unknown.
            clprChannel
                    .inboundBundleQueue()
                    .offer(followUp.bundleResponseOrThrow().bundlePayload(), null);
        } catch (final Exception e) {
            syncErrors.increment("channel_id", channelIdLabel, "reason", "submit_error");
            log.warn("sync: failed to enqueue the peer's phase-2 bundle; completing the exchange", e);
        }
    }
}
