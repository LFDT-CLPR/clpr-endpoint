// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.grpc.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.hapi.node.state.clpr.BundleResponse;
import com.hedera.hapi.node.state.clpr.ClprEndpoint;
import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.clpr.relay.core.BundleConstructor;
import org.hiero.clpr.relay.core.BundleWithConstructionMetadata;
import org.hiero.clpr.relay.core.InboundBundleQueue;
import org.hiero.clpr.relay.core.metrics.SimpleMetrics;
import org.hiero.clpr.relay.core.testfixtures.MockClprChannelOps;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ClprSyncSession} — the responder side of one two-phase {@code sync}
 * bidirectional-streaming call, driven with hand-rolled collaborators.
 *
 * <ul>
 *   <li>Phase 1: the peer's request draws exactly one response (a bundle answering it, plus our own
 *       request), leaving the stream open.</li>
 *   <li>Phase 2: the peer's follow-up (a bundle response) is submitted to the local chain; the session
 *       then emits a final empty payload and completes the stream.</li>
 * </ul>
 */
class ClprSyncSessionTest {

    private static final Bytes CHANNEL_ID = Bytes.wrap(new byte[] {1, 2, 3});

    /** Records everything the session writes to its outbound response stream. */
    private static final class RecordingResponses implements Pipeline<ClprSyncPayload> {
        final List<ClprSyncPayload> onNext = new ArrayList<>();
        final AtomicBoolean completed = new AtomicBoolean(false);
        Throwable error;

        @Override
        public void onSubscribe(final Flow.Subscription subscription) {}

        @Override
        public void onNext(final ClprSyncPayload item) {
            onNext.add(item);
        }

        @Override
        public void onError(final Throwable throwable) {
            error = throwable;
        }

        @Override
        public void onComplete() {
            completed.set(true);
        }
    }

    /** A {@link BundleConstructor} that records the requests it answered and returns a fixed bundle. */
    private static final class RecordingBundleConstructor implements BundleConstructor {
        final List<BundleRequest> requests = new ArrayList<>();
        final BundleWithConstructionMetadata response =
                new BundleWithConstructionMetadata(Bytes.wrap(new byte[] {0x0A}), 7L);

        @Override
        public BundleWithConstructionMetadata createBundle(final Bytes channelId, final BundleRequest request) {
            requests.add(request);
            return response;
        }
    }

    /** An {@link InboundBundleQueue} that records the raw bundle bytes offered for submission. */
    private static final class RecordingSubmitter implements InboundBundleQueue {
        final List<Bytes> offered = new ArrayList<>();

        @Override
        public void offer(final Bytes rawBundleBytes, final ClprEndpoint peer) {
            offered.add(rawBundleBytes);
        }
    }

    /**
     * Builds a session via the {@link ClprSyncHandler} factory (which wires the metrics counters and
     * resolves the channel through a {@link org.hiero.clpr.relay.core.ChannelLookup}), so the exact
     * production composition is exercised. The channel's collaborators are supplied by a
     * {@link MockClprChannelOps}: the parse/throttle/submit of an inbound bundle now lives behind
     * {@link org.hiero.clpr.relay.core.ClprChannelOps#inboundBundleQueue()}, so the {@code submitter}
     * receives the raw phase-2 payload directly.
     */
    private static ClprSyncSession session(
            final RecordingResponses responses,
            final BundleConstructor bundleConstructor,
            final InboundBundleQueue submitter) {
        final var ops = new MockClprChannelOps()
                .channelId(CHANNEL_ID)
                .bundleConstructor(bundleConstructor)
                .inboundBundleQueue(submitter);
        final var handler = new ClprSyncHandler(channelId -> Optional.of(ops), new SimpleMetrics(), "test");
        return handler.newSession(responses);
    }

    private static ClprSyncPayload requestPayload() {
        return ClprSyncPayload.newBuilder()
                .channelId(CHANNEL_ID)
                .bundleRequest(
                        BundleRequest.newBuilder().currentReceivedMessageId(3L).build())
                .build();
    }

    private static ClprSyncPayload followUpPayload() {
        return ClprSyncPayload.newBuilder()
                .channelId(CHANNEL_ID)
                .bundleResponse(BundleResponse.newBuilder()
                        .bundlePayload(Bytes.wrap(new byte[] {0x42}))
                        .build())
                .build();
    }

    @Test
    void onSubscribe_requestsUnbounded() {
        // PBJ's bidi pipeline never subscribes this handler and requests unbounded itself, so the
        // session must NOT do per-message back-pressure — it requests Long.MAX_VALUE defensively.
        final var session =
                session(new RecordingResponses(), new RecordingBundleConstructor(), new RecordingSubmitter());
        final var requested = new AtomicLong(0);
        session.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(final long n) {
                requested.set(n);
            }

            @Override
            public void cancel() {}
        });
        assertThat(requested.get()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void phase1_answersPeerRequestAndAttachesOwnRequest() {
        final var responses = new RecordingResponses();
        final var bundleConstructor = new RecordingBundleConstructor();
        final var session = session(responses, bundleConstructor, new RecordingSubmitter());

        session.onNext(requestPayload());

        // Exactly one reply, the stream stays open.
        assertThat(responses.onNext).hasSize(1);
        assertThat(responses.completed).isFalse();

        final ClprSyncPayload reply = responses.onNext.get(0);
        assertThat(reply.channelId()).isEqualTo(CHANNEL_ID);
        // We built a bundle answering the peer's request …
        assertThat(bundleConstructor.requests).hasSize(1);
        assertThat(reply.hasBundleResponse()).isTrue();
        // The session assembles the wire BundleResponse from the constructor result's payload.
        assertThat(reply.bundleResponseOrThrow().bundlePayload()).isEqualTo(bundleConstructor.response.bundlePayload());
        // … and attached our own request so the peer can serve us in phase 2.
        assertThat(reply.hasBundleRequest()).isTrue();
    }

    @Test
    void phase2_submitsPeerResponseThenCompletes() {
        final var responses = new RecordingResponses();
        final var submitter = new RecordingSubmitter();
        final var session = session(responses, new RecordingBundleConstructor(), submitter);

        session.onNext(requestPayload()); // phase 1
        session.onNext(followUpPayload()); // phase 2

        // The follow-up bundle is submitted to the local chain, then a final empty payload is emitted to
        // release the initiator's blocking read before the stream completes.
        assertThat(submitter.offered).hasSize(1);
        assertThat(responses.onNext).hasSize(2);
        assertThat(responses.onNext.get(1).hasBundleResponse()).isFalse();
        assertThat(responses.onNext.get(1).hasBundleRequest()).isFalse();
        assertThat(responses.completed).isTrue();
    }

    @Test
    void phase2_withMismatchedChannelId_isNotSubmitted() {
        final var responses = new RecordingResponses();
        final var submitter = new RecordingSubmitter();
        final var session = session(responses, new RecordingBundleConstructor(), submitter);

        session.onNext(requestPayload()); // phase 1 for CHANNEL_ID
        // A phase-2 follow-up claiming a different channel must be rejected: the per-channel authorization ran
        // in phase 1 for CHANNEL_ID only, and phase 2 does not re-run it.
        session.onNext(ClprSyncPayload.newBuilder()
                .channelId(Bytes.wrap(new byte[] {9, 9, 9}))
                .bundleResponse(BundleResponse.newBuilder()
                        .bundlePayload(Bytes.wrap(new byte[] {0x42}))
                        .build())
                .build());

        // Nothing submitted; the stream still finishes (phase-1 reply + terminal), just without enqueueing.
        assertThat(submitter.offered).isEmpty();
        assertThat(responses.onNext).hasSize(2);
        assertThat(responses.completed).isTrue();
    }

    @Test
    void terminalMessageInPhase1_completesWithoutReply() {
        final var responses = new RecordingResponses();
        final var submitter = new RecordingSubmitter();
        final var session = session(responses, new RecordingBundleConstructor(), submitter);

        // A terminal message (both fields absent) received before any request closes the stream cleanly.
        session.onNext(ClprSyncPayload.newBuilder().build());

        assertThat(responses.onNext).isEmpty();
        assertThat(submitter.offered).isEmpty();
        assertThat(responses.completed).isTrue();
    }

    @Test
    void extraMessageAfterCompletion_isIgnored() {
        final var responses = new RecordingResponses();
        final var submitter = new RecordingSubmitter();
        final var session = session(responses, new RecordingBundleConstructor(), submitter);

        session.onNext(requestPayload()); // phase 1
        session.onNext(followUpPayload()); // phase 2 -> DONE
        session.onNext(followUpPayload()); // extra, must be ignored

        // Phase 1 reply + the final empty terminator = 2; the extra message adds nothing.
        assertThat(responses.onNext).hasSize(2);
        assertThat(submitter.offered).hasSize(1);
        assertThat(responses.completed).isTrue();
    }

    @Test
    void clientCompletesEarly_doesNotEmitResponse() {
        final var responses = new RecordingResponses();
        final var session = session(responses, new RecordingBundleConstructor(), new RecordingSubmitter());

        session.onComplete(); // client half-closes before sending anything

        assertThat(responses.onNext).isEmpty();
        assertThat(responses.completed).isFalse();
    }
}
