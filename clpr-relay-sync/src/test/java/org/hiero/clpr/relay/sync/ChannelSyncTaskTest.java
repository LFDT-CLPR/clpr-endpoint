// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.sync;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.hapi.node.state.clpr.BundleResponse;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprEndpoint;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.hapi.node.state.clpr.ClprQueueMetadata;
import com.hedera.hapi.node.state.clpr.ClprServiceEndpoint;
import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.Metrics;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.hiero.clpr.relay.core.BundleConstructor;
import org.hiero.clpr.relay.core.BundlePayloadCodec;
import org.hiero.clpr.relay.core.BundleWithConstructionMetadata;
import org.hiero.clpr.relay.core.ClprChannelOps;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.InboundBundleQueue;
import org.hiero.clpr.relay.core.ParsedBundle;
import org.hiero.clpr.relay.core.PeerEndpointCache;
import org.hiero.clpr.relay.core.metrics.MetricLabels;
import org.hiero.clpr.relay.core.metrics.SimpleMetrics;
import org.hiero.clpr.relay.core.testfixtures.MockClprChannelOps;
import org.hiero.clpr.relay.grpc.client.ClprEndpointClient;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class ChannelSyncTaskTest {

    private static final Bytes CHANNEL_ID = Bytes.wrap(new byte[] {1, 2, 3});
    public static final long TEST_SYNC_INTERVAL_MS = 1000L;

    // -------------------------------------------------------------------------
    // Minimal stub implementations (no Mockito needed — avoids JPMS friction)
    // -------------------------------------------------------------------------

    /** A ContractStateReader whose returned channel can be configured per test. */
    static class StubStateReader implements ContractStateReader {
        ClprChannel channel;

        StubStateReader(ClprChannel channel) {
            this.channel = channel;
        }

        @NonNull
        @Override
        public Optional<ClprChannel> readChannelState(@NonNull Bytes channelId, @NonNull String blockTag) {
            return Optional.ofNullable(channel);
        }

        @NonNull
        @Override
        public List<QueuedMessage> readQueuedMessages(
                @NonNull Bytes channelId, long fromId, long toId, @NonNull String blockTag) {
            return List.of();
        }

        @NonNull
        @Override
        public ClprLedgerConfiguration readLedgerConfiguration(@NonNull CommitmentLevel level) {
            return ClprLedgerConfiguration.DEFAULT;
        }
    }

    /** A BundleConstructor that returns an empty (no-proof) response and is never expected to be consulted. */
    static class EmptyBundleConstructor implements BundleConstructor {
        @NonNull
        @Override
        public BundleWithConstructionMetadata createBundle(@NonNull Bytes channelId, @NonNull BundleRequest request) {
            return new BundleWithConstructionMetadata(Bytes.EMPTY, 0L);
        }
    }

    /** A BundleConstructor that records whether {@link #createBundle} was consulted and returns a fixed bundle. */
    static class RecordingBundleConstructor implements BundleConstructor {
        volatile boolean createCalled = false;

        @NonNull
        @Override
        public BundleWithConstructionMetadata createBundle(@NonNull Bytes channelId, @NonNull BundleRequest request) {
            createCalled = true;
            return new BundleWithConstructionMetadata(Bytes.wrap(new byte[] {0x01}), 0L);
        }
    }

    /** A BundleConstructor whose {@code createBundle} always throws — simulates a phase-2 build failure. */
    static class ThrowingBundleConstructor implements BundleConstructor {
        @NonNull
        @Override
        public BundleWithConstructionMetadata createBundle(@NonNull Bytes channelId, @NonNull BundleRequest request) {
            throw new RuntimeException("simulated phase-2 build failure");
        }
    }

    static class NoOpCodec implements BundlePayloadCodec {
        @NonNull
        @Override
        public ParsedBundle decodeBundle(@NonNull Bytes p) {
            throw new UnsupportedOperationException();
        }

        @NonNull
        @Override
        public ParsedBundle parseBundle(@NonNull Bytes p, long receivedMessageId) {
            throw new UnsupportedOperationException();
        }
    }

    /** Returns an empty-message ParsedBundle carrying the raw bytes. */
    static class StubCodec implements BundlePayloadCodec {
        @NonNull
        @Override
        public ParsedBundle decodeBundle(@NonNull Bytes p) {
            return new ParsedBundle(ClprQueueMetadata.DEFAULT, List.of(), p);
        }

        @NonNull
        @Override
        public ParsedBundle parseBundle(@NonNull Bytes p, long receivedMessageId) {
            return decodeBundle(p);
        }
    }

    static class NoOpSubmitter implements InboundBundleQueue {
        boolean called = false;

        @Override
        public void offer(@NonNull Bytes rawBundleBytes, ClprEndpoint peer) {
            called = true;
        }
    }

    static class EmptyPeerSelector extends PeerSelector {
        EmptyPeerSelector() {
            super(new PeerEndpointCache());
        }
    }

    /** A PeerSelector that returns a peer with a real service endpoint (needed for sync call). */
    static class SinglePeerSelectorWithEndpoint extends PeerSelector {
        SinglePeerSelectorWithEndpoint() {
            super(makeCache());
        }

        private static PeerEndpointCache makeCache() {
            var cache = new PeerEndpointCache();
            var endpoint = new ClprServiceEndpoint("127.0.0.1", 19999);
            cache.replaceAll(List.of(new ClprEndpoint(endpoint, Bytes.EMPTY, Bytes.wrap(new byte[] {9}))));
            return cache;
        }
    }

    // -------------------------------------------------------------------------
    // Endpoint-client stubs (two-phase sync: sync(host, port, request, followUpFactory))
    // -------------------------------------------------------------------------

    private static ClprEndpointClient stubPayloadClprEndpointClient(
            Function<ClprSyncPayload, ClprSyncPayload> provider) {
        return new ClprEndpointClient(1_048_576) {
            @Override
            public ClprSyncPayload sync(
                    final String host,
                    final int port,
                    final ClprSyncPayload request,
                    final Function<ClprSyncPayload, ClprSyncPayload> followUpFactory) {
                final ClprSyncPayload reply = provider.apply(request);
                // Mirror the real client: the peer's phase-1 reply drives the follow-up factory, which is
                // where the task consumes (enqueues) the inbound bundle before building its phase-2 answer.
                followUpFactory.apply(reply);
                return reply;
            }
        };
    }

    /** Peer that answers phase 1 with nothing to submit. */
    private static final ClprEndpointClient NO_OP_CLPR_ENDPOINT_CLIENT =
            stubPayloadClprEndpointClient(_ -> ClprSyncPayload.newBuilder().build());

    /** Peer that answers phase 1 with a non-empty bundle_response, so the task submits it. */
    private static final ClprEndpointClient NON_EMPTY_CLPR_ENDPOINT_CLIENT =
            stubPayloadClprEndpointClient(_ -> ClprSyncPayload.newBuilder()
                    .bundleResponse(BundleResponse.newBuilder()
                            .bundlePayload(Bytes.wrap(new byte[] {0x42}))
                            .build())
                    .build());

    private static final long TEST_BACKOFF_BASE_MS = 1_000L;
    private static final long TEST_BACKOFF_CAP_MS = 30_000L;

    /**
     * A {@link ClprChannelOps} wrapping the given per-channel collaborators. The inbound codec is no
     * longer a {@code ChannelSyncTask} collaborator — inbound bundles flow through
     * {@link ClprChannelOps#inboundBundleQueue()} — so {@code codec} is ignored by the callers.
     */
    private static ClprChannelOps ops(
            final ContractStateReader reader, final BundleConstructor proof, final InboundBundleQueue submitter) {
        return new MockClprChannelOps()
                .channelId(CHANNEL_ID)
                .contractStateReader(reader)
                .bundleConstructor(proof)
                .inboundBundleQueue(submitter);
    }

    /** Build a task with the given metrics and the default test backoff policy. */
    private ChannelSyncTask task(
            ContractStateReader reader,
            BundleConstructor proof,
            BundlePayloadCodec codec,
            InboundBundleQueue submitter,
            PeerSelector peer,
            ClprEndpointClient clprEndpointClient,
            Metrics metrics) {
        return new ChannelSyncTask(
                ops(reader, proof, submitter),
                CommitmentLevel.LATEST,
                peer,
                clprEndpointClient,
                metrics,
                TEST_BACKOFF_BASE_MS,
                TEST_BACKOFF_CAP_MS,
                TEST_SYNC_INTERVAL_MS);
    }

    /** Same as {@link #task} with a throwaway metrics registry. */
    private ChannelSyncTask task(
            ContractStateReader reader,
            BundleConstructor proof,
            BundlePayloadCodec codec,
            InboundBundleQueue submitter,
            PeerSelector peer,
            ClprEndpointClient clprEndpointClient) {
        return task(reader, proof, codec, submitter, peer, clprEndpointClient, new SimpleMetrics());
    }

    private ChannelSyncTask makeTask(ContractStateReader reader, BundleConstructor proof, PeerSelector peer) {
        return task(reader, proof, new NoOpCodec(), new NoOpSubmitter(), peer, NO_OP_CLPR_ENDPOINT_CLIENT);
    }

    @Test
    void stopsOnClosedChannel() {
        var channel = ClprChannel.newBuilder()
                .status(ClprChannelStatus.CLOSED)
                .nextMessageId(1L)
                .ackedMessageId(0L)
                .build();
        var task = makeTask(new StubStateReader(channel), new EmptyBundleConstructor(), new EmptyPeerSelector());

        task.executeSyncCycle();

        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void initiatesSyncWhenOutboundQueueEmpty() {
        // nextMessageId == ackedMessageId → no outbound messages, but the two-phase cycle must still
        // initiate a sync (phase-1 request) so the peer can serve us. Assert the RPC IS dispatched.
        var channel = ClprChannel.newBuilder()
                .status(ClprChannelStatus.ACTIVE)
                .nextMessageId(5L)
                .ackedMessageId(5L)
                .build();
        var recording = new RecordingClprEndpointClient();
        var task = task(
                new StubStateReader(channel),
                new EmptyBundleConstructor(),
                new NoOpCodec(),
                new NoOpSubmitter(),
                new SinglePeerSelectorWithEndpoint(),
                recording);

        task.executeSyncCycle();

        assertThat(recording.callCount).isEqualTo(1);
        assertThat(recording.capturedRequest.hasBundleRequest()).isTrue();
        assertThat(task.isRunning()).isTrue();
    }

    @Test
    void submitsWhenPeerReturnsBundle() {
        // receivedMessageId=3, the peer answers phase 1 with a non-empty bundle_response → submit it.
        var channel = ClprChannel.newBuilder()
                .status(ClprChannelStatus.ACTIVE)
                .nextMessageId(5L)
                .ackedMessageId(3L)
                .receivedMessageId(3L)
                .build();
        var submitter = new NoOpSubmitter();
        var task = task(
                new StubStateReader(channel),
                new EmptyBundleConstructor(),
                new StubCodec(),
                submitter,
                new SinglePeerSelectorWithEndpoint(),
                NON_EMPTY_CLPR_ENDPOINT_CLIENT);

        task.executeSyncCycle();

        assertThat(submitter.called).isTrue();
        assertThat(task.isRunning()).isTrue();
    }

    @Test
    void submitsInboundReplyEvenWhenPhase2BuildFails() {
        // Decoupling regression: the peer's phase-1 reply answers OUR request, so we enqueue it the
        // moment it arrives — even if building our own phase-2 answer then throws (e.g. a local RPC blip
        // in createBundle). Otherwise a phase-2 failure would drop an inbound bundle we already hold.
        var channel = ClprChannel.newBuilder()
                .status(ClprChannelStatus.ACTIVE)
                .nextMessageId(5L)
                .ackedMessageId(3L)
                .receivedMessageId(3L)
                .build();
        var submitter = new NoOpSubmitter();
        // The reply carries both a bundle_response (answers our request) and a bundle_request (which our
        // phase-2 leg must answer) — and the constructor throws while building that phase-2 answer.
        var recording = new RecordingClprEndpointClient();
        recording.peerReplyForFactory = ClprSyncPayload.newBuilder()
                .channelId(CHANNEL_ID)
                .bundleResponse(BundleResponse.newBuilder()
                        .bundlePayload(Bytes.wrap(new byte[] {0x42}))
                        .build())
                .bundleRequest(
                        BundleRequest.newBuilder().currentReceivedMessageId(0L).build())
                .build();
        var task = task(
                new StubStateReader(channel),
                new ThrowingBundleConstructor(),
                new StubCodec(),
                submitter,
                new SinglePeerSelectorWithEndpoint(),
                recording);

        // The cycle catches the phase-2 build failure internally (recorded as a peer failure), so it does
        // not throw — but the inbound bundle was enqueued before that build was attempted.
        task.executeSyncCycle();

        assertThat(submitter.called)
                .as("the peer's answer to our request must be enqueued even when our phase-2 build fails")
                .isTrue();
        assertThat(task.isRunning()).isTrue();
    }

    @Test
    void phase2StillBuiltWhenInboundOfferThrows() {
        // Reverse decoupling (review #20): if enqueueing the peer's inbound bundle throws — offer() reaches
        // the serial submitter, so a full queue or submitter fault surfaces here — we must still build and
        // send our own phase-2 bundle, and the cycle must not fail.
        var channel = ClprChannel.newBuilder()
                .status(ClprChannelStatus.ACTIVE)
                .nextMessageId(5L)
                .ackedMessageId(3L)
                .receivedMessageId(3L)
                .build();
        final InboundBundleQueue throwingSubmitter = (bytes, peer) -> {
            throw new RuntimeException("submit queue full");
        };
        var proof = new RecordingBundleConstructor();
        // The reply answers our request (bundle_response → triggers the throwing offer) and carries the
        // peer's own request (bundle_request → our phase-2 leg must still answer it).
        var recording = new RecordingClprEndpointClient();
        recording.peerReplyForFactory = ClprSyncPayload.newBuilder()
                .channelId(CHANNEL_ID)
                .bundleResponse(BundleResponse.newBuilder()
                        .bundlePayload(Bytes.wrap(new byte[] {0x42}))
                        .build())
                .bundleRequest(
                        BundleRequest.newBuilder().currentReceivedMessageId(0L).build())
                .build();
        var task = task(
                new StubStateReader(channel),
                proof,
                new StubCodec(),
                throwingSubmitter,
                new SinglePeerSelectorWithEndpoint(),
                recording);

        task.executeSyncCycle();

        assertThat(proof.createCalled)
                .as("our phase-2 bundle must still be built when the inbound offer throws")
                .isTrue();
        assertThat(recording.followUpResult).isNotNull();
        assertThat(recording.followUpResult.hasBundleResponse()).isTrue();
        assertThat(task.isRunning()).isTrue();
    }

    @Test
    void buildsBundleForPeerRequestInFollowUp() {
        // The peer's phase-1 reply carries its own bundle_request; the phase-2 follow-up must answer it
        // by building a bundle ad hoc via createBundle.
        var channel = ClprChannel.newBuilder()
                .status(ClprChannelStatus.ACTIVE)
                .nextMessageId(5L)
                .ackedMessageId(3L)
                .receivedMessageId(3L)
                .build();
        var proof = new RecordingBundleConstructor();
        var recording = new RecordingClprEndpointClient();
        recording.peerReplyForFactory = ClprSyncPayload.newBuilder()
                .channelId(CHANNEL_ID)
                .bundleRequest(
                        BundleRequest.newBuilder().currentReceivedMessageId(0L).build())
                .build();
        var task = task(
                new StubStateReader(channel),
                proof,
                new StubCodec(),
                new NoOpSubmitter(),
                new SinglePeerSelectorWithEndpoint(),
                recording);

        task.executeSyncCycle();

        assertThat(proof.createCalled).isTrue();
        assertThat(recording.followUpResult).isNotNull();
        assertThat(recording.followUpResult.hasBundleResponse()).isTrue();
    }

    @Test
    void failedAfterSend_countsNonReciprocityWhenExchangeBreaksAfterWeSentABundle() {
        // Non-reciprocity signal: we build+send a non-empty phase-2 bundle to the peer, then the stream
        // breaks. We may have delivered messages the peer never acked back → sync.exchanges.failed_after_send.
        var channel = ClprChannel.newBuilder()
                .status(ClprChannelStatus.ACTIVE)
                .nextMessageId(5L)
                .ackedMessageId(3L)
                .receivedMessageId(3L)
                .build();
        // Peer asks us for messages → followUp builds a non-empty bundle (RecordingBundleConstructor
        // returns {0x01}); the client then throws AFTER driving the factory (exchange breaks post-send).
        var recording = new RecordingClprEndpointClient();
        recording.peerReplyForFactory = ClprSyncPayload.newBuilder()
                .channelId(CHANNEL_ID)
                .bundleRequest(
                        BundleRequest.newBuilder().currentReceivedMessageId(0L).build())
                .build();
        recording.throwAfterFollowUp = true;
        var metrics = new SimpleMetrics();
        var task = task(
                new StubStateReader(channel),
                new RecordingBundleConstructor(),
                new StubCodec(),
                new NoOpSubmitter(),
                new SinglePeerSelectorWithEndpoint(),
                recording,
                metrics);

        task.executeSyncCycle();

        final String channelIdLabel = HexFormat.of().formatHex(CHANNEL_ID.toByteArray());
        // SinglePeerSelectorWithEndpoint's peer is labelled by its service endpoint.
        final var failedAfterSend = (Counter) metrics.getMetric(
                "sync",
                MetricLabels.labeled(
                        "exchanges.failed_after_send",
                        Map.of("role", "initiator", "channel_id", channelIdLabel, "peer", "127.0.0.1:19999")));
        assertThat(failedAfterSend).isNotNull();
        assertThat(failedAfterSend.get()).isEqualTo(1L);

        final var failed = (Counter) metrics.getMetric(
                "sync",
                MetricLabels.labeled("exchanges.failed", Map.of("role", "initiator", "channel_id", channelIdLabel)));
        assertThat(failed).isNotNull();
        assertThat(failed.get()).isEqualTo(1L);
    }

    @Test
    void skipsWhenNoPeersAvailable() {
        var channel = ClprChannel.newBuilder()
                .status(ClprChannelStatus.ACTIVE)
                .nextMessageId(5L)
                .ackedMessageId(2L)
                .build();
        var submitter = new NoOpSubmitter();
        var task = task(
                new StubStateReader(channel),
                new EmptyBundleConstructor(),
                new NoOpCodec(),
                submitter,
                new EmptyPeerSelector(),
                NO_OP_CLPR_ENDPOINT_CLIENT); // no peers

        task.executeSyncCycle();

        assertThat(submitter.called).isFalse();
        assertThat(task.isRunning()).isTrue();
    }

    @Test
    void executeSyncCycle_syncsWhenPaused() {
        // Spec §3.1.3 / §4.5: PAUSED keeps syncing — auto-resume needs the peer's next
        // correctly-ordered bundle, which requires both sides to stay reachable.
        assertSyncsForStatus(ClprChannelStatus.PAUSED);
    }

    @Test
    void executeSyncCycle_syncsWhenClosing() {
        // Spec §3.1.3 / §4.2 step 5b: CLOSING keeps shipping bundles so the peer can drain
        // acks and the chain can auto-transition to DRAINED.
        assertSyncsForStatus(ClprChannelStatus.CLOSING);
    }

    @Test
    void executeSyncCycle_syncsWhenDrained() {
        // Spec §3.1.3 / §4.2 step 5a: DRAINED still ships bundles so the peer's DRAINED
        // metadata can drive the auto-transition to CLOSED.
        assertSyncsForStatus(ClprChannelStatus.DRAINED);
    }

    private void assertSyncsForStatus(final ClprChannelStatus status) {
        var channel = ClprChannel.newBuilder()
                .status(status)
                .nextMessageId(5L)
                .ackedMessageId(2L)
                .build();
        var submitter = new NoOpSubmitter();
        var recording = new RecordingClprEndpointClient();
        var task = task(
                new StubStateReader(channel),
                new EmptyBundleConstructor(),
                new NoOpCodec(),
                submitter,
                new SinglePeerSelectorWithEndpoint(),
                recording);

        task.executeSyncCycle();

        assertThat(recording.callCount).isEqualTo(1);
        // Empty response bundle ⇒ nothing to submit on chain.
        assertThat(submitter.called).isFalse();
        assertThat(task.isRunning()).isTrue();
    }

    @Test
    void executeSyncCycle_idlesWhenNoChannelRecord() {
        // Empty Optional ⇒ no Channel record on chain (PENDING or unregistered). Spec
        // §3.1.3: "no messaging, no syncing." The task must idle without an outbound RPC.
        var submitter = new NoOpSubmitter();
        var recording = new RecordingClprEndpointClient();
        var task = task(
                new StubStateReader(null), // null => Optional.empty()
                new EmptyBundleConstructor(),
                new NoOpCodec(),
                submitter,
                new SinglePeerSelectorWithEndpoint(),
                recording);

        task.executeSyncCycle();

        assertThat(recording.callCount).isEqualTo(0);
        assertThat(submitter.called).isFalse();
        assertThat(task.isRunning()).isTrue();
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Test
    void channelIdIsPreserved() {
        var task = makeTask(
                new StubStateReader(ClprChannel.DEFAULT), new EmptyBundleConstructor(), new EmptyPeerSelector());
        assertThat(task.channelId()).isEqualTo(CHANNEL_ID);
    }

    @Test
    void isRunningByDefault() {
        var task = makeTask(
                new StubStateReader(ClprChannel.DEFAULT), new EmptyBundleConstructor(), new EmptyPeerSelector());
        assertThat(task.isRunning()).isTrue();
    }

    @Test
    void stopSetsRunningFalse() {
        var task = makeTask(
                new StubStateReader(ClprChannel.DEFAULT), new EmptyBundleConstructor(), new EmptyPeerSelector());
        task.stop();
        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void swallowsSyncFailureAfterStop() {
        // Once stopped, a peer RPC that fails because its channel was closed mid-shutdown is
        // expected teardown: the cycle must neither propagate nor count it as a failure.
        var channel = ClprChannel.newBuilder()
                .status(ClprChannelStatus.ACTIVE)
                .nextMessageId(5L)
                .ackedMessageId(3L)
                .receivedMessageId(3L)
                .build();
        final var closedClient = new ClprEndpointClient(1_048_576) {
            @Override
            public ClprSyncPayload sync(
                    final String host,
                    final int port,
                    final ClprSyncPayload request,
                    final Function<ClprSyncPayload, ClprSyncPayload> followUpFactory) {
                throw new IllegalStateException("ClprEndpointClient is closed");
            }
        };
        var metrics = new SimpleMetrics();
        var task = task(
                new StubStateReader(channel),
                new EmptyBundleConstructor(),
                new StubCodec(),
                new NoOpSubmitter(),
                new SinglePeerSelectorWithEndpoint(),
                closedClient,
                metrics);

        task.stop();

        task.executeSyncCycle();

        // Shutdown-induced failures are not recorded — the counter is never registered.
        final String channelIdLabel = HexFormat.of().formatHex(CHANNEL_ID.toByteArray());
        var cyclesFailed = (Counter) metrics.getMetric(
                "sync",
                MetricLabels.labeled("cycles.failed", Map.of("channel_id", channelIdLabel, "reason", "runtime_error")));
        assertThat(cyclesFailed).isNull();
    }

    /** ClprEndpointClient that records the last two-phase call's arguments. */
    static class RecordingClprEndpointClient extends ClprEndpointClient {
        RecordingClprEndpointClient() {
            super(1_048_576);
        }

        String host;
        int port;
        Bytes channelId;
        int callCount;
        ClprSyncPayload capturedRequest;

        /** When non-null, {@link #sync} invokes the follow-up factory with this simulated peer reply. */
        ClprSyncPayload peerReplyForFactory;

        ClprSyncPayload followUpResult;
        ClprSyncPayload phase1Response = ClprSyncPayload.newBuilder().build();

        /** When true, {@link #sync} throws after driving the follow-up factory — the exchange breaks
         * <em>after</em> we have built/sent our phase-2 bundle. */
        boolean throwAfterFollowUp = false;

        @NonNull
        @Override
        public ClprSyncPayload sync(
                @NonNull String h,
                int p,
                @NonNull ClprSyncPayload request,
                @NonNull Function<ClprSyncPayload, ClprSyncPayload> followUpFactory) {
            host = h;
            port = p;
            channelId = request.channelId();
            capturedRequest = request;
            callCount++;
            if (peerReplyForFactory != null) {
                followUpResult = followUpFactory.apply(peerReplyForFactory);
            }
            if (throwAfterFollowUp) {
                throw new RuntimeException("stream failed after phase-2 send");
            }
            return phase1Response;
        }
    }

    @Test
    void dispatchesSyncWhenPeerHasServiceEndpoint() {
        var channel = ClprChannel.newBuilder()
                .status(ClprChannelStatus.ACTIVE)
                .nextMessageId(5L)
                .ackedMessageId(2L)
                .build();
        var endpoint = ClprEndpoint.newBuilder()
                .serviceEndpoint(new ClprServiceEndpoint("10.0.0.1", 9000))
                .accountId(Bytes.wrap(new byte[] {7}))
                .build();
        var cache = new PeerEndpointCache();
        cache.replaceAll(List.of(endpoint));
        var peerSelector = new PeerSelector(cache);

        var recording = new RecordingClprEndpointClient();
        var task = task(
                new StubStateReader(channel),
                new EmptyBundleConstructor(),
                new NoOpCodec(),
                new NoOpSubmitter(),
                peerSelector,
                recording);

        task.executeSyncCycle();

        assertThat(recording.callCount).isEqualTo(1);
        assertThat(recording.host).isEqualTo("10.0.0.1");
        assertThat(recording.port).isEqualTo(9000);
        assertThat(recording.channelId).isEqualTo(CHANNEL_ID);
        // The phase-1 payload carries our own bundle_request (what we want next), not a bundle payload.
        assertThat(recording.capturedRequest.hasBundleRequest()).isTrue();
    }

    // -------------------------------------------------------------------------
    // Resilience tests: the run() loop must survive unchecked exceptions, back off
    // with growing bounded delays, reset on the first clean cycle, and exit promptly
    // when interrupted mid-backoff.
    // -------------------------------------------------------------------------

    /** A state reader driven by a scripted list of per-call actions (last action repeats). */
    static final class ScriptedStateReader implements ContractStateReader {
        enum Action {
            THROW,
            OK
        }

        private final List<Action> script;
        private final ClprChannel okChannel;
        private final AtomicInteger idx = new AtomicInteger(0);

        ScriptedStateReader(final List<Action> script, final ClprChannel okChannel) {
            this.script = script;
            this.okChannel = okChannel;
        }

        @NonNull
        @Override
        public Optional<ClprChannel> readChannelState(@NonNull Bytes channelId, @NonNull String blockTag) {
            final int i = idx.getAndIncrement();
            final Action a = i < script.size() ? script.get(i) : script.get(script.size() - 1);
            if (a == Action.THROW) {
                // Same throw site every call → stable fingerprint, mirroring a real persistent fault.
                throw new IllegalStateException("simulated RPC failure");
            }
            return Optional.of(okChannel);
        }

        @NonNull
        @Override
        public List<QueuedMessage> readQueuedMessages(
                @NonNull Bytes channelId, long fromId, long toId, @NonNull String blockTag) {
            return List.of();
        }

        @NonNull
        @Override
        public ClprLedgerConfiguration readLedgerConfiguration(@NonNull CommitmentLevel level) {
            return ClprLedgerConfiguration.DEFAULT;
        }
    }

    private static ChannelSyncTask resilientTask(
            final ContractStateReader reader,
            final SimpleMetrics metrics,
            final long backoffBaseMs,
            final long backoffCapMs) {
        return new ChannelSyncTask(
                ops(reader, new EmptyBundleConstructor(), new NoOpSubmitter()),
                CommitmentLevel.LATEST,
                new EmptyPeerSelector(),
                NO_OP_CLPR_ENDPOINT_CLIENT,
                metrics,
                backoffBaseMs,
                backoffCapMs,
                TEST_SYNC_INTERVAL_MS);
    }

    private static ClprChannel activeNoOutbound() {
        return ClprChannel.newBuilder()
                .status(ClprChannelStatus.ACTIVE)
                .nextMessageId(5L)
                .ackedMessageId(5L)
                .build();
    }

    private static void awaitUntil(final long timeoutMs, final BooleanSupplier cond) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("condition not met within " + timeoutMs + " ms");
    }

    // Each per-cycle collaborator that runs BEFORE the peer RPC is trapped by the channel-level
    // FailState (not the peer-scoped inner catch). When one of them throws, the loop must stay alive,
    // advance the task's FailState streak, and clear on the first clean cycle. The task is launched on
    // a real virtual thread so run()'s failure handling is exercised end-to-end. (The aggregate
    // sync.channels.failing gauge that reads this streak is owned by RelayInstance, tested there.)

    /** While {@code active}, the targeted collaborator throws; cleared to let the loop recover. */
    private static final class Fault {
        volatile boolean active = true;
    }

    /** The per-cycle collaborator whose unchecked exception {@code FailState} must trap. */
    private enum FailingStep {
        READ_STATE,
        SELECT_PEER
    }

    @ParameterizedTest
    @EnumSource(FailingStep.class)
    void run_trapsAndRecoversFromEachPreRpcFailure(final FailingStep step) throws Exception {
        final var metrics = new SimpleMetrics();
        final var fault = new Fault();
        final var task = taskFailingAt(step, fault, metrics);
        final var thread = Thread.startVirtualThread(task::run); // real run() on a virtual thread
        try {
            // The step's exception propagates to run()'s FailState catch → the streak rises.
            awaitUntil(2_000L, () -> task.failState().consecutiveFailures() >= 1);
            assertThat(cyclesFailed(metrics)).isGreaterThanOrEqualTo(1L);

            // The first clean cycle clears the streak.
            fault.active = false;
            awaitUntil(2_000L, () -> task.failState().consecutiveFailures() == 0);
        } finally {
            task.stop();
            thread.join(2_000L);
        }
    }

    /** Wire a task whose only failing collaborator is {@code step}; everything upstream of it succeeds. */
    private static ChannelSyncTask taskFailingAt(
            final FailingStep step, final Fault fault, final SimpleMetrics metrics) {
        final ContractStateReader reader =
                step == FailingStep.READ_STATE ? readerFailingWhile(fault) : new StubStateReader(activeNoOutbound());
        final PeerSelector peer =
                step == FailingStep.SELECT_PEER ? selectorFailingWhile(fault) : new EmptyPeerSelector();
        return new ChannelSyncTask(
                ops(reader, new EmptyBundleConstructor(), new NoOpSubmitter()),
                CommitmentLevel.LATEST,
                peer,
                NO_OP_CLPR_ENDPOINT_CLIENT,
                metrics,
                5L, // tiny backoff so the loop churns and recovers fast
                50L,
                TEST_SYNC_INTERVAL_MS);
    }

    private static ContractStateReader readerFailingWhile(final Fault fault) {
        return new ContractStateReader() {
            @NonNull
            @Override
            public Optional<ClprChannel> readChannelState(@NonNull Bytes id, @NonNull String tag) {
                if (fault.active) {
                    throw new IllegalStateException("readChannelState failed");
                }
                return Optional.of(activeNoOutbound());
            }

            @NonNull
            @Override
            public List<QueuedMessage> readQueuedMessages(@NonNull Bytes id, long f, long t, @NonNull String tag) {
                return List.of();
            }

            @NonNull
            @Override
            public ClprLedgerConfiguration readLedgerConfiguration(@NonNull CommitmentLevel l) {
                return ClprLedgerConfiguration.DEFAULT;
            }
        };
    }

    private static PeerSelector selectorFailingWhile(final Fault fault) {
        return new PeerSelector(new PeerEndpointCache()) {
            @Override
            public Optional<ClprEndpoint> selectPeer() {
                if (fault.active) {
                    throw new IllegalStateException("selectPeer failed");
                }
                return Optional.empty();
            }
        };
    }

    private static long cyclesFailed(final SimpleMetrics metrics) {
        // cyclesFailed is a LabeledCounter registered lazily with channel_id + reason labels.
        final String channelIdLabel = HexFormat.of().formatHex(CHANNEL_ID.toByteArray());
        return metrics.findMetricsByCategory("sync").stream()
                .filter(m ->
                        m.getName().startsWith("cycles.failed") && m.getName().contains("channel_id=" + channelIdLabel))
                .filter(Counter.class::isInstance)
                .mapToLong(m -> ((Counter) m).get())
                .sum();
    }

    @Test
    void run_exitsPromptlyOnInterruptDuringBackoff() throws Exception {
        // Always throws, with a long backoff so the loop is parked in Thread.sleep when interrupted.
        final var reader = new ScriptedStateReader(List.of(ScriptedStateReader.Action.THROW), activeNoOutbound());
        final var metrics = new SimpleMetrics();
        final var task = resilientTask(reader, metrics, 60_000L, 60_000L);

        final var thread = Thread.startVirtualThread(task::run);
        try {
            awaitUntil(5_000L, () -> cyclesFailed(metrics) >= 1L); // first failure → now parked in backoff
            task.stop(); // interrupts the backoff sleep
            thread.join(2_000L);
            assertThat(thread.isAlive()).isFalse();
        } finally {
            thread.interrupt();
            thread.join(2_000L);
        }
    }
}
