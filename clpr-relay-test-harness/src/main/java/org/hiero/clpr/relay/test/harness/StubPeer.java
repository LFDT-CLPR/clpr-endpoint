// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.test.harness;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.grpc.helidon.config.PbjConfig;
import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.GrpcStatus;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import io.helidon.webserver.WebServer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Flow;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import org.hiero.clpr.relay.grpc.client.ClprEndpointClient;

/**
 * A controllable CLPR peer endpoint for single-side integration tests, speaking the two-phase
 * {@code sync} bidirectional-streaming protocol.
 *
 * <p>Hosts a real Helidon + PBJ gRPC server (the same stack as the production {@code ClprGrpcServer}),
 * so the endpoint-under-test's real {@code ClprEndpointClient} talks to it over real HTTP/2. Two
 * directions:
 *
 * <ul>
 *   <li><b>Server (responder):</b> answers the endpoint's outbound two-phase {@code sync} streams. The
 *       phase-1 reply is decided synchronously by the configured policy ({@link #respondEmpty()} /
 *       {@link #respondWith(ClprSyncPayload)} / {@link #onSync(Function)}). Every inbound message
 *       (both the phase-1 request and the phase-2 follow-up) is recorded for assertions.</li>
 *   <li><b>Client (injector):</b> {@link #pokeSync} runs a two-phase stream <em>into</em> the
 *       endpoint-under-test and returns its phase-1 reply.</li>
 * </ul>
 *
 * <p>{@link #awaitSync} is a read-only tap on the recorded message stream; it does not shape replies.
 * The standing responder must always return promptly (the endpoint's client read timeout is ~10s).
 */
public final class StubPeer implements AutoCloseable {

    private static final int DEFAULT_MAX_MESSAGE_SIZE = 1 << 20;

    private final WebServer server;
    private final ClprEndpointClient client;
    private final BlockingQueue<ClprSyncPayload> received = new LinkedBlockingQueue<>();
    private final AtomicInteger syncCount = new AtomicInteger();

    /** Standing phase-1 response policy; defaults to a responsive-but-silent peer that asks for our data. */
    private volatile Function<ClprSyncPayload, ClprSyncPayload> syncResponder = StubPeer::requestOnlyReply;

    private StubPeer(final int maxMessageSize) {
        this.client = new ClprEndpointClient(maxMessageSize);
        this.server = WebServer.builder()
                .name("stub-peer clpr-grpc")
                .port(0) // ephemeral; read back via port()
                .addProtocol(PbjConfig.builder()
                        .name("pbj")
                        .maxMessageSizeBytes(maxMessageSize)
                        .build())
                .addRouting(PbjRouting.builder().service(new StubService()))
                .build()
                .start();
    }

    /** Start a stub peer on an ephemeral port with the default max message size. */
    public static StubPeer start() {
        return new StubPeer(DEFAULT_MAX_MESSAGE_SIZE);
    }

    /** Start a stub peer on an ephemeral port with an explicit max gRPC message size. */
    public static StubPeer start(final int maxMessageSize) {
        return new StubPeer(maxMessageSize);
    }

    /** The bound (ephemeral) port. */
    public int port() {
        return server.port();
    }

    // ── Response policy (server thread) ───────────────────────────────────────

    /**
     * Answer every inbound phase-1 request by asking the endpoint-under-test for its data (a
     * {@code bundle_request} from message 1) while injecting no bundle of our own. This is the
     * responsive-but-silent default: it lets the endpoint deliver its own bundle in phase 2 (so the
     * relay's outbound is observable via {@link #awaitSync}) without pushing any inbound progress.
     */
    public StubPeer respondEmpty() {
        this.syncResponder = StubPeer::requestOnlyReply;
        return this;
    }

    /** Answer every inbound phase-1 request with a fixed payload. */
    public StubPeer respondWith(final ClprSyncPayload fixed) {
        this.syncResponder = req -> fixed;
        return this;
    }

    /**
     * Answer each inbound phase-1 request with a payload derived from the request. This runs on the
     * server thread and MUST return promptly.
     */
    public StubPeer onSync(final Function<ClprSyncPayload, ClprSyncPayload> handler) {
        this.syncResponder = handler;
        return this;
    }

    // ── Observation (test thread) ─────────────────────────────────────────────

    /** Number of inbound sync messages received so far (phase-1 requests + phase-2 follow-ups). */
    public int syncCount() {
        return syncCount.get();
    }

    /** Snapshot of all inbound sync payloads recorded so far (oldest first). */
    public List<ClprSyncPayload> received() {
        return List.copyOf(received);
    }

    /**
     * Whether the endpoint-under-test has, at any point since the last {@link #drain()}, delivered a
     * non-empty bundle to us (a phase-2 {@code bundle_response} with a non-empty payload). Full-duplex
     * tests gate an injected inbound bundle on this so they only ack the endpoint's outbound once it has
     * actually been sent — acking before that would revert on the contract ({@code received >= next}).
     */
    public boolean sawOutboundBundle() {
        return received.stream()
                .anyMatch(p -> p.hasBundleResponse()
                        && p.bundleResponse().bundlePayload().length() > 0);
    }

    /** Discard all recorded captures (use before a triggering action to assert on a clean slate). */
    public void drain() {
        received.clear();
    }

    /**
     * Block until the next inbound sync message arrives, or the timeout elapses.
     *
     * @return the next recorded payload, or {@code null} on timeout
     */
    public ClprSyncPayload awaitSync(final Duration timeout) {
        try {
            return received.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /**
     * Block until an inbound sync message matching {@code predicate} arrives, discarding non-matching
     * captures, or the timeout elapses.
     *
     * @return the first matching payload, or {@code null} on timeout
     */
    public ClprSyncPayload awaitSync(final Predicate<ClprSyncPayload> predicate, final Duration timeout) {
        final long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            final long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0) {
                return null;
            }
            final ClprSyncPayload payload;
            try {
                payload = received.poll(remainingNanos, TimeUnit.NANOSECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
            if (payload == null) {
                return null;
            }
            if (predicate.test(payload)) {
                return payload;
            }
        }
    }

    // ── Client path ───────────────────────────────────────────────────────────

    /**
     * Run a two-phase sync <em>into</em> the endpoint-under-test with an empty phase-2 follow-up, and
     * return the endpoint's phase-1 reply. Use to observe the endpoint's own bundle/request.
     *
     * @param host    the endpoint-under-test's host
     * @param port    the endpoint-under-test's gRPC port
     * @param phase1  the crafted phase-1 payload (typically carrying our {@code bundle_request})
     * @return the endpoint's phase-1 reply payload
     * @throws Exception if the RPC fails
     */
    public ClprSyncPayload pokeSync(final String host, final int port, final ClprSyncPayload phase1) throws Exception {
        return pokeSync(host, port, phase1, reply -> ClprSyncPayload.newBuilder()
                .channelId(phase1.channelId())
                .build());
    }

    /**
     * Run a full two-phase sync into the endpoint-under-test: send {@code phase1}, then send the
     * follow-up produced by {@code followUp} from the endpoint's reply (this is where a crafted
     * {@code bundle_response} is injected for the endpoint to submit). Returns the endpoint's phase-1
     * reply.
     *
     * @param host     the endpoint-under-test's host
     * @param port     the endpoint-under-test's gRPC port
     * @param phase1   the crafted phase-1 payload
     * @param followUp maps the endpoint's phase-1 reply to our phase-2 follow-up
     * @return the endpoint's phase-1 reply payload
     * @throws Exception if the RPC fails
     */
    public ClprSyncPayload pokeSync(
            final String host,
            final int port,
            final ClprSyncPayload phase1,
            final Function<ClprSyncPayload, ClprSyncPayload> followUp)
            throws Exception {
        return client.sync(host, port, phase1, followUp);
    }

    @Override
    public void close() {
        client.close();
        server.stop();
    }

    /**
     * A phase-1 reply that asks the endpoint-under-test for its data (a {@code bundle_request} from
     * message 1) but injects no bundle of our own — the responsive-but-silent default.
     */
    private static ClprSyncPayload requestOnlyReply(final ClprSyncPayload request) {
        return ClprSyncPayload.newBuilder()
                .channelId(request.channelId())
                .bundleRequest(
                        BundleRequest.newBuilder().currentReceivedMessageId(0L).build())
                .build();
    }

    /** Minimal PBJ {@link ServiceInterface} exposing the two-phase {@code sync} (recorded). */
    private final class StubService implements ServiceInterface {
        private static final ServiceInterface.Method SYNC = () -> "sync";

        @Override
        public String serviceName() {
            return "ClprEndpointService";
        }

        @Override
        public String fullName() {
            return "proto.ClprEndpointService";
        }

        @Override
        public List<Method> methods() {
            return List.of(SYNC);
        }

        @Override
        public Pipeline<? super Bytes> open(
                final Method method, final RequestOptions opts, final Pipeline<? super Bytes> responses)
                throws GrpcException {
            final boolean isProtobuf = opts.isProtobuf();
            if ("sync".equals(method.name())) {
                return Pipelines.<ClprSyncPayload, ClprSyncPayload>bidiStreaming()
                        .mapRequest(bytes ->
                                isProtobuf ? ClprSyncPayload.PROTOBUF.parse(bytes) : ClprSyncPayload.JSON.parse(bytes))
                        .method(out -> new TwoPhaseResponder(out))
                        .mapResponse(reply -> isProtobuf
                                ? ClprSyncPayload.PROTOBUF.toBytes(reply)
                                : Bytes.wrap(ClprSyncPayload.JSON.toJSON(reply)))
                        .respondTo(responses)
                        .build();
            }
            throw new GrpcException(GrpcStatus.UNIMPLEMENTED, "Unknown method: " + method.name());
        }
    }

    /**
     * Server-side two-phase session: records the phase-1 request, replies once via the configured
     * responder, then records the phase-2 follow-up and completes.
     */
    private final class TwoPhaseResponder implements Pipeline<ClprSyncPayload> {
        private final Pipeline<? super ClprSyncPayload> out;
        private boolean awaitingRequest = true;

        TwoPhaseResponder(final Pipeline<? super ClprSyncPayload> out) {
            this.out = out;
        }

        @Override
        public void onSubscribe(final Flow.Subscription subscription) {
            // PBJ's bidi pipeline drives delivery from its own source subscription and does not subscribe
            // this handler, so onNext fires for every inbound message without us requesting. Request
            // unbounded defensively; never per-message back-pressure on a subscription we are never handed.
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(final ClprSyncPayload message) {
            received.offer(message);
            syncCount.incrementAndGet();
            if (awaitingRequest) {
                awaitingRequest = false;
                out.onNext(syncResponder.apply(message));
            } else {
                out.onComplete();
            }
        }

        @Override
        public void onError(final Throwable throwable) {
            // Client aborted; nothing to clean up.
        }

        @Override
        public void onComplete() {
            // Client half-closed.
        }
    }
}
