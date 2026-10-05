// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.grpc.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Metrics;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.hiero.clpr.relay.core.Certs;
import org.hiero.clpr.relay.core.LeafKeyManager;
import org.hiero.clpr.relay.core.metrics.SimpleMetrics;
import org.hiero.clpr.relay.core.testfixtures.CertFixtures;
import org.junit.jupiter.api.Test;

class ClprEndpointClientTest {

    private Metrics metrics = new SimpleMetrics();

    private static ClprSyncPayload emptyPayload() {
        return ClprSyncPayload.newBuilder().channelId(Bytes.wrap(new byte[32])).build();
    }

    /**
     * Regression: after {@link ClprEndpointClient#close()} a still-running sync task must not be able
     * to build and cache a fresh channel — otherwise that channel outlives the drain and grpc-java's
     * orphan detector logs a {@code SEVERE} when it is garbage collected. The call must fail fast
     * without leaving anything in the cache (no network connection is attempted).
     */
    @Test
    void syncAfterClose_throwsAndLeavesNoCachedChannel() {
        final ClprEndpointClient client = new ClprEndpointClient(1_048_576, null);
        client.close();

        assertThatThrownBy(() -> client.sync("127.0.0.1", 1, emptyPayload(), reply -> reply, null))
                .isInstanceOf(IllegalStateException.class);

        assertThat(client.cachedChannels())
                .as("no channel may be cached once the client is closed")
                .isEmpty();
    }

    /** Returns a port number that is (momentarily) free, so a connect attempt is refused. */
    private static int closedPort() throws Exception {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    void close_shutsDownAndClearsCachedChannels() throws Exception {
        final int deadPort = closedPort();
        // connection-refused surfaces as UNAVAILABLE (immediate) regardless of timeout — only
        // DEADLINE_EXCEEDED auto-evicts, so the refused channel stays cached for close() to drain.
        final var client = new ClprEndpointClient(1_048_576, null);
        final var payload = ClprSyncPayload.newBuilder()
                .channelId(Bytes.wrap(new byte[] {1}))
                .build();

        // The call fails, but the lazily-created channel stays cached (we only evict on
        // DEADLINE_EXCEEDED) — this is exactly the channel that would otherwise be leaked.
        assertThatThrownBy(() -> client.sync("127.0.0.1", deadPort, payload, reply -> reply, null))
                .isInstanceOf(StatusRuntimeException.class);

        final var cached = client.cachedChannels();
        assertThat(cached).hasSize(1);
        assertThat(cached.get(0).isShutdown()).isFalse();

        client.close();

        assertThat(client.cachedChannels()).isEmpty();
        assertThat(cached.get(0).isShutdown()).isTrue();
    }

    @Test
    void close_isIdempotentAndSafeOnEmptyClient() {
        final var client = new ClprEndpointClient(1_048_576, null);
        assertThatCode(client::close).doesNotThrowAnyException();
        assertThatCode(client::close).doesNotThrowAnyException();
    }

    // ---------------------------------------------------------------------
    // Outbound TLS: own material + per-peer channel selection
    // ---------------------------------------------------------------------

    @Test
    void secureWithOwnLeaf_constructs() throws Exception {
        final var keyManager =
                new LeafKeyManager(Certs.parsePrivateKey(CertFixtures.CA_A_KEY_DER), Duration.ZERO, metrics);

        try (var client = new ClprEndpointClient(1 << 20, keyManager)) {
            assertThat(client.cachedChannels()).isEmpty();
        }
    }

    @Test
    void plaintext_withNullLeaf_constructs() {
        try (var client = new ClprEndpointClient(1 << 20, null)) {
            assertThat(client.cachedChannels()).isEmpty();
        }
    }

    @Test
    void certRotation_retiresStaleChannelWithoutGrowingCache() throws Exception {
        final int deadPort = closedPort();
        final var keyManager =
                new LeafKeyManager(Certs.parsePrivateKey(CertFixtures.CA_A_KEY_DER), Duration.ZERO, metrics);
        // Secure profile so dials are TLS and channels key on the peer-cert fingerprint.
        // connection-refused is UNAVAILABLE (immediate), so the channel stays cached after the failed call.
        final var client = new ClprEndpointClient(1_048_576, keyManager);

        // First dial pins cert A → one cached channel.
        assertThatThrownBy(() -> client.sync(
                        "127.0.0.1", deadPort, emptyPayload(), reply -> reply, Bytes.wrap(CertFixtures.CA_A_CERT_DER)))
                .isInstanceOf(StatusRuntimeException.class);
        final var afterFirst = client.cachedChannels();
        assertThat(afterFirst).hasSize(1);
        final var staleChannel = afterFirst.get(0);

        // Peer rotates its on-chain cert to B → the new channel supersedes the old; the cache does not grow.
        assertThatThrownBy(() -> client.sync(
                        "127.0.0.1", deadPort, emptyPayload(), reply -> reply, Bytes.wrap(CertFixtures.CA_B_CERT_DER)))
                .isInstanceOf(StatusRuntimeException.class);

        assertThat(client.cachedChannels()).hasSize(1);
        assertThat(staleChannel.isShutdown())
                .as("the pre-rotation channel is retired")
                .isTrue();

        client.close();
    }

    /**
     * Regression: when the phase-2 {@code followUpFactory} throws (e.g. the local {@code createBundle}
     * hits an EVM-RPC failure) <em>after</em> the peer's phase-1 reply has already arrived, {@code sync}
     * must cancel the half-open call in its {@code finally}. Otherwise the client stream and the peer's
     * {@code ClprSyncSession} stay pinned until the ~10s {@code CallOptions} deadline fires — a stream
     * leaked per failing cycle under a sustained outage. We stand up a minimal gRPC server that answers
     * phase 1 once and never completes, drive a phase-2 factory that throws, and assert the server
     * observes the cancellation promptly rather than at the deadline.
     */
    @Test
    void followUpFactoryThrows_cancelsHalfOpenCallPromptly() throws Exception {
        final CountDownLatch serverSawCancel = new CountDownLatch(1);
        final Server server = startReplyOnceThenAwaitServer(serverSawCancel);
        try (var client = new ClprEndpointClient(1 << 20, null)) {
            final int port = server.getPort();

            assertThatThrownBy(() -> client.sync(
                            "127.0.0.1",
                            port,
                            emptyPayload(),
                            reply -> {
                                throw new RuntimeException("phase-2 build failed");
                            },
                            null))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("phase-2 build failed");

            // With the cancel-on-abnormal-exit fix this fires in milliseconds; without it the server
            // would only see cancellation once the client's ~10s CallOptions deadline elapsed.
            assertThat(serverSawCancel.await(5, TimeUnit.SECONDS))
                    .as("the peer must observe the aborted call cancelled promptly, not at the deadline")
                    .isTrue();
        } finally {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /** Raw byte marshaller mirroring the one {@link ClprEndpointClient} uses for the {@code sync} RPC. */
    private static final MethodDescriptor.Marshaller<byte[]> BYTE_MARSHALLER = new MethodDescriptor.Marshaller<>() {
        @Override
        public InputStream stream(final byte[] value) {
            return new ByteArrayInputStream(value);
        }

        @Override
        public byte[] parse(final InputStream stream) {
            try {
                return stream.readAllBytes();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
    };

    /** The same {@code proto.ClprEndpointService/sync} bidi descriptor the client dials. */
    private static final MethodDescriptor<byte[], byte[]> SYNC_METHOD = MethodDescriptor.<byte[], byte[]>newBuilder()
            .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
            .setFullMethodName(MethodDescriptor.generateFullMethodName("proto.ClprEndpointService", "sync"))
            .setRequestMarshaller(BYTE_MARSHALLER)
            .setResponseMarshaller(BYTE_MARSHALLER)
            .build();

    /**
     * Starts a Netty gRPC server on an ephemeral port whose {@code sync} handler answers the first
     * inbound message with one reply and then deliberately leaves the stream open (mirroring a responder
     * waiting in {@code AWAITING_FOLLOW_UP}). {@code sawCancel} counts down when the call is cancelled.
     */
    private static Server startReplyOnceThenAwaitServer(final CountDownLatch sawCancel) throws Exception {
        final byte[] reply = ClprSyncPayload.PROTOBUF.toBytes(emptyPayload()).toByteArray();
        final ServerServiceDefinition service = ServerServiceDefinition.builder("proto.ClprEndpointService")
                .addMethod(SYNC_METHOD, ServerCalls.asyncBidiStreamingCall(responseObserver -> {
                    final ServerCallStreamObserver<byte[]> resp = (ServerCallStreamObserver<byte[]>) responseObserver;
                    resp.setOnCancelHandler(sawCancel::countDown);
                    return new StreamObserver<>() {
                        private boolean replied = false;

                        @Override
                        public void onNext(final byte[] value) {
                            if (!replied) {
                                replied = true;
                                resp.onNext(reply); // phase-1 answer; never onCompleted — stay open
                            }
                        }

                        @Override
                        public void onError(final Throwable t) {}

                        @Override
                        public void onCompleted() {}
                    };
                }))
                .build();
        return NettyServerBuilder.forPort(0).addService(service).build().start();
    }

    @Test
    void differentPeers_keepSeparateChannels() throws Exception {
        final int portA;
        final int portB;
        try (var s1 = new ServerSocket(0);
                var s2 = new ServerSocket(0)) {
            portA = s1.getLocalPort();
            portB = s2.getLocalPort();
        }
        // Secure profile so per-peer dials build distinct TLS channels.
        final var client = new ClprEndpointClient(1_048_576, null);

        assertThatThrownBy(() -> client.sync(
                        "127.0.0.1", portA, emptyPayload(), reply -> reply, Bytes.wrap(CertFixtures.CA_A_CERT_DER)))
                .isInstanceOf(StatusRuntimeException.class);
        assertThatThrownBy(() -> client.sync(
                        "127.0.0.1", portB, emptyPayload(), reply -> reply, Bytes.wrap(CertFixtures.CA_A_CERT_DER)))
                .isInstanceOf(StatusRuntimeException.class);

        assertThat(client.cachedChannels())
                .as("distinct peers keep distinct channels")
                .hasSize(2);

        client.close();
    }
}
