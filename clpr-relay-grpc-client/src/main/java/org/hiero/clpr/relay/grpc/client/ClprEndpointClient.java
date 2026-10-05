// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.grpc.client;

import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.logging.api.Logger;
import com.swirlds.logging.api.Loggers;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.KeyManagerFactorySpi;
import javax.net.ssl.ManagerFactoryParameters;
import org.hiero.clpr.relay.core.Certs;
import org.hiero.clpr.relay.core.LeafKeyManager;
import org.jspecify.annotations.Nullable;

/**
 * gRPC client for the mutual-TLS-capable {@code sync} RPC against peer CLPR endpoints.
 *
 * <p>Uses {@code grpc-netty-shaded}'s {@code NettyChannelBuilder} for outbound
 * HTTP/2 transport and standard grpc-java {@link ClientCalls} for the unary
 * {@code sync} RPC against remote peers.
 *
 * <p>We do <b>not</b> use Helidon's {@code GrpcClient} or PBJ's
 * {@code pbj-grpc-client-helidon} here: both omit the {@code TE: trailers}
 * HTTP/2 request header that grpc-java's Netty server enforces, so a Hiero
 * consensus node (which uses grpc-java/Netty) returns {@code NOT_FOUND} for
 * every gRPC call from a Helidon-backed client. Once the upstream PBJ bug
 * is fixed, we can move back to a single transport.
 */
public class ClprEndpointClient implements AutoCloseable {

    private static final Logger LOGGER = Loggers.getLogger(ClprEndpointClient.class);

    /** The gRPC service name. */
    private static final String SERVICE_NAME = "proto.ClprEndpointService";

    /**
     * Default read timeout for gRPC calls. Sent as the {@code grpc-timeout} HTTP/2 header.
     * Computed as 5* RECEIPT_POLL_ATTEMPTS * RECEIPT_POLL_SLEEP_MS to provide comfortable headroom.
     */
    private static final Duration DEFAULT_READ_TIMEOUT = Duration.ofMillis(20 * 100 * 5);

    /** An empty DER buffer meaning "no peer certificate" → dial plaintext. */
    private static final byte[] NO_PEER_CERT = new byte[0];

    /** Maximum inbound gRPC message size in bytes; applied to every cached channel. */
    private final int maxInboundMessageSize;

    /**
     * This endpoint's dynamic TLS leaf key manager, presented as the client certificate when dialing a
     * peer over mutual TLS. {@code null} when no secure material is configured — such a relay can still
     * dial plaintext peers but cannot present a client certificate to a mandatory-mTLS peer.
     *
     * <p>The key manager re-mints its leaf on demand when the rotation window elapses, so new TLS
     * connections opened through any channel automatically use the current leaf.
     */
    @Nullable
    private final LeafKeyManager leafKeyManager;

    /**
     * Construct with the given transport settings and an optional leaf key manager.
     *
     * <p>The caller is responsible for creating {@code leafKeyManager} and passing the same instance to
     * both this client and the inbound {@code ClprGrpcServer}. The read timeout is always
     * {@link #DEFAULT_READ_TIMEOUT}.
     *
     * @param maxInboundMessageSize maximum inbound gRPC message size in bytes; must be positive
     * @param leafKeyManager        this endpoint's dynamic leaf key manager; required when {@code tlsEnabled}
     *                              and this endpoint must present a client certificate, {@code null} otherwise
     */
    public ClprEndpointClient(final int maxInboundMessageSize, @Nullable final LeafKeyManager leafKeyManager) {
        if (maxInboundMessageSize <= 0) {
            throw new IllegalArgumentException("maxInboundMessageSize must be positive, got " + maxInboundMessageSize);
        }
        this.maxInboundMessageSize = maxInboundMessageSize;
        this.leafKeyManager = leafKeyManager;
    }

    /** A simple byte-array marshaller for protobuf-encoded messages. */
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
                throw new RuntimeException("Failed to read gRPC response", e);
            }
        }
    };

    /**
     * HTTP/2 keepalive ping interval. Keeps the connection alive through idle periods and
     * allows the OS to detect a dead peer before the next RPC deadline fires.
     */
    private static final Duration KEEPALIVE_TIME = Duration.ofSeconds(20);

    /**
     * How long to wait for a keepalive ping ACK before treating the connection as dead.
     * A short timeout ensures that a broken connection is detected quickly.
     */
    private static final Duration KEEPALIVE_TIMEOUT = Duration.ofSeconds(5);

    /**
     * Per-peer channel cache keyed by {@code "host:port"}.
     *
     * <p>Each {@link ManagedChannel} owns a Netty event-loop group and an HTTP/2 connection.
     * Creating one per RPC is expensive; caching gives us a single long-lived connection per
     * peer that is reused across calls.
     *
     * <p>On {@code DEADLINE_EXCEEDED} the stale channel is evicted so the next call opens a
     * fresh connection rather than inheriting a potentially broken one.
     */
    private final ConcurrentHashMap<String, ManagedChannel> channelCache = new ConcurrentHashMap<>();

    /**
     * Set once {@link #close()} begins. Published volatile so a concurrent {@link #channelFor} can
     * observe it and avoid leaving a freshly-built channel behind after the cache has been drained.
     * Without this, a sync task still running after {@code close()} (the orchestrator only signals
     * its tasks, it does not join them) can {@code computeIfAbsent} a new {@link ManagedChannel} that
     * nobody ever shuts down — grpc-java's orphan detector then logs a {@code SEVERE} when it is GC'd.
     */
    private volatile boolean closed = false;

    /** The grpc-java method descriptor for the two-phase sync bidirectional-streaming RPC. */
    private static final MethodDescriptor<byte[], byte[]> SYNC_METHOD_DESCRIPTOR =
            MethodDescriptor.<byte[], byte[]>newBuilder()
                    .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                    .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "sync"))
                    .setRequestMarshaller(BYTE_MARSHALLER)
                    .setResponseMarshaller(BYTE_MARSHALLER)
                    .build();

    /**
     * Runs a two-phase {@code sync} exchange with a peer endpoint over a single bidirectional
     * stream, driven synchronously:
     *
     * <ol>
     *   <li>open the stream and send {@code request} (phase 1),</li>
     *   <li>block until the peer's single response arrives,</li>
     *   <li>derive the follow-up from that response via {@code followUpFactory}, send it (phase 2),
     *       then half-close,</li>
     *   <li>wait for the server to complete the stream.</li>
     * </ol>
     *
     * <p>The peer emits exactly one response (to the phase-1 request); the phase-2 follow-up is not
     * answered. The call is bounded by the read timeout at each blocking step. Transport (mutual-TLS
     * vs plaintext) is chosen exactly as in {@link #sync}.
     *
     * @param host               the peer's hostname or IP address
     * @param port               the peer's gRPC port
     * @param request            the phase-1 payload to send
     * @param followUpFactory    maps the peer's response to the phase-2 follow-up payload
     * @param peerTlsCertificate the peer's on-chain {@code tls_certificate} (CA DER); required when
     *                           this endpoint's {@code sync.tlsEnabled}, ignored otherwise
     * @return the peer's phase-1 response payload
     * @throws Exception if the call fails or does not complete within the read timeout
     */
    public ClprSyncPayload sync(
            final String host,
            final int port,
            final ClprSyncPayload request,
            final Function<ClprSyncPayload, ClprSyncPayload> followUpFactory,
            @Nullable final Bytes peerTlsCertificate)
            throws Exception {
        final byte[] peerCertDer;
        if (leafKeyManager != null) {
            if (peerTlsCertificate == null || peerTlsCertificate.length() == 0) {
                throw new IllegalStateException("sync.tlsEnabled=true but peer " + host + ":" + port
                        + " publishes no on-chain tls_certificate; cannot establish the required mTLS channel");
            }
            peerCertDer = peerTlsCertificate.toByteArray();
        } else {
            peerCertDer = NO_PEER_CERT;
        }
        final String key = cacheKey(host, port, peerCertDer);
        final ManagedChannel channel = channelFor(key, host, port, () -> buildChannel(host, port, peerCertDer));
        final var callOptions =
                CallOptions.DEFAULT.withDeadlineAfter(DEFAULT_READ_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        final BlockingQueue<ClprSyncPayload> replies = new ArrayBlockingQueue<>(2);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final CountDownLatch completed = new CountDownLatch(1);

        final StreamObserver<byte[]> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(final byte[] value) {
                try {
                    if (!replies.offer(ClprSyncPayload.PROTOBUF.parse(Bytes.wrap(value)))) {
                        LOGGER.warn("Received more than 2 messages while syncing with peer");
                    }
                } catch (final Exception e) {
                    error.compareAndSet(null, e);
                }
            }

            @Override
            public void onError(final Throwable t) {
                error.compareAndSet(null, t);
                completed.countDown();
            }

            @Override
            public void onCompleted() {
                completed.countDown();
            }
        };

        // asyncBidiStreamingCall always returns a ClientCallStreamObserver; casting lets us cancel the
        // call in the finally below, which onError() alone cannot do once the request stream is half-closed.
        final ClientCallStreamObserver<byte[]> requestObserver =
                (ClientCallStreamObserver<byte[]>) ClientCalls.asyncBidiStreamingCall(
                        channel.newCall(SYNC_METHOD_DESCRIPTOR, callOptions), responseObserver);
        // Only a fully-completed exchange (server finished the stream cleanly) leaves the call in a state
        // that needs no teardown. Every other exit — a phase-2 build failure, an await timeout, a
        // transport error, or no phase-1 reply — must cancel the half-open call so the client stream and
        // the peer's ClprSyncSession are released now, not when the CallOptions deadline eventually fires.
        boolean completedNormally = false;
        try {
            // Phase 1: send the request.
            requestObserver.onNext(ClprSyncPayload.PROTOBUF.toBytes(request).toByteArray());

            // Block for the peer's single response.
            final ClprSyncPayload reply = replies.poll(DEFAULT_READ_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            if (reply == null) {
                throw toException(
                        error.get(),
                        Status.DEADLINE_EXCEEDED
                                .withDescription("sync: no response from peer within read timeout")
                                .asRuntimeException());
            }

            // Phase 2: send the follow-up derived from the response, then half-close.
            final ClprSyncPayload followUp = followUpFactory.apply(reply);
            requestObserver.onNext(ClprSyncPayload.PROTOBUF.toBytes(followUp).toByteArray());
            requestObserver.onCompleted();

            // Wait for the server to finish the stream.
            if (!completed.await(DEFAULT_READ_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                throw Status.DEADLINE_EXCEEDED
                        .withDescription("sync: stream did not complete within read timeout")
                        .asRuntimeException();
            }
            final Throwable t = error.get();
            if (t != null) {
                throw toException(t, null);
            }
            completedNormally = true;
            return reply;
        } catch (final StatusRuntimeException e) {
            if (isTransportError(e)) {
                evictChannel(key, channel);
            }
            throw e;
        } finally {
            if (!completedNormally) {
                // cancel() is idempotent and safe on an already-terminated call; it is the only teardown
                // that works both before and after the half-close (onCompleted) above.
                requestObserver.cancel("sync: aborted before completion", null);
            }
        }
    }

    /**
     * Chooses the exception to surface from a failed {@code sync} call: the observed transport
     * error when present, otherwise the supplied fallback. Re-throws a {@link RuntimeException}
     * directly; wraps anything else.
     */
    private static Exception toException(
            @Nullable final Throwable observed, @Nullable final StatusRuntimeException fallback) {
        final Throwable t = observed != null ? observed : fallback;
        if (t instanceof RuntimeException re) {
            throw re;
        }
        if (t instanceof Exception e) {
            return e;
        }
        return new RuntimeException("sync failed", t);
    }

    /**
     * Cache key for a channel. Plaintext channels key on
     * {@code host:port}; mutual-TLS sync channels add the peer-certificate fingerprint so a roster CA
     * rotation opens a fresh channel rather than reusing a stale one.
     */
    private static String cacheKey(final String host, final int port, final byte[] peerCertDer) {
        if (peerCertDer.length == 0) {
            return host + ":" + port;
        }
        return host + ":" + port + ":" + Certs.sha256Hex(peerCertDer);
    }

    /**
     * Returns a cached gRPC channel for the given key, creating one if absent. TLS-vs-plaintext is
     * decided by {@code peerCertDer} (empty → plaintext). Channels are built with HTTP/2 keepalives so
     * idle connections stay alive and broken ones are detected before the next RPC deadline fires.
     * See class-level javadoc for why we use grpc-netty here instead of Helidon.
     */
    private ManagedChannel channelFor(
            final String key, final String host, final int port, final Supplier<ManagedChannel> factory) {
        if (closed) {
            throw new IllegalStateException("ClprEndpointClient is closed");
        }
        final boolean[] created = {false};
        final ManagedChannel channel = channelCache.computeIfAbsent(key, _ -> {
            created[0] = true;
            return factory.get();
        });
        // A concurrent close() may have drained the cache after the open-check above. Re-check the
        // published flag: if close() has begun, evict and shut down the channel we just inserted so
        // it can never be orphaned. close() setting the flag happens-before its cache traversal, so
        // exactly one of {close(), this re-check} owns the shutdown — shutdownNow() is idempotent.
        if (closed) {
            channelCache.remove(key, channel);
            channel.shutdownNow();
            throw new IllegalStateException("ClprEndpointClient is closed");
        }
        if (created[0]) {
            // A newly-built channel for this peer supersedes any prior channel to the same host:port —
            // a rotated on-chain certificate (different fingerprint) or a plaintext↔TLS switch. Retire
            // the stale one so its keepAlive connection is not orphaned in the cache indefinitely.
            evictStaleSiblings(host, port, key);
        }
        return channel;
    }

    /**
     * Shut down and remove any cached channel to {@code host:port} other than {@code keepKey}. Siblings
     * are the plaintext entry ({@code host:port}) and any TLS entry ({@code host:port:<fingerprint>}).
     */
    private void evictStaleSiblings(final String host, final int port, final String keepKey) {
        final String hostPort = host + ":" + port;
        final String tlsPrefix = hostPort + ":";
        for (final var entry : channelCache.entrySet()) {
            final String k = entry.getKey();
            if (!k.equals(keepKey) && (k.equals(hostPort) || k.startsWith(tlsPrefix))) {
                if (channelCache.remove(k, entry.getValue())) {
                    entry.getValue().shutdownNow();
                }
            }
        }
    }

    /** Build a plaintext or (peer-pinned, optionally mutual) TLS channel. */
    private ManagedChannel buildChannel(final String host, final int port, final byte[] peerCertDer) {
        final var builder = NettyChannelBuilder.forAddress(host, port)
                .maxInboundMessageSize(maxInboundMessageSize)
                .keepAliveTime(KEEPALIVE_TIME.toSeconds(), TimeUnit.SECONDS)
                .keepAliveTimeout(KEEPALIVE_TIMEOUT.toSeconds(), TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true);
        if (peerCertDer.length == 0) {
            builder.usePlaintext();
        } else {
            try {
                // A deterministic authority; the CA-chain trust manager checks identity, so hostname
                // verification is moot.
                builder.sslContext(clientSslContext(peerCertDer)).overrideAuthority("clpr");
            } catch (final Exception e) {
                throw new IllegalStateException(
                        "failed to build a TLS channel to " + host + ":" + port + ": " + e.getMessage(), e);
            }
        }
        return builder.build();
    }

    /**
     * Build the client {@link SslContext} that validates the peer's presented leaf against its on-chain
     * CA certificate and (when this endpoint has its own material) presents its client leaf for mutual
     * TLS. The JDK provider is forced so the custom trust manager behaves portably.
     *
     * <p>When a {@link LeafKeyManager} is available, it is wrapped in a thin {@link KeyManagerFactory}
     * and passed to the builder so the JDK TLS stack calls it on every new handshake — enabling the
     * dynamic rotation behaviour without rebuilding the channel or the {@code SslContext}.
     */
    private SslContext clientSslContext(final byte[] peerCertDer) throws Exception {
        final var builder =
                SslContextBuilder.forClient().trustManager(new ClientSideTrustManager(Certs.parse(peerCertDer)));
        if (leafKeyManager != null) {
            builder.keyManager(wrapAsFactory(leafKeyManager));
        }
        // configure() applies the gRPC/HTTP-2 ALPN settings appropriate for the chosen provider; forcing
        // JDK keeps the custom trust manager portable (avoids tcnative-specific verification quirks).
        GrpcSslContexts.configure(builder, SslProvider.JDK);
        return builder.build();
    }

    /**
     * Wraps {@code keyManager} in a {@link KeyManagerFactory} whose {@code getKeyManagers()} delegates
     * to it. This is the only way to supply a custom {@link javax.net.ssl.KeyManager} to Netty's
     * {@link SslContextBuilder} without abandoning the JDK provider.
     *
     * <p>The {@code engineInit} methods are empty because {@code SslContextBuilder} only calls
     * {@code engineGetKeyManagers()}; the wrapped key manager is already initialised at construction.
     */
    private static KeyManagerFactory wrapAsFactory(final LeafKeyManager keyManager) throws NoSuchAlgorithmException {
        final var spi = new KeyManagerFactorySpi() {
            @Override
            protected void engineInit(final KeyStore ks, final char[] password) {}

            @Override
            protected void engineInit(final ManagerFactoryParameters spec) {}

            @Override
            protected KeyManager[] engineGetKeyManagers() {
                return new KeyManager[] {keyManager};
            }
        };
        final var alg = KeyManagerFactory.getDefaultAlgorithm();
        return new KeyManagerFactory(spi, KeyManagerFactory.getInstance(alg).getProvider(), alg) {};
    }

    /**
     * Returns {@code true} when the gRPC failure warrants evicting the cached channel.
     *
     * <p>We only evict on {@code DEADLINE_EXCEEDED}: a timed-out call means the peer was
     * reachable but slow, so the channel itself may be stuck. Evicting lets the next call
     * open a fresh connection.
     *
     * <p>We intentionally do <b>not</b> evict on {@code UNAVAILABLE}: this code is typically
     * returned immediately when DNS resolution fails (e.g. the peer's Service hasn't registered
     * yet at startup). Evicting and immediately recreating the channel triggers a tight DNS-retry
     * loop that floods the logs and saturates the DNS server with queries. Instead we let
     * grpc-java's built-in exponential-backoff reconnect logic handle {@code UNAVAILABLE} — the
     * channel will retry with increasing delays and succeed once the peer becomes routable.
     */
    private static boolean isTransportError(final StatusRuntimeException e) {
        final Status.Code code = e.getStatus().getCode();
        return code == Status.Code.DEADLINE_EXCEEDED;
    }

    /**
     * Removes the given channel from the cache (if it is still the current entry for {@code key})
     * and shuts it down immediately.
     */
    private void evictChannel(final String key, final ManagedChannel channel) {
        channelCache.remove(key, channel);
        channel.shutdownNow();
    }

    /**
     * Shuts down every cached channel and clears the cache.
     *
     * <p>Each cached {@link ManagedChannel} owns a Netty event-loop group and an HTTP/2 connection;
     * without this they are leaked when the client is discarded, and grpc-java's orphan detector
     * eventually logs a {@code SEVERE} when the channel is garbage collected un-shut-down. Call this
     * when the owning relay stops. Idempotent: a second call sees an empty cache and does nothing.
     *
     * <p>Also marks the client closed so any sync task still in flight after this returns cannot
     * {@link #channelFor build} and cache a new channel that would outlive the drain.
     */
    @Override
    public void close() {
        closed = true;
        int count = 0;
        for (final var entry : channelCache.entrySet()) {
            if (channelCache.remove(entry.getKey(), entry.getValue())) {
                entry.getValue().shutdownNow();
                count++;
            }
        }
        if (count > 0) {
            LOGGER.info("ClprEndpointClient closed {} peer channel(s)", count);
        }
    }

    /** Returns a snapshot of the currently cached channels. Visible for testing. */
    List<ManagedChannel> cachedChannels() {
        return List.copyOf(channelCache.values());
    }
}
