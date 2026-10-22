// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.grpc.client.testfixtures;

import com.hedera.hapi.node.state.clpr.ClprGetLedgerConfigurationRequest;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfigurationResponse;
import com.hedera.hapi.node.state.clpr.QbftLedgerConfigurationPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.logging.api.Logger;
import com.swirlds.logging.api.Loggers;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCalls;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Test-fixture gRPC client for the CLPR endpoint's {@code getLedgerConfiguration} RPC, which returns
 * chain-verifiable data.
 *
 * <p>Uses {@code grpc-netty-shaded}'s {@code NettyChannelBuilder} for outbound HTTP/2 transport and
 * standard grpc-java {@link ClientCalls} for the unary calls.
 */
public class ClprEndpointInfoClient implements AutoCloseable {

    private static final Logger LOGGER = Loggers.getLogger(ClprEndpointInfoClient.class);

    /** The gRPC service name. */
    private static final String SERVICE_NAME = "proto.ClprEndpointService";

    /**
     * Default read timeout for gRPC calls. Sent as the {@code grpc-timeout} HTTP/2 header. Sized with
     * headroom for a locally-started test server.
     */
    private static final Duration DEFAULT_READ_TIMEOUT = Duration.ofMillis(20 * 100 * 5);

    /**
     * Default maximum inbound gRPC message size in bytes (1 MiB). Mirrors the production
     * default of {@code RelayConfig.GrpcConfig.maxMessageSize}.
     */
    private static final int DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 1024 * 1024;

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

    /** The read timeout used by this instance. */
    private final Duration readTimeout;

    /** Maximum inbound gRPC message size in bytes; applied to every cached channel. */
    private final int maxInboundMessageSize;

    /** Construct with the production default read timeout and default max message size. */
    public ClprEndpointInfoClient() {
        this(DEFAULT_READ_TIMEOUT, DEFAULT_MAX_INBOUND_MESSAGE_SIZE);
    }

    /**
     * Construct with a custom read timeout. Intended for tests that run against a
     * locally-started server whose cold-start + shutdown overhead can exceed the
     * production-grade default.
     *
     * @param readTimeout the deadline applied to every gRPC call
     */
    public ClprEndpointInfoClient(final Duration readTimeout) {
        this(readTimeout, DEFAULT_MAX_INBOUND_MESSAGE_SIZE);
    }

    /**
     * Construct with the production default read timeout and a custom maximum inbound message size.
     *
     * @param maxInboundMessageSize maximum inbound gRPC message size in bytes
     */
    public ClprEndpointInfoClient(final int maxInboundMessageSize) {
        this(DEFAULT_READ_TIMEOUT, maxInboundMessageSize);
    }

    /**
     * Construct with explicit read timeout and maximum inbound message size.
     *
     * @param readTimeout           the deadline applied to every gRPC call
     * @param maxInboundMessageSize maximum inbound gRPC message size in bytes
     */
    public ClprEndpointInfoClient(final Duration readTimeout, final int maxInboundMessageSize) {
        if (maxInboundMessageSize <= 0) {
            throw new IllegalArgumentException("maxInboundMessageSize must be positive, got " + maxInboundMessageSize);
        }
        this.readTimeout = readTimeout;
        this.maxInboundMessageSize = maxInboundMessageSize;
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
     * Per-peer channel cache keyed by {@code "host:port"}.
     *
     * <p>Each {@link ManagedChannel} owns a Netty event-loop group and an HTTP/2 connection; caching
     * gives us a single long-lived connection per peer that is reused across calls. On
     * {@code DEADLINE_EXCEEDED} the stale channel is evicted so the next call opens a fresh one.
     */
    private final ConcurrentHashMap<String, ManagedChannel> channelCache = new ConcurrentHashMap<>();

    /**
     * Set once {@link #close()} begins. Published volatile so a concurrent {@link #channelFor} can
     * observe it and avoid leaving a freshly-built channel behind after the cache has been drained.
     */
    private volatile boolean closed = false;

    /** The grpc-java method descriptor for the getLedgerConfiguration RPC. */
    private static final MethodDescriptor<byte[], byte[]> GET_LEDGER_CONFIGURATION_METHOD_DESCRIPTOR =
            MethodDescriptor.<byte[], byte[]>newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "getLedgerConfiguration"))
                    .setRequestMarshaller(BYTE_MARSHALLER)
                    .setResponseMarshaller(BYTE_MARSHALLER)
                    .build();

    /**
     * Retrieves the ledger configuration from a peer.
     *
     * @param host    the peer's hostname or IP address
     * @param port    the peer's gRPC port
     * @param request the ledger configuration request
     * @return the peer's QBFT ledger configuration payload
     * @throws Exception if the call fails
     */
    public QbftLedgerConfigurationPayload getLedgerConfiguration(
            final String host, final int port, final ClprGetLedgerConfigurationRequest request) throws Exception {
        final String key = cacheKey(host, port);
        final ManagedChannel channel = channelFor(key, () -> buildChannel(host, port));
        try {
            final var callOptions =
                    CallOptions.DEFAULT.withDeadlineAfter(readTimeout.toMillis(), TimeUnit.MILLISECONDS);
            final var requestBytes = ClprGetLedgerConfigurationRequest.PROTOBUF.toBytes(request);
            final var responseBytes = ClientCalls.blockingUnaryCall(
                    channel, GET_LEDGER_CONFIGURATION_METHOD_DESCRIPTOR, callOptions, requestBytes.toByteArray());
            final var response = ClprLedgerConfigurationResponse.PROTOBUF.parse(Bytes.wrap(responseBytes));
            if (!response.hasQbft()) {
                throw new IllegalStateException(
                        "getLedgerConfiguration: peer returned a response with no qbft payload");
            }
            return response.qbftOrThrow();
        } catch (final StatusRuntimeException e) {
            if (isTransportError(e)) {
                evictChannel(key, channel);
            }
            throw e;
        }
    }

    /** Cache key for a peer channel. */
    private static String cacheKey(final String host, final int port) {
        return host + ":" + port;
    }

    /**
     * Returns a cached gRPC channel for the given key, creating one if absent. Channels are built with
     * HTTP/2 keepalives so idle connections stay alive and broken ones are detected before the next RPC
     * deadline fires.
     */
    private ManagedChannel channelFor(final String key, final Supplier<ManagedChannel> factory) {
        if (closed) {
            throw new IllegalStateException("ClprEndpointInfoClient is closed");
        }
        final ManagedChannel channel = channelCache.computeIfAbsent(key, _ -> factory.get());
        // A concurrent close() may have drained the cache after the open-check above. Re-check the
        // published flag: if close() has begun, evict and shut down the channel we just inserted so
        // it can never be orphaned. close() setting the flag happens-before its cache traversal, so
        // exactly one of {close(), this re-check} owns the shutdown — shutdownNow() is idempotent.
        if (closed) {
            channelCache.remove(key, channel);
            channel.shutdownNow();
            throw new IllegalStateException("ClprEndpointInfoClient is closed");
        }
        return channel;
    }

    /** Build a channel to the peer's gRPC port. */
    private ManagedChannel buildChannel(final String host, final int port) {
        return NettyChannelBuilder.forAddress(host, port)
                .maxInboundMessageSize(maxInboundMessageSize)
                .keepAliveTime(KEEPALIVE_TIME.toSeconds(), TimeUnit.SECONDS)
                .keepAliveTimeout(KEEPALIVE_TIMEOUT.toSeconds(), TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true)
                .usePlaintext()
                .build();
    }

    /**
     * Returns {@code true} when the gRPC failure warrants evicting the cached channel.
     *
     * <p>We only evict on {@code DEADLINE_EXCEEDED}: a timed-out call means the peer was reachable but
     * slow, so the channel itself may be stuck. We intentionally do <b>not</b> evict on
     * {@code UNAVAILABLE} — recreating the channel on an immediate DNS failure would tighten into a
     * DNS-retry loop; grpc-java's built-in exponential backoff handles that case instead.
     */
    private static boolean isTransportError(final StatusRuntimeException e) {
        return e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED;
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
     * eventually logs a {@code SEVERE} when the channel is garbage collected un-shut-down. Idempotent:
     * a second call sees an empty cache and does nothing.
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
            LOGGER.info("ClprEndpointInfoClient closed {} peer channel(s)", count);
        }
    }
}
