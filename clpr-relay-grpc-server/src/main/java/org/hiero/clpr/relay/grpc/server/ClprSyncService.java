// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.grpc.server;

import com.hedera.hapi.node.state.clpr.ClprEndpoint;
import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.GrpcStatus;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;
import org.hiero.clpr.relay.core.PeerEndpointTlsRegistry;
import org.jspecify.annotations.Nullable;

/**
 * Data-plane {@link ServiceInterface} for the CLPR endpoint service: the two-phase {@code sync}
 * bidirectional-streaming RPC. Hosted on the plaintext ({@code plain}) and mandatory-mTLS
 * ({@code secure}) listeners. The {@code discoverEndpoints} and {@code getLedgerConfiguration} RPCs
 * are served on a separate info listener.
 *
 * <p>On the mandatory-mTLS listener the dialer has already been proven to hold a roster-registered
 * certificate; each accepted stream resolves <em>which</em> registered endpoint that is from the client
 * leaf certificate (via {@link PeerEndpointTlsRegistry#matchByCa}) and hands it to the session.
 */
public final class ClprSyncService implements ServiceInterface {

    /** Proto service name shared by both endpoint-service planes. */
    private static final String SERVICE_NAME = "ClprEndpointService";

    private static final String FULL_NAME = "proto.ClprEndpointService";

    private static final Method SYNC_METHOD = () -> "sync";

    private final ClprSyncHandler syncHandler;
    private final PeerEndpointTlsRegistry tlsRegistry;

    /**
     * @param syncHandler the factory for the two-phase {@code sync} bidirectional-streaming sessions
     * @param tlsRegistry global registry of peer TLS CA certificates; used to resolve the authenticated
     *                    peer from the mutual-TLS client certificate on each incoming sync stream
     */
    public ClprSyncService(final ClprSyncHandler syncHandler, final PeerEndpointTlsRegistry tlsRegistry) {
        this.syncHandler = syncHandler;
        this.tlsRegistry = tlsRegistry;
    }

    @Override
    public String serviceName() {
        return SERVICE_NAME;
    }

    @Override
    public String fullName() {
        return FULL_NAME;
    }

    @Override
    public List<Method> methods() {
        return List.of(SYNC_METHOD);
    }

    @Override
    public Pipeline<? super Bytes> open(
            final Method method, final RequestOptions opts, final Pipeline<? super Bytes> responses)
            throws GrpcException {
        if (!"sync".equals(method.name())) {
            throw new GrpcException(
                    GrpcStatus.UNIMPLEMENTED, "method not served on the sync listener: " + method.name());
        }
        return openSync(opts, responses);
    }

    /**
     * Opens a bidirectional-streaming pipeline for the two-phase {@code sync} RPC.
     *
     * <p>Resolves the authenticated peer once per RPC — on the mandatory-mTLS listener this names which
     * registered endpoint the (already-proven) dialer is; {@code null} for a plaintext dial or an mTLS
     * certificate that matches no roster entry — then hands each accepted stream to a fresh
     * {@link ClprSyncSession}. PBJ invokes the {@code method} function with the outbound response pipeline
     * and expects back the inbound request pipeline — here, the session.
     */
    private Pipeline<? super Bytes> openSync(final RequestOptions opts, final Pipeline<? super Bytes> responses) {
        final ClprEndpoint peer = peer(leafCertificate(opts)).orElse(null);
        final var isProtobuf = opts.isProtobuf();
        return Pipelines.<ClprSyncPayload, ClprSyncPayload>bidiStreaming()
                .mapRequest(
                        bytes -> isProtobuf ? ClprSyncPayload.PROTOBUF.parse(bytes) : ClprSyncPayload.JSON.parse(bytes))
                .method(out -> syncHandler.newSession(out, peer))
                .mapResponse(reply -> isProtobuf
                        ? ClprSyncPayload.PROTOBUF.toBytes(reply)
                        : Bytes.wrap(ClprSyncPayload.JSON.toJSON(reply)))
                .respondTo(responses)
                .build();
    }

    /** The client's leaf certificate from a mutual-TLS connection, or {@code null} if none was presented. */
    @Nullable
    private static X509Certificate leafCertificate(final RequestOptions opts) {
        final Optional<Certificate[]> chain = opts.remoteCertificateChain();
        if (chain.isEmpty()) {
            return null;
        }
        final Certificate[] certs = chain.get();
        return (certs.length > 0 && certs[0] instanceof X509Certificate leaf) ? leaf : null;
    }

    /**
     * Resolve the roster endpoint whose {@code tls_certificate} (an ECDSA P-384 CA) issued {@code leaf}
     * (the mutual-TLS client certificate). Empty when no certificate was presented (plaintext peer) or
     * none of the registered endpoints issued it.
     *
     * @param leaf the presented client leaf certificate, or {@code null}
     * @return the matching registered endpoint, if any
     */
    Optional<ClprEndpoint> peer(@Nullable final X509Certificate leaf) {
        return tlsRegistry.matchByCa(leaf);
    }
}
