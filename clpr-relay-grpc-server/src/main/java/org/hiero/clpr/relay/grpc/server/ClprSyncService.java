// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.grpc.server;

import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.GrpcStatus;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;

/**
 * {@link ServiceInterface} for the CLPR endpoint service's two-phase {@code sync} bidirectional-streaming
 * RPC. Served alongside {@link ClprInfoService} on the {@link ClprGrpcServer} port.
 */
public final class ClprSyncService implements ServiceInterface {

    /** Proto service name shared with {@link ClprInfoService}; PBJ routes per method. */
    private static final String SERVICE_NAME = "ClprEndpointService";

    private static final String FULL_NAME = "proto.ClprEndpointService";

    private static final Method SYNC_METHOD = () -> "sync";

    private final ClprSyncHandler syncHandler;

    /**
     * @param syncHandler the factory for the two-phase {@code sync} bidirectional-streaming sessions
     */
    public ClprSyncService(final ClprSyncHandler syncHandler) {
        this.syncHandler = syncHandler;
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
            throw new GrpcException(GrpcStatus.UNIMPLEMENTED, "unsupported method: " + method.name());
        }
        return openSync(opts, responses);
    }

    /**
     * Opens a bidirectional-streaming pipeline for the two-phase {@code sync} RPC.
     *
     * <p>Hands each accepted stream to a fresh {@link ClprSyncSession}. PBJ invokes the {@code method}
     * function with the outbound response pipeline and expects back the inbound request pipeline — here,
     * the session.
     */
    private Pipeline<? super Bytes> openSync(final RequestOptions opts, final Pipeline<? super Bytes> responses) {
        final var isProtobuf = opts.isProtobuf();
        return Pipelines.<ClprSyncPayload, ClprSyncPayload>bidiStreaming()
                .mapRequest(
                        bytes -> isProtobuf ? ClprSyncPayload.PROTOBUF.parse(bytes) : ClprSyncPayload.JSON.parse(bytes))
                .method(syncHandler::newSession)
                .mapResponse(reply -> isProtobuf
                        ? ClprSyncPayload.PROTOBUF.toBytes(reply)
                        : Bytes.wrap(ClprSyncPayload.JSON.toJSON(reply)))
                .respondTo(responses)
                .build();
    }
}
