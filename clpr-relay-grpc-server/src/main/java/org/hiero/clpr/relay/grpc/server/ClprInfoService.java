// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.grpc.server;

import com.hedera.hapi.node.state.clpr.ClprGetLedgerConfigurationRequest;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfigurationResponse;
import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.GrpcStatus;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * {@link ServiceInterface} for the CLPR endpoint service's {@code getLedgerConfiguration} RPC. Served
 * alongside {@link ClprSyncService} on the {@link ClprGrpcServer} port.
 */
public final class ClprInfoService implements ServiceInterface {

    /** Proto service name shared with {@link ClprSyncService}; PBJ routes per method. */
    private static final String SERVICE_NAME = "ClprEndpointService";

    private static final String FULL_NAME = "proto.ClprEndpointService";

    private static final Method GET_LEDGER_CONFIGURATION_METHOD = () -> "getLedgerConfiguration";

    @Nullable
    private final GetLedgerConfigurationHandler getLedgerConfigurationHandler;

    /**
     * @param getLedgerConfigurationHandler the handler for {@code getLedgerConfiguration} RPCs,
     *                                      or {@code null} if no local chain target is configured;
     *                                      callers receive {@link GrpcStatus#UNIMPLEMENTED} in
     *                                      that case
     */
    public ClprInfoService(final GetLedgerConfigurationHandler getLedgerConfigurationHandler) {
        this.getLedgerConfigurationHandler = getLedgerConfigurationHandler;
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
        return List.of(GET_LEDGER_CONFIGURATION_METHOD);
    }

    @Override
    public Pipeline<? super Bytes> open(
            final Method method, final RequestOptions opts, final Pipeline<? super Bytes> responses)
            throws GrpcException {
        if ("getLedgerConfiguration".equals(method.name())) {
            if (getLedgerConfigurationHandler == null) {
                throw new GrpcException(
                        GrpcStatus.UNIMPLEMENTED,
                        "getLedgerConfiguration is not available: no local chain target is configured");
            }
            return openGetLedgerConfiguration(opts, responses);
        }
        throw new GrpcException(GrpcStatus.UNIMPLEMENTED, "unsupported method: " + method.name());
    }

    /**
     * Opens a unary pipeline for the {@code getLedgerConfiguration} RPC. Delegates to the injected
     * {@link GetLedgerConfigurationHandler}; the response wire type is {@link ClprLedgerConfigurationResponse}.
     */
    private Pipeline<? super Bytes> openGetLedgerConfiguration(
            final RequestOptions opts, final Pipeline<? super Bytes> responses) {
        final var isProtobuf = opts.isProtobuf();
        return Pipelines.<ClprGetLedgerConfigurationRequest, ClprLedgerConfigurationResponse>unary()
                .mapRequest(bytes -> isProtobuf
                        ? ClprGetLedgerConfigurationRequest.PROTOBUF.parse(bytes)
                        : ClprGetLedgerConfigurationRequest.JSON.parse(bytes))
                .method(getLedgerConfigurationHandler::handle)
                .mapResponse(reply -> isProtobuf
                        ? ClprLedgerConfigurationResponse.PROTOBUF.toBytes(reply)
                        : Bytes.wrap(ClprLedgerConfigurationResponse.JSON.toJSON(reply)))
                .respondTo(responses)
                .build();
    }
}
