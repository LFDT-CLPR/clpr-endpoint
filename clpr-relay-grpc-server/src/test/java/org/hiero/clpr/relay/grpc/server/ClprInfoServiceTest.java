// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.grpc.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.pbj.runtime.grpc.GrpcException;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.junit.jupiter.api.Test;

class ClprInfoServiceTest {

    @Test
    void serviceMetadata_matchesProtoNames() {
        final var service = new ClprInfoService(null);
        assertThat(service.serviceName()).isEqualTo("ClprEndpointService");
        assertThat(service.fullName()).isEqualTo("proto.ClprEndpointService");
        assertThat(service.methods()).singleElement().satisfies(m -> assertThat(m.name())
                .isEqualTo("getLedgerConfiguration"));
    }

    @Test
    void open_getLedgerConfiguration_withNoHandler_throwsUnimplemented() {
        // No local chain target configured → the metadata RPC is deliberately UNIMPLEMENTED
        // (rather than INTERNAL) so callers don't retry-storm.
        final var service = new ClprInfoService(null);
        assertThatThrownBy(() -> service.open(() -> "getLedgerConfiguration", null, null))
                .isInstanceOf(GrpcException.class)
                .hasMessageContaining("no local chain target is configured");
    }

    @Test
    void open_unknownMethod_throwsUnimplemented() {
        // Even with a handler present, a method not served on the info plane is rejected.
        final var handler = new GetLedgerConfigurationHandler(serviceAddress -> null, CommitmentLevel.FINALIZED);
        final var service = new ClprInfoService(handler);
        assertThatThrownBy(() -> service.open(() -> "sync", null, null))
                .isInstanceOf(GrpcException.class)
                .hasMessageContaining("method not served on the info listener");
    }
}
