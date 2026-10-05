// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.grpc.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import java.util.Optional;
import org.hiero.clpr.relay.core.PeerEndpointTlsRegistry;
import org.hiero.clpr.relay.core.metrics.SimpleMetrics;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ClprSyncService} — the data-plane {@code ServiceInterface} hosting the single
 * two-phase {@code sync} bidirectional-streaming RPC. Each accepted stream resolves the authenticated
 * peer from the mutual-TLS client certificate via a {@link PeerEndpointTlsRegistry}.
 */
class ClprSyncServiceTest {

    /** A handler whose channel lookup is never exercised by these method-routing tests. */
    private static ClprSyncHandler handler() {
        return new ClprSyncHandler(channelId -> Optional.empty(), new SimpleMetrics(), "test");
    }

    private static ClprSyncService service() {
        return new ClprSyncService(handler(), new PeerEndpointTlsRegistry());
    }

    @Test
    void advertisesSync() {
        assertThat(service().methods())
                .extracting(ServiceInterface.Method::name)
                .containsExactly("sync");
    }

    @Test
    void rejectsNonSyncMethods() {
        final var service = service();

        // The guard rejects the method before touching request options/responses, so null args are safe.
        assertThatThrownBy(() -> service.open(() -> "discoverEndpoints", null, null))
                .isInstanceOf(GrpcException.class);
        assertThatThrownBy(() -> service.open(() -> "getLedgerConfiguration", null, null))
                .isInstanceOf(GrpcException.class);
    }

    @Test
    void resolvesNoPeerWithoutClientCertificate() {
        // A plaintext dial (no leaf certificate) resolves to no authenticated peer; the empty registry
        // would likewise match nothing for an unknown cert.
        assertThat(service().peer(null)).isEmpty();
    }
}
