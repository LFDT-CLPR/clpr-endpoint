// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.grpc.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ClprGrpcServerTest {

    private static final int MAX_MSG = 1 << 20;

    private ClprGrpcServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    /** Returns a momentarily-free local port. */
    private static int freePort() throws IOException {
        try (final var s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static GetLedgerConfigurationHandler handler() {
        return new GetLedgerConfigurationHandler(
                _ -> {
                    throw new UnsupportedOperationException("not exercised");
                },
                CommitmentLevel.FINALIZED);
    }

    @Test
    void startsOnConfiguredPort() throws Exception {
        final int port = freePort();
        server = new ClprGrpcServer(port, MAX_MSG, null, handler(), "test");

        server.start();

        assertThat(server.isRunning()).isTrue();
        assertThat(server.port()).isEqualTo(port);
    }
}
