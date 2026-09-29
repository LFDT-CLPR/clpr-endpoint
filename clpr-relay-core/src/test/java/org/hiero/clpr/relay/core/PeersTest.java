// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.ClprEndpoint;
import com.hedera.hapi.node.state.clpr.ClprServiceEndpoint;
import org.junit.jupiter.api.Test;

class PeersTest {

    @Test
    void peerLabel_withServiceEndpoint_returnsHostPort() {
        final var peer = ClprEndpoint.newBuilder()
                .serviceEndpoint(new ClprServiceEndpoint("10.0.0.1", 9545))
                .build();
        assertThat(Peers.peerLabel(peer)).isEqualTo("10.0.0.1:9545");
    }

    @Test
    void peerLabel_withoutServiceEndpoint_returnsUnknown() {
        assertThat(Peers.peerLabel(ClprEndpoint.newBuilder().build())).isEqualTo("unknown");
    }

    @Test
    void peerLabel_blankIpAddress_returnsUnknown() {
        final var peer = ClprEndpoint.newBuilder()
                .serviceEndpoint(new ClprServiceEndpoint("", 9545))
                .build();
        assertThat(Peers.peerLabel(peer)).isEqualTo("unknown");
    }
}
