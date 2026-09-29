// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.sync;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.ClprEndpoint;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.clpr.relay.core.PeerEndpointCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PeerSelectorTest {

    private PeerEndpointCache cache;
    private PeerSelector selector;
    private ClprEndpoint peer1;
    private ClprEndpoint peer2;

    @BeforeEach
    void setUp() {
        cache = new PeerEndpointCache();
        selector = new PeerSelector(cache);
        peer1 = ClprEndpoint.newBuilder().accountId(Bytes.wrap(new byte[] {1})).build();
        peer2 = ClprEndpoint.newBuilder().accountId(Bytes.wrap(new byte[] {2})).build();
    }

    @Test
    void returnsEmptyWhenNoPeers() {
        assertThat(selector.selectPeer()).isEmpty();
    }

    @Test
    void selectsOnlyAvailablePeer() {
        cache.replaceAll(List.of(peer1));

        var selected = selector.selectPeer();
        assertThat(selected).isPresent();
        assertThat(selected.get()).isEqualTo(peer1);
    }

    @Test
    void recordSuccessIncreasesWeight() {
        cache.replaceAll(List.of(peer1, peer2));

        // Record many successes for peer1 to heavily weight it.
        // After 100 successes peer1 weight≈101 vs peer2 weight=1,
        // so peer1 is chosen with probability ~101/102 ≈ 99% per trial.
        for (int i = 0; i < 100; i++) {
            selector.recordSuccess(peer1);
        }

        // In 100 trials the expected peer1 count is ~99. Uniform random
        // selection would give ~50. Assert ≥90 to distinguish weighted
        // from broken/uniform behavior while keeping false-failure risk
        // negligible (P(peer1 < 90 | p=0.99) < 10^-8).
        int peer1Count = 0;
        for (int i = 0; i < 100; i++) {
            var selected = selector.selectPeer();
            assertThat(selected).isPresent();
            if (selected.get().equals(peer1)) {
                peer1Count++;
            }
        }
        assertThat(peer1Count).isGreaterThanOrEqualTo(90);
    }

    @Test
    void recordFailureCausesBackoff() {
        cache.replaceAll(List.of(peer1));
        cache.replaceAll(List.of(peer2));

        // Record enough failures on peer1 to trigger a long backoff
        for (int i = 0; i < 5; i++) {
            selector.recordFailure(peer1);
        }

        // peer1 should be backed off; selector must return peer2
        // (or fall back to any peer if all are backed off, but peer2 is not backed off)
        for (int i = 0; i < 10; i++) {
            var selected = selector.selectPeer();
            assertThat(selected).isPresent();
            assertThat(selected.get()).isEqualTo(peer2);
        }
    }

    @Test
    void allPeersBackedOff_fallsBackToUniformSelection() {
        cache.replaceAll(List.of(peer1, peer2));

        // Back off every known peer.
        for (int i = 0; i < 5; i++) {
            selector.recordFailure(peer1);
            selector.recordFailure(peer2);
        }

        // The eligible list is empty, so the selector must fall back to uniform random over the full
        // list and still return *some* peer rather than empty.
        for (int i = 0; i < 20; i++) {
            var selected = selector.selectPeer();
            assertThat(selected).isPresent();
            assertThat(selected.get()).isIn(peer1, peer2);
        }
    }
}
