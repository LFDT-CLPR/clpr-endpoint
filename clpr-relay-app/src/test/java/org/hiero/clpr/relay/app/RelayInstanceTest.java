// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RelayInstance} helpers that don't require standing up EVM infrastructure.
 * Full {@code build()} wiring is exercised in the integration layer.
 */
class RelayInstanceTest {

    private static RelayConfig.ClprServiceConfig svc(final String network, final String address) {
        return new RelayConfig.ClprServiceConfig("0xkey", network, address, false, 0L, List.of(), Map.of());
    }

    private static RelayConfig.LocalNetworkConfig net(final String id, final String jsonRpcUrl) {
        return new RelayConfig.LocalNetworkConfig(
                id,
                ProofType.QBFT,
                new RelayConfig.CommonEvmParams(jsonRpcUrl, 1L, Long.MAX_VALUE, 0L, 1.2, 1000L, 30_000L, 3),
                null,
                new RelayConfig.QbftConfig(30_000L, 5, 10),
                null);
    }

    @Test
    void dedupeLocalNetworks_dropsRepeatedId_keepingFirst() {
        final var first = net("besu1", "http://a:8545");
        final var duplicate = net("besu1", "http://b:8545"); // same id → ignored, first kept
        assertThat(RelayInstance.dedupeLocalNetworks(List.of(first, duplicate)))
                .hasSize(1)
                .containsExactly(first);
    }

    @Test
    void dedupeLocalNetworks_keepsDistinctIds() {
        final var besu1 = net("besu1", "http://a:8545");
        final var besu2 = net("besu2", "http://b:8545");
        assertThat(RelayInstance.dedupeLocalNetworks(List.of(besu1, besu2))).containsExactly(besu1, besu2);
    }

    @Test
    void dedupeClprServices_dropsRepeatedNetworkAndAddress_keepingFirst() {
        final var first = svc("besu1", "0xAAA");
        final var duplicate = svc("besu1", "0xAAA"); // same (network, address) → ignored
        assertThat(RelayInstance.dedupeClprServices(List.of(first, duplicate)))
                .hasSize(1)
                .containsExactly(first);
    }

    @Test
    void dedupeClprServices_keepsSameAddressOnDifferentNetworks() {
        final var onBesu1 = svc("besu1", "0xAAA");
        final var onBesu2 = svc("besu2", "0xAAA"); // same address, different network → distinct deployment
        assertThat(RelayInstance.dedupeClprServices(List.of(onBesu1, onBesu2))).containsExactly(onBesu1, onBesu2);
    }

    @Test
    void dedupeClprServices_isCaseSensitive() {
        // Addresses are used as provided (no case normalisation), so mixed-case is not a duplicate.
        final var lower = svc("besu1", "0xaaa");
        final var upper = svc("besu1", "0xAAA");
        assertThat(RelayInstance.dedupeClprServices(List.of(lower, upper))).containsExactly(lower, upper);
    }
}
