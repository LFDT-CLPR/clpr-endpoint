// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.model.BlockHeader;
import org.hiero.clpr.relay.evm.testfixtures.TestEvmJsonRpcClient;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Eip1559GasStrategy}. Uses a stub {@link EvmJsonRpcClient} subclass that
 * returns a configurable {@code baseFeePerGas} from {@code eth_getBlockByNumber("latest", ...)}.
 */
class Eip1559GasStrategyTest {

    private static final long GWEI = 1_000_000_000L;
    private static final long NO_CAP = Long.MAX_VALUE;

    // -------------------------------------------------------------------------
    // Happy path
    // -------------------------------------------------------------------------

    @Test
    void happyPath_appliesBufferAndAddsPriority() {
        // baseFee = 10 gwei, multiplier 1.2, priority 2 gwei → maxFee = 12 + 2 = 14 gwei
        final var strategy =
                new Eip1559GasStrategy(TestEvmJsonRpcClient.stubWithBaseFee(10L * GWEI), NO_CAP, 2L * GWEI, 1.2);
        final var fees = strategy.computeFees();

        assertThat(fees.maxPriorityFeePerGas()).isEqualTo(2L * GWEI);
        assertThat(fees.maxFeePerGas()).isEqualTo(14L * GWEI);
    }

    @Test
    void baseFeeZero_maxFeeEqualsPriority() {
        // Dev/genesis chain: no baseFee → maxFee = 0 + priority
        final var strategy = new Eip1559GasStrategy(TestEvmJsonRpcClient.stubWithBaseFee(0L), NO_CAP, 3L * GWEI, 1.2);
        final var fees = strategy.computeFees();

        assertThat(fees.maxPriorityFeePerGas()).isEqualTo(3L * GWEI);
        assertThat(fees.maxFeePerGas()).isEqualTo(3L * GWEI);
    }

    @Test
    void baseFeeFieldMissing_treatedAsZero() {
        // Non-1559 chain: baseFeePerGas absent from the block → treated as 0
        final var strategy = new Eip1559GasStrategy(TestEvmJsonRpcClient.stubWithBaseFee(null), NO_CAP, 1L * GWEI, 1.5);
        final var fees = strategy.computeFees();

        assertThat(fees.maxFeePerGas()).isEqualTo(1L * GWEI);
    }

    @Test
    void priorityFeeZero_maxFeeIsBufferedBaseOnly() {
        final var strategy = new Eip1559GasStrategy(TestEvmJsonRpcClient.stubWithBaseFee(20L * GWEI), NO_CAP, 0L, 1.2);
        final var fees = strategy.computeFees();

        assertThat(fees.maxPriorityFeePerGas()).isEqualTo(0L);
        assertThat(fees.maxFeePerGas()).isEqualTo(24L * GWEI);
    }

    // -------------------------------------------------------------------------
    // Cap behaviour
    // -------------------------------------------------------------------------

    @Test
    void capExceeded_throws() {
        // baseFee=10gwei, multiplier=1.2 → 12gwei + 2gwei priority = 14gwei; cap at 10gwei → throw
        final var strategy =
                new Eip1559GasStrategy(TestEvmJsonRpcClient.stubWithBaseFee(10L * GWEI), 10L * GWEI, 2L * GWEI, 1.2);

        assertThatThrownBy(strategy::computeFees)
                .isInstanceOf(JsonRpcException.class)
                .hasMessageContaining("gas price cap exceeded")
                .hasMessageContaining("maxFeePerGas=14000000000")
                .hasMessageContaining("cap=10000000000");
    }

    @Test
    void capExactlyMet_succeeds() {
        // maxFee lands exactly on the cap → must not throw
        final var strategy =
                new Eip1559GasStrategy(TestEvmJsonRpcClient.stubWithBaseFee(10L * GWEI), 14L * GWEI, 2L * GWEI, 1.2);
        final var fees = strategy.computeFees();

        assertThat(fees.maxFeePerGas()).isEqualTo(14L * GWEI);
    }

    // -------------------------------------------------------------------------
    // computeFeesCapped: re-price an in-flight tx — clamp, never throw
    // -------------------------------------------------------------------------

    @Test
    void computeFeesCapped_belowCap_matchesComputeFees() {
        final var strategy =
                new Eip1559GasStrategy(TestEvmJsonRpcClient.stubWithBaseFee(10L * GWEI), NO_CAP, 2L * GWEI, 1.2);

        assertThat(strategy.computeFeesCapped().maxFeePerGas()).isEqualTo(14L * GWEI);
        assertThat(strategy.computeFeesCapped().maxPriorityFeePerGas()).isEqualTo(2L * GWEI);
    }

    @Test
    void computeFeesCapped_aboveCap_pinsAtCapInsteadOfThrowing() {
        // Same inputs that make computeFees() throw (14gwei > 10gwei cap): capped variant clamps.
        final var strategy =
                new Eip1559GasStrategy(TestEvmJsonRpcClient.stubWithBaseFee(10L * GWEI), 10L * GWEI, 2L * GWEI, 1.2);

        final var fees = strategy.computeFeesCapped();
        assertThat(fees.maxFeePerGas()).isEqualTo(10L * GWEI);
        // Priority is clamped to the (lower) capped maxFee so priority never exceeds maxFee.
        assertThat(fees.maxPriorityFeePerGas()).isLessThanOrEqualTo(fees.maxFeePerGas());
    }

    @Test
    void computeFeesCapped_higherBaseFee_yieldsHigherFee() {
        final var low = new Eip1559GasStrategy(TestEvmJsonRpcClient.stubWithBaseFee(10L * GWEI), NO_CAP, 2L * GWEI, 1.2)
                .computeFeesCapped();
        final var high = new Eip1559GasStrategy(
                        TestEvmJsonRpcClient.stubWithBaseFee(50L * GWEI), NO_CAP, 2L * GWEI, 1.2)
                .computeFeesCapped();

        assertThat(high.maxFeePerGas()).isGreaterThan(low.maxFeePerGas());
    }

    // -------------------------------------------------------------------------
    // Constructor validation
    // -------------------------------------------------------------------------

    @Test
    void rejectsNonPositiveCap() {
        assertThatThrownBy(() -> new Eip1559GasStrategy(TestEvmJsonRpcClient.stubWithBaseFee(0L), 0L, 0L, 1.2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNegativePriorityFee() {
        assertThatThrownBy(() -> new Eip1559GasStrategy(TestEvmJsonRpcClient.stubWithBaseFee(0L), NO_CAP, -1L, 1.2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonPositiveMultiplier() {
        assertThatThrownBy(() -> new Eip1559GasStrategy(TestEvmJsonRpcClient.stubWithBaseFee(0L), NO_CAP, 0L, 0.0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // -------------------------------------------------------------------------
    // Degenerate RPC / arithmetic edges
    // -------------------------------------------------------------------------

    @Test
    void nullBlockHeader_throws() {
        final EvmJsonRpcClient nullHeaderRpc = new TestEvmJsonRpcClient("http://unused") {
            @Override
            public BlockHeader ethGetBlockHeaderByNumber(final String blockTag) {
                return null;
            }
        };
        final var strategy = new Eip1559GasStrategy(nullHeaderRpc, NO_CAP, GWEI, 1.2);
        assertThatThrownBy(strategy::computeFees)
                .isInstanceOf(JsonRpcException.class)
                .hasMessageContaining("returned null for 'latest' block");
    }

    @Test
    void feeAdditionOverflow_throwsAsCapExceeded() {
        // baseFee = Long.MAX_VALUE, multiplier 1.0 → bufferedBase saturates to Long.MAX_VALUE; adding
        // a positive priority fee overflows to a negative maxFee. The overflow guard must catch this
        // rather than reporting a bogus (negative) fee.
        final var strategy =
                new Eip1559GasStrategy(TestEvmJsonRpcClient.stubWithBaseFee(Long.MAX_VALUE), NO_CAP, 1L, 1.0);
        assertThatThrownBy(strategy::computeFees)
                .isInstanceOf(JsonRpcException.class)
                .hasMessageContaining("gas price cap exceeded");
    }
}
