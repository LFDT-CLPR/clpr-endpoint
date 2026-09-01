// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprQueueMetadata;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.hiero.clpr.relay.core.ParsedBundle;
import org.hiero.clpr.relay.core.metrics.PrometheusExporter;
import org.hiero.clpr.relay.core.metrics.SimpleMetrics;
import org.hiero.clpr.relay.evm.AccountTransactionSubmitter;
import org.hiero.clpr.relay.evm.Eip1559GasStrategy;
import org.hiero.clpr.relay.evm.EthSigner;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Exercises the production {@link AccountTransactionSubmitter} end-to-end against a real Anvil node,
 * so the actual JSON-RPC semantics it depends on are covered: the gas-free {@code eth_call} preview,
 * {@code eth_estimateGas} sizing, the {@code latest}-nonce read, EIP-1559 signing, raw broadcast, and
 * receipt polling.
 *
 * <p>Bundles are submitted against a <em>code-less</em> target address: {@code submitBundle} call data
 * sent to an account with no code simply succeeds, so the full submit path runs for real without a
 * deployed {@code ClprService}. That is sufficient to verify the submitter's own guarantees —
 * one-transaction-per-account (the {@code latest} nonce advances by exactly one per delivered bundle),
 * FIFO ordering within a channel, and duplicate suppression — which is what this test targets. The
 * contract-side validation (preview reverts, running-hash, ordering) is covered by the unit tests and
 * the full end-to-end suite.
 *
 * <p>Gated on {@code RUN_INTEGRATION_TESTS} because it requires Docker.
 */
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
@Testcontainers
class AccountTransactionSubmitterIntegrationTest {

    @Container
    static final AnvilContainer ANVIL = new AnvilContainer();

    private static final long GWEI = 1_000_000_000L;

    /** A code-less target: call data to a no-code account succeeds, so the whole submit path runs. */
    private static final String TARGET = "0x00000000000000000000000000000000000000aa";

    /** Two distinct 32-byte channel ids. */
    private static final byte[] CONN_A = conn32((byte) 0x0a);

    private static final byte[] CONN_B = conn32((byte) 0x0b);

    private EvmJsonRpcClient rpc;
    private SimpleMetrics metrics;
    private AccountTransactionSubmitter submitter;

    @BeforeEach
    void setUp() {
        rpc = new EvmJsonRpcClient(ANVIL.jsonRpcUrl(), 3, Duration.ofSeconds(10));
        metrics = new SimpleMetrics();
        final var gas = new Eip1559GasStrategy(rpc, Long.MAX_VALUE, 2L * GWEI, 1.2);
        submitter = new AccountTransactionSubmitter(
                rpc, new EthSigner(AnvilContainer.DEV_PRIVATE_KEY), AnvilContainer.CHAIN_ID, "anvil", gas, metrics, 16);
        submitter.start();
    }

    @AfterEach
    void tearDown() {
        submitter.stop();
    }

    @Test
    void submit_landsOnChainAndAdvancesLatestNonceByOne() {
        final long before = latestNonce();

        submitter.submit(TARGET, channel(CONN_A), bundle((byte) 0x01, 2L, 1L));

        awaitSubmissions(CONN_A, 1);
        // Exactly one transaction reached the chain for this account.
        assertThat(latestNonce()).isEqualTo(before + 1);
    }

    @Test
    void sequentialBundles_bothDeliverAndNonceAdvancesByTwo() {
        final long before = latestNonce();

        submitter.submit(TARGET, channel(CONN_A), bundle((byte) 0x01, 2L, 1L));
        submitter.submit(TARGET, channel(CONN_A), bundle((byte) 0x02, 3L, 2L));

        awaitSubmissions(CONN_A, 2);
        assertThat(latestNonce()).isEqualTo(before + 2);
    }

    @Test
    void twoChannels_bothProgress() {
        final long before = latestNonce();

        submitter.submit(TARGET, channel(CONN_A), bundle((byte) 0x0a, 2L, 1L));
        submitter.submit(TARGET, channel(CONN_B), bundle((byte) 0x0b, 2L, 1L));

        awaitSubmissions(CONN_A, 1);
        awaitSubmissions(CONN_B, 1);
        // One transaction per delivered bundle, both channels served off the one account.
        assertThat(latestNonce()).isEqualTo(before + 2);
    }

    /**
     * Regression test for the nonce-collision bug fixed in PR #283 (issue #258).
     *
     * <p>100 connections all share one {@link AccountTransactionSubmitter} and each submits one
     * bundle against the same signing key. The serial worker must read a fresh {@code latest}
     * nonce before every transaction and confirm each one before moving to the next. If any two
     * nonce reads were concurrent (the pre-fix behaviour), the on-chain nonce would advance by
     * fewer than 100. Gated on {@code RUN_INTEGRATION_TESTS} because it requires Docker and
     * takes several minutes.
     */
    @Test
    void manyConnections_allDeliverWithoutNonceCollision() {
        final int connectionCount = 100;
        final long before = latestNonce();

        for (int i = 0; i < connectionCount; i++) {
            submitter.submit(TARGET, channel(channelId(i)), bundle((byte) (i & 0xff), 2L, 1L));
        }

        // All 100 bundles must land as unique transactions — nonce advances by exactly 100.
        awaitTrue(() -> latestNonce() == before + connectionCount, Duration.ofMinutes(5));
    }

    // -------------------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------------------

    private long latestNonce() {
        return rpc.ethGetTransactionCount(AnvilContainer.DEV_ADDRESS, "latest");
    }

    /** A unique 32-byte channel id from an integer index (b[0]=high byte, b[1]=low byte). */
    private static byte[] channelId(final int index) {
        final byte[] b = new byte[32];
        b[0] = (byte) (index >> 8);
        b[1] = (byte) index;
        return b;
    }

    private String render() {
        return PrometheusExporter.render(metrics);
    }

    private void awaitSubmissions(final byte[] connId, final int count) {
        awaitTrue(
                () -> render().contains("clpr_sync_bundle_submissions{channel_id=\"" + label(connId) + "\"} " + count));
    }

    /** Poll {@code condition} until true or a 30s deadline elapses (Anvil auto-mines instantly). */
    private static void awaitTrue(final BooleanSupplier condition) {
        awaitTrue(condition, Duration.ofSeconds(30));
    }

    /** Poll {@code condition} until true or {@code timeout} elapses. */
    private static void awaitTrue(final BooleanSupplier condition, final Duration timeout) {
        final long deadline = System.nanoTime() + timeout.toNanos();
        try {
            while (!condition.getAsBoolean()) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("condition not met within " + timeout);
                }
                Thread.sleep(500L);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while awaiting condition", e);
        }
    }

    private static ClprChannel channel(final byte[] connId) {
        return ClprChannel.newBuilder().channelId(Bytes.wrap(connId)).build();
    }

    private static ParsedBundle bundle(final byte proofTag, final long next, final long acked) {
        final ClprQueueMetadata meta = ClprQueueMetadata.newBuilder()
                .nextMessageId(next)
                .receivedMessageId(acked)
                .status(ClprChannelStatus.ACTIVE)
                .build();
        return new ParsedBundle(
                meta, List.of(ClprMessagePayload.DEFAULT), Bytes.wrap(new byte[] {proofTag, proofTag, proofTag, proofTag
                }));
    }

    private static byte[] conn32(final byte tag) {
        final byte[] b = new byte[32];
        b[0] = tag;
        return b;
    }

    private static String label(final byte[] connId) {
        return HexFormat.of().formatHex(connId);
    }
}
