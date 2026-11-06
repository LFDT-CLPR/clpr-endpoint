// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.clpr.relay.evm.ByteUtils.ZERO_HASH;
import static org.hiero.clpr.relay.evm.EvmErrorParser.CLPR_REPLAY_DETECTED;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprQueueMetadata;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import org.hiero.clpr.relay.core.EvmSubmissionConfig;
import org.hiero.clpr.relay.core.ParsedBundle;
import org.hiero.clpr.relay.core.metrics.PrometheusExporter;
import org.hiero.clpr.relay.core.metrics.SimpleMetrics;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.model.BlockHeader;
import org.hiero.clpr.relay.evm.testfixtures.TestEvmJsonRpcClient;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link AccountTransactionSubmitter}.
 *
 * <p>Most paths are driven through the package-visible {@link AccountTransactionSubmitter#process}
 * method so the assertions are deterministic without waiting on the worker thread; one test exercises the real bounded
 * queue + worker end-to-end with a latch to prove FIFO ordering and a fresh nonce read per request.
 */
class AccountTransactionSubmitterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String CONTRACT = "0x1111111111111111111111111111111111111111";
    private static final String TX_HASH = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab";
    private static final long GWEI = 1_000_000_000L;
    private static final long CHAIN_ID = 1L;

    /** Anvil dev account 0 private key — produces a deterministic test signer. */
    private static final String TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    /** No-op sleeper so tests do not block on receipt polling. */
    private static final Sleeper NO_SLEEP = ms -> {};

    private static EthSigner testSigner() {
        return new EthSigner(TEST_PRIVATE_KEY);
    }

    private static Eip1559GasStrategy defaultGas(final EvmJsonRpcClient rpc) {
        return new Eip1559GasStrategy(rpc, Long.MAX_VALUE, 2L * GWEI, 1.2);
    }

    private static AccountTransactionSubmitter submitter(final EvmJsonRpcClient rpc, final SimpleMetrics metrics) {
        return submitter(rpc, metrics, 256);
    }

    private static AccountTransactionSubmitter submitter(
            final EvmJsonRpcClient rpc, final SimpleMetrics metrics, final int queueCapacity) {
        return new AccountTransactionSubmitter(
                rpc, testSigner(), CHAIN_ID, "test-net", defaultGas(rpc), metrics, queueCapacity, NO_SLEEP);
    }

    /** A well-formed 32-byte channel id (submitBundle ABI-encoding requires the full width). */
    private static byte[] conn32() {
        final byte[] b = new byte[32];
        b[0] = 0x0a;
        return b;
    }

    private static String label(final byte[] connId) {
        return HexFormat.of().formatHex(connId);
    }

    private static AccountTransactionSubmitter.SubmitRequest request(final byte[] proof, final int messageCount) {
        final byte[] connId = conn32();
        return new AccountTransactionSubmitter.SubmitRequest(
                CONTRACT, Bytes.wrap(connId), Bytes.wrap(proof), messageCount, label(connId), null);
    }

    private static Bytes channel() {
        return Bytes.wrap(conn32());
    }

    /** A distinct 32-byte channel id (first byte {@code tag}) for multi-channel tests. */
    private static Bytes channelWithTag(final byte tag) {
        final byte[] b = new byte[32];
        b[0] = tag;
        return Bytes.wrap(b);
    }

    /** A unique 32-byte connection id from an integer index (supports up to 65536 connections). */
    private static ClprChannel channelWithIndex(final int index) {
        final byte[] b = new byte[32];
        b[0] = (byte) (index >> 8);
        b[1] = (byte) index;
        return ClprChannel.newBuilder().channelId(Bytes.wrap(b)).build();
    }

    /**
     * A unique non-empty ACTIVE bundle for the given (connection, bundle) index pair. Proof bytes encode both indices
     * so the dedup guard never suppresses distinct bundles. next=bundleIdx+2, acked=bundleIdx+1 clears the empty-bundle
     * guard.
     */
    private static ParsedBundle bundleForIndex(final int connIdx, final int bundleIdx) {
        final int code = (connIdx << 4) | (bundleIdx & 0xf);
        final byte[] proof = {(byte) (code >> 24), (byte) (code >> 16), (byte) (code >> 8), (byte) code};
        return bundle(Bytes.wrap(proof), 1, bundleIdx + 2L, bundleIdx + 1L);
    }

    /** Non-empty ACTIVE bundle (next=2, acked=1) that clears the submit() empty-bundle guard. */
    private static ParsedBundle bundle(final Bytes proof, final int messageCount) {
        return bundle(proof, messageCount, 2L, 1L);
    }

    private static ParsedBundle bundle(final Bytes proof, final int messageCount, final long next, final long acked) {
        return bundle(proof, messageCount, next, acked, ClprChannelStatus.ACTIVE);
    }

    private static ParsedBundle bundle(
            final Bytes proof,
            final int messageCount,
            final long next,
            final long acked,
            final ClprChannelStatus status) {
        final ClprQueueMetadata meta = ClprQueueMetadata.newBuilder()
                .nextMessageId(next)
                .receivedMessageId(acked)
                .status(status)
                .build();
        final var messages = new java.util.ArrayList<ClprMessagePayload>();
        for (int i = 0; i < messageCount; i++) {
            messages.add(ClprMessagePayload.DEFAULT);
        }
        return new ParsedBundle(meta, messages, proof);
    }

    /** Builds a Jackson receipt node with the given status string (e.g. "0x1" or "0x0"). */
    private static ObjectNode receipt(final String status) {
        final ObjectNode r = MAPPER.createObjectNode();
        r.put("status", status);
        r.put("transactionHash", TX_HASH);
        return r;
    }

    // -------------------------------------------------------------------------
    // Stub RPC client covering every JSON-RPC method the submitter + gas strategy touch.
    // -------------------------------------------------------------------------

    @Test
    void process_previewReverts_skipsWithoutSending() {
        final var rpc = new StubRpc();
        rpc.previewReverts = true;
        rpc.previewRevertMessage = "execution reverted: " + CLPR_REPLAY_DETECTED;
        final var metrics = new SimpleMetrics();

        submitter(rpc, metrics).process(request(new byte[] {0x01}, 1));

        assertThat(rpc.sendRawCalls.get()).isZero();
        assertThat(PrometheusExporter.render(metrics))
                .contains("clpr_sync_bundle_skipped{channel_id=\"" + label(conn32())
                        + "\",peer=\"anonymous\",reason=\"rejected\"} 1");
    }

    @Test
    void process_happyPath_sendsEip1559AndCountsSuccess() {
        final var rpc = new StubRpc();
        final var metrics = new SimpleMetrics();

        submitter(rpc, metrics).process(request(new byte[] {0x01, 0x02, 0x03}, 2));

        assertThat(rpc.sendRawCalls.get()).isEqualTo(1);
        // The nonce is read once, from the latest (mined) count.
        assertThat(rpc.txCountCalls.get()).isEqualTo(1);
        // The transaction is sized once via eth_estimateGas.
        assertThat(rpc.estimateGasCalls.get()).isEqualTo(1);
        final String out = PrometheusExporter.render(metrics);
        final String conn = label(conn32());
        assertThat(out).contains("clpr_sync_bundle_submissions{channel_id=\"" + conn + "\",peer=\"anonymous\"} 1");
        assertThat(out).contains("clpr_sync_bundle_messages_submitted{channel_id=\"" + conn + "\"} 2");
        assertThat(out).contains("clpr_sync_bundle_bytes_submitted{channel_id=\"" + conn + "\"} 3");
        assertThat(out).contains("clpr_evm_tx_submissions{channel_id=\"" + conn + "\"} 1");
    }

    // -------------------------------------------------------------------------
    // process(): preview gate
    // -------------------------------------------------------------------------

    @Test
    void process_revertedReceipt_countsReverted() {
        final var rpc = new StubRpc();
        rpc.receiptStatus = "0x0";
        final var metrics = new SimpleMetrics();

        submitter(rpc, metrics).process(request(new byte[] {0x01}, 1));

        assertThat(rpc.sendRawCalls.get()).isEqualTo(1);
        final String out = PrometheusExporter.render(metrics);
        final String conn = label(conn32());
        assertThat(out).contains("clpr_sync_bundle_reverted{channel_id=\"" + conn + "\",peer=\"anonymous\"} 1");
        assertThat(out).contains("clpr_evm_tx_reverts{channel_id=\"" + conn + "\"} 1");
    }

    // -------------------------------------------------------------------------
    // process(): happy path + revert detection
    // -------------------------------------------------------------------------

    @Test
    void process_notMinedThenMined_reSendsSameNonceAndCountsSuccessOnce() {
        final var rpc = new StubRpc();
        // One full poll round returns no receipt, forcing exactly one re-send.
        rpc.nullReceiptsBeforeSuccess = EvmSubmissionConfig.RECEIPT_POLL_ATTEMPTS;
        final var metrics = new SimpleMetrics();

        submitter(rpc, metrics).process(request(new byte[] {0x01}, 1));

        // The transaction was re-sent, but the nonce was read exactly once (never advanced).
        assertThat(rpc.sendRawCalls.get()).isEqualTo(2);
        assertThat(rpc.txCountCalls.get()).isEqualTo(1);
        final String conn = label(conn32());
        assertThat(PrometheusExporter.render(metrics))
                .contains("clpr_sync_bundle_submissions{channel_id=\"" + conn + "\",peer=\"anonymous\"} 1");
    }

    @Test
    void process_transientSendError_retriesThenSucceeds() {
        final var rpc = new StubRpc();
        rpc.sendErrorsBeforeSuccess = 1;
        rpc.sendErrorMessage = "HTTP 503 after 3 retries"; // transient — must retry, not skip
        final var metrics = new SimpleMetrics();

        submitter(rpc, metrics).process(request(new byte[] {0x01}, 1));

        assertThat(rpc.sendRawCalls.get()).isEqualTo(2);
        assertThat(rpc.txCountCalls.get()).isEqualTo(1);
        final String conn = label(conn32());
        assertThat(PrometheusExporter.render(metrics))
                .contains("clpr_sync_bundle_submissions{channel_id=\"" + conn + "\",peer=\"anonymous\"} 1");
    }

    // -------------------------------------------------------------------------
    // process(): re-send loop — never skip on uncertainty, one nonce per request
    // -------------------------------------------------------------------------

    @Test
    void process_definiteSendRejection_skipsAndCountsFailure() {
        final var rpc = new StubRpc();
        rpc.sendErrorsBeforeSuccess = 99; // always
        rpc.sendErrorMessage = "Intrinsic gas exceeds gas limit"; // definite — safe to skip
        final var metrics = new SimpleMetrics();

        submitter(rpc, metrics).process(request(new byte[] {0x01}, 1));

        // One send attempt, then a definite skip — no re-send, no success.
        assertThat(rpc.sendRawCalls.get()).isEqualTo(1);
        final String out = PrometheusExporter.render(metrics);
        final String conn = label(conn32());
        assertThat(out).contains("clpr_evm_tx_failures{channel_id=\"" + conn + "\",reason=\"send\"} 1");
        assertThat(out).doesNotContain("clpr_sync_bundle_submissions{channel_id=\"" + conn + "\"}");
    }

    @Test
    void process_underpriced_raisesFeeOnceThenSucceeds() {
        final var rpc = new StubRpc();
        rpc.sendErrorsBeforeSuccess = 1;
        rpc.sendErrorMessage = "replacement transaction underpriced"; // must bump, then succeed
        final var metrics = new SimpleMetrics();

        submitter(rpc, metrics).process(request(new byte[] {0x01}, 1));

        assertThat(rpc.sendRawCalls.get()).isEqualTo(2);
        assertThat(rpc.txCountCalls.get()).isEqualTo(1);
        final String conn = label(conn32());
        assertThat(PrometheusExporter.render(metrics))
                .contains("clpr_sync_bundle_submissions{channel_id=\"" + conn + "\",peer=\"anonymous\"} 1");
    }

    @Test
    void process_baseFeeRisesAfterPooling_repricesToLiveMarketFee() {
        final var rpc = new StubRpc();
        rpc.baseFeePerGas = 10L * GWEI; // initial fee is priced off 10 gwei
        rpc.raisedBaseFeePerGas = 100L * GWEI; // then the market jumps after the first send
        // One full poll round yields no receipt, so the worker re-reads the (now higher) fee and
        // re-sends before it mines on the second round.
        rpc.nullReceiptsBeforeSuccess = EvmSubmissionConfig.RECEIPT_POLL_ATTEMPTS;
        final var metrics = new SimpleMetrics();

        submitter(rpc, metrics).process(request(new byte[] {0x01}, 1));

        // Re-sent once at the same nonce, but the replacement is a DISTINCT raw tx — re-signed at the
        // higher live fee rather than the original tx re-broadcast verbatim.
        assertThat(rpc.sendRawCalls.get()).isEqualTo(2);
        assertThat(rpc.txCountCalls.get()).isEqualTo(1);
        assertThat(rpc.sentRawTxs).doesNotHaveDuplicates();
        final String conn = label(conn32());
        assertThat(PrometheusExporter.render(metrics))
                .contains("clpr_sync_bundle_submissions{channel_id=\"" + conn + "\",peer=\"anonymous\"} 1");
    }

    @Test
    void process_baseFeeSteady_reSendsSameTxVerbatim() {
        final var rpc = new StubRpc();
        rpc.baseFeePerGas = 10L * GWEI;
        rpc.raisedBaseFeePerGas = null; // no market move — re-price must be a no-op
        rpc.nullReceiptsBeforeSuccess = EvmSubmissionConfig.RECEIPT_POLL_ATTEMPTS;
        final var metrics = new SimpleMetrics();

        submitter(rpc, metrics).process(request(new byte[] {0x01}, 1));

        // Two sends, but the SAME signed tx both times (deterministic signer): no needless re-price.
        assertThat(rpc.sendRawCalls.get()).isEqualTo(2);
        assertThat(rpc.sentRawTxs).hasSize(2);
        assertThat(rpc.sentRawTxs.get(0)).isEqualTo(rpc.sentRawTxs.get(1));
    }

    @Test
    void process_feeCapExceededWithNothingPooled_failsFast() {
        final var rpc = new StubRpc();
        rpc.sendErrorsBeforeSuccess = 99; // always
        rpc.sendErrorMessage = "Transaction fee cap exceeded"; // fee too high, nothing affordable pooled
        final var metrics = new SimpleMetrics();

        submitter(rpc, metrics).process(request(new byte[] {0x01}, 1));

        // No affordable version ever pooled → fail fast rather than spin forever.
        assertThat(rpc.sendRawCalls.get()).isEqualTo(1);
        final String out = PrometheusExporter.render(metrics);
        final String conn = label(conn32());
        assertThat(out).contains("clpr_evm_tx_failures{channel_id=\"" + conn + "\",reason=\"send\"} 1");
        assertThat(out).doesNotContain("clpr_sync_bundle_submissions{channel_id=\"" + conn + "\"}");
    }

    @Test
    void process_neverMines_abandonsAfterSafetyValveInsteadOfWedging() {
        final var rpc = new StubRpc();
        rpc.nullReceiptsBeforeSuccess = Integer.MAX_VALUE; // receipt never appears → never a verdict
        final var metrics = new SimpleMetrics();

        // Must terminate (not spin forever) and record an abandonment so the sync loop can re-drive.
        submitter(rpc, metrics).process(request(new byte[] {0x01}, 1));

        final String out = PrometheusExporter.render(metrics);
        final String conn = label(conn32());
        assertThat(out).contains("clpr_evm_tx_failures{channel_id=\"" + conn + "\",reason=\"abandoned\"} 1");
        assertThat(out).doesNotContain("clpr_sync_bundle_submissions{channel_id=\"" + conn + "\"}");
    }

    @Test
    void process_nonceConsumedByUntrackedTx_terminatesWhenOnChainNonceAdvances() {
        final var rpc = new StubRpc();
        rpc.sendErrorsBeforeSuccess = 99; // always
        rpc.sendErrorMessage = "nonce too low"; // NONCE_ALREADY_MINED
        rpc.nullReceiptsBeforeSuccess = Integer.MAX_VALUE; // the mined tx is not one of ours
        rpc.laterNonce = 5L; // on-chain nonce has moved past ours (0) → slot definitively consumed
        final var metrics = new SimpleMetrics();

        submitter(rpc, metrics).process(request(new byte[] {0x01}, 1));

        // One attempt, then it confirms the nonce advanced and stops — no infinite "nonce too low"
        // loop, and not an abandonment.
        assertThat(rpc.sendRawCalls.get()).isEqualTo(1);
        final String out = PrometheusExporter.render(metrics);
        final String conn = label(conn32());
        assertThat(out).doesNotContain("reason=\"abandoned\"");
        assertThat(out).doesNotContain("clpr_sync_bundle_submissions{channel_id=\"" + conn + "\"}");
    }

    @Test
    void submit_queueFull_dropsAndCounts() {
        final var rpc = new StubRpc();
        final var metrics = new SimpleMetrics();
        // Per-channel capacity 1, worker never started → the first submit (same channel) fills
        // that channel's queue and the second is dropped.
        final var sub = submitter(rpc, metrics, 1);

        // Two non-empty bundles for the same channel; the second is dropped on queue-full.
        sub.submit(CONTRACT, channel(), bundle(Bytes.wrap(new byte[] {0x01}), 1, 2L, 1L), null);
        sub.submit(CONTRACT, channel(), bundle(Bytes.wrap(new byte[] {0x02}), 1, 3L, 2L), null);

        assertThat(PrometheusExporter.render(metrics))
                .contains("clpr_evm_tx_queue_dropped{channel_id=\"" + label(conn32()) + "\"} 1");
        // Nothing was sent — the worker was never started.
        assertThat(rpc.sendRawCalls.get()).isZero();
    }

    @Test
    void submit_identicalBundleAlreadyQueued_isSuppressed() {
        final var rpc = new StubRpc();
        final var metrics = new SimpleMetrics();
        // Ample capacity, worker not started → both would enqueue if not deduped.
        final var sub = submitter(rpc, metrics, 1024);
        final String conn = label(conn32());

        final Bytes proof = Bytes.wrap(new byte[] {0x01, 0x02, 0x03});
        sub.submit(CONTRACT, channel(), bundle(proof, 1, 2L, 1L), null);
        sub.submit(CONTRACT, channel(), bundle(proof, 1, 2L, 1L), null); // identical bytes → suppressed
        // A bundle with distinct bytes for the same channel is NOT suppressed.
        sub.submit(CONTRACT, channel(), bundle(Bytes.wrap(new byte[] {0x09}), 1, 3L, 2L), null);

        final String out = PrometheusExporter.render(metrics);
        assertThat(out).contains("clpr_evm_tx_queue_deduped{channel_id=\"" + conn + "\"} 1");
        // The re-offer was suppressed at enqueue, not dropped for a full queue.
        assertThat(out).doesNotContain("clpr_evm_tx_queue_dropped{channel_id=\"" + conn + "\"}");
        assertThat(rpc.sendRawCalls.get()).isZero();
    }

    // -------------------------------------------------------------------------
    // submit(): queue-full drop
    // -------------------------------------------------------------------------

    @Test
    void submit_duplicateOfNonTailBundle_isSuppressed() {
        final var rpc = new StubRpc();
        final var metrics = new SimpleMetrics();
        final var sub = submitter(rpc, metrics, 1024);
        final String conn = label(conn32());

        // Re-offer matches a bundle at the HEAD (not the tail): the hash-set index must catch any
        // queued entry, not just the most recent one.
        final Bytes first = Bytes.wrap(new byte[] {0x01});
        sub.submit(CONTRACT, channel(), bundle(first, 1, 2L, 1L), null);
        sub.submit(CONTRACT, channel(), bundle(Bytes.wrap(new byte[] {0x02}), 1, 3L, 2L), null);
        sub.submit(CONTRACT, channel(), bundle(first, 1, 2L, 1L), null); // duplicate of the head → suppressed

        final String out = PrometheusExporter.render(metrics);
        assertThat(out).contains("clpr_evm_tx_queue_deduped{channel_id=\"" + conn + "\"} 1");
        assertThat(out).doesNotContain("clpr_evm_tx_queue_dropped{channel_id=\"" + conn + "\"}");
    }

    // -------------------------------------------------------------------------
    // submit(): exact-duplicate suppression at enqueue
    // -------------------------------------------------------------------------

    @Test
    void submit_identicalBundleAfterDrain_enqueuesAgain() throws Exception {
        final var rpc = new StubRpc();
        rpc.sendLatch = new CountDownLatch(2);
        final var metrics = new SimpleMetrics();
        final var sub = submitter(rpc, metrics, 1024);
        final String conn = label(conn32());
        sub.start();
        try {
            // Same bundle submitted, drained by the worker, then submitted again: once the first copy
            // has left the queue the identical re-offer is NOT a duplicate and must enqueue + send.
            sub.submit(CONTRACT, channel(), bundle(Bytes.wrap(new byte[] {0x01}), 1, 2L, 1L), null);
            Thread.sleep(150); // let the worker drain the first before the second arrives
            sub.submit(CONTRACT, channel(), bundle(Bytes.wrap(new byte[] {0x01}), 1, 2L, 1L), null);
            assertThat(rpc.sendLatch.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            sub.stop();
        }
        // Both were sent (not deduped across the drain boundary).
        assertThat(rpc.sendRawCalls.get()).isEqualTo(2);
        assertThat(PrometheusExporter.render(metrics))
                .doesNotContain("clpr_evm_tx_queue_deduped{channel_id=\"" + conn + "\"}");
    }

    @Test
    void submit_emptyActiveInitialBundle_skipped() {
        final var rpc = new StubRpc();
        final var metrics = new SimpleMetrics();
        // next=1, acked=0, ACTIVE is the initial post-completeChannel state — nothing to deliver.
        submitter(rpc, metrics)
                .submit(
                        CONTRACT,
                        channel(),
                        bundle(Bytes.wrap(new byte[] {0x01}), 1, 1L, 0L, ClprChannelStatus.ACTIVE),
                        null);

        assertThat(PrometheusExporter.render(metrics))
                .contains("clpr_sync_bundle_skipped{channel_id=\"" + label(conn32())
                        + "\",peer=\"anonymous\",reason=\"empty\"} 1");
    }

    @Test
    void submit_statusOnlyCloseAtInitialCounters_isNotDroppedAsEmpty() throws Exception {
        final var rpc = new StubRpc();
        rpc.sendLatch = new CountDownLatch(1);
        final var metrics = new SimpleMetrics();
        final var sub = submitter(rpc, metrics);
        sub.start();
        try {
            // A status-only close rides at next=1/acked=0 on a never-used channel but conveys
            // CLOSING — it must be submitted (the empty-bundle guard is ACTIVE-only), not dropped.
            sub.submit(
                    CONTRACT,
                    channel(),
                    bundle(Bytes.wrap(new byte[] {0x01}), 0, 1L, 0L, ClprChannelStatus.CLOSING),
                    null);

            assertThat(rpc.sendLatch.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(rpc.sendRawCalls.get()).isEqualTo(1);
            assertThat(PrometheusExporter.render(metrics))
                    .doesNotContain("clpr_sync_bundle_skipped{channel_id=\"" + label(conn32())
                            + "\",peer=\"anonymous\",reason=\"empty\"}");
        } finally {
            sub.stop();
        }
    }

    // -------------------------------------------------------------------------
    // submit(): empty-bundle guard
    // -------------------------------------------------------------------------

    @Test
    void worker_processesInFifoOrderWithFreshNoncePerRequest() throws Exception {
        final var rpc = new StubRpc();
        rpc.sendLatch = new CountDownLatch(2);
        final var metrics = new SimpleMetrics();
        final var sub = submitter(rpc, metrics);
        sub.start();
        try {
            // Distinct proofs so the previews are distinguishable and the enqueue order is checkable.
            sub.submit(CONTRACT, channel(), bundle(Bytes.wrap(new byte[] {0x0a, 0x0a, 0x0a, 0x0a}), 1, 2L, 1L), null);
            sub.submit(CONTRACT, channel(), bundle(Bytes.wrap(new byte[] {0x0b, 0x0b, 0x0b, 0x0b}), 1, 3L, 2L), null);

            assertThat(rpc.sendLatch.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(rpc.sendRawCalls.get()).isEqualTo(2);
            // One latest-nonce read per request (no cache).
            assertThat(rpc.txCountCalls.get()).isEqualTo(2);
            // Previews (and therefore submissions) happened in enqueue order.
            assertThat(rpc.previewDataAtPending).hasSize(2);
            assertThat(rpc.previewDataAtPending.get(0)).contains("0a0a0a0a");
            assertThat(rpc.previewDataAtPending.get(1)).contains("0b0b0b0b");
        } finally {
            sub.stop();
        }
    }

    @Test
    void worker_roundRobinsAcrossChannelsSoNoneStarves() throws Exception {
        final var rpc = new StubRpc();
        rpc.sendLatch = new CountDownLatch(3);
        final var metrics = new SimpleMetrics();
        final var sub = submitter(rpc, metrics);
        final var connA = channel(); // id first byte 0x0a
        final var connB = channelWithTag((byte) 0x0b);
        // Enqueue A1, A2 (same conn) then B1 BEFORE starting the worker. FIFO would drain A1, A2, B1
        // and starve B behind A; round-robin must instead drain A1, B1, A2.
        sub.submit(
                CONTRACT,
                connA,
                bundle(Bytes.wrap(new byte[] {(byte) 0xa1, (byte) 0xa1, (byte) 0xa1, (byte) 0xa1}), 1, 2L, 1L),
                null);
        sub.submit(
                CONTRACT,
                connA,
                bundle(Bytes.wrap(new byte[] {(byte) 0xa2, (byte) 0xa2, (byte) 0xa2, (byte) 0xa2}), 1, 3L, 2L),
                null);
        sub.submit(
                CONTRACT,
                connB,
                bundle(Bytes.wrap(new byte[] {(byte) 0xb1, (byte) 0xb1, (byte) 0xb1, (byte) 0xb1}), 1, 2L, 1L),
                null);
        sub.start();
        try {
            assertThat(rpc.sendLatch.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(rpc.previewDataAtPending).hasSize(3);
            assertThat(rpc.previewDataAtPending.get(0)).contains("a1a1a1a1");
            assertThat(rpc.previewDataAtPending.get(1)).contains("b1b1b1b1");
            assertThat(rpc.previewDataAtPending.get(2)).contains("a2a2a2a2");
        } finally {
            sub.stop();
        }
    }

    // -------------------------------------------------------------------------
    // Real queue + worker thread: FIFO ordering and a fresh nonce per request.
    // -------------------------------------------------------------------------

    /**
     * Regression test for the nonce-collision bug fixed in PR #283 (issue #258).
     *
     * <p>1000 connections all share one {@link AccountTransactionSubmitter} (same signing key, same
     * network — the production shape via {@code LocalNetworkAdapter.accountSubmitterFor}). Each submits
     * {@code bundlesPerConnection} distinct bundles. The single serial worker must read a fresh {@code latest} nonce
     * before every transaction and confirm each one before moving to the next, so every nonce it reads must be unique.
     * Any concurrent nonce read — which was the pre-fix bug — would produce a duplicate and cause a node rejection on
     * the real chain.
     *
     * <p>{@link NonceProbingRpc} models {@code latest} the way a real node does — a pure read of the mined count that
     * advances only on broadcast — so two overlapping readers genuinely observe the same value. Three independent
     * assertions then pin the invariant: the read/broadcast critical section is never entered twice at once, every
     * observed count is distinct, and the nonces decoded back off the signed wire bytes are exactly
     * {@code 0..totalBundles-1} with no repeats and no gaps.
     */
    @ParameterizedTest(name = "bundlesPerConnection={0}")
    @ValueSource(ints = {1, 4, 8})
    void nonce_isNeverReusedAcrossConnections_highConcurrency(final int bundlesPerConnection) throws Exception {
        final int connectionCount = 1_000;
        final int totalBundles = connectionCount * bundlesPerConnection;

        final var rpc = new NonceProbingRpc();
        rpc.sendLatch = new CountDownLatch(totalBundles);
        final var metrics = new SimpleMetrics();
        final var sub = submitter(rpc, metrics); // default capacity 256 >= max(8)

        sub.start();
        try {
            for (int c = 0; c < connectionCount; c++) {
                final var channel = channelWithIndex(c);
                for (int b = 0; b < bundlesPerConnection; b++) {
                    sub.submit(CONTRACT, channel.channelId(), bundleForIndex(c, b), null);
                }
            }

            assertThat(rpc.sendLatch.await(120, TimeUnit.SECONDS)).isTrue();

            // One broadcast and one latest-nonce read per bundle — the serial worker never batches.
            assertThat(rpc.sendRawCalls.get()).isEqualTo(totalBundles);
            assertThat(rpc.txCountCalls.get()).isEqualTo(totalBundles);

            // The invariant the whole fix rests on: never two nonce holders at once.
            assertThat(rpc.maxInFlight.get()).isEqualTo(1);

            // Every read of the mined count returned a distinct value.
            assertThat(new HashSet<>(rpc.observedNonces)).hasSize(totalBundles);

            // Ground truth off the wire: decode what was actually signed. Concurrent readers would
            // sign duplicate nonces here regardless of how the stub answered the query.
            final List<Long> signedNonces = rpc.sentRawTxs.stream()
                    .map(AccountTransactionSubmitterTest::signedNonceOf)
                    .toList();
            assertThat(signedNonces)
                    .isEqualTo(LongStream.range(0, totalBundles).boxed().toList());
        } finally {
            sub.stop();
        }
    }

    private static class StubRpc extends TestEvmJsonRpcClient {
        final AtomicInteger sendRawCalls = new AtomicInteger(0);
        final AtomicInteger txCountCalls = new AtomicInteger(0);
        final AtomicInteger receiptCalls = new AtomicInteger(0);
        final AtomicInteger estimateGasCalls = new AtomicInteger(0);
        final AtomicInteger blockHeaderCalls = new AtomicInteger(0);
        final List<String> previewDataAtPending = new CopyOnWriteArrayList<>();
        final List<String> sentRawTxs = new CopyOnWriteArrayList<>();
        long baseFeePerGas = 10L * GWEI;
        // After the first block-header read (which sets the initial fee), return this base fee instead,
        // simulating a mid-flight base-fee rise the per-round re-price should track. Null = no rise.
        @Nullable
        Long raisedBaseFeePerGas = null;

        String receiptStatus = "0x1";
        boolean previewReverts = false;
        String previewRevertMessage = "execution reverted: ClprReplayDetected";
        // Return a null receipt for the first N receipt reads (simulates a not-yet-mined tx).
        int nullReceiptsBeforeSuccess = 0;
        // Throw on the first N sends (simulates transient/definite send errors).
        int sendErrorsBeforeSuccess = 0;
        int sendErrorCode = -32000;
        String sendErrorMessage = "boom";
        long estimatedGas = 5_000_000L;
        // Nonce returned by ethGetTransactionCount: the first read (the request's nonce) is 0; later
        // reads return this, letting a test simulate the on-chain nonce advancing past ours.
        long laterNonce = 0L;

        @Nullable
        CountDownLatch sendLatch;

        @Override
        public BlockHeader ethGetBlockHeaderByNumber(final String blockTag) {
            final long fee = blockHeaderCalls.incrementAndGet() > 1 && raisedBaseFeePerGas != null
                    ? raisedBaseFeePerGas
                    : baseFeePerGas;
            return minimalBlockHeader(BigInteger.valueOf(fee));
        }

        private BlockHeader minimalBlockHeader(final BigInteger baseFeePerGas) {
            return new BlockHeader(
                    ZERO_HASH,
                    ZERO_HASH,
                    Address.ZERO,
                    ZERO_HASH,
                    ZERO_HASH,
                    ZERO_HASH,
                    ZERO_HASH,
                    BigInteger.ZERO,
                    BigInteger.ONE,
                    BigInteger.ZERO,
                    BigInteger.ZERO,
                    BigInteger.ZERO,
                    Bytes.EMPTY,
                    ZERO_HASH,
                    ZERO_HASH,
                    baseFeePerGas,
                    ZERO_HASH,
                    BigInteger.ZERO,
                    BigInteger.ZERO,
                    ZERO_HASH,
                    ZERO_HASH,
                    ZERO_HASH);
        }

        @Override
        public long ethGetTransactionCount(final String address, final String blockTag) {
            return txCountCalls.incrementAndGet() == 1 ? 0L : laterNonce;
        }

        @Override
        public long ethEstimateGas(final String from, final String to, final String data) {
            estimateGasCalls.incrementAndGet();
            return estimatedGas;
        }

        @Override
        public String ethSendRawTransaction(final String signedTxHex) {
            final int n = sendRawCalls.incrementAndGet();
            sentRawTxs.add(signedTxHex);
            if (sendLatch != null) {
                sendLatch.countDown();
            }
            if (n <= sendErrorsBeforeSuccess) {
                throw new JsonRpcException(sendErrorCode, sendErrorMessage);
            }
            return TX_HASH;
        }

        @Override
        public @Nullable JsonNode ethGetTransactionReceipt(final String txHash) {
            if (receiptCalls.incrementAndGet() <= nullReceiptsBeforeSuccess) {
                return null;
            }
            return receipt(receiptStatus);
        }

        @Override
        public String ethCallFrom(final String from, final String to, final String data, final String blockTag) {
            if ("pending".equals(blockTag)) {
                previewDataAtPending.add(data);
                if (previewReverts) {
                    throw new JsonRpcException(-32000, previewRevertMessage);
                }
            }
            return "0x";
        }
    }

    // -------------------------------------------------------------------------
    // Nonce-collision regression: 1000 connections × {1,4,8} bundles per connection
    // -------------------------------------------------------------------------

    /**
     * A {@link StubRpc} variant that models the real {@code eth_getTransactionCount(addr, "latest")} contract: the call
     * is a <em>pure read</em> of the mined transaction count, and that count advances only when a transaction is
     * actually broadcast (and, in this stub, mines immediately).
     *
     * <p>This modelling is what gives the nonce-collision test its teeth. A stub that handed out an ever-incrementing
     * counter per call would report unique nonces however many threads asked at once, making any uniqueness assertion a
     * tautology. Here two overlapping readers both observe the same un-advanced {@link #minedNonce} and go on to sign
     * at the same slot — which is exactly the pre-fix collision.
     *
     * <p>{@link #maxInFlight} additionally records the greatest number of threads simultaneously between a nonce read
     * and its matching broadcast. The submitter's core invariant is that this never exceeds one.
     */
    private static class NonceProbingRpc extends StubRpc {

        /** Mined transaction count. Advances only on broadcast, never on read. */
        final AtomicLong minedNonce = new AtomicLong(0);

        /** Every value returned by {@code ethGetTransactionCount}, in call order. */
        final CopyOnWriteArrayList<Long> observedNonces = new CopyOnWriteArrayList<>();
        /** High-water mark of {@link #inFlight}. Must stay at one. */
        final AtomicInteger maxInFlight = new AtomicInteger(0);
        /** Threads currently between a nonce read and its broadcast. */
        private final AtomicInteger inFlight = new AtomicInteger(0);

        @Override
        public long ethGetTransactionCount(final String address, final String blockTag) {
            txCountCalls.incrementAndGet();
            maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
            final long mined = minedNonce.get();
            observedNonces.add(mined);
            return mined;
        }

        @Override
        public String ethSendRawTransaction(final String signedTxHex) {
            // The broadcast transaction mines immediately (receiptStatus "0x1"), so latest advances by
            // one. Both updates land before super() counts the latch down, so a waiting main thread
            // never observes a half-applied state.
            minedNonce.incrementAndGet();
            inFlight.decrementAndGet();
            return super.ethSendRawTransaction(signedTxHex);
        }
    }

    /**
     * Decode the {@code nonce} field of a signed EIP-1559 transaction ({@code 0x02 || rlp([chainId, nonce, …])}).
     * Reading the nonce back off the wire is the ground truth for what the submitter actually signed, independent of
     * how the stub answered the nonce query.
     */
    private static long signedNonceOf(final String rawTxHex) {
        final byte[] raw = HexFormat.of().parseHex(rawTxHex.substring(2));
        int i = 1; // skip the 0x02 transaction-type byte
        final int listPrefix = raw[i] & 0xff; // RLP list header
        i += listPrefix >= 0xf8 ? 1 + (listPrefix - 0xf7) : 1;
        i = skipRlpItem(raw, i); // field 1: chainId
        return readRlpLong(raw, i); // field 2: nonce
    }

    private static int skipRlpItem(final byte[] raw, final int i) {
        final int p = raw[i] & 0xff;
        if (p < 0x80) {
            return i + 1; // single byte encodes itself
        }
        if (p <= 0xb7) {
            return i + 1 + (p - 0x80); // short string
        }
        throw new IllegalArgumentException("unexpected RLP prefix 0x" + Integer.toHexString(p));
    }

    private static long readRlpLong(final byte[] raw, final int i) {
        final int p = raw[i] & 0xff;
        if (p < 0x80) {
            return p; // single byte encodes itself
        }
        if (p > 0xb7) {
            throw new IllegalArgumentException("unexpected RLP prefix 0x" + Integer.toHexString(p));
        }
        long value = 0; // 0x80 is the empty string, i.e. zero
        for (int k = 0; k < p - 0x80; k++) {
            value = (value << 8) | (raw[i + 1 + k] & 0xff);
        }
        return value;
    }
}
