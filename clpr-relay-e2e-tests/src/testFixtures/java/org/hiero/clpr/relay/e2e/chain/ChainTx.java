// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.chain;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigInteger;
import java.time.Duration;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.test.harness.ChainTxSubmitter;
import org.jspecify.annotations.Nullable;

/**
 * Sends a transaction and waits for a <em>successful</em> receipt.
 *
 * <p>The distinction matters more than it looks: a reverted transaction is mined and charged exactly
 * like a successful one, so code that only waits for "a receipt" happily proceeds after a failed
 * setup step and fails later somewhere unrelated. Every call here either lands or throws with the
 * receipt attached.
 */
public final class ChainTx {

    /**
     * Gas limit for framework-issued calls.
     *
     * <p>Deliberately below the EIP-7825 cap of 2^24 that Osaka would impose — the E2E genesis leaves
     * Osaka off precisely because inbound bundles can exceed it, but nothing the framework itself sends
     * (registrations, deploys, sends, transfers) comes close, and the relay's own {@code submitBundle}
     * goes through its own submitter rather than this class.
     *
     * <p>Sized further below EIP-7825 than a lone-call budget would need. Besu's EIP-1559 admission
     * check reserves {@code gasLimit × maxFeePerGas} against the sender's balance at pool-entry
     * ({@code 4M × 100 gwei = 0.4 ETH} per pending tx against a genesis-funded 8-ETH account), and
     * many-channel wiring drives dozens of sequential framework calls through the same account. A
     * 12M limit here made single pending txs sit in the pool while Besu produced empty blocks
     * ("1 pending / 0 gas") long past any receipt-wait budget; 4M cleared it while still comfortably
     * covering the largest observed call at ~1.2M actual gas.
     */
    public static final long DEFAULT_GAS_LIMIT = 4_000_000L;

    /**
     * Gas limit for a plain ETH transfer ({@link #fund}). A transfer carries no calldata, so it only
     * ever runs the recipient's {@code receive()}; even a nontrivial one comfortably fits under 100k,
     * a small fraction of {@link #DEFAULT_GAS_LIMIT}. Observed necessary: a 12,000,000 declared gas
     * limit on a transaction that only ever spends ~21-22k left it accepted into the pool (a hash
     * comes back) but never selected into a block by Besu's QBFT block-transaction-selector, even
     * across 100+ subsequent one-second blocks with nothing else contending for space — a right-sized
     * limit clears it.
     */
    public static final long PLAIN_TRANSFER_GAS_LIMIT = 100_000L;

    /** Receipt polling: 240 x 250 ms = 60 s, comfortably above the 1 s block period. */
    public static final int RECEIPT_POLL_ATTEMPTS = 240;

    public static final long RECEIPT_POLL_INTERVAL_MS = 250L;

    /** Default receipt-wait budget: {@link #RECEIPT_POLL_ATTEMPTS} x {@link #RECEIPT_POLL_INTERVAL_MS}. */
    public static final Duration DEFAULT_RECEIPT_TIMEOUT =
            Duration.ofMillis(RECEIPT_POLL_ATTEMPTS * RECEIPT_POLL_INTERVAL_MS);

    /**
     * Largest value a single transaction can carry.
     *
     * <p>{@code ChainTxSubmitter.sendRawTx} and {@code EthSigner.signEip1559Transaction} both take
     * {@code valueWei} as a {@code long}, so this is a hard ceiling on any transfer the framework
     * makes — and therefore on any balance a fault can sweep. The genesis balance is chosen to stay
     * under it; see {@code BesuQbftGenesis.DEFAULT_BALANCE_WEI}.
     */
    public static final BigInteger MAX_TRANSFERABLE_WEI = BigInteger.valueOf(Long.MAX_VALUE);

    private ChainTx() {}

    /**
     * Send a contract call and wait for it to succeed.
     *
     * @param rpc JSON-RPC client for receipt polling
     * @param submitter client-side signer for the sending account
     * @param to target contract
     * @param callData ABI-encoded call data
     * @param valueWei value to attach, {@code 0} for a plain call
     * @param what short description used in error messages
     * @return the receipt
     */
    public static JsonNode sendAndAwait(
            final EvmJsonRpcClient rpc,
            final ChainTxSubmitter submitter,
            final String to,
            final byte[] callData,
            final BigInteger valueWei,
            final String what) {
        return sendAndAwait(rpc, null, submitter, to, callData, valueWei, what, DEFAULT_RECEIPT_TIMEOUT);
    }

    /**
     * Send a contract call and wait for it to succeed, with a caller-supplied receipt-wait budget.
     *
     * <p>For callers issuing many sequential transactions back-to-back (e.g. wiring dozens of
     * channels) where a transient stall on the chain or host can push a single receipt past the
     * default 60s comfortably-above-block-period budget without anything actually being wrong.
     *
     * @param timeout how long to wait for a receipt before giving up
     */
    public static JsonNode sendAndAwait(
            final EvmJsonRpcClient rpc,
            final ChainTxSubmitter submitter,
            final String to,
            final byte[] callData,
            final BigInteger valueWei,
            final String what,
            final Duration timeout) {
        return sendAndAwait(rpc, null, submitter, to, callData, valueWei, what, timeout);
    }

    /**
     * Send a contract call and wait for it to succeed, replaying a revert to recover its reason.
     *
     * <p>A receipt records only {@code status: 0x0}; the revert data is not in it. Replaying the same
     * call with {@code eth_call} at the block it was mined in returns that data, which for a CLPR
     * custom error is the difference between "completeChannel reverted" and "the verifier rejected the
     * trust anchor". Worth the extra round trip on a path that only runs when something is broken.
     *
     * @param rpc JSON-RPC client for receipt polling
     * @param raw raw JSON-RPC caller used to replay a revert; may be {@code null} to skip the replay
     * @param submitter client-side signer for the sending account
     * @param to target contract
     * @param callData ABI-encoded call data
     * @param valueWei value to attach
     * @param what short description used in error messages
     * @return the receipt
     */
    public static JsonNode sendAndAwait(
            final EvmJsonRpcClient rpc,
            @Nullable final RawJsonRpc raw,
            final ChainTxSubmitter submitter,
            final String to,
            final byte[] callData,
            final BigInteger valueWei,
            final String what) {
        return sendAndAwait(rpc, raw, submitter, to, callData, valueWei, what, DEFAULT_RECEIPT_TIMEOUT);
    }

    /**
     * Send a contract call and wait for it to succeed, replaying a revert to recover its reason, with
     * a caller-supplied receipt-wait budget. See {@link #sendAndAwait(EvmJsonRpcClient,
     * ChainTxSubmitter, String, byte[], BigInteger, String, Duration)} for why a caller would want one.
     *
     * @param timeout how long to wait for a receipt before giving up
     */
    public static JsonNode sendAndAwait(
            final EvmJsonRpcClient rpc,
            @Nullable final RawJsonRpc raw,
            final ChainTxSubmitter submitter,
            final String to,
            final byte[] callData,
            final BigInteger valueWei,
            final String what,
            final Duration timeout) {
        return sendAndAwait(rpc, raw, submitter, to, callData, valueWei, what, timeout, DEFAULT_GAS_LIMIT);
    }

    /**
     * Send a raw transaction and wait for it to succeed, with a caller-supplied gas limit.
     *
     * <p>Every other overload hands this {@link #DEFAULT_GAS_LIMIT}, generous headroom for a contract
     * call. {@link #fund} hands it {@link #PLAIN_TRANSFER_GAS_LIMIT} instead — see that constant's
     * javadoc for why an oversized declared limit on a plain transfer is not merely wasteful but can
     * leave the transaction accepted into the pool and never selected into a block.
     *
     * @param gasLimit the transaction's declared gas limit
     */
    private static JsonNode sendAndAwait(
            final EvmJsonRpcClient rpc,
            @Nullable final RawJsonRpc raw,
            final ChainTxSubmitter submitter,
            final String to,
            final byte[] callData,
            final BigInteger valueWei,
            final String what,
            final Duration timeout,
            final long gasLimit) {
        final String txHash = submitter.sendRawTx(to, callData, requireTransferable(valueWei, what), gasLimit);
        final JsonNode receipt = awaitReceipt(rpc, txHash, what, timeout);
        final JsonNode status = receipt.get("status");
        final String value = status == null || status.isNull() ? "0x0" : status.asText();
        if ("0x1".equalsIgnoreCase(value)) {
            return receipt;
        }
        throw new IllegalStateException(what + " reverted (tx " + txHash + ")"
                + replayReason(raw, receipt, submitter.address(), to, callData) + ", receipt=" + receipt);
    }

    /**
     * Poll {@code eth_getTransactionReceipt} until a non-null receipt appears or the {@code timeout}
     * elapses. Status/reverted handling is left to the caller — this only owns the "wait for the
     * receipt to become visible" step, since {@link #sendAndAwait} and
     * {@code awaitContractAddress} both need it but diverge on what they read out
     * of the receipt afterwards.
     */
    static JsonNode awaitReceipt(
            final EvmJsonRpcClient rpc, final String txHash, final String what, final Duration timeout) {
        // Math.max guards against sub-interval timeouts silently degrading to zero polls; at least
        // one lookup is always attempted so the failure message reflects an actual node round-trip
        // rather than an off-by-truncation "0s" report.
        final int attempts = Math.max(1, (int) (timeout.toMillis() / RECEIPT_POLL_INTERVAL_MS));
        for (int attempt = 0; attempt < attempts; attempt++) {
            final JsonNode receipt = rpc.ethGetTransactionReceipt(txHash);
            if (receipt != null && !receipt.isNull()) {
                return receipt;
            }
            sleep();
        }
        throw new IllegalStateException(
                "no receipt for " + what + " (tx " + txHash + ") after " + timeout.toSeconds() + "s");
    }

    /**
     * Replay a reverted call to recover its revert data, and name the custom error when recognised.
     *
     * @return a human-readable suffix, or an empty string when the reason cannot be recovered
     */
    private static String replayReason(
            @Nullable final RawJsonRpc raw,
            final JsonNode receipt,
            final String from,
            final String to,
            final byte[] callData) {
        if (raw == null) {
            return "";
        }
        try {
            final JsonNode blockNumber = receipt.get("blockNumber");
            final String blockTag = blockNumber == null || blockNumber.isNull() ? "latest" : blockNumber.asText();
            raw.call(
                    "eth_call",
                    "{\"from\":\"" + from + "\",\"to\":\"" + to + "\",\"data\":\"" + AbiCodec.toHex(callData) + "\"}",
                    RawJsonRpc.str(blockTag));
            // The replay succeeded, so the failure was state- or gas-dependent rather than a
            // deterministic revert — say so instead of implying no reason exists.
            return " (replay at the mined block did not revert; the failure depends on state or gas)";
        } catch (final RuntimeException e) {
            final String message = e.getMessage() == null ? "" : e.getMessage();
            return " — revert data: " + message + describeSelector(message);
        }
    }

    /** Name a CLPR custom error from the 4-byte selector embedded in the revert data. */
    private static String describeSelector(final String revertData) {
        for (final String signature : KNOWN_ERRORS) {
            final String selector = AbiCodec.toHexNoPrefix(AbiCodec.functionSelector(signature));
            if (revertData.toLowerCase(java.util.Locale.ROOT).contains(selector)) {
                return " [" + signature + "]";
            }
        }
        return "";
    }

    /**
     * Custom errors worth naming. Mostly the QBFT verifier's, because those are the ones a
     * hand-assembled config proof trips over, and their selectors are otherwise unreadable hex.
     */
    private static final java.util.List<String> KNOWN_ERRORS = java.util.List.of(
            "InvalidTrustAnchor()",
            "InvalidPayloadShape()",
            "InvalidProofPayloadShape()",
            "InvalidHeader()",
            "InvalidConfigHeader()",
            "InvalidConfigThrottleFieldCount()",
            "InvalidTrustAnchorFields()",
            "EpochMismatch()",
            "EmptyValidator()",
            "MultiValidatorNotSupported()",
            "InvalidNumberOfSealSignatures()",
            "ClprDisabled()",
            "ClprInvalidEndpointAddress()",
            "ClprEndpointNotRegistered()",
            "ClprChannelNotFound()",
            "ClprConnectorNotFound()",
            "ClprInvalidSignature()",
            "ClprInvalidCommitment()",
            "ClprQueueFull()");

    /**
     * Transfer ETH and wait for the transfer to succeed.
     *
     * @param rpc JSON-RPC client
     * @param submitter client-side signer
     * @param to recipient
     * @param valueWei amount
     * @param what short description for error messages
     * @return the receipt
     */
    public static JsonNode fund(
            final EvmJsonRpcClient rpc,
            final ChainTxSubmitter submitter,
            final String to,
            final BigInteger valueWei,
            final String what) {
        return fund(rpc, submitter, to, valueWei, what, DEFAULT_RECEIPT_TIMEOUT);
    }

    /**
     * Transfer ETH and wait for the transfer to succeed, with a caller-supplied receipt-wait budget.
     *
     * @param timeout how long to wait for a receipt before giving up
     */
    public static JsonNode fund(
            final EvmJsonRpcClient rpc,
            final ChainTxSubmitter submitter,
            final String to,
            final BigInteger valueWei,
            final String what,
            final Duration timeout) {
        return sendAndAwait(rpc, null, submitter, to, new byte[0], valueWei, what, timeout, PLAIN_TRANSFER_GAS_LIMIT);
    }

    /**
     * Poll until a receipt appears, then require status {@code 0x1}.
     *
     * @param rpc JSON-RPC client
     * @param txHash the transaction hash
     * @param what short description for error messages
     * @return the receipt
     */
    public static JsonNode awaitSuccess(final EvmJsonRpcClient rpc, final String txHash, final String what) {
        for (int attempt = 0; attempt < RECEIPT_POLL_ATTEMPTS; attempt++) {
            final JsonNode receipt = rpc.ethGetTransactionReceipt(txHash);
            if (receipt != null && !receipt.isNull()) {
                final JsonNode status = receipt.get("status");
                final String value = status == null || status.isNull() ? "0x0" : status.asText();
                if ("0x1".equalsIgnoreCase(value)) {
                    return receipt;
                }
                throw new IllegalStateException(what + " reverted (tx " + txHash + "), receipt=" + receipt);
            }
            sleep();
        }
        throw new IllegalStateException("no receipt for " + what + " (tx " + txHash + ") after "
                + (RECEIPT_POLL_ATTEMPTS * RECEIPT_POLL_INTERVAL_MS / 1000) + "s");
    }

    /**
     * Reject a value a single transaction cannot carry, naming the reason.
     *
     * <p>Without this the failure is a bare {@link ArithmeticException} from {@code longValueExact()},
     * which says nothing about the {@code long}-typed submitter API being the actual constraint.
     */
    private static long requireTransferable(final BigInteger valueWei, final String what) {
        if (valueWei.signum() < 0) {
            throw new IllegalArgumentException(what + ": value must not be negative, got " + valueWei);
        }
        if (valueWei.compareTo(MAX_TRANSFERABLE_WEI) > 0) {
            throw new IllegalArgumentException(what + ": " + valueWei
                    + " wei exceeds what one transaction can carry (" + MAX_TRANSFERABLE_WEI
                    + "). sendRawTx takes valueWei as a long, so transfers are capped there.");
        }
        return valueWei.longValueExact();
    }

    private static void sleep() {
        try {
            Thread.sleep(RECEIPT_POLL_INTERVAL_MS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while polling for a receipt", e);
        }
    }

    /**
     * Read a {@code bytes32} return value from a view-style call.
     *
     * @param rpc JSON-RPC client
     * @param to target contract
     * @param callData ABI-encoded call data
     * @return the 32-byte result
     */
    public static byte[] callBytes32(final EvmJsonRpcClient rpc, final String to, final byte[] callData) {
        final byte[] decoded = AbiCodec.fromHex(rpc.ethCall(to, AbiCodec.toHex(callData), "latest"));
        if (decoded.length < 32) {
            throw new IllegalStateException("expected 32 bytes from " + to + ", got " + decoded.length);
        }
        return AbiCodec.decodeBytes32(decoded, 0);
    }

    /** @return the {@code to} field of a receipt, or {@code null} for a contract creation. */
    @Nullable
    public static String receiptTo(final JsonNode receipt) {
        final JsonNode to = receipt.get("to");
        return to == null || to.isNull() ? null : to.asText();
    }
}
