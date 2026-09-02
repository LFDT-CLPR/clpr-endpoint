// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.beacon;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import org.hiero.clpr.relay.evm.ssz.Ssz;

/**
 * The Deneb SSZ {@code ExecutionPayloadHeader} (17 fields). The relay merkleizes these fields to
 * build the depth-5 sub-branch from {@code state_root} (field index {@value #STATE_ROOT_FIELD_INDEX})
 * up to the {@code ExecutionPayloadHeader} root, which composes with the beacon block body's
 * depth-4 {@code execution_payload} branch into the depth-9 branch the peer verifier checks against
 * the attested {@code bodyRoot}.
 *
 * <p>Field leaf encodings (SSZ {@code hash_tree_root} per field):
 * <ul>
 *   <li>{@code Root}/{@code Bytes32} — the 32 bytes verbatim.</li>
 *   <li>{@code fee_recipient} ({@code Bytes20}) — left-aligned in a 32-byte chunk.</li>
 *   <li>{@code logs_bloom} ({@code ByteVector[256]}) — merkleize of its 8 32-byte chunks.</li>
 *   <li>{@code uint64} — little-endian in a 32-byte chunk.</li>
 *   <li>{@code base_fee_per_gas} ({@code uint256}) — little-endian 32 bytes.</li>
 *   <li>{@code extra_data} ({@code ByteList[32]}) — {@code mix_in_length(pack, len)}.</li>
 * </ul>
 */
public record ExecutionPayloadHeader(
        Bytes parentHash,
        Bytes feeRecipient,
        Bytes stateRoot,
        Bytes receiptsRoot,
        Bytes logsBloom,
        Bytes prevRandao,
        long blockNumber,
        long gasLimit,
        long gasUsed,
        long timestamp,
        Bytes extraData,
        BigInteger baseFeePerGas,
        Bytes blockHash,
        Bytes transactionsRoot,
        Bytes withdrawalsRoot,
        long blobGasUsed,
        long excessBlobGas) {

    /** SSZ field index of {@code state_root} within the container. */
    public static final int STATE_ROOT_FIELD_INDEX = 2;

    private static final int FIELD_COUNT = 17;
    /** Next power of two ≥ 17. */
    private static final int PADDED_LEAVES = 32;

    /** The 17 field {@code hash_tree_root} leaves, in SSZ container order. */
    byte[][] fieldLeaves() {
        final byte[][] leaves = new byte[FIELD_COUNT][];
        leaves[0] = bytes32(parentHash);
        leaves[1] = leftAligned32(feeRecipient); // Bytes20
        leaves[2] = bytes32(stateRoot);
        leaves[3] = bytes32(receiptsRoot);
        leaves[4] = byteVectorRoot(logsBloom); // ByteVector[256]
        leaves[5] = bytes32(prevRandao);
        leaves[6] = Ssz.uint64Leaf(blockNumber);
        leaves[7] = Ssz.uint64Leaf(gasLimit);
        leaves[8] = Ssz.uint64Leaf(gasUsed);
        leaves[9] = Ssz.uint64Leaf(timestamp);
        leaves[10] = byteListRoot(extraData); // ByteList[32]
        leaves[11] = uint256Le(baseFeePerGas);
        leaves[12] = bytes32(blockHash);
        leaves[13] = bytes32(transactionsRoot);
        leaves[14] = bytes32(withdrawalsRoot);
        leaves[15] = Ssz.uint64Leaf(blobGasUsed);
        leaves[16] = Ssz.uint64Leaf(excessBlobGas);
        return leaves;
    }

    /** Pads the 17 field leaves to 32 with zero chunks (the SSZ vector layout for merkleization). */
    private byte[][] paddedLeaves() {
        final byte[][] padded = new byte[PADDED_LEAVES][];
        final byte[][] leaves = fieldLeaves();
        System.arraycopy(leaves, 0, padded, 0, FIELD_COUNT);
        for (int i = FIELD_COUNT; i < PADDED_LEAVES; i++) {
            padded[i] = new byte[32];
        }
        return padded;
    }

    /** {@code hash_tree_root} of the whole {@code ExecutionPayloadHeader} container. */
    public Bytes hashTreeRoot() {
        return Bytes.wrap(Ssz.merkleize(paddedLeaves()));
    }

    /**
     * The depth-5 SSZ branch from {@code state_root} up to this header's root. Compose it (below) with
     * the beacon body's depth-4 {@code execution_payload} branch to reach the depth-9 branch.
     */
    public byte[][] stateRootSubBranch() {
        return Ssz.generateProof(paddedLeaves(), STATE_ROOT_FIELD_INDEX);
    }

    // ── leaf encoders ────────────────────────────────────────────────────────

    private static byte[] bytes32(final Bytes b) {
        final byte[] raw = b.toByteArray();
        if (raw.length != 32) {
            throw new IllegalArgumentException("expected 32 bytes, got " + raw.length);
        }
        return raw;
    }

    private static byte[] leftAligned32(final Bytes b) {
        final byte[] raw = b.toByteArray();
        if (raw.length > 32) {
            throw new IllegalArgumentException("value longer than 32 bytes: " + raw.length);
        }
        final byte[] out = new byte[32];
        System.arraycopy(raw, 0, out, 0, raw.length);
        return out;
    }

    /** {@code hash_tree_root} of a fixed-length byte vector: merkleize its 32-byte chunks. */
    private static byte[] byteVectorRoot(final Bytes b) {
        final byte[] raw = b.toByteArray();
        if (raw.length % 32 != 0 || raw.length == 0) {
            throw new IllegalArgumentException(
                    "byte vector length must be a positive multiple of 32, got " + raw.length);
        }
        final int chunks = raw.length / 32;
        if (chunks == 1) {
            return raw;
        }
        final byte[][] leaves = new byte[chunks][];
        for (int i = 0; i < chunks; i++) {
            leaves[i] = new byte[32];
            System.arraycopy(raw, i * 32, leaves[i], 0, 32);
        }
        return Ssz.merkleize(leaves);
    }

    /**
     * {@code hash_tree_root} of {@code extra_data} ({@code ByteList[32]}): {@code mix_in_length}
     * of the single packed chunk with the element count. For a ≤32-byte list the data packs into one
     * chunk (zero chunk when empty); the list limit is one chunk.
     */
    private static byte[] byteListRoot(final Bytes b) {
        final byte[] raw = b.toByteArray();
        if (raw.length > 32) {
            throw new IllegalArgumentException("extra_data exceeds 32 bytes: " + raw.length);
        }
        final byte[] chunk = new byte[32];
        System.arraycopy(raw, 0, chunk, 0, raw.length);
        return Ssz.sha256(chunk, uint256Le(BigInteger.valueOf(raw.length)));
    }

    /** Encodes an unsigned integer as a 32-byte little-endian SSZ {@code uint256} chunk. */
    private static byte[] uint256Le(final BigInteger value) {
        if (value.signum() < 0) {
            throw new IllegalArgumentException("uint256 must be non-negative");
        }
        final byte[] be = value.toByteArray(); // big-endian, possibly with a leading zero sign byte
        int from = (be.length > 0 && be[0] == 0) ? 1 : 0;
        final int len = be.length - from;
        if (len > 32) {
            throw new IllegalArgumentException("value exceeds uint256");
        }
        final byte[] le = new byte[32];
        for (int i = 0; i < len; i++) {
            le[i] = be[be.length - 1 - i];
        }
        return le;
    }
}
