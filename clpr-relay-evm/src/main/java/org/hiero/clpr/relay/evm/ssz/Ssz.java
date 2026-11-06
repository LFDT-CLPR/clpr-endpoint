// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.ssz;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * SSZ (Simple Serialize) primitives the Ethereum-PoS bundle/config <em>producer</em> needs:
 * SHA-256 merkleization, the {@code uint64} leaf encoding, sync-committee signing domain/root, and
 * Merkle-branch generation + verification.
 *
 * <p>This is the producer-side mirror of {@code clpr-hiero}'s
 * {@code com.hedera.node.app.service.clpr.impl.verifier.ethereum.Ssz} (the consumer). The relay
 * builds the proofs that verifier checks, so the hashing rules, generalized indices, and branch
 * orderings here must match it exactly: a single divergent leaf encoding or index makes the peer's
 * {@code SszMerkleBranch.proves(...)} fail.
 *
 * <p>The generalized-index constants are derived in the consensus spec. Depths/leaf-indices for the
 * branches the relay constructs:
 *
 * <ul>
 *   <li>{@code execution_payload.state_root} under the beacon block {@code bodyRoot}: depth 9, leaf
 *       index 290 (execution_payload at body gindex 25 ⇒ index 9 at depth 4; state_root at field 2
 *       of the ExecutionPayloadHeader ⇒ index 2 at depth 5; composed 2 + (9 &lt;&lt; 5) = 290).</li>
 *   <li>{@code BeaconState.current_sync_committee}: depth 6, leaf index 22.</li>
 *   <li>{@code BeaconState.next_sync_committee}: depth 6, leaf index 23.</li>
 * </ul>
 */
public final class Ssz {

    private Ssz() {}

    // ── Sync committee / BLS sizes (Altair, unchanged through Electra) ──
    public static final int SYNC_COMMITTEE_SIZE = 512;
    public static final int SYNC_COMMITTEE_BITS_LENGTH = SYNC_COMMITTEE_SIZE / 8;
    public static final int BLS_PUBKEY_LENGTH = 48;
    public static final int BLS_SIGNATURE_LENGTH = 96;
    public static final int FORK_VERSION_LENGTH = 4;

    /** BeaconBlockHeader SSZ container fields: slot, proposerIndex, parentRoot, stateRoot, bodyRoot. */
    public static final int BEACON_HEADER_FIELDS = 5;

    /** {@code DOMAIN_SYNC_COMMITTEE} domain type from the consensus spec. */
    private static final byte[] DOMAIN_SYNC_COMMITTEE = {0x07, 0x00, 0x00, 0x00};

    public static final int EXECUTION_STATE_ROOT_BRANCH_DEPTH = 9;
    public static final int EXECUTION_STATE_ROOT_LEAF_INDEX = 290;

    public static final int CURRENT_SYNC_COMMITTEE_BRANCH_DEPTH = 6;
    public static final int CURRENT_SYNC_COMMITTEE_LEAF_INDEX = 22;

    public static final int NEXT_SYNC_COMMITTEE_BRANCH_DEPTH = 6;
    public static final int NEXT_SYNC_COMMITTEE_LEAF_INDEX = 23;

    /**
     * Per-thread reusable SHA-256 digest ({@link MessageDigest} is stateful and not thread-safe).
     */
    private static final ThreadLocal<MessageDigest> SHA_256 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    });

    /** SHA-256 over the concatenation of {@code inputs}. */
    public static byte[] sha256(final byte[]... inputs) {
        final MessageDigest digest = SHA_256.get();
        digest.reset();
        for (final byte[] input : inputs) {
            digest.update(input);
        }
        return digest.digest();
    }

    /**
     * Computes the root of a balanced binary Merkle tree (SHA-256) over the given leaves — the core
     * of SSZ {@code hash_tree_root} for fixed-size containers and vectors. {@code chunks.length} must
     * be a power of two; callers pad to the next power of two with all-zero chunks.
     */
    public static byte[] merkleize(final byte[][] chunks) {
        if (Integer.bitCount(chunks.length) != 1) {
            throw new IllegalArgumentException("chunk count must be a power of two, got " + chunks.length);
        }
        byte[][] level = chunks;
        while (level.length > 1) {
            final byte[][] next = new byte[level.length / 2][];
            for (int i = 0; i < next.length; i++) {
                next[i] = sha256(level[2 * i], level[2 * i + 1]);
            }
            level = next;
        }
        return level[0];
    }

    /**
     * Generates the SSZ Merkle inclusion branch for the leaf at {@code leafIndex} within
     * {@code chunks} (a power-of-two leaf vector), ordered bottom-up — {@code branch[0]} is the
     * leaf's direct sibling. This is the producer-side inverse of {@link #verifyBranch}: feeding the
     * returned branch and the same {@code leafIndex} back into {@link #verifyBranch} against
     * {@link #merkleize}{@code (chunks)} returns true.
     */
    public static byte[][] generateProof(final byte[][] chunks, final int leafIndex) {
        if (Integer.bitCount(chunks.length) != 1) {
            throw new IllegalArgumentException("chunk count must be a power of two, got " + chunks.length);
        }
        if (leafIndex < 0 || leafIndex >= chunks.length) {
            throw new IllegalArgumentException("leafIndex " + leafIndex + " out of range [0," + chunks.length + ")");
        }
        final int depth = Integer.numberOfTrailingZeros(chunks.length);
        final byte[][] branch = new byte[depth][];
        byte[][] level = chunks.clone();
        int index = leafIndex;
        for (int d = 0; d < depth; d++) {
            branch[d] = level[index ^ 1];
            final byte[][] next = new byte[level.length / 2][];
            for (int i = 0; i < next.length; i++) {
                next[i] = sha256(level[2 * i], level[2 * i + 1]);
            }
            level = next;
            index >>= 1;
        }
        return branch;
    }

    /**
     * Folds {@code leaf32} up the {@code branch} (bottom-up siblings) per the consensus-spec ordering
     * rule — bit {@code i} of {@code index} selects left/right at level {@code i} — and returns the
     * recomputed root. A direct port of the fold in {@code is_valid_merkle_branch}.
     */
    public static byte[] branchRoot(final byte[] leaf32, final byte[][] branch, final int index) {
        byte[] node = leaf32;
        for (int i = 0; i < branch.length; i++) {
            node = ((index >>> i) & 1) == 1 ? sha256(branch[i], node) : sha256(node, branch[i]);
        }
        return node;
    }

    /** True iff {@code leaf32} sits at {@code index} of the tree whose root is {@code root32}. */
    public static boolean verifyBranch(
            final byte[] leaf32, final byte[][] branch, final byte[] root32, final int index) {
        return Arrays.equals(branchRoot(leaf32, branch, index), root32);
    }

    /**
     * {@code hash_tree_root} of a 48-byte BLS pubkey: SHA-256 of the pubkey zero-padded to 64 bytes
     * (SSZ packs the 48-byte vector into two 32-byte chunks and merkleizes them, which equals
     * {@code sha256(pubkey ‖ 16 zero bytes ‖ 32 zero bytes)} = {@code sha256(64-byte padding)}).
     */
    public static byte[] pubkeyHash64(final byte[] pubkey48) {
        if (pubkey48.length != BLS_PUBKEY_LENGTH) {
            throw new IllegalArgumentException(
                    "pubkey must be " + BLS_PUBKEY_LENGTH + " bytes, got " + pubkey48.length);
        }
        final byte[] padded = new byte[64];
        System.arraycopy(pubkey48, 0, padded, 0, BLS_PUBKEY_LENGTH);
        return sha256(padded);
    }

    /** SSZ {@code uint64} leaf: value little-endian in the first 8 bytes of a 32-byte zero chunk. */
    public static byte[] uint64Leaf(final long value) {
        final byte[] leaf = new byte[32];
        long v = value;
        for (int i = 0; i < 8; i++) {
            leaf[i] = (byte) (v & 0xFF);
            v >>>= 8;
        }
        return leaf;
    }

    /** {@code compute_domain(DOMAIN_SYNC_COMMITTEE, forkVersion, genesisValidatorsRoot)}. */
    public static byte[] computeSyncCommitteeDomain(final byte[] forkVersion4, final byte[] genesisValidatorsRoot32) {
        final byte[] paddedVersion = new byte[32];
        System.arraycopy(forkVersion4, 0, paddedVersion, 0, FORK_VERSION_LENGTH);
        final byte[] forkDataRoot = sha256(paddedVersion, genesisValidatorsRoot32);
        final byte[] domain = new byte[32];
        System.arraycopy(DOMAIN_SYNC_COMMITTEE, 0, domain, 0, 4);
        System.arraycopy(forkDataRoot, 0, domain, 4, 28);
        return domain;
    }

    /** {@code compute_signing_root(object, domain) = sha256(objectRoot ‖ domain)}. */
    public static byte[] computeSigningRoot(final byte[] objectRoot32, final byte[] domain32) {
        return sha256(objectRoot32, domain32);
    }
}
