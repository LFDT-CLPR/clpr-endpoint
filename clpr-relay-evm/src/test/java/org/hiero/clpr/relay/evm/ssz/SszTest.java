// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.ssz;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.MessageDigest;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

class SszTest {

    private static final HexFormat HEX = HexFormat.of();

    /** Well-known SSZ zero-subtree hash: zh[1] = sha256(64 zero bytes). */
    private static final byte[] ZERO_HASH_1 =
            HEX.parseHex("f5a5fd42d16a20302798ef6ed309979b43003d2320d9f0e8ea9831a92759fb4b");

    /** Independent JDK SHA-256 oracle (not Ssz.sha256), so we are not testing the code against itself. */
    private static byte[] oracleSha256(final byte[]... inputs) {
        try {
            final MessageDigest d = MessageDigest.getInstance("SHA-256");
            for (final byte[] in : inputs) {
                d.update(in);
            }
            return d.digest();
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[][] deterministicChunks(final int count) {
        final byte[][] chunks = new byte[count][];
        for (int i = 0; i < count; i++) {
            final byte[] c = new byte[32];
            for (int j = 0; j < 32; j++) {
                c[j] = (byte) (i * 31 + j);
            }
            chunks[i] = c;
        }
        return chunks;
    }

    @Test
    void merkleizeTwoZeroChunks_matchesKnownZeroHash() {
        final byte[][] chunks = {new byte[32], new byte[32]};
        assertThat(Ssz.merkleize(chunks)).isEqualTo(ZERO_HASH_1);
    }

    @Test
    void merkleize_matchesIndependentFold() {
        final byte[][] chunks = deterministicChunks(8);
        // Independent two-level fold.
        final byte[] l0 = oracleSha256(chunks[0], chunks[1]);
        final byte[] l1 = oracleSha256(chunks[2], chunks[3]);
        final byte[] l2 = oracleSha256(chunks[4], chunks[5]);
        final byte[] l3 = oracleSha256(chunks[6], chunks[7]);
        final byte[] expected = oracleSha256(oracleSha256(l0, l1), oracleSha256(l2, l3));
        assertThat(Ssz.merkleize(chunks)).isEqualTo(expected);
    }

    @Test
    void generateProof_roundTripsThroughVerifyBranch_forEveryLeaf() {
        final byte[][] chunks = deterministicChunks(16);
        final byte[] root = Ssz.merkleize(chunks);
        for (int i = 0; i < chunks.length; i++) {
            final byte[][] branch = Ssz.generateProof(chunks, i);
            assertThat(branch.length).isEqualTo(4); // log2(16)
            assertThat(Ssz.verifyBranch(chunks[i], branch, root, i))
                    .as("leaf %d should verify against the merkle root", i)
                    .isTrue();
            assertThat(Ssz.branchRoot(chunks[i], branch, i)).isEqualTo(root);
        }
    }

    @Test
    void verifyBranch_rejectsWrongLeafIndex() {
        final byte[][] chunks = deterministicChunks(8);
        final byte[] root = Ssz.merkleize(chunks);
        final byte[][] branch = Ssz.generateProof(chunks, 3);
        assertThat(Ssz.verifyBranch(chunks[3], branch, root, 4)).isFalse();
    }

    @Test
    void uint64Leaf_isLittleEndianInFirstEightBytes() {
        final byte[] leaf = Ssz.uint64Leaf(0x0102030405060708L);
        assertThat(leaf).hasSize(32);
        assertThat(leaf[0]).isEqualTo((byte) 0x08);
        assertThat(leaf[7]).isEqualTo((byte) 0x01);
        for (int i = 8; i < 32; i++) {
            assertThat(leaf[i]).isEqualTo((byte) 0x00);
        }
    }

    @Test
    void merkleize_rejectsNonPowerOfTwo() {
        assertThatThrownBy(() -> Ssz.merkleize(deterministicChunks(3)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("power of two");
    }

    @Test
    void signingRoot_matchesConsensusSpecConstruction() {
        final byte[] forkVersion = {0x05, 0x00, 0x00, 0x00};
        final byte[] gvr = new byte[32];
        for (int i = 0; i < 32; i++) {
            gvr[i] = (byte) (0x4B + i * 31);
        }
        final byte[] headerRoot = deterministicChunks(1)[0];

        // Independent re-derivation.
        final byte[] paddedVersion = new byte[32];
        System.arraycopy(forkVersion, 0, paddedVersion, 0, 4);
        final byte[] forkDataRoot = oracleSha256(paddedVersion, gvr);
        final byte[] domain = new byte[32];
        domain[0] = 0x07;
        System.arraycopy(forkDataRoot, 0, domain, 4, 28);
        final byte[] expected = oracleSha256(headerRoot, domain);

        final byte[] actual = Ssz.computeSigningRoot(headerRoot, Ssz.computeSyncCommitteeDomain(forkVersion, gvr));
        assertThat(actual).isEqualTo(expected);
    }
}
