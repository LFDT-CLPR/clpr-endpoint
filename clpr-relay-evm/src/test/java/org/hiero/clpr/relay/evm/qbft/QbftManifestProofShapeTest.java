// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.qbft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.clpr.relay.evm.ByteUtils.ZERO_HASH;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.List;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.model.BlockHeader;
import org.hiero.clpr.relay.evm.model.EvmStorageMerkleProof;
import org.hiero.clpr.relay.evm.qbft.QbftBundleConstructor.QbftBundlePayload;
import org.junit.jupiter.api.Test;

/**
 * Regression guard for the manifest-storage-proof RLP shape. SC-189's
 * {@code ClprEvmStateProof.verifyProvenSlots} (called by {@code _verifyEndpointManifest}) decodes the
 * manifest proof as a list of {@code [slotKey, [nodes]]} ENTRIES and does {@code RLP.readList} on each
 * entry. The bundle proof (payload index 5) must therefore be entry-wrapped, exactly like the
 * channel storage proof (index 3) — a flat list of node strings makes the on-chain
 * {@code readList} revert {@code RLPInvalidEncoding} and stalls every manifest-carrying bundle.
 */
class QbftManifestProofShapeTest {

    /** A 32-byte big-endian value (mirrors the manifest commitment slot key the verifier matches). */
    private static Bytes slotKey(final long v) {
        final byte[] b = new byte[32];
        for (int i = 0; i < 8; i++) b[31 - i] = (byte) (v >>> (8 * i));
        return Bytes.wrap(b);
    }

    /** A realistic (>55-byte, long-form) MPT node blob. */
    private static Bytes node(final int len, final int seed) {
        final byte[] b = new byte[len];
        for (int i = 0; i < len; i++) b[i] = (byte) (seed + i);
        return Bytes.wrap(b);
    }

    private static BlockHeader minimalHeader() {
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
                BigInteger.ONE,
                ZERO_HASH,
                BigInteger.ZERO,
                BigInteger.ZERO,
                ZERO_HASH,
                ZERO_HASH,
                ZERO_HASH);
    }

    // ── minimal strict RLP walker (matches OZ item framing) ──────────────────────
    private static long be(final byte[] b, final int off, final int n) {
        long v = 0;
        for (int i = 0; i < n; i++) v = (v << 8) | (b[off + i] & 0xffL);
        return v;
    }

    /** {contentStart, contentLen, isList(1/0)} for the RLP item at {@code s}. */
    private static int[] head(final byte[] b, final int s) {
        final int p = b[s] & 0xff;
        if (p < 0x80) return new int[] {s, 1, 0};
        if (p <= 0xb7) return new int[] {s + 1, p - 0x80, 0};
        if (p < 0xc0) {
            final int ll = p - 0xb7;
            return new int[] {s + 1 + ll, (int) be(b, s + 1, ll), 0};
        }
        if (p <= 0xf7) return new int[] {s + 1, p - 0xc0, 1};
        final int ll = p - 0xf7;
        return new int[] {s + 1 + ll, (int) be(b, s + 1, ll), 1};
    }

    /** Return the top-level list element slices as {elemStart, contentStart, contentLen, isList}. */
    private static int[][] topElements(final byte[] b) {
        final int[] top = head(b, 0);
        final int end = top[0] + top[1];
        int pos = top[0];
        final int[][] out = new int[8][];
        int i = 0;
        while (pos < end && i < 8) {
            final int[] h = head(b, pos);
            out[i] = new int[] {pos, h[0], h[1], h[2]};
            pos = (h[0] - pos) + h[1] + pos;
            i++;
        }
        return java.util.Arrays.copyOf(out, i);
    }

    @Test
    void manifestProof_isEntryWrapped_notFlatNodeStrings() {
        final Bytes key = slotKey(18); // ENDPOINT_MANIFEST_COMMITMENT_SLOT
        final Bytes n0 = node(532, 1);
        final Bytes n1 = node(83, 2);
        final EvmStorageMerkleProof manifestEntry = new EvmStorageMerkleProof(key, List.of(n0, n1));

        final QbftBundlePayload payload = new QbftBundlePayload(
                List.of(), // epoch headers
                minimalHeader(),
                List.of(node(70, 3)), // account proof node
                List.of(new EvmStorageMerkleProof(slotKey(15), List.of(node(200, 4)))), // storage proof entry
                Bytes.wrap(new byte[] {0x01, 0x02, 0x03}), // bundle content
                manifestEntry,
                Bytes.wrap(new byte[] {0x08, 0x01})); // manifest preimage (protobuf {version:1})

        final byte[] proof =
                QbftBundleConstructor.serializeBundlePayload(payload).toByteArray();

        final int[][] el = topElements(proof);
        assertThat(el.length).as("7-element (with-manifest) bundle proof").isEqualTo(7);

        // element[5] = manifest proof: must be a LIST of entries whose first element is itself a LIST
        // (the [slotKey, [nodes]] entry) — NOT a flat node string.
        final int[] manifestElem = el[5];
        assertThat(manifestElem[3]).as("manifest proof element is a list").isEqualTo(1);
        final int firstSubPrefix = proof[manifestElem[1]] & 0xff;
        assertThat(firstSubPrefix)
                .as("first manifest-proof sub-element must be a LIST (entry), not a string (<0xC0)")
                .isGreaterThanOrEqualTo(0xC0);

        // Decode the single entry and assert [key(32B) == slot 18, [nodes]].
        final int[] entry = head(proof, manifestElem[1]); // the entry
        assertThat(entry[2]).as("entry is a list").isEqualTo(1);
        final int[] keyField = head(proof, entry[0]); // entry field[0] = slot key
        assertThat(keyField[2]).as("entry key is a data string").isEqualTo(0);
        assertThat(keyField[1]).as("entry key is 32 bytes (bytes32 slot)").isEqualTo(32);
        final byte[] keyBytes = java.util.Arrays.copyOfRange(proof, keyField[0], keyField[0] + keyField[1]);
        assertThat(Bytes.wrap(keyBytes)).as("entry key == commitment slot 18").isEqualTo(key);
        final int nodesFieldStart = entry[0] + (keyField[0] - entry[0]) + keyField[1];
        assertThat(proof[nodesFieldStart] & 0xff)
                .as("entry field[1] (nodes) is a list")
                .isGreaterThanOrEqualTo(0xC0);
    }
}
