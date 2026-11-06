// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Reproduction harness for the e2e {@code RLPInvalidEncoding} finding: the on-chain QBFT verifier
 * decodes the bundle proof with OpenZeppelin's strict {@code RLP.sol} and rejects the relay's
 * {@link AbiCodec} RLP for larger (~>=8 KB) bundles. This test ports OZ's exact {@code _decodeLength}
 * rules and checks the relay encoder's output against them, sweeping sizes/structures to localise the
 * exact divergence.
 */
class AbiCodecOzRlpReproTest {

    // ── Faithful port of OpenZeppelin utils/RLP.sol _decodeLength + strict list walk ──
    private static final int SHORT_THRESHOLD = 55;
    private static final int SHORT_OFFSET = 0x80;
    private static final int LONG_OFFSET = 0xC0;

    static final class OzRlpInvalid extends RuntimeException {
        OzRlpInvalid(final String why, final int pos) {
            super(why + " @pos=" + pos);
        }
    }

    private static void req(final boolean cond, final String why, final int pos) {
        if (!cond) throw new OzRlpInvalid(why, pos);
    }

    private static long readBE(final byte[] b, final int off, final int n) {
        long v = 0;
        for (int i = 0; i < n; i++) v = (v << 8) | (b[off + i] & 0xffL);
        return v;
    }

    /** Validate one item in b[start..end); return the number of bytes it consumes. Recurses into lists. */
    private static int ozItem(final byte[] b, final int start, final int end) {
        final int itemLength = end - start;
        req(itemLength != 0, "empty item", start);
        final int prefix = b[start] & 0xff;
        if (prefix < LONG_OFFSET) {
            if (prefix < SHORT_OFFSET) {
                return 1; // single data byte < 0x80
            } else if (prefix <= SHORT_OFFSET + SHORT_THRESHOLD) {
                final int strLen = prefix - SHORT_OFFSET;
                req(itemLength > strLen && (strLen != 1 || (b[start + 1] & 0xff) >= SHORT_OFFSET), "short-str", start);
                return 1 + strLen;
            } else {
                final int lengthLength = prefix - SHORT_OFFSET - SHORT_THRESHOLD;
                req(itemLength > lengthLength, "long-str lenlen", start);
                req((b[start + 1] & 0xff) != 0x00, "long-str leading-zero", start);
                final long len = readBE(b, start + 1, lengthLength);
                req(len > SHORT_THRESHOLD && (long) (itemLength - lengthLength) > len, "long-str len", start);
                return (int) (1 + lengthLength + len);
            }
        } else {
            if (prefix <= LONG_OFFSET + SHORT_THRESHOLD) {
                final int listLen = prefix - LONG_OFFSET;
                req(itemLength > listLen, "short-list", start);
                ozListContent(b, start + 1, start + 1 + listLen);
                return 1 + listLen;
            } else {
                final int lengthLength = prefix - LONG_OFFSET - SHORT_THRESHOLD;
                req(itemLength > lengthLength, "long-list lenlen", start);
                req((b[start + 1] & 0xff) != 0x00, "long-list leading-zero", start);
                final long len = readBE(b, start + 1, lengthLength);
                req(len > SHORT_THRESHOLD && (long) (itemLength - lengthLength) > len, "long-list len", start);
                ozListContent(b, start + 1 + lengthLength, (int) (start + 1 + lengthLength + len));
                return (int) (1 + lengthLength + len);
            }
        }
    }

    private static void ozListContent(final byte[] b, final int cStart, final int cEnd) {
        req(cEnd <= b.length, "list-content exceeds buffer", cStart);
        int pos = cStart;
        while (pos < cEnd) {
            final int consumed = ozItem(b, pos, cEnd); // slice = pos..cEnd, mirroring OZ's remaining-length semantics
            req(pos + consumed <= cEnd, "element exceeds list bound", pos);
            pos += consumed;
        }
        req(pos == cEnd, "list not exactly consumed", cStart);
    }

    /** Decode the whole buffer as OZ would (RLP.decodeList): one item consuming exactly the buffer. */
    private static void ozValidate(final byte[] b) {
        final int consumed = ozItem(b, 0, b.length);
        req(consumed == b.length, "top item not exactly buffer (" + consumed + "/" + b.length + ")", 0);
    }

    private static byte[] bytesOf(final int len, final int seed) {
        final byte[] b = new byte[len];
        new Random(seed).nextBytes(b);
        return b;
    }

    // ── 1. Fuzz the length codec: rlpBytes / rlpList over every length up to ~12 KB ──

    @Test
    void rlpBytes_everyLength_isOzStrictValid() {
        for (int len = 0; len <= 12_000; len++) {
            final byte[] enc = AbiCodec.rlpBytes(bytesOf(len, len));
            try {
                ozValidate(enc);
            } catch (final OzRlpInvalid e) {
                throw new AssertionError("rlpBytes(len=" + len + ") rejected by OZ: " + e.getMessage(), e);
            }
        }
    }

    @Test
    void rlpList_singleStringItem_everyLength_isOzStrictValid() {
        for (int len = 0; len <= 12_000; len++) {
            final byte[] enc = AbiCodec.rlpList(AbiCodec.rlpBytes(bytesOf(len, len)));
            try {
                ozValidate(enc);
            } catch (final OzRlpInvalid e) {
                throw new AssertionError("rlpList[str(len=" + len + ")] rejected by OZ: " + e.getMessage(), e);
            }
        }
    }

    // ── 2. Synthetic bundle proof mirroring QbftBundleConstructor.PayloadRlpEncoder (5-element) ──
    //    Sweep total size 6 KB → 9 KB by growing the MPT proof-node bytes, matching the e2e where
    //    <=~7635 B verified and >=~8248 B failed with RLPInvalidEncoding.

    private static byte[] syntheticProof(final int proofNodeBytes) {
        // [0] current block header: a ~17-field list incl. a large extraData (QBFT seals grow per block)
        final byte[][] headerFields = new byte[17][];
        for (int i = 0; i < 17; i++) headerFields[i] = AbiCodec.rlpBytes(bytesOf(i == 12 ? 210 : 32, 1000 + i));
        final byte[] blockHeader = AbiCodec.rlpList(headerFields);
        // [1] epoch headers: empty
        final byte[] epochHeaders = AbiCodec.rlpList();
        // [2] account proof: 3 nodes
        final byte[] accountProof = AbiCodec.rlpList(
                AbiCodec.rlpBytes(bytesOf(532, 1)),
                AbiCodec.rlpBytes(bytesOf(83, 2)),
                AbiCodec.rlpBytes(bytesOf(70, 3)));
        // [3] storage proofs: 6 entries, each [key, [nodes...]]; the last node sized by proofNodeBytes
        final byte[][] entries = new byte[6][];
        for (int i = 0; i < 6; i++) {
            entries[i] = AbiCodec.rlpList(
                    AbiCodec.rlpBytes(bytesOf(32, 500 + i)),
                    AbiCodec.rlpList(
                            AbiCodec.rlpBytes(bytesOf(532, 600 + i)),
                            AbiCodec.rlpBytes(bytesOf(proofNodeBytes, 700 + i))));
        }
        final byte[] storageProofs = AbiCodec.rlpList(entries);
        // [4] bundle content
        final byte[] content = AbiCodec.rlpBytes(bytesOf(193, 42));
        return AbiCodec.rlpList(blockHeader, epochHeaders, accountProof, storageProofs, content);
    }

    @Test
    void syntheticBundleProof_sweep6to9kb_isOzStrictValidAtEverySize() {
        int firstFail = -1;
        String firstFailMsg = null;
        for (int node = 100; node <= 800; node++) {
            final byte[] proof = syntheticProof(node);
            try {
                ozValidate(proof);
            } catch (final OzRlpInvalid e) {
                if (firstFail < 0) {
                    firstFail = proof.length;
                    firstFailMsg = e.getMessage();
                }
            }
        }
        final String msg = firstFail < 0
                ? "all sizes OZ-valid"
                : String.format(
                        "Synthetic proof first rejected by OZ at total size %d bytes: %s", firstFail, firstFailMsg);
        assertThat(firstFail).as(msg).isEqualTo(-1);
    }

    // ── 3. Synthetic 7-element (WITH-MANIFEST) proof — the shape the failing bundles use, since the
    //    local manifest is v1 so the relay attaches a manifest storage-proof [5] + preimage [6]. ──

    private static byte[] syntheticProofWithManifest(final int proofNodeBytes, final int manifestPreimageBytes) {
        final byte[][] headerFields = new byte[17][];
        for (int i = 0; i < 17; i++) headerFields[i] = AbiCodec.rlpBytes(bytesOf(i == 12 ? 210 : 32, 1000 + i));
        final byte[] blockHeader = AbiCodec.rlpList(headerFields);
        final byte[] epochHeaders = AbiCodec.rlpList();
        final byte[] accountProof = AbiCodec.rlpList(
                AbiCodec.rlpBytes(bytesOf(532, 1)),
                AbiCodec.rlpBytes(bytesOf(83, 2)),
                AbiCodec.rlpBytes(bytesOf(70, 3)));
        final byte[][] entries = new byte[6][];
        for (int i = 0; i < 6; i++) {
            entries[i] = AbiCodec.rlpList(
                    AbiCodec.rlpBytes(bytesOf(32, 500 + i)),
                    AbiCodec.rlpList(
                            AbiCodec.rlpBytes(bytesOf(532, 600 + i)),
                            AbiCodec.rlpBytes(bytesOf(proofNodeBytes, 700 + i))));
        }
        final byte[] storageProofs = AbiCodec.rlpList(entries);
        final byte[] content = AbiCodec.rlpBytes(bytesOf(193, 42));
        // [5] manifest storage proof (list of MPT nodes), [6] manifest preimage (protobuf bytes)
        final byte[] manifestProof =
                AbiCodec.rlpList(AbiCodec.rlpBytes(bytesOf(532, 900)), AbiCodec.rlpBytes(bytesOf(320, 901)));
        final byte[] manifestPreimage = AbiCodec.rlpBytes(bytesOf(manifestPreimageBytes, 902));
        return AbiCodec.rlpList(
                blockHeader, epochHeaders, accountProof, storageProofs, content, manifestProof, manifestPreimage);
    }

    @Test
    void syntheticBundleProofWithManifest_sweep_isOzStrictValidAtEverySize() {
        int firstFail = -1;
        String firstFailMsg = null;
        for (int node = 100; node <= 800; node++) {
            for (final int pre : new int[] {40, 128, 300}) {
                final byte[] proof = syntheticProofWithManifest(node, pre);
                try {
                    ozValidate(proof);
                } catch (final OzRlpInvalid e) {
                    if (firstFail < 0) {
                        firstFail = proof.length;
                        firstFailMsg = "node=" + node + " preimage=" + pre + ": " + e.getMessage();
                    }
                }
            }
        }
        final String msg = firstFail < 0
                ? "all sizes OZ-valid"
                : String.format("7-element proof first rejected by OZ at %d bytes: %s", firstFail, firstFailMsg);
        assertThat(firstFail).as(msg).isEqualTo(-1);
    }
}
