// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

class Secp256k1UtilsTest {

    private static final HexFormat HEX = HexFormat.of();

    // Anvil / Hardhat dev account #0 — a well-known secp256k1 key/address pair.
    private static final byte[] ANVIL_KEY =
            HEX.parseHex("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80");
    private static final String ANVIL_ADDRESS = "f39fd6e51aad88f6f4ce6ab8827279cfffb92266";

    private static byte[] ascii(final String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // ── keccak256 (Ethereum flavour) ───────────────────────────────────────────

    @Test
    void keccak256_emptyInput_matchesKnownVector() {
        assertThat(HEX.formatHex(Secp256k1Utils.keccak256(new byte[0])))
                .isEqualTo("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470");
    }

    @Test
    void keccak256_abc_matchesKnownVector() {
        assertThat(HEX.formatHex(Secp256k1Utils.keccak256(ascii("abc"))))
                .isEqualTo("4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45");
    }

    @Test
    void keccak256_twoArgs_equalsConcatenatedDigest() {
        assertThat(Secp256k1Utils.keccak256(ascii("hello"), ascii("world")))
                .isEqualTo(Secp256k1Utils.keccak256(ascii("helloworld")));
    }

    @Test
    void keccak256_multiBlockInput_matchesSingleCall() {
        // > RATE_BYTES (136) input exercises the full-block absorb loop.
        final byte[] big = new byte[300];
        Arrays.fill(big, (byte) 0xAB);
        assertThat(Secp256k1Utils.keccak256(big)).hasSize(32);
    }

    // ── deriveUncompressedPublicKey ────────────────────────────────────────────

    @Test
    void deriveUncompressedPublicKey_anvilKey_yieldsKnownAddress() {
        final byte[] pub = Secp256k1Utils.deriveUncompressedPublicKey(ANVIL_KEY);
        assertThat(pub).hasSize(64);

        // The Ethereum address is the low 20 bytes of keccak256(uncompressed pubkey).
        final byte[] hash = Secp256k1Utils.keccak256(pub);
        assertThat(HEX.formatHex(Arrays.copyOfRange(hash, 12, 32))).isEqualTo(ANVIL_ADDRESS);
    }

    @Test
    void deriveUncompressedPublicKey_wrongLength_throws() {
        assertThatThrownBy(() -> Secp256k1Utils.deriveUncompressedPublicKey(new byte[31]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("32 bytes");
    }

    @Test
    void deriveUncompressedPublicKey_zeroKey_throws() {
        assertThatThrownBy(() -> Secp256k1Utils.deriveUncompressedPublicKey(new byte[32]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of range");
    }

    @Test
    void deriveUncompressedPublicKey_keyEqualToOrder_throws() {
        final byte[] nBytes = Secp256k1Utils.bigIntTo32Bytes(Secp256k1Utils.N);
        assertThatThrownBy(() -> Secp256k1Utils.deriveUncompressedPublicKey(nBytes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of range");
    }

    // ── sign / recover round trip ──────────────────────────────────────────────

    @Test
    void sign_thenRecover_roundTripsToDerivedPublicKey() {
        final byte[] digest = Secp256k1Utils.keccak256(ascii("clpr round-trip"));
        final byte[] sig = Secp256k1Utils.sign(digest, ANVIL_KEY);
        assertThat(sig).hasSize(65);

        final byte[] recovered = Secp256k1Utils.recoverPublicKey(sig, digest);
        assertThat(recovered).isEqualTo(Secp256k1Utils.deriveUncompressedPublicKey(ANVIL_KEY));
    }

    @Test
    void sign_isDeterministic() {
        final byte[] digest = Secp256k1Utils.keccak256(ascii("deterministic"));
        assertThat(Secp256k1Utils.sign(digest, ANVIL_KEY)).isEqualTo(Secp256k1Utils.sign(digest, ANVIL_KEY));
    }

    @Test
    void sign_producesLowSAndSingleByteRecoveryId() {
        final byte[] digest = Secp256k1Utils.keccak256(ascii("low-s"));
        final byte[] sig = Secp256k1Utils.sign(digest, ANVIL_KEY);

        final BigInteger s = new BigInteger(1, Arrays.copyOfRange(sig, 32, 64));
        assertThat(s).isLessThanOrEqualTo(Secp256k1Utils.N.shiftRight(1));
        assertThat(sig[64] & 0xff).isIn(0, 1);
    }

    @Test
    void sign_wrongDigestLength_throws() {
        assertThatThrownBy(() -> Secp256k1Utils.sign(new byte[31], ANVIL_KEY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Digest must be 32 bytes");
    }

    @Test
    void sign_wrongKeyLength_throws() {
        assertThatThrownBy(() -> Secp256k1Utils.sign(new byte[32], new byte[16]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Private key must be 32 bytes");
    }

    @Test
    void sign_zeroKey_throws() {
        assertThatThrownBy(() -> Secp256k1Utils.sign(new byte[32], new byte[32]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of range");
    }

    // ── recoverPublicKey validation ────────────────────────────────────────────

    @Test
    void recoverPublicKey_wrongSignatureLength_throws() {
        assertThatThrownBy(() -> Secp256k1Utils.recoverPublicKey(new byte[64], new byte[32]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("65 bytes");
    }

    @Test
    void recoverPublicKey_wrongDigestLength_throws() {
        assertThatThrownBy(() -> Secp256k1Utils.recoverPublicKey(new byte[65], new byte[31]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Digest must be 32 bytes");
    }

    @Test
    void recoverPublicKey_unsupportedRecoveryId_returnsNull() {
        final byte[] sig = new byte[65];
        sig[64] = 2; // v > 1 is not a supported recovery id
        assertThat(Secp256k1Utils.recoverPublicKey(sig, new byte[32])).isNull();
    }

    // ── bigIntTo32Bytes ────────────────────────────────────────────────────────

    @Test
    void bigIntTo32Bytes_shortValue_leftPadsWithZeros() {
        final byte[] out = Secp256k1Utils.bigIntTo32Bytes(BigInteger.ONE);
        assertThat(out).hasSize(32);
        assertThat(out[31]).isEqualTo((byte) 1);
        assertThat(Arrays.copyOfRange(out, 0, 31)).containsOnly((byte) 0);
    }

    @Test
    void bigIntTo32Bytes_oversizedValue_takesLowest32Bytes() {
        final byte[] all0xff = new byte[32];
        Arrays.fill(all0xff, (byte) 0xff);
        // new BigInteger(1, 32×0xFF).toByteArray() is 33 bytes (leading sign byte 0x00).
        assertThat(Secp256k1Utils.bigIntTo32Bytes(new BigInteger(1, all0xff))).isEqualTo(all0xff);
    }

    // ── Known-answer vectors ────────────────────────────────────────────────────
    // Independent reference values generated with Foundry `cast` (alloy/k256, a separate
    // audited secp256k1 implementation) so a self-consistent-but-wrong regression in this
    // pure-Java implementation (RFC 6979 k, low-S, recovery id, EC arithmetic) is caught:
    //   - point multiples:  cast wallet public-key --private-key 0x..0{1,2,3}
    //   - signatures:       cast wallet sign --no-hash --private-key <key> <digest>
    //   - keccak:           cast keccak <input>
    // cast returns the Ethereum-legacy recovery id v = 27 + recId; this class emits recId (0/1),
    // so the reference v of 0x1c (28) maps to a trailing 0x01 byte here.

    // Multiples of the secp256k1 generator G (uncompressed X||Y, 64 bytes) — 1G is (GX, GY).
    private static final String G1 = "79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
            + "483ada7726a3c4655da4fbfc0e1108a8fd17b448a68554199c47d08ffb10d4b8";
    private static final String G2 = "c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5"
            + "1ae168fea63dc339a3c58419466ceaeef7f632653266d0e1236431a950cfe52a";
    private static final String G3 = "f9308a019258c31049344f85f89d5229b531c845836f99b08601f113bce036f9"
            + "388f7b0f632de8140fe337e62a37f3566500a99934c2231b6cb9fd7584b8e672";

    private static byte[] scalarKey(final long v) {
        return Secp256k1Utils.bigIntTo32Bytes(BigInteger.valueOf(v));
    }

    private static BigInteger x(final String point128) {
        return new BigInteger(point128.substring(0, 64), 16);
    }

    private static BigInteger y(final String point128) {
        return new BigInteger(point128.substring(64), 16);
    }

    private static String pointHex(final BigInteger[] p) {
        return HEX.formatHex(Secp256k1Utils.bigIntTo32Bytes(p[0]))
                + HEX.formatHex(Secp256k1Utils.bigIntTo32Bytes(p[1]));
    }

    @Test
    void deriveUncompressedPublicKey_smallScalars_matchKnownMultiplesOfG() {
        assertThat(HEX.formatHex(Secp256k1Utils.deriveUncompressedPublicKey(scalarKey(1))))
                .isEqualTo(G1);
        assertThat(HEX.formatHex(Secp256k1Utils.deriveUncompressedPublicKey(scalarKey(2))))
                .isEqualTo(G2);
        assertThat(HEX.formatHex(Secp256k1Utils.deriveUncompressedPublicKey(scalarKey(3))))
                .isEqualTo(G3);
    }

    @Test
    void pointMultiply_smallScalars_matchKnownMultiplesOfG() {
        assertThat(pointHex(Secp256k1Utils.pointMultiply(x(G1), y(G1), BigInteger.TWO)))
                .isEqualTo(G2);
        assertThat(pointHex(Secp256k1Utils.pointMultiply(x(G1), y(G1), BigInteger.valueOf(3))))
                .isEqualTo(G3);
    }

    @Test
    void pointAdd_doublingAndDistinctAddends_matchKnownMultiplesOfG() {
        // Doubling path: G + G = 2G.
        assertThat(pointHex(Secp256k1Utils.pointAdd(x(G1), y(G1), x(G1), y(G1))))
                .isEqualTo(G2);
        // Distinct-addends path: 2G + G = 3G.
        assertThat(pointHex(Secp256k1Utils.pointAdd(x(G2), y(G2), x(G1), y(G1))))
                .isEqualTo(G3);
    }

    @Test
    void keccak256_multiBlockInput_matchesKnownVector() {
        // 200 bytes of 0xAB spans two absorb blocks (> RATE_BYTES = 136).
        final byte[] big = new byte[200];
        Arrays.fill(big, (byte) 0xAB);
        assertThat(HEX.formatHex(Secp256k1Utils.keccak256(big)))
                .isEqualTo("8ca353cee0a5c5b999a0916e88da37a16293cf14a73c735d6b5197b50c3d6656");
    }

    @Test
    void sign_matchesIndependentKat_anvilKey() {
        final byte[] digest = HEX.parseHex("74908806344e2b0a7d20a03a2d4944ebfa243d77d20ef87564635607b144351d");
        final byte[] sig = Secp256k1Utils.sign(digest, ANVIL_KEY);
        assertThat(HEX.formatHex(sig))
                .isEqualTo("a5457d3ef44d5f3ff00cd417d937ecf927076c864185372f0f390cb3d6b48b26"
                        + "5893294a59065e53b00da2bbe6f1beb8d4473169e91bbcc22fe916601ab8a22d"
                        + "01");
        // The recovery id in the KAT must recover exactly the signer's public key.
        assertThat(Secp256k1Utils.recoverPublicKey(sig, digest))
                .isEqualTo(Secp256k1Utils.deriveUncompressedPublicKey(ANVIL_KEY));
    }

    @Test
    void sign_matchesIndependentKat_scalarTwoKey() {
        final byte[] digest = HEX.parseHex("aacd1f84ac88c9124e4dc1b4b14475414d197f1ae9ec15ad96f34da5077adb12");
        final byte[] sig = Secp256k1Utils.sign(digest, scalarKey(2));
        assertThat(HEX.formatHex(sig))
                .isEqualTo("cd38443023aeb07f83d215c6a851fb19077e1f8637acce3dd66ab6b4e6eb27ae"
                        + "3281ba59eff3719b0608778d61ecb96a6b4db0e231e81cff6f58f76eac2acf33"
                        + "01");
        assertThat(Secp256k1Utils.recoverPublicKey(sig, digest))
                .isEqualTo(Secp256k1Utils.deriveUncompressedPublicKey(scalarKey(2)));
    }

    @Test
    void recoverPublicKeyFromComponents_rNotOnCurve_returnsNull() {
        // x = 5 is not a valid secp256k1 x-coordinate: x^3 + 7 is a quadratic non-residue mod p,
        // so no point has this x and recovery must fail cleanly (null) rather than return garbage.
        assertThat(Secp256k1Utils.recoverPublicKeyFromComponents(
                        BigInteger.valueOf(5L), BigInteger.TWO, BigInteger.ONE, 0))
                .isNull();
    }
}
