// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link EthSigner}.
 *
 * <p>Uses the Besu dev-mode account as a well-known test vector:
 * <ul>
 *   <li>Private key: {@code 8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63}</li>
 *   <li>Address: {@code 0xfe3b557e8fb62b89f4916b721be55ceb828dbd73}</li>
 * </ul>
 */
class EthSignerTest {

    /** Besu dev-mode account 0 private key (well-known public test vector). */
    private static final String DEV_PRIVATE_KEY = "8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63";

    /** Expected Ethereum address for the above key. */
    private static final String DEV_ADDRESS = "0xfe3b557e8fb62b89f4916b721be55ceb828dbd73";

    // -------------------------------------------------------------------------
    // Address derivation
    // -------------------------------------------------------------------------

    @Test
    void addressDerivedCorrectly_withoutPrefix() {
        final EthSigner signer = new EthSigner(DEV_PRIVATE_KEY);
        assertThat(signer.address()).isEqualTo(DEV_ADDRESS);
    }

    @Test
    void addressDerivedCorrectly_withPrefix() {
        final EthSigner signer = new EthSigner("0x" + DEV_PRIVATE_KEY);
        assertThat(signer.address()).isEqualTo(DEV_ADDRESS);
    }

    @Test
    void rejectsZeroKey() {
        assertThatThrownBy(() -> new EthSigner("00".repeat(32))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsKeyEqualToN() {
        // N = curve order — just above the valid range
        final String nHex = EthSigner.N.toString(16);
        assertThatThrownBy(() -> new EthSigner(nHex)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void derivePublicKey_matchesKnownKeyPair() {
        // Known-answer vector: reference public key generated independently with Foundry
        // `cast wallet public-key --private-key 0x<DEV_PRIVATE_KEY>` (alloy/k256).
        final byte[] pub = EthSigner.derivePublicKey(DEV_PRIVATE_KEY);
        assertThat(pub).hasSize(64);
        assertThat(HexFormat.of().formatHex(pub))
                .isEqualTo("09b02f8a5fddd222ade4ea4528faefc399623af3f736be3c44f03e2df22fb792"
                        + "f3931a4d9573d333ca74343305762a753388c3422a86d98b713fc91c1ea04842");
        // The 0x prefix is accepted and yields the same key; and the address is the low 20 bytes
        // of keccak256(pubKey), tying this vector back to DEV_ADDRESS.
        assertThat(EthSigner.derivePublicKey("0x" + DEV_PRIVATE_KEY)).isEqualTo(pub);
        final byte[] hash = AbiCodec.Keccak256.keccak256(pub);
        assertThat("0x" + HexFormat.of().formatHex(hash, 12, 32)).isEqualTo(DEV_ADDRESS);
    }

    // -------------------------------------------------------------------------
    // ECDSA signature properties
    // -------------------------------------------------------------------------

    @Test
    void signatureHasCorrectStructure() {
        final EthSigner signer = new EthSigner(DEV_PRIVATE_KEY);
        final byte[] hash = AbiCodec.Keccak256.keccak256("hello world".getBytes());
        final BigInteger[] sig = signer.ecdsaSign(hash);

        assertThat(sig).hasSize(3);
        final BigInteger r = sig[0];
        final BigInteger s = sig[1];
        final BigInteger recId = sig[2];

        // r and s must be in [1, n-1]
        assertThat(r.signum()).isGreaterThan(0);
        assertThat(r.compareTo(EthSigner.N)).isLessThan(0);
        assertThat(s.signum()).isGreaterThan(0);
        assertThat(s.compareTo(EthSigner.N)).isLessThan(0);

        // Low-S: s <= n/2
        assertThat(s.compareTo(EthSigner.N.shiftRight(1))).isLessThanOrEqualTo(0);

        // recoveryId is 0 or 1
        assertThat(recId.longValue()).isBetween(0L, 1L);
    }

    @Test
    void signatureIsDeterministic() {
        final EthSigner signer = new EthSigner(DEV_PRIVATE_KEY);
        final byte[] hash = AbiCodec.Keccak256.keccak256("determinism test".getBytes());

        final BigInteger[] sig1 = signer.ecdsaSign(hash);
        final BigInteger[] sig2 = signer.ecdsaSign(hash);

        assertThat(sig1[0]).isEqualTo(sig2[0]); // r
        assertThat(sig1[1]).isEqualTo(sig2[1]); // s
        assertThat(sig1[2]).isEqualTo(sig2[2]); // recId
    }

    @Test
    void differentMessagesProduceDifferentSignatures() {
        final EthSigner signer = new EthSigner(DEV_PRIVATE_KEY);
        final byte[] hash1 = AbiCodec.Keccak256.keccak256("msg1".getBytes());
        final byte[] hash2 = AbiCodec.Keccak256.keccak256("msg2".getBytes());

        final BigInteger[] sig1 = signer.ecdsaSign(hash1);
        final BigInteger[] sig2 = signer.ecdsaSign(hash2);

        // r values should differ (they always do for different messages with RFC 6979)
        assertThat(sig1[0]).isNotEqualTo(sig2[0]);
    }

    // -------------------------------------------------------------------------
    // signEip1559Transaction — structural checks
    // -------------------------------------------------------------------------

    @Test
    void signEip1559Transaction_returnsNonEmptyBytes() {
        final EthSigner signer = new EthSigner(DEV_PRIVATE_KEY);
        final byte[] rawTx = signer.signEip1559Transaction(
                0L, // nonce
                2_000_000_000L, // maxPriorityFeePerGas (2 gwei)
                30_000_000_000L, // maxFeePerGas (30 gwei)
                100_000L, // gasLimit
                "0x1234567890123456789012345678901234567890", // to
                0L, // value
                new byte[] {0x12, 0x34}, // data
                1L // chainId
                );

        assertThat(rawTx).isNotNull();
        assertThat(rawTx.length).isGreaterThan(10);
    }

    @Test
    void signEip1559Transaction_startsWith0x02() {
        final EthSigner signer = new EthSigner(DEV_PRIVATE_KEY);
        final byte[] rawTx = signer.signEip1559Transaction(
                0L,
                2_000_000_000L,
                30_000_000_000L,
                100_000L,
                "0x1234567890123456789012345678901234567890",
                0L,
                new byte[0],
                1L);

        // EIP-1559 envelope begins with the type byte 0x02
        assertThat(rawTx[0]).isEqualTo((byte) 0x02);
        // Second byte must be an RLP list header (0xc0..0xff range)
        assertThat(rawTx[1] & 0xff).isGreaterThanOrEqualTo(0xc0);
    }

    @Test
    void signEip1559Transaction_isDeterministic() {
        final EthSigner signer = new EthSigner(DEV_PRIVATE_KEY);

        final byte[] rawTx1 = signer.signEip1559Transaction(
                5L,
                1_500_000_000L,
                25_000_000_000L,
                200_000L,
                "0x1234567890123456789012345678901234567890",
                0L,
                new byte[0],
                1L);
        final byte[] rawTx2 = signer.signEip1559Transaction(
                5L,
                1_500_000_000L,
                25_000_000_000L,
                200_000L,
                "0x1234567890123456789012345678901234567890",
                0L,
                new byte[0],
                1L);

        assertThat(rawTx1).isEqualTo(rawTx2);
    }

    @Test
    void signEip1559Transaction_differentNoncesProduceDifferentTx() {
        final EthSigner signer = new EthSigner(DEV_PRIVATE_KEY);

        final byte[] rawTx1 = signer.signEip1559Transaction(
                0L,
                1_500_000_000L,
                25_000_000_000L,
                100_000L,
                "0x1234567890123456789012345678901234567890",
                0L,
                new byte[0],
                1L);
        final byte[] rawTx2 = signer.signEip1559Transaction(
                1L,
                1_500_000_000L,
                25_000_000_000L,
                100_000L,
                "0x1234567890123456789012345678901234567890",
                0L,
                new byte[0],
                1L);

        assertThat(rawTx1).isNotEqualTo(rawTx2);
    }

    /**
     * Cross-check our EIP-1559 output byte-for-byte against ethers.js v6. The signed bytes were
     * generated by:
     * <pre>
     *   const wallet = new Wallet("0x" + DEV_PRIVATE_KEY);
     *   wallet.signTransaction({
     *     type: 2, chainId: 1n, nonce: 7,
     *     maxPriorityFeePerGas: 2_000_000_000n, maxFeePerGas: 30_000_000_000n,
     *     gasLimit: 100_000n,
     *     to: "0x1234567890123456789012345678901234567890",
     *     value: 0n, data: "0xdeadbeef", accessList: [],
     *   });
     * </pre>
     * Byte-identity confirms (1) correct RLP layout, (2) correct signing-hash construction
     * (type byte prepended), and (3) RFC 6979 produces the same k as ethers' deterministic ECDSA.
     */
    @Test
    void signEip1559Transaction_matchesEthersJsVector() {
        final EthSigner signer = new EthSigner(DEV_PRIVATE_KEY);
        final byte[] rawTx = signer.signEip1559Transaction(
                7L, // nonce
                2_000_000_000L, // maxPriorityFeePerGas
                30_000_000_000L, // maxFeePerGas
                100_000L, // gasLimit
                "0x1234567890123456789012345678901234567890", // to
                0L, // value
                new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef}, // data
                1L // chainId
                );

        final String expectedHex = "02f870010784773594008506fc23ac00830186a09412345678901234567890123456789012345"
                + "678908084deadbeefc080a0817f2909c8d50ed289fe96fe6a3866ad396c3f4cbbd0393e9218c0b70ee22d52a06725"
                + "0c6a390c886d14c97d338bfd5f970240143ec19218518f9ddf3db56692c8";
        assertThat(AbiCodec.toHexNoPrefix(rawTx)).isEqualTo(expectedHex);
    }

    /**
     * Cross-check the contract-creation case ({@code to == null}) byte-for-byte against ethers.js
     * v6. The {@code to} field is RLP-encoded as the empty byte string ({@code 0x80}) per EIP-1559.
     */
    @Test
    void signEip1559Transaction_contractCreation_matchesEthersJsVector() {
        final EthSigner signer = new EthSigner(DEV_PRIVATE_KEY);
        final byte[] rawTx = signer.signEip1559Transaction(
                7L,
                2_000_000_000L,
                30_000_000_000L,
                100_000L,
                null, // contract creation
                0L,
                new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef},
                1L);

        final String expectedHex = "02f85c010784773594008506fc23ac00830186a0808084deadbeefc080a081d7b6b0c7f8b58b0"
                + "cf7cf7aa4e1cb30f59de8147e2a0ec348c57f86c2d2acf2a0103dff660dc55c8bafddc82ef18c64c374c2e1c2f9d"
                + "22827950a840b686ea1e5";
        assertThat(AbiCodec.toHexNoPrefix(rawTx)).isEqualTo(expectedHex);
    }

    // -------------------------------------------------------------------------
    // signMessage — cross-check against ethers.js
    // -------------------------------------------------------------------------

    /**
     * Cross-check our {@link EthSigner#signMessage(byte[])} output byte-for-byte against ethers.js
     * v6, which produces the same bytes Anvil/Besu's {@code eth_sign} JSON-RPC method does.
     * Generated by:
     * <pre>
     *   const wallet = new Wallet("0x" + DEV_PRIVATE_KEY);
     *   wallet.signMessage(Uint8Array.from(Buffer.from(msgHashHex, "hex")));
     * </pre>
     */
    @Test
    void signMessage_matchesEthersJsVector() {
        final EthSigner signer = new EthSigner(DEV_PRIVATE_KEY);
        final byte[] msgHash = AbiCodec.fromHex("0x1122334455667788112233445566778811223344556677881122334455667788");

        final byte[] sig = signer.signMessage(msgHash);

        final String expectedHex =
                "d19a620f5b6a48ad5105cc6266f69eaf4abad4bc9980eb53f641f1fe0b837920647468596ed5681cfe9bf180a57ea4c4da45b4ca3d772fa66bdf55450724beb41c";
        assertThat(AbiCodec.toHexNoPrefix(sig)).isEqualTo(expectedHex);
    }

    // -------------------------------------------------------------------------
    // RLP encoding helpers
    // -------------------------------------------------------------------------

    @Test
    void rlpEncodeEmptyList_returnsC0() {
        assertThat(EthSigner.rlpEncodeEmptyList()).containsExactly(0xc0);
    }

    @Test
    void rlpEncodeUnsignedLong_zero() {
        final byte[] enc = EthSigner.rlpEncodeUnsignedLong(0L);
        // RLP for zero is 0x80 (empty byte string)
        assertThat(enc).containsExactly(0x80);
    }

    @Test
    void rlpEncodeUnsignedLong_oneByte() {
        final byte[] enc = EthSigner.rlpEncodeUnsignedLong(0x7fL);
        // Single byte below 0x80 is its own encoding
        assertThat(enc).containsExactly(0x7f);
    }

    @Test
    void rlpEncodeUnsignedLong_twoBytes() {
        final byte[] enc = EthSigner.rlpEncodeUnsignedLong(0x100L);
        // 0x82 (length=2) || 0x01 0x00
        assertThat(enc).containsExactly(0x82, 0x01, 0x00);
    }
}
