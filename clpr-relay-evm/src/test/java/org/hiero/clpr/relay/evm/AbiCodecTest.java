// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

class AbiCodecTest {

    // ------------------------------------------------------------------
    // Keccak-256
    // ------------------------------------------------------------------

    @Test
    void keccak256_emptyInput() {
        final byte[] hash = AbiCodec.Keccak256.keccak256(new byte[0]);
        // Known digest of keccak256("")
        assertThat(AbiCodec.toHex(hash))
                .isEqualTo("0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470");
    }

    @Test
    void keccak256_abc() {
        final byte[] hash = AbiCodec.Keccak256.keccak256("abc".getBytes(StandardCharsets.UTF_8));
        // Known digest of keccak256("abc")
        assertThat(AbiCodec.toHex(hash))
                .isEqualTo("0x4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45");
    }

    @Test
    void keccak256_longInput() {
        // A message strictly longer than the rate (136 bytes) forces multi-block absorption.
        final byte[] in = new byte[200];
        for (int i = 0; i < in.length; i++) {
            in[i] = (byte) i;
        }
        final byte[] hash = AbiCodec.Keccak256.keccak256(in);
        assertThat(hash).hasSize(32);
    }

    // ------------------------------------------------------------------
    // Function selectors
    // ------------------------------------------------------------------

    @Test
    void functionSelector_transfer() {
        // Well-known: keccak256("transfer(address,uint256)")[0:4] == 0xa9059cbb
        final byte[] sel = AbiCodec.functionSelector("transfer(address,uint256)");
        assertThat(AbiCodec.toHex(sel)).isEqualTo("0xa9059cbb");
    }

    @Test
    void encodeFunctionCall_submitBundleSelectorPrefix() {
        final byte[] connId = new byte[32];
        final byte[] proof = new byte[] {1, 2, 3};

        final byte[] call = AbiCodec.encodeSubmitBundle(connId, proof);
        final byte[] expectedSel = AbiCodec.functionSelector("submitBundle(bytes32,bytes)");

        assertThat(call.length).isGreaterThanOrEqualTo(4);
        final byte[] firstFour = new byte[4];
        System.arraycopy(call, 0, firstFour, 0, 4);
        assertThat(firstFour).isEqualTo(expectedSel);
    }

    @Test
    void encodeGetEndpointManifest_isSelectorOnly() {
        // Well-known: keccak256("getEndpointManifest()")[0:4] == 0x0971e3db (cast sig).
        final byte[] call = AbiCodec.encodeGetEndpointManifest();
        assertThat(AbiCodec.toHex(call)).isEqualTo("0x0971e3db");
    }

    @Test
    void encodeGetPeerEndpointManifest_selectorPlusChannelId() {
        // Well-known: keccak256("getPeerEndpointManifest(bytes32)")[0:4] == 0xcd20affb (cast sig).
        final byte[] connId = new byte[32];
        connId[31] = 0x2a;
        final byte[] call = AbiCodec.encodeGetPeerEndpointManifest(connId);

        assertThat(call).hasSize(4 + 32);
        final byte[] firstFour = new byte[4];
        System.arraycopy(call, 0, firstFour, 0, 4);
        assertThat(AbiCodec.toHex(firstFour)).isEqualTo("0xcd20affb");
        // The 32-byte channelId argument follows the selector verbatim.
        assertThat(java.util.Arrays.copyOfRange(call, 4, 36)).isEqualTo(connId);
    }

    @Test
    void encodeGetPeerEndpointManifest_rejectsNon32ByteChannelId() {
        assertThatThrownBy(() -> AbiCodec.encodeGetPeerEndpointManifest(new byte[31]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------------
    // encodeUint / decodeUint64
    // ------------------------------------------------------------------

    @Test
    void encodeUint_zero() {
        final byte[] enc = AbiCodec.encodeUint(0L);
        assertThat(enc).hasSize(32);
        for (final byte b : enc) {
            assertThat(b).isEqualTo((byte) 0);
        }
    }

    @Test
    void encodeUint_one() {
        final byte[] enc = AbiCodec.encodeUint(1L);
        assertThat(enc).hasSize(32);
        assertThat(enc[31]).isEqualTo((byte) 1);
        for (int i = 0; i < 31; i++) {
            assertThat(enc[i]).isEqualTo((byte) 0);
        }
    }

    @Test
    void encodeUint_longMax() {
        final byte[] enc = AbiCodec.encodeUint(Long.MAX_VALUE);
        assertThat(enc).hasSize(32);
        // Top 24 bytes zero, byte 24 = 0x7f, rest = 0xff
        for (int i = 0; i < 24; i++) {
            assertThat(enc[i]).isEqualTo((byte) 0);
        }
        assertThat(enc[24]).isEqualTo((byte) 0x7f);
        for (int i = 25; i < 32; i++) {
            assertThat(enc[i]).isEqualTo((byte) 0xff);
        }
    }

    @Test
    void encodeUint_rejectsNegative() {
        assertThatThrownBy(() -> AbiCodec.encodeUint(-1L)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void encodeDecodeUint_roundtrip() {
        for (final long v : new long[] {0L, 1L, 255L, 256L, 0xdeadbeefL, Long.MAX_VALUE}) {
            final byte[] enc = AbiCodec.encodeUint(v);
            assertThat(AbiCodec.decodeUint64(enc, 0)).isEqualTo(v);
        }
    }

    // ------------------------------------------------------------------
    // decodeUint96
    // ------------------------------------------------------------------

    /** Pack {@code value} (assumed to fit in 96 bits) into the low 12 bytes of a 32-byte word. */
    private static byte[] uint96Word(final BigInteger value) {
        final byte[] word = new byte[32];
        BigInteger v = value;
        for (int i = 31; i >= 20; i--) {
            word[i] = v.byteValue();
            v = v.shiftRight(8);
        }
        return word;
    }

    @Test
    void decodeUint96_zero() {
        assertThat(AbiCodec.decodeUint96(new byte[32], 0)).isEqualTo(BigInteger.ZERO);
    }

    @Test
    void decodeUint96_smallValueReadsLow12Bytes() {
        final byte[] word = new byte[32];
        word[31] = 0x05;
        assertThat(AbiCodec.decodeUint96(word, 0)).isEqualTo(BigInteger.valueOf(5));
    }

    @Test
    void decodeUint96_maxValueIsNonNegative() {
        // All twelve low bytes 0xff → 2^96 - 1. The whole point of the BigInteger(1, ...) path is
        // that a set high bit decodes as a positive value, not a negative two's-complement one.
        final byte[] word = new byte[32];
        for (int i = 20; i < 32; i++) {
            word[i] = (byte) 0xff;
        }
        final BigInteger expected = BigInteger.ONE.shiftLeft(96).subtract(BigInteger.ONE);
        final BigInteger decoded = AbiCodec.decodeUint96(word, 0);
        assertThat(decoded).isEqualTo(expected);
        assertThat(decoded.signum()).isEqualTo(1);
    }

    @Test
    void decodeUint96_ignoresUpper20Bytes() {
        // Upper 20 bytes are garbage; only the low 12 must be read. Mirrors the contract storing a
        // uint96 right-aligned in a 32-byte ABI word with non-zero (but irrelevant) high bytes.
        final byte[] word = new byte[32];
        for (int i = 0; i < 20; i++) {
            word[i] = (byte) 0xff;
        }
        word[31] = 0x2a; // low 12 bytes encode 42
        assertThat(AbiCodec.decodeUint96(word, 0)).isEqualTo(BigInteger.valueOf(42));
    }

    @Test
    void decodeUint96_valueExceedingLongMax() {
        // 2^80 does not fit in a Java long — exercises the reason this returns BigInteger.
        final BigInteger value = BigInteger.ONE.shiftLeft(80);
        assertThat(AbiCodec.decodeUint96(uint96Word(value), 0)).isEqualTo(value);
    }

    @Test
    void decodeUint96_readsAtOffset() {
        final BigInteger value = BigInteger.valueOf(0xdeadbeefL);
        final byte[] buffer = new byte[64];
        System.arraycopy(uint96Word(value), 0, buffer, 32, 32);
        assertThat(AbiCodec.decodeUint96(buffer, 32)).isEqualTo(value);
    }

    @Test
    void decodeUint96_agreesWithEncodeUintOverLongRange() {
        // A uint256-encoded long is right-aligned, so its low 12 bytes equal the value; decodeUint96
        // must therefore agree with decodeUint64 across the long range.
        for (final long v : new long[] {0L, 1L, 255L, 256L, 0xdeadbeefL, Long.MAX_VALUE}) {
            assertThat(AbiCodec.decodeUint96(AbiCodec.encodeUint(v), 0)).isEqualTo(BigInteger.valueOf(v));
        }
    }

    @Test
    void decodeUint96_outOfRangeThrows() {
        assertThatThrownBy(() -> AbiCodec.decodeUint96(new byte[32], 1)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    // ------------------------------------------------------------------
    // encodeBytes32 / decodeBytes32
    // ------------------------------------------------------------------

    @Test
    void encodeBytes32_roundtrip() {
        final byte[] value = new byte[32];
        for (int i = 0; i < 32; i++) {
            value[i] = (byte) (i * 7);
        }
        final byte[] enc = AbiCodec.encodeBytes32(value);
        assertThat(enc).isEqualTo(value);
        final byte[] dec = AbiCodec.decodeBytes32(enc, 0);
        assertThat(dec).isEqualTo(value);
    }

    @Test
    void encodeBytes32_rejectsWrongLength() {
        assertThatThrownBy(() -> AbiCodec.encodeBytes32(new byte[16])).isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------------
    // encodeBytes (dynamic)
    // ------------------------------------------------------------------

    @Test
    void encodeBytes_padsToMultipleOf32() {
        final byte[] data = new byte[] {1, 2, 3};
        final byte[] enc = AbiCodec.encodeBytes(data);
        // 32 bytes length + 32 bytes padded data
        assertThat(enc).hasSize(64);
        // First 32 bytes: length = 3
        assertThat(enc[31]).isEqualTo((byte) 3);
        // Next three bytes: data
        assertThat(enc[32]).isEqualTo((byte) 1);
        assertThat(enc[33]).isEqualTo((byte) 2);
        assertThat(enc[34]).isEqualTo((byte) 3);
        // Remaining 29 bytes: zero
        for (int i = 35; i < 64; i++) {
            assertThat(enc[i]).isEqualTo((byte) 0);
        }
    }

    // ------------------------------------------------------------------
    // decodeDynamicBytes / decodeString roundtrip inside a fabricated head+tail
    // ------------------------------------------------------------------

    @Test
    void decodeDynamicBytes_fromHeadTail() {
        // Layout: [head: offset][tail: length|data padded]
        final byte[] payload = "hello-world".getBytes(StandardCharsets.UTF_8);
        final byte[] head = AbiCodec.encodeUint(32L); // offset to tail
        final byte[] tail = AbiCodec.encodeBytes(payload);

        final byte[] combined = new byte[head.length + tail.length];
        System.arraycopy(head, 0, combined, 0, head.length);
        System.arraycopy(tail, 0, combined, head.length, tail.length);

        assertThat(AbiCodec.decodeDynamicBytes(combined, 0)).isEqualTo(payload);
        assertThat(AbiCodec.decodeString(combined, 0)).isEqualTo("hello-world");
    }

    // ------------------------------------------------------------------
    // decodeAddress
    // ------------------------------------------------------------------

    @Test
    void decodeAddress_rightAligned() {
        final byte[] word = new byte[32];
        // Last 20 bytes are the address; first 12 are zero padding.
        for (int i = 0; i < 20; i++) {
            word[12 + i] = (byte) (0xa0 + i);
        }
        final String addr = AbiCodec.decodeAddress(word, 0);
        assertThat(addr).startsWith("0x").hasSize(42);
        assertThat(addr.substring(2, 4)).isEqualTo("a0");
    }

    // ------------------------------------------------------------------
    // decodeOffsetOrLength (hardened uint256 offset/length narrowing)
    // ------------------------------------------------------------------

    @Test
    void decodeOffsetOrLength_smallValue() {
        final byte[] word = AbiCodec.encodeUint256(BigInteger.valueOf(0x20));
        assertThat(AbiCodec.decodeOffsetOrLength(word, 0)).isEqualTo(0x20);
    }

    @Test
    void decodeOffsetOrLength_zero() {
        assertThat(AbiCodec.decodeOffsetOrLength(new byte[32], 0)).isZero();
    }

    @Test
    void decodeOffsetOrLength_maxIntIsAccepted() {
        final byte[] word = AbiCodec.encodeUint256(BigInteger.valueOf(Integer.MAX_VALUE));
        assertThat(AbiCodec.decodeOffsetOrLength(word, 0)).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void decodeOffsetOrLength_maxIntPlusOne_throws() {
        final byte[] word = AbiCodec.encodeUint256(BigInteger.valueOf(Integer.MAX_VALUE + 1L));
        assertThatThrownBy(() -> AbiCodec.decodeOffsetOrLength(word, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void decodeOffsetOrLength_lowWordHighBitSet_wouldWrapUnderDecodeUint64_throws() {
        // Low 64 bits = 2^63 → (int) decodeUint64 would wrap to a large negative int that can slip past
        // a naive bounds check; decodeOffsetOrLength rejects it instead.
        final byte[] word = new byte[32];
        word[24] = (byte) 0x80; // sets bit 63 of the low-64-bit word
        assertThatThrownBy(() -> AbiCodec.decodeOffsetOrLength(word, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void decodeOffsetOrLength_upperBitsSet_decodeUint64WouldDropThem_throws() {
        // A byte in the upper 192 bits (which decodeUint64 never reads) makes this a huge uint256.
        final byte[] word = new byte[32];
        word[0] = 0x01; // most-significant byte → value ~ 2^248
        assertThatThrownBy(() -> AbiCodec.decodeOffsetOrLength(word, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void decodeOffsetOrLength_allOnes_throws() {
        final byte[] word = new byte[32];
        Arrays.fill(word, (byte) 0xff);
        assertThatThrownBy(() -> AbiCodec.decodeOffsetOrLength(word, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void decodeOffsetOrLength_readsAtOffset() {
        final byte[] buf = new byte[64];
        System.arraycopy(AbiCodec.encodeUint256(BigInteger.valueOf(5)), 0, buf, 32, 32);
        assertThat(AbiCodec.decodeOffsetOrLength(buf, 32)).isEqualTo(5);
    }

    @Test
    void decodeOffsetOrLength_wordOutOfRange_throws() {
        assertThatThrownBy(() -> AbiCodec.decodeOffsetOrLength(new byte[16], 0))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    // ------------------------------------------------------------------
    // Hex helpers
    // ------------------------------------------------------------------

    @Test
    void toHex_fromHex_roundtrip() {
        final byte[] input = new byte[] {0x00, 0x0f, (byte) 0xff, 0x42};
        final String hex = AbiCodec.toHex(input);
        assertThat(hex).isEqualTo("0x000fff42");
        assertThat(AbiCodec.fromHex(hex)).isEqualTo(input);
        assertThat(AbiCodec.fromHex("000fff42")).isEqualTo(input);
    }

    @Test
    void fromHex_oddLength_throws() {
        assertThatThrownBy(() -> AbiCodec.fromHex("0xabc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("odd length");
    }

    @Test
    void fromHex_invalidCharacter_throws() {
        assertThatThrownBy(() -> AbiCodec.fromHex("0xzz"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid hex character");
    }

    // ------------------------------------------------------------------
    // channelId / head-param length guards
    // ------------------------------------------------------------------

    @Test
    void encodeFunctionCall_rejectsNon32HeadParam() {
        assertThatThrownBy(() -> AbiCodec.encodeFunctionCall("f(bytes32)", new byte[31]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("head param must be 32 bytes");
    }

    @Test
    void encodeSubmitBundle_encodesOffsetAndLength() {
        final byte[] cid = new byte[32];
        cid[31] = 0x07;
        final byte[] proof = new byte[] {(byte) 0xaa, (byte) 0xbb};
        final byte[] encoded = AbiCodec.encodeSubmitBundle(cid, proof);

        // selector(4) + channelId(32) + offset(32) + length(32) + padded proof(32) = 132
        assertThat(encoded).hasSize(132);
        // offset word == 0x40 (64) in its last byte
        assertThat(encoded[4 + 32 + 31]).isEqualTo((byte) 0x40);
        // length word == 2
        assertThat(encoded[4 + 64 + 31]).isEqualTo((byte) 0x02);
    }

    @Test
    void encodeSubmitBundle_rejectsNon32ChannelId() {
        assertThatThrownBy(() -> AbiCodec.encodeSubmitBundle(new byte[31], new byte[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("channelId must be 32 bytes");
    }

    @Test
    void encodeGetMessage_rejectsNon32ChannelId() {
        assertThatThrownBy(() -> AbiCodec.encodeGetMessage(new byte[31], 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("channelId must be 32 bytes");
    }

    @Test
    void encodeGetChannel_rejectsNon32ChannelId() {
        assertThatThrownBy(() -> AbiCodec.encodeGetChannel(new byte[31]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("channelId must be 32 bytes");
    }

    @Test
    void encodeGetPeerEndpointManifest_rejectsNon32ChannelId() {
        assertThatThrownBy(() -> AbiCodec.encodeGetPeerEndpointManifest(new byte[31]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("channelId must be 32 bytes");
    }

    @Test
    void encodeGetMessage_prependsSelectorAndArgs() {
        final byte[] encoded = AbiCodec.encodeGetMessage(new byte[32], 5L);
        // selector(4) + channelId(32) + messageId(32)
        assertThat(encoded).hasSize(68);
        assertThat(encoded[67]).isEqualTo((byte) 5);
    }

    // ------------------------------------------------------------------
    // encodeUint256
    // ------------------------------------------------------------------

    @Test
    void encodeUint256_encodesRightAlignedValue() {
        final byte[] out = AbiCodec.encodeUint256(BigInteger.valueOf(255));
        assertThat(out).hasSize(32);
        assertThat(out[31]).isEqualTo((byte) 0xff);
        assertThat(out[30]).isEqualTo((byte) 0x00);
    }

    @Test
    void encodeUint256_fullWidthValue_stripsSignByte() {
        // 2^256 - 1: toByteArray() is 33 bytes (leading sign 0x00); encoder must drop it.
        final BigInteger max = BigInteger.TWO.pow(256).subtract(BigInteger.ONE);
        final byte[] out = AbiCodec.encodeUint256(max);
        final byte[] expected = new byte[32];
        java.util.Arrays.fill(expected, (byte) 0xff);
        assertThat(out).isEqualTo(expected);
    }

    @Test
    void encodeUint256_rejectsNegative() {
        assertThatThrownBy(() -> AbiCodec.encodeUint256(BigInteger.valueOf(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("negative");
    }

    @Test
    void encodeUint256_rejectsValueExceeding256Bits() {
        assertThatThrownBy(() -> AbiCodec.encodeUint256(BigInteger.TWO.pow(256)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds 256 bits");
    }

    // ------------------------------------------------------------------
    // address helpers
    // ------------------------------------------------------------------

    @Test
    void encodeAddress_leftPadsTo32Bytes() {
        final byte[] out = AbiCodec.encodeAddress("0x" + "ab".repeat(20));
        assertThat(out).hasSize(32);
        assertThat(HexFormat.of().formatHex(out)).isEqualTo("0".repeat(24) + "ab".repeat(20));
    }

    @Test
    void encodeAddress_rejectsNon20Byte() {
        assertThatThrownBy(() -> AbiCodec.encodeAddress("0xabcd"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("address must be 20 bytes");
    }

    @Test
    void predictContractAddress_matchesKnownCreateVector() {
        // Canonical Ethereum CREATE vector: keccak256(rlp([sender, nonce]))[12:].
        final String predicted = AbiCodec.predictContractAddress("0x6ac7ea33f8831ea9dcc53393aaa88b25a785dbf0", 0);
        assertThat(predicted).isEqualTo("0xcd234a471b72ba2f1ccf0a70fcaba648a5eecd8d");
    }

    @Test
    void predictContractAddress_rejectsNegativeNonce() {
        assertThatThrownBy(() -> AbiCodec.predictContractAddress("0x" + "ab".repeat(20), -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nonce must be non-negative");
    }

    @Test
    void predictContractAddress_rejectsNon20ByteSender() {
        assertThatThrownBy(() -> AbiCodec.predictContractAddress("0xabcd", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sender must be 20 bytes");
    }

    // ------------------------------------------------------------------
    // public RLP helpers
    // ------------------------------------------------------------------

    @Test
    void rlpBigInt_zeroAndNull_encodeAsEmptyString() {
        assertThat(AbiCodec.rlpBigInt(BigInteger.ZERO)).isEqualTo(new byte[] {(byte) 0x80});
        assertThat(AbiCodec.rlpBigInt(null)).isEqualTo(new byte[] {(byte) 0x80});
    }

    @Test
    void rlpBigInt_valueWithHighBit_stripsSignByte() {
        // 0x80 has its high bit set, so BigInteger prepends a sign byte that must be stripped.
        assertThat(AbiCodec.rlpBigInt(BigInteger.valueOf(0x80))).isEqualTo(new byte[] {(byte) 0x81, (byte) 0x80});
    }

    @Test
    void rlpBytes_singleLowByte_isItsOwnEncoding() {
        assertThat(AbiCodec.rlpBytes(new byte[] {0x7f})).isEqualTo(new byte[] {0x7f});
    }

    @Test
    void rlpList_prependsListHeader() {
        assertThat(AbiCodec.rlpList(new byte[] {0x01}, new byte[] {0x02}))
                .isEqualTo(new byte[] {(byte) 0xc2, 0x01, 0x02});
    }

    @Test
    void rlpLen_payloadOf56_usesLongLengthPrefix() {
        // length >= 56 switches from the short form to a length-of-length prefix.
        assertThat(AbiCodec.rlpLen(56, 0x80)).isEqualTo(new byte[] {(byte) 0xb8, 0x38});
    }

    @Test
    void decodeString_roundTripsUtf8() {
        final byte[] encoded = AbiCodec.concat(
                AbiCodec.encodeUint(0x20), AbiCodec.encodeBytes("héllo".getBytes(StandardCharsets.UTF_8)));
        assertThat(AbiCodec.decodeString(encoded, 0)).isEqualTo("héllo");
    }
}
