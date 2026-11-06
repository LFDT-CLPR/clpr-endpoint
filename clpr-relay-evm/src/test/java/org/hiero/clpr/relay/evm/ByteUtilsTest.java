// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import org.junit.jupiter.api.Test;

class ByteUtilsTest {

    @Test
    void leftPad32_shortInput_leftPadsWithZeros() {
        final Bytes out = ByteUtils.leftPad32(Bytes.fromHex("0102"));
        assertThat(out.length()).isEqualTo(32L);
        assertThat(out.toHex()).startsWith("0000").endsWith("0102");
    }

    @Test
    void leftPad32_alreadyFullWidth_returnedUnchanged() {
        final Bytes in = Bytes.wrap(new byte[32]);
        assertThat(ByteUtils.leftPad32(in)).isSameAs(in);
    }

    @Test
    void leftPad32_oversized_returnedUnchanged() {
        final Bytes in = Bytes.wrap(new byte[40]);
        assertThat(ByteUtils.leftPad32(in)).isSameAs(in);
    }

    @Test
    void fromPrefixedHex_withPrefix() {
        assertThat(ByteUtils.fromPrefixedHex("0x0102")).isEqualTo(Bytes.fromHex("0102"));
    }

    @Test
    void fromPrefixedHex_withoutPrefix() {
        assertThat(ByteUtils.fromPrefixedHex("0102")).isEqualTo(Bytes.fromHex("0102"));
    }

    @Test
    void fromPrefixedHex_oddLength_leftPadsSingleNibble() {
        // Besu returns non-canonical odd-nibble hex like "0x123"; it must be padded to "0123".
        assertThat(ByteUtils.fromPrefixedHex("0x123")).isEqualTo(Bytes.fromHex("0123"));
    }

    @Test
    void zeroHash_is32Bytes() {
        assertThat(ByteUtils.ZERO_HASH.length()).isEqualTo(32L);
    }
}
