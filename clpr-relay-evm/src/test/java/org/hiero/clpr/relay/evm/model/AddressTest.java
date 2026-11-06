// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class AddressTest {

    @Test
    void fromHexString_withPrefix_parses20Bytes() {
        final Address a = Address.fromHexString("0x" + "ab".repeat(20));
        assertThat(a.value()).hasSize(20);
        assertThat(a.toHexString()).isEqualTo("0x" + "ab".repeat(20));
    }

    @Test
    void fromHexString_withoutPrefix_parses() {
        assertThat(Address.fromHexString("ab".repeat(20)).toHexString()).isEqualTo("0x" + "ab".repeat(20));
    }

    @Test
    void fromHexString_wrongLength_throws() {
        assertThatThrownBy(() -> Address.fromHexString("0xabcd"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("40 hex chars");
    }

    @Test
    void constructor_wrongByteLength_throws() {
        assertThatThrownBy(() -> new Address(new byte[19]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("20 bytes");
    }

    @Test
    void toChecksumHexString_matchesEip55Vector() {
        // Canonical EIP-55 test vector.
        final Address a = Address.fromHexString("0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed");
        assertThat(a.toChecksumHexString()).isEqualTo("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed");
        assertThat(a).hasToString("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed");
    }

    @Test
    void equalsAndHashCode_areValueBased() {
        final Address a = Address.fromHexString("0x" + "11".repeat(20));
        final Address b = Address.fromHexString("0x" + "11".repeat(20));
        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(Address.ZERO);
        assertThat(a).isNotEqualTo("not an address");
    }

    @Test
    void zero_isAllZeroBytes() {
        assertThat(Address.ZERO.toHexString()).isEqualTo("0x" + "00".repeat(20));
    }
}
