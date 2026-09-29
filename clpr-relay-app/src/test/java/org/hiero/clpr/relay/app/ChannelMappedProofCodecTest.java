// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.app;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ChannelMappedProofCodecTest {

    @Test
    void codecFor_noHandler_throws() {
        final var codec = new ChannelMappedProofCodec(id -> Optional.empty());
        assertThatThrownBy(() -> codec.codecFor(Bytes.wrap(new byte[] {4})))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No channel handler registered");
    }
}
