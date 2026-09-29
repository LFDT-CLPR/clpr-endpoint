// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.app;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ChannelMappedBundleConstructorTest {

    private static final Bytes CID = Bytes.wrap(new byte[] {7});

    private final ChannelMappedBundleConstructor constructor =
            new ChannelMappedBundleConstructor(id -> Optional.empty());

    @Test
    void getLatestBundlePayload_noHandler_throws() {
        assertThatThrownBy(() -> constructor.getLatestBundlePayload(CID))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No channel handler registered");
    }

    @Test
    void onStateChanged_noHandler_throws() {
        final var channel = ClprChannel.newBuilder().channelId(CID).build();
        assertThatThrownBy(() -> constructor.onStateChanged(BigInteger.ONE, CID, channel, List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No channel handler registered");
    }
}
