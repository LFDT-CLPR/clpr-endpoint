// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.app;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprQueueMetadata;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.Optional;
import org.hiero.clpr.relay.core.ParsedBundle;
import org.junit.jupiter.api.Test;

class ChannelMappedTransactionSubmitterTest {

    @Test
    void submitBundle_noHandler_throws() {
        final var submitter = new ChannelMappedTransactionSubmitter(id -> Optional.empty());
        final var channel =
                ClprChannel.newBuilder().channelId(Bytes.wrap(new byte[] {9})).build();
        final var bundle = new ParsedBundle(
                ClprQueueMetadata.newBuilder().nextMessageId(1L).build(), List.of(), Bytes.EMPTY);

        assertThatThrownBy(() -> submitter.submitBundle(channel, bundle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No channel handler registered");
    }
}
