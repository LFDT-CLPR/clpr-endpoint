// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ChannelMappedChannelLookupTest {

    private static final Bytes CID = Bytes.wrap(new byte[] {1, 2, 3});

    private final ChannelMappedChannelLookup lookup = new ChannelMappedChannelLookup(id -> Optional.empty());

    @Test
    void readChannelState_noHandler_returnsEmpty() {
        // flatMap over an absent handler yields empty rather than throwing.
        assertThat(lookup.readChannelState(CID, "latest")).isEmpty();
    }
}
