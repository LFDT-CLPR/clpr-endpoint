// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprEndpointManifest;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Exercises the {@link ContractStateReader} interface's default-method contract. */
class ContractStateReaderTest {

    private static final Bytes CID = Bytes.wrap(new byte[] {1});

    /** Implements only the abstract methods so the interface defaults are exercised as written. */
    private static final ContractStateReader MINIMAL = new ContractStateReader() {
        @Override
        public Optional<ClprChannel> readChannelState(final Bytes channelId, final String blockTag) {
            return Optional.empty();
        }

        @Override
        public List<QueuedMessage> readQueuedMessages(
                final Bytes channelId, final long fromId, final long toId, final String blockTag) {
            return List.of();
        }

        @Override
        public ClprLedgerConfiguration readLedgerConfiguration(final CommitmentLevel commitmentLevel) {
            return ClprLedgerConfiguration.DEFAULT;
        }
    };

    @Test
    void readLedgerConfiguration_blockTagOverload_isUnsupportedByDefault() {
        assertThatThrownBy(() -> MINIMAL.readLedgerConfiguration("latest"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void endpointManifestOverloads_defaultToEmptyManifest() {
        assertThat(MINIMAL.readEndpointManifest("latest")).isEqualTo(ClprEndpointManifest.DEFAULT);
        assertThat(MINIMAL.readPeerEndpointManifest(CID, CommitmentLevel.LATEST))
                .isEqualTo(ClprEndpointManifest.DEFAULT);
        assertThat(MINIMAL.readPeerEndpointManifest(CID, "latest")).isEqualTo(ClprEndpointManifest.DEFAULT);
    }
}
