// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.Optional;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.jspecify.annotations.Nullable;

/**
 * A minimal {@link ContractStateReader} for the constructor integration tests: returns a fixed channel
 * and message list regardless of the block tag, so {@code createBundle} can build a bundle from known-good
 * fixture state while the proof itself is fetched from a real EVM node. A {@code null} channel models
 * an absent on-chain record (the "no channel" path). The ledger-configuration read is unused by
 * bundle construction and throws.
 */
final class FixedStateReader implements ContractStateReader {

    @Nullable
    private final ClprChannel channel;

    private final List<QueuedMessage> messages;

    FixedStateReader(@Nullable final ClprChannel channel, final List<QueuedMessage> messages) {
        this.channel = channel;
        this.messages = List.copyOf(messages);
    }

    @Override
    public Optional<ClprChannel> readChannelState(final Bytes channelId, final String blockTag) {
        return Optional.ofNullable(channel);
    }

    @Override
    public List<QueuedMessage> readQueuedMessages(
            final Bytes channelId, final long fromId, final long toId, final String blockTag) {
        return messages.stream()
                .filter(m -> {
                    final long id = m.messageId().longValueExact();
                    return id >= fromId && id < toId;
                })
                .toList();
    }

    @Override
    public ClprLedgerConfiguration readLedgerConfiguration(final CommitmentLevel commitmentLevel) {
        throw new UnsupportedOperationException("readLedgerConfiguration not used by constructor tests");
    }
}
