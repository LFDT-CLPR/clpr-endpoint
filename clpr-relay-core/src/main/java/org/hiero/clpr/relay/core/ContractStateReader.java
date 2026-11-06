// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprEndpointManifest;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.hapi.node.state.clpr.ClprMessageValue;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

/**
 * Reads CLPR-related state from the EVM smart contract.
 *
 * <p>All reads are performed at a specified {@link CommitmentLevel} to allow callers to
 * trade off between data recency and finality guarantees.
 */
public interface ContractStateReader {

    record QueuedMessage(BigInteger messageId, ClprMessageValue value) {}

    /**
     * Reads a range of queued messages for the given channel.
     *
     * @param channelId the CLPR channel identifier
     * @param fromId the inclusive start of the message ID range
     * @param toId the exclusive end of the message ID range (half-open interval {@code [fromId, toId)})
     * @param blockTag the block to read against - hex number of tag ("latest", "safe", etc)
     * @return the ordered list of queued message values in the specified range
     */
    List<QueuedMessage> readQueuedMessages(
            final Bytes channelId, final long fromId, final long toId, final String blockTag);

    /**
     * Reads the ledger configuration from the local contract.
     *
     * <p>Returns the current ledger configuration. The endpoint manifest is now maintained
     * separately; use {@link #readEndpointManifest(String)} to obtain the current endpoint set.
     *
     * @param commitmentLevel the desired commitment level for the read
     * @return the current ledger configuration
     */
    ClprLedgerConfiguration readLedgerConfiguration(final CommitmentLevel commitmentLevel);

    /**
     * Reads the ledger configuration from the local contract pinned to an exact block.
     *
     * <p>Use this overload when the read must be consistent with other reads against the
     * same block — e.g. when a state proof from {@code eth_getProof} at the same block is
     * needed to verify the configuration value on a peer. Symbolic tags like
     * {@code "finalized"} can resolve to different blocks between successive RPC calls; a
     * caller that needs a single-block snapshot resolves the tag once (via
     * {@code eth_getBlockByNumber}) and passes the resulting hex block number here.
     *
     * <p>Default implementation throws {@link UnsupportedOperationException}; implementations
     * that support block-pinned reads override it.
     *
     * @param blockTag the block to read against — typically a {@code 0x}-prefixed hex block
     *                 number (e.g. {@code "0x123abc"}); symbolic tags also accepted
     * @return the ledger configuration at that block
     */
    default ClprLedgerConfiguration readLedgerConfiguration(final String blockTag) {
        throw new UnsupportedOperationException("readLedgerConfiguration(String) not implemented by this "
                + getClass().getSimpleName());
    }

    /**
     * Reads the Channel's cached peer {@code ClprEndpointManifest}.
     * Returns the default (empty, version 0) manifest for an unknown channel.
     *
     * @param channelId the CLPR channel identifier
     * @param commitmentLevel the desired commitment level for the read
     * @return the cached peer endpoint manifest for the channel
     */
    default ClprEndpointManifest readPeerEndpointManifest(
            final Bytes channelId, final CommitmentLevel commitmentLevel) {
        return ClprEndpointManifest.DEFAULT;
    }

    /**
     * Reads the Channel's cached peer {@code ClprEndpointManifest} pinned to an exact block,
     * for reads that must be consistent with other state read at the same block.
     *
     * @param channelId the CLPR channel identifier
     * @param blockTag the block to read against — a {@code 0x}-prefixed hex block number or a
     *                 symbolic tag (e.g. {@code "finalized"})
     * @return the cached peer endpoint manifest for the channel
     */
    default ClprEndpointManifest readPeerEndpointManifest(final Bytes channelId, final String blockTag) {
        return ClprEndpointManifest.DEFAULT;
    }

    /**
     * Reads the local CLPR Service's current {@code ClprEndpointManifest} pinned to an exact block,
     * for reads that must be consistent with other state read at the same block.
     *
     * @param blockTag the block to read against — a {@code 0x}-prefixed hex block number or a
     *                 symbolic tag (e.g. {@code "finalized"})
     * @return the local endpoint manifest at that block
     */
    default ClprEndpointManifest readEndpointManifest(final String blockTag) {
        return ClprEndpointManifest.DEFAULT;
    }

    Optional<ClprChannel> readChannelState(final Bytes channelId, final String blockTag);
}
