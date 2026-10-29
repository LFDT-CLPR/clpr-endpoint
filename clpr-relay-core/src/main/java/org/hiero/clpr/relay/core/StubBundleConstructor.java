// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.hapi.node.state.clpr.ClprBundleContent;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprMessageValue;
import com.hedera.hapi.node.state.clpr.ClprQueueMetadata;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;

/**
 * A stub implementation of {@link BundleConstructor} for use during development and testing.
 *
 * <p>This implementation tags proof bytes with a magic prefix ({@code "CLPRSTUB"}) and serializes the
 * bundle content using PBJ-generated types (no real cryptographic proof). It reads the channel
 * state and requested messages from a {@link ContractStateReader}, so the full two-phase sync flow can
 * be exercised end-to-end before real proof logic is involved.
 *
 * <p>It reads at a fixed commitment tag rather than a numeric block, so it reports a
 * {@link #NO_HEIGHT} construction height; the stub is never wrapped in {@link CachedBundleConstructor}
 * in production, so that sentinel is inert.
 */
public final class StubBundleConstructor implements BundleConstructor {

    /** Reported construction height: the stub reads at a commitment tag and tracks no numeric height. */
    private static final long NO_HEIGHT = 0L;

    /** Magic prefix bytes that tag all proofs produced by this stub implementation. */
    static final Bytes MAGIC_PREFIX = Bytes.wrap("CLPRSTUB".getBytes());

    private final ContractStateReader stateReader;
    private final CommitmentLevel commitmentLevel;

    /**
     * @param stateReader     reads local channel state and queued messages
     * @param commitmentLevel the commitment level to read state at
     */
    public StubBundleConstructor(final ContractStateReader stateReader, final CommitmentLevel commitmentLevel) {
        this.stateReader = stateReader;
        this.commitmentLevel = commitmentLevel;
    }

    @Override
    public BundleWithConstructionMetadata createBundle(final Bytes channelId, final BundleRequest request) {
        final String blockTag = commitmentLevel.toBlockTag();
        final var maybeChannel = stateReader.readChannelState(channelId, blockTag);
        if (maybeChannel.isEmpty()) {
            return new BundleWithConstructionMetadata(Bytes.EMPTY, NO_HEIGHT);
        }
        final ClprChannel channel = maybeChannel.get();

        // Sender-side trim: serve messages from the id the peer asked for, up to what we have sent.
        final long fromId = Math.max(1L, request.currentReceivedMessageId() + 1);
        final List<ContractStateReader.QueuedMessage> messages = fromId >= channel.nextMessageId()
                ? List.of()
                : stateReader.readQueuedMessages(channelId, fromId, channel.nextMessageId(), blockTag);

        final List<ClprMessagePayload> payloads = messages.stream()
                .map(ContractStateReader.QueuedMessage::value)
                .map(ClprMessageValue::payload)
                .toList();
        final ClprBundleContent content = ClprBundleContent.newBuilder()
                .metadata(buildQueueMetadata(channel))
                .messages(payloads)
                .build();
        final Bytes serialized = ClprBundleContent.PROTOBUF.toBytes(content);
        return new BundleWithConstructionMetadata(prependMagicPrefix(serialized), NO_HEIGHT);
    }

    /**
     * Builds {@link ClprQueueMetadata} from the given channel state.
     *
     * @param channel the current channel state
     * @return the queue metadata derived from the channel
     */
    private static ClprQueueMetadata buildQueueMetadata(final ClprChannel channel) {
        return ClprQueueMetadata.newBuilder()
                .nextMessageId(channel.nextMessageId())
                .sentRunningHash(channel.sentRunningHash())
                .receivedMessageId(channel.receivedMessageId())
                .receivedRunningHash(channel.receivedRunningHash())
                .status(channel.status())
                .trustAnchorId(channel.trustAnchorId())
                .endpointManifestVersion(channel.endpointManifestVersion())
                .build();
    }

    /**
     * Prepends the magic prefix to the given serialized bytes.
     *
     * @param serialized the data to prefix
     * @return a new {@link Bytes} instance containing the prefix followed by the data
     */
    private static Bytes prependMagicPrefix(final Bytes serialized) {
        final byte[] prefixBytes = MAGIC_PREFIX.toByteArray();
        final byte[] dataBytes = serialized.toByteArray();
        final byte[] combined = new byte[prefixBytes.length + dataBytes.length];
        System.arraycopy(prefixBytes, 0, combined, 0, prefixBytes.length);
        System.arraycopy(dataBytes, 0, combined, prefixBytes.length, dataBytes.length);
        return Bytes.wrap(combined);
    }
}
