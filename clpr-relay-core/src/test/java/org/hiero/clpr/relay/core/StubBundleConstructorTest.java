// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprMessageValue;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link StubBundleConstructor} under the two-phase sync API: a bundle is built
 * <b>ad hoc</b> from a peer's {@link BundleRequest} via {@link StubBundleConstructor#createBundle} —
 * request-driven, with no state-change-triggered "latest bundle" cache.
 */
class StubBundleConstructorTest {

    private static final Bytes MAGIC_PREFIX = Bytes.wrap("CLPRSTUB".getBytes());

    private StubStateReader stateReader;
    private StubBundleConstructor constructor;
    private Bytes channelId1;
    private Bytes channelId2;

    @BeforeEach
    void setUp() {
        stateReader = new StubStateReader();
        constructor = new StubBundleConstructor(stateReader, CommitmentLevel.LATEST);
        channelId1 = Bytes.wrap(new byte[32]); // all-zeros 32-byte ID
        final byte[] id2bytes = new byte[32];
        id2bytes[0] = 1;
        channelId2 = Bytes.wrap(id2bytes);
    }

    private static BundleRequest requestFrom(final long fromMessageId) {
        return BundleRequest.newBuilder()
                .currentReceivedMessageId(Math.max(0L, fromMessageId - 1))
                .build();
    }

    private static byte[] payloadBytes(final BundleWithConstructionMetadata response) {
        return response.bundlePayload().toByteArray();
    }

    @Test
    void returnsEmptyPayloadForUnknownChannel() {
        // No Channel record on chain → the stub yields an empty (no-proof) response.
        final BundleWithConstructionMetadata response = constructor.createBundle(channelId1, requestFrom(1L));
        assertThat(response.bundlePayload()).isEqualTo(Bytes.EMPTY);
    }

    @Test
    void tagsPayloadWithMagicPrefixForKnownChannel() {
        stateReader.put(channelId1, channel(1L));

        final BundleWithConstructionMetadata response = constructor.createBundle(channelId1, requestFrom(1L));

        final byte[] bundleBytes = payloadBytes(response);
        final byte[] prefixBytes = MAGIC_PREFIX.toByteArray();
        assertThat(bundleBytes.length).isGreaterThanOrEqualTo(prefixBytes.length);
        for (int i = 0; i < prefixBytes.length; i++) {
            assertThat(bundleBytes[i]).isEqualTo(prefixBytes[i]);
        }
    }

    @Test
    void differentChannelsAreServedIndependently() {
        // Only channel 1 has an on-chain record.
        stateReader.put(channelId1, channel(1L));

        assertThat(payloadBytes(constructor.createBundle(channelId1, requestFrom(1L))))
                .startsWith(MAGIC_PREFIX.toByteArray());
        // Channel 2 has no record → empty payload.
        assertThat(constructor.createBundle(channelId2, requestFrom(1L)).bundlePayload())
                .isEqualTo(Bytes.EMPTY);
    }

    @Test
    void payloadReflectsChannelState() {
        // First: channel with no undelivered messages (nextMessageId == fromId).
        stateReader.put(channelId1, channel(1L));
        final Bytes first =
                constructor.createBundle(channelId1, requestFrom(1L)).bundlePayload();

        // Then: channel advanced with a queued message → different metadata and message list.
        stateReader.put(channelId1, channel(2L));
        stateReader.setQueued(List.of(new ContractStateReader.QueuedMessage(
                BigInteger.ONE,
                ClprMessageValue.newBuilder()
                        .payload(ClprMessagePayload.DEFAULT)
                        .build())));
        final Bytes second =
                constructor.createBundle(channelId1, requestFrom(1L)).bundlePayload();

        assertThat(second).isNotEqualTo(first);
    }

    private static ClprChannel channel(final long nextMessageId) {
        return ClprChannel.newBuilder().nextMessageId(nextMessageId).build();
    }

    /** A {@link ContractStateReader} whose channel records and queued messages are configured per test. */
    private static final class StubStateReader implements ContractStateReader {
        private final Map<Bytes, ClprChannel> channels = new HashMap<>();
        private List<QueuedMessage> queued = List.of();

        void put(final Bytes channelId, final ClprChannel channel) {
            channels.put(channelId, channel);
        }

        void setQueued(final List<QueuedMessage> queued) {
            this.queued = queued;
        }

        @Override
        public Optional<ClprChannel> readChannelState(final Bytes channelId, final String blockTag) {
            return Optional.ofNullable(channels.get(channelId));
        }

        @Override
        public List<QueuedMessage> readQueuedMessages(
                final Bytes channelId, final long fromId, final long toId, final String blockTag) {
            return queued;
        }

        @Override
        public ClprLedgerConfiguration readLedgerConfiguration(final CommitmentLevel commitmentLevel) {
            return ClprLedgerConfiguration.DEFAULT;
        }
    }
}
