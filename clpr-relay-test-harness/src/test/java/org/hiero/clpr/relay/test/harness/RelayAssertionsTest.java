// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.test.harness;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hiero.clpr.relay.test.harness.RelayAssertions.assertChannel;
import static org.hiero.clpr.relay.test.harness.RelayAssertions.assertMetadata;
import static org.hiero.clpr.relay.test.harness.RelayAssertions.assertMetric;
import static org.hiero.clpr.relay.test.harness.RelayAssertions.assertReplyMetadata;

import com.hedera.hapi.node.state.clpr.BundleResponse;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.hapi.node.state.clpr.ClprQueueMetadata;
import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.Optional;
import org.hiero.clpr.relay.core.BundlePayloadCodec;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.StubProofCodec;
import org.hiero.clpr.relay.core.metrics.LabeledCounter;
import org.hiero.clpr.relay.core.metrics.SimpleMetrics;
import org.junit.jupiter.api.Test;

/** No-Docker checks of the three assertion surfaces in {@link RelayAssertions}. */
class RelayAssertionsTest {

    private final byte[] channelId = new byte[32];

    @Test
    void metadataSurface() {
        final ClprQueueMetadata metadata = ClprQueueMetadata.newBuilder()
                .status(ClprChannelStatus.CLOSING)
                .receivedMessageId(3)
                .nextMessageId(5)
                .build();
        assertThatNoException().isThrownBy(() -> assertMetadata(metadata)
                .hasStatus(ClprChannelStatus.CLOSING)
                .hasReceivedMessageId(3));
        assertThatThrownBy(() -> assertMetadata(metadata).hasStatus(ClprChannelStatus.ACTIVE))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void metricSurface() {
        final SimpleMetrics metrics = new SimpleMetrics();
        final LabeledCounter skipped = new LabeledCounter("sync", "bundle.skipped", "test counter", metrics);

        assertMetric(metrics, "sync", "bundle.skipped").isZero();
        skipped.increment("channel_id", "deadbeef");
        skipped.increment("channel_id", "deadbeef");
        assertMetric(metrics, "sync", "bundle.skipped").isEqualTo(2).isPositive();
    }

    @Test
    void onChainSurface() {
        final ClprChannel active = ClprChannel.newBuilder()
                .status(ClprChannelStatus.ACTIVE)
                .nextMessageId(2)
                .receivedMessageId(3)
                .build();
        final ContractStateReader present = new FakeReader(Optional.of(active));
        final ContractStateReader absent = new FakeReader(Optional.empty());

        assertThatNoException().isThrownBy(() -> assertChannel(present, channelId)
                .isPresent()
                .hasStatus(ClprChannelStatus.ACTIVE)
                .hasReceivedMessageId(3));
        assertThatNoException()
                .isThrownBy(() -> assertChannel(absent, channelId).isAbsent());
        assertThatThrownBy(() -> assertChannel(present, channelId).hasStatus(ClprChannelStatus.CLOSED))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void peerObservedSurfaceDecodesCraftedBundle() {
        final BundlePayloadCodec codec = new StubProofCodec();
        final Bytes bundle = StubBundles.stubProof()
                .status(ClprChannelStatus.DRAINED)
                .receivedMessageId(7)
                .build();
        final ClprSyncPayload reply = ClprSyncPayload.newBuilder()
                .channelId(Bytes.wrap(channelId))
                .bundleResponse(
                        BundleResponse.newBuilder().bundlePayload(bundle).build())
                .build();

        assertThatNoException().isThrownBy(() -> assertReplyMetadata(codec, reply)
                .hasStatus(ClprChannelStatus.DRAINED)
                .hasReceivedMessageId(7));
    }

    /** Minimal {@link ContractStateReader} returning a fixed channel record. */
    private record FakeReader(Optional<ClprChannel> channel) implements ContractStateReader {
        @Override
        public Optional<ClprChannel> readChannelState(final Bytes channelId, final String blockTag) {
            return channel;
        }

        @Override
        public List<QueuedMessage> readQueuedMessages(
                final Bytes channelId, final long fromId, final long toId, final String blockTag) {
            return List.of();
        }

        @Override
        public ClprLedgerConfiguration readLedgerConfiguration(final CommitmentLevel commitmentLevel) {
            throw new UnsupportedOperationException();
        }
    }
}
