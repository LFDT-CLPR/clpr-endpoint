// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.test.harness;

import com.hedera.hapi.node.state.clpr.ClprQueueMetadata;
import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Metrics;
import org.hiero.clpr.relay.core.BundlePayloadCodec;
import org.hiero.clpr.relay.core.ContractStateReader;

/**
 * Entry points for the harness's three assertion surfaces:
 *
 * <ul>
 *   <li><b>on-chain</b> — {@link #assertChannel} reads ground-truth Channel state via a
 *       {@link ContractStateReader} (the contract is the oracle);</li>
 *   <li><b>peer-observed</b> — {@link #assertReplyMetadata}/{@link #assertBundleMetadata} decode the
 *       endpoint-under-test's emitted bundle into {@link ClprQueueMetadata} (what it tells peers);</li>
 *   <li><b>metrics</b> — {@link #assertMetric} reads operational counters (which internal path it took).</li>
 * </ul>
 */
public final class RelayAssertions {

    private RelayAssertions() {}

    /** Assert on a channel's on-chain state, read through {@code reader}. */
    public static ChannelStateAssert assertChannel(final ContractStateReader reader, final byte[] channelId) {
        return new ChannelStateAssert(reader, channelId);
    }

    /**
     * Assert on an endpoint counter family, e.g. {@code assertMetric(metrics, "sync", "exchanges.started")}
     * or {@code assertMetric(metrics, "sync", "bundle.skipped")}. The value sums every
     * labelled series sharing the base name.
     */
    public static MetricAssert assertMetric(final Metrics metrics, final String category, final String baseName) {
        return new MetricAssert(metrics, category, baseName);
    }

    /** Assert directly on a verified {@link ClprQueueMetadata}. */
    public static QueueMetadataAssert assertMetadata(final ClprQueueMetadata metadata) {
        return new QueueMetadataAssert(metadata);
    }

    /**
     * Decode {@code bundlePayload} with {@code codec} and assert on the resulting metadata. Any
     * {@link BundlePayloadCodec} works (e.g. {@code HieroProofCodec} / {@code QbftProofCodec}); the
     * decode reads metadata without any client-side crypto — the proof is verified on chain.
     */
    public static QueueMetadataAssert assertBundleMetadata(final BundlePayloadCodec codec, final Bytes bundlePayload) {
        return new QueueMetadataAssert(codec.decodeBundle(bundlePayload).metadata());
    }

    /** Decode the bundle carried in a two-phase sync reply/capture and assert on its metadata. */
    public static QueueMetadataAssert assertReplyMetadata(final BundlePayloadCodec codec, final ClprSyncPayload reply) {
        return assertBundleMetadata(codec, reply.bundleResponse().bundlePayload());
    }
}
