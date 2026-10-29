// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.LongGauge;
import com.swirlds.metrics.api.Metric;
import com.swirlds.metrics.api.Metrics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.hiero.clpr.relay.core.metrics.SimpleMetrics;
import org.hiero.clpr.relay.evm.qbft.QbftProofCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Verify that the relay's per-channel loops survive a transient EVM-node outage and recover. The Anvil
 * container is paused — freezing the JSON-RPC endpoint while preserving its port mapping and in-memory
 * chain — so every loop iteration throws. The test asserts the relay stays alive and faulting (failure
 * gauges rise), then, once the node is unpaused, that a freshly-queued message is delivered to the peer
 * and the gauges return to zero.
 *
 * <p>If the loops had died on the first throw, the post-recovery message would never reach the peer. The
 * precise no-hot-loop / backoff assertions live in the unit layer ({@code ConnectionSyncTaskTest},
 * {@code EvmConnectionStateChangeTaskTest}); here each failing iteration is paced by the JSON-RPC request
 * timeout, so the windows are intentionally generous.
 */
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
class TransientNodeOutageRecoveryTest extends OneSidedStubPeerTestBase {

    @Test
    void survivesNodeOutageAndRecovers() {
        peer.respondEmpty();
        // Both loops (sync + state listener) must run to exercise outage survival and delivery.
        restartFullDuplex();

        // 1. Sanity: the relay delivers a message to the peer before the outage.
        interactor.sendMessage(channelId, connectorId, targetApp(), "MSG1".getBytes(StandardCharsets.UTF_8));
        awaitDelivered("MSG1".getBytes(StandardCharsets.UTF_8));

        // 2. Outage: pause the Anvil node. Every loop iteration now blocks on RPC and throws.
        ANVIL.getDockerClient().pauseContainerCmd(ANVIL.getContainerId()).exec();
        try {
            // 3. Survival: the relay is alive and faulting, not dead — a failure gauge rises above zero.
            TestConditions.awaitCondition(
                    Duration.ofSeconds(60),
                    () -> gauge("sync", "channels.failing") > 0 || gauge("evm.listener", "channels.failing") > 0);
        } finally {
            // 4. Restore the node.
            ANVIL.getDockerClient().unpauseContainerCmd(ANVIL.getContainerId()).exec();
        }

        // 5. Recovery (primary assertion): queue a new message and assert it reaches the peer. This only
        //    passes if a loop survived the outage, recovered, re-read state, rebuilt the proof, and synced.
        interactor.sendMessage(channelId, connectorId, targetApp(), "MSG2".getBytes(StandardCharsets.UTF_8));
        awaitDelivered("MSG2".getBytes(StandardCharsets.UTF_8));

        // 6. Clean recovery: the failure gauges return to zero (the first clean cycle reset them).
        TestConditions.awaitCondition(
                Duration.ofSeconds(60),
                () -> gauge("sync", "channels.failing") == 0
                        && gauge("sync", "channels.max_consecutive_failures") == 0
                        && gauge("evm.listener", "channels.failing") == 0);
    }

    /** Wait until the relay syncs a bundle carrying {@code msgData} to the stub peer. */
    private static void awaitDelivered(final byte[] msgData) {
        final ClprSyncPayload delivered = peer.awaitSync(p -> carriesMessage(p, msgData), Duration.ofSeconds(180));
        assertThat(delivered)
                .as("the relay must deliver the queued message to the peer")
                .isNotNull();
    }

    /** True if {@code payload}'s bundle decodes to a DATA message whose data equals {@code msgData}. */
    private static boolean carriesMessage(final ClprSyncPayload payload, final byte[] msgData) {
        if (replyBundle(payload).length() == 0) {
            return false;
        }
        try {
            return new QbftProofCodec(Bytes.wrap(channelId))
                    .decodeBundle(replyBundle(payload)).messages().stream()
                            .anyMatch(m ->
                                    m.hasMessage() && m.message().messageData().equals(Bytes.wrap(msgData)));
        } catch (final RuntimeException e) {
            return false; // not a decodable QBFT bundle (e.g. an empty probe) — keep waiting
        }
    }

    /** Read an aggregate failure gauge from the relay, running scrape-time updaters first; -1 if absent. */
    private static long gauge(final String category, final String name) {
        final Metrics metrics = relay.metrics();
        if (metrics instanceof SimpleMetrics simple) {
            simple.runUpdaters();
        }
        final Metric metric = metrics.getMetric(category, name);
        return (metric instanceof LongGauge longGauge) ? longGauge.get() : -1L;
    }
}
