// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import org.hiero.clpr.relay.e2e.E2EEnvironment;
import org.hiero.clpr.relay.e2e.E2ETest;
import org.hiero.clpr.relay.e2e.chain.BesuNetwork;
import org.hiero.clpr.relay.e2e.chain.BesuNetworkSpec;
import org.hiero.clpr.relay.e2e.channel.Channel;
import org.hiero.clpr.relay.e2e.channel.ChannelSpec;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpoint;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpointSpec;
import org.hiero.clpr.relay.e2e.load.InjectionRun;
import org.hiero.clpr.relay.e2e.load.MessageShape;

/**
 * Two independent CLPR channels multiplexed across the same pair of chains, each served by its own
 * relay pair signing with its own account, proving neither channel's traffic leaks into the other.
 *
 * <pre>
 *   besuA  ── channel X ──  ep-a-x  &lt;--sync--&gt;  ep-b-x  ── channel X ──  besuB
 *   besuA  ── channel Y ──  ep-a-y  &lt;--sync--&gt;  ep-b-y  ── channel Y ──  besuB
 * </pre>
 *
 * <p>A single {@code @E2ETest} method rather than one per claim: {@code @E2ETest} rebuilds two chains
 * and four relay containers for every method it isolates, and three separate positive-delivery claims
 * would triple that cost while proving less than one method that also asserts the negative — that
 * sending on one channel does <b>not</b> move the other channel's watermark, which the isolation claim
 * actually depends on.
 */
class TwoChannelIsolationE2ETest {

    @E2ETest(timeoutSeconds = 900)
    void channels_deliver_independently_with_no_cross_talk(final E2EEnvironment env) {
        final BesuNetwork besuA = env.besuNetworks().add(BesuNetworkSpec.singleNode());
        final BesuNetwork besuB = env.besuNetworks().add(BesuNetworkSpec.singleNode());

        // Each channel gets its own endpoint pair — and therefore its own signing account per
        // EvmEndpoints.add — so the two channels' submitBundle transactions never race for the same
        // nonce on either chain.
        final EvmEndpoint epAX = env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-a-x"));
        final EvmEndpoint epBX = env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-b-x"));
        final EvmEndpoint epAY = env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-a-y"));
        final EvmEndpoint epBY = env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-b-y"));

        final Channel channelX =
                env.channels().create(ChannelSpec.between(besuA, besuB).endpoints(epAX, epBX));
        final Channel channelY =
                env.channels().create(ChannelSpec.between(besuA, besuB).endpoints(epAY, epBY));

        env.startAll();

        assertThat(channelX.idHex()).as("the two channels have distinct ids").isNotEqualTo(channelY.idHex());

        for (final EvmEndpoint endpoint : new EvmEndpoint[] {epAX, epBX}) {
            env.await().channelLive(endpoint, channelX.metricLabel(), Duration.ofMinutes(5));
        }
        for (final EvmEndpoint endpoint : new EvmEndpoint[] {epAY, epBY}) {
            env.await().channelLive(endpoint, channelY.metricLabel(), Duration.ofMinutes(5));
        }

        // Channel X only: channel Y's watermark must stay untouched by traffic that never named it.
        final InjectionRun runX1 = env.messages().enqueue(channelX.a(), 1, MessageShape.small());
        runX1.awaitAllConfirmed(Duration.ofMinutes(2));
        assertThat(runX1.reverted()).as("channel X injection did not revert").isZero();
        assertThat(runX1.confirmed())
                .as("channel X injection was mined successfully")
                .isEqualTo(1);
        env.await().peerReceivedAll(channelX.a(), runX1.messageIds(), Duration.ofMinutes(3));
        assertThat(channelY.b().state().receivedMessageId())
                .as("channel Y is untouched by channel X's traffic")
                .isZero();

        // Channel Y only: the reverse check, against channel X's watermark as it stood after the send
        // above.
        final long xWatermarkBeforeY = channelX.b().state().receivedMessageId();
        final InjectionRun runY1 = env.messages().enqueue(channelY.a(), 1, MessageShape.small());
        runY1.awaitAllConfirmed(Duration.ofMinutes(2));
        env.await().peerReceivedAll(channelY.a(), runY1.messageIds(), Duration.ofMinutes(3));
        assertThat(channelX.b().state().receivedMessageId())
                .as("channel X is untouched by channel Y's traffic")
                .isEqualTo(xWatermarkBeforeY);

        // Both channels at once: their bundles are picked up and submitted by four independently
        // running relay sync loops, not sequenced by this test.
        final long xBeforeBurst = channelX.b().state().receivedMessageId();
        final long yBeforeBurst = channelY.b().state().receivedMessageId();
        final InjectionRun runX2 = env.messages().enqueue(channelX.a(), 3, MessageShape.small());
        final InjectionRun runY2 = env.messages().enqueue(channelY.a(), 3, MessageShape.small());
        runX2.awaitAllConfirmed(Duration.ofMinutes(2));
        runY2.awaitAllConfirmed(Duration.ofMinutes(2));
        env.await().peerReceivedAll(channelX.a(), runX2.messageIds(), Duration.ofMinutes(3));
        env.await().peerReceivedAll(channelY.a(), runY2.messageIds(), Duration.ofMinutes(3));
        assertThat(channelX.b().state().receivedMessageId())
                .as("channel X kept advancing during the concurrent burst")
                .isGreaterThan(xBeforeBurst);
        assertThat(channelY.b().state().receivedMessageId())
                .as("channel Y kept advancing during the concurrent burst")
                .isGreaterThan(yBeforeBurst);

        // Per-endpoint attribution: the wrong endpoint must not have submitted anything for a channel
        // it was never wired to. Stricter than the watermark cross-talk check above — that one proves
        // the peer chain never saw stray traffic; this one proves the misrouted endpoint never even
        // tried, which would catch a per-channel routing bug at the source rather than after the fact.
        final Map<String, String> xLabel = Map.of("channel_id", channelX.metricLabel());
        final Map<String, String> yLabel = Map.of("channel_id", channelY.metricLabel());
        for (final EvmEndpoint endpoint : new EvmEndpoint[] {epAY, epBY}) {
            assertThat(endpoint.metrics().counter("clpr_sync_bundle_messages_submitted", xLabel))
                    .as("%s never submitted for channel X", endpoint.id())
                    .isZero();
        }
        for (final EvmEndpoint endpoint : new EvmEndpoint[] {epAX, epBX}) {
            assertThat(endpoint.metrics().counter("clpr_sync_bundle_messages_submitted", yLabel))
                    .as("%s never submitted for channel Y", endpoint.id())
                    .isZero();
        }

        for (final BesuNetwork network : env.besuNetworks().all()) {
            assertThat(network.inspect()
                            .revertedTransactions(0, network.inspect().headBlockNumber()))
                    .as("no reverted transactions on " + network.id())
                    .isEmpty();
        }
        for (final EvmEndpoint endpoint : new EvmEndpoint[] {epAX, epBX, epAY, epBY}) {
            assertThat(endpoint.logs().errorLines())
                    .as(endpoint.id() + " logged no errors")
                    .isEmpty();
        }
    }
}
