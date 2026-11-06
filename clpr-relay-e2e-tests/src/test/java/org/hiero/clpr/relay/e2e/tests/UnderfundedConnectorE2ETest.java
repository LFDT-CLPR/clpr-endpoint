// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import java.math.BigInteger;
import java.time.Duration;
import org.hiero.clpr.relay.e2e.E2EEnvironment;
import org.hiero.clpr.relay.e2e.E2ETest;
import org.hiero.clpr.relay.e2e.chain.BesuNetwork;
import org.hiero.clpr.relay.e2e.chain.BesuNetworkSpec;
import org.hiero.clpr.relay.e2e.channel.Channel;
import org.hiero.clpr.relay.e2e.channel.ChannelSpec;
import org.hiero.clpr.relay.e2e.channel.Connector;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpoint;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpointSpec;
import org.hiero.clpr.relay.e2e.load.InjectionRun;
import org.hiero.clpr.relay.e2e.load.MessageShape;

/**
 * A connector that cannot pay for inbound execution, and the channel's recovery from it.
 *
 * <p>The point is that this is a <b>business outcome, not a relay failure</b>. Before dispatching an
 * inbound message the service computes
 * {@code affordableGas = connectorBalance * 100 / ((100 + margin) * tx.gasprice)}; when that is zero
 * it slashes the connector and answers with {@code CONNECTOR_UNDERFUNDED} instead of calling the
 * application. Messages are not lost and the relay is not at fault — so the relay must keep working,
 * the channel must stay ACTIVE, and nothing may revert on chain.
 *
 * <p>Recovery needs a <em>new</em> connector, not a top-up: after {@code slashBanThreshold} slashes the
 * old one is banned, and a banned connector cannot be revived.
 *
 * <p>And it needs one on <b>both</b> sides. The penalty is symmetric: {@code BundleLib} slashes the
 * <em>sending</em> connector too when a {@code CONNECTOR_UNDERFUNDED} reply comes back, so starving one
 * side bans the connectors at both ends. Missing that produces a "recovery" that reverts every send
 * with the far side apparently healthy.
 *
 * <p>Requires a non-zero gas price. The affordability check is guarded by {@code tx.gasprice > 0}, so
 * on a zero-price chain this whole path is silently unreachable — which is why the framework defaults
 * {@code minGasPriceWei} to 1 rather than 0.
 */
class UnderfundedConnectorE2ETest {

    /** Enough messages to exceed the default {@code slashBanThreshold} of 5, but under the bundle batch. */
    private static final int STARVED_MESSAGES = 6;

    // Budget note: the awaits below sum to well under the method budget, so a partial stall surfaces
    // the awaiting helper's named diagnostic ("received watermark stalled at N of M") rather than the
    // extension's generic timeout, which would say nothing about where it stopped.
    @E2ETest(timeoutSeconds = 900)
    void starved_connector_is_slashed_and_the_channel_recovers_with_a_fresh_one(final E2EEnvironment env) {
        final BesuNetwork besuA = env.besuNetworks().add(BesuNetworkSpec.singleNode());
        final BesuNetwork besuB = env.besuNetworks().add(BesuNetworkSpec.singleNode());
        final EvmEndpoint epA = env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-a"));
        final EvmEndpoint epB = env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-b"));

        // Side B's connector gets no execution budget; side A's is funded normally so the replies
        // travelling back can still be dispatched. Starving both would produce two failures at once
        // and make the outcome ambiguous.
        final Channel channel = env.channels()
                .create(ChannelSpec.between(besuA, besuB)
                        .endpoints(epA, epB)
                        .connectorFunding(ChannelSpec.DEFAULT_CONNECTOR_FUNDING_WEI, BigInteger.ZERO));

        env.startAll();

        assertThat(channel.b().connector().balance())
                .as("side B's connector starts with no execution budget")
                .isZero();

        final InjectionRun starved = env.messages().enqueue(channel.a(), STARVED_MESSAGES, MessageShape.small());
        starved.awaitAllConfirmed(Duration.ofMinutes(3));
        assertThat(starved.confirmed()).isEqualTo(STARVED_MESSAGES);
        assertThat(starved.reverted())
                .as("queueing a message is unaffected by the connector")
                .isZero();

        // The messages ARE processed — the received watermark advances — they simply come back as
        // CONNECTOR_UNDERFUNDED instead of reaching the application. That distinction is the whole
        // point: an underfunded connector must not stall the channel.
        env.await().peerReceivedAll(channel.a(), starved.messageIds(), Duration.ofMinutes(3));

        assertThat(channel.b().state().receivedMessageId())
                .as("side B processed every starved message rather than stalling")
                .isEqualTo(STARVED_MESSAGES);

        // Replies flow back to A, so A's queue keeps moving too.
        env.await().queueDrained(channel.a(), Duration.ofMinutes(3));

        // The channel is still usable. Compared against the real enum, not against the other side's
        // status: "both sides agree" would also hold if both had closed.
        assertThat(channel.a().status())
                .as("side A's channel is still ACTIVE after the slashing")
                .isEqualTo(ClprChannelStatus.ACTIVE);
        assertThat(channel.b().status())
                .as("side B's channel is still ACTIVE after the slashing")
                .isEqualTo(ClprChannelStatus.ACTIVE);

        // The relay did nothing wrong and must not report otherwise: this is an expected on-chain
        // business outcome, so no ERROR logs and no reverted transactions anywhere.
        assertThat(epA.logs().errorLines()).as("relay A logged no errors").isEmpty();
        assertThat(epB.logs().errorLines()).as("relay B logged no errors").isEmpty();
        for (final BesuNetwork network : env.besuNetworks().all()) {
            assertThat(network.inspect()
                            .revertedTransactions(0, network.inspect().headBlockNumber()))
                    .as("no reverted transactions on " + network.id())
                    .isEmpty();
        }

        // ── The sending side is penalised too ─────────────────────────────────
        // Each returning CONNECTOR_UNDERFUNDED reply slashes the SOURCE connector as well, so after
        // six of them side A's connector is banned even though it was fully funded. Prove it: a send
        // through it now reverts.
        final InjectionRun blocked = env.messages().enqueue(channel.a(), 1, MessageShape.small());
        blocked.awaitAllConfirmed(Duration.ofMinutes(2));
        assertThat(blocked.reverted())
                .as("side A's connector is banned by the returning UNDERFUNDED replies, so the send reverts")
                .isEqualTo(1);

        // ── Recovery ──────────────────────────────────────────────────────────
        // Capture the old ids first: registerFreshConnector installs the replacement on the side, so
        // reading afterwards would compare each new connector with itself.
        final byte[] starvedConnectorId = channel.b().connector().id();
        final byte[] senderConnectorId = channel.a().connector().id();

        // Both ends need a fresh connector; a top-up cannot revive a banned one.
        final Connector replacementB = env.channels()
                .registerFreshConnector(
                        channel,
                        channel.b(),
                        ChannelSpec.DEFAULT_CONNECTOR_STAKE_WEI,
                        ChannelSpec.DEFAULT_CONNECTOR_FUNDING_WEI);
        final Connector replacementA = env.channels()
                .registerFreshConnector(
                        channel,
                        channel.a(),
                        ChannelSpec.DEFAULT_CONNECTOR_STAKE_WEI,
                        ChannelSpec.DEFAULT_CONNECTOR_FUNDING_WEI);
        assertThat(replacementB.balance()).isEqualTo(ChannelSpec.DEFAULT_CONNECTOR_FUNDING_WEI);
        assertThat(replacementB.id())
                .as("the replacement has a different id — a banned connector cannot be reused")
                .isNotEqualTo(starvedConnectorId);
        assertThat(replacementA.id()).isNotEqualTo(senderConnectorId);
        assertThat(channel.b().connector().id())
                .as("side B now routes through its replacement")
                .isEqualTo(replacementB.id());

        final long receivedBefore = channel.b().state().receivedMessageId();
        final InjectionRun recovered = env.messages().enqueue(channel.a(), 4, MessageShape.small());
        recovered.awaitAllConfirmed(Duration.ofMinutes(3));
        assertThat(recovered.confirmed()).isEqualTo(4);
        assertThat(recovered.reverted()).isZero();

        env.await().peerReceived(channel.a(), receivedBefore + 4, Duration.ofMinutes(3));
        env.await().queueDrained(channel.a(), Duration.ofMinutes(3));

        assertThat(channel.b().state().receivedMessageId())
                .as("the channel kept delivering after the connector was replaced")
                .isEqualTo(receivedBefore + 4);
    }
}
