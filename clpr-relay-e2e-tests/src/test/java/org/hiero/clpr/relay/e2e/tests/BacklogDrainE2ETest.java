// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import org.hiero.clpr.relay.e2e.Capability;
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
import org.junit.jupiter.api.Disabled;

/**
 * Whether the relay drains a queue that built up while it was down.
 *
 * <p>Split by expected outcome, because the two halves of that question have different answers today.
 * A backlog <b>up to</b> the relay's per-bundle batch drains correctly. A backlog <b>beyond</b> it does
 * not: the relay bundles only the first batch but carries a running hash for the whole queue, so the
 * peer rejects every bundle, the received watermark never advances, and the channel is wedged for good
 * (a known upstream limitation).
 *
 * <p>Keeping them in one class with honest names is deliberate. A single "drains 500 messages" test
 * would be red and would say nothing about whether the framework works.
 */
class BacklogDrainE2ETest {

    /** Comfortably below the default {@code maxMessagesPerBundle} of 10. */
    private static final int SUB_BATCH_BACKLOG = 8;

    @E2ETest(timeoutSeconds = 900)
    void drains_a_sub_batch_backlog_after_the_endpoints_start(final E2EEnvironment env) {
        final Fixture fixture = Fixture.create(env);
        env.startAll();

        // Stop the relays and let the queue build with nobody draining it. This is the scenario the
        // test exists for: the backlog must be present before anything starts consuming it.
        env.endpoints().stopAll();

        final InjectionRun run = env.messages().enqueue(fixture.channel.a(), SUB_BATCH_BACKLOG, MessageShape.small());
        run.awaitAllConfirmed(Duration.ofMinutes(3));

        assertThat(run.errorHistogram())
                .as("no send was rejected before reaching the pool")
                .isEmpty();
        assertThat(run.reverted()).as("no send reverted on chain").isZero();
        assertThat(run.confirmed()).isEqualTo(SUB_BATCH_BACKLOG);
        assertThat(run.messageIds())
                .as("every send reported its assigned id via MessageQueued")
                .hasSize(SUB_BATCH_BACKLOG);

        assertThat(fixture.channel.a().queueDepth())
                .as("the whole backlog is queued while the relays are down")
                .isEqualTo(SUB_BATCH_BACKLOG);

        env.endpoints().startAll();

        // The peer's received watermark is the oracle: it advances only after a message is processed
        // and is monotonic, so reaching N proves 1..N were handled in order.
        env.await().peerReceivedAll(fixture.channel.a(), run.messageIds(), Duration.ofMinutes(5));
        env.await().queueDrained(fixture.channel.a(), Duration.ofMinutes(5));

        assertThat(fixture.channel.b().state().receivedMessageId())
                .as("side B processed every queued message")
                .isEqualTo(SUB_BATCH_BACKLOG);

        // Throughput is recorded, not asserted. A hard threshold before several stable runs would
        // just be a flake generator; the number belongs in the log until a baseline exists.
        System.out.println("[e2e] drained " + SUB_BATCH_BACKLOG + " messages in " + run.wallClock());

        assertNoRelaySubmissionFailures(fixture, env);
    }

    /**
     * Known-red canary for {@code clpr-hiero#171}: a backlog larger than the relay's bundle batch.
     *
     * <p>Marked {@link Capability#DESTRUCTIVE} because the failure is not transient — once the peer has
     * rejected a bundle for a queue it cannot represent, the channel never advances again. It therefore
     * owns its environment and must never share one with another channel.
     *
     * <p><b>Currently {@link Disabled @Disabled}</b> pending the fix for {@code clpr-hiero#171}. Written
     * as a normal assertion rather than an inverted "assert it stays stuck": inverting it would make the
     * test pass forever and quietly stop tracking anything, so its red-to-green transition is the
     * evidence that a fix works. Re-enable by removing the {@code @Disabled} annotation once the relay
     * drains backlogs deeper than {@code maxMessagesPerBundle}. The suite now gates every PR as a
     * required check, so leaving this one red would additionally mask the next real regression instead
     * of reporting this one.
     */
    @Disabled("Known red — tracks clpr-hiero#171. Re-enable when the relay drains backlogs > maxMessagesPerBundle.")
    @E2ETest(timeoutSeconds = 1200, requires = Capability.DESTRUCTIVE)
    void drains_a_backlog_larger_than_the_bundle_batch(final E2EEnvironment env) {
        final Fixture fixture = Fixture.create(env);
        env.startAll();
        env.endpoints().stopAll();

        final int backlog = 25; // > maxMessagesPerBundle (10)
        final InjectionRun run = env.messages().enqueue(fixture.channel.a(), backlog, MessageShape.small());
        run.awaitAllConfirmed(Duration.ofMinutes(3));

        // The send side must be healthy, so a failure here is unambiguously about the send path and
        // not about the relay.
        assertThat(run.errorHistogram()).isEmpty();
        assertThat(run.reverted()).isZero();
        assertThat(run.confirmed()).isEqualTo(backlog);
        assertThat(fixture.channel.a().queueDepth()).isEqualTo(backlog);

        env.endpoints().startAll();

        // The relay assertion. Fails today: the peer rejects every bundle and the watermark stays at 0.
        env.await().peerReceivedAll(fixture.channel.a(), run.messageIds(), Duration.ofMinutes(8));
        env.await().queueDrained(fixture.channel.a(), Duration.ofMinutes(2));
    }

    @E2ETest(timeoutSeconds = 1200)
    void survives_rolling_restarts_under_load(final E2EEnvironment env) {
        final Fixture fixture = Fixture.create(env);
        env.startAll();

        // Backpressure keeps the in-flight window below the bundle batch, so no backlog can form and
        // the run measures restart resilience rather than re-testing clpr-hiero#171.
        final int total = 40;
        final InjectionRun run = env.messages()
                .enqueueWithBackpressure(fixture.channel.a(), total, 6, Duration.ofMinutes(8), MessageShape.small());
        run.awaitAllConfirmed(Duration.ofMinutes(3));

        assertThat(run.confirmed()).isEqualTo(total);
        assertThat(run.reverted()).isZero();

        // Restart mid-flight. The relay keeps no local state, so it has to resume from chain alone.
        env.endpoints().rollingRestart(Duration.ofSeconds(5));

        env.await().peerReceivedAll(fixture.channel.a(), run.messageIds(), Duration.ofMinutes(6));
        env.await().queueDrained(fixture.channel.a(), Duration.ofMinutes(3));

        assertThat(fixture.channel.b().state().receivedMessageId())
                .as("every message survived the restarts")
                .isEqualTo(total);

        // Restarts are where duplicate submissions would show up: a relay that re-sent an
        // already-processed bundle would either revert or double-count.
        assertNoRelaySubmissionFailures(fixture, env);
        for (final BesuNetwork network : env.besuNetworks().all()) {
            final long head = network.inspect().headBlockNumber();
            for (final EvmEndpoint endpoint : env.endpoints().all()) {
                assertThat(network.inspect().duplicatedNonces(endpoint.signingAddress(), 0, head))
                        .as("no nonce was used twice by " + endpoint.id() + " on " + network.id())
                        .isEmpty();
            }
        }
    }

    /** No relay-side submission may have reverted, been dropped, or been rejected on preview. */
    private static void assertNoRelaySubmissionFailures(final Fixture fixture, final E2EEnvironment env) {
        final Map<String, String> byChannel = Map.of("channel_id", fixture.channel.metricLabel());
        for (final EvmEndpoint endpoint : env.endpoints().all()) {
            assertThat(endpoint.metrics().counter("clpr_evm_tx_reverts", byChannel))
                    .as(endpoint.id() + " landed no reverted transaction")
                    .isZero();
            assertThat(endpoint.metrics().counterSum("clpr_evm_tx_queue_dropped"))
                    .as(endpoint.id() + " dropped nothing from its submit queue")
                    .isZero();
            assertThat(endpoint.metrics().counter("clpr_sync_bundle_skipped", byChannel))
                    .as(endpoint.id() + " had no bundle rejected by the on-chain preview")
                    .isZero();
        }
        for (final BesuNetwork network : env.besuNetworks().all()) {
            assertThat(network.inspect()
                            .revertedTransactions(0, network.inspect().headBlockNumber()))
                    .as("no reverted transactions on " + network.id())
                    .isEmpty();
        }
    }

    /** The topology every test here uses: two chains, two endpoints, one channel. */
    private record Fixture(BesuNetwork besuA, BesuNetwork besuB, EvmEndpoint epA, EvmEndpoint epB, Channel channel) {

        static Fixture create(final E2EEnvironment env) {
            final BesuNetwork besuA = env.besuNetworks().add(BesuNetworkSpec.singleNode());
            final BesuNetwork besuB = env.besuNetworks().add(BesuNetworkSpec.singleNode());
            final EvmEndpoint epA =
                    env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-a"));
            final EvmEndpoint epB =
                    env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-b"));
            final Channel channel =
                    env.channels().create(ChannelSpec.between(besuA, besuB).endpoints(epA, epB));
            return new Fixture(besuA, besuB, epA, epB, channel);
        }
    }
}
