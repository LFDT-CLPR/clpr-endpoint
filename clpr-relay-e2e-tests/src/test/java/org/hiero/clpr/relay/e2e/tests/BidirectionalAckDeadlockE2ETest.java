// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.hiero.clpr.relay.e2e.E2EEnvironment;
import org.hiero.clpr.relay.e2e.E2ETest;
import org.hiero.clpr.relay.e2e.TwoChainOneChannel;
import org.hiero.clpr.relay.e2e.load.InjectionRun;
import org.hiero.clpr.relay.e2e.load.MessageShape;

/**
 * Regression guard for the CLPR symmetric-send replay deadlock.
 *
 * <p>Scenario: both sides queue messages while their endpoints are down, so both peers come back up
 * facing {@code recv=N, acked=N-1}. With the bug, the piggybacked ack rides on a replay bundle the
 * on-chain preview rejects as {@code ClprInvalidPreviousMessageHash} and {@code ackedMessageId} stalls
 * on both sides forever. Fix requires all three: SC accepts duplicate messages, endpoints trim
 * duplicates before submitting, and the reciprocal ack is delivered independently of the replay
 * bundle's acceptance.
 *
 * <p>Draining both queues is the oracle: {@code queueDepth == 0} on side X means every message X sent
 * was acknowledged by its peer, which is exactly the property the deadlock destroys.
 */
class BidirectionalAckDeadlockE2ETest {

    /** Two per side — the smallest amount that produces a queue depth &gt; 0 on both sides while down. */
    private static final int MESSAGES_PER_SIDE = 2;

    @E2ETest(timeoutSeconds = 900)
    void both_sides_drain_after_symmetric_backlog_with_endpoints_down(final E2EEnvironment env) {
        final TwoChainOneChannel fixture = TwoChainOneChannel.declare(env);
        env.startAll();

        // Bring the relays down so the queue can build unattended on both sides. That is what forces
        // the symmetric recv=N, acked=N-1 state as soon as the relays come back up.
        env.endpoints().stopAll();

        final InjectionRun runFromA =
                env.messages().enqueue(fixture.channel().a(), MESSAGES_PER_SIDE, MessageShape.small());
        final InjectionRun runFromB =
                env.messages().enqueue(fixture.channel().b(), MESSAGES_PER_SIDE, MessageShape.small());

        runFromA.awaitAllConfirmed(Duration.ofMinutes(3));
        runFromB.awaitAllConfirmed(Duration.ofMinutes(3));

        assertThat(runFromA.errorHistogram())
                .as("side A sends were all accepted by the pool")
                .isEmpty();
        assertThat(runFromB.errorHistogram())
                .as("side B sends were all accepted by the pool")
                .isEmpty();
        assertThat(runFromA.reverted()).as("no side A send reverted").isZero();
        assertThat(runFromB.reverted()).as("no side B send reverted").isZero();
        assertThat(runFromA.confirmed()).isEqualTo(MESSAGES_PER_SIDE);
        assertThat(runFromB.confirmed()).isEqualTo(MESSAGES_PER_SIDE);

        assertThat(fixture.channel().a().queueDepth())
                .as("side A's whole backlog is queued while relays are down")
                .isEqualTo(MESSAGES_PER_SIDE);
        assertThat(fixture.channel().b().queueDepth())
                .as("side B's whole backlog is queued while relays are down")
                .isEqualTo(MESSAGES_PER_SIDE);

        env.endpoints().startAll();

        // Both peers must process the messages sent to them. Delivery is a necessary precondition for
        // acknowledgement; splitting it out makes a delivery-side stall (recv never advances) legible
        // separately from an ack-side stall (recv advances, acked doesn't) — which is the deadlock.
        env.await().peerReceivedAll(fixture.channel().a(), runFromA.messageIds(), Duration.ofMinutes(5));
        env.await().peerReceivedAll(fixture.channel().b(), runFromB.messageIds(), Duration.ofMinutes(5));

        // The deadlock oracle: with the bug, one side (or both) stays wedged at acked = N-1 forever.
        env.await().queueDrained(fixture.channel().a(), Duration.ofMinutes(5));
        env.await().queueDrained(fixture.channel().b(), Duration.ofMinutes(5));

        assertThat(fixture.channel().a().state().ackedMessageId())
                .as("side A eventually learned B acknowledged every message it sent")
                .isGreaterThanOrEqualTo(MESSAGES_PER_SIDE);
        assertThat(fixture.channel().b().state().ackedMessageId())
                .as("side B eventually learned A acknowledged every message it sent")
                .isGreaterThanOrEqualTo(MESSAGES_PER_SIDE);
    }
}
