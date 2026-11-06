// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import org.hiero.clpr.relay.e2e.E2EEnvironment;
import org.hiero.clpr.relay.e2e.E2ETest;
import org.hiero.clpr.relay.e2e.E2ETiming;
import org.hiero.clpr.relay.e2e.TwoChainOneChannel;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpoint;
import org.hiero.clpr.relay.e2e.load.InjectionRun;
import org.hiero.clpr.relay.e2e.load.MessageShape;
import org.hiero.clpr.relay.integration.TestConditions;

/**
 * Whether the relays survive losing peer connectivity — the classic "relay A cannot reach relay B"
 * outage — and recover once the link is restored.
 *
 * <p>Uses the Toxiproxy shim (opt-in via {@link E2EEnvironment#enablePeerNetworkShim()}) so only the
 * peer-to-peer plane is broken. Chain traffic keeps flowing on both sides, so a failure here really
 * is a peer-connectivity failure and not a compounding chain outage. Recovery is asserted the way
 * every fault-injection test in this file does it: queue new load <em>after</em> healing and prove
 * it reaches the peer end-to-end. If the sync loops had died during the outage that final delivery
 * would never happen, no matter how healthy the relays look on paper.
 */
class PeerNetworkFaultsE2ETest {

    /**
     * Small burst so the queue is deep enough to observe pause/resume behaviour, but shallow enough
     * to stay under the default {@code maxMessagesPerBundle} (10) and avoid the
     * {@code clpr-hiero#171} wedge that {@link BacklogDrainE2ETest} documents.
     */
    private static final int MID_LOAD_BATCH = 6;

    /**
     * Twenty seconds is long enough for the per-channel sync loops to fail several times and enter
     * exponential backoff, without turning the test into a long-running one. Any observable "the
     * outage is doing something" signal (failing gauge, dropped connections) will have surfaced by
     * then.
     */
    private static final Duration OUTAGE_WINDOW = Duration.ofSeconds(20);

    @E2ETest(timeoutSeconds = 900)
    void relays_survive_peer_disconnect_and_deliver_after_reconnect(final E2EEnvironment env) {
        env.enablePeerNetworkShim();
        final TwoChainOneChannel fixture = TwoChainOneChannel.declare(env);
        env.startAll();

        // Sanity: the channel delivers before the disconnect, so a post-heal failure is attributable
        // to the outage rather than a broken bring-up. Wait for the ack to round-trip so we sample a
        // clean receivedBefore, otherwise this test races the ack.
        final InjectionRun sanity = env.messages().enqueue(fixture.channel().a(), 1, MessageShape.small());
        // messageIds() requires the receipts to have been read — awaitAllConfirmed does that.
        sanity.awaitAllConfirmed(Duration.ofMinutes(2));
        env.await().peerReceivedAll(fixture.channel().a(), sanity.messageIds(), Duration.ofMinutes(3));
        env.await().queueDrained(fixture.channel().a(), Duration.ofMinutes(2));
        final long receivedBefore = fixture.channel().b().state().receivedMessageId();

        // Snapshot ep-a's sync-loop failed-cycle counter before cutting — the observed-outage
        // assertion below asserts an increase, so it needs a baseline.
        final long epAFailedBefore = syncCyclesFailed(fixture.epA());

        // Cut the peer link. Chain traffic keeps flowing on both sides, so the relays keep observing
        // channel state — only the outbound-sync and inbound-sync paths break.
        env.faults().disconnectPeers(fixture.epA(), fixture.epB());

        // Enqueue a message during the outage. Two purposes: (1) the sync loop follows a
        // push-iff-progress model, so without pending work it stays idle and never records a
        // failure — this send gives it real work to try and fail on, so a no-op disconnect shows
        // up as "counter didn't move"; (2) the same message doubles as the recovery oracle — it
        // must be delivered once the link is restored.
        final InjectionRun blocked = env.messages().enqueue(fixture.channel().a(), 1, MessageShape.small());
        blocked.awaitAllConfirmed(Duration.ofMinutes(2));

        E2ETiming.pause(OUTAGE_WINDOW, "peer-disconnect outage window");
        // Confirm the disconnect actually cut the peer path before healing: otherwise the recovery
        // assertion below would be checking "things work when nothing broke".
        assertPeerDisconnectObservedBySyncLoop(fixture, epAFailedBefore);
        env.faults().reconnectPeers(fixture.epA(), fixture.epB());

        // Recovery: the message enqueued during the outage must land now that the link is back.
        // Only holds if both sync loops survived the outage instead of dying.
        env.await().peerReceivedAll(fixture.channel().a(), blocked.messageIds(), Duration.ofMinutes(3));
        env.await().queueDrained(fixture.channel().a(), Duration.ofMinutes(2));

        // Secondary assertion: exactly one delivery across the disconnect, no duplicates or gaps.
        assertThat(fixture.channel().b().state().receivedMessageId())
                .as("watermark on B advanced by exactly one across the disconnect")
                .isEqualTo(receivedBefore + 1);
        assertThat(blocked.messageIds()).hasSize(1);

        // The aggregate failing-channels gauge (process-wide, no per-channel label — the scrape
        // exposes `clpr_sync_channels_failing 0`) must return to zero once the link is healed.
        // Awaited because a skip cycle can transiently reset consecutiveFailures via onSuccess and
        // then a subsequent real failure can re-raise it — the gauge oscillates. A short polling
        // window lets both loops settle.
        for (final EvmEndpoint endpoint : new EvmEndpoint[] {fixture.epA(), fixture.epB()}) {
            TestConditions.awaitCondition(
                    Duration.ofSeconds(60),
                    () -> endpoint.metrics().gauge("clpr_sync_channels_failing", Map.of()) == 0.0);
            assertThat(endpoint.metrics().gauge("clpr_sync_channels_failing", Map.of()))
                    .as(endpoint.id() + " reports no failing channels after the heal")
                    .isEqualTo(0.0);
        }
    }

    @E2ETest(timeoutSeconds = 900)
    void mid_load_disconnect_and_reconnect_drains_without_duplication(final E2EEnvironment env) {
        env.enablePeerNetworkShim();
        final TwoChainOneChannel fixture = TwoChainOneChannel.declare(env);
        env.startAll();

        // Enqueue the full batch before cutting the link, so the disconnect straddles in-flight
        // delivery. `enqueue` returns once the sends are confirmed on chain — the peer processing
        // that follows is what the outage interrupts, not the queueing itself.
        final long receivedBefore = fixture.channel().b().state().receivedMessageId();
        final InjectionRun run = env.messages().enqueue(fixture.channel().a(), MID_LOAD_BATCH, MessageShape.small());
        run.awaitAllConfirmed(Duration.ofMinutes(2));

        // Snapshot ep-a's sync-loop failed-cycle counter before the cut — the observed-outage
        // assertion below asserts an increase, so it needs a baseline.
        final long epAFailedBefore = syncCyclesFailed(fixture.epA());

        // Immediately cut the link. The peer may already have processed some of the batch — the point
        // is that it must not lose or double-count any of it across the outage.
        env.faults().disconnectPeers(fixture.epA(), fixture.epB());

        // Enqueue one more during the outage. If the batch above was drained faster than we cut,
        // sync would have gone idle (push-iff-progress) and the failed-cycles counter would never
        // move. This keeper guarantees the sync loop has work to try and fail on so a no-op
        // disconnect surfaces as "counter didn't move". Counted in the recovery assertion below.
        final InjectionRun keeper = env.messages().enqueue(fixture.channel().a(), 1, MessageShape.small());
        keeper.awaitAllConfirmed(Duration.ofMinutes(2));

        E2ETiming.pause(OUTAGE_WINDOW, "mid-load peer-disconnect outage window");
        // Confirm the disconnect actually cut the peer path before healing: otherwise the recovery
        // assertion below would be checking "things work when nothing broke".
        assertPeerDisconnectObservedBySyncLoop(fixture, epAFailedBefore);
        env.faults().reconnectPeers(fixture.epA(), fixture.epB());

        // The whole batch plus the keeper must land — the count is exact, the watermark is
        // monotonic, and any duplicated message would break the running-hash chain the peer verifies
        // on every bundle.
        final int totalDelivered = MID_LOAD_BATCH + 1;
        env.await().peerReceived(fixture.channel().a(), receivedBefore + totalDelivered, Duration.ofMinutes(5));
        env.await().queueDrained(fixture.channel().a(), Duration.ofMinutes(2));

        assertThat(fixture.channel().b().state().receivedMessageId())
                .as("watermark on B advanced by exactly the batch size + keeper after the heal")
                .isEqualTo(receivedBefore + totalDelivered);
    }

    /**
     * Assert the peer disconnect actually reached the sending relay. Compares the monotonic
     * {@code clpr_sync_cycles_failed} counter on ep-a against a pre-disconnect baseline: any
     * failed sync cycle during the window bumps the total, so a zero delta means
     * {@code disconnectPeers} was a no-op (Toxiproxy proxy didn't disable, roster routed around
     * the shim, etc.), and the "recovery" step that follows would exercise nothing.
     *
     * <p>A counter, not a gauge. The equivalent gauge ({@code clpr_sync_channels_failing}) is
     * derived from {@code FailState.consecutiveFailures()} which is reset to 0 by
     * {@code failState.onSuccess()} at the end of every non-throwing {@code executeSyncCycle}. A
     * skip cycle (empty bundle cache, no available peer) returns normally and calls
     * {@code onSuccess} — so a single skip between failures silently zeros the gauge. Counters
     * don't have that problem.
     *
     * <p>Only the sending side (ep-a) is checked. The disconnect is bidirectional at the network
     * layer, but the receiving side's sync loop follows push-iff-progress and has no reason to
     * push during the outage: nothing new arrives from A (peer unreachable), so B's chain state
     * doesn't advance, so B's sync stays idle. An idle loop doesn't fail — asserting an increase
     * on ep-b here would fail deterministically for the wrong reason.
     */
    private static void assertPeerDisconnectObservedBySyncLoop(
            final TwoChainOneChannel fixture, final long epAFailedCyclesBefore) {
        assertThat(fixture.epA().metrics().counterSum("clpr_sync_cycles_failed"))
                .as("ep-a sync loop must record at least one failed cycle during the disconnect window")
                .isGreaterThan(epAFailedCyclesBefore);
    }

    /** Convenience: read the sync-loop cycles-failed counter for an endpoint. */
    private static long syncCyclesFailed(final EvmEndpoint endpoint) {
        return endpoint.metrics().counterSum("clpr_sync_cycles_failed");
    }
}
