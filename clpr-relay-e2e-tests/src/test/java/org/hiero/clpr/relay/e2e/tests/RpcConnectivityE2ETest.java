// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import org.hiero.clpr.relay.e2e.E2EEnvironment;
import org.hiero.clpr.relay.e2e.E2ETest;
import org.hiero.clpr.relay.e2e.E2ETiming;
import org.hiero.clpr.relay.e2e.TwoChainOneChannel;
import org.hiero.clpr.relay.e2e.chain.BesuNetwork;
import org.hiero.clpr.relay.e2e.chain.BesuNetworkSpec;
import org.hiero.clpr.relay.e2e.chain.BesuNode;
import org.hiero.clpr.relay.e2e.channel.Channel;
import org.hiero.clpr.relay.e2e.channel.ChannelSpec;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpoint;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpointSpec;
import org.hiero.clpr.relay.e2e.load.InjectionRun;
import org.hiero.clpr.relay.e2e.load.MessageShape;
import org.hiero.clpr.relay.integration.TestConditions;

/**
 * Whether the relays survive losing connectivity to their EVM RPC node and recover once it is back.
 *
 * <p>Covers issue #348. The point of splitting on failure mode is that "chain is unreachable" is not
 * one thing: a paused container keeps its port mapped and blocks reads until it is resumed (the
 * relay sees RPC timeouts); a stopped container is gone, its port unbound, its data on the host
 * bind mount (so a restart genuinely resumes the chain rather than starting from genesis) — the
 * relay sees TCP {@code connection refused}. Two paths through the relay's outbound-RPC retry
 * logic; two tests.
 *
 * <p>Recovery is asserted the way {@link PeerNetworkFaultsE2ETest} does it: enqueue a fresh message
 * <em>after</em> healing and prove it reaches the peer end-to-end. If the per-channel sync loop or
 * the state-listener loop had died during the outage the message would never reach the peer,
 * regardless of what the metric gauges say.
 */
class RpcConnectivityE2ETest {

    /**
     * Twenty seconds is long enough for the sync and state-listener loops to fail several times and
     * enter exponential backoff, but short enough to keep the test brisk. Any observable
     * "the outage is doing something" signal (failing gauges, dropped bundles) surfaces by then.
     */
    private static final Duration OUTAGE_WINDOW = Duration.ofSeconds(20);

    @E2ETest(timeoutSeconds = 900)
    void relay_survives_transient_besu_pause_and_recovers(final E2EEnvironment env) {
        final TwoChainOneChannel fixture = TwoChainOneChannel.declare(env);
        env.startAll();

        // Sanity: the channel delivers before the outage, so a post-heal failure is attributable to
        // the outage and not a broken bring-up.
        sendAndAwait(env, fixture, 1, "MSG-BEFORE");
        final long receivedBefore = fixture.channel().b().state().receivedMessageId();

        // Docker pause: the container is frozen but its port stays mapped, so the relay sees RPC
        // timeouts — the shape of a real node outage from a client — instead of connection refusals.
        env.faults().pauseChain(fixture.besuB());
        try {
            E2ETiming.pause(OUTAGE_WINDOW, "besu-pause outage window");
            // Confirm the outage actually reached the SUT before healing: otherwise the recovery
            // assertion below would be checking "things work when nothing broke".
            assertOutageObservedByStateListener(fixture, "pause");
        } finally {
            env.faults().unpauseChain(fixture.besuB());
        }

        // Recovery: a message queued after the outage must land, which only holds if both the sync
        // loop and the state-listener loop survived their failures instead of dying.
        sendAndAwait(env, fixture, 1, "MSG-AFTER-PAUSE");
        assertRecovered(fixture, receivedBefore + 1);
        assertFailureGaugesCleared(fixture);
    }

    @E2ETest(timeoutSeconds = 900)
    void relay_survives_abrupt_besu_stop_and_recovers(final E2EEnvironment env) {
        final TwoChainOneChannel fixture = TwoChainOneChannel.declare(env);
        env.startAll();

        sendAndAwait(env, fixture, 1, "MSG-BEFORE");
        final long receivedBefore = fixture.channel().b().state().receivedMessageId();

        // Stop the primary Besu node of chain B outright: the container goes away, the port is
        // unmapped. The relay's next JSON-RPC call sees TCP `connection refused`, which travels a
        // different retry path from the timeouts a paused container produces. The chain's data
        // directory lives on a host bind mount, so start() resumes from where the chain left off
        // rather than genesis-resetting the world; without that the recovery assertion would pass
        // for the wrong reason.
        final BesuNode besuBPrimary = fixture.besuB().primaryNode();
        besuBPrimary.stop();
        // Framework-side sanity: confirm stop() actually took the container down rather than
        // silently no-op'ing. Cheap, catches a whole class of "the fault was never applied" bugs
        // that would otherwise make the recovery assertion pass for the wrong reason.
        assertThat(besuBPrimary.isRunning())
                .as("besuB primary must be stopped before the outage window begins")
                .isFalse();
        try {
            E2ETiming.pause(OUTAGE_WINDOW, "besu-stop outage window");
            // SUT-side sanity: same reason as above, one layer up. Proves the relay actually saw
            // the chain go away, so the recovery below is exercising real recovery machinery.
            assertOutageObservedByStateListener(fixture, "stop");
        } finally {
            // start() reassigns the host-mapped RPC port and fires a callback on BesuNetwork that
            // rebuilds the test JVM's cached JSON-RPC clients — nothing more is needed here.
            besuBPrimary.start();
        }

        sendAndAwait(env, fixture, 1, "MSG-AFTER-STOP");
        assertRecovered(fixture, receivedBefore + 1);
        assertFailureGaugesCleared(fixture);
    }

    /**
     * Isolates the transaction submitter's RPC-outage recovery path from the state-listener and
     * sync-loop paths the sibling tests already cover. Uses the E2E filter proxy to block only
     * {@code eth_sendRawTransaction} and {@code eth_getTransactionReceipt} on chain B while every
     * read method ({@code eth_call}, {@code eth_getBalance}, etc.) keeps flowing untouched.
     *
     * <p>The submitter path was invisible to the {@code docker pause} sibling test: pausing the
     * whole container freezes every RPC method at once, so ep-b's inbound gRPC handler bails at
     * its state-read {@code eth_call} before ever reaching {@code TransactionSubmitter.submitBundle}
     * and {@code clpr_evm_tx_failures} never moves. This variant keeps state reads alive, so the
     * handler proceeds all the way to the submitter and the submitter is where the outage lands —
     * exactly the surface issue #327 asks us to cover.
     *
     * <p>Chain A is unaffected throughout, so {@code sendMessage} keeps queueing on A during the
     * outage and the relay's queue must drain end-to-end once the writes are restored. The load-
     * bearing assertion is that {@code clpr_evm_tx_failures} on ep-b grows during the outage —
     * without that, the recovery claim could pass for the wrong reason (peer never routed anything
     * to the submitter).
     */
    @E2ETest(timeoutSeconds = 900)
    void submitter_recovers_when_send_rpc_is_blocked(final E2EEnvironment env) {
        // Inline topology: chain B opts into the filter proxy. TwoChainOneChannel does not because
        // proxy startup is opt-in per network — the only test that needs it is this one.
        final BesuNetwork besuA = env.besuNetworks().add(BesuNetworkSpec.singleNode());
        final BesuNetwork besuB =
                env.besuNetworks().add(BesuNetworkSpec.singleNode().withRpcFilterProxy());
        final EvmEndpoint epA = env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-a"));
        final EvmEndpoint epB = env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-b"));
        final Channel channel =
                env.channels().create(ChannelSpec.between(besuA, besuB).endpoints(epA, epB));
        env.startAll();

        // Sanity: clean bring-up delivers before we break anything, so a post-heal failure is
        // attributable to the outage rather than a broken topology.
        sendAndAwaitOnChannel(env, channel, 1, "MSG-BEFORE");
        final long receivedBefore = channel.b().state().receivedMessageId();
        // Baseline for the submitter-side counter. Subsequent growth is the evidence the submitter
        // actually attempted a submitBundle while the write RPC was blocked.
        final long submitterFailuresBefore = epB.metrics().counterSum("clpr_evm_tx_failures");

        final int pending = 3;
        final InjectionRun run;
        env.faults().breakSubmitRpc(besuB);
        try {
            // Chain A is up and chain B's reads still work, so both connectors are healthy and the
            // sendMessage txs land on A immediately. Relay A picks them up via its state listener
            // and syncs them to ep-b; ep-b's ClprSyncHandler now runs all the way down to the
            // submitter (state reads succeed) and only there hits the blocked write RPC.
            run = env.messages().enqueue(channel.a(), pending, MessageShape.small());
            run.awaitAllConfirmed(Duration.ofMinutes(2));
            assertThat(run.errorHistogram())
                    .as("no send error while queueing during the outage")
                    .isEmpty();
            assertThat(run.confirmed())
                    .as("every send made it into A's queue during the outage")
                    .isEqualTo(pending);

            E2ETiming.pause(OUTAGE_WINDOW, "eth_sendRawTransaction blocked on chain B");

            // Queue-side sanity: A's queue only drains once B has acked delivery, which requires
            // B's submitter to land the bundle on chain B. A drained queue here would mean the
            // outage never held anything up.
            assertThat(channel.a().queueDepth())
                    .as("A's queue must not drain while ep-b cannot land writes on chain B")
                    .isPositive();
            // The load-bearing assertion for this test's scope: the submitter got called and its
            // write-RPC failed. Uses counterSum across the reason label because both preverify (if
            // it also depended on the receipt call) and rpc_error paths are legal.
            assertThat(epB.metrics().counterSum("clpr_evm_tx_failures"))
                    .as("ep-b's transaction submitter must attempt submitBundle and fail against the"
                            + " blocked write RPC")
                    .isGreaterThan(submitterFailuresBefore);
        } finally {
            env.faults().restoreSubmitRpc(besuB);
        }

        // Recovery: the pending queue must drain end-to-end. Without a working submitter retry,
        // either the queue would stall or a restart would be needed — which we deliberately do not
        // do. No listener/sync gauge assertions here: eth_call stayed alive throughout, so those
        // gauges are expected to be zero the whole time. The submitter-counter growth above is
        // this test's oracle.
        env.await().peerReceivedAll(channel.a(), run.messageIds(), Duration.ofMinutes(5));
        env.await().queueDrained(channel.a(), Duration.ofMinutes(2));
        assertThat(channel.b().state().receivedMessageId())
                .as("watermark on B advanced by exactly the pending count after the write RPC was restored")
                .isEqualTo(receivedBefore + pending);
        assertThat(channel.a().queueDepth())
                .as("A's queue drained after the write RPC was restored")
                .isZero();
    }

    /**
     * Channel-flavored variant of {@link #sendAndAwait} for tests that declare their topology
     * inline rather than through {@link TwoChainOneChannel}.
     */
    private static void sendAndAwaitOnChannel(
            final E2EEnvironment env, final Channel channel, final int count, final String label) {
        final InjectionRun run = env.messages().enqueue(channel.a(), count, MessageShape.small());
        run.awaitAllConfirmed(Duration.ofMinutes(2));
        env.await().peerReceivedAll(channel.a(), run.messageIds(), Duration.ofMinutes(5));
        env.await().queueDrained(channel.a(), Duration.ofMinutes(2));
        assertThat(run.errorHistogram()).as("no send error for " + label).isEmpty();
        assertThat(run.confirmed()).as(label + " confirmed count").isEqualTo(count);
    }

    /**
     * Enqueue {@code count} messages on side A and wait for the whole round trip: on-chain receipts
     * on A, delivery to B, then the ack draining A's queue. Returning from this helper means the
     * send has fully settled — assertions on {@code queueDepth}/{@code receivedMessageId} that
     * follow can be exact rather than eventual.
     */
    private static void sendAndAwait(
            final E2EEnvironment env, final TwoChainOneChannel fixture, final int count, final String label) {
        final InjectionRun run = env.messages().enqueue(fixture.channel().a(), count, MessageShape.small());
        // messageIds() requires the receipts to have been read — awaitAllConfirmed does that.
        run.awaitAllConfirmed(Duration.ofMinutes(2));
        env.await().peerReceivedAll(fixture.channel().a(), run.messageIds(), Duration.ofMinutes(5));
        // A's queue only drains once the peer's ack returns; awaiting that here lets callers assert
        // queueDepth exactly instead of racing the ack.
        env.await().queueDrained(fixture.channel().a(), Duration.ofMinutes(2));
        // Fail loudly if the send itself misbehaved before we blame the outage window.
        assertThat(run.errorHistogram()).as("no send error for " + label).isEmpty();
        assertThat(run.confirmed()).as(label + " confirmed count").isEqualTo(count);
    }

    /** Assert side B advanced to {@code expected} and its queue drained. */
    private static void assertRecovered(final TwoChainOneChannel fixture, final long expected) {
        assertThat(fixture.channel().b().state().receivedMessageId())
                .as("watermark on B advanced to exactly the expected id after the heal")
                .isEqualTo(expected);
        assertThat(fixture.channel().a().queueDepth())
                .as("side A queue drained after the heal")
                .isZero();
    }

    /**
     * Assert the RPC-node outage actually reached the relay. Called from inside the outage window,
     * just before healing — by then the state-listener loop on ep-b has failed several times against
     * the unreachable chain and its process-wide failure gauge is above zero. A zero here would mean
     * the fault-injection call was a no-op, so the "recovery" step that follows would exercise
     * nothing.
     */
    private static void assertOutageObservedByStateListener(final TwoChainOneChannel fixture, final String faultKind) {
        assertThat(fixture.epB().metrics().gauge("clpr_evm_listener_channels_failing", Map.of()))
                .as("ep-b's state-listener must observe besu-B as unreachable during the " + faultKind
                        + " outage window")
                .isGreaterThan(0.0);
    }

    /**
     * The process-wide failure gauges — sync loop and state-listener loop — must both settle to zero.
     * They rise during the outage; a persistent non-zero here would mean a loop is still stuck.
     *
     * <p>Awaited rather than sampled. The sync loop and state-listener loop run on independent
     * intervals and only clear the gauge on their next successful cycle after the heal. When the
     * recovery message lands, one of the two loops has just succeeded (whichever pushed the
     * bundle) but the other may still be waiting out its exponential backoff from the outage or
     * its next poll tick. Sampling right after {@code sendAndAwait} races that gap; awaiting it
     * closes cleanly the moment the slower loop next runs.
     */
    private static void assertFailureGaugesCleared(final TwoChainOneChannel fixture) {
        for (final EvmEndpoint endpoint : new EvmEndpoint[] {fixture.epA(), fixture.epB()}) {
            TestConditions.awaitCondition(
                    Duration.ofSeconds(60),
                    () -> endpoint.metrics().gauge("clpr_sync_channels_failing", Map.of()) == 0.0);
            TestConditions.awaitCondition(
                    Duration.ofSeconds(60),
                    () -> endpoint.metrics().gauge("clpr_evm_listener_channels_failing", Map.of()) == 0.0);
            assertThat(endpoint.metrics().gauge("clpr_sync_channels_failing", Map.of()))
                    .as(endpoint.id() + " sync loop reports no failing channels")
                    .isEqualTo(0.0);
            assertThat(endpoint.metrics().gauge("clpr_evm_listener_channels_failing", Map.of()))
                    .as(endpoint.id() + " state-listener reports no failing channels")
                    .isEqualTo(0.0);
        }
    }
}
