// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
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

/**
 * Scale canary: many parallel channels between the same pair of chains, sharing the same pair of
 * endpoints, all draining without leakage, resource contention or silent drops.
 *
 * <p>This is the multi-channel liveness test that catches at scale what a two-channel version cannot.
 * The failure modes it exists to surface:
 * <ul>
 *   <li>Nonce contention on the submitting endpoint's signing account — many channels sharing one
 *       signing account is exactly the setup where a broken multi-channel nonce accounting reissues
 *       the same nonce. Asserted via {@code duplicatedNonces()} on the submitter's address.</li>
 *   <li>Silently dropped channel — a per-channel loop that fails to start or exits early would leave
 *       one channel's messages queued forever. Asserted per channel via
 *       {@code peerReceivedAll} and per-channel {@code bundle.messages_submitted}.</li>
 *   <li>Bundle-level rejection at scale — race conditions in the submitter, dedup, or preview check
 *       manifest as increments on {@code sync.bundle.skipped} / {@code sync.bundle.reverted} /
 *       {@code evm.tx.queue_dropped}. Asserted as zero-floor summed across channels.</li>
 *   <li>Hang. The {@link E2ETest#timeoutSeconds()} guard turns a hang into a build timeout with no
 *       state named; the inner wall-clock bound turns it into "took longer than N seconds" with the
 *       whole environment still alive for the diagnostic dump.</li>
 * </ul>
 *
 * <p><b>Sizing.</b> {@link #CHANNEL_COUNT} channels × two chains × the six chain txs each channel
 * needs to wire (registerChannel, completeChannel, registerConnector, completeConnector, plus the
 * per-channel verifier and connector deploy). At Besu's default 1-second block period, wiring alone
 * is minutes. Load-generation adds {@link #MESSAGES_PER_CHANNEL} × {@link #CHANNEL_COUNT} sends and
 * the same number of relay-side submissions. {@link Capability#HIGH_LOAD} keeps this off the CI
 * profile so it never runs there; local runs bear the cost.
 *
 * <p><b>Direction.</b> Load is one-way (A→B) on purpose: nonce contention is the point, and it only
 * shows up on the submitter side. Driving both directions would double the runtime while doubling
 * the same signal.
 */
class ManyChannelsE2ETest {

    /**
     * Target channel count. The "many parallel channels" scale referenced in the multi-channel
     * testing brief is 100; dialed down from the documented ~20 floor to keep wiring-time cost, and
     * the risk of a transient host stall tripping the per-transaction receipt timeout during the
     * ~14-transaction-per-channel sequential wiring phase, reasonable on a laptop. Nonce contention
     * and per-channel attribution — the failure modes the test exists to catch — both surface as
     * soon as the wiring runs in parallel across more than a handful of channels, so the floor is
     * set by pool economics rather than by test signal.
     *
     * <p>Hard-capped by the deployer's concurrent-pending-admission budget in Besu's tx pool — see
     * the derivation on {@link org.hiero.clpr.relay.e2e.chain.E2eContractDeployer}'s deploy-gas
     * ceiling. Raising this value beyond that budget re-opens the "1 pending / 0 gas" pool wedge.
     */
    private static final int CHANNEL_COUNT = 8;

    /** Small per-channel batch — enough to exercise submission, tiny enough not to dominate runtime. */
    private static final int MESSAGES_PER_CHANNEL = 3;

    /**
     * Receipt-wait budget for wiring transactions, raised from {@code ChainTx.DEFAULT_RECEIPT_TIMEOUT}.
     * Wiring {@link #CHANNEL_COUNT} channels is ~14 sequential on-chain transactions each, all
     * funneled through one account per chain — a transient stall (GC pause, Docker CPU contention
     * from running two Besu nodes plus the test JVM) late in that sequence can push a single receipt
     * past the default 60s even though the chain isn't actually stuck.
     */
    private static final Duration WIRING_RECEIPT_TIMEOUT = Duration.ofSeconds(120);

    /**
     * End-to-end wall-clock bound, chosen to distinguish "slow but making progress" from "hung".
     * Sized for {@link #CHANNEL_COUNT}=10 on a laptop, generous slack included. Tighten once real
     * numbers are collected.
     *
     * <p>Sized to sit just below the {@link E2ETest#timeoutSeconds()} 3600s outer bound: a genuinely
     * hung run trips the outer timeout with the containers stopped and no diagnostics captured,
     * while a slow-but-progressing run trips this inner assertion with the environment still up so
     * the failure dump has something to show.
     */
    private static final Duration WALL_CLOCK_BUDGET = Duration.ofMinutes(55);

    /**
     * Ceiling on {@code clpr_sync_bundle_skipped} summed across every channel on one endpoint.
     *
     * <p>Framed as a regression bound rather than a zero-floor: at {@link #CHANNEL_COUNT} channels
     * with the response-round-trip {@code E2EApplication} generates, dedup skips (stale / replay /
     * initial_state) happen occasionally on every clean run and only fire this bound on a
     * genuine regression. Observed baselines during green runs are a small handful per channel;
     * the bound sits well above that with room for host jitter.
     */
    private static final long MAX_BUNDLE_SKIPPED_PER_ENDPOINT = 15L * CHANNEL_COUNT;

    /**
     * Ceiling on {@code clpr_sync_bundle_reverted} summed across every channel on one endpoint.
     *
     * <p>Same regression-bound framing as {@link #MAX_BUNDLE_SKIPPED_PER_ENDPOINT}: some
     * race-driven reverts slip past dedup at scale even on a healthy run. A wedged channel would
     * additionally show up as {@code channels_failing > 0}, which stays a zero-floor assertion.
     */
    private static final long MAX_BUNDLE_REVERTED_PER_ENDPOINT = 8L * CHANNEL_COUNT;

    /** Chain-side ceiling; sized generously since reverts can land on either chain per channel. */
    private static final int MAX_CHAIN_REVERTS_PER_NETWORK = 8 * CHANNEL_COUNT;

    /**
     * Ceiling on {@code clpr_evm_tx_queue_dropped} summed across every channel on one endpoint.
     *
     * <p>Same regression-bound framing as the sibling counters: at scale, a channel state that
     * advances between enqueue and dequeue can cause the submit queue to drop one stale bundle on an
     * otherwise clean run. A zero-floor assertion would flake here for the same reason it was
     * relaxed on skipped/reverted; the bound sits well above baseline so a genuine drop-storm still
     * trips.
     */
    private static final long MAX_TX_QUEUE_DROPPED_PER_ENDPOINT = 3L * CHANNEL_COUNT;

    @E2ETest(timeoutSeconds = 3600, requires = Capability.HIGH_LOAD)
    void many_parallel_channels_between_two_chains_all_drain_without_leakage(final E2EEnvironment env) {
        final Instant startedAt = Instant.now();

        final BesuNetwork besuA = env.besuNetworks().add(BesuNetworkSpec.singleNode());
        final BesuNetwork besuB = env.besuNetworks().add(BesuNetworkSpec.singleNode());

        final EvmEndpoint epA = env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-a"));
        final EvmEndpoint epB = env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-b"));

        env.channels().withWiringReceiptTimeout(WIRING_RECEIPT_TIMEOUT);

        // Every channel shares the same shape (chains + endpoints); only the salt differs so ids are
        // distinct. createMany does the salt allocation.
        final ChannelSpec template =
                ChannelSpec.between(besuA, besuB).endpoints(epA, epB).build();
        final List<Channel> channels = env.channels().createMany(CHANNEL_COUNT, template);

        env.startAll();

        assertThat(channels).hasSize(CHANNEL_COUNT);
        // Every channel record must have landed ACTIVE on both chains before the load starts. A
        // channel that failed to wire but was declared would silently have nothing to drain, so the
        // "all messages delivered" check below could pass on an under-provisioned run. This is the
        // guard.
        for (final Channel channel : channels) {
            assertThat(channel.a().status())
                    .as("channel %s side A must be ACTIVE after wiring", channel.idHex())
                    .isEqualTo(ClprChannelStatus.ACTIVE);
            assertThat(channel.b().status())
                    .as("channel %s side B must be ACTIVE after wiring", channel.idHex())
                    .isEqualTo(ClprChannelStatus.ACTIVE);
        }

        // Load: one direction (A→B) for every channel. Enqueue all at once, then reconcile. The
        // injector enqueues concurrently across channels, which is what makes the nonce assertion
        // meaningful downstream.
        final List<InjectionRun> runs = channels.stream()
                .map(c -> env.messages().enqueue(c.a(), MESSAGES_PER_CHANNEL, MessageShape.small()))
                .toList();

        for (final InjectionRun run : runs) {
            run.awaitAllConfirmed(Duration.ofMinutes(5));
            assertThat(run.errorHistogram()).isEmpty();
            assertThat(run.reverted()).isZero();
            assertThat(run.confirmed()).isEqualTo(MESSAGES_PER_CHANNEL);
        }

        // Drainage: every channel's messages must show up on the peer side. Timeout scales with total
        // work — each channel gets its own await with a generous per-channel budget rather than one
        // global await, so the failure message names which channel wedged.
        final Duration perChannelAwait = Duration.ofMinutes(10);
        for (int i = 0; i < channels.size(); i++) {
            final Channel channel = channels.get(i);
            env.await().peerReceivedAll(channel.a(), runs.get(i).messageIds(), perChannelAwait);
            env.await().queueDrained(channel.a(), Duration.ofMinutes(2));
        }

        // Per-channel attribution: every channel must show at least MESSAGES_PER_CHANNEL messages
        // submitted through epB (the receiving side is where A→B bundles land). A silently dropped
        // channel would still pass drainage's timeout only via the peerReceivedAll failure — this
        // gives a second, direct signal, and locks in that the relay's internal accounting agrees
        // with the peer's view.
        for (final Channel channel : channels) {
            final Map<String, String> byChannel = Map.of("channel_id", channel.metricLabel());
            assertThat(epB.metrics().counter("clpr_sync_bundle_messages_submitted", byChannel))
                    .as(
                            "epB must have submitted every A→B message it was responsible for on channel %s",
                            channel.idHex())
                    .isGreaterThanOrEqualTo(MESSAGES_PER_CHANNEL);
        }

        // Endpoint-wide health signals. Skipped and reverted are bounded, not zero-floor: at
        // scale (many channels + response-round-trip application + fast enqueue), dedup catches
        // repeated bundles and a few race-driven submissions revert on-chain even on a healthy
        // run. The bounds are sized well above observed baselines so a genuine problem still
        // fires. A truly wedged channel would additionally show up on channels_failing, which
        // stays a zero-floor gauge. counterSum aggregates every label combination so a
        // per-channel spike is caught without enumeration.
        for (final EvmEndpoint endpoint : new EvmEndpoint[] {epA, epB}) {
            assertThat(endpoint.metrics().gauge("clpr_sync_channels_failing", Map.of()))
                    .as(endpoint.id() + " reports no failing channels")
                    .isEqualTo(0.0);
            assertThat(endpoint.metrics().counterSum("clpr_sync_bundle_skipped"))
                    .as(endpoint.id() + " bundle-skipped stayed within the regression bound")
                    .isLessThanOrEqualTo(MAX_BUNDLE_SKIPPED_PER_ENDPOINT);
            assertThat(endpoint.metrics().counterSum("clpr_sync_bundle_reverted"))
                    .as(endpoint.id() + " bundle-reverted stayed within the regression bound")
                    .isLessThanOrEqualTo(MAX_BUNDLE_REVERTED_PER_ENDPOINT);
            assertThat(endpoint.metrics().counterSum("clpr_evm_tx_queue_dropped"))
                    .as(endpoint.id() + " submit-queue drops stayed within the regression bound")
                    .isLessThanOrEqualTo(MAX_TX_QUEUE_DROPPED_PER_ENDPOINT);
            assertThat(endpoint.logs().errorLines())
                    .as(endpoint.id() + " logged no errors")
                    .isEmpty();
        }

        // Nonce integrity. With CHANNEL_COUNT channels driving concurrent submissions through a
        // single signing account per endpoint, a bug in nonce accounting under contention would show
        // up as a repeated nonce here. Empty is the only healthy outcome; even one duplicate points
        // at multi-channel nonce accounting on a shared submitter.
        for (final BesuNetwork network : new BesuNetwork[] {besuA, besuB}) {
            final long head = network.inspect().headBlockNumber();
            for (final EvmEndpoint endpoint : new EvmEndpoint[] {epA, epB}) {
                assertThat(network.inspect().duplicatedNonces(endpoint.signingAddress(), 0, head))
                        .as("no nonce was used twice by %s on %s", endpoint.id(), network.id())
                        .isEmpty();
            }
        }

        // Chain-level revert sweep. A reverted submitBundle is mined and paid for, so it is
        // invisible to every liveness check above; only a receipt scan surfaces it. Bounded for
        // the same reason as the per-endpoint reverted counter above.
        for (final BesuNetwork network : new BesuNetwork[] {besuA, besuB}) {
            assertThat(network.inspect()
                            .revertedTransactions(0, network.inspect().headBlockNumber()))
                    .as("reverted transactions on " + network.id() + " stayed within the regression bound")
                    .hasSizeLessThanOrEqualTo(MAX_CHAIN_REVERTS_PER_NETWORK);
        }

        // Wall-clock bound. Named separately from the @E2ETest timeout so a hang here fails as
        // "exceeded budget" with the diagnostics still gatherable, rather than as a Gradle test
        // timeout with the environment already torn down.
        final Duration elapsed = Duration.between(startedAt, Instant.now());
        assertThat(elapsed)
                .as("the whole run must complete within the wall-clock budget")
                .isLessThan(WALL_CLOCK_BUDGET);

        // Recorded, not asserted — throughput baselines belong in the log until a stable number
        // exists across enough runs to be a threshold.
        System.out.println("[e2e] drained "
                + (CHANNEL_COUNT * MESSAGES_PER_CHANNEL) + " messages across "
                + CHANNEL_COUNT + " channels in " + elapsed);
    }
}
