// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.hiero.clpr.relay.e2e.E2EEnvironment;
import org.hiero.clpr.relay.e2e.E2ETest;
import org.hiero.clpr.relay.e2e.chain.BesuNetwork;
import org.hiero.clpr.relay.e2e.chain.BesuNetworkSpec;
import org.hiero.clpr.relay.e2e.chain.E2eContracts;
import org.hiero.clpr.relay.e2e.chain.TxSummary;
import org.hiero.clpr.relay.e2e.channel.Channel;
import org.hiero.clpr.relay.e2e.channel.ChannelSpec;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpoint;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpointSpec;
import org.hiero.clpr.relay.e2e.load.InjectionRun;
import org.hiero.clpr.relay.e2e.load.MessageShape;

/**
 * A single relay per side operating <b>two</b> {@code clprServices[]} entries on each chain, one
 * per channel, proving traffic on channel X never lands on channel Y's service and vice versa.
 *
 * <pre>
 *   besuA (svc_A_x, svc_A_y)  ── channel X ──  ep-a  &lt;--sync--&gt;  ep-b  ── channel X ──  besuB (svc_B_x, svc_B_y)
 *                             ── channel Y ──   |                 |   ── channel Y ──
 * </pre>
 *
 * <p>Distinct from {@code TwoChannelIsolationE2ETest}: that one runs two channels through two
 * <em>separate</em> endpoint pairs on the same pair of chains, so cross-talk would require the
 * chains themselves to leak. This one puts both channels through a <em>single</em> endpoint pair,
 * each configured with two {@code clprServices[]} entries with different signing keys, so
 * cross-talk would show up as a bundle landing on the wrong service address or being signed by the
 * wrong key — the two things a per-service routing bug in the relay would produce.
 *
 * <p>Attribution is by {@code (to, from)} pair. Every {@code submitBundle} transaction the relay
 * emits names the {@code ClprService} it targets as its {@code to} address and the signing account
 * for that service entry as its {@code from} address. The claim collapses to: no transaction with a
 * channel-X {@code to} was signed by a channel-Y key, and vice versa.
 */
class ServiceRoutingE2ETest {

    @E2ETest(timeoutSeconds = 900)
    void bundles_from_channel_X_never_land_on_service_of_channel_Y(final E2EEnvironment env) {
        final BesuNetwork besuA = env.besuNetworks().add(BesuNetworkSpec.singleNode());
        final BesuNetwork besuB = env.besuNetworks().add(BesuNetworkSpec.singleNode());

        // One endpoint per side. Each will pick up two clprServices[] entries — one per channel —
        // with its own signing key per entry, exactly the shape the multi-service isolation claim
        // is about.
        final EvmEndpoint epA = env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-a"));
        final EvmEndpoint epB = env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-b"));

        // Bring the chains up so the primary CLPR stack (used by channel X) is deployed. Deploying
        // the additional stack for channel Y is separate because deployAdditionalService() requires
        // the chain to be started; the environment defers that until startAll(), so we split the
        // topology declaration into two stages via prepareContracts().
        env.prepareContracts();

        final E2eContracts svcAy = besuA.deployAdditionalService();
        final E2eContracts svcBy = besuB.deployAdditionalService();

        assertThat(svcAy.clprService())
                .as("side A's channel-Y service must be a different address from its primary")
                .isNotEqualToIgnoringCase(besuA.contracts().clprService());
        assertThat(svcBy.clprService())
                .as("side B's channel-Y service must be a different address from its primary")
                .isNotEqualToIgnoringCase(besuB.contracts().clprService());

        // channel X uses the chains' primary services (default behaviour).
        final Channel channelX =
                env.channels().create(ChannelSpec.between(besuA, besuB).endpoints(epA, epB));
        // channel Y uses the additional deployments — the same two chains and endpoints, different
        // service addresses.
        final Channel channelY = env.channels()
                .create(ChannelSpec.between(besuA, besuB).endpoints(epA, epB).services(svcAy, svcBy));

        env.startAll();

        assertThat(channelX.idHex()).as("the two channels have distinct ids").isNotEqualTo(channelY.idHex());
        for (final EvmEndpoint endpoint : new EvmEndpoint[] {epA, epB}) {
            env.await().channelLive(endpoint, channelX.metricLabel(), Duration.ofMinutes(5));
            env.await().channelLive(endpoint, channelY.metricLabel(), Duration.ofMinutes(5));
        }

        // Each endpoint should have been given TWO signing accounts by Channels.configureEndpoints
        // — one per (endpoint, chain, service) group. The first is the primary account allocated
        // at endpoint creation time; the second is the extra account allocated on second use.
        assertThat(epA.signers())
                .as("ep-a should carry two signers, one per service on chain A")
                .hasSize(2);
        assertThat(epB.signers())
                .as("ep-b should carry two signers, one per service on chain B")
                .hasSize(2);

        // Inject on each channel in BOTH directions. Bundle submissions are done by the RECEIVING
        // side's relay (a message queued on A is bundled by A, sync'd over gRPC to B, and submitted
        // to chain B by ep-b) — so one-directional traffic exercises only two of the four signers.
        // The routing assertion below is per-signer, and the strongest form is "every signer that
        // actually operated the relay ended up submitting to only its own service": that needs
        // every signer to have produced at least one bundle transaction, hence traffic both ways.
        final InjectionRun runXAB = env.messages().enqueue(channelX.a(), 2, MessageShape.small());
        final InjectionRun runXBA = env.messages().enqueue(channelX.b(), 2, MessageShape.small());
        final InjectionRun runYAB = env.messages().enqueue(channelY.a(), 2, MessageShape.small());
        final InjectionRun runYBA = env.messages().enqueue(channelY.b(), 2, MessageShape.small());
        for (final InjectionRun run : new InjectionRun[] {runXAB, runXBA, runYAB, runYBA}) {
            run.awaitAllConfirmed(Duration.ofMinutes(2));
            assertThat(run.reverted()).as("no injections reverted").isZero();
        }
        env.await().peerReceivedAll(channelX.a(), runXAB.messageIds(), Duration.ofMinutes(3));
        env.await().peerReceivedAll(channelX.b(), runXBA.messageIds(), Duration.ofMinutes(3));
        env.await().peerReceivedAll(channelY.a(), runYAB.messageIds(), Duration.ofMinutes(3));
        env.await().peerReceivedAll(channelY.b(), runYBA.messageIds(), Duration.ofMinutes(3));

        // Signing accounts, per (endpoint, service). Assignment mirrors Channels.configureEndpoints:
        // the primary signer is used for the FIRST service the endpoint sees (channel X, declared
        // first) and a fresh signer is used for each subsequent service (channel Y).
        final String signerAx = epA.signers().get(0).address().toLowerCase(Locale.ROOT);
        final String signerAy = epA.signers().get(1).address().toLowerCase(Locale.ROOT);
        final String signerBx = epB.signers().get(0).address().toLowerCase(Locale.ROOT);
        final String signerBy = epB.signers().get(1).address().toLowerCase(Locale.ROOT);
        assertThat(Set.of(signerAx, signerAy, signerBx, signerBy))
                .as("all four (endpoint, service) signing accounts must be distinct")
                .hasSize(4);

        // Per-signer routing attribution. Every transaction each relay signer emitted must have
        // landed on the {@code ClprService} that signer was configured for, and none on the other
        // service. Checking by {@code from} rather than {@code to} filters out unrelated activity —
        // deployer bootstrap, channel operator wiring, message injectors — that also targets the
        // same service addresses but is not what the routing claim is about; a bug in the relay's
        // per-service dispatch would show up here as a submitBundle with a mismatched {@code to}.
        assertRelaySignerTargetsOnly(besuA, signerAx, besuA.contracts().clprService(), svcAy.clprService());
        assertRelaySignerTargetsOnly(
                besuA, signerAy, svcAy.clprService(), besuA.contracts().clprService());
        assertRelaySignerTargetsOnly(besuB, signerBx, besuB.contracts().clprService(), svcBy.clprService());
        assertRelaySignerTargetsOnly(
                besuB, signerBy, svcBy.clprService(), besuB.contracts().clprService());

        for (final BesuNetwork network : env.besuNetworks().all()) {
            assertThat(network.inspect()
                            .revertedTransactions(0, network.inspect().headBlockNumber()))
                    .as("no reverted transactions on " + network.id())
                    .isEmpty();
        }
        for (final EvmEndpoint endpoint : new EvmEndpoint[] {epA, epB}) {
            assertThat(endpoint.logs().errorLines())
                    .as(endpoint.id() + " logged no errors")
                    .isEmpty();
        }
    }

    /**
     * Assert every transaction {@code signer} sent on {@code network} targeted
     * {@code expectedService} — never {@code wrongService}. Also asserts the signer produced at
     * least one transaction, so a routing test that quietly skipped bundle submission cannot pass
     * vacuously.
     */
    private static void assertRelaySignerTargetsOnly(
            final BesuNetwork network, final String signer, final String expectedService, final String wrongService) {
        final long head = network.inspect().headBlockNumber();
        final List<TxSummary> transactions = network.inspect().transactionsFrom(signer, 0, head);
        final String expectedLower = expectedService.toLowerCase(Locale.ROOT);
        final String wrongLower = wrongService.toLowerCase(Locale.ROOT);
        assertThat(transactions)
                .as("relay signer %s must have submitted at least one transaction on %s", signer, network.id())
                .isNotEmpty();
        assertThat(transactions)
                .as(
                        "every transaction from %s on %s must target the matching service %s",
                        signer, network.id(), expectedService)
                .allSatisfy(tx -> assertThat(tx.to()).isEqualTo(expectedLower));
        assertThat(transactions)
                .as("no transaction from %s on %s may target the wrong service %s", signer, network.id(), wrongService)
                .noneSatisfy(tx -> assertThat(tx.to()).isEqualTo(wrongLower));
    }
}
