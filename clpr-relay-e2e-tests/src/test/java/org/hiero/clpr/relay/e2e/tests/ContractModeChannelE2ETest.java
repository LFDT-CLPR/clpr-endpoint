// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.hiero.clpr.relay.e2e.E2EEnvironment;
import org.hiero.clpr.relay.e2e.E2ETest;
import org.hiero.clpr.relay.e2e.chain.BesuNetwork;
import org.hiero.clpr.relay.e2e.chain.BesuNetworkSpec;
import org.hiero.clpr.relay.e2e.channel.Channel;
import org.hiero.clpr.relay.e2e.channel.ChannelSpec;
import org.hiero.clpr.relay.e2e.channel.Channels;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpoint;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpointSpec;
import org.hiero.clpr.relay.e2e.endpoint.RelayYamlWriter;
import org.hiero.clpr.relay.e2e.load.InjectionRun;
import org.hiero.clpr.relay.e2e.load.MessageShape;

/**
 * A channel between two independent Besu QBFT chains, completed against the real
 * {@code QbftContractModeVerifier} — the standalone verifier for Besu's "contract"
 * validator-selection mode, deployed and driven via {@link Channels#wireContractMode}.
 *
 * <p>Covers, in both directions against the real verifier: {@code verifyConfig} (via
 * {@code QbftContractModeConfigProofBuilder}) and {@code verifyBundle} in the steady state (no
 * rotation), exercising {@code QbftContractModeBundleConstructor} in {@code clpr-relay-evm}. It does
 * <b>not</b> cover a validator-set rotation (a "hop") or
 * {@code QbftContractModeLedgerConfigurationProvider} (the {@code getLedgerConfiguration}
 * contract-mode payload) — both Besu chains stay in ordinary header/{@code extra_data} consensus
 * mode throughout, and this test never calls {@code getLedgerConfiguration}; see
 * {@link Channels#wireContractMode} for why steady-state bundle delivery is nonetheless a faithful
 * test of the contract-mode verifier's bundle path.
 *
 * <p>Bypasses {@link E2EEnvironment#startAll()} (which would wire every declared channel through the
 * legacy header-mode path via {@link Channels#wireAll()}) and replicates its sequence manually,
 * substituting {@link Channels#wireContractMode} and configuring the endpoints' {@code relay.yaml}
 * with a {@code qbftContractMode} block by hand — see {@link RelayYamlWriter.ContractValidators}.
 */
class ContractModeChannelE2ETest {

    private static final int MESSAGES_PER_SIDE = 2;

    @E2ETest(timeoutSeconds = 900)
    void channel_delivers_messages_in_both_directions_with_the_real_contract_mode_verifier(final E2EEnvironment env) {
        final BesuNetwork besuA = env.besuNetworks().add(BesuNetworkSpec.singleNode());
        final BesuNetwork besuB = env.besuNetworks().add(BesuNetworkSpec.singleNode());

        final EvmEndpoint epA = env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-a"));
        final EvmEndpoint epB = env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-b"));

        final Channel channel =
                env.channels().create(ChannelSpec.between(besuA, besuB).endpoints(epA, epB));

        env.besuNetworks().startAll();
        env.besuNetworks().deployAll();
        final Channels.ContractModeWiring wiring = env.channels().wireContractMode(channel);
        configureEndpoint(epA, besuA, besuB, channel, wiring.a());
        configureEndpoint(epB, besuB, besuA, channel, wiring.b());
        env.endpoints().startAll();

        assertThat(channel.idHex()).hasSize(66).startsWith("0x");
        assertThat(channel.a().status()).isEqualTo(ClprChannelStatus.ACTIVE);
        assertThat(channel.b().status()).isEqualTo(ClprChannelStatus.ACTIVE);
        assertThat(channel.a().queueDepth()).isZero();
        assertThat(channel.b().queueDepth()).isZero();

        assertThat(channel.a().connector().balance())
                .as("side A connector is funded")
                .isEqualTo(ChannelSpec.DEFAULT_CONNECTOR_FUNDING_WEI);
        assertThat(channel.b().connector().balance())
                .as("side B connector is funded")
                .isEqualTo(ChannelSpec.DEFAULT_CONNECTOR_FUNDING_WEI);

        for (final EvmEndpoint endpoint : new EvmEndpoint[] {epA, epB}) {
            env.await().channelLive(endpoint, channel.metricLabel(), Duration.ofMinutes(3));
        }

        // channelLive only asserts a sync cycle happened — it says nothing about verifyBundle ever
        // being called with a non-trivial bundle. Sending in both directions and requiring delivery
        // is what actually exercises QbftContractModeBundleConstructor's output against the real
        // on-chain verifier.
        final InjectionRun runFromA = env.messages().enqueue(channel.a(), MESSAGES_PER_SIDE, MessageShape.small());
        final InjectionRun runFromB = env.messages().enqueue(channel.b(), MESSAGES_PER_SIDE, MessageShape.small());

        runFromA.awaitAllConfirmed(Duration.ofMinutes(3));
        runFromB.awaitAllConfirmed(Duration.ofMinutes(3));
        assertThat(runFromA.reverted()).as("no side A send reverted").isZero();
        assertThat(runFromB.reverted()).as("no side B send reverted").isZero();

        env.await().peerReceivedAll(channel.a(), runFromA.messageIds(), Duration.ofMinutes(5));
        env.await().peerReceivedAll(channel.b(), runFromB.messageIds(), Duration.ofMinutes(5));
        env.await().queueDrained(channel.a(), Duration.ofMinutes(5));
        env.await().queueDrained(channel.b(), Duration.ofMinutes(5));

        for (final EvmEndpoint endpoint : new EvmEndpoint[] {epA, epB}) {
            assertThat(endpoint.metrics().gauge("clpr_sync_channels_failing", Map.of()))
                    .as(endpoint.id() + " reports no failing channels")
                    .isEqualTo(0.0);
        }

        for (final BesuNetwork network : env.besuNetworks().all()) {
            assertThat(network.inspect()
                            .revertedTransactions(0, network.inspect().headBlockNumber()))
                    .as("no reverted transactions on " + network.id())
                    .isEmpty();
        }

        assertThat(epA.logs().errorLines()).as("relay A logged no errors").isEmpty();
        assertThat(epB.logs().errorLines()).as("relay B logged no errors").isEmpty();
    }

    /**
     * Wires {@code endpoint} to serve {@code local}'s side of {@code channel}, with a
     * {@code qbftContractMode} block naming {@code side}'s validator contract — the one piece
     * {@link EvmEndpoint#serve} cannot express.
     */
    private static void configureEndpoint(
            final EvmEndpoint endpoint,
            final BesuNetwork local,
            final BesuNetwork peer,
            final Channel channel,
            final Channels.ContractModeSide side) {
        final EvmEndpointSpec spec = endpoint.spec();
        endpoint.addLocalNetwork(new RelayYamlWriter.LocalNetwork(
                local.id(),
                local.chainId(),
                local.endpointRpcUrl(),
                spec.pollInterval().toMillis(),
                spec.requestTimeoutMs(),
                spec.maxRpcRetries(),
                spec.maxGasPriceCapWei(),
                spec.maxMessagesPerBundle(),
                new RelayYamlWriter.ContractValidators(
                        side.validatorContract().toHexString(),
                        "0x" + side.arraySlot().toString(16))));
        endpoint.addService(new RelayYamlWriter.Service(
                local.contracts().clprService(),
                local.id(),
                endpoint.signingKeySlot(),
                spec.discoverChannels(),
                List.of(channel.idHex())));
        endpoint.addPeerProofType(peer.caip2(), "QBFTContractMode");
    }
}
