// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.fault;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.ContainerNetwork;
import java.math.BigInteger;
import java.util.function.Supplier;
import org.hiero.clpr.relay.e2e.chain.BesuNetwork;
import org.hiero.clpr.relay.e2e.chain.BesuNode;
import org.hiero.clpr.relay.e2e.chain.ChainTx;
import org.hiero.clpr.relay.e2e.channel.ChannelSide;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpoint;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.jspecify.annotations.Nullable;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;

/**
 * Runtime fault injection.
 *
 * <p>Only faults that can genuinely be introduced <em>while the environment runs</em> live here.
 * Starving a connector is deliberately absent: {@code MockClprConnector} has no way to move ETH back
 * out, so the only honest way to produce an underfunded connector is to register it with a zero
 * budget in the first place — {@code ChannelSpec.connectorFunding(a, b)}. Offering a
 * {@code drainConnector()} that quietly did nothing would be worse than not offering it.
 */
public final class Faults {

    private final Network dockerNetwork;

    /** Supplier so the shim can be installed after Faults is constructed but before the tests run. */
    private final Supplier<@Nullable PeerNetworkShim> shimSupplier;

    public Faults(final Network dockerNetwork, final Supplier<@Nullable PeerNetworkShim> shimSupplier) {
        this.dockerNetwork = dockerNetwork;
        this.shimSupplier = shimSupplier;
    }

    // ── chain faults ──────────────────────────────────────────────────────────

    /**
     * Freeze every node of a chain.
     *
     * <p>{@code docker pause} rather than a stop: the port mapping and in-memory chain survive, so the
     * relay sees RPC timeouts instead of connection refusals. That is what a real node outage looks
     * like from a client, and the two failure modes travel different paths through the relay's retry
     * logic.
     *
     * @param network the chain to freeze
     */
    public void pauseChain(final BesuNetwork network) {
        network.nodes().forEach(BesuNode::pause);
    }

    /**
     * Resume every node of a chain.
     *
     * @param network the chain to resume
     */
    public void unpauseChain(final BesuNetwork network) {
        network.nodes().forEach(BesuNode::unpause);
    }

    /**
     * Freeze a single node.
     *
     * @param node the node to freeze
     */
    public void pauseNode(final BesuNode node) {
        node.pause();
    }

    /**
     * Resume a single node.
     *
     * @param node the node to resume
     */
    public void unpauseNode(final BesuNode node) {
        node.unpause();
    }

    // ── endpoint faults ───────────────────────────────────────────────────────

    /**
     * SIGKILL an endpoint — an abrupt crash with no shutdown hook and no graceful drain.
     *
     * @param endpoint the endpoint to kill
     */
    public void killEndpoint(final EvmEndpoint endpoint) {
        endpoint.kill();
    }

    /**
     * Freeze an endpoint's process, keeping its ports mapped.
     *
     * @param endpoint the endpoint to freeze
     */
    public void pauseEndpoint(final EvmEndpoint endpoint) {
        endpoint.pause();
    }

    /**
     * Resume a frozen endpoint.
     *
     * @param endpoint the endpoint to resume
     */
    public void unpauseEndpoint(final EvmEndpoint endpoint) {
        endpoint.unpause();
    }

    /**
     * Detach an endpoint from the Docker network, making it unreachable to its peer and to the chains.
     *
     * <p>Distinct from pausing: the process keeps running and keeps trying, so this exercises the
     * relay's outbound-failure handling rather than its unresponsiveness.
     *
     * @param endpoint the endpoint to isolate
     */
    public void partition(final EvmEndpoint endpoint) {
        dockerClient()
                .disconnectFromNetworkCmd()
                .withNetworkId(dockerNetwork.getId())
                .withContainerId(endpoint.containerId())
                .exec();
    }

    /**
     * Reattach a partitioned endpoint, restoring its pinned address.
     *
     * <p>The address has to come back unchanged: it is published verbatim in the on-chain peer roster,
     * so an endpoint that reconnected on a different address would stay unreachable to its peer even
     * though the network was "restored".
     *
     * @param endpoint the endpoint to reattach
     */
    public void rejoin(final EvmEndpoint endpoint) {
        dockerClient()
                .connectToNetworkCmd()
                .withNetworkId(dockerNetwork.getId())
                .withContainerId(endpoint.containerId())
                .withContainerNetwork(new ContainerNetwork()
                        .withIpamConfig(new ContainerNetwork.Ipam().withIpv4Address(endpoint.containerIp()))
                        .withAliases(endpoint.id()))
                .exec();
    }

    /**
     * Cut the peer-to-peer sync link between two endpoints, bidirectionally, without touching their
     * chain connectivity.
     *
     * <p>Different from {@link #partition(EvmEndpoint)} in two ways. That one detaches an endpoint
     * from the whole Docker network, so the endpoint also loses its EVM node — two failures at once,
     * hard to attribute. This one only disables the Toxiproxy proxies that front each endpoint's
     * inbound peer port, so chain traffic (JSON-RPC and receipt polls) keeps flowing. It also drops
     * live gRPC streams cleanly, so the sync loop sees a real "connection reset" rather than
     * hanging on TCP keepalive.
     *
     * <p>Requires the environment's peer network shim to be enabled before {@code startAll()} —
     * otherwise there are no proxies to disable. See {@code E2EEnvironment#enablePeerNetworkShim()}.
     *
     * <p>In the current two-endpoint topologies this is exactly A↔B, since disabling one endpoint's
     * proxy blocks every peer's inbound to it. A three-endpoint test that needed to isolate one pair
     * without affecting the third peer would need per-source proxies — not implemented yet.
     *
     * @param a one endpoint
     * @param b the other
     */
    public void disconnectPeers(final EvmEndpoint a, final EvmEndpoint b) {
        final PeerNetworkShim shim = requireShim();
        shim.setEnabled(a, false);
        shim.setEnabled(b, false);
    }

    /**
     * Restore the peer-to-peer sync link between two endpoints previously cut with
     * {@link #disconnectPeers(EvmEndpoint, EvmEndpoint)}.
     *
     * @param a one endpoint
     * @param b the other
     */
    public void reconnectPeers(final EvmEndpoint a, final EvmEndpoint b) {
        final PeerNetworkShim shim = requireShim();
        shim.setEnabled(a, true);
        shim.setEnabled(b, true);
    }

    private PeerNetworkShim requireShim() {
        final PeerNetworkShim shim = shimSupplier.get();
        if (shim == null) {
            throw new IllegalStateException("peer network shim is not enabled; call"
                    + " env.enablePeerNetworkShim() before startAll() to opt this test in");
        }
        return shim;
    }

    /**
     * Sweep an endpoint's signing account, leaving it unable to pay for gas.
     *
     * <p>A gas reserve is left behind on purpose — the sweeping transaction itself costs gas, and a
     * transfer of the entire balance would simply fail. The remainder is far below what a
     * {@code submitBundle} needs.
     *
     * @param endpoint the endpoint whose signer to drain
     * @param network the chain to drain it on
     * @param recipient address to sweep the balance to
     */
    public void drainSigningAccount(final EvmEndpoint endpoint, final BesuNetwork network, final String recipient) {
        final BigInteger balance = network.rpc().ethGetBalance(endpoint.signingAddress(), "latest");
        // Leave a reserve: the sweeping transaction itself costs gas, so moving the entire balance
        // would simply fail. What remains is far below a submitBundle's cost.
        final BigInteger reserve = BigInteger.valueOf(2_000_000_000_000_000L); // 0.002 ETH
        final BigInteger transferable = balance.subtract(reserve);
        if (transferable.signum() <= 0) {
            return; // already drained
        }
        if (transferable.compareTo(ChainTx.MAX_TRANSFERABLE_WEI) > 0) {
            throw new IllegalStateException("cannot drain " + endpoint.id() + " on " + network.id()
                    + ": its balance of " + balance + " wei exceeds what one transaction can move ("
                    + ChainTx.MAX_TRANSFERABLE_WEI + "). Genesis funds accounts below that ceiling"
                    + " precisely so this fault works — check BesuQbftGenesis.DEFAULT_BALANCE_WEI.");
        }
        ChainTx.fund(
                network.rpc(),
                network.submitterFor(endpoint.signerAccount()),
                recipient,
                transferable,
                "drain signing account of " + endpoint.id());
    }

    // ── connector faults ──────────────────────────────────────────────────────

    /**
     * Make a connector's {@code payForExecution} revert, so the service cannot recover its costs.
     *
     * <p>A different path from an underfunded connector even though both end in a slash: here the
     * balance is fine but the charge fails. Injectable at runtime, unlike starvation.
     *
     * @param side the side whose connector to break
     * @param reverts {@code true} to make payment revert, {@code false} to restore it
     */
    public void setConnectorPaymentReverts(final ChannelSide side, final boolean reverts) {
        final BesuNetwork net = side.network();
        ChainTx.sendAndAwait(
                net.rpc(),
                net.rawRpc(),
                net.deployerSubmitter(),
                side.connector().contractAddress(),
                AbiCodec.concat(
                        AbiCodec.functionSelector("setPayReverts(bool)"), AbiCodec.encodeUint(reverts ? 1L : 0L)),
                BigInteger.ZERO,
                "setPayReverts(" + reverts + ") on " + net.id());
    }

    private static DockerClient dockerClient() {
        return DockerClientFactory.instance().client();
    }
}
