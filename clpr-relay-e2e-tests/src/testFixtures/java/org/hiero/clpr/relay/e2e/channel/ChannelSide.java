// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.channel;

import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import java.util.List;
import org.hiero.clpr.relay.e2e.chain.BesuNetwork;
import org.hiero.clpr.relay.e2e.chain.E2eContracts;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpoint;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.integration.ContractInteractor;
import org.jspecify.annotations.Nullable;

/** One end of a channel: the chain, the endpoints serving it, its connector and its live state. */
public final class ChannelSide {

    private final BesuNetwork network;
    private final E2eContracts contracts;
    private final List<EvmEndpoint> endpoints;
    private final byte[] channelId;
    private final String verifierAddress;
    private final String applicationAddress;

    @Nullable
    private Connector connector;

    @Nullable
    private ChannelSide peer;

    ChannelSide(
            final BesuNetwork network,
            final E2eContracts contracts,
            final List<EvmEndpoint> endpoints,
            final byte[] channelId,
            final String verifierAddress,
            final String applicationAddress) {
        this.network = network;
        this.contracts = contracts;
        this.endpoints = List.copyOf(endpoints);
        this.channelId = channelId.clone();
        this.verifierAddress = verifierAddress;
        this.applicationAddress = applicationAddress;
    }

    /**
     * @return the CLPR stack this side of the channel is wired against. Usually the chain's primary
     *     ({@code network.contracts()}), but a channel built with {@link ChannelSpec.Builder#services}
     *     points at a different deployment
     */
    public E2eContracts contracts() {
        return contracts;
    }

    /**
     * @return the {@code ClprService} address this side of the channel is wired against.
     *     Shorthand for {@code contracts().clprService()}
     */
    public String clprServiceAddress() {
        return contracts.clprService();
    }

    void connector(final Connector value) {
        this.connector = value;
    }

    void peer(final ChannelSide value) {
        this.peer = value;
    }

    /**
     * The other end of this channel.
     *
     * <p>Needed constantly: a message sent here is addressed to the <em>peer's</em> application and
     * shows up in the peer's received watermark, so almost every assertion spans both sides.
     *
     * @return the opposite side
     */
    public ChannelSide peer() {
        if (peer == null) {
            throw new IllegalStateException("channel side on " + network.id() + " is not fully wired yet");
        }
        return peer;
    }

    /** @return the chain this side lives on. */
    public BesuNetwork network() {
        return network;
    }

    /**
     * The endpoint driving this side. With a redundant side this is the first of several; use
     * {@link #endpoints()} when the test cares about all of them.
     *
     * @return the primary endpoint
     */
    public EvmEndpoint endpoint() {
        return endpoints.getFirst();
    }

    /** @return every endpoint serving this side. */
    public List<EvmEndpoint> endpoints() {
        return endpoints;
    }

    /** @return this side's per-channel {@code QbftE2EVerifier} wrapper. */
    public String verifierAddress() {
        return verifierAddress;
    }

    /** @return the {@code E2EApplication} inbound messages are dispatched to on this side. */
    public String applicationAddress() {
        return applicationAddress;
    }

    /** @return the registered connector. */
    public Connector connector() {
        if (connector == null) {
            throw new IllegalStateException("no connector registered on " + network.id() + " for channel "
                    + AbiCodec.toHex(channelId) + " yet");
        }
        return connector;
    }

    /** @return the 32-byte channel id. */
    public byte[] channelId() {
        return channelId.clone();
    }

    /**
     * The decoded on-chain channel record, read from this side's CLPR stack (which may be an override
     * service rather than the chain's primary).
     *
     * <p>The interactor is resolved on every call: a node restart rebuilds the network's host JSON-RPC
     * clients, so an interactor captured earlier would keep reading through the closed pre-restart
     * client.
     *
     * @return the decoded on-chain channel record
     */
    public ContractInteractor.ChannelState state() {
        return network.interactorFor(network.deployerAccount(), contracts.core())
                .requireChannelState(channelId);
    }

    /**
     * The channel's lifecycle status on this side, decoded.
     *
     * <p>Returned as the enum rather than the raw ordinal so assertions compare against
     * {@code ClprChannelStatus.ACTIVE} instead of a magic number — or, worse, against the other
     * side's status, which would pass even if both sides were closed.
     *
     * @return the decoded status
     */
    public ClprChannelStatus status() {
        return ClprChannelStatus.fromProtobufOrdinal(state().status());
    }

    /**
     * Messages queued on this side and not yet acknowledged by the peer.
     *
     * <p>{@code nextMessageId} is the id the next send will take, so the highest queued id is one
     * less; subtracting the acknowledged watermark gives the outstanding count.
     *
     * @return the queue depth
     */
    public long queueDepth() {
        final ContractInteractor.ChannelState state = state();
        return Math.max(0L, state.nextMessageId() - 1L - state.ackedMessageId());
    }

    @Override
    public String toString() {
        return "ChannelSide[" + network.id() + " endpoints="
                + endpoints.stream().map(EvmEndpoint::id).toList() + "]";
    }
}
