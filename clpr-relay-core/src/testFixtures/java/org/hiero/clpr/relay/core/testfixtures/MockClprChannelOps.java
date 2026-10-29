// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core.testfixtures;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.Optional;
import org.hiero.clpr.relay.core.BundleConstructor;
import org.hiero.clpr.relay.core.BundleRequestConstructor;
import org.hiero.clpr.relay.core.BundleWithConstructionMetadata;
import org.hiero.clpr.relay.core.ClprChannelOps;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.InboundBundleQueue;
import org.hiero.clpr.relay.core.PeerEndpointCache;

/**
 * A flexible test double for {@link ClprChannelOps}. Every collaborator has a harmless default (empty
 * state reader, empty bundle, default request, no-op inbound queue); tests override only what they
 * exercise via the fluent setters, e.g.:
 *
 * <pre>{@code
 * var ops = new MockClprChannelOps()
 *         .channelId(id)
 *         .contractStateReader(myReader)
 *         .inboundBundleQueue((bytes, peer) -> submitted.add(bytes));
 * }</pre>
 */
public final class MockClprChannelOps implements ClprChannelOps {

    private Bytes channelId = Bytes.wrap(new byte[32]);
    private BundleConstructor bundleConstructor = (id, request) -> new BundleWithConstructionMetadata(Bytes.EMPTY, 0L);
    private BundleRequestConstructor bundleRequestConstructor = () -> BundleRequest.DEFAULT;
    private InboundBundleQueue inboundBundleQueue = (rawBundleBytes, peer) -> {};
    private ContractStateReader contractStateReader = EMPTY_READER;
    private PeerEndpointCache peerEndpointCache = new PeerEndpointCache();

    /** A {@link ContractStateReader} whose reads all return empty / default. */
    public static final ContractStateReader EMPTY_READER = new ContractStateReader() {
        @Override
        public List<QueuedMessage> readQueuedMessages(
                final Bytes channelId, final long fromId, final long toId, final String blockTag) {
            return List.of();
        }

        @Override
        public ClprLedgerConfiguration readLedgerConfiguration(final CommitmentLevel commitmentLevel) {
            return ClprLedgerConfiguration.DEFAULT;
        }

        @Override
        public Optional<ClprChannel> readChannelState(final Bytes channelId, final String blockTag) {
            return Optional.empty();
        }
    };

    public MockClprChannelOps channelId(final Bytes value) {
        this.channelId = value;
        return this;
    }

    public MockClprChannelOps bundleConstructor(final BundleConstructor value) {
        this.bundleConstructor = value;
        return this;
    }

    public MockClprChannelOps bundleRequestConstructor(final BundleRequestConstructor value) {
        this.bundleRequestConstructor = value;
        return this;
    }

    public MockClprChannelOps inboundBundleQueue(final InboundBundleQueue value) {
        this.inboundBundleQueue = value;
        return this;
    }

    public MockClprChannelOps contractStateReader(final ContractStateReader value) {
        this.contractStateReader = value;
        return this;
    }

    public MockClprChannelOps peerEndpointCache(final PeerEndpointCache value) {
        this.peerEndpointCache = value;
        return this;
    }

    @Override
    public Bytes channelId() {
        return channelId;
    }

    @Override
    public BundleConstructor bundleConstructor() {
        return bundleConstructor;
    }

    @Override
    public BundleRequestConstructor bundleRequestConstructor() {
        return bundleRequestConstructor;
    }

    @Override
    public InboundBundleQueue inboundBundleQueue() {
        return inboundBundleQueue;
    }

    @Override
    public ContractStateReader contractStateReader() {
        return contractStateReader;
    }

    @Override
    public PeerEndpointCache peerEndpointCache() {
        return peerEndpointCache;
    }
}
