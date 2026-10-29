// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration.channelLifecycle;

import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprMessageReplyStatus;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.time.Duration;
import org.hiero.clpr.relay.integration.ContractInteractor;
import org.hiero.clpr.relay.integration.OneSidedStubPeerTestBase;
import org.hiero.clpr.relay.integration.TestConditions;
import org.hiero.clpr.relay.test.harness.StubBundles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Peer-initiated close via the <b>inbound</b> path: with <em>no</em> admin close, a single peer bundle
 * that declares {@code status=CLOSING} drives the local channel {@code ACTIVE → CLOSING}, and — because
 * the same bundle acks (and replies to) the outbound DATA — on to {@code DRAINED} in one submission. A
 * second bundle, declaring the peer {@code DRAINED}, then completes the handshake to {@code CLOSED}.
 *
 * <p>Delivered through {@link #poke} (the relay's gRPC server / {@code ClprSyncHandler}), so this
 * exercises the inbound-handler submission path. The {@code …PollStubPeerTest} sibling exercises the same
 * behavior through the relay's own outbound sync loop.
 */
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
class PeerInitiatedCloseInboundStubPeerTest extends OneSidedStubPeerTestBase {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    void peerDeclaringClosingDrivesLocalCloseAndDrain() throws Exception {
        peer.respondEmpty();

        // Outbound DATA, but NO admin close — the peer drives the close.
        interactor.sendMessage(channelId, connectorId, targetApp(), "A->B 0".getBytes());
        final ContractInteractor.ChannelState state = interactor.requireChannelState(channelId);

        // One peer bundle: ack DATA 1 + reply (keeps response ordering valid on the way to DRAINED) +
        // status=CLOSING. The contract flips ACTIVE → CLOSING off the peer's declared status, then drains
        // the channel once the ack covers all outbound.
        poke(StubBundles.stubProof()
                .ackedMessageId(state.receivedMessageId())
                .receivedMessageId(state.nextMessageId() - 1)
                .reply(1L, ClprMessageReplyStatus.SUCCESS, new byte[] {1})
                .status(ClprChannelStatus.CLOSING)
                .build());

        TestConditions.awaitStatus(interactor, channelId, ClprChannelStatus.DRAINED, TIMEOUT);

        // The peer, now also drained, sends a status-only DRAINED bundle. With every outbound already acked
        // and the peer reporting DRAINED, the channel completes the handshake: DRAINED → CLOSED.
        final ContractInteractor.ChannelState drained = interactor.requireChannelState(channelId);
        poke(StubBundles.stubProof()
                .ackedMessageId(drained.receivedMessageId())
                .receivedMessageId(drained.ackedMessageId())
                .chainFrom(Bytes.wrap(drained.receivedRunningHash()))
                .status(ClprChannelStatus.DRAINED)
                .build());

        TestConditions.awaitStatus(interactor, channelId, ClprChannelStatus.CLOSED, TIMEOUT);
    }
}
