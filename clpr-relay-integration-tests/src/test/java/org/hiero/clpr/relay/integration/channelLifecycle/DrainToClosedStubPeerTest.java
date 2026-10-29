// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration.channelLifecycle;

import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprMessageReplyStatus;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.time.Duration;
import org.hiero.clpr.relay.integration.ContractInteractor.ChannelState;
import org.hiero.clpr.relay.integration.OneSidedStubPeerTestBase;
import org.hiero.clpr.relay.integration.TestConditions;
import org.hiero.clpr.relay.test.harness.StubBundles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Drive a closing channel all the way to {@code CLOSED}, including the terminal
 * {@code DRAINED → CLOSED} hop via a status-only bundle — an empty-payload bundle that carries only a peer
 * status change. This single-side test drives each step deterministically with one poke, replacing a pair
 * of two-relay convergence tests that needed a synchronized drain race between two live relays.
 *
 * <p>The final hop relies on the contract accepting an otherwise-no-progress bundle when it carries a
 * state transition: a peer announcing {@code DRAINED} while this side is already {@code DRAINED} drives it
 * to {@code CLOSED}.
 */
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
class DrainToClosedStubPeerTest extends OneSidedStubPeerTestBase {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    void closingDrainsToClosedViaStatusOnlyBundle() throws Exception {
        peer.respondEmpty();

        // 1. One outbound DATA, then admin-close: with the queue not yet acked, ACTIVE → CLOSING.
        interactor.sendMessage(channelId, connectorId, targetApp(), "A->B 0".getBytes());
        interactor.closeChannel(channelId);
        TestConditions.awaitStatus(interactor, channelId, ClprChannelStatus.CLOSING, TIMEOUT);

        // 2. Peer acks + replies to DATA 1 in order → the outbound queue is fully acked → CLOSING → DRAINED.
        final ChannelState closing = interactor.requireChannelState(channelId);
        final Bytes drain = StubBundles.stubProof()
                .ackedMessageId(closing.receivedMessageId())
                .receivedMessageId(closing.nextMessageId() - 1)
                .reply(1L, ClprMessageReplyStatus.SUCCESS, new byte[] {1})
                .build();
        poke(drain);
        TestConditions.awaitStatus(interactor, channelId, ClprChannelStatus.DRAINED, TIMEOUT);

        // 3. Peer (also drained) sends a status-only bundle declaring DRAINED → DRAINED → CLOSED.
        //    Empty payload, no ack progress — accepted only because it carries a state transition.
        final ChannelState drained = interactor.requireChannelState(channelId);
        final Bytes closeNotification = StubBundles.stubProof()
                .ackedMessageId(drained.receivedMessageId())
                .receivedMessageId(drained.ackedMessageId())
                .chainFrom(Bytes.wrap(drained.receivedRunningHash()))
                .status(ClprChannelStatus.DRAINED)
                .build();
        poke(closeNotification);
        TestConditions.awaitStatus(interactor, channelId, ClprChannelStatus.CLOSED, TIMEOUT);
    }
}
