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
 * A channel must not reach {@code CLOSED} until <em>both</em> conditions hold at once: every outbound
 * message has been acked by the peer, <em>and</em> the peer has announced it is {@code DRAINED} on its own
 * side. {@link DrainToClosedStubPeerTest} proves the positive hop (both conditions satisfied →
 * {@code CLOSED}); this test isolates each condition and proves that, on its own, neither is enough.
 *
 * <p>One narrative drives the single shared channel through the two gates in sequence:
 *
 * <ol>
 *   <li><b>Peer drained, queue not fully acked.</b> The peer declares {@code DRAINED} but acks only the
 *       first of two outbound messages. The channel must stay {@code CLOSING} — a peer announcement
 *       cannot pull it forward while local messages are still unacked.</li>
 *   <li><b>Queue fully acked, peer not drained.</b> The peer acks the remaining message but is still
 *       {@code CLOSING} (not yet drained). The channel advances to {@code DRAINED} and stops there — it
 *       must not slip to {@code CLOSED} while the peer has not finished draining.</li>
 *   <li><b>Both satisfied.</b> The peer announces {@code DRAINED}; with all outbound already acked, the
 *       channel finally reaches {@code CLOSED}.</li>
 * </ol>
 *
 * <p>Runs in {@code POKE_ONLY} mode, so the only driver of any transition is an explicit poke: each gate's
 * outcome is unambiguously attributable to the bundle that was injected.
 */
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
class ClosedRequiresAckedAndPeerDrainedStubPeerTest extends OneSidedStubPeerTestBase {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final Duration STABLE_WINDOW = Duration.ofSeconds(4);

    @Test
    void closedRequiresBothFullAckAndPeerDrained() throws Exception {
        peer.respondEmpty();

        // Two outbound DATA, then admin-close: with the queue unacked, ACTIVE → CLOSING.
        interactor.sendMessage(channelId, connectorId, targetApp(), "A->B 0".getBytes());
        interactor.sendMessage(channelId, connectorId, targetApp(), "A->B 1".getBytes());
        interactor.closeChannel(channelId);
        TestConditions.awaitStatus(interactor, channelId, ClprChannelStatus.CLOSING, TIMEOUT);

        // ── Gate A: peer DRAINED but our queue is only partially acked → must stay CLOSING ──
        // The peer acks message 1 only (not message 2) and announces DRAINED. The ack lands, but because
        // the outbound queue is not fully acked the announcement cannot move us past CLOSING.
        final ChannelState beforeA = interactor.requireChannelState(channelId);
        final Bytes partialAckDrained = StubBundles.stubProof()
                .ackedMessageId(beforeA.receivedMessageId())
                .receivedMessageId(1L)
                .reply(1L, ClprMessageReplyStatus.SUCCESS, new byte[] {1})
                .status(ClprChannelStatus.DRAINED)
                .build();
        poke(partialAckDrained);
        // The bundle is processed (our acked frontier advances to 1) yet the status holds at CLOSING.
        TestConditions.awaitCondition(
                TIMEOUT, () -> interactor.requireChannelState(channelId).ackedMessageId() == 1L);
        TestConditions.assertStatusStable(interactor, channelId, ClprChannelStatus.CLOSING, STABLE_WINDOW);

        // ── Gate B: queue fully acked but peer still CLOSING (not drained) → DRAINED, never CLOSED ──
        // The peer acks the final message and replies in order, but reports itself as still CLOSING. We
        // wind down to DRAINED and must stop there: a fully-drained local side still needs the peer drained.
        final ChannelState beforeB = interactor.requireChannelState(channelId);
        final Bytes fullAckNotDrained = StubBundles.stubProof()
                .ackedMessageId(beforeB.receivedMessageId())
                .receivedMessageId(beforeB.nextMessageId() - 1)
                .chainFrom(Bytes.wrap(beforeB.receivedRunningHash()))
                .reply(2L, ClprMessageReplyStatus.SUCCESS, new byte[] {2})
                .status(ClprChannelStatus.CLOSING)
                .build();
        poke(fullAckNotDrained);
        TestConditions.awaitStatus(interactor, channelId, ClprChannelStatus.DRAINED, TIMEOUT);
        TestConditions.assertStatusStable(interactor, channelId, ClprChannelStatus.DRAINED, STABLE_WINDOW);

        // ── Both satisfied: peer announces DRAINED with all outbound acked → DRAINED → CLOSED ──
        final ChannelState drained = interactor.requireChannelState(channelId);
        final Bytes peerDrained = StubBundles.stubProof()
                .ackedMessageId(drained.receivedMessageId())
                .receivedMessageId(drained.ackedMessageId())
                .chainFrom(Bytes.wrap(drained.receivedRunningHash()))
                .status(ClprChannelStatus.DRAINED)
                .build();
        poke(peerDrained);
        TestConditions.awaitStatus(interactor, channelId, ClprChannelStatus.CLOSED, TIMEOUT);
    }
}
