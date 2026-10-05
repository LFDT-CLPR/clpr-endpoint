// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration.channelLifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.time.Duration;
import java.util.EnumSet;
import org.hiero.clpr.relay.core.ParsedBundle;
import org.hiero.clpr.relay.evm.qbft.QbftProofCodec;
import org.hiero.clpr.relay.integration.ContractInteractor.ChannelState;
import org.hiero.clpr.relay.integration.OneSidedStubPeerTestBase;
import org.hiero.clpr.relay.integration.TestConditions;
import org.hiero.clpr.relay.test.harness.StubBundles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * When a peer initiates closing on a channel that has carried no data, the relay must still reciprocate
 * with a signed, verifiable bundle that reflects its own status change — otherwise the peer never learns
 * the relay acknowledged the close and the handshake cannot complete on the peer's side.
 *
 * <p>The inbound close itself now lands — the guarded submitter treats a declared status differing from the
 * local status as progress, so the peer's status-only close drives the relay through
 * {@code ACTIVE → CLOSING → DRAINED} (the channel rides straight to {@code DRAINED} because nothing was
 * ever queued outbound). The remaining gap is the <em>reciprocal</em>: the relay can act on the peer's
 * status but cannot send its own back.
 *
 * <p>The relay must build a reciprocal proof for a bare status change even when nothing else rides along —
 * no pending outbound, no trust-anchor advance, no inbound ack. A closing/draining status is itself
 * something the peer must receive, so the bundle carries the relay's status back to the peer rather than
 * short-circuiting to an empty payload (an empty payload is not a verifiable proof and the peer would
 * discard it, leaving the close unable to complete on the peer's side).
 *
 * <p>This uses a never-used (zero received frontier) channel by necessity — any message the relay
 * receives advances the frontier past zero and takes the ordinary message/ack path, so the bare-status
 * path is only reachable here. The test drives a status-only peer close, waits for the relay to wind down,
 * asserts the reply carries a non-empty bundle reporting the relay's own closing status, then has the peer
 * declare itself {@code CLOSED} to complete the handshake to {@code CLOSED}. The terminal bundle declares
 * {@code CLOSED} rather than {@code DRAINED} because, on a channel whose counters never moved, a
 * peer-{@code DRAINED} bundle would be byte-identical to the relay's own initial state and suppressed.
 */
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
class StatusOnlyCloseAckReciprocatedStubPeerTest extends OneSidedStubPeerTestBase {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    void statusOnlyPeerCloseIsReciprocatedWithASignedStatusBundle() throws Exception {
        peer.respondEmpty();

        // No message has ever been exchanged: the relay's received frontier stays at zero, so the only thing
        // it could ever send the peer is its own status. Peer initiates closing; with nothing queued the
        // relay winds straight down: ACTIVE → CLOSING → DRAINED.
        final ChannelState active = interactor.requireChannelState(channelId);
        final Bytes peerClosing = StubBundles.stubProof()
                .ackedMessageId(active.receivedMessageId())
                .receivedMessageId(active.ackedMessageId())
                .chainFrom(Bytes.wrap(active.receivedRunningHash()))
                .status(ClprChannelStatus.CLOSING)
                .build();
        poke(peerClosing);
        TestConditions.awaitStatusIn(
                interactor, channelId, EnumSet.of(ClprChannelStatus.CLOSING, ClprChannelStatus.DRAINED), TIMEOUT);

        // The relay has wound down. Its next reply to the peer must carry that status so the peer can move
        // its own side forward — a non-empty bundle that decodes to a closing status, not an empty payload.
        final ClprSyncPayload reply = awaitNonEmptyReply(TIMEOUT);

        assertThat(replyBundle(reply).length())
                .as("the relay must reciprocate a status-only close with a non-empty bundle, not an empty payload")
                .isGreaterThan(0L);
        final ParsedBundle reciprocal = new QbftProofCodec(Bytes.wrap(channelId)).decodeBundle(replyBundle(reply));
        assertThat(reciprocal.metadata().status())
                .as("the reciprocated bundle must report the relay's own closing status to the peer")
                .isIn(ClprChannelStatus.CLOSING, ClprChannelStatus.DRAINED);

        // The peer now declares itself CLOSED. The relay, having wound down to DRAINED, completes the
        // handshake: DRAINED → CLOSED.
        final ChannelState drained = interactor.requireChannelState(channelId);
        final Bytes peerClosed = StubBundles.stubProof()
                .ackedMessageId(drained.receivedMessageId())
                .receivedMessageId(drained.ackedMessageId())
                .chainFrom(Bytes.wrap(drained.receivedRunningHash()))
                .status(ClprChannelStatus.CLOSED)
                .build();
        poke(peerClosed);
        TestConditions.awaitStatus(interactor, channelId, ClprChannelStatus.CLOSED, TIMEOUT);
    }

    /**
     * Ping the relay with an authenticated empty-payload request until its reply carries a non-empty
     * payload, or the timeout elapses. In the two-phase protocol the relay builds its reciprocal
     * synchronously from the request and its current on-chain channel state — there is no background
     * poll and no cached bundle — so a single ping after the transition is confirmed normally suffices;
     * the loop only absorbs the brief window where the relay's commitment-level read has not yet caught
     * up to the just-confirmed transition.
     */
    private static ClprSyncPayload awaitNonEmptyReply(final Duration timeout) throws Exception {
        final long deadline = System.currentTimeMillis() + timeout.toMillis();
        ClprSyncPayload last = poke(Bytes.EMPTY);
        while (replyBundle(last).length() == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(500);
            last = poke(Bytes.EMPTY);
        }
        return last;
    }
}
