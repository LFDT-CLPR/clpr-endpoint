// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration.channelLifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.time.Duration;
import java.util.EnumSet;
import org.hiero.clpr.relay.integration.OneSidedStubPeerTestBase;
import org.hiero.clpr.relay.integration.TestConditions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * A CLOSING channel must keep being served by the inbound sync responder: when the relay answers the
 * peer's request via {@code createBundle}, a CLOSING (or DRAINED) status is itself something the peer
 * must receive, so the bundle is built with the relay's lifecycle status rather than suppressed to an
 * empty payload the way a settled ACTIVE channel is. (A CLOSED channel conveys no lifecycle and a
 * PENDING one has no on-chain record, so those two do yield nothing.)
 *
 * <p>Drives the channel to CLOSING with a single relay + admin close (no second relay), then sends an
 * authenticated empty-payload request via {@link #poke} and asserts the relay answers with a non-empty
 * status bundle rather than an empty payload. Peer identity is the mTLS roster-CA check on the data
 * plane — there is no payload-level endpoint signature.
 */
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
class ClosingChannelSyncStubPeerTest extends OneSidedStubPeerTestBase {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    void inboundSyncOnClosingChannelIsNotRejected() throws Exception {
        peer.respondEmpty();

        // Send one message so the outbound queue is not fully acked at close time; admin close then
        // transitions ACTIVE → CLOSING (rather than straight to DRAINED — spec §2.1.1).
        interactor.sendMessage(channelId, connectorId, targetApp(), "A->B 0".getBytes());
        interactor.closeChannel(channelId);
        // CLOSING (or DRAINED if it raced past); CLOSED is excluded — the handler correctly rejects CLOSED.
        TestConditions.awaitStatusIn(
                interactor, channelId, EnumSet.of(ClprChannelStatus.CLOSING, ClprChannelStatus.DRAINED), TIMEOUT);

        // Authenticated empty-payload request: nothing is submitted, but the relay must still answer the
        // request with a non-empty reciprocal carrying its CLOSING status.
        final ClprSyncPayload reply = poke(Bytes.EMPTY);

        assertThat(replyBundle(reply).length())
                .as("a CLOSING channel must be served — the relay must answer with a non-empty status bundle")
                .isGreaterThan(0L);
    }
}
