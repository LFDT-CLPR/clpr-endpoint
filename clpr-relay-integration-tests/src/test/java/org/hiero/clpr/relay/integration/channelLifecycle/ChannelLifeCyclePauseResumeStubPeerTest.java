// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration.channelLifecycle;

import static org.assertj.core.api.Assertions.assertThat;

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
 * Single-side pause/resume lifecycle (CLC-PSE-1 / CLC-PSE-2): a peer response-ordering violation
 * auto-pauses the channel (ACTIVE→PAUSED); the corrected in-order reply auto-resumes it
 * (PAUSED→ACTIVE). Both transitions are driven deterministically from one {@link StubBundles}-crafted
 * bundle each, poked into a single relay running in {@code POKE_ONLY} mode. The local CLPR Service is the
 * oracle for the transitions.
 *
 * <p>Spec: {@code ClprChannelStatus.PAUSED} — "auto-triggered by response ordering violation … syncs
 * continue … auto-resumes when peer fixes ordering" (clpr-service-spec.md §4.5). The PAUSE is a
 * <em>successful</em> submission (the contract pauses, it does not revert — see {@code BundleLib} Step 7).
 *
 * <p>The crafted-bundle field values below are validated against the live contract by running this test
 * (Docker + {@code RUN_INTEGRATION_TESTS=1}); they encode the §4.2 acceptance rules (replay frontier,
 * running-hash chain, ack) the {@code StubBundles} javadoc describes.
 */
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
class ChannelLifeCyclePauseResumeStubPeerTest extends OneSidedStubPeerTestBase {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    void orderingViolationPausesAndCorrectionResumes() throws Exception {
        peer.respondEmpty();

        // 1. ACTIVE baseline: send a message so the relay's chain holds an outbound DATA awaiting a reply.
        interactor.sendMessage(channelId, connectorId, targetApp(), "A->B 0".getBytes());
        final ChannelState afterSend = interactor.requireChannelState(channelId);
        assertThat(afterSend.nextMessageId()).isEqualTo(2L); // one outbound DATA enqueued (id 1)

        // 2. PAUSE: the peer acks the outbound DATA (received_message_id = 1) but supplies NO reply for it
        //    — a response-ordering violation. The contract accepts the (verifier-valid) bundle and flips
        //    ACTIVE→PAUSED without reverting (the ack is not applied — earlyReturn).
        final Bytes violating = StubBundles.stubProof()
                .ackedMessageId(afterSend.receivedMessageId()) // leaf.acked → replay frontier (= local received)
                .receivedMessageId(afterSend.nextMessageId() - 1) // ack DATA 1
                .build();
        poke(violating);
        TestConditions.awaitStatus(interactor, channelId, ClprChannelStatus.PAUSED, TIMEOUT);

        // 3. RESUME: the peer now supplies the in-order reply for the oldest unreplied DATA (id 1). The
        //    ordering check passes, so the contract auto-resumes PAUSED→ACTIVE and applies the bundle.
        final ChannelState paused = interactor.requireChannelState(channelId);
        final Bytes corrected = StubBundles.stubProof()
                .ackedMessageId(paused.receivedMessageId())
                .receivedMessageId(paused.nextMessageId() - 1)
                .reply(1L, ClprMessageReplyStatus.SUCCESS, new byte[] {1})
                .build();
        poke(corrected);
        TestConditions.awaitStatus(interactor, channelId, ClprChannelStatus.ACTIVE, TIMEOUT);
    }
}
