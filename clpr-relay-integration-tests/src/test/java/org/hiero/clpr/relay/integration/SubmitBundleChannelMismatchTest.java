// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Isolation between two channels sharing a single {@code ClprService}: a bundle whose proof is
 * consistent with channel1's on-chain state does <em>not</em> land under channel2's id — the contract
 * rejects it and both channels' state records are unaffected.
 *
 * <p>This is the assertion the multi-channel E2E drainage tests cannot make directly. Those prove
 * both channels drain, but their bundles were correctly labeled by the relay so a cross-attributed
 * submission never gets built. Here we bypass the relay and hand-forge the cross submission via
 * {@link ContractInteractor#submitBundle(byte[], byte[])}, which lets the contract's own §4.2
 * verification be the oracle.
 *
 * <p><b>Why the contract rejects the mismatched submission.</b> The bundle carries a leaf whose
 * {@code acked_message_id} and {@code sent_running_hash} were chained from channel1's actual
 * {@code receivedMessageId} and {@code receivedRunningHash}. The contract, however, verifies the
 * bundle against channel2's on-chain state — where {@code receivedMessageId=0} and
 * {@code receivedRunningHash=zeros}. The mismatch fails Step 3 (replay defense: first message id
 * must equal channel2's {@code receivedMessageId + 1}) OR Step 4 (running-hash chain: the bundle's
 * {@code sent_running_hash} must equal SHA-256 chain over the messages seeded from channel2's
 * {@code receivedRunningHash}). Either revert proves the same invariant: proof verification is
 * scoped to the channel identified by the {@code channelId} argument on submitBundle, not to any
 * shared service-level state.
 *
 * <p>Test isolation from the base's relay: neither the relay nor the {@link
 * org.hiero.clpr.relay.test.harness.StubPeer} participates. Everything is driven through the
 * interactor as the admitted dev endpoint.
 */
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
class SubmitBundleChannelMismatchTest extends OneSidedStubPeerTestBase {

    /** Salt for the second channel; must differ from the base's all-zero salt to yield a distinct id. */
    private static final byte[] CHANNEL2_SALT = channel2Salt();

    /** The second channel, registered on the same {@code ClprService} the base already deployed. */
    private static byte[] channelId2;

    @BeforeAll
    static void registerSecondChannel() {
        channelId2 = interactor.registerActiveChannel("eip155:1337", CHANNEL2_SALT, contracts.stubVerifierAddress());
        // No connector for channel2 on purpose: the cross-submit must revert before dispatch anyway,
        // and adding a second connector registration would only add setup that could mask a bug in
        // that path with unrelated failures.
        assertThat(channelId2).as("channel2 id must be a fresh 32 bytes").hasSize(32);
        assertThat(channelId2)
                .as("channel1 and channel2 must derive distinct on-chain ids")
                .isNotEqualTo(channelId);
    }

    @Test
    void bundleValidForChannel1RevertsWhenSubmittedUnderChannel2Id() {
        // ── Step 1: advance channel1 to a non-initial state via a legitimate direct submission. ──
        //
        // Bypassing poke() on purpose: the base's relay is only configured to serve channel1, and
        // this test does not exercise the relay path at all. Direct interactor submission suffices
        // to move channel1's on-chain state, and keeps the test's causal chain short — every state
        // change here is either an explicit submitBundle from this test or a rejected one.
        //
        // The channel is wired to the StubClprVerifier so the payload must be a CLPRSTUB-prefixed
        // ClprBundleContent, not a Hiero StateProof — the latter is only accepted via the relay's
        // HieroProofHandler re-encode path, which we bypass here.
        final ContractInteractor.ChannelState channel1Initial = interactor.requireChannelState(channelId);
        final byte[] advanceChannel1 = interactor.craftDataBundleBytes(
                channel1Initial, connectorId, targetApp(), new byte[20], "channel1-msg-1".getBytes());
        interactor.submitBundle(channelId, advanceChannel1);

        final ContractInteractor.ChannelState channel1AfterAdvance = interactor.requireChannelState(channelId);
        assertThat(channel1AfterAdvance.receivedMessageId())
                .as("channel1 must have accepted the first inbound message")
                .isOne();
        assertThat(channel1AfterAdvance.receivedRunningHash())
                .as("channel1's receivedRunningHash must have advanced past zeros")
                .isNotEqualTo(new byte[32]);

        final ContractInteractor.ChannelState channel2Initial = interactor.requireChannelState(channelId2);
        assertThat(channel2Initial.receivedMessageId())
                .as("channel2 must not have advanced while channel1 was being driven")
                .isZero();

        // ── Step 2: craft a bundle valid ONLY for channel1's now-advanced state. ──
        //
        // Using channel1's post-advance state as the local frontier means this proof would
        // legitimately extend channel1 to receivedMessageId=2. For channel2 (receivedMessageId=0,
        // receivedRunningHash=zeros) it is invalid on both counts — either replay defense or the
        // running-hash chain will trip on Step 3 / Step 4 of §4.2.
        final byte[] proofForChannel1AtAdvancedState = interactor.craftDataBundleBytes(
                channel1AfterAdvance, connectorId, targetApp(), new byte[20], "channel1-msg-2".getBytes());

        // ── Step 3: the cross-submission MUST revert. ──
        //
        // interactor.submitBundle throws IllegalStateException on any non-1 receipt status, embedding
        // the eth_call replay reason. That is the closest we get to a typed revert without pulling in
        // the contract's error selectors here; the invariant is "revert of any flavor", not a
        // specific error code.
        // Pin failure to the receipt-status-0 branch of ContractInteractor.waitForSuccessfulReceipt
        // ("Transaction <hash> failed, status=<hex>, revert: <reason>"), not a receipt-timeout or
        // JSON-RPC transport error path — either of those would satisfy a bare "revert" substring
        // without proving the contract actually rejected the cross-channel submission. The
        // doesNotContain("CLPRSTUB") guard locks in the intended failure mode: this test must not
        // pass because the stub verifier tripped on the payload prefix (which would mean
        // craftDataBundleBytes drifted from the verifier), it must pass because the contract's §4.2
        // replay/running-hash checks caught the wrong-channel proof.
        assertThatThrownBy(() -> interactor.submitBundle(channelId2, proofForChannel1AtAdvancedState))
                .as("submitBundle(channel2, channel1's proof) must revert")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("failed, status=")
                .hasMessageContaining("revert:")
                .hasMessageNotContaining("CLPRSTUB");

        // ── Step 4: neither channel's state changed as a result of the failed cross-submission. ──
        //
        // A reverted transaction is mined and paid for, so absence of state change is the only
        // signal that separates "the contract rejected the proof cleanly" from "the contract
        // silently absorbed it and mutated something anyway".
        final ContractInteractor.ChannelState channel1Final = interactor.requireChannelState(channelId);
        assertThat(channel1Final.receivedMessageId())
                .as("channel1's receivedMessageId must be unchanged by a failed submit against channel2")
                .isEqualTo(channel1AfterAdvance.receivedMessageId());
        assertThat(channel1Final.receivedRunningHash())
                .as("channel1's receivedRunningHash must be unchanged")
                .isEqualTo(channel1AfterAdvance.receivedRunningHash());

        final ContractInteractor.ChannelState channel2Final = interactor.requireChannelState(channelId2);
        assertThat(channel2Final.receivedMessageId())
                .as("channel2's receivedMessageId must still be zero — the rejected proof did not advance it")
                .isZero();
        assertThat(channel2Final.receivedRunningHash())
                .as("channel2's receivedRunningHash must still be zeros")
                .isEqualTo(new byte[32]);

        // Sanity: getChannel routes strictly by argument id — reading channel1 does not return
        // channel2's fields and vice versa. Trivially true if storage is keyed by channelId, but
        // asserting the pair explicitly costs nothing and locks it in.
        assertThat(channel1Final.receivedMessageId())
                .as("channel1 and channel2 read distinct receivedMessageId values")
                .isNotEqualTo(channel2Final.receivedMessageId());
    }

    /** A 32-byte salt with byte 0 = 0x02 — distinct from the base's all-zero salt so channel id differs. */
    private static byte[] channel2Salt() {
        final byte[] salt = new byte[32];
        salt[0] = (byte) 0x02;
        return salt;
    }
}
