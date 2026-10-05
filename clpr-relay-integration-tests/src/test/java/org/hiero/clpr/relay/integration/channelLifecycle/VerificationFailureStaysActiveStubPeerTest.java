// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration.channelLifecycle;

import static org.hiero.clpr.relay.test.harness.RelayAssertions.assertMetric;

import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.time.Duration;
import java.util.Arrays;
import org.hiero.clpr.relay.integration.OneSidedStubPeerTestBase;
import org.hiero.clpr.relay.integration.TestConditions;
import org.hiero.clpr.relay.test.harness.StubBundles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * A bundle that fails <em>verification</em> (here, a broken running-hash chain) is rejected by the contract
 * and the channel stays {@code ACTIVE} — it does NOT pause. PAUSED is reserved exclusively for response-ordering violations (spec §4.5 "Distinction from bad
 * inbound bundles"); verification failures cause a plain revert with no state change.
 *
 * <p>The relay-observable side: the gas-free preview ({@code eth_call}) rejects the bundle, so the relay
 * skips the paid submission ({@code sync.bundle.skipped} increments with {@code reason=rejected}) rather
 * than landing a reverting transaction; the relay still serves the channel and the on-chain status is
 * unchanged. This is the discriminator the two-relay suite could only assert indirectly.
 */
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
class VerificationFailureStaysActiveStubPeerTest extends OneSidedStubPeerTestBase {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    void badRunningHashRejectedAndStaysActive() throws Exception {
        peer.respondEmpty();

        // A message whose declared running-hash chain is seeded from a wrong base, so the contract's
        // recomputation from the channel's actual received_running_hash (zeros) will not match the
        // bundle's sent_running_hash → ClprRunningHashMismatch revert (Step 4, before any dispatch).
        final byte[] wrongBase = new byte[32];
        Arrays.fill(wrongBase, (byte) 0xff);
        final Bytes badHash = StubBundles.stubProof()
                .ackedMessageId(0)
                .chainFrom(Bytes.wrap(wrongBase))
                .message(connectorId, targetApp(), new byte[20], "x".getBytes())
                .build();

        poke(badHash);

        // The relay's gas-free preview rejected it (eth_call reverted), so it was skipped, not submitted.
        assertMetric(relay.metrics(), "sync", "bundle.skipped")
                .eventually(TIMEOUT)
                .isPositive();
        // Crucially: a verification failure does NOT pause — the channel is still ACTIVE.
        TestConditions.awaitStatus(interactor, channelId, ClprChannelStatus.ACTIVE, Duration.ofSeconds(5));
    }
}
