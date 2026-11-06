// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import static org.hiero.clpr.relay.test.harness.RelayAssertions.assertMetric;

import com.hedera.hapi.node.state.clpr.ClprMessageReplyStatus;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.time.Duration;
import org.hiero.clpr.relay.test.harness.StubBundles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * A byte-identical re-injected bundle is skipped by the
 * {@link org.hiero.clpr.relay.evm.AccountTransactionSubmitter} — observed through the {@code sync.bundle.skipped}
 * counter ({@code reason=rejected}): once the first submission has landed, the re-poked bundle fails the gas-free
 * {@code eth_call} preview (the contract would revert it as stale/replay), so the submitter skips it before it
 * reaches the chain. The identical re-poke isolates dedup-by-preview as the cause.
 */
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
class DedupStubPeerTest extends OneSidedStubPeerTestBase {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    void identicalRepokeIsDeduped() throws Exception {
        peer.respondEmpty();

        // Give the relay an outbound DATA to ack/reply, then craft a bundle that the contract accepts.
        interactor.sendMessage(channelId, connectorId, targetApp(), "A->B 0".getBytes());
        final ContractInteractor.ChannelState state = interactor.requireChannelState(channelId);
        final Bytes bundle = StubBundles.stubProof()
                .ackedMessageId(state.receivedMessageId())
                .receivedMessageId(state.nextMessageId() - 1)
                .reply(1L, ClprMessageReplyStatus.SUCCESS, new byte[] {1})
                .build();

        assertMetric(relay.metrics(), "sync", "bundle.skipped")
                .eventually(TIMEOUT)
                .isZero();

        // First injection submits and advances state; the byte-identical second is dropped by the dedup
        // submitter before it ever reaches the contract.
        poke(bundle);
        poke(bundle);

        assertMetric(relay.metrics(), "sync", "bundle.skipped")
                .eventually(TIMEOUT)
                .isPositive();
    }
}
