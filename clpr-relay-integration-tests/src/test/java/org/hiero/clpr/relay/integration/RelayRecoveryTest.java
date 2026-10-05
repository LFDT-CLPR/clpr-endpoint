// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.hiero.clpr.relay.core.ParsedBundle;
import org.hiero.clpr.relay.evm.qbft.QbftProofCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Verify that the relay can be stopped, a message submitted on-chain in its absence, and that after
 * restart the relay re-detects that message and delivers it to the peer — checking not just that a sync
 * arrives, but that the delivered bundle actually carries the message queued during downtime.
 */
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
class RelayRecoveryTest extends OneSidedStubPeerTestBase {

    private static final Duration RECOVERY_TIMEOUT = Duration.ofSeconds(120);

    @Test
    void messageDeliveredAfterRelayRestart() {
        peer.respondEmpty();
        relay.stop();

        // Submit a message on the chain while the relay is down — it must re-detect it on restart.
        final byte[] msgData = "DELAYED".getBytes(StandardCharsets.UTF_8);
        interactor.sendMessage(channelId, connectorId, targetApp(), msgData);

        restartFullDuplex();

        // Recovery: the restarted relay reads the chain, finds the missed message, and syncs it out.
        final ClprSyncPayload delivered = peer.awaitSync(p -> replyBundle(p).length() > 0, RECOVERY_TIMEOUT);
        assertThat(delivered)
                .as("relay must deliver a bundle to the peer after restart")
                .isNotNull();

        // Decode the delivered bundle (QbftProofHandler extracts the content — no crypto needed) and confirm
        // it carries the exact message that was enqueued while the relay was down.
        final ParsedBundle bundle = new QbftProofCodec(Bytes.wrap(channelId)).decodeBundle(replyBundle(delivered));
        assertThat(bundle.messages())
                .as("the delivered bundle must carry the message enqueued during downtime")
                .anyMatch(p -> p.hasMessage() && p.message().messageData().equals(Bytes.wrap(msgData)));
    }
}
