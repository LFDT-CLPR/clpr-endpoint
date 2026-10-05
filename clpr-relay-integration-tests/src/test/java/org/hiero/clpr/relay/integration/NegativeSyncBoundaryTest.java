// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Negative-boundary test for the two-phase {@code sync} endpoint: a phase-1 {@code bundle_request} for a
 * channel id with no on-chain record must produce an empty phase-1 {@code bundle_response}. The state
 * reader returns empty for the phantom id, so {@code createBundle} short-circuits with an empty payload
 * rather than building a bundle or faulting.
 *
 * <p>Note: the two-phase protocol authenticates peers at the mTLS layer (roster-CA client-cert
 * validation), not with a payload-level endpoint signature, so there is no per-payload signature-mismatch
 * rejection to exercise here — that boundary moved to the transport and is covered by the TLS loopback
 * tests.
 */
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
class NegativeSyncBoundaryTest extends OneSidedStubPeerTestBase {

    @Test
    void phantomConnectionIdReturnsEmptyProof() throws Exception {
        // A channel id with no on-chain record. The state reader returns empty and createBundle
        // short-circuits with an empty phase-1 bundle_response.
        final byte[] phantomId = new byte[32];
        Arrays.fill(phantomId, (byte) 0x42);

        assertEmptyProof(sendSync(Bytes.wrap(phantomId)));
    }

    private static ClprSyncPayload sendSync(final Bytes channelId) throws Exception {
        return peer.pokeSync(
                RELAY_HOST,
                relay.grpcPort(),
                ClprSyncPayload.newBuilder()
                        .channelId(channelId)
                        .bundleRequest(BundleRequest.newBuilder()
                                .currentReceivedMessageId(0L)
                                .build())
                        .build());
    }

    private static void assertEmptyProof(final ClprSyncPayload response) {
        assertThat(replyBundle(response).length())
                .as("a request for an absent channel must return an empty bundle")
                .isZero();
    }
}
