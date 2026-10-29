// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.test.harness;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.BundleResponse;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * No-Docker loopback checks of the {@link StubPeer} Helidon/PBJ gRPC server and recording. The stub
 * pokes its own server via the real {@code ClprEndpointClient}, exercising the full HTTP/2 + PBJ
 * roundtrip in-process — the same path the endpoint-under-test will use.
 */
class StubPeerTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private final Bytes channelId = Bytes.wrap(new byte[32]);

    @Test
    void recordsInboundAndRespondsEmptyByDefault() throws Exception {
        try (StubPeer peer = StubPeer.start()) {
            final ClprSyncPayload request = ClprSyncPayload.newBuilder()
                    .channelId(channelId)
                    .bundleResponse(BundleResponse.newBuilder()
                            .bundlePayload(Bytes.wrap(new byte[] {1, 2, 3}))
                            .build())
                    .build();

            final ClprSyncPayload reply = peer.pokeSync("127.0.0.1", peer.port(), request);

            // The default responder asks for our data (a bundle_request) and injects no bundle of its own.
            assertThat(reply.hasBundleResponse()).isFalse();
            // A two-phase pokeSync records both the phase-1 request and the phase-2 follow-up.
            assertThat(peer.syncCount()).isEqualTo(2);
            assertThat(peer.received()).hasSize(2);
            assertThat(peer.received().get(0).bundleResponse().bundlePayload())
                    .isEqualTo(Bytes.wrap(new byte[] {1, 2, 3}));
        }
    }

    @Test
    void onSyncReplyDependsOnRequest() throws Exception {
        try (StubPeer peer = StubPeer.start()) {
            final Bytes crafted =
                    StubBundles.stubProof().status(ClprChannelStatus.CLOSING).build();
            peer.onSync(req -> ClprSyncPayload.newBuilder()
                    .channelId(req.channelId())
                    .bundleResponse(
                            BundleResponse.newBuilder().bundlePayload(crafted).build())
                    .build());

            final ClprSyncPayload reply = peer.pokeSync(
                    "127.0.0.1",
                    peer.port(),
                    ClprSyncPayload.newBuilder().channelId(channelId).build());

            assertThat(reply.bundleResponse().bundlePayload()).isEqualTo(crafted);
        }
    }

    @Test
    void awaitSyncReturnsRecordedCapture() throws Exception {
        try (StubPeer peer = StubPeer.start()) {
            peer.pokeSync(
                    "127.0.0.1",
                    peer.port(),
                    ClprSyncPayload.newBuilder()
                            .channelId(channelId)
                            .bundleResponse(BundleResponse.newBuilder()
                                    .bundlePayload(Bytes.wrap(new byte[] {9}))
                                    .build())
                            .build());

            final ClprSyncPayload captured = peer.awaitSync(TIMEOUT);
            assertThat(captured).isNotNull();
            assertThat(captured.bundleResponse().bundlePayload()).isEqualTo(Bytes.wrap(new byte[] {9}));
        }
    }
}
