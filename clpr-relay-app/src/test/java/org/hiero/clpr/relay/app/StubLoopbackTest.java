// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.hapi.node.state.clpr.BundleResponse;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprEndpoint;
import com.hedera.hapi.node.state.clpr.ClprGetLedgerConfigurationRequest;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfigurationResponse;
import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.hapi.node.state.clpr.QbftLedgerConfigurationPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.InboundBundleQueue;
import org.hiero.clpr.relay.core.StubBundleConstructor;
import org.hiero.clpr.relay.core.metrics.SimpleMetrics;
import org.hiero.clpr.relay.core.testfixtures.MockClprChannelOps;
import org.hiero.clpr.relay.grpc.client.ClprEndpointClient;
import org.hiero.clpr.relay.grpc.client.testfixtures.ClprEndpointInfoClient;
import org.hiero.clpr.relay.grpc.server.ClprGrpcServer;
import org.hiero.clpr.relay.grpc.server.ClprSyncHandler;
import org.hiero.clpr.relay.grpc.server.GetLedgerConfigurationHandler;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

/**
 * Verifies the sync and getLedgerConfiguration RPCs end-to-end in a loopback configuration (client talks to
 * a local server).
 */
class StubLoopbackTest {

    /** Minimal stub state reader that always returns ACTIVE for any channel. */
    static class ActiveStateReader implements ContractStateReader {
        @Override
        public Optional<ClprChannel> readChannelState(Bytes channelId, String blockTag) {
            return Optional.of(ClprChannel.newBuilder()
                    .channelId(channelId)
                    .status(ClprChannelStatus.ACTIVE)
                    .nextMessageId(1L)
                    .ackedMessageId(0L)
                    .build());
        }

        @Override
        public List<QueuedMessage> readQueuedMessages(Bytes channelId, long fromId, long toId, String blockTag) {
            return List.of();
        }

        @Override
        public ClprLedgerConfiguration readLedgerConfiguration(CommitmentLevel level) {
            return ClprLedgerConfiguration.DEFAULT;
        }
    }

    /** Records each offered inbound bundle so tests can assert what reached the channel's queue. */
    static class TrackingSubmitter implements InboundBundleQueue {
        final List<Bytes> offered = new ArrayList<>();

        @Override
        public void offer(@NonNull Bytes rawBundleBytes, ClprEndpoint peer) {
            offered.add(rawBundleBytes);
        }
    }

    @Test
    void syncRoundTripWithStubProofs() throws Exception {
        final var channelId = Bytes.wrap(new byte[32]); // all zeros
        final var stateReader = new ActiveStateReader();
        // The stub constructor builds its bundle ad hoc from the peer's request against local state.
        final var proofConstructor = new StubBundleConstructor(stateReader, CommitmentLevel.FINALIZED);

        final var txSubmitter = new TrackingSubmitter();

        // The channel's collaborators are resolved through a ChannelLookup returning this ClprChannelOps;
        // the inbound bundle's parse/throttle/submit lives behind inboundBundleQueue() (here, txSubmitter).
        final var ops = new MockClprChannelOps()
                .channelId(channelId)
                .contractStateReader(stateReader)
                .bundleConstructor(proofConstructor)
                .inboundBundleQueue(txSubmitter);
        final var syncHandler = new ClprSyncHandler(id -> Optional.of(ops), new SimpleMetrics(), "");

        final var ledgerConfigHandler = new GetLedgerConfigurationHandler(
                _ -> _ -> ClprLedgerConfigurationResponse.newBuilder()
                        .qbft(QbftLedgerConfigurationPayload.DEFAULT)
                        .build(),
                CommitmentLevel.FINALIZED);
        final int port;
        try (var s = new java.net.ServerSocket(0)) {
            port = s.getLocalPort();
        }
        final var server = new ClprGrpcServer(port, 1_048_576, syncHandler, ledgerConfigHandler, "");
        server.start();

        try {
            assertThat(server.port()).isEqualTo(port);
            assertThat(server.isRunning()).isTrue();

            // Phase 1: send our bundle_request; the endpoint answers with a stub bundle of its own.
            final var phase1 = ClprSyncPayload.newBuilder()
                    .channelId(channelId)
                    .bundleRequest(BundleRequest.newBuilder()
                            .currentReceivedMessageId(0L)
                            .build())
                    .build();

            // Phase 2: deliver a crafted inbound bundle for the endpoint to submit to the local chain.
            final Bytes inboundBundle = Bytes.wrap(new byte[] {1, 2, 3});

            final var client = new ClprEndpointClient(1_048_576);
            final var response = client.sync("localhost", port, phase1, reply -> ClprSyncPayload.newBuilder()
                    .channelId(channelId)
                    .bundleResponse(BundleResponse.newBuilder()
                            .bundlePayload(inboundBundle)
                            .build())
                    .build());

            // Verify the endpoint's phase-1 reply carries its own bundle.
            assertThat(response).isNotNull();
            assertThat(response.channelId()).isEqualTo(channelId);
            assertThat(response.bundleResponse().bundlePayload().length()).isGreaterThan(0);

            // Verify the phase-2 inbound bundle actually reached the channel's inbound queue verbatim.
            assertThat(txSubmitter.offered).hasSize(1);
            assertThat(txSubmitter.offered.get(0)).isEqualTo(inboundBundle);

            // getLedgerConfiguration is served on the same port as sync.
            try (var infoClient = new ClprEndpointInfoClient()) {
                assertThat(infoClient.getLedgerConfiguration(
                                "localhost", port, ClprGetLedgerConfigurationRequest.DEFAULT))
                        .isEqualTo(QbftLedgerConfigurationPayload.DEFAULT);
            }
        } finally {
            server.stop();
        }
    }
}
