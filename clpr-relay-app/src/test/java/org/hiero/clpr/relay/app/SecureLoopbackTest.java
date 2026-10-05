// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.hapi.node.state.clpr.ClprEndpoint;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfigurationResponse;
import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.hapi.node.state.clpr.QbftLedgerConfigurationPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.hiero.clpr.relay.core.Certs;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.LeafKeyManager;
import org.hiero.clpr.relay.core.PeerEndpointCache;
import org.hiero.clpr.relay.core.PeerEndpointTlsRegistry;
import org.hiero.clpr.relay.core.StubBundleConstructor;
import org.hiero.clpr.relay.core.metrics.SimpleMetrics;
import org.hiero.clpr.relay.core.testfixtures.CertFixtures;
import org.hiero.clpr.relay.core.testfixtures.MockClprChannelOps;
import org.hiero.clpr.relay.grpc.client.ClprEndpointClient;
import org.hiero.clpr.relay.grpc.server.ClprGrpcServer;
import org.hiero.clpr.relay.grpc.server.ClprSyncHandler;
import org.hiero.clpr.relay.grpc.server.GetLedgerConfigurationHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end mutual-TLS validation over a real Helidon/gRPC loopback: the secure listener
 * ({@code clientAuth=REQUIRED} + roster CA-chain validation) against a {@link ClprEndpointClient}
 * presenting a leaf generated from its own CA. Exercises the whole handshake the unit tests stop short
 * of — the happy path plus the three rejection modes (client under a non-roster CA, no client cert, wrong
 * server CA).
 *
 * <p>Identity map: the server's CA is {@code CA_A} (published to peers as the server's on-chain cert);
 * the registered client's CA is {@code CA_B} (the only CA in the server's roster).
 */
class SecureLoopbackTest {

    public static final SimpleMetrics METRICS = new SimpleMetrics();
    private ClprGrpcServer server;
    private ClprEndpointClient client;
    private int port;

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    /**
     * Start a secure ({@code clientAuth=REQUIRED}) server presenting a leaf under {@code CA_A}, whose
     * client-cert roster contains only {@code CA_B}. Returns the sync payload a peer should send.
     */
    private ClprSyncPayload startSecureServer(final Path tmp, final Bytes channelId) throws Exception {
        final var stateReader = new StubLoopbackTest.ActiveStateReader();
        final var proofConstructor = new StubBundleConstructor(stateReader, CommitmentLevel.FINALIZED);

        final var ops = new MockClprChannelOps()
                .channelId(channelId)
                .contractStateReader(stateReader)
                .bundleConstructor(proofConstructor)
                .inboundBundleQueue(new StubLoopbackTest.TrackingSubmitter());
        final var syncHandler = new ClprSyncHandler(id -> Optional.of(ops), METRICS, "");
        final var ledgerConfigHandler = new GetLedgerConfigurationHandler(
                _ -> _ -> ClprLedgerConfigurationResponse.newBuilder()
                        .qbft(QbftLedgerConfigurationPayload.DEFAULT)
                        .build(),
                CommitmentLevel.FINALIZED);

        // The TLS registry the server's trust manager reads: only CA_B is registered.
        final var tlsRegistry = new PeerEndpointTlsRegistry();
        final ClprEndpoint peerEndpoint = ClprEndpoint.newBuilder()
                .accountId(Bytes.wrap(new byte[] {0x01}))
                .tlsCertificate(Bytes.wrap(CertFixtures.CA_B_CERT_DER))
                .build();
        tlsRegistry.update(channelId, List.of(peerEndpoint));

        // The same peer must also be in the channel's endpoint manifest: the session rejects a sync from a
        // mTLS-authenticated peer that is not present in the channel's PeerEndpointCache. In production both
        // are populated from the same on-chain manifest.
        final var peerCache = new PeerEndpointCache();
        peerCache.replaceAll(List.of(peerEndpoint));
        ops.peerEndpointCache(peerCache);

        final var serverKeyManager =
                new LeafKeyManager(Certs.parsePrivateKey(CertFixtures.CA_A_KEY_DER), Duration.ZERO, METRICS);

        final int infoPort;
        try (var s1 = new ServerSocket(0);
                var s2 = new ServerSocket(0)) {
            port = s1.getLocalPort();
            infoPort = s2.getLocalPort();
        }
        server = new ClprGrpcServer(
                new ClprGrpcServer.Listeners(port, infoPort, serverKeyManager),
                1_048_576,
                syncHandler,
                tlsRegistry,
                ledgerConfigHandler,
                "");
        server.start();

        // The peer's phase-1 payload: our bundle_request. The endpoint answers it with a stub bundle.
        return ClprSyncPayload.newBuilder()
                .channelId(channelId)
                .bundleRequest(
                        BundleRequest.newBuilder().currentReceivedMessageId(0L).build())
                .build();
    }

    private ClprEndpointClient client(final byte[] caKeyDer) throws Exception {
        // Secure profile: dial mutual TLS. A null caKeyDer yields a keyless client that still attempts the
        // handshake (and is rejected by the clientAuth=REQUIRED server), exercising the no-client-cert path.
        final LeafKeyManager keyManager =
                caKeyDer == null ? null : new LeafKeyManager(Certs.parsePrivateKey(caKeyDer), Duration.ZERO, METRICS);
        return new ClprEndpointClient(1 << 20, keyManager);
    }

    @Test
    void registeredClient_completesMutualTlsSync(@TempDir final Path tmp) throws Exception {
        final var channelId = Bytes.wrap(new byte[32]);
        final var outbound = startSecureServer(tmp, channelId);

        client = client(CertFixtures.CA_B_KEY_DER);
        final var response = client.sync(
                "localhost",
                port,
                outbound,
                reply -> ClprSyncPayload.newBuilder().channelId(channelId).build(),
                Bytes.wrap(CertFixtures.CA_A_CERT_DER));

        assertThat(response).isNotNull();
        assertThat(response.channelId()).isEqualTo(channelId);
        assertThat(response.bundleResponse().bundlePayload().length()).isGreaterThan(0);
    }

    @Test
    void clientUnderNonRosterCa_rejectedAtHandshake(@TempDir final Path tmp) throws Exception {
        final var channelId = Bytes.wrap(new byte[32]);
        final var outbound = startSecureServer(tmp, channelId);

        // A valid CA/leaf, but CA_A is not in the server's roster (only CA_B is).
        client = client(CertFixtures.CA_A_KEY_DER);
        assertThatThrownBy(() -> client.sync(
                        "localhost",
                        port,
                        outbound,
                        reply -> ClprSyncPayload.newBuilder()
                                .channelId(channelId)
                                .build(),
                        Bytes.wrap(CertFixtures.CA_A_CERT_DER)))
                .isInstanceOf(Exception.class);
    }

    @Test
    void noClientCert_rejectedAtHandshake(@TempDir final Path tmp) throws Exception {
        final var channelId = Bytes.wrap(new byte[32]);
        final var outbound = startSecureServer(tmp, channelId);

        client = client(null); // presents no client certificate
        assertThatThrownBy(() -> client.sync(
                        "localhost",
                        port,
                        outbound,
                        reply -> ClprSyncPayload.newBuilder()
                                .channelId(channelId)
                                .build(),
                        Bytes.wrap(CertFixtures.CA_A_CERT_DER)))
                .isInstanceOf(Exception.class);
    }

    @Test
    void wrongServerCa_rejectedByClient(@TempDir final Path tmp) throws Exception {
        final var channelId = Bytes.wrap(new byte[32]);
        final var outbound = startSecureServer(tmp, channelId);

        client = client(CertFixtures.CA_B_KEY_DER);
        // Expect the server under CA_B, but it presents a leaf under CA_A — the client rejects the chain.
        assertThatThrownBy(() -> client.sync(
                        "localhost",
                        port,
                        outbound,
                        reply -> ClprSyncPayload.newBuilder()
                                .channelId(channelId)
                                .build(),
                        Bytes.wrap(CertFixtures.CA_B_CERT_DER)))
                .isInstanceOf(Exception.class);
    }
}
