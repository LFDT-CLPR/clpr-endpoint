// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.swirlds.metrics.api.Metrics;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.hiero.clpr.relay.app.ProofType;
import org.hiero.clpr.relay.app.RelayInstance;
import org.hiero.clpr.relay.core.Certs;
import org.hiero.clpr.relay.core.LeafKeyManager;
import org.hiero.clpr.relay.core.metrics.SimpleMetrics;
import org.hiero.clpr.relay.core.testfixtures.CertFixtures;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.EthSigner;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.testfixtures.TestEvmJsonRpcClient;
import org.hiero.clpr.relay.test.harness.StubPeer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Triple;

/**
 * Verifies relay-level leaf-certificate scenarios over the full Anvil-backed <b>mutual TLS</b> path. For
 * direct TLS-handshake rejection tests that require no relay or Anvil, see {@link ClientTlsDialTest}.
 *
 * <p>Self-contained (does not extend {@link OneSidedStubPeerTestBase}) because that base runs the relay
 * over <em>plaintext</em> two-phase sync so its {@link OneSidedStubPeerTestBase#poke} can inject a crafted
 * {@code bundle_response} — a combination the {@link StubPeer} harness cannot express over mutual TLS.
 * This test instead stands up a mandatory-mTLS relay against a TLS-enabled {@link StubPeer} and exercises
 * the relay's <em>outbound</em> dial:
 *
 * <ul>
 *   <li>The relay's leaf is minted by CA-A (whose private key is written to {@link #tempDir} and passed to
 *       {@link RelayTestSupport#secureRelayConfig}).</li>
 *   <li>The stub peer presents a leaf signed by CA-B and requires connecting clients to chain to CA-A; its
 *       CA-B DER is published in the on-chain roster entry (via {@link ContractInteractor#configureVerifierConfig}),
 *       so the relay dials it over mutual TLS.</li>
 * </ul>
 */
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
@Testcontainers
class LeafRevocationTest {

    private static final String RELAY_HOST = "127.0.0.1";
    private static final BigInteger CONNECTOR_STAKE = BigInteger.valueOf(100_000_000_000_000_000L);

    @Container
    static final AnvilContainer ANVIL = new AnvilContainer();

    /** The relay-under-test's signing key (account 2), distinct from the harness's account 0. */
    private static final String RELAY_PRIVATE_KEY = AnvilContainer.DEV_PRIVATE_KEY_3;

    /** 64-byte uncompressed secp256k1 public key for the harness's admin/connector account (account 0). */
    private static final byte[] DEV_PUB_KEY_64 = EthSigner.derivePublicKey(AnvilContainer.DEV_PRIVATE_KEY);

    /** Temporary directory holding the relay's CA private-key file. Injected by JUnit before {@code @BeforeAll}. */
    @TempDir
    static Path tempDir;

    private static final Metrics metrics = new SimpleMetrics();

    static EvmJsonRpcClient rpc;
    static AnvilTxSubmitter txSubmitter;
    static DeployedContracts contracts;
    static ContractInteractor interactor;
    static byte[] channelId;
    static byte[] connectorId;
    static RelayInstance relay;
    static StubPeer peer;
    static int relayPort;

    /** Path to the relay's CA-A private key on disk; reused by {@link #restartFullDuplex}. */
    static Path relayKeyPath;

    @BeforeAll
    static void setupSecure() throws Exception {
        // Write the relay's CA-A private key to disk so secureRelayConfig can load it.
        relayKeyPath = tempDir.resolve("relay-ca-a.der");
        Files.write(relayKeyPath, CertFixtures.CA_A_KEY_DER);

        rpc = new TestEvmJsonRpcClient(ANVIL.jsonRpcUrl());
        final var devSigner = new EthSigner(AnvilContainer.DEV_PRIVATE_KEY);
        txSubmitter = new AnvilTxSubmitter(rpc, devSigner, AnvilContainer.CHAIN_ID);
        contracts = new ContractDeployer(rpc, txSubmitter).deploy();
        interactor = new ContractInteractor(rpc, txSubmitter, devSigner, DEV_PUB_KEY_64, contracts);

        // The stub peer presents a TLS leaf signed by CA-B and requires connecting clients to present
        // a leaf that chains to CA-A (the relay's CA). Duration.ZERO disables rotation so the leaf is
        // valid for 10 years — no rotation churn during a short test run.
        peer = StubPeer.start(
                new LeafKeyManager(Certs.parsePrivateKey(CertFixtures.CA_B_KEY_DER), Duration.ZERO, metrics),
                CertFixtures.CA_A_CERT_DER);

        relayPort = RelayTestSupport.findFreePort();
        final byte[] serviceAddr = AbiCodec.fromHex(contracts.clprServiceAddress());

        // The service deploys default-disabled; initialize + setClprEnabled(true) via bootstrapDefaults
        // before any mutating op, otherwise the calls below revert with ClprDisabled().
        interactor.bootstrapDefaults(serviceAddr);

        // Publish CA-B's DER in the stub peer's on-chain roster entry. The relay reads that cert and
        // uses it as the trust anchor when dialing the stub peer's TLS port (spec §2.4.2). Using the
        // 6-argument overload so the peer's tls_certificate field is non-empty, which triggers the
        // relay's ClprEndpointClient to build a mutual-TLS channel rather than a plaintext one.
        interactor.configureVerifierConfig(
                contracts.stubVerifierAddress(),
                "eip155:1337",
                serviceAddr,
                1000,
                peer.port(),
                CertFixtures.CA_B_CERT_DER);

        final byte[] pong = {'P', 'O', 'N', 'G'};
        interactor.configureAppResponse(contracts.mockAppAddress(), pong);

        final byte[] salt = new byte[32];
        channelId = interactor.registerActiveChannel("eip155:1337", salt, contracts.stubVerifierAddress());

        RelayTestSupport.sendEthAndWait(txSubmitter, rpc, contracts.mockConnectorAddress(), 1_000_000_000_000_000_000L);
        connectorId = interactor.registerConnector(
                channelId, salt, contracts.mockConnectorAddress(), AnvilContainer.DEV_ADDRESS, CONNECTOR_STAKE);

        final String channelIdHex = "0x" + AbiCodec.toHexNoPrefix(channelId);
        relay = RelayTestSupport.buildRelay(
                RelayTestSupport.secureRelayConfig(
                        ANVIL.jsonRpcUrl(),
                        relayPort,
                        relayKeyPath.toString(),
                        RELAY_PRIVATE_KEY,
                        List.of(Triple.of(channelIdHex, contracts.clprServiceAddress(), ProofType.Hiero))),
                "A");
        // POKE_ONLY: start the inbound gRPC server + state listener, but NOT the outbound sync loop.
        relay.start(RelayInstance.StartOptions.inboundOnly());
    }

    @AfterAll
    static void teardownSecure() {
        if (relay != null) {
            relay.stop();
        }
        if (peer != null) {
            peer.close();
        }
    }

    /** The peer-side target application address for outbound {@code sendMessage} calls (a placeholder here). */
    private static byte[] targetApp() {
        return AbiCodec.fromHex(contracts.mockAppAddress());
    }

    /**
     * Stop the relay and restart it in FULL_DUPLEX mode using the supplied CA key and leaf validity.
     * Callers pass an alternative key path to test CA-mismatch rejection, or a short validity to
     * exercise leaf rotation before the first dial.
     *
     * @param keyPath                 path to the CA PKCS#8 private key to use for the new relay instance
     * @param leafCertValiditySeconds seconds between leaf re-mints for the new relay instance
     */
    private static void restartFullDuplex(final String keyPath, final long leafCertValiditySeconds) {
        relay.stop();
        final String channelIdHex = "0x" + AbiCodec.toHexNoPrefix(channelId);
        relay = RelayTestSupport.buildRelay(
                RelayTestSupport.secureRelayConfig(
                        ANVIL.jsonRpcUrl(),
                        relayPort,
                        keyPath,
                        leafCertValiditySeconds,
                        RELAY_PRIVATE_KEY,
                        List.of(Triple.of(channelIdHex, contracts.clprServiceAddress(), ProofType.Hiero))),
                "A");
        relay.start(RelayInstance.StartOptions.full());
    }

    /**
     * A relay presenting a leaf signed by CA-B (the wrong CA) must be rejected by the stub peer,
     * which requires CA-A. No sync payload should reach the stub peer within the observation window.
     */
    @Test
    void relayWithUntrustedCaIsRejectedByMtlsPeer() throws Exception {
        final var wrongKeyPath = tempDir.resolve("wrong-ca.der");
        Files.write(wrongKeyPath, CertFixtures.CA_B_KEY_DER);

        // Relay now holds a CA-B leaf — the stub peer's trust anchor is CA-A, so it will reject
        // every TLS handshake attempt.
        restartFullDuplex(wrongKeyPath.toString(), 86400L);
        peer.drain();

        // Give the relay a bundle to send so the sync loop actually attempts to dial the peer.
        interactor.sendMessage(channelId, connectorId, targetApp(), new byte[] {1});

        final ClprSyncPayload received = peer.awaitSync(Duration.ofSeconds(3));
        assertThat(received)
                .as("stub peer must reject a leaf signed by the wrong CA")
                .isNull();
    }

    /**
     * When the relay's initial leaf expires before the first dial, {@link LeafKeyManager} must
     * transparently rotate to a fresh leaf on the first TLS handshake. The stub peer should accept
     * the new leaf because it still chains to CA-A.
     */
    @Test
    void relayLeafRotatesBeforeFirstDial() throws InterruptedException {
        // 2-second validity: the initial leaf (minted at relay startup) will expire well before
        // the sleep below ends, so the first dial triggers a rotation.
        restartFullDuplex(relayKeyPath.toString(), 2L);
        peer.drain();

        // Wait past the rotation window so the initial leaf's validity has elapsed.
        Thread.sleep(4_000);

        // Give the relay a bundle to send so the sync loop actually dials the peer. The first
        // handshake triggers getCertificateChain(), which sees the rotation window elapsed and
        // mints a new leaf still signed by CA-A.
        interactor.sendMessage(channelId, connectorId, targetApp(), new byte[] {1});

        final ClprSyncPayload received = peer.awaitSync(Duration.ofSeconds(30));
        assertThat(received)
                .as("rotated leaf must still be accepted by stub peer (still signed by CA-A)")
                .isNotNull();
    }
}
