// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import java.math.BigInteger;
import java.util.List;
import org.hiero.clpr.relay.app.ProofType;
import org.hiero.clpr.relay.app.RelayConfig;
import org.hiero.clpr.relay.app.RelayInstance;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.EthSigner;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.testfixtures.TestEvmJsonRpcClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Triple;

/**
 * Base class for end-to-end integration tests that spin up two full relay instances
 * (A and B) wired to a single Anvil dev-mode container with two independent deployments
 * of the CLPR contract set.
 *
 * <p>Tests are gated on the {@code RUN_INTEGRATION_TESTS} environment variable because
 * they require Docker. The CI workflow sets this variable; locally, developers may
 * run them with {@code RUN_INTEGRATION_TESTS=1 ./gradlew :clpr-relay-integration-tests:test}.
 */
@Testcontainers
abstract class IntegrationTestBase {

    /** Shared Anvil dev container used by all tests in a class. */
    @Container
    protected static final AnvilContainer ANVIL = new AnvilContainer();

    /** 64-byte uncompressed secp256k1 public key (X||Y) derived from the Anvil dev private key. */
    protected static final byte[] DEV_PUB_KEY_64 = EthSigner.derivePublicKey(AnvilContainer.DEV_PRIVATE_KEY);

    /**
     * 64-byte uncompressed secp256k1 public key for Anvil dev account 1. Relay B signs and
     * submits as this account so it doesn't race relay A for nonces on the shared dev account.
     */
    protected static final byte[] DEV_PUB_KEY_64_2 = EthSigner.derivePublicKey(AnvilContainer.DEV_PRIVATE_KEY_2);

    protected static EvmJsonRpcClient rpc;
    protected static AnvilTxSubmitter txSubmitter;
    protected static DeployedContracts contractsA;
    protected static DeployedContracts contractsB;
    protected static ContractInteractor interactorA;
    protected static ContractInteractor interactorB;
    protected static byte[] channelIdAtoB;
    protected static byte[] connectorIdAtoB;
    protected static RelayInstance relayA;
    protected static RelayInstance relayB;

    @BeforeAll
    static void setupInfra() throws Exception {
        rpc = new TestEvmJsonRpcClient(ANVIL.jsonRpcUrl());
        final var devSigner = new EthSigner(AnvilContainer.DEV_PRIVATE_KEY);
        txSubmitter = new AnvilTxSubmitter(rpc, devSigner, AnvilContainer.CHAIN_ID);
        final ContractDeployer deployer = new ContractDeployer(rpc, txSubmitter);
        contractsA = deployer.deploy();
        contractsB = deployer.deploy();

        interactorA = new ContractInteractor(rpc, txSubmitter, devSigner, DEV_PUB_KEY_64, contractsA);
        interactorB = new ContractInteractor(rpc, txSubmitter, devSigner, DEV_PUB_KEY_64, contractsB);

        // Allocate gRPC ports upfront so we can register them in the on-chain ledger config
        // before starting the relays. Each chain's ledger config stores the PEER relay's
        // endpoint so that the local relay reads its own chain to discover where to sync.
        final int portA = RelayTestSupport.findFreePort();
        final int portB = RelayTestSupport.findFreePort();

        // The service address for each side is its own CLPR Service contract address.
        final byte[] serviceAddrA = AbiCodec.fromHex(contractsA.clprServiceAddress());
        final byte[] serviceAddrB = AbiCodec.fromHex(contractsB.clprServiceAddress());

        // Default configuration on both sides. seed_endpoints is each ledger's own outward
        // self-advertisement (spec §1.1 / §2.4.2): chain A advertises relay A (portA), chain B
        // advertises relay B (portB). The relay no longer reads seed_endpoints for peer discovery
        // — it reads the per-Channel peer roster (populated via the verifier at
        // completeChannel; see configureVerifierConfig below). seed_endpoints is kept
        // spec-correct here so the fixture is honest (it must NOT point at the peer) and so the
        // throttles carried in the ledger config are still exercised.
        // The service deploys default-disabled; initialize (applies the default ledger + economic
        // config) and setClprEnabled(true) via bootstrapDefaults before any mutating op, otherwise
        // every config/registration call below reverts with ClprDisabled().
        interactorA.bootstrapDefaults(serviceAddrA);
        interactorB.bootstrapDefaults(serviceAddrB);

        // Register the dev account as an endpoint on both sides before any channel or connector
        // registration. The bond must be >= minEndpointBond (0.01 ether = 10_000_000_000_000_000 wei).
        interactorA.registerEndpoint(BigInteger.valueOf(10_000_000_000_000_000L));
        interactorB.registerEndpoint(BigInteger.valueOf(10_000_000_000_000_000L));

        // Also register Anvil dev account 1 as an endpoint on both chains. Relay B signs and
        // submits as account 1 (see configB below) so the two relays don't race the shared
        // dev-account nonce; account 1 must be a registered endpoint to call submitBundle.
        final var signer2 = new EthSigner(AnvilContainer.DEV_PRIVATE_KEY_2);
        final var txSubmitter2 = new AnvilTxSubmitter(rpc, signer2, AnvilContainer.CHAIN_ID);
        final ContractInteractor interactorA2 =
                new ContractInteractor(rpc, txSubmitter2, signer2, DEV_PUB_KEY_64_2, contractsA);
        final ContractInteractor interactorB2 =
                new ContractInteractor(rpc, txSubmitter2, signer2, DEV_PUB_KEY_64_2, contractsB);
        interactorA2.registerEndpoint(BigInteger.valueOf(10_000_000_000_000_000L));
        interactorB2.registerEndpoint(BigInteger.valueOf(10_000_000_000_000_000L));

        // Configure each verifier to accept the peer's config. The endpoint manifest that
        // completeChannel derives from the configProof must point at the PEER relay's gRPC port
        // and carry the PEER relay's signing key: chain A's manifest → relay B (portB, account 1),
        // chain B's manifest → relay A (portA, account 0). The relay reads this manifest to find
        // its sync target and to verify inbound endpoint signatures.
        interactorA.configureVerifierConfig(contractsA.stubVerifierAddress(), "eip155:1337", serviceAddrB, 1000, portB);
        interactorB.configureVerifierConfig(contractsB.stubVerifierAddress(), "eip155:1337", serviceAddrA, 1000, portA);

        // Pre-load the mock application response on both sides (a "PONG" payload).
        final byte[] pong = new byte[] {0x50, 0x4F, 0x4E, 0x47};
        interactorA.configureAppResponse(contractsA.mockAppAddress(), pong);
        interactorB.configureAppResponse(contractsB.mockAppAddress(), pong);

        // Register the same channel id on both sides (commit-reveal, client-side signed).
        // peerChainId must match what MockClprVerifier is configured to return ("eip155:1337").
        final byte[] testSalt = new byte[32]; // all-zero salt for tests
        channelIdAtoB = interactorA.registerActiveChannel("eip155:1337", testSalt, contractsA.stubVerifierAddress());
        interactorB.registerActiveChannel("eip155:1337", testSalt, contractsB.stubVerifierAddress());

        // Fund the MockClprConnector contracts so they can pay for inbound message execution.
        RelayTestSupport.sendEthAndWait(
                txSubmitter, rpc, contractsA.mockConnectorAddress(), 1_000_000_000_000_000_000L);
        RelayTestSupport.sendEthAndWait(
                txSubmitter, rpc, contractsB.mockConnectorAddress(), 1_000_000_000_000_000_000L);

        // Register a connector on both sides using commit-reveal. Stake = 0.1 ether (min).
        final byte[] connectorSalt = new byte[32]; // all-zero salt for tests
        final BigInteger minStake = BigInteger.valueOf(100_000_000_000_000_000L);
        connectorIdAtoB = interactorA.registerConnector(
                channelIdAtoB, connectorSalt, contractsA.mockConnectorAddress(), AnvilContainer.DEV_ADDRESS, minStake);
        interactorB.registerConnector(
                channelIdAtoB, connectorSalt, contractsB.mockConnectorAddress(), AnvilContainer.DEV_ADDRESS, minStake);

        // Start relays. Peer endpoints come from the on-chain ledger config, not from relay config.
        final String channelIdHex = "0x" + AbiCodec.toHexNoPrefix(channelIdAtoB);

        // Relay B signs/submits as Anvil dev account 1 (distinct from relay A's account 0) so the
        // two relays don't race the shared dev-account nonce. Each relay binds its gRPC server on
        // portA/portB (the same port each chain's roster advertises as the dial target).
        // Both relays are real, but the on-chain channel verifier is StubClprVerifier (the suite deploys
        // only mock/stub verifiers — Anvil is not a real QBFT chain). So each relay both builds
        // (local proof type) and parses (peerProofType) CLPRSTUB bundles: StubBundleConstructor reads
        // real channel state and emits "CLPRSTUB || protobuf(ClprBundleContent)", which the receiving
        // relay submits verbatim and StubClprVerifier accepts.
        final var connsA = List.of(Triple.of(channelIdHex, contractsA.clprServiceAddress(), ProofType.Stub));
        final var connsB = List.of(Triple.of(channelIdHex, contractsB.clprServiceAddress(), ProofType.Stub));
        final RelayConfig configA = RelayTestSupport.relayConfig(
                ANVIL.jsonRpcUrl(), portA, AnvilContainer.DEV_PRIVATE_KEY, connsA, ProofType.Stub);
        final RelayConfig configB = RelayTestSupport.relayConfig(
                ANVIL.jsonRpcUrl(), portB, AnvilContainer.DEV_PRIVATE_KEY_2, connsB, ProofType.Stub);

        relayA = RelayTestSupport.buildRelay(configA, "A");
        relayB = RelayTestSupport.buildRelay(configB, "B");
        relayA.start();
        relayB.start();
    }

    @AfterAll
    static void teardown() throws Exception {
        if (relayA != null) {
            relayA.stop();
        }
        if (relayB != null) {
            relayB.stop();
        }
    }

    /**
     * Decode a hex string (with or without {@code 0x} prefix) into a byte array.
     *
     * @param hex the hex string
     * @return the decoded bytes
     */
    protected static byte[] hexBytes(final String hex) {
        return AbiCodec.fromHex(hex);
    }

    /** Convenience: render a {@code long} as a {@link BigInteger} for readability in tests. */
    @SuppressWarnings("unused")
    protected static BigInteger wei(final long v) {
        return BigInteger.valueOf(v);
    }
}
