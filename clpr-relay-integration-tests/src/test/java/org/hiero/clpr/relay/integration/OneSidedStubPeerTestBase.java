// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.hapi.node.state.clpr.BundleResponse;
import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.List;
import org.hiero.clpr.relay.app.ProofType;
import org.hiero.clpr.relay.app.RelayInstance;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.EthSigner;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.testfixtures.TestEvmJsonRpcClient;
import org.hiero.clpr.relay.test.harness.StubPeer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Triple;

/**
 * Base for <b>single-side</b> lifecycle tests: one real relay against one Anvil-deployed CLPR contract
 * set, with the peer replaced by a controllable {@link StubPeer} and the chain driven directly by a
 * {@link ContractInteractor}. The relay is started in {@code POKE_ONLY} mode (outbound sync orchestrator
 * not started), so the <em>only</em> trigger for any transition is an explicit {@link #poke} — every
 * effect is unambiguously attributable to the injected bundle.
 *
 * <p>Replaces the two-relay {@link IntegrationTestBase} for lifecycle cases: half the contract
 * deployments and full control over the peer's bundles. The relay-under-test signs from a dedicated
 * account (account 2) distinct from the harness's account 0, so the relay's on-chain submissions and the
 * harness's sendMessage / closeConnection draw from independent nonce sequences and never collide. The
 * local CLPR Service remains the correctness oracle (it enforces §4.2 verification on every poked bundle).
 *
 * <p>Gated on {@code RUN_INTEGRATION_TESTS} + Docker like the rest of the suite.
 */
@Testcontainers
public abstract class OneSidedStubPeerTestBase {

    protected static final String RELAY_HOST = "127.0.0.1";
    private static final BigInteger CONNECTOR_STAKE = BigInteger.valueOf(100_000_000_000_000_000L);

    @Container
    protected static final AnvilContainer ANVIL = new AnvilContainer();

    /**
     * The relay-under-test's signing key (account 2). One key serves both roles the relay holds: EVM
     * transaction signing and endpoint sync-payload signing. It is deliberately distinct from the
     * harness's account 0 so the relay's submitBundle transactions and the harness's sendMessage /
     * closeConnection draw from independent nonce sequences and never collide. Per the contract
     * ({@code BundleLogic.submitBundle}) any registered endpoint may submit a bundle for any channel —
     * the submitter need not own the channel — so account 2 only has to register itself as an endpoint.
     */
    protected static final String RELAY_PRIVATE_KEY = AnvilContainer.DEV_PRIVATE_KEY_3;

    /** 64-byte uncompressed secp256k1 public key for the harness's admin/connector account (account 0). */
    protected static final byte[] DEV_PUB_KEY_64 = EthSigner.derivePublicKey(AnvilContainer.DEV_PRIVATE_KEY);

    protected static EvmJsonRpcClient rpc;
    protected static AnvilTxSubmitter txSubmitter;
    protected static DeployedContracts contracts;
    protected static ContractInteractor interactor;
    protected static byte[] channelId;
    protected static byte[] connectorId;
    protected static RelayInstance relay;
    protected static StubPeer peer;
    protected static int relayPort;

    @BeforeAll
    static void setupOneSided() throws Exception {
        rpc = new TestEvmJsonRpcClient(ANVIL.jsonRpcUrl());
        final var devSigner = new EthSigner(AnvilContainer.DEV_PRIVATE_KEY);
        txSubmitter = new AnvilTxSubmitter(rpc, devSigner, AnvilContainer.CHAIN_ID);
        contracts = new ContractDeployer(rpc, txSubmitter).deploy();
        interactor = new ContractInteractor(rpc, txSubmitter, devSigner, DEV_PUB_KEY_64, contracts);

        // The peer is a controllable StubPeer signing as Anvil dev account 1 (distinct from both the
        // harness's account 0 and the relay's account 2). Its key need not be a registered LOCAL endpoint
        // — it only has to appear in the relay's PEER roster (set via configureVerifierConfig) for inbound
        // signature verification.
        peer = StubPeer.start();

        relayPort = RelayTestSupport.findFreePort();
        final byte[] serviceAddr = AbiCodec.fromHex(contracts.clprServiceAddress());

        // The service deploys default-disabled; initialize + setClprEnabled(true) via bootstrapDefaults
        // before any mutating op, otherwise the calls below revert with ClprDisabled().
        interactor.bootstrapDefaults(serviceAddr);

        // Point the channel's peer roster at the StubPeer.
        // The relay verifies inbound endpoint signatures against this roster (spec §2.4.2).
        // peerServiceAddr is a placeholder (the relay never dials the peer's CLPR Service — it talks to
        // the StubPeer over gRPC and reads its sync target from this roster).
        interactor.configureVerifierConfig(
                contracts.stubVerifierAddress(), "eip155:1337", serviceAddr, 1000, peer.port());

        final byte[] pong = {'P', 'O', 'N', 'G'};
        interactor.configureAppResponse(contracts.mockAppAddress(), pong);

        final byte[] salt = new byte[32];
        channelId = interactor.registerActiveChannel("eip155:1337", salt, contracts.stubVerifierAddress());

        RelayTestSupport.sendEthAndWait(txSubmitter, rpc, contracts.mockConnectorAddress(), 1_000_000_000_000_000_000L);
        connectorId = interactor.registerConnector(
                channelId, salt, contracts.mockConnectorAddress(), AnvilContainer.DEV_ADDRESS, CONNECTOR_STAKE);

        final String channelIdHex = "0x" + AbiCodec.toHexNoPrefix(channelId);
        relay = RelayTestSupport.buildRelay(
                RelayTestSupport.relayConfig(
                        ANVIL.jsonRpcUrl(),
                        relayPort,
                        RELAY_PRIVATE_KEY,
                        List.of(Triple.of(channelIdHex, contracts.clprServiceAddress(), ProofType.Stub))),
                "A");
        // POKE_ONLY: start the inbound gRPC server + state listener, but NOT the outbound sync loop.
        relay.start(RelayInstance.StartOptions.inboundOnly());
    }

    @AfterAll
    static void teardownOneSided() {
        if (relay != null) {
            relay.stop();
        }
        if (peer != null) {
            peer.close();
        }
    }

    /**
     * Inject a crafted bundle into the relay-under-test via a two-phase sync and return the relay's
     * phase-1 reply. Phase 1 asks the relay for its own reciprocal (a {@code bundle_request} from message
     * 1); the relay answers with a bundle plus its own request. Phase 2 then delivers {@code bundlePayload}
     * as our {@code bundle_response}, which the relay verifies + submits to the local CLPR Service (an empty
     * payload is a responsive no-op that submits nothing). The relay's reciprocal bundle is read from the
     * returned reply via {@link #replyBundle}.
     *
     * @param bundlePayload the crafted bundle (e.g. via {@code StubBundles}), or {@link Bytes#EMPTY} to submit nothing
     * @return the relay's phase-1 reply (carrying its reciprocal {@code bundle_response} and its own {@code bundle_request})
     * @throws Exception if the gRPC call fails
     */
    protected static ClprSyncPayload poke(final Bytes bundlePayload) throws Exception {
        final Bytes connId = Bytes.wrap(channelId);
        final ClprSyncPayload phase1 = ClprSyncPayload.newBuilder()
                .channelId(connId)
                .bundleRequest(
                        BundleRequest.newBuilder().currentReceivedMessageId(0L).build())
                .build();
        return peer.pokeSync(RELAY_HOST, relay.grpcPort(), phase1, reply -> ClprSyncPayload.newBuilder()
                .channelId(connId)
                .bundleResponse(
                        BundleResponse.newBuilder().bundlePayload(bundlePayload).build())
                .build());
    }

    /**
     * The reciprocal bundle payload the relay returned in its phase-1 reply, or {@link Bytes#EMPTY} if it
     * built none (e.g. no on-chain record, or nothing to convey).
     */
    protected static Bytes replyBundle(final ClprSyncPayload reply) {
        return reply.hasBundleResponse() ? reply.bundleResponse().bundlePayload() : Bytes.EMPTY;
    }

    /** The peer-side target application address for outbound {@code sendMessage} calls (a placeholder here). */
    protected static byte[] targetApp() {
        return AbiCodec.fromHex(contracts.mockAppAddress());
    }

    /**
     * Stop the relay and restart it in FULL_DUPLEX mode (outbound sync orchestrator running), reusing the
     * same gRPC port. Used by tests that exercise the relay's own sync loop — e.g. draining against a
     * purely-responding {@link StubPeer} — rather than driving every transition via {@link #poke}.
     */
    protected static void restartFullDuplex() {
        relay.stop();
        final String channelIdHex = "0x" + AbiCodec.toHexNoPrefix(channelId);
        relay = RelayTestSupport.buildRelay(
                RelayTestSupport.relayConfig(
                        ANVIL.jsonRpcUrl(),
                        relayPort,
                        RELAY_PRIVATE_KEY,
                        List.of(Triple.of(channelIdHex, contracts.clprServiceAddress(), ProofType.Stub))),
                "A");
        relay.start(RelayInstance.StartOptions.full());
    }
}
