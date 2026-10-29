// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import static org.hiero.clpr.relay.test.harness.RelayAssertions.assertMetric;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.hapi.node.state.clpr.BundleResponse;
import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import org.hiero.clpr.relay.app.ProofType;
import org.hiero.clpr.relay.app.RelayInstance;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.EthSigner;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.testfixtures.TestEvmJsonRpcClient;
import org.hiero.clpr.relay.test.harness.StubBundles;
import org.hiero.clpr.relay.test.harness.StubPeer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Triple;

/**
 * A mid-run manifest version advance (driven by a submitted bundle that causes the verifier to
 * return a new {@code ClprEndpointManifest}) is detected by the relay's listener, which refreshes
 * the cached peer endpoint manifest and increments the {@code evm.listener.manifest.refreshed}
 * counter.
 *
 * <p>Uses {@code MockClprVerifier} (not {@code StubClprVerifier}) as the channel verifier
 * because {@code MockClprVerifier.verifyBundle} can be pre-configured (via
 * {@code setNewEndpointManifest}) to return a manifest with any version. When the relay submits a
 * poked bundle, the contract calls {@code verifyBundle}, gets version-2 (> current
 * {@code endpointManifestVersion=1}), and writes {@code conn.endpointManifestVersion=2}. The
 * listener detects this on its next poll and increments the counter.
 *
 * <p>Stands alone (not on {@link OneSidedStubPeerTestBase}) because the base class hard-wires
 * {@code StubClprVerifier} whose {@code verifyBundle} always returns version 0 (absent).
 */
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
@Testcontainers
class PeerManifestAdvanceStubPeerTest {

    private static final String RELAY_HOST = "127.0.0.1";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Container
    static final AnvilContainer ANVIL = new AnvilContainer();

    static EvmJsonRpcClient rpc;
    static ContractInteractor interactor;
    static DeployedContracts contracts;
    static byte[] channelId;
    static RelayInstance relay;
    static StubPeer peer;
    static int relayPort;

    @BeforeAll
    static void setup() throws Exception {
        rpc = new TestEvmJsonRpcClient(ANVIL.jsonRpcUrl());
        final var devSigner = new EthSigner(AnvilContainer.DEV_PRIVATE_KEY);
        final var txSubmitter = new AnvilTxSubmitter(rpc, devSigner, AnvilContainer.CHAIN_ID);
        final byte[] devPubKey64 = EthSigner.derivePublicKey(AnvilContainer.DEV_PRIVATE_KEY);
        contracts = new ContractDeployer(rpc, txSubmitter).deploy();
        interactor = new ContractInteractor(rpc, txSubmitter, devSigner, devPubKey64, contracts);

        peer = StubPeer.start();
        peer.respondEmpty();

        relayPort = RelayTestSupport.findFreePort();
        final byte[] serviceAddr = AbiCodec.fromHex(contracts.clprServiceAddress());

        // The service deploys default-disabled; initialize + setClprEnabled(true) via bootstrapDefaults
        // before any mutating op, otherwise the calls below revert with ClprDisabled().
        interactor.bootstrapDefaults(serviceAddr);
        // Use MockClprVerifier: its verifyBundle returns a configurable ClprEndpointManifest.
        // StubClprVerifier always returns version=0 from verifyBundle and cannot be configured.
        interactor.configureVerifierConfig(
                contracts.mockVerifierAddress(), "eip155:1337", serviceAddr, 1000, peer.port());
        interactor.configureAppResponse(contracts.mockAppAddress(), new byte[] {0x50, 0x4F, 0x4E, 0x47});

        final byte[] salt = new byte[32];
        channelId = interactor.registerActiveChannel("eip155:1337", salt, contracts.mockVerifierAddress());

        RelayTestSupport.sendEthAndWait(txSubmitter, rpc, contracts.mockConnectorAddress(), 1_000_000_000_000_000_000L);
        interactor.registerConnector(
                channelId,
                salt,
                contracts.mockConnectorAddress(),
                AnvilContainer.DEV_ADDRESS,
                BigInteger.valueOf(100_000_000_000_000_000L));

        final String channelIdHex = "0x" + AbiCodec.toHexNoPrefix(channelId);
        relay = RelayTestSupport.buildRelay(
                RelayTestSupport.relayConfig(
                        ANVIL.jsonRpcUrl(),
                        relayPort,
                        AnvilContainer.DEV_PRIVATE_KEY_3,
                        List.of(Triple.of(channelIdHex, contracts.clprServiceAddress(), ProofType.Stub))),
                "A");
        relay.start(RelayInstance.StartOptions.inboundOnly());
    }

    @AfterAll
    static void teardown() {
        if (relay != null) relay.stop();
        if (peer != null) peer.close();
    }

    @Test
    void manifestVersionAdvanceTriggersManifestRefresh() throws Exception {
        // Wait for the initial startup refresh (version=1 from completeChannel) and capture baseline.
        assertMetric(relay.metrics(), "evm.listener", "manifest.refreshed")
                .eventually(TIMEOUT)
                .isPositive();
        final long baseline = assertMetric(relay.metrics(), "evm.listener", "manifest.refreshed")
                .value();

        // Configure MockClprVerifier to return nextMessageId=1 from verifyBundle.
        // The contract's step-4 replay check requires metadata.nextMessageId >= conn.receivedMessageId+1 = 1.
        // Default metadata has nextMessageId=0 which triggers ClprReplayDetected; set it to 1 (no new messages).
        // status=1 (ACTIVE) to avoid triggering an unintended status transition.
        interactor.configureVerifierBundleMetadata(contracts.mockVerifierAddress(), 1L, 0L, 1);

        // Configure MockClprVerifier to return a version-2 manifest from verifyBundle.
        // The manifest must include the peer endpoint so the listener increments manifest.refreshed
        // (the listener skips the counter when the refreshed manifest is empty).
        interactor.configureVerifierBundleManifest(contracts.mockVerifierAddress(), 2L, "127.0.0.1", peer.port());

        // Poke a Hiero bundle that bypasses GuardedTransactionSubmitter's initial-state guard.
        // The guard skips bundles with bundleNext<=1 && bundleAck==0 (nothing to deliver post-completeChannel).
        // Setting receivedMessageId(1) fakes the peer's ACK of outbound message 0, giving bundleAck=1>0.
        // MockClprVerifier.verifyBundle ignores the bundle content and returns default metadata
        // (receivedMessageId=0) so no actual ACK is applied on-chain; only the manifest update fires.
        final ContractInteractor.ChannelState state = interactor.requireChannelState(channelId);
        final Bytes bundle = StubBundles.stubProof()
                .ackedMessageId(state.receivedMessageId())
                .receivedMessageId(1)
                .build();
        // Two-phase: phase 1 asks the relay for its data (a bundle_request); phase 2 delivers the crafted
        // bundle as our bundle_response, which the relay verifies + submits to the local CLPR Service — the
        // submission drives MockClprVerifier.verifyBundle to return the version-2 manifest.
        final Bytes connId = Bytes.wrap(channelId);
        peer.pokeSync(
                RELAY_HOST,
                relayPort,
                ClprSyncPayload.newBuilder()
                        .channelId(connId)
                        .bundleRequest(BundleRequest.newBuilder()
                                .currentReceivedMessageId(0L)
                                .build())
                        .build(),
                reply -> ClprSyncPayload.newBuilder()
                        .channelId(connId)
                        .bundleResponse(BundleResponse.newBuilder()
                                .bundlePayload(bundle)
                                .build())
                        .build());

        // The listener detects endpointManifestVersion=2 != 1 and refreshes, incrementing the counter.
        assertMetric(relay.metrics(), "evm.listener", "manifest.refreshed")
                .eventually(TIMEOUT)
                .isGreaterThan(baseline);
    }
}
