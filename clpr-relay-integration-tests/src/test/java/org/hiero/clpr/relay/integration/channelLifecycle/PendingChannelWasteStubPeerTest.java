// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration.channelLifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import org.hiero.clpr.relay.app.ProofType;
import org.hiero.clpr.relay.app.RelayInstance;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.EthSigner;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.testfixtures.TestEvmJsonRpcClient;
import org.hiero.clpr.relay.integration.AnvilContainer;
import org.hiero.clpr.relay.integration.AnvilTxSubmitter;
import org.hiero.clpr.relay.integration.ContractDeployer;
import org.hiero.clpr.relay.integration.ContractInteractor;
import org.hiero.clpr.relay.integration.DeployedContracts;
import org.hiero.clpr.relay.integration.OneSidedStubPeerTestBase;
import org.hiero.clpr.relay.integration.RelayTestSupport;
import org.hiero.clpr.relay.test.harness.StubPeer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Triple;

/**
 * A PENDING (commit-only) channel must neither initiate outbound sync nor accept inbound sync. Stands alone (not
 * on {@link OneSidedStubPeerTestBase}) because it needs a commit-only channel — no
 * {@code completeChannel}, no connector — rather than the base's fully-ACTIVE setup.
 */
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
@Testcontainers
class PendingChannelWasteStubPeerTest {

    private static final String RELAY_HOST = "127.0.0.1";

    @Container
    static final AnvilContainer ANVIL = new AnvilContainer();

    static EvmJsonRpcClient rpc;
    static ContractInteractor interactor;
    static byte[] channelId;
    static RelayInstance relay;
    static StubPeer peer;

    @BeforeAll
    static void setup() throws Exception {
        rpc = new TestEvmJsonRpcClient(ANVIL.jsonRpcUrl());
        final var signer = new EthSigner(AnvilContainer.DEV_PRIVATE_KEY);
        final var txSubmitter = new AnvilTxSubmitter(rpc, signer, AnvilContainer.CHAIN_ID);
        final byte[] devPubKey64 = EthSigner.derivePublicKey(AnvilContainer.DEV_PRIVATE_KEY);
        final DeployedContracts contracts = new ContractDeployer(rpc, txSubmitter).deploy();
        interactor = new ContractInteractor(rpc, txSubmitter, signer, devPubKey64, contracts);

        peer = StubPeer.start();

        final int relayPort = RelayTestSupport.findFreePort();
        final byte[] serviceAddr = AbiCodec.fromHex(contracts.clprServiceAddress());
        // The service deploys default-disabled; initialize + setClprEnabled(true) via bootstrapDefaults
        // before any mutating op, otherwise the calls below revert with ClprDisabled().
        interactor.bootstrapDefaults(serviceAddr);
        interactor.registerEndpoint(BigInteger.valueOf(10_000_000_000_000_000L));

        // Register the commitment only — no completeChannel — leaving the id PENDING.
        channelId = interactor.registerChannelCommitOnly("eip155:1337", new byte[32]);

        final String channelIdHex = "0x" + AbiCodec.toHexNoPrefix(channelId);
        relay = RelayTestSupport.buildRelay(
                RelayTestSupport.relayConfig(
                        ANVIL.jsonRpcUrl(),
                        relayPort,
                        AnvilContainer.DEV_PRIVATE_KEY,
                        List.of(Triple.of(channelIdHex, contracts.clprServiceAddress(), ProofType.Hiero))),
                "pending");
        // FULL_DUPLEX so that, if the relay were going to initiate outbound sync, it would.
        relay.start(RelayInstance.StartOptions.full());
    }

    @AfterAll
    static void teardown() {
        if (relay != null) {
            relay.stop();
        }
        if (peer != null) {
            peer.close();
        }
    }

    @Test
    void pendingChannelDoesNotInitiateOutboundSync() throws Exception {
        Thread.sleep(5_000);
        assertThat(RelayTestSupport.outboundAttempts(relay))
                .as("a PENDING channel must not initiate outbound sync (spec §3.1.3)")
                .isZero();
        assertThat(peer.syncCount())
                .as("the stub peer must receive no syncs from a PENDING-channel relay")
                .isZero();
    }

    @Test
    void pendingChannelRejectsInboundSync() throws Exception {
        final var reply = peer.pokeSync(
                RELAY_HOST,
                relay.grpcPort(),
                ClprSyncPayload.newBuilder()
                        .channelId(Bytes.wrap(channelId))
                        .bundleRequest(BundleRequest.newBuilder()
                                .currentReceivedMessageId(0L)
                                .build())
                        .build());
        final long replyPayloadLength = reply.hasBundleResponse()
                ? reply.bundleResponse().bundlePayload().length()
                : 0L;
        assertThat(replyPayloadLength)
                .as("a PENDING channel must reject inbound sync with an empty-proof response (spec §3.1.3)")
                .isZero();
    }

    @Test
    void neverRegisteredChannelReportsEmpty() {
        final byte[] phantom = new byte[32];
        Arrays.fill(phantom, (byte) 0x77);
        assertThat(interactor.readChannelState(phantom))
                .as("a never-registered channel id reports empty, identical to PENDING")
                .isEmpty();
    }
}
