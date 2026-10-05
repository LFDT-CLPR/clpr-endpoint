// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprMessageValue;
import com.hedera.hapi.node.state.clpr.ClprSeiBundlePayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.EthSigner;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.sei.CometBftRpcClient;
import org.hiero.clpr.relay.evm.sei.SeiBundleConstructor;
import org.hiero.clpr.relay.evm.testfixtures.TestEvmJsonRpcClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration test for {@link SeiBundleConstructor} against a live Sei devnet.
 *
 * <p>The {@code ClprServiceTestFixture} contract — whose storage layout mirrors the real
 * {@code ClprServiceStorage} at exactly the slots {@link SeiBundleConstructor} reads and proves —
 * is deployed on the Sei EVM, then its {@code setConnection} / {@code setMessage} functions are
 * called to populate one channel plus message 1's running hash. The constructor then ABCI-queries
 * those same slots, fetches real ICS-23 IAVL proofs from the CometBFT node, and assembles a
 * {@link ClprSeiBundlePayload}. The fixture's source is {@code ClprServiceTestFixture.sol}; its
 * compiled creation bytecode is loaded from the {@code ClprServiceTestFixture.bin} classpath resource.
 *
 * <p>Tests are gated on {@code RUN_INTEGRATION_TESTS} because they require Docker.
 */
@Testcontainers
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
class SeiBundleConstructorIntegrationTest {

    /** Ordinal of {@code ConnectionStatus.ACTIVE} in {@code ClprServiceTestFixture.sol}. */
    private static final long STATUS_ACTIVE = 1L;

    /**
     * All-zero 32-byte channel ID used for slot derivation.
     * Must match the {@code channelId} set in {@link #minimalChannel()} and written to the fixture.
     */
    static final Bytes CHANNEL_ID = Bytes.wrap(new byte[32]);

    /**
     * Running hash stored in message 1's slot. A non-zero value yields an ICS-23 existence proof,
     * and it must equal the {@code runningHashAfterProcessing} passed to {@link #minimalMessage}
     * so {@link SeiBundleConstructor}'s consistency check passes.
     */
    static final Bytes MSG_RUNNING_HASH =
            Bytes.fromHex("cafecafe000000000000000000000000000000000000000000000000cafecafe");

    /**
     * Non-zero channel running hashes, written to the fixture and mirrored in
     * {@link #minimalChannel()}, so their storage slots yield ICS-23 existence proofs.
     */
    static final Bytes SENT_RUNNING_HASH =
            Bytes.fromHex("1111111111111111111111111111111111111111111111111111111111111111");

    static final Bytes RECEIVED_RUNNING_HASH =
            Bytes.fromHex("2222222222222222222222222222222222222222222222222222222222222222");

    /** Blocks {@code createBundle} backs the EVM head off so CometBFT headers H+1/H+2 are committed. */
    private static final int PROOF_LAG_BLOCKS = 2;

    @Container
    static final SeiContainer SEI = new SeiContainer();

    static TestEvmJsonRpcClient evmRpc;
    static CometBftRpcClient cometBftRpcClient;
    static String fixtureAddress;

    @BeforeAll
    static void setUp() throws Exception {
        evmRpc = new TestEvmJsonRpcClient(SEI.evmRpcUrl());
        cometBftRpcClient = new CometBftRpcClient(SEI.cometBftRpcUrl(), 3, Duration.ofSeconds(30));

        final var signer = new EthSigner(SeiContainer.DEV_PRIVATE_KEY);
        final var txSubmitter = new SeiTxSubmitter(evmRpc, signer, SeiContainer.EVM_CHAIN_ID);

        // Deploy ClprServiceTestFixture (zero-arg constructor).
        final byte[] initCode = AbiCodec.fromHex("0x" + loadFixtureBytecode());
        final var deployReceipt = txSubmitter.sendAndWait(null, initCode, 0L, 1_000_000L);
        fixtureAddress = deployReceipt.get("contractAddress").asText();

        // Populate the channel. The four fields below land in the four channel slots
        // SeiBundleConstructor proves (status/nextMessageId, receivedMessageId, sentRunningHash,
        // receivedRunningHash); all are non-zero so each yields an ICS-23 existence proof.
        final byte[] setChannel = AbiCodec.encodeFunctionCall(
                "setChannel(bytes32,address,uint8,uint64,uint64,uint64,bytes32,bytes32)",
                AbiCodec.encodeBytes32(CHANNEL_ID.toByteArray()),
                AbiCodec.encodeAddress("0x0000000000000000000000000000000000000000"), // verifier (unused)
                AbiCodec.encodeUint(STATUS_ACTIVE),
                AbiCodec.encodeUint(2L), // nextMessageId
                AbiCodec.encodeUint(0L), // ackedMessageId
                AbiCodec.encodeUint(1L), // receivedMessageId
                AbiCodec.encodeBytes32(SENT_RUNNING_HASH.toByteArray()),
                AbiCodec.encodeBytes32(RECEIVED_RUNNING_HASH.toByteArray()));
        txSubmitter.sendAndWait(fixtureAddress, setChannel, 0L, 300_000L);

        // Populate message 1's running hash — must equal MSG_RUNNING_HASH (the fixture value the
        // constructor reads back from chain) so its running-hash consistency check passes.
        final byte[] setMessage = AbiCodec.encodeFunctionCall(
                "setMessage(bytes32,uint64,bytes32)",
                AbiCodec.encodeBytes32(CHANNEL_ID.toByteArray()),
                AbiCodec.encodeUint(1L),
                AbiCodec.encodeBytes32(MSG_RUNNING_HASH.toByteArray()));
        txSubmitter.sendAndWait(fixtureAddress, setMessage, 0L, 300_000L);

        // Determine the block at which setup completed, then wait for enough further blocks that
        // createBundle's chosen height (head - PROOF_LAG_BLOCKS) lands strictly after the fixture write
        // AND its anchor headers (blockNum + 1 / + 2) are committed.
        final long setupHeight =
                evmRpc.ethGetBlockHeaderByNumber("latest").number().longValueExact();
        awaitHeader(setupHeight + PROOF_LAG_BLOCKS + 2);
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    @Test
    void absentConnectionReturnsEmptyBundle() {
        // A state reader with no channel record models the PENDING / unregistered case: createBundle
        // short-circuits with an empty payload rather than fetching any proof.
        final var stateReader = new FixedStateReader(null, List.of());
        final var constructor = new SeiBundleConstructor(
                Address.fromHexString(fixtureAddress),
                10,
                10,
                cometBftRpcClient,
                evmRpc,
                stateReader,
                CommitmentLevel.LATEST,
                PROOF_LAG_BLOCKS);
        assertThat(constructor
                        .createBundle(CHANNEL_ID, fromMessageOne())
                        .bundlePayload()
                        .length())
                .isZero();
    }

    @Test
    void createBundleReturnsNonEmptyPayload() {
        final var constructor = newConstructor();

        final var response = constructor.createBundle(CHANNEL_ID, fromMessageOne());

        assertThat(response.bundlePayload().length()).isGreaterThan(0);
    }

    @Test
    void bundlePayloadDeserializesAsClprSeiBundlePayload() throws Exception {
        final var constructor = newConstructor();

        final Bytes raw = constructor.createBundle(CHANNEL_ID, fromMessageOne()).bundlePayload();
        final ClprSeiBundlePayload payload = ClprSeiBundlePayload.PROTOBUF.parse(raw);

        assertThat(payload.hasStateProof()).isTrue();
        assertThat(payload.bundleContent().length()).isGreaterThan(0);
        assertThat(payload.stateProof().hasSignedHeader()).isTrue();
        assertThat(payload.stateProof().storageProofs())
                .hasSize(6); // 5 channel fields (incl manifest version) + 1 msg hash
    }

    /** A constructor wired to the live Sei node and a state reader serving the fixture channel + message 1. */
    private static SeiBundleConstructor newConstructor() {
        final var stateReader = new FixedStateReader(minimalChannel(), List.of(minimalMessage(1L, MSG_RUNNING_HASH)));
        return new SeiBundleConstructor(
                Address.fromHexString(fixtureAddress),
                10,
                10,
                cometBftRpcClient,
                evmRpc,
                stateReader,
                CommitmentLevel.LATEST,
                PROOF_LAG_BLOCKS);
    }

    /** The peer's request: give me everything from message 1. */
    private static BundleRequest fromMessageOne() {
        return BundleRequest.newBuilder().currentReceivedMessageId(0L).build();
    }

    // ── fixtures ──────────────────────────────────────────────────────────────

    static ClprChannel minimalChannel() {
        return ClprChannel.newBuilder()
                .channelId(CHANNEL_ID)
                .status(ClprChannelStatus.ACTIVE)
                .nextMessageId(2L)
                .ackedMessageId(0L)
                .sentRunningHash(SENT_RUNNING_HASH)
                .receivedMessageId(1L)
                .receivedRunningHash(RECEIVED_RUNNING_HASH)
                .trustAnchorId(Bytes.EMPTY)
                .build();
    }

    static ContractStateReader.QueuedMessage minimalMessage(final long id, final Bytes runningHash) {
        final ClprMessagePayload payload = ClprMessagePayload.newBuilder().build();
        final ClprMessageValue value = ClprMessageValue.newBuilder()
                .payload(payload)
                .runningHashAfterProcessing(runningHash)
                .build();
        return new ContractStateReader.QueuedMessage(BigInteger.valueOf(id), value);
    }

    // ── setup helpers ─────────────────────────────────────────────────────────

    /** Load the fixture's compiled creation bytecode (no {@code 0x} prefix) from the classpath. */
    private static String loadFixtureBytecode() throws IOException {
        try (var in = SeiBundleConstructorIntegrationTest.class.getResourceAsStream("/ClprServiceTestFixture.bin")) {
            if (in == null) {
                throw new IllegalStateException("ClprServiceTestFixture.bin not found on the test classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8).strip();
        }
    }

    /**
     * Wait until the signed header at {@code height} is available on the CometBFT node. This guarantees
     * the chain has advanced far enough that {@link SeiBundleConstructor#createBundle} — which reads the
     * head and backs off {@link #PROOF_LAG_BLOCKS} — anchors its proofs to committed headers at a height
     * after the fixture write.
     */
    private static void awaitHeader(final long height) throws InterruptedException {
        for (int i = 0; i < 120; i++) {
            try {
                cometBftRpcClient.getSignedHeader(height);
                return;
            } catch (final Exception ignored) {
                Thread.sleep(500L);
            }
        }
        throw new IllegalStateException("Block " + height + " not committed within 60s");
    }
}
