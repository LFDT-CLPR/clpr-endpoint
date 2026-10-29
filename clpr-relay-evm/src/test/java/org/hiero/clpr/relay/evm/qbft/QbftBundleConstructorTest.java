// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.qbft;

import static org.assertj.core.api.Assertions.*;
import static org.hiero.clpr.relay.evm.ByteUtils.ZERO_HASH;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprEndpointManifest;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprMessageValue;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import org.assertj.core.api.Assertions;
import org.hiero.clpr.relay.core.BundleConstructor;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.evm.BundleConstructorManifestContract;
import org.hiero.clpr.relay.evm.ByteUtils;
import org.hiero.clpr.relay.evm.EvmUtils;
import org.hiero.clpr.relay.evm.RlpReader;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.model.BlockHeader;
import org.hiero.clpr.relay.evm.model.ProofResponse;
import org.hiero.clpr.relay.evm.testfixtures.TestEvmJsonRpcClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link QbftBundleConstructor#createBundle}. In the two-phase sync protocol a bundle is
 * built ad hoc from a peer's {@link BundleRequest}: the channel state, queued messages and local
 * manifest come from a stubbed {@link ContractStateReader}, and the message range / trust anchor /
 * peer-known manifest version come from the request.
 */
class QbftBundleConstructorTest {

    // ── stubs ────────────────────────────────────────────────────────────────

    private static class StubRpcClient extends TestEvmJsonRpcClient {

        // queue allows returning different values on successive calls
        private final Queue<BlockHeader> blockResponses = new ArrayDeque<>();
        private final Queue<ProofResponse> proofResponses = new ArrayDeque<>();

        // captured args for assertions
        String lastBlockTag;
        String lastProofAddress;
        String[] lastProofStorageKeys;
        String lastProofBlockTag;
        int blockCallCount;
        int proofCallCount;

        void setBlockResponse(BlockHeader block) {
            blockResponses.clear();
            blockResponses.add(block);
        }

        void setProofResponse(ProofResponse proof) {
            proofResponses.clear();
            proofResponses.add(proof);
        }

        void addProofResponse(ProofResponse proof) {
            proofResponses.add(proof);
        }

        @Override
        public BlockHeader ethGetBlockHeaderByNumber(String blockTag) {
            this.lastBlockTag = blockTag;
            this.blockCallCount++;
            final BlockHeader resp = blockResponses.poll();
            if (resp == null) throw new IllegalStateException("No block response queued");
            // re-queue last response so repeated calls keep returning it
            blockResponses.add(resp);
            return resp;
        }

        @Override
        public ProofResponse ethGetProof(String address, String[] storageKeys, String blockTag) {
            this.lastProofAddress = address;
            this.lastProofStorageKeys = storageKeys;
            this.lastProofBlockTag = blockTag;
            this.proofCallCount++;
            final ProofResponse resp = proofResponses.poll();
            if (resp == null) throw new IllegalStateException("No proof response queued");
            return resp;
        }
    }

    /**
     * A {@link ContractStateReader} that returns a fixed channel, a fixed queued-message list and a
     * fixed local endpoint manifest — the three inputs {@code createBundle} reads from state.
     */
    private static final class StubStateReader implements ContractStateReader {
        private Optional<ClprChannel> channel = Optional.empty();
        private List<QueuedMessage> messages = List.of();
        private ClprEndpointManifest manifest = ClprEndpointManifest.DEFAULT;

        StubStateReader channel(final ClprChannel c) {
            this.channel = Optional.of(c);
            return this;
        }

        StubStateReader noChannel() {
            this.channel = Optional.empty();
            return this;
        }

        StubStateReader messages(final List<QueuedMessage> m) {
            this.messages = m;
            return this;
        }

        StubStateReader manifest(final ClprEndpointManifest m) {
            this.manifest = m;
            return this;
        }

        @Override
        public Optional<ClprChannel> readChannelState(final Bytes id, final String blockTag) {
            return channel;
        }

        @Override
        public List<QueuedMessage> readQueuedMessages(
                final Bytes id, final long fromId, final long toId, final String blockTag) {
            return messages;
        }

        @Override
        public ClprLedgerConfiguration readLedgerConfiguration(final CommitmentLevel commitmentLevel) {
            return ClprLedgerConfiguration.DEFAULT;
        }

        @Override
        public ClprEndpointManifest readEndpointManifest(final String blockTag) {
            return manifest;
        }
    }

    // ── fixtures ──────────────────────────────────────────────────────────────

    static final String CONTRACT = "0x" + "cc".repeat(20);

    static final Bytes CHANNEL_ID = Bytes.wrap(new byte[32]);

    StubRpcClient rpcClient;

    @BeforeEach
    void setUp() {
        rpcClient = new StubRpcClient();
    }

    private QbftBundleConstructor constructorWith(final ContractStateReader reader) {
        return new QbftBundleConstructor(
                Address.fromHexString(CONTRACT), 30_000L, 10, 10, rpcClient, reader, CommitmentLevel.LATEST);
    }

    /** A request asking for messages from {@code fromMessageId} with the given trust anchor / peer version. */
    private static BundleRequest request(
            final long currentReceivedMessageId, final Bytes trustAnchor, final long manifestVersion) {
        return BundleRequest.newBuilder()
                .currentReceivedMessageId(currentReceivedMessageId)
                .currentTrustAnchorId(trustAnchor)
                .currentEndpointManifestVersion(manifestVersion)
                .build();
    }

    /** Convenience: from message 1, trust anchor at epoch 0, peer manifest version 0. */
    private static BundleRequest defaultRequest() {
        return request(0L, encodedTrustAnchorId(0), 0L);
    }

    /** ACTIVE channel expecting message 2 next, nothing acked/received — carries pending message 1. */
    static ClprChannel messageChannel() {
        return ClprChannel.newBuilder()
                .channelId(CHANNEL_ID)
                .nextMessageId(2L)
                .sentRunningHash(Bytes.wrap(new byte[32]))
                .receivedMessageId(0L)
                .receivedRunningHash(Bytes.wrap(new byte[32]))
                .status(ClprChannelStatus.ACTIVE)
                .build();
    }

    /** ACTIVE channel with an ACK to convey (receivedMessageId > 0) and no pending messages. */
    static ClprChannel ackChannel() {
        return ClprChannel.newBuilder()
                .channelId(CHANNEL_ID)
                .nextMessageId(2L)
                .sentRunningHash(Bytes.fromHex("ff"))
                .receivedMessageId(1L)
                .receivedRunningHash(Bytes.fromHex("aa"))
                .status(ClprChannelStatus.ACTIVE)
                .build();
    }

    /** ACTIVE channel with nothing to send: next==1 (no messages), no ACK. */
    static ClprChannel minimalChannel() {
        return statusOnlyChannel(ClprChannelStatus.ACTIVE);
    }

    static ClprChannel statusOnlyChannel(final ClprChannelStatus status) {
        return ClprChannel.newBuilder()
                .channelId(CHANNEL_ID)
                .nextMessageId(1L)
                .sentRunningHash(Bytes.wrap(new byte[32]))
                .receivedMessageId(0L)
                .receivedRunningHash(Bytes.wrap(new byte[32]))
                .status(status)
                .build();
    }

    static ContractStateReader.QueuedMessage minimalMessage(long id, Bytes runningHashAfterProcessing) {
        final ClprMessagePayload payload = ClprMessagePayload.newBuilder().build();
        final ClprMessageValue value = ClprMessageValue.newBuilder()
                .payload(payload)
                .runningHashAfterProcessing(runningHashAfterProcessing)
                .build();
        return new ContractStateReader.QueuedMessage(BigInteger.valueOf(id), value);
    }

    /** Encode a QBFT trust-anchor id (an epoch) as its 8-byte big-endian form, as the constructor decodes. */
    private static Bytes encodedTrustAnchorId(final long epoch) {
        return Bytes.wrap(ByteBuffer.allocate(Long.BYTES).putLong(epoch).array());
    }

    // ── RLP encoding ──────────────────────────────────────────────────────────

    @Nested
    class RlpEncoding {

        @Test
        void encodedBytesAreNonEmpty() {
            final var reader = new StubStateReader()
                    .channel(messageChannel())
                    .messages(List.of(minimalMessage(1L, Bytes.fromHex("FF"))));
            rpcClient.setBlockResponse(minimalBlockHeader("0xdeadbeef"));
            rpcClient.setProofResponse(minimalProof("0xFF"));

            final var payload = constructorWith(reader)
                    .createBundle(CHANNEL_ID, defaultRequest())
                    .bundlePayload();

            assertThat(payload.length()).isGreaterThan(0);
        }

        @Test
        void encodedBytesStartWithRlpListPrefix() {
            final var reader = new StubStateReader()
                    .channel(messageChannel())
                    .messages(List.of(minimalMessage(1L, Bytes.fromHex("FF"))));
            rpcClient.setBlockResponse(minimalBlockHeader("0xdeadbeef"));
            rpcClient.setProofResponse(minimalProof("0xFF"));

            final byte[] encoded = constructorWith(reader)
                    .createBundle(CHANNEL_ID, defaultRequest())
                    .bundlePayload()
                    .toByteArray();

            assertThat(encoded[0] & 0xFF).isGreaterThanOrEqualTo(0xC0);
        }

        @Test
        void deterministicEncoding() {
            final var reader = new StubStateReader()
                    .channel(messageChannel())
                    .messages(List.of(minimalMessage(1L, Bytes.fromHex("FF"))));

            rpcClient.setBlockResponse(minimalBlockHeader("0xdeadbeef"));
            rpcClient.setProofResponse(minimalProof("0xFF"));
            final byte[] first = constructorWith(reader)
                    .createBundle(CHANNEL_ID, defaultRequest())
                    .bundlePayload()
                    .toByteArray()
                    .clone();

            // A fresh constructor with identical inputs must produce byte-identical output.
            rpcClient.setBlockResponse(minimalBlockHeader("0xdeadbeef"));
            rpcClient.setProofResponse(minimalProof("0xFF"));
            final byte[] second = constructorWith(reader)
                    .createBundle(CHANNEL_ID, defaultRequest())
                    .bundlePayload()
                    .toByteArray();

            assertThat(first).isEqualTo(second);
        }

        @Test
        void differentStorageValueProducesDifferentEncoding() {
            final var reader = new StubStateReader().channel(messageChannel());

            reader.messages(List.of(minimalMessage(1L, Bytes.fromHex("FF"))));
            rpcClient.setBlockResponse(blockHeaderAtNumber(1L));
            rpcClient.setProofResponse(minimalProof("0xFF"));
            final byte[] first = constructorWith(reader)
                    .createBundle(CHANNEL_ID, defaultRequest())
                    .bundlePayload()
                    .toByteArray()
                    .clone();

            reader.messages(List.of(minimalMessage(1L, Bytes.fromHex("DD"))));
            rpcClient.setBlockResponse(blockHeaderAtNumber(2L));
            rpcClient.setProofResponse(minimalProof("0xDD"));
            final byte[] second = constructorWith(reader)
                    .createBundle(CHANNEL_ID, defaultRequest())
                    .bundlePayload()
                    .toByteArray();

            assertThat(first).isNotEqualTo(second);
        }
    }

    // ── createBundle ────────────────────────────────────────────────────────

    @Nested
    class CreateBundle {

        @Test
        void emptyChannelStateProducesEmptyPayload() {
            final var reader = new StubStateReader().noChannel();
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));

            final var payload = constructorWith(reader)
                    .createBundle(CHANNEL_ID, defaultRequest())
                    .bundlePayload();

            assertThat(payload.length()).isZero();
            assertThat(rpcClient.proofCallCount).isZero();
        }

        @Test
        void emptyMessagesDoesNotCallProofRpc() {
            final var reader = new StubStateReader().channel(minimalChannel());
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));

            final var payload = constructorWith(reader)
                    .createBundle(CHANNEL_ID, defaultRequest())
                    .bundlePayload();

            assertThat(rpcClient.proofCallCount).isZero();
            assertThat(payload.length()).isZero();
        }

        @Test
        void emptyMessagesWithReceivedStateBuildsAckOnlyPayload() {
            final var reader = new StubStateReader().channel(ackChannel());
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.setProofResponse(minimalProof5("0xCAFE"));

            final var payload = constructorWith(reader)
                    .createBundle(CHANNEL_ID, defaultRequest())
                    .bundlePayload();

            assertThat(rpcClient.proofCallCount).isEqualTo(1);
            // 5 channel-field slots only — no message running-hash slot when there are no messages
            assertThat(rpcClient.lastProofStorageKeys).hasSize(5);
            assertThat(rpcClient.lastProofStorageKeys).allMatch(k -> k.matches("0x[0-9a-f]{64}"));
            assertThat(payload.length()).isGreaterThan(0);
        }

        @Test
        void closingStatusBuildsStatusOnlyBundle() {
            final var reader = new StubStateReader().channel(statusOnlyChannel(ClprChannelStatus.CLOSING));
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.setProofResponse(minimalProof5("0xCAFE"));

            final var payload = constructorWith(reader)
                    .createBundle(CHANNEL_ID, defaultRequest())
                    .bundlePayload();

            // A closing status must reach the peer even with no messages, acks or epoch headers: the bundle
            // is built from the five channel-field slots alone (no message running-hash slot).
            assertThat(rpcClient.proofCallCount).isEqualTo(1);
            assertThat(rpcClient.lastProofStorageKeys).hasSize(5);
            assertThat(payload.length()).isGreaterThan(0);
        }

        @Test
        void drainedStatusBuildsStatusOnlyBundle() {
            final var reader = new StubStateReader().channel(statusOnlyChannel(ClprChannelStatus.DRAINED));
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.setProofResponse(minimalProof5("0xCAFE"));

            final var payload = constructorWith(reader)
                    .createBundle(CHANNEL_ID, defaultRequest())
                    .bundlePayload();

            assertThat(payload.length()).isGreaterThan(0);
        }

        @Test
        void bundleExceedingMaxBundleSizeYieldsEmptyPayload() {
            // A CLOSING channel builds a non-empty status-only bundle (see closingStatusBuildsStatusOnlyBundle).
            // With maxBundleSize = 1 byte the assembled proof far exceeds the cap, so createBundle suppresses it
            // (returns an empty payload) rather than emitting a bundle a peer's submitBundle would choke on.
            final var reader = new StubStateReader().channel(statusOnlyChannel(ClprChannelStatus.CLOSING));
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.setProofResponse(minimalProof5("0xCAFE"));
            final var constructor = new QbftBundleConstructor(
                    Address.fromHexString(CONTRACT), 30_000L, 10, 10, rpcClient, reader, CommitmentLevel.LATEST, 1L);

            final var payload =
                    constructor.createBundle(CHANNEL_ID, defaultRequest()).bundlePayload();

            assertThat(payload.length()).isZero();
        }

        @Test
        void activeStatusWithNothingToConveySkips() {
            final var reader = new StubStateReader().channel(statusOnlyChannel(ClprChannelStatus.ACTIVE));
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));

            final var payload = constructorWith(reader)
                    .createBundle(CHANNEL_ID, defaultRequest())
                    .bundlePayload();

            // An ACTIVE channel with nothing to deliver and no lifecycle status to convey must not build
            // an empty bundle — the peer would reject it as no-progress.
            assertThat(rpcClient.proofCallCount).isZero();
            assertThat(payload.length()).isZero();
        }

        @Test
        void callsRpcWithCorrectBlockTag() {
            // The block number to prove against comes from the local head; 255 → "0xff".
            final var reader = new StubStateReader()
                    .channel(messageChannel())
                    .messages(List.of(minimalMessage(1L, Bytes.fromHex("FF"))));
            rpcClient.setBlockResponse(blockHeaderAtNumber(255L));
            rpcClient.setProofResponse(minimalProof("0xFF"));

            constructorWith(reader).createBundle(CHANNEL_ID, defaultRequest());

            assertThat(rpcClient.lastBlockTag).isEqualTo("0xff");
            assertThat(rpcClient.lastProofBlockTag).isEqualTo("0xff");
        }

        @Test
        void callsGetProofForCorrectContract() {
            final var reader = new StubStateReader()
                    .channel(messageChannel())
                    .messages(List.of(minimalMessage(1L, Bytes.fromHex("FF"))));
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.setProofResponse(minimalProof("0xFF"));

            constructorWith(reader).createBundle(CHANNEL_ID, defaultRequest());

            assertThat(rpcClient.lastProofAddress).isEqualTo(CONTRACT);
        }

        @Test
        void differentHeightsProduceDifferentBundles() {
            final var reader = new StubStateReader().channel(messageChannel());

            reader.messages(List.of(minimalMessage(1L, Bytes.fromHex("FF"))));
            rpcClient.setBlockResponse(blockHeaderAtNumber(1L));
            rpcClient.setProofResponse(minimalProof("0xFF"));
            final var constructor = constructorWith(reader);
            final byte[] first = constructor
                    .createBundle(CHANNEL_ID, defaultRequest())
                    .bundlePayload()
                    .toByteArray()
                    .clone();

            reader.messages(List.of(minimalMessage(1L, Bytes.fromHex("CC"))));
            rpcClient.setBlockResponse(blockHeaderAtNumber(2L));
            rpcClient.setProofResponse(minimalProof("0xCC"));
            final byte[] second = constructor
                    .createBundle(CHANNEL_ID, defaultRequest())
                    .bundlePayload()
                    .toByteArray();

            // Different local height → a fresh, distinct bundle.
            assertThat(first).isNotEqualTo(second);
        }

        @Test
        void differentChannelIdsBuiltIndependently() {
            final Bytes otherId = Bytes.wrap(new byte[] {0x01});
            final var reader = new StubStateReader()
                    .channel(messageChannel())
                    .messages(List.of(minimalMessage(1L, Bytes.fromHex("FF"))));

            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.addProofResponse(minimalProof("0xFF"));
            rpcClient.addProofResponse(minimalProof("0xFF"));
            final var constructor = constructorWith(reader);

            // The reader returns a channel whose channelId is the 32-zero fixture regardless of the
            // requested id, so both bundles build independently.
            final var first =
                    constructor.createBundle(CHANNEL_ID, defaultRequest()).bundlePayload();
            final var second =
                    constructor.createBundle(otherId, defaultRequest()).bundlePayload();

            assertThat(first.length()).isGreaterThan(0);
            assertThat(second.length()).isGreaterThan(0);
        }

        @Test
        void remoteAheadOfCurrentEpochProducesEmpty() {
            // block 1 → epoch 0; request trust anchor = 1 → epoch 1 > current → early return
            final var reader = new StubStateReader()
                    .channel(messageChannel())
                    .messages(List.of(minimalMessage(1L, Bytes.fromHex("FF"))));
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));

            final var payload = constructorWith(reader)
                    .createBundle(CHANNEL_ID, request(0L, encodedTrustAnchorId(1), 0L))
                    .bundlePayload();

            assertThat(rpcClient.proofCallCount).isZero();
            assertThat(payload.length()).isZero();
        }

        @Test
        void unparseableTrustAnchorAtEpochZeroDoesNotThrow() {
            // A trust anchor wider than a long must not crash bundle construction — it decodes to empty
            // and defaults to the current epoch (0 here), so messages keep flowing.
            final Bytes garbage = Bytes.wrap(new byte[] {
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF
            });
            final var reader = new StubStateReader()
                    .channel(messageChannel())
                    .messages(List.of(minimalMessage(1L, Bytes.fromHex("FF"))));
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.setProofResponse(minimalProof("0xFF"));

            assertThatCode(() -> {
                        final var payload = constructorWith(reader)
                                .createBundle(CHANNEL_ID, request(0L, garbage, 0L))
                                .bundlePayload();
                        assertThat(payload.length()).isGreaterThan(0);
                    })
                    .doesNotThrowAnyException();
        }

        @Test
        void unparseableTrustAnchorPastEpochZeroSkipsCatchUpAndKeepsMessagesFlowing() {
            // head number = 90_001 → epoch 3. A garbage trust anchor must fall back to "matches current
            // epoch" rather than "epoch 0", so the relay doesn't treat the remote as 3 epochs behind and
            // suppress messages while chasing a catch-up that can never converge.
            final Bytes garbage = Bytes.wrap(new byte[] {
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF
            });
            final var reader = new StubStateReader()
                    .channel(messageChannel())
                    .messages(List.of(minimalMessage(1L, Bytes.fromHex("FF"))));
            rpcClient.setBlockResponse(blockHeaderAtNumber(90_001L));
            rpcClient.setProofResponse(minimalProof("0xFF"));

            final var payload = constructorWith(reader)
                    .createBundle(CHANNEL_ID, request(0L, garbage, 0L))
                    .bundlePayload();

            // head + currentBlockHeader only — no epoch-header catch-up requested.
            assertThat(rpcClient.blockCallCount).isEqualTo(2);
            assertThat(rpcClient.proofCallCount).isEqualTo(1);
            // Messages are included (6 slots: 5 channel-field slots + 1 message running-hash slot).
            assertThat(rpcClient.lastProofStorageKeys).hasSize(6);
            assertThat(payload.length()).isGreaterThan(0);
        }

        @Test
        void remoteBehindByOneEpochIncludesOneEpochHeaderAndMessages() {
            // head number = 30_001 → epoch 1; request epoch = 0 → 1 epoch header + messages
            final var reader = new StubStateReader()
                    .channel(messageChannel())
                    .messages(List.of(minimalMessage(1L, Bytes.fromHex("FF"))));
            rpcClient.setBlockResponse(blockHeaderAtNumber(30_001L));
            rpcClient.setProofResponse(minimalProof("0xFF"));

            final var payload = constructorWith(reader)
                    .createBundle(CHANNEL_ID, request(0L, encodedTrustAnchorId(0), 0L))
                    .bundlePayload();

            // head + currentBlockHeader + 1 epoch header
            assertThat(rpcClient.blockCallCount).isEqualTo(3);
            assertThat(rpcClient.proofCallCount).isEqualTo(1);
            // 1 epoch header ≤ 1 → messages included → 6 storage slots
            assertThat(rpcClient.lastProofStorageKeys).hasSize(6);
            assertThat(payload.length()).isGreaterThan(0);
        }

        @Test
        void remoteBehindByManyEpochsIncludesEpochHeadersButNoMessages() {
            // head number = 90_001 → epoch 3; request epoch = 0 → 3 epoch headers, no messages
            final ClprChannel farBehind = ClprChannel.newBuilder()
                    .channelId(CHANNEL_ID)
                    .nextMessageId(2L)
                    .sentRunningHash(Bytes.wrap(new byte[32]))
                    .receivedMessageId(1L)
                    .receivedRunningHash(Bytes.wrap(new byte[32]))
                    .status(ClprChannelStatus.ACTIVE)
                    .build();
            final var reader =
                    new StubStateReader().channel(farBehind).messages(List.of(minimalMessage(1L, Bytes.fromHex("FF"))));
            rpcClient.setBlockResponse(blockHeaderAtNumber(90_001L));
            rpcClient.setProofResponse(minimalProof5("0xFF"));

            final var payload = constructorWith(reader)
                    .createBundle(CHANNEL_ID, request(0L, encodedTrustAnchorId(0), 0L))
                    .bundlePayload();

            // head + currentBlockHeader + 3 epoch headers (epochs 1, 2, 3)
            assertThat(rpcClient.blockCallCount).isEqualTo(5);
            assertThat(rpcClient.proofCallCount).isEqualTo(1);
            // >1 epoch headers → messages excluded → 5 channel-field slots only
            assertThat(rpcClient.lastProofStorageKeys).hasSize(5);
            assertThat(payload.length()).isGreaterThan(0);
        }

        @Test
        void unsetTrustAnchorDefaultsToCurrentEpochNotZero() {
            // An unset (empty) trust anchor now defaults to the CURRENT epoch, not 0. A peer with no
            // anchor yet is therefore treated as already up-to-date: no epoch-header catch-up is performed
            // even at a high epoch. head number = 90_001 → epoch 3.
            final var reader = new StubStateReader()
                    .channel(messageChannel())
                    .messages(List.of(minimalMessage(1L, Bytes.fromHex("FF"))));
            rpcClient.setBlockResponse(blockHeaderAtNumber(90_001L));
            rpcClient.setProofResponse(minimalProof("0xFF"));

            final var payload = constructorWith(reader)
                    .createBundle(CHANNEL_ID, request(0L, Bytes.EMPTY, 0L))
                    .bundlePayload();

            // Default == current epoch → not behind → head + currentBlockHeader only, no epoch headers.
            assertThat(rpcClient.blockCallCount).isEqualTo(2);
            // No epoch headers → messages included → 6 storage slots.
            assertThat(rpcClient.lastProofStorageKeys).hasSize(6);
            assertThat(payload.length()).isGreaterThan(0);
        }

        /**
         * Reproduction guard: drive a spread of random 32-byte channel ids through the real
         * slot-derivation path ({@code createBundle} → {@code calculateChannelFieldStorageSlot} /
         * {@code calculateMsgRunningHashStorageSlot} → {@link EvmUtils#toSlotHex}) and assert every
         * storage key handed to {@code eth_getProof} is a well-formed 32-byte DATA value. Before the fix,
         * ~28% of channels produced at least one odd-length / short key on every proof build.
         */
        @Test
        void realSlotPathAlwaysProduces64HexKeys() {
            final Random random = new Random(0xBADC0DEL);
            for (int i = 0; i < 64; i++) {
                final byte[] idBytes = new byte[32];
                random.nextBytes(idBytes);
                final Bytes channelId = Bytes.wrap(idBytes);
                final ClprChannel conn = ClprChannel.newBuilder()
                        .channelId(channelId)
                        .nextMessageId(2L)
                        .sentRunningHash(Bytes.wrap(new byte[32]))
                        .receivedMessageId(0L)
                        .receivedRunningHash(Bytes.wrap(new byte[32]))
                        .status(ClprChannelStatus.ACTIVE)
                        .build();
                final var reader =
                        new StubStateReader().channel(conn).messages(List.of(minimalMessage(1L, Bytes.fromHex("FF"))));

                rpcClient.setBlockResponse(minimalBlockHeader("0x"));
                rpcClient.setProofResponse(minimalProof("0xFF"));
                constructorWith(reader).createBundle(channelId, defaultRequest());

                assertThat(rpcClient.lastProofStorageKeys).hasSize(6);
                for (final String key : rpcClient.lastProofStorageKeys) {
                    assertThat(key).matches("0x[0-9a-f]{64}");
                }
            }
        }
    }

    // ── manifest proof branch (spec §4.2 Step 1b) ──────────────────────────────

    @Test
    void manifestVersionAhead_buildsSevenElementBundleWithManifestProof() {
        final ClprEndpointManifest manifest = ClprEndpointManifest.newBuilder()
                .version(1L)
                .serviceAddress(Bytes.wrap(new byte[20]))
                .build();
        final var reader = new StubStateReader().channel(ackChannel()).manifest(manifest);

        rpcClient.setBlockResponse(minimalBlockHeader("0x"));
        rpcClient.setProofResponse(minimalProof5("0xCAFE")); // channel-slots proof
        rpcClient.addProofResponse(minimalProof5("0xBEEF")); // manifest-commitment-slot proof

        // Peer holds version 0 → 1 > 0 → attach.
        final var payload = constructorWith(reader)
                .createBundle(CHANNEL_ID, request(0L, encodedTrustAnchorId(0), 0L))
                .bundlePayload();

        // Two ethGetProof calls: channel slots + the separate manifest commitment slot.
        assertThat(rpcClient.proofCallCount).isEqualTo(2);
        // The manifest proof (last call) requests exactly the endpoint-manifest commitment slot (18).
        assertThat(rpcClient.lastProofStorageKeys).hasSize(1);
        assertThat(new BigInteger(rpcClient.lastProofStorageKeys[0].substring(2), 16))
                .isEqualTo(BigInteger.valueOf(18));

        // The payload is the 7-element QBFT tuple (5 base elements + manifest proof + preimage).
        assertThat(payload.length()).isGreaterThan(0);
        Assertions.assertThat(RlpReader.splitList(payload)).hasSize(7);
    }

    @Test
    void defaultManifest_buildsFiveElementBundle() {
        // The default (version 0) local manifest is never strictly ahead of the peer → manifest branch skipped.
        final var reader = new StubStateReader().channel(ackChannel());
        rpcClient.setBlockResponse(minimalBlockHeader("0x"));
        rpcClient.setProofResponse(minimalProof5("0xCAFE"));

        final var payload = constructorWith(reader)
                .createBundle(CHANNEL_ID, request(0L, encodedTrustAnchorId(0), 0L))
                .bundlePayload();

        assertThat(rpcClient.proofCallCount).isEqualTo(1);
        assertThat(RlpReader.splitList(payload)).hasSize(5);
    }

    @Test
    void peerAlreadyCurrent_skipsManifestProof_buildsFiveElementBundle() {
        final ClprEndpointManifest manifest = ClprEndpointManifest.newBuilder()
                .version(1L)
                .serviceAddress(Bytes.wrap(new byte[20]))
                .build();
        final var reader = new StubStateReader().channel(ackChannel()).manifest(manifest);
        rpcClient.setBlockResponse(minimalBlockHeader("0x"));
        rpcClient.setProofResponse(minimalProof5("0xCAFE"));

        // The peer already reports holding version 1 of our manifest → 1 > 1 is false → skip.
        final var payload = constructorWith(reader)
                .createBundle(CHANNEL_ID, request(0L, encodedTrustAnchorId(0), 1L))
                .bundlePayload();

        // Only the channel-slots proof — no separate manifest-commitment proof.
        assertThat(rpcClient.proofCallCount).isEqualTo(1);
        assertThat(RlpReader.splitList(payload)).hasSize(5);
    }

    @Test
    void emptyServiceAddressManifest_skipsManifestProof_buildsFiveElementBundle() {
        // A version >= 1 manifest whose serviceAddress is still empty (freshly initialized ledger with
        // no admitted endpoint yet) must not be proven — _verifyEndpointManifest would reject it on-chain
        // with ManifestServiceAddressMismatch and stall delivery.
        final ClprEndpointManifest manifest =
                ClprEndpointManifest.newBuilder().version(1L).build();
        final var reader = new StubStateReader().channel(ackChannel()).manifest(manifest);
        rpcClient.setBlockResponse(minimalBlockHeader("0x"));
        rpcClient.setProofResponse(minimalProof5("0xCAFE"));

        final var payload = constructorWith(reader)
                .createBundle(CHANNEL_ID, request(0L, encodedTrustAnchorId(0), 0L))
                .bundlePayload();

        assertThat(rpcClient.proofCallCount).isEqualTo(1);
        assertThat(RlpReader.splitList(payload)).hasSize(5);
    }

    private BlockHeader minimalBlockHeader(String extraData) {
        return new BlockHeader(
                ZERO_HASH,
                ZERO_HASH,
                Address.ZERO,
                ZERO_HASH,
                ZERO_HASH,
                ZERO_HASH,
                ZERO_HASH,
                BigInteger.ZERO,
                BigInteger.ONE,
                BigInteger.ZERO,
                BigInteger.ZERO,
                BigInteger.ZERO,
                ByteUtils.fromPrefixedHex(extraData),
                ZERO_HASH,
                ZERO_HASH,
                BigInteger.ZERO,
                ZERO_HASH,
                BigInteger.ZERO,
                BigInteger.ZERO,
                ZERO_HASH,
                ZERO_HASH,
                ZERO_HASH);
    }

    private BlockHeader blockHeaderAtNumber(long blockNumber) {
        return new BlockHeader(
                ZERO_HASH,
                ZERO_HASH,
                Address.ZERO,
                ZERO_HASH,
                ZERO_HASH,
                ZERO_HASH,
                ZERO_HASH,
                BigInteger.ZERO,
                BigInteger.valueOf(blockNumber),
                BigInteger.ZERO,
                BigInteger.ZERO,
                BigInteger.ZERO,
                Bytes.EMPTY,
                ZERO_HASH,
                ZERO_HASH,
                BigInteger.ZERO,
                ZERO_HASH,
                BigInteger.ZERO,
                BigInteger.ZERO,
                ZERO_HASH,
                ZERO_HASH,
                ZERO_HASH);
    }

    private ProofResponse minimalProof(String storageValue) {
        return new ProofResponse(
                List.of(ByteUtils.fromPrefixedHex("0xf8518080"), ByteUtils.fromPrefixedHex("0xf85180")),
                List.of(
                        new ProofResponse.StorageProofEntry(
                                Bytes.fromHex("ccfe08badd7fbee8a36c1d2ba2b3090f679bf1a4970d307adddb9d938fc7bd73"),
                                ByteUtils.fromPrefixedHex(storageValue),
                                List.of(
                                        ByteUtils.fromPrefixedHex("0xf8518080"),
                                        ByteUtils.fromPrefixedHex(storageValue))),
                        new ProofResponse.StorageProofEntry(
                                Bytes.fromHex("00"),
                                ByteUtils.fromPrefixedHex(storageValue),
                                List.of(
                                        ByteUtils.fromPrefixedHex("0xf8518080"),
                                        ByteUtils.fromPrefixedHex(storageValue))),
                        new ProofResponse.StorageProofEntry(
                                Bytes.fromHex("11"),
                                ByteUtils.fromPrefixedHex(storageValue),
                                List.of(
                                        ByteUtils.fromPrefixedHex("0xf8518080"),
                                        ByteUtils.fromPrefixedHex(storageValue))),
                        new ProofResponse.StorageProofEntry(
                                Bytes.fromHex("22"),
                                ByteUtils.fromPrefixedHex(storageValue),
                                List.of(
                                        ByteUtils.fromPrefixedHex("0xf8518080"),
                                        ByteUtils.fromPrefixedHex(storageValue))),
                        new ProofResponse.StorageProofEntry(
                                Bytes.fromHex("44"),
                                ByteUtils.fromPrefixedHex(storageValue),
                                List.of(
                                        ByteUtils.fromPrefixedHex("0xf8518080"),
                                        ByteUtils.fromPrefixedHex(storageValue))),
                        new ProofResponse.StorageProofEntry(
                                Bytes.fromHex("33"),
                                ByteUtils.fromPrefixedHex(storageValue),
                                List.of(
                                        ByteUtils.fromPrefixedHex("0xf8518080"),
                                        ByteUtils.fromPrefixedHex("0xe2a0")))));
    }

    /** 5-entry proof for ack-only / epoch-headers-only bundles (no message running-hash slot). */
    private ProofResponse minimalProof5(String storageValue) {
        return new ProofResponse(
                List.of(ByteUtils.fromPrefixedHex("0xf8518080"), ByteUtils.fromPrefixedHex("0xf85180")),
                List.of(
                        new ProofResponse.StorageProofEntry(
                                Bytes.fromHex("ccfe08badd7fbee8a36c1d2ba2b3090f679bf1a4970d307adddb9d938fc7bd73"),
                                ByteUtils.fromPrefixedHex(storageValue),
                                List.of(
                                        ByteUtils.fromPrefixedHex("0xf8518080"),
                                        ByteUtils.fromPrefixedHex(storageValue))),
                        new ProofResponse.StorageProofEntry(
                                Bytes.fromHex("00"),
                                ByteUtils.fromPrefixedHex(storageValue),
                                List.of(
                                        ByteUtils.fromPrefixedHex("0xf8518080"),
                                        ByteUtils.fromPrefixedHex(storageValue))),
                        new ProofResponse.StorageProofEntry(
                                Bytes.fromHex("11"),
                                ByteUtils.fromPrefixedHex(storageValue),
                                List.of(
                                        ByteUtils.fromPrefixedHex("0xf8518080"),
                                        ByteUtils.fromPrefixedHex(storageValue))),
                        new ProofResponse.StorageProofEntry(
                                Bytes.fromHex("22"),
                                ByteUtils.fromPrefixedHex(storageValue),
                                List.of(
                                        ByteUtils.fromPrefixedHex("0xf8518080"),
                                        ByteUtils.fromPrefixedHex(storageValue))),
                        new ProofResponse.StorageProofEntry(
                                Bytes.fromHex("44"),
                                ByteUtils.fromPrefixedHex(storageValue),
                                List.of(
                                        ByteUtils.fromPrefixedHex("0xf8518080"),
                                        ByteUtils.fromPrefixedHex("0xe2a0")))));
    }

    /**
     * {@link BundleConstructorManifestContract} for the QBFT format: a manifest-carrying bundle is the
     * 7-element tuple, a skipped one is 5 elements.
     */
    @Nested
    class ManifestAttachmentContract extends BundleConstructorManifestContract {
        @Override
        protected BundleConstructor newConstructor(final ContractStateReader stateReader) {
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.setProofResponse(minimalProof5("0xCAFE")); // channel-slots proof
            rpcClient.addProofResponse(
                    minimalProof5("0xBEEF")); // manifest-commitment-slot proof (used only when attaching)
            return new QbftBundleConstructor(
                    Address.fromHexString(CONTRACT), 30_000L, 10, 10, rpcClient, stateReader, CommitmentLevel.LATEST);
        }

        @Override
        protected Bytes driveOneBundle(final BundleConstructor constructor, final long peerKnownVersion) {
            return constructor
                    .createBundle(CONN, request(0L, encodedTrustAnchorId(0), peerKnownVersion))
                    .bundlePayload();
        }

        @Override
        protected boolean manifestAttached(final Bytes payload) {
            return RlpReader.splitList(payload).size() == 7;
        }
    }
}
