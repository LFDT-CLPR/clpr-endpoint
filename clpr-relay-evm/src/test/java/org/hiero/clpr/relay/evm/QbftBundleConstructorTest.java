// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import static org.assertj.core.api.Assertions.*;
import static org.hiero.clpr.relay.evm.ByteUtils.ZERO_HASH;

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
import org.hiero.clpr.relay.core.BundleConstructor;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.PeerManifestVersionCache;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.model.BlockHeader;
import org.hiero.clpr.relay.evm.model.ProofResponse;
import org.hiero.clpr.relay.evm.testfixtures.TestEvmJsonRpcClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class QbftBundleConstructorTest {

    // ── stub ─────────────────────────────────────────────────────────────────

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

    // ── fixtures ──────────────────────────────────────────────────────────────

    static final String CONTRACT = "0x" + "cc".repeat(20);

    static final Bytes CHANNEL_ID = Bytes.wrap(new byte[32]);

    StubRpcClient rpcClient;
    QbftBundleConstructor constructor;

    @BeforeEach
    void setUp() {
        rpcClient = new StubRpcClient();
        constructor = new QbftBundleConstructor(Address.fromHexString(CONTRACT), 30_000L, 10, 10, rpcClient, null);
    }

    static ClprChannel minimalChannel() {
        return ClprChannel.newBuilder()
                .channelId(CHANNEL_ID)
                .nextMessageId(1L)
                .sentRunningHash(Bytes.wrap(new byte[32]))
                .receivedMessageId(0L)
                .receivedRunningHash(Bytes.wrap(new byte[32]))
                .status(ClprChannelStatus.ACTIVE)
                .remoteTrustAnchorId(encodedTrustAnchorId(0))
                .build();
    }

    static ClprChannel channelWithReceivedMessage() {
        return ClprChannel.newBuilder()
                .channelId(CHANNEL_ID)
                .nextMessageId(2L)
                .sentRunningHash(Bytes.fromHex("ff"))
                .receivedMessageId(1L)
                .receivedRunningHash(Bytes.fromHex("aa"))
                .status(ClprChannelStatus.ACTIVE)
                .remoteTrustAnchorId(encodedTrustAnchorId(0))
                .build();
    }

    static ClprChannel statusOnlyChannel(final ClprChannelStatus status) {
        return ClprChannel.newBuilder()
                .channelId(CHANNEL_ID)
                .nextMessageId(1L)
                .sentRunningHash(Bytes.wrap(new byte[32]))
                .receivedMessageId(0L)
                .receivedRunningHash(Bytes.wrap(new byte[32]))
                .status(status)
                .remoteTrustAnchorId(encodedTrustAnchorId(0))
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
        void encodedBytesAreNonEmpty() throws Exception {
            rpcClient.setBlockResponse(minimalBlockHeader("0xdeadbeef"));
            rpcClient.setProofResponse(minimalProof("0xFF"));

            constructor.onStateChanged(
                    BigInteger.ONE, CHANNEL_ID, minimalChannel(), List.of(minimalMessage(1L, Bytes.fromHex("FF"))));

            assertThat(constructor.getLatestBundlePayload(CHANNEL_ID))
                    .isPresent()
                    .get()
                    .extracting(b -> b.toByteArray())
                    .satisfies(b -> assertThat(b.length > 0));
        }

        @Test
        void encodedBytesStartWithRlpListPrefix() throws Exception {
            rpcClient.setBlockResponse(minimalBlockHeader("0xdeadbeef"));
            rpcClient.setProofResponse(minimalProof("0xFF"));

            constructor.onStateChanged(
                    BigInteger.ONE, CHANNEL_ID, minimalChannel(), List.of(minimalMessage(1L, Bytes.fromHex("FF"))));

            final byte[] encoded =
                    constructor.getLatestBundlePayload(CHANNEL_ID).get().toByteArray();

            assertThat(encoded[0] & 0xFF).isGreaterThanOrEqualTo(0xC0);
        }

        @Test
        void deterministicEncoding() throws Exception {
            rpcClient.setBlockResponse(minimalBlockHeader("0xdeadbeef"));
            rpcClient.setProofResponse(minimalProof("0xFF"));

            constructor.onStateChanged(
                    BigInteger.ONE, CHANNEL_ID, minimalChannel(), List.of(minimalMessage(1L, Bytes.fromHex("FF"))));
            final byte[] first = constructor
                    .getLatestBundlePayload(CHANNEL_ID)
                    .get()
                    .toByteArray()
                    .clone();

            rpcClient.setProofResponse(minimalProof("0xFF")); // re-queue for second call
            constructor.onStateChanged(
                    BigInteger.ONE, CHANNEL_ID, minimalChannel(), List.of(minimalMessage(1L, Bytes.fromHex("FF"))));
            final byte[] second =
                    constructor.getLatestBundlePayload(CHANNEL_ID).get().toByteArray();

            assertThat(first).isEqualTo(second);
        }

        @Test
        void differentStorageValueProducesDifferentEncoding() throws Exception {
            rpcClient.setBlockResponse(minimalBlockHeader("0xdeadbeef"));
            rpcClient.setProofResponse(minimalProof("0xFF"));

            constructor.onStateChanged(
                    BigInteger.ONE, CHANNEL_ID, minimalChannel(), List.of(minimalMessage(1L, Bytes.fromHex("FF"))));
            final byte[] first = constructor
                    .getLatestBundlePayload(CHANNEL_ID)
                    .get()
                    .toByteArray()
                    .clone();

            rpcClient.setProofResponse(minimalProof("0xDD"));
            constructor.onStateChanged(
                    BigInteger.ONE, CHANNEL_ID, minimalChannel(), List.of(minimalMessage(1L, Bytes.fromHex("DD"))));
            final byte[] second =
                    constructor.getLatestBundlePayload(CHANNEL_ID).get().toByteArray();

            assertThat(first).isNotEqualTo(second);
        }
    }

    // ── onStateChanged ────────────────────────────────────────────────────────

    @Nested
    class OnStateChanged {

        @Test
        void emptyMessagesDoesNotCallProofRpc() {
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));

            constructor.onStateChanged(BigInteger.ONE, CHANNEL_ID, minimalChannel(), List.of());

            assertThat(rpcClient.blockCallCount).isEqualTo(1);
            assertThat(rpcClient.proofCallCount).isZero();
        }

        @Test
        void emptyMessagesLeavesNothingCached() {
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));

            constructor.onStateChanged(BigInteger.ONE, CHANNEL_ID, minimalChannel(), List.of());

            assertThat(constructor.getLatestBundlePayload(CHANNEL_ID)).isEmpty();
        }

        @Test
        void emptyMessagesWithReceivedStateCachesAckOnlyPayload() throws Exception {
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.setProofResponse(minimalProof5("0xCAFE"));

            constructor.onStateChanged(BigInteger.ONE, CHANNEL_ID, channelWithReceivedMessage(), List.of());

            assertThat(rpcClient.blockCallCount).isEqualTo(1);
            assertThat(rpcClient.proofCallCount).isEqualTo(1);
            // 5 channel-field slots only — no message running-hash slot when there are no messages
            assertThat(rpcClient.lastProofStorageKeys).hasSize(5);
            assertThat(rpcClient.lastProofStorageKeys).allMatch(k -> k.matches("0x[0-9a-f]{64}"));
            assertThat(constructor.getLatestBundlePayload(CHANNEL_ID)).isPresent();
        }

        @Test
        void bundleExceedingMaxBundleSizeIsNotEmitted() throws Exception {
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.setProofResponse(minimalProof5("0xCAFE"));
            // maxBundleSize = 1 byte → any assembled proof exceeds it, so nothing is cached or emitted.
            final var guarded = new QbftBundleConstructor(
                    Address.fromHexString(CONTRACT), 30_000L, 10, 10, rpcClient, null, null, 1L);

            guarded.onStateChanged(BigInteger.ONE, CHANNEL_ID, channelWithReceivedMessage(), List.of());

            assertThat(guarded.getLatestBundlePayload(CHANNEL_ID)).isEmpty();
        }

        @Test
        void closingStatusWithFrozenCountersCachesStatusOnlyBundle() throws Exception {
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.setProofResponse(minimalProof5("0xCAFE"));

            constructor.onStateChanged(
                    BigInteger.ONE, CHANNEL_ID, statusOnlyChannel(ClprChannelStatus.CLOSING), List.of());

            // A closing status must reach the peer even with no messages, acks, or epoch headers: the bundle
            // is built from the five channel-field slots alone (no message running-hash slot).
            assertThat(rpcClient.proofCallCount).isEqualTo(1);
            assertThat(rpcClient.lastProofStorageKeys).hasSize(5);
            assertThat(constructor.getLatestBundlePayload(CHANNEL_ID)).isPresent();
        }

        @Test
        void drainedStatusWithFrozenCountersCachesStatusOnlyBundle() throws Exception {
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.setProofResponse(minimalProof5("0xCAFE"));

            constructor.onStateChanged(
                    BigInteger.ONE, CHANNEL_ID, statusOnlyChannel(ClprChannelStatus.DRAINED), List.of());

            assertThat(constructor.getLatestBundlePayload(CHANNEL_ID)).isPresent();
        }

        @Test
        void activeStatusWithFrozenCountersStillSkips() {
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));

            constructor.onStateChanged(
                    BigInteger.ONE, CHANNEL_ID, statusOnlyChannel(ClprChannelStatus.ACTIVE), List.of());

            // An ACTIVE channel with nothing to deliver and no lifecycle status to convey must not build
            // an empty bundle — the peer would reject it as no-progress.
            assertThat(rpcClient.proofCallCount).isZero();
            assertThat(constructor.getLatestBundlePayload(CHANNEL_ID)).isEmpty();
        }

        @Test
        void cachesPayloadAfterSuccessfulCall() throws Exception {
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.setProofResponse(minimalProof("0xCAFE"));

            constructor.onStateChanged(
                    BigInteger.ONE, CHANNEL_ID, minimalChannel(), List.of(minimalMessage(1L, Bytes.fromHex("CAFE"))));

            assertThat(constructor.getLatestBundlePayload(CHANNEL_ID)).isPresent();
        }

        @Test
        void callsRpcWithCorrectBlockTag() throws Exception {
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.setProofResponse(minimalProof("0xFF"));

            constructor.onStateChanged(
                    BigInteger.valueOf(255),
                    CHANNEL_ID,
                    minimalChannel(),
                    List.of(minimalMessage(1L, Bytes.fromHex("FF"))));

            assertThat(rpcClient.lastBlockTag).isEqualTo("0xff");
        }

        @Test
        void callsGetProofForCorrectContract() throws Exception {
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.setProofResponse(minimalProof("0xFF"));

            constructor.onStateChanged(
                    BigInteger.ONE, CHANNEL_ID, minimalChannel(), List.of(minimalMessage(1L, Bytes.fromHex("FF"))));

            assertThat(rpcClient.lastProofAddress).isEqualTo(CONTRACT);
        }

        @Test
        void latestCacheEntryOverwritesPrevious() throws Exception {
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.setProofResponse(minimalProof("0xFF"));

            constructor.onStateChanged(
                    BigInteger.ONE, CHANNEL_ID, minimalChannel(), List.of(minimalMessage(1L, Bytes.fromHex("FF"))));
            final byte[] first = constructor
                    .getLatestBundlePayload(CHANNEL_ID)
                    .get()
                    .toByteArray()
                    .clone();

            rpcClient.setProofResponse(minimalProof("0xCC"));
            constructor.onStateChanged(
                    BigInteger.TWO, CHANNEL_ID, minimalChannel(), List.of(minimalMessage(1L, Bytes.fromHex("CC"))));
            final byte[] second =
                    constructor.getLatestBundlePayload(CHANNEL_ID).get().toByteArray();

            assertThat(first).isNotEqualTo(second);
        }

        @Test
        void differentChannelIdsCachedIndependently() throws Exception {
            final Bytes otherId = Bytes.wrap(new byte[] {0x01});

            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.addProofResponse(minimalProof("0xFF"));
            rpcClient.addProofResponse(minimalProof("0xBB"));

            constructor.onStateChanged(
                    BigInteger.ONE, CHANNEL_ID, minimalChannel(), List.of(minimalMessage(1L, Bytes.fromHex("FF"))));
            constructor.onStateChanged(
                    BigInteger.ONE, otherId, minimalChannel(), List.of(minimalMessage(1L, Bytes.fromHex("BB"))));

            assertThat(constructor.getLatestBundlePayload(CHANNEL_ID)).isPresent();
            assertThat(constructor.getLatestBundlePayload(otherId)).isPresent();
        }

        @Test
        void remoteAheadOfCurrentEpochDoesNotCachePayload() {
            // block 1 → epoch 0; remoteTrustAnchorId = 1 → epoch 1 > current → early return
            final ClprChannel aheadChannel = ClprChannel.newBuilder()
                    .channelId(CHANNEL_ID)
                    .nextMessageId(1L)
                    .sentRunningHash(Bytes.wrap(new byte[32]))
                    .receivedMessageId(0L)
                    .receivedRunningHash(Bytes.wrap(new byte[32]))
                    .status(ClprChannelStatus.ACTIVE)
                    .remoteTrustAnchorId(encodedTrustAnchorId(1))
                    .build();

            rpcClient.setBlockResponse(minimalBlockHeader("0x"));

            constructor.onStateChanged(
                    BigInteger.ONE, CHANNEL_ID, aheadChannel, List.of(minimalMessage(1L, Bytes.fromHex("FF"))));

            assertThat(rpcClient.proofCallCount).isZero();
            assertThat(constructor.getLatestBundlePayload(CHANNEL_ID)).isEmpty();
        }

        @Test
        void unparseableRemoteTrustAnchorIdAtEpochZeroDoesNotThrow() {
            // A remoteTrustAnchorId wider than a long (e.g. read against a contract that doesn't yet
            // populate this field per the expected ABI shape) must not crash the state-change poll.
            final ClprChannel garbageAnchorChannel = ClprChannel.newBuilder()
                    .channelId(CHANNEL_ID)
                    .nextMessageId(1L)
                    .sentRunningHash(Bytes.wrap(new byte[32]))
                    .receivedMessageId(0L)
                    .receivedRunningHash(Bytes.wrap(new byte[32]))
                    .status(ClprChannelStatus.ACTIVE)
                    .remoteTrustAnchorId(Bytes.wrap(new byte[] {
                        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                        (byte) 0xFF
                    }))
                    .build();

            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.setProofResponse(minimalProof("0xFF"));

            assertThatCode(() -> constructor.onStateChanged(
                            BigInteger.ONE,
                            CHANNEL_ID,
                            garbageAnchorChannel,
                            List.of(minimalMessage(1L, Bytes.fromHex("FF")))))
                    .doesNotThrowAnyException();

            assertThat(constructor.getLatestBundlePayload(CHANNEL_ID)).isPresent();
        }

        @Test
        void unparseableRemoteTrustAnchorIdPastEpochZeroSkipsCatchUpAndKeepsMessagesFlowing() {
            // currentBlockHeader.number = 90_001 → epoch 3. A garbage remoteTrustAnchorId (the field
            // isn't populated by the currently deployed contract; see clpr-smart-contracts#192) must
            // fall back to "matches current epoch" rather than "epoch 0", so the relay doesn't treat
            // the remote as 3 epochs behind and suppress messages while chasing a catch-up that can
            // never converge (the underlying field doesn't exist on-chain yet to converge against).
            rpcClient.setBlockResponse(blockHeaderAtNumber(90_001L));
            rpcClient.setProofResponse(minimalProof("0xFF"));

            final ClprChannel garbageAnchorChannel = ClprChannel.newBuilder()
                    .channelId(CHANNEL_ID)
                    .nextMessageId(1L)
                    .sentRunningHash(Bytes.wrap(new byte[32]))
                    .receivedMessageId(0L)
                    .receivedRunningHash(Bytes.wrap(new byte[32]))
                    .status(ClprChannelStatus.ACTIVE)
                    .remoteTrustAnchorId(Bytes.wrap(new byte[] {
                        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                        (byte) 0xFF
                    }))
                    .build();

            constructor.onStateChanged(
                    BigInteger.valueOf(90_001L),
                    CHANNEL_ID,
                    garbageAnchorChannel,
                    List.of(minimalMessage(1L, Bytes.fromHex("FF"))));

            // Only the current-block fetch — no epoch-header catch-up requested.
            assertThat(rpcClient.blockCallCount).isEqualTo(1);
            assertThat(rpcClient.proofCallCount).isEqualTo(1);
            // Messages are included (6 slots: 5 channel-field slots + 1 message running-hash slot).
            assertThat(rpcClient.lastProofStorageKeys).hasSize(6);
            assertThat(constructor.getLatestBundlePayload(CHANNEL_ID)).isPresent();
        }

        @Test
        void remoteBehindByOneEpochIncludesOneEpochHeaderAndMessages() {
            // currentBlockHeader.number = 30_001 → epoch 1; remote epoch = 0 → 1 epoch header + messages
            rpcClient.setBlockResponse(blockHeaderAtNumber(30_001L));
            rpcClient.setProofResponse(minimalProof("0xFF"));

            final ClprChannel behindChannel = ClprChannel.newBuilder()
                    .channelId(CHANNEL_ID)
                    .nextMessageId(1L)
                    .sentRunningHash(Bytes.wrap(new byte[32]))
                    .receivedMessageId(0L)
                    .receivedRunningHash(Bytes.wrap(new byte[32]))
                    .status(ClprChannelStatus.ACTIVE)
                    .remoteTrustAnchorId(encodedTrustAnchorId(0))
                    .build();

            constructor.onStateChanged(
                    BigInteger.valueOf(30_001L),
                    CHANNEL_ID,
                    behindChannel,
                    List.of(minimalMessage(1L, Bytes.fromHex("FF"))));

            // 1 call for current block + 1 call for the single missing epoch header
            assertThat(rpcClient.blockCallCount).isEqualTo(2);
            assertThat(rpcClient.proofCallCount).isEqualTo(1);
            // 1 epoch header ≤ 1 → messages included → 6 storage slots
            assertThat(rpcClient.lastProofStorageKeys).hasSize(6);
            assertThat(constructor.getLatestBundlePayload(CHANNEL_ID)).isPresent();
        }

        @Test
        void remoteBehindByManyEpochsIncludesEpochHeadersButNoMessages() {
            // currentBlockHeader.number = 90_001 → epoch 3; remote epoch = 0 → 3 epoch headers, no messages
            rpcClient.setBlockResponse(blockHeaderAtNumber(90_001L));
            rpcClient.setProofResponse(minimalProof5("0xFF"));

            final ClprChannel farBehindChannel = ClprChannel.newBuilder()
                    .channelId(CHANNEL_ID)
                    .nextMessageId(1L)
                    .sentRunningHash(Bytes.wrap(new byte[32]))
                    .receivedMessageId(1L)
                    .receivedRunningHash(Bytes.wrap(new byte[32]))
                    .status(ClprChannelStatus.ACTIVE)
                    .remoteTrustAnchorId(encodedTrustAnchorId(0))
                    .build();

            constructor.onStateChanged(
                    BigInteger.valueOf(90_001L),
                    CHANNEL_ID,
                    farBehindChannel,
                    List.of(minimalMessage(1L, Bytes.fromHex("FF"))));

            // 1 call for current block + 3 calls for epoch headers (epochs 1, 2, 3)
            assertThat(rpcClient.blockCallCount).isEqualTo(4);
            assertThat(rpcClient.proofCallCount).isEqualTo(1);
            // >1 epoch headers → messages excluded → 5 channel-field slots only
            assertThat(rpcClient.lastProofStorageKeys).hasSize(5);
            assertThat(constructor.getLatestBundlePayload(CHANNEL_ID)).isPresent();
        }

        @Test
        void unsetRemoteTrustAnchorDefaultsToCurrentEpochNotZero() {
            // An unset (empty) remoteTrustAnchorId now defaults to the CURRENT epoch, not 0. A peer
            // with no anchor yet is therefore treated as already up-to-date: no epoch-header catch-up
            // is performed even at a high epoch. currentBlockHeader.number = 90_001 → epoch 3.
            rpcClient.setBlockResponse(blockHeaderAtNumber(90_001L));
            rpcClient.setProofResponse(minimalProof("0xFF"));

            final ClprChannel unsetAnchorChannel = ClprChannel.newBuilder()
                    .channelId(CHANNEL_ID)
                    .nextMessageId(1L)
                    .sentRunningHash(Bytes.wrap(new byte[32]))
                    .receivedMessageId(0L)
                    .receivedRunningHash(Bytes.wrap(new byte[32]))
                    .status(ClprChannelStatus.ACTIVE)
                    .remoteTrustAnchorId(Bytes.EMPTY)
                    .build();

            constructor.onStateChanged(
                    BigInteger.valueOf(90_001L),
                    CHANNEL_ID,
                    unsetAnchorChannel,
                    List.of(minimalMessage(1L, Bytes.fromHex("FF"))));

            // Default == current epoch → not behind → only the current block is fetched, no epoch
            // headers. Under the old default (epoch 0) this would have fetched 3 epoch headers
            // (blockCallCount == 4).
            assertThat(rpcClient.blockCallCount).isEqualTo(1);
            // No epoch headers → messages are included → 6 storage slots (4 channel fields +
            // endpointManifestVersion + the last message's running hash).
            assertThat(rpcClient.lastProofStorageKeys).hasSize(6);
            assertThat(constructor.getLatestBundlePayload(CHANNEL_ID)).isPresent();
        }

        /**
         * Reproduction guard: drive a spread of random 32-byte channel ids through the real
         * slot-derivation path ({@code onStateChanged} → {@code calculateChannelFieldStorageSlot}
         * / {@code calculateMsgRunningHashStorageSlot} → {@link EvmUtils#toSlotHex}) and assert
         * every storage key handed to {@code eth_getProof} is a well-formed 32-byte DATA value.
         * Before the fix, ~28% of channels produced at least one odd-length / short key on every
         * proof build.
         */
        @Test
        void realSlotPathAlwaysProduces64HexKeys() throws Exception {
            final Random random = new Random(0xBADC0DEL);
            for (int i = 0; i < 64; i++) {
                final byte[] idBytes = new byte[32];
                random.nextBytes(idBytes);
                final Bytes channelId = Bytes.wrap(idBytes);
                final ClprChannel conn = ClprChannel.newBuilder()
                        .channelId(channelId)
                        .nextMessageId(1L)
                        .sentRunningHash(Bytes.wrap(new byte[32]))
                        .receivedMessageId(0L)
                        .receivedRunningHash(Bytes.wrap(new byte[32]))
                        .status(ClprChannelStatus.ACTIVE)
                        .remoteTrustAnchorId(encodedTrustAnchorId(0))
                        .build();

                rpcClient.setBlockResponse(minimalBlockHeader("0x"));
                rpcClient.setProofResponse(minimalProof("0xFF"));
                constructor.onStateChanged(
                        BigInteger.ONE, channelId, conn, List.of(minimalMessage(1L, Bytes.fromHex("FF"))));

                assertThat(rpcClient.lastProofStorageKeys).hasSize(6);
                for (final String key : rpcClient.lastProofStorageKeys) {
                    assertThat(key).matches("0x[0-9a-f]{64}");
                }
            }
        }
    }

    // ── getLatestBundlePayload ────────────────────────────────────────────────

    @Nested
    class GetLatestBundlePayload {

        @Test
        void emptyBeforeAnyCall() {
            assertThat(constructor.getLatestBundlePayload(CHANNEL_ID)).isEmpty();
        }

        @Test
        void emptyForUnknownChannelId() throws Exception {
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.setProofResponse(minimalProof("0x00"));

            constructor.onStateChanged(
                    BigInteger.ONE, CHANNEL_ID, minimalChannel(), List.of(minimalMessage(1L, Bytes.fromHex("FF"))));

            assertThat(constructor.getLatestBundlePayload(Bytes.wrap(new byte[] {(byte) 0xFF})))
                    .isEmpty();
        }
    }

    // ── manifest proof branch ──────────────────────────────────────────────────

    /** Minimal {@link ContractStateReader} that only supplies a local endpoint manifest. */
    private static final class StubManifestReader implements ContractStateReader {
        private final ClprEndpointManifest manifest;

        StubManifestReader(final ClprEndpointManifest manifest) {
            this.manifest = manifest;
        }

        @Override
        public ClprEndpointManifest readEndpointManifest(final String blockTag) {
            return manifest;
        }

        @Override
        public Optional<ClprChannel> readChannelState(final Bytes id, final String blockTag) {
            return Optional.empty();
        }

        @Override
        public List<QueuedMessage> readQueuedMessages(
                final Bytes id, final long fromId, final long toId, final String blockTag) {
            return List.of();
        }

        @Override
        public ClprLedgerConfiguration readLedgerConfiguration(final CommitmentLevel commitmentLevel) {
            return ClprLedgerConfiguration.DEFAULT;
        }
    }

    /**
     * A channel with an advanced ack (so a bundle is actually built — mirrors
     * {@link #channelWithReceivedMessage()}) plus a positive endpoint-manifest version.
     */
    private static ClprChannel channelWithManifestVersion(final long version) {
        return ClprChannel.newBuilder()
                .channelId(CHANNEL_ID)
                .nextMessageId(2L)
                .sentRunningHash(Bytes.fromHex("ff"))
                .receivedMessageId(1L)
                .receivedRunningHash(Bytes.fromHex("aa"))
                .status(ClprChannelStatus.ACTIVE)
                .remoteTrustAnchorId(Bytes.fromHex("00"))
                .endpointManifestVersion(version)
                .build();
    }

    @Test
    void manifestVersionPositive_buildsSevenElementBundleWithManifestProof() throws Exception {
        final ClprEndpointManifest manifest = ClprEndpointManifest.newBuilder()
                .version(1L)
                .serviceAddress(Bytes.wrap(new byte[20]))
                .build();
        final QbftBundleConstructor withReader = new QbftBundleConstructor(
                Address.fromHexString(CONTRACT), 30_000L, 10, 10, rpcClient, new StubManifestReader(manifest));

        rpcClient.setBlockResponse(minimalBlockHeader("0x"));
        rpcClient.setProofResponse(minimalProof5("0xCAFE")); // channel-slots proof
        rpcClient.addProofResponse(minimalProof5("0xBEEF")); // manifest-commitment-slot proof

        withReader.onStateChanged(BigInteger.ONE, CHANNEL_ID, channelWithManifestVersion(1L), List.of());

        // Two ethGetProof calls: channel slots + the separate manifest commitment slot.
        assertThat(rpcClient.proofCallCount).isEqualTo(2);
        // The manifest proof (last call) requests exactly the endpoint-manifest commitment slot (18).
        assertThat(rpcClient.lastProofStorageKeys).hasSize(1);
        assertThat(new BigInteger(rpcClient.lastProofStorageKeys[0].substring(2), 16))
                .isEqualTo(BigInteger.valueOf(18));

        // The cached payload is the 7-element QBFT tuple (5 base elements + manifest proof + preimage).
        final Optional<Bytes> payload = withReader.getLatestBundlePayload(CHANNEL_ID);
        assertThat(payload).isPresent();
        assertThat(RlpReader.splitList(payload.get())).hasSize(7);
    }

    @Test
    void manifestVersionPositiveButNoStateReader_buildsFiveElementBundle() throws Exception {
        rpcClient.setBlockResponse(minimalBlockHeader("0x"));
        rpcClient.setProofResponse(minimalProof5("0xCAFE"));

        // `constructor` (from setUp) has a null stateReader → the manifest branch is skipped even
        // though endpointManifestVersion > 0.
        constructor.onStateChanged(BigInteger.ONE, CHANNEL_ID, channelWithManifestVersion(1L), List.of());

        assertThat(rpcClient.proofCallCount).isEqualTo(1);
        final Optional<Bytes> payload = constructor.getLatestBundlePayload(CHANNEL_ID);
        assertThat(payload).isPresent();
        assertThat(RlpReader.splitList(payload.get())).hasSize(5);
    }

    @Test
    void peerAlreadyCurrent_skipsManifestProof_buildsFiveElementBundle() throws Exception {
        final ClprEndpointManifest manifest = ClprEndpointManifest.newBuilder()
                .version(1L)
                .serviceAddress(Bytes.wrap(new byte[20]))
                .build();
        // The peer has already reported holding version 1 of our manifest, so 1 > 1 is false and the
        // manifest proof must be skipped (spec §4.2 Step 1b — the peer would silently drop a re-send).
        final var peerVersions = new PeerManifestVersionCache();
        peerVersions.record(CHANNEL_ID, 1L);
        final QbftBundleConstructor withReader = new QbftBundleConstructor(
                Address.fromHexString(CONTRACT),
                30_000L,
                10,
                10,
                rpcClient,
                new StubManifestReader(manifest),
                peerVersions);

        rpcClient.setBlockResponse(minimalBlockHeader("0x"));
        rpcClient.setProofResponse(minimalProof5("0xCAFE"));

        withReader.onStateChanged(BigInteger.ONE, CHANNEL_ID, channelWithManifestVersion(1L), List.of());

        // Only the channel-slots proof — no separate manifest-commitment proof.
        assertThat(rpcClient.proofCallCount).isEqualTo(1);
        final Optional<Bytes> payload = withReader.getLatestBundlePayload(CHANNEL_ID);
        assertThat(payload).isPresent();
        assertThat(RlpReader.splitList(payload.get())).hasSize(5);
    }

    @Test
    void emptyServiceAddressManifest_skipsManifestProof_buildsFiveElementBundle() throws Exception {
        // A version >= 1 manifest whose serviceAddress is still empty (freshly initialized ledger with
        // no admitted endpoint yet) must not be proven — _verifyEndpointManifest would reject it on-chain
        // with ManifestServiceAddressMismatch and stall delivery.
        final ClprEndpointManifest manifest =
                ClprEndpointManifest.newBuilder().version(1L).build();
        final QbftBundleConstructor withReader = new QbftBundleConstructor(
                Address.fromHexString(CONTRACT), 30_000L, 10, 10, rpcClient, new StubManifestReader(manifest));

        rpcClient.setBlockResponse(minimalBlockHeader("0x"));
        rpcClient.setProofResponse(minimalProof5("0xCAFE"));

        withReader.onStateChanged(BigInteger.ONE, CHANNEL_ID, channelWithManifestVersion(1L), List.of());

        assertThat(rpcClient.proofCallCount).isEqualTo(1);
        final Optional<Bytes> payload = withReader.getLatestBundlePayload(CHANNEL_ID);
        assertThat(payload).isPresent();
        assertThat(RlpReader.splitList(payload.get())).hasSize(5);
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
        protected BundleConstructor newConstructor(
                final ContractStateReader manifestReader, final PeerManifestVersionCache peerVersions) {
            rpcClient.setBlockResponse(minimalBlockHeader("0x"));
            rpcClient.setProofResponse(minimalProof5("0xCAFE")); // channel-slots proof
            rpcClient.addProofResponse(
                    minimalProof5("0xBEEF")); // manifest-commitment-slot proof (used only when attaching)
            return new QbftBundleConstructor(
                    Address.fromHexString(CONTRACT), 30_000L, 10, 10, rpcClient, manifestReader, peerVersions);
        }

        @Override
        protected void driveOneBundle(final BundleConstructor constructor) {
            constructor.onStateChanged(BigInteger.ONE, CONN, channelWithManifestVersion(1L), List.of());
        }

        @Override
        protected boolean manifestAttached(final Bytes payload) {
            return RlpReader.splitList(payload).size() == 7;
        }
    }
}
