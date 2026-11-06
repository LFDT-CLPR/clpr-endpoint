// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.qbft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.clpr.relay.evm.ByteUtils.ZERO_HASH;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.ParsedBundle;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.ByteUtils;
import org.hiero.clpr.relay.evm.RlpReader;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.model.BlockHeader;
import org.hiero.clpr.relay.evm.model.EvmStorageMerkleProof;
import org.hiero.clpr.relay.evm.model.ProofResponse;
import org.hiero.clpr.relay.evm.qbft.ValidatorContractProofSource.ContractProof;
import org.hiero.clpr.relay.evm.qbft.ValidatorContractProofSource.ProvenValidatorSet;
import org.hiero.clpr.relay.evm.qbft.ValidatorContractProofSource.ValidatorSet;
import org.hiero.clpr.relay.evm.testfixtures.TestEvmJsonRpcClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QbftContractModeBundleConstructorTest {

    /** Models on-chain validator-array state: the set effective at or after each registered block. */
    private static class FakeValidatorSource implements ValidatorContractProofSource {
        private final NavigableMap<Long, List<Address>> setsByEffectiveBlock = new TreeMap<>();
        int callCount;

        void setEffectiveFrom(final long blockNumber, final List<Address> members) {
            setsByEffectiveBlock.put(blockNumber, members);
        }

        @Override
        public ProvenValidatorSet proveValidatorSet(final BigInteger blockNumber) {
            callCount++;
            final var floor = setsByEffectiveBlock.floorEntry(blockNumber.longValueExact());
            if (floor == null) {
                throw new IllegalStateException("no validator set known at or before block " + blockNumber);
            }
            final var proof = new ContractProof(
                    Address.fromHexString("0x" + "ee".repeat(20)),
                    List.of(ByteUtils.fromPrefixedHex("0xf8518080")),
                    Bytes.wrap(AbiCodec.encodeUint256(BigInteger.TWO)),
                    List.of(new EvmStorageMerkleProof(
                            ByteUtils.leftPad32(ByteUtils.fromPrefixedHex("0x02")),
                            List.of(ByteUtils.fromPrefixedHex("0xf8518080")))));
            return new ProvenValidatorSet(new ValidatorSet(floor.getValue()), proof);
        }
    }

    private static class StubRpcClient extends TestEvmJsonRpcClient {
        /** The block number a symbolic tag (the commitment-level head read) resolves to. */
        long head;

        int proofCallCount;
        String[] lastProofStorageKeys;

        @Override
        public BlockHeader ethGetBlockHeaderByNumber(final String blockTag) {
            return blockHeaderAtNumber(blockTag.startsWith("0x") ? Long.decode(blockTag) : head);
        }

        @Override
        public ProofResponse ethGetProof(final String address, final String[] storageKeys, final String blockTag) {
            proofCallCount++;
            lastProofStorageKeys = storageKeys;
            return channelProof(storageKeys.length);
        }

        private static BlockHeader blockHeaderAtNumber(final long number) {
            return new BlockHeader(
                    ZERO_HASH,
                    ZERO_HASH,
                    Address.ZERO,
                    ZERO_HASH,
                    ZERO_HASH,
                    ZERO_HASH,
                    ZERO_HASH,
                    BigInteger.ZERO,
                    BigInteger.valueOf(number),
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

        private static ProofResponse channelProof(final int slotCount) {
            final var entries = new ArrayList<ProofResponse.StorageProofEntry>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                entries.add(new ProofResponse.StorageProofEntry(
                        Bytes.fromHex(String.format("%02x", i)),
                        ByteUtils.fromPrefixedHex("0xCAFE"),
                        List.of(ByteUtils.fromPrefixedHex("0xf8518080"))));
            }
            return new ProofResponse(List.of(ByteUtils.fromPrefixedHex("0xf8518080")), entries);
        }
    }

    /**
     * A {@link ContractStateReader} returning a fixed channel and no queued messages; the local endpoint
     * manifest is the interface default (empty), so no manifest proof is ever attached.
     */
    private static final class StubStateReader implements ContractStateReader {
        Optional<ClprChannel> channel = Optional.empty();

        @Override
        public Optional<ClprChannel> readChannelState(final Bytes id, final String blockTag) {
            return channel;
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

    static final Bytes CHANNEL_ID = Bytes.wrap(new byte[32]);
    static final Address ADDR_A = Address.fromHexString("0x" + "aa".repeat(20));
    static final Address ADDR_B = Address.fromHexString("0x" + "bb".repeat(20));

    /** A distinct, deterministic single-byte-repeated address, for scenarios needing several. */
    private static Address addr(final int n) {
        return Address.fromHexString("0x" + String.format("%02x", n).repeat(20));
    }

    StubRpcClient rpcClient;
    StubStateReader stateReader;
    FakeValidatorSource validatorSource;
    QbftContractModeBundleConstructor constructor;

    @BeforeEach
    void setUp() {
        rpcClient = new StubRpcClient();
        stateReader = new StubStateReader();
        validatorSource = new FakeValidatorSource();
        constructor = new QbftContractModeBundleConstructor(
                Address.fromHexString("0x" + "cc".repeat(20)),
                validatorSource,
                10,
                rpcClient,
                stateReader,
                CommitmentLevel.LATEST);
    }

    private static Bytes encodedTrustAnchorId(final long blockNumber) {
        return Bytes.wrap(ByteBuffer.allocate(Long.BYTES).putLong(blockNumber).array());
    }

    /** An ACTIVE channel with nothing queued but an inbound message to acknowledge. */
    private static ClprChannel ackingChannel() {
        return ClprChannel.newBuilder()
                .channelId(CHANNEL_ID)
                .nextMessageId(1L)
                .sentRunningHash(Bytes.wrap(new byte[32]))
                .receivedMessageId(1L)
                .receivedRunningHash(Bytes.wrap(new byte[32]))
                .status(ClprChannelStatus.ACTIVE)
                .build();
    }

    /**
     * Builds a bundle for {@link #ackingChannel()} with the local head at {@code head}, for a peer whose
     * {@link BundleRequest} reports {@code remoteTrustAnchorId} as its current trust anchor. Returns
     * {@link Bytes#EMPTY} when the constructor has nothing to send.
     */
    private Bytes bundleAt(final long head, final Bytes remoteTrustAnchorId) {
        return bundleAt(head, ackingChannel(), remoteTrustAnchorId);
    }

    private Bytes bundleAt(final long head, final ClprChannel channel, final Bytes remoteTrustAnchorId) {
        rpcClient.head = head;
        stateReader.channel = Optional.of(channel);
        return constructor
                .createBundle(CHANNEL_ID, request(remoteTrustAnchorId))
                .bundlePayload();
    }

    private static BundleRequest request(final Bytes remoteTrustAnchorId) {
        return BundleRequest.newBuilder()
                .currentReceivedMessageId(0L)
                .currentTrustAnchorId(remoteTrustAnchorId)
                .currentEndpointManifestVersion(0L)
                .build();
    }

    // ── RLP field decoding helpers ──────────────────────────────────────────────

    private static List<Bytes> validatorSourceItems(final Bytes proofBytes) {
        final var outer = RlpReader.splitList(proofBytes);
        return RlpReader.splitList(outer.get(1));
    }

    private static List<Bytes> hopItems(final Bytes proofBytes) {
        return RlpReader.splitList(validatorSourceItems(proofBytes).get(1));
    }

    private static int hopCount(final Bytes proofBytes) {
        return hopItems(proofBytes).size();
    }

    private static List<Address> trustedSet(final Bytes proofBytes) {
        final var trustedSetItems =
                RlpReader.splitList(validatorSourceItems(proofBytes).get(0));
        return trustedSetItems.stream()
                .map(item -> new Address(RlpReader.bytesValue(item).toByteArray()))
                .toList();
    }

    /** Asserts {@code wrapped} is an RLP byte string whose payload is a 4-item list. */
    private static void assertWrappedContractProofShape(final Bytes wrapped) {
        final Bytes inner = RlpReader.bytesValue(wrapped);
        assertThat(RlpReader.splitList(inner)).hasSize(4);
    }

    private static long headerBlockNumber(final Bytes headerRaw) {
        final var fields = RlpReader.splitList(headerRaw);
        return new BigInteger(1, RlpReader.bytesValue(fields.get(8)).toByteArray()).longValueExact();
    }

    private static long hopBlockNumber(final Bytes proofBytes, final int hopIndex) {
        final var hop = RlpReader.splitList(hopItems(proofBytes).get(hopIndex));
        return headerBlockNumber(hop.get(0));
    }

    // ── tests ────────────────────────────────────────────────────────────────

    @Test
    void anchorEqualsCurrentBlockIsSkippedEntirely() {
        validatorSource.setEffectiveFrom(0, List.of(ADDR_A));

        final Bytes payload = bundleAt(100, encodedTrustAnchorId(100));

        // The verifier requires the current block strictly after the anchor block
        // (CurrentHeaderBehindWalk otherwise), so an anchor == current bundle must never be built.
        assertThat(payload).isEqualTo(Bytes.EMPTY);
        assertThat(validatorSource.callCount).isZero();
        assertThat(rpcClient.proofCallCount).isZero();
    }

    @Test
    void noHopsWhenValidatorSetUnchangedAcrossRange() {
        validatorSource.setEffectiveFrom(0, List.of(ADDR_A));

        final Bytes payload = bundleAt(100, encodedTrustAnchorId(0));

        assertThat(payload).isNotEqualTo(Bytes.EMPTY);
        assertThat(hopCount(payload)).isZero();
        assertThat(trustedSet(payload)).containsExactly(ADDR_A);
    }

    @Test
    void findsExactRotationBlockViaBisection() {
        validatorSource.setEffectiveFrom(0, List.of(ADDR_A));
        validatorSource.setEffectiveFrom(50, List.of(ADDR_B));

        final Bytes payload = bundleAt(100, encodedTrustAnchorId(0));

        assertThat(payload).isNotEqualTo(Bytes.EMPTY);
        assertThat(hopCount(payload)).isEqualTo(1);
        assertThat(hopBlockNumber(payload, 0)).isEqualTo(50);
        assertThat(trustedSet(payload)).containsExactly(ADDR_A);
    }

    @Test
    void rotationExactlyAtCurrentBlockProducesNoHop() {
        // The verifier's own quorum/harvest check (not this hop search) is what reacts to a change at
        // the current block itself; a hop must never name the current block (rule: a hop is always
        // strictly before it — see findHops's javadoc on the one-block lag).
        validatorSource.setEffectiveFrom(0, List.of(ADDR_A));
        validatorSource.setEffectiveFrom(100, List.of(ADDR_B));

        final Bytes payload = bundleAt(100, encodedTrustAnchorId(0));

        assertThat(payload).isNotEqualTo(Bytes.EMPTY);
        assertThat(hopCount(payload)).isZero();
    }

    @Test
    void rotationAtCurrentMinusOneProducesHopThere() {
        validatorSource.setEffectiveFrom(0, List.of(ADDR_A));
        validatorSource.setEffectiveFrom(99, List.of(ADDR_B));

        final Bytes payload = bundleAt(100, encodedTrustAnchorId(0));

        assertThat(payload).isNotEqualTo(Bytes.EMPTY);
        assertThat(hopCount(payload)).isEqualTo(1);
        assertThat(hopBlockNumber(payload, 0)).isEqualTo(99);
    }

    @Test
    void rotationUndoneAtCurrentBlockStillNeedsEarlierHop() {
        // A -> B at 50 (inside the searched range), then B -> A again exactly at current = 100 (outside
        // it, per the one-block lag): only the first rotation is a hop.
        validatorSource.setEffectiveFrom(0, List.of(ADDR_A));
        validatorSource.setEffectiveFrom(50, List.of(ADDR_B));
        validatorSource.setEffectiveFrom(100, List.of(ADDR_A));

        final Bytes payload = bundleAt(100, encodedTrustAnchorId(0));

        assertThat(payload).isNotEqualTo(Bytes.EMPTY);
        assertThat(hopCount(payload)).isEqualTo(1);
        assertThat(hopBlockNumber(payload, 0)).isEqualTo(50);
    }

    @Test
    void sixRotationsProduceSixHopsSinceThereIsNoCap() {
        // Six DISTINCT sets (not alternating A/B) so no pair of rotations can cancel out and leave the
        // range-endpoint shortcut in findFirstChangeBlock blind to the rotations between them.
        validatorSource.setEffectiveFrom(0, List.of(addr(0)));
        validatorSource.setEffectiveFrom(10, List.of(addr(1)));
        validatorSource.setEffectiveFrom(20, List.of(addr(2)));
        validatorSource.setEffectiveFrom(30, List.of(addr(3)));
        validatorSource.setEffectiveFrom(40, List.of(addr(4)));
        validatorSource.setEffectiveFrom(50, List.of(addr(5)));
        validatorSource.setEffectiveFrom(60, List.of(addr(6)));

        final Bytes payload = bundleAt(100, encodedTrustAnchorId(0));

        assertThat(payload).isNotEqualTo(Bytes.EMPTY);
        assertThat(hopCount(payload)).isEqualTo(6);
        final var hopBlocks = new ArrayList<Long>();
        for (int i = 0; i < hopCount(payload); i++) {
            hopBlocks.add(hopBlockNumber(payload, i));
        }
        assertThat(hopBlocks).containsExactly(10L, 20L, 30L, 40L, 50L, 60L);
    }

    @Test
    void undecodableAnchorFallsBackToStateBeforeCurrentBlock() {
        // A request whose trust anchor does not decode leaves the relay without a starting point, so
        // it must assume steady state — the set behind the (unknown) anchor is whatever set seals the
        // block right before the current one — and must not scan for hops.
        validatorSource.setEffectiveFrom(0, List.of(ADDR_A));
        // A rotation exactly AT current=100 changes state(100), not state(99) — the fallback must still
        // read state(current - 1), so this rotation must not show up as the trusted set or a hop. This
        // would catch a regression back to reading state(current) directly.
        validatorSource.setEffectiveFrom(100, List.of(ADDR_B));

        final Bytes payload = bundleAt(100, Bytes.EMPTY);

        assertThat(payload).isNotEqualTo(Bytes.EMPTY);
        assertThat(hopCount(payload)).isZero();
        assertThat(trustedSet(payload)).containsExactly(ADDR_A);
        // Exactly two reads: the trusted set at current - 1, and the current contract proof — the
        // empty-range hop search must not add a third.
        assertThat(validatorSource.callCount).isEqualTo(2);
    }

    @Test
    void remoteAnchorAheadOfCurrentBlockSkipsWithoutCallingValidatorSource() {
        final Bytes payload = bundleAt(10, encodedTrustAnchorId(20));

        assertThat(payload).isEqualTo(Bytes.EMPTY);
        assertThat(validatorSource.callCount).isZero();
        assertThat(rpcClient.proofCallCount).isZero();
    }

    @Test
    void activeChannelWithNothingToDeliverAndNoRotationSkipsBundle() {
        validatorSource.setEffectiveFrom(0, List.of(ADDR_A));
        final var channel = ackingChannel().copyBuilder().receivedMessageId(0L).build();

        final Bytes payload = bundleAt(1, channel, encodedTrustAnchorId(0));

        assertThat(payload).isEqualTo(Bytes.EMPTY);
        assertThat(rpcClient.proofCallCount).isZero();
    }

    @Test
    void closedPeerSkipsBundleWithoutReadingChannelState() {
        validatorSource.setEffectiveFrom(0, List.of(ADDR_A));
        rpcClient.head = 100;
        stateReader.channel = Optional.of(ackingChannel());
        final BundleRequest closedPeer = request(encodedTrustAnchorId(0))
                .copyBuilder()
                .currentStatus(ClprChannelStatus.CLOSED)
                .build();

        final var bundle = constructor.createBundle(CHANNEL_ID, closedPeer);

        assertThat(bundle.bundlePayload()).isEqualTo(Bytes.EMPTY);
        assertThat(bundle.constructedAtHeight()).isEqualTo(100L);
        assertThat(validatorSource.callCount).isZero();
    }

    @Test
    void pendingOrUnknownChannelSkipsBundle() {
        validatorSource.setEffectiveFrom(0, List.of(ADDR_A));

        final Bytes pending = bundleAt(
                100,
                ackingChannel().copyBuilder().status(ClprChannelStatus.PENDING).build(),
                encodedTrustAnchorId(0));
        stateReader.channel = Optional.empty();
        final Bytes unknown = constructor
                .createBundle(CHANNEL_ID, request(encodedTrustAnchorId(0)))
                .bundlePayload();

        assertThat(pending).isEqualTo(Bytes.EMPTY);
        assertThat(unknown).isEqualTo(Bytes.EMPTY);
        assertThat(validatorSource.callCount).isZero();
    }

    @Test
    void ackOnlyBundleProvesFiveChannelSlots() {
        validatorSource.setEffectiveFrom(0, List.of(ADDR_A));

        final Bytes payload = bundleAt(1, encodedTrustAnchorId(0));

        assertThat(payload).isNotEqualTo(Bytes.EMPTY);
        assertThat(rpcClient.lastProofStorageKeys).hasSize(5);
    }

    @Test
    void encodedProofIsAnRlpListWithWellShapedContractProofs() {
        validatorSource.setEffectiveFrom(0, List.of(ADDR_A));
        validatorSource.setEffectiveFrom(1, List.of(ADDR_B));

        final Bytes payload = bundleAt(2, encodedTrustAnchorId(0));

        assertThat(payload).isNotEqualTo(Bytes.EMPTY);
        final byte[] encoded = payload.toByteArray();
        assertThat(encoded[0] & 0xFF).isGreaterThanOrEqualTo(0xC0);
        assertThat(RlpReader.splitList(Bytes.wrap(encoded))).hasSize(5);

        // Field 1 item 2 (the current contract proof) is a byte string wrapping a 4-item list.
        final var validatorSourceItems = validatorSourceItems(payload);
        assertWrappedContractProofShape(validatorSourceItems.get(2));

        // With a rotation at block 1, there is exactly one hop, whose item 1 is wrapped the same way.
        assertThat(hopCount(payload)).isEqualTo(1);
        final var hop = RlpReader.splitList(hopItems(payload).get(0));
        assertThat(hop).hasSize(2);
        assertWrappedContractProofShape(hop.get(1));

        // Every trusted-set item is a bare 20-byte address.
        for (final Bytes item : RlpReader.splitList(validatorSourceItems.get(0))) {
            assertThat(RlpReader.bytesValue(item).length()).isEqualTo(20);
        }
    }

    @Test
    void bundleDecodesWithQbftProofCodec() {
        // Contract mode shares header mode's outer tuple (QbftVerifierBase's PAYLOAD_*_INDEX constants
        // in clpr-smart-contracts serve both verifiers); only field 1 differs, and QbftProofCodec
        // passes that through opaquely — so the receiving relay decodes it with the same codec.
        validatorSource.setEffectiveFrom(0, List.of(ADDR_A));
        validatorSource.setEffectiveFrom(50, List.of(ADDR_B));
        final ClprChannel channel = ackingChannel();

        final Bytes payload = bundleAt(100, channel, encodedTrustAnchorId(0));

        final QbftProofCodec codec = new QbftProofCodec(CHANNEL_ID);
        final ParsedBundle parsed = codec.decodeBundle(payload);
        assertThat(parsed.metadata().nextMessageId()).isEqualTo(channel.nextMessageId());
        assertThat(parsed.metadata().receivedMessageId()).isEqualTo(channel.receivedMessageId());
        assertThat(parsed.metadata().status()).isEqualTo(channel.status());
        assertThat(parsed.messages()).isEmpty();
        // Nothing to trim: the bundle is submitted byte-identical, hop included.
        assertThat(codec.parseBundle(payload, 0L).rawBundleBytes()).isEqualTo(payload);
    }
}
