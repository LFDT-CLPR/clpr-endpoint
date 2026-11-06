// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.hapi.node.state.clpr.ClprBundleContent;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprEndpointManifest;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprMessageValue;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import org.hiero.clpr.relay.core.BundleConstructor;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.evm.beacon.BeaconApiClient;
import org.hiero.clpr.relay.evm.beacon.BeaconBlockHeader;
import org.hiero.clpr.relay.evm.beacon.ExecutionPayloadHeader;
import org.hiero.clpr.relay.evm.beacon.LightClientBootstrap;
import org.hiero.clpr.relay.evm.beacon.LightClientHeader;
import org.hiero.clpr.relay.evm.beacon.LightClientUpdate;
import org.hiero.clpr.relay.evm.beacon.SyncAggregate;
import org.hiero.clpr.relay.evm.beacon.SyncCommittee;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.model.ProofResponse;
import org.hiero.clpr.relay.evm.ssz.Ssz;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class EthereumPosBundleConstructorTest {

    private static final String CONTRACT = "0x" + "cc".repeat(20);
    private static final Bytes CONNECTION_ID = Bytes.wrap(new byte[32]);
    // BLS12-381 G1 generator point, ZCash compressed (valid for decompressG1ToEip2537)
    private static final Bytes G1_COMPRESSED = Bytes.wrap(HexFormat.of()
            .parseHex("97f1d3a73197d7942695638c4fa9ac0f"
                    + "c3688c4f9774b905a14e3a3f171bac58"
                    + "6c55e83ff97a1aeffb3af00adb22c6bb"));

    private static Bytes det(final int len, final int seed) {
        final byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[i] = (byte) (seed + i * 31);
        }
        return Bytes.wrap(b);
    }

    private static ExecutionPayloadHeader sampleEph() {
        return new ExecutionPayloadHeader(
                det(32, 0x01),
                det(20, 0x02),
                det(32, 0x03),
                det(32, 0x04),
                det(256, 0x05),
                det(32, 0x06),
                123_456L,
                30_000_000L,
                21_000L,
                1_700_000_000L,
                det(12, 0x07),
                BigInteger.valueOf(1_000_000_007L),
                det(32, 0x08),
                det(32, 0x09),
                det(32, 0x0A),
                262_144L,
                131_072L);
    }

    /** A self-consistent finality update whose depth-9 branch folds the exec state root to bodyRoot. */
    private static LightClientUpdate selfConsistentUpdate() {
        final ExecutionPayloadHeader eph = sampleEph();
        final List<Bytes> executionBranch = List.of(det(32, 0x21), det(32, 0x22), det(32, 0x23), det(32, 0x24));
        final byte[][] branch4 = {
            executionBranch.get(0).toByteArray(), executionBranch.get(1).toByteArray(),
            executionBranch.get(2).toByteArray(), executionBranch.get(3).toByteArray()
        };
        final byte[] bodyRoot = Ssz.branchRoot(eph.hashTreeRoot().toByteArray(), branch4, 9);
        final BeaconBlockHeader beacon =
                new BeaconBlockHeader(7_000_000L, 11L, det(32, 0x31), det(32, 0x32), Bytes.wrap(bodyRoot));
        final LightClientHeader header = new LightClientHeader(beacon, eph, executionBranch);
        final SyncAggregate aggregate = new SyncAggregate(det(64, 0x40), det(96, 0x50));
        return new LightClientUpdate(header, aggregate, 7_000_001L, null, null);
    }

    private static final class StubBeaconClient extends BeaconApiClient {
        private final LightClientUpdate update;

        StubBeaconClient(final LightClientUpdate update) {
            super("http://localhost:5052", 0, Duration.ofSeconds(1));
            this.update = update;
        }

        @Override
        public LightClientUpdate getFinalityUpdate() {
            return update;
        }

        @Override
        public java.util.List<LightClientUpdate> getUpdates(final long startPeriod, final int count) {
            return java.util.List.of(update);
        }

        @Override
        public String getFinalizedBlockRootHex() {
            return "0x" + "ab".repeat(32);
        }

        @Override
        public LightClientBootstrap getBootstrap(final String blockRootHex) {
            return new LightClientBootstrap(update.attestedHeader(), sampleCommittee(), java.util.List.of(det(32, 1)));
        }

        @Override
        public long getSlotsPerSyncCommitteePeriod() {
            return PERIOD_SLOTS;
        }
    }

    private static final class StubRpcClient extends EvmJsonRpcClient {
        StubRpcClient() {
            super("http://localhost:8545", 0, Duration.ofSeconds(1));
        }

        @Override
        public ProofResponse ethGetProof(final String address, final String[] storageKeys, final String blockTag) {
            final List<Bytes> accountProof =
                    List.of(ByteUtils.fromPrefixedHex("0xf8518080"), ByteUtils.fromPrefixedHex("0xf85180"));
            final var entries = new java.util.ArrayList<ProofResponse.StorageProofEntry>();
            for (final String key : storageKeys) {
                entries.add(new ProofResponse.StorageProofEntry(
                        ByteUtils.fromPrefixedHex(key),
                        ByteUtils.fromPrefixedHex("0xff"),
                        List.of(ByteUtils.fromPrefixedHex("0xf8518080"), ByteUtils.fromPrefixedHex("0xe2a0"))));
            }
            return new ProofResponse(accountProof, List.copyOf(entries));
        }
    }

    private static ClprChannel channel() {
        return ClprChannel.newBuilder()
                .channelId(CONNECTION_ID)
                .nextMessageId(2L)
                .sentRunningHash(Bytes.wrap(new byte[32]))
                .receivedMessageId(1L)
                .receivedRunningHash(Bytes.wrap(new byte[32]))
                .status(ClprChannelStatus.ACTIVE)
                .trustAnchorId(Bytes.fromHex("00"))
                .build();
    }

    private static ContractStateReader.QueuedMessage message(final long id) {
        final ClprMessageValue value = ClprMessageValue.newBuilder()
                .payload(ClprMessagePayload.newBuilder().build())
                .runningHashAfterProcessing(Bytes.wrap(new byte[32]))
                .build();
        return new ContractStateReader.QueuedMessage(BigInteger.valueOf(id), value);
    }

    /** A state reader returning the fixed channel + queued messages {@code createBundle} reads. */
    private static final class StubStateReader implements ContractStateReader {
        private Optional<ClprChannel> channelState = Optional.of(channel());
        private List<QueuedMessage> messages = List.of(message(1));

        @Override
        public Optional<ClprChannel> readChannelState(final Bytes id, final String blockTag) {
            return channelState;
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
            return ClprEndpointManifest.DEFAULT;
        }
    }

    /** A request from {@code fromMessageId} carrying the given remote-anchor period + peer manifest version. */
    private static BundleRequest request(
            final long currentReceivedMessageId, final long remotePeriod, final long manifestVersion) {
        return BundleRequest.newBuilder()
                .currentReceivedMessageId(currentReceivedMessageId)
                .currentStatus(ClprChannelStatus.ACTIVE)
                .currentTrustAnchorId(encodedPeriod(remotePeriod))
                .currentEndpointManifestVersion(manifestVersion)
                .build();
    }

    /** The sync-committee period as an 8-byte big-endian trust-anchor id (see {@code remotePeriodFromRequest}). */
    private static Bytes encodedPeriod(final long period) {
        return Bytes.wrap(ByteBuffer.allocate(Long.BYTES).putLong(period).array());
    }

    private static final long PERIOD_SLOTS = 32L * 256L;

    /** The sync-committee period the sample updates (slot 7_000_000) sit in: 7_000_000 / 8192. */
    private static final long SAMPLE_PERIOD = 7_000_000L / PERIOD_SLOTS;

    private static boolean isRlpList(final Bytes item) {
        return item.length() > 0 && (item.getByte(0) & 0xff) >= 0xc0;
    }

    private static SyncCommittee sampleCommittee() {
        final List<Bytes> pubkeys = new java.util.ArrayList<>(512);
        for (int i = 0; i < 512; i++) {
            pubkeys.add(G1_COMPRESSED);
        }
        return new SyncCommittee(pubkeys, det(48, 0x77));
    }

    /** Self-consistent update at {@code slot} that also carries a next_sync_committee + depth-6 branch. */
    private static LightClientUpdate rotatingUpdate(final long slot) {
        final ExecutionPayloadHeader eph = sampleEph();
        final List<Bytes> executionBranch = List.of(det(32, 0x21), det(32, 0x22), det(32, 0x23), det(32, 0x24));
        final byte[][] branch4 = {
            executionBranch.get(0).toByteArray(), executionBranch.get(1).toByteArray(),
            executionBranch.get(2).toByteArray(), executionBranch.get(3).toByteArray()
        };
        final byte[] bodyRoot = Ssz.branchRoot(eph.hashTreeRoot().toByteArray(), branch4, 9);
        final BeaconBlockHeader beacon =
                new BeaconBlockHeader(slot, 11L, det(32, 0x31), det(32, 0x32), Bytes.wrap(bodyRoot));
        final LightClientHeader header = new LightClientHeader(beacon, eph, executionBranch);
        final SyncAggregate aggregate = new SyncAggregate(det(64, 0x40), det(96, 0x50));
        final List<Bytes> nextBranch = List.of(det(32, 1), det(32, 2), det(32, 3), det(32, 4), det(32, 5), det(32, 6));
        return new LightClientUpdate(header, aggregate, slot + 1, sampleCommittee(), nextBranch);
    }

    private static final class MutableStubBeaconClient extends BeaconApiClient {
        LightClientUpdate finalityUpdate;
        private final LightClientUpdate periodUpdate;

        MutableStubBeaconClient(final LightClientUpdate periodUpdate) {
            super("http://localhost:5052", 0, Duration.ofSeconds(1));
            this.periodUpdate = periodUpdate;
            this.finalityUpdate = periodUpdate;
        }

        @Override
        public LightClientUpdate getFinalityUpdate() {
            return finalityUpdate;
        }

        @Override
        public List<LightClientUpdate> getUpdates(final long startPeriod, final int count) {
            return List.of(periodUpdate);
        }

        @Override
        public String getFinalizedBlockRootHex() {
            return "0x" + "ab".repeat(32);
        }

        @Override
        public LightClientBootstrap getBootstrap(final String blockRootHex) {
            return new LightClientBootstrap(periodUpdate.attestedHeader(), sampleCommittee(), List.of(det(32, 1)));
        }

        @Override
        public long getSlotsPerSyncCommitteePeriod() {
            return PERIOD_SLOTS;
        }
    }

    @Test
    void populatesRotationOnceChainAdvancesPastTheAnchorPeriod() {
        // The anchor-period update (period 854) carries the next committee for period 855.
        final var anchorUpdate = rotatingUpdate(854 * PERIOD_SLOTS);
        final var stub = new MutableStubBeaconClient(anchorUpdate);
        final var constructor = new EthereumPosBundleConstructor(
                Address.fromHexString(CONTRACT),
                10,
                new StubRpcClient(),
                stub,
                new StubStateReader(),
                CommitmentLevel.LATEST);

        // Cycle 1: chain still in period 854, anchor (from the request) at 854 → no rotation (items 4/5 absent).
        stub.finalityUpdate = rotatingUpdate(854 * PERIOD_SLOTS);
        final List<Bytes> steady = RlpReader.splitList(
                constructor.createBundle(CONNECTION_ID, request(0L, 854L, 0L)).bundlePayload());
        assertThat(isRlpList(steady.get(4)))
                .as("nextCommittee absent in steady state")
                .isFalse();
        assertThat(RlpReader.splitList(steady.get(5)))
                .as("nextCommitteeBranch empty")
                .isEmpty();

        // Cycle 2: chain advanced to period 855, anchor still 854 → rotation due; items 4/5 now populated.
        stub.finalityUpdate = rotatingUpdate(855 * PERIOD_SLOTS);
        final List<Bytes> rotated = RlpReader.splitList(
                constructor.createBundle(CONNECTION_ID, request(0L, 854L, 0L)).bundlePayload());
        assertThat(isRlpList(rotated.get(4)))
                .as("nextCommittee present after boundary")
                .isTrue();
        assertThat(RlpReader.splitList(rotated.get(4)))
                .as("[pubkeys[], aggregate]")
                .hasSize(2);
        assertThat(RlpReader.splitList(rotated.get(5)))
                .as("depth-6 committee branch")
                .hasSize(6);
    }

    @Test
    void buildsTenItemBundleWhoseBranchFoldsToBodyRoot() {
        final LightClientUpdate update = selfConsistentUpdate();
        final var constructor = new EthereumPosBundleConstructor(
                Address.fromHexString(CONTRACT),
                10,
                new StubRpcClient(),
                new StubBeaconClient(update),
                new StubStateReader(),
                CommitmentLevel.LATEST);

        final Bytes bundle = constructor
                .createBundle(CONNECTION_ID, request(0L, SAMPLE_PERIOD, 0L))
                .bundlePayload();
        final List<Bytes> items = RlpReader.splitList(bundle);
        assertThat(items).hasSize(10);

        // attestedHeader (item 0): 5 fields; bodyRoot is field 4.
        final List<Bytes> header = RlpReader.splitList(items.get(0));
        assertThat(header).hasSize(5);
        final byte[] bodyRoot = RlpReader.bytesValue(header.get(4)).toByteArray();

        // executionStateRoot (item 2) + executionBranch (item 3, 9 nodes) must fold to bodyRoot at index 290.
        final byte[] stateRoot = RlpReader.bytesValue(items.get(2)).toByteArray();
        final List<Bytes> branchItems = RlpReader.splitList(items.get(3));
        assertThat(branchItems).hasSize(Ssz.EXECUTION_STATE_ROOT_BRANCH_DEPTH);
        final byte[][] branch = new byte[branchItems.size()][];
        for (int i = 0; i < branchItems.size(); i++) {
            branch[i] = RlpReader.bytesValue(branchItems.get(i)).toByteArray();
        }
        assertThat(Ssz.verifyBranch(stateRoot, branch, bodyRoot, Ssz.EXECUTION_STATE_ROOT_LEAF_INDEX))
                .as("emitted depth-9 branch must fold the execution state root to the attested bodyRoot")
                .isTrue();

        // storageProof (item 7): 5 fixed channel-metadata entries + 1 for the included message's
        // running hash, each [key, proofNodes] (2 fields, no value).
        final List<Bytes> storageEntries = RlpReader.splitList(items.get(7));
        assertThat(storageEntries).hasSize(6);
        for (final Bytes entry : storageEntries) {
            assertThat(RlpReader.splitList(entry)).hasSize(2);
        }

        // bundleContent (item 8) decodes and carries the one message.
        final ClprBundleContent content = assertParsesBundleContent(RlpReader.bytesValue(items.get(8)));
        assertThat(content.messages()).hasSize(1);

        // nextCommittee (item 4) absent = empty string; nextCommitteeBranch (item 5) absent = empty list.
        assertThat(RlpReader.bytesValue(items.get(4)).length()).isZero();
        assertThat(RlpReader.splitList(items.get(5))).isEmpty();
    }

    private static ClprBundleContent assertParsesBundleContent(final Bytes bytes) {
        try {
            return ClprBundleContent.PROTOBUF.parse(bytes);
        } catch (final Exception e) {
            throw new AssertionError("bundle content did not parse", e);
        }
    }

    /**
     * {@link BundleConstructorManifestContract} for the Ethereum-PoS format: the manifest proof rides
     * in the optional trailing [10]/[11] items of the RLP bundle (present iff both are non-null).
     */
    @Nested
    class ManifestAttachmentContract extends BundleConstructorManifestContract {
        @Override
        protected BundleConstructor newConstructor(final ContractStateReader stateReader) {
            return new EthereumPosBundleConstructor(
                    Address.fromHexString(CONTRACT),
                    10,
                    new StubRpcClient(),
                    new StubBeaconClient(selfConsistentUpdate()),
                    stateReader,
                    CommitmentLevel.LATEST);
        }

        @Override
        protected Bytes driveOneBundle(final BundleConstructor constructor, final long peerKnownVersion) {
            // The base contract's reader supplies an ack-only channel (receivedMessageId > 0, no pending
            // messages), so a bundle is produced whose only optional element is the manifest proof.
            return constructor
                    .createBundle(CONN, request(0L, SAMPLE_PERIOD, peerKnownVersion))
                    .bundlePayload();
        }

        @Override
        protected boolean manifestAttached(final Bytes payload) {
            // 10 items steady-state, 12 when the trailing manifest proof + preimage are attached.
            return RlpReader.splitList(payload).size() > 10;
        }
    }
}
