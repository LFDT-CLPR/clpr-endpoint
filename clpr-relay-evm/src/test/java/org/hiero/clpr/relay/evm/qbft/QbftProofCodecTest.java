// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.qbft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.hapi.node.state.clpr.ClprBundleContent;
import com.hedera.hapi.node.state.clpr.ClprMessage;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprQueueMetadata;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.List;
import org.hiero.clpr.relay.core.ParsedBundle;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.ByteUtils;
import org.hiero.clpr.relay.evm.EvmUtils;
import org.hiero.clpr.relay.evm.RlpReader;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.model.EvmStorageMerkleProof;
import org.hiero.clpr.relay.evm.qbft.ValidatorContractProofSource.ContractProof;
import org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout;
import org.junit.jupiter.api.Test;

/** Tests {@link QbftProofCodec}'s format detection, bundle-content extraction, and message trimming. */
class QbftProofCodecTest {

    private final Bytes channelId = Bytes.wrap(new byte[32]);
    private final QbftProofCodec codec = new QbftProofCodec(channelId);

    /** Decode, drop already-received messages, and re-encode the retained suffix — the production flow. */
    private ParsedBundle trim(final Bytes proof, final long receivedMessageId) {
        return codec.parseBundle(proof, receivedMessageId);
    }

    /** Build a QBFT-shaped proof: the 5-element RLP tuple with {@code content} as the 5th element. */
    private static Bytes qbftProof(final ClprBundleContent content) {
        return qbftProof(content, AbiCodec.rlpList()); // empty storage-proof list
    }

    /** Build a QBFT-shaped proof with an explicit (already RLP-encoded) storage-proof list element. */
    private static Bytes qbftProof(final ClprBundleContent content, final byte[] storageProofsRlp) {
        final byte[] contentBytes = ClprBundleContent.PROTOBUF.toBytes(content).toByteArray();
        return Bytes.wrap(AbiCodec.rlpList(
                AbiCodec.rlpList(), // currentBlockHeader (placeholder — not parsed by the codec)
                AbiCodec.rlpList(), // epochBlockHeaders (placeholder — not parsed by the codec)
                AbiCodec.rlpList(), // clprServiceAccountProof
                storageProofsRlp, // clprServiceStorageProofs
                AbiCodec.rlpBytes(contentBytes)));
    }

    private static ClprMessagePayload msg(final String data) {
        return ClprMessagePayload.newBuilder()
                .message(ClprMessage.newBuilder()
                        .messageData(Bytes.wrap(data.getBytes()))
                        .build())
                .build();
    }

    private static ClprBundleContent content(final long nextMessageId, final List<ClprMessagePayload> messages) {
        return ClprBundleContent.newBuilder()
                .metadata(ClprQueueMetadata.newBuilder()
                        .nextMessageId(nextMessageId)
                        .build())
                .messages(messages)
                .build();
    }

    /** One storage-proof entry: {@code [key, [proofNodes...]]} with the running-hash slot of {@code messageId}. */
    private byte[] runningHashProofEntry(final long messageId) {
        final byte[] keyBytes = ByteUtils.fromPrefixedHex(
                        EvmUtils.toSlotHex(ClprServiceStorageLayout.calculateMsgRunningHashStorageSlot(
                                channelId, BigInteger.valueOf(messageId))))
                .toByteArray();
        return AbiCodec.rlpList(AbiCodec.rlpBytes(keyBytes), AbiCodec.rlpList());
    }

    private static byte[] dummyProofEntry(final int seed) {
        final byte[] key = new byte[32];
        key[31] = (byte) seed;
        return AbiCodec.rlpList(AbiCodec.rlpBytes(key), AbiCodec.rlpList());
    }

    @Test
    void parseBundleExtractsMetadataMessagesAndPassesRawProofThrough() {
        final ClprBundleContent content = content(7L, List.of(msg("hello")));
        final Bytes proof = qbftProof(content);

        final ParsedBundle parsed = codec.decodeBundle(proof);

        assertThat(parsed.metadata().nextMessageId()).isEqualTo(7L);
        assertThat(parsed.messages()).hasSize(1);
        assertThat(parsed.messages().get(0).message().messageData()).isEqualTo(Bytes.wrap("hello".getBytes()));
        assertThat(parsed.rawBundleBytes()).isEqualTo(proof);
    }

    @Test
    void rejectsTupleWithWrongElementCount() {
        final byte[] tooFew = AbiCodec.rlpList(AbiCodec.rlpList(), AbiCodec.rlpBytes(new byte[] {1}));
        assertThatThrownBy(() -> codec.decodeBundle(Bytes.wrap(tooFew)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("QBFT bundle");
    }

    @Test
    void rejectsMalformedBundleContentElement() {
        // Right tuple shape, but the 5th element is not a valid ClprBundleContent protobuf.
        final byte[] proof = AbiCodec.rlpList(
                AbiCodec.rlpList(),
                AbiCodec.rlpList(),
                AbiCodec.rlpList(),
                AbiCodec.rlpList(),
                AbiCodec.rlpBytes(new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff}));
        assertThatThrownBy(() -> codec.decodeBundle(Bytes.wrap(proof)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Failed to parse QBFT bundle proof");
    }

    /** Build a QBFT-shaped proof with manifest proof elements (7-element tuple). */
    private static Bytes qbftProofWithManifest(final ClprBundleContent content) {
        final byte[] contentBytes = ClprBundleContent.PROTOBUF.toBytes(content).toByteArray();
        return Bytes.wrap(AbiCodec.rlpList(
                AbiCodec.rlpList(), // currentBlockHeader
                AbiCodec.rlpList(), // epochBlockHeaders
                AbiCodec.rlpList(), // clprServiceAccountProof
                AbiCodec.rlpList(), // clprServiceStorageProofs
                AbiCodec.rlpBytes(contentBytes), // bundleContent (element 5)
                AbiCodec.rlpList(), // manifestStorageProof (element 6)
                AbiCodec.rlpBytes(new byte[] {0x01}) // serializedEndpointManifest (element 7)
                ));
    }

    @Test
    void acceptsSevenElementTupleWithManifestProof() {
        final ClprBundleContent content = content(3L, List.of(msg("hello")));
        final Bytes proof = qbftProofWithManifest(content);

        final ParsedBundle parsed = codec.decodeBundle(proof);

        assertThat(parsed.metadata().nextMessageId()).isEqualTo(3L);
        assertThat(parsed.messages()).hasSize(1);
        assertThat(parsed.rawBundleBytes()).isEqualTo(proof);
    }

    @Test
    void trimReturnsBundleVerbatimWhenNothingAlreadyReceived() {
        // ids [1]; receiver has received 0 — nothing to drop, identical bytes back.
        final Bytes proof = qbftProof(content(2L, List.of(msg("m1"))));

        final ParsedBundle parsed = trim(proof, 0L);

        assertThat(parsed.messages()).hasSize(1);
        assertThat(parsed.rawBundleBytes()).isEqualTo(proof);
    }

    @Test
    void trimDropsAlreadyReceivedLeadingMessages() {
        // ids [3,4,5]; receiver already has up to 4 — keep only message 5.
        final Bytes proof = qbftProof(content(6L, List.of(msg("m3"), msg("m4"), msg("m5"))));

        final ParsedBundle parsed = trim(proof, 4L);

        assertThat(parsed.messages()).hasSize(1);
        assertThat(parsed.messages().get(0).message().messageData()).isEqualTo(Bytes.wrap("m5".getBytes()));
        // nextMessageId (sender frontier) is preserved.
        assertThat(parsed.metadata().nextMessageId()).isEqualTo(6L);
        // The re-encoded bytes round-trip to the trimmed content.
        final ParsedBundle reparsed = codec.decodeBundle(parsed.rawBundleBytes());
        assertThat(reparsed.messages()).hasSize(1);
        assertThat(reparsed.messages().get(0).message().messageData()).isEqualTo(Bytes.wrap("m5".getBytes()));
    }

    @Test
    void trimToAckOnlyRemovesAllMessagesAndDropsRunningHashProof() {
        // ids [1]; receiver already has 1 — fully trimmed to an ack-only bundle. The bundle proves the
        // last message's running hash (id 1) plus two unrelated channel-field slots; only the former
        // must be dropped.
        final byte[] storage = AbiCodec.rlpList(dummyProofEntry(1), runningHashProofEntry(1L), dummyProofEntry(2));
        final Bytes proof = qbftProof(content(2L, List.of(msg("m1"))), storage);

        final ParsedBundle parsed = trim(proof, 1L);

        assertThat(parsed.messages()).isEmpty();
        // Re-encoded content has no messages but keeps the metadata (the acknowledgment).
        final ParsedBundle reparsed = codec.decodeBundle(parsed.rawBundleBytes());
        assertThat(reparsed.messages()).isEmpty();
        assertThat(reparsed.metadata().nextMessageId()).isEqualTo(2L);
        // The running-hash proof entry is gone; the two unrelated entries survive.
        final List<Bytes> storageEntries =
                RlpReader.splitList(RlpReader.splitList(parsed.rawBundleBytes()).get(3));
        assertThat(storageEntries).hasSize(2);
        final Bytes droppedKey = ByteUtils.leftPad32(ByteUtils.fromPrefixedHex(EvmUtils.toSlotHex(
                ClprServiceStorageLayout.calculateMsgRunningHashStorageSlot(channelId, BigInteger.ONE))));
        assertThat(storageEntries).noneMatch(e -> ByteUtils.leftPad32(
                        RlpReader.bytesValue(RlpReader.splitList(e).getFirst()))
                .equals(droppedKey));
    }

    // ── QBFT contract validator mode ─────────────────────────────────────────────

    /**
     * A contract-mode field 1 {@code [trustedSet, hops[], contractProof]} with one hop, its contract
     * proofs encoded by the production {@link ValidatorContractProofRlp}.
     */
    private static byte[] contractModeValidatorSource() {
        final byte[] contractProof = ValidatorContractProofRlp.encodeWrapped(new ContractProof(
                Address.fromHexString("0x" + "ee".repeat(20)),
                List.of(Bytes.wrap(new byte[] {0x01})),
                Bytes.wrap(new byte[32]),
                List.of(new EvmStorageMerkleProof(Bytes.wrap(new byte[32]), List.of(Bytes.wrap(new byte[] {0x02}))))));
        final byte[] hop = AbiCodec.rlpList(AbiCodec.rlpList() /* change-block header placeholder */, contractProof);
        return AbiCodec.rlpList(
                AbiCodec.rlpList(AbiCodec.rlpBytes(
                        Address.fromHexString("0x" + "aa".repeat(20)).value())), // trustedSet
                AbiCodec.rlpList(hop), // hops
                contractProof); // contract proof at the current block
    }

    /** Build a contract-mode proof: header mode's outer tuple, with a contract-mode field 1. */
    private static Bytes contractModeProof(
            final ClprBundleContent content, final byte[] storageProofsRlp, final boolean withManifest) {
        final byte[] header = AbiCodec.rlpList(); // currentBlockHeader (placeholder — not parsed by the codec)
        final byte[] accountProof = AbiCodec.rlpList(AbiCodec.rlpBytes(new byte[] {0x03}));
        final byte[] contentElem =
                AbiCodec.rlpBytes(ClprBundleContent.PROTOBUF.toBytes(content).toByteArray());
        if (withManifest) {
            return Bytes.wrap(AbiCodec.rlpList(
                    header,
                    contractModeValidatorSource(),
                    accountProof,
                    storageProofsRlp,
                    contentElem,
                    AbiCodec.rlpList(dummyProofEntry(9)), // manifestStorageProof
                    AbiCodec.rlpBytes(new byte[] {0x01}))); // serializedEndpointManifest
        }
        return Bytes.wrap(
                AbiCodec.rlpList(header, contractModeValidatorSource(), accountProof, storageProofsRlp, contentElem));
    }

    @Test
    void contractModeTrimKeepsEveryElementButTheContentByteIdentical() {
        // ids [3,4,5]; receiver already has up to 4 — keep only message 5. Message 5 is still the last
        // one, so its running-hash proof stays valid and only the content element (index 4) changes.
        final byte[] storage = AbiCodec.rlpList(dummyProofEntry(1), runningHashProofEntry(5L));
        final Bytes proof = contractModeProof(content(6L, List.of(msg("m3"), msg("m4"), msg("m5"))), storage, true);

        final ParsedBundle parsed = trim(proof, 4L);

        assertThat(parsed.messages()).hasSize(1);
        assertThat(parsed.messages().getFirst().message().messageData()).isEqualTo(Bytes.wrap("m5".getBytes()));
        final List<Bytes> before = RlpReader.splitList(proof);
        final List<Bytes> after = RlpReader.splitList(parsed.rawBundleBytes());
        assertThat(after).hasSize(7);
        for (final int i : new int[] {0, 1, 2, 3, 5, 6}) {
            assertThat(after.get(i)).as("tuple element %d", i).isEqualTo(before.get(i));
        }
        final ParsedBundle reparsed = codec.decodeBundle(parsed.rawBundleBytes());
        assertThat(reparsed.messages()).hasSize(1);
        assertThat(reparsed.metadata().nextMessageId()).isEqualTo(6L);
    }

    @Test
    void contractModeTrimToAckOnlyDropsRunningHashProofAndKeepsValidatorSource() {
        // ids [1]; receiver already has 1 — fully trimmed to an ack-only bundle.
        final byte[] storage = AbiCodec.rlpList(dummyProofEntry(1), runningHashProofEntry(1L), dummyProofEntry(2));
        final Bytes proof = contractModeProof(content(2L, List.of(msg("m1"))), storage, false);

        final ParsedBundle parsed = trim(proof, 1L);

        assertThat(parsed.messages()).isEmpty();
        final List<Bytes> before = RlpReader.splitList(proof);
        final List<Bytes> after = RlpReader.splitList(parsed.rawBundleBytes());
        assertThat(after).hasSize(5);
        assertThat(after.get(1)).isEqualTo(before.get(1));
        // Only the running-hash entry is dropped; the two unrelated entries survive in order.
        assertThat(RlpReader.splitList(after.get(3)))
                .containsExactly(Bytes.wrap(dummyProofEntry(1)), Bytes.wrap(dummyProofEntry(2)));
        assertThat(codec.decodeBundle(parsed.rawBundleBytes()).metadata().nextMessageId())
                .isEqualTo(2L);
    }
}
