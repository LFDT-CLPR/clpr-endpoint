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
}
