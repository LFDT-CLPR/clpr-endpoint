// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.sei;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.hapi.node.state.clpr.ClprBundleContent;
import com.hedera.hapi.node.state.clpr.ClprMessage;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprQueueMetadata;
import com.hedera.hapi.node.state.clpr.ClprSeiBundlePayload;
import com.hedera.hapi.node.state.clpr.SeiStateProof;
import com.hedera.hapi.node.state.clpr.SeiStorageProofEntry;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.List;
import org.hiero.clpr.relay.core.ParsedBundle;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout;
import org.junit.jupiter.api.Test;

/** Tests {@link SeiProofCodec}'s bundle-content extraction and message trimming. */
class SeiProofCodecTest {

    private final Bytes channelId = Bytes.wrap(new byte[32]);
    private final SeiProofCodec codec = new SeiProofCodec(channelId);

    /** Decode, drop already-received messages, and re-encode the retained suffix — the production flow. */
    private ParsedBundle trim(final Bytes proof, final long receivedMessageId) {
        return codec.parseBundle(proof, receivedMessageId);
    }

    /** Wrap {@code content} in a {@code ClprSeiBundlePayload} (field 2 = bundle_content). */
    private static Bytes seiProof(final ClprBundleContent content) {
        return seiProof(content, null);
    }

    /** Wrap {@code content} with an optional {@code SeiStateProof}. */
    private static Bytes seiProof(final ClprBundleContent content, final SeiStateProof stateProof) {
        final var builder =
                ClprSeiBundlePayload.newBuilder().bundleContent(ClprBundleContent.PROTOBUF.toBytes(content));
        if (stateProof != null) {
            builder.stateProof(stateProof);
        }
        return ClprSeiBundlePayload.PROTOBUF.toBytes(builder.build());
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

    /** A storage-proof entry whose ABCI key (0x03 || 20-byte addr || 32-byte slot) targets a message's running hash. */
    private SeiStorageProofEntry runningHashEntry(final long messageId) {
        final byte[] slot = AbiCodec.encodeUint256(
                ClprServiceStorageLayout.calculateMsgRunningHashStorageSlot(channelId, BigInteger.valueOf(messageId)));
        final byte[] key = new byte[53];
        key[0] = 0x03; // x/evm StateKeyPrefix; bytes 1..21 are the (zeroed) contract address here
        System.arraycopy(slot, 0, key, 21, 32);
        return SeiStorageProofEntry.newBuilder().key(Bytes.wrap(key)).build();
    }

    private static SeiStorageProofEntry dummyEntry(final int seed) {
        final byte[] key = new byte[53];
        key[0] = 0x03;
        key[52] = (byte) seed;
        return SeiStorageProofEntry.newBuilder().key(Bytes.wrap(key)).build();
    }

    @Test
    void parseBundleExtractsMetadataMessagesAndPassesRawProofThrough() {
        final ClprBundleContent content = content(7L, List.of(msg("hello")));
        final Bytes proof = seiProof(content);

        final ParsedBundle parsed = codec.decodeBundle(proof);

        assertThat(parsed.metadata().nextMessageId()).isEqualTo(7L);
        assertThat(parsed.messages()).hasSize(1);
        assertThat(parsed.messages().get(0).message().messageData()).isEqualTo(Bytes.wrap("hello".getBytes()));
        // The raw proof bytes pass through unchanged for on-chain submission.
        assertThat(parsed.rawBundleBytes()).isEqualTo(proof);
    }

    @Test
    void rejectsMalformedPayload() {
        // field 1 (LEN) claiming 127 bytes with none following — invalid protobuf wire format.
        final Bytes malformed = Bytes.wrap(new byte[] {0x0a, 0x7f});
        assertThatThrownBy(() -> codec.decodeBundle(malformed))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Sei bundle");
    }

    @Test
    void trimReturnsBundleVerbatimWhenNothingAlreadyReceived() {
        final Bytes proof = seiProof(content(2L, List.of(msg("m1"))));

        final ParsedBundle parsed = trim(proof, 0L);

        assertThat(parsed.messages()).hasSize(1);
        assertThat(parsed.rawBundleBytes()).isEqualTo(proof);
    }

    @Test
    void trimDropsAlreadyReceivedLeadingMessages() {
        // ids [3,4,5]; receiver already has up to 4 — keep only message 5.
        final Bytes proof = seiProof(content(6L, List.of(msg("m3"), msg("m4"), msg("m5"))));

        final ParsedBundle parsed = trim(proof, 4L);

        assertThat(parsed.messages()).hasSize(1);
        assertThat(parsed.metadata().nextMessageId()).isEqualTo(6L);
        final ParsedBundle reparsed = codec.decodeBundle(parsed.rawBundleBytes());
        assertThat(reparsed.messages()).hasSize(1);
        assertThat(reparsed.messages().get(0).message().messageData()).isEqualTo(Bytes.wrap("m5".getBytes()));
    }

    @Test
    void trimToAckOnlyRemovesAllMessagesAndDropsRunningHashProof() {
        // ids [1]; receiver already has 1 — fully trimmed. State proof has the message-1 running-hash entry
        // plus two unrelated channel-field slots; only the former must be dropped.
        final SeiStateProof stateProof = SeiStateProof.newBuilder()
                .storageProofs(List.of(dummyEntry(1), runningHashEntry(1L), dummyEntry(2)))
                .build();
        final Bytes proof = seiProof(content(2L, List.of(msg("m1"))), stateProof);

        final ParsedBundle parsed = trim(proof, 1L);

        assertThat(parsed.messages()).isEmpty();
        final ParsedBundle reparsed = codec.decodeBundle(parsed.rawBundleBytes());
        assertThat(reparsed.messages()).isEmpty();
        assertThat(reparsed.metadata().nextMessageId()).isEqualTo(2L);
        // The running-hash entry is gone; the two unrelated entries survive.
        final ClprSeiBundlePayload rebuilt = readPayload(parsed.rawBundleBytes());
        assertThat(rebuilt.stateProof().storageProofs()).hasSize(2);
        final byte[] droppedSlot = AbiCodec.encodeUint256(
                ClprServiceStorageLayout.calculateMsgRunningHashStorageSlot(channelId, BigInteger.ONE));
        assertThat(rebuilt.stateProof().storageProofs())
                .noneMatch(e -> e.key().slice(e.key().length() - 32, 32).equals(Bytes.wrap(droppedSlot)));
    }

    @Test
    void trimPreservesManifestProof() {
        // A bundle carrying an endpoint-manifest proof (fields 4/5) that is partially trimmed must keep
        // the proof + preimage — it proves slot 18, not a message slot, so trimming never orphans it.
        final SeiStorageProofEntry manifestEntry = dummyEntry(9);
        final Bytes manifestPreimage = Bytes.wrap(new byte[] {0x01, 0x02, 0x03});
        final var payload = ClprSeiBundlePayload.newBuilder()
                .bundleContent(
                        ClprBundleContent.PROTOBUF.toBytes(content(6L, List.of(msg("m3"), msg("m4"), msg("m5")))))
                .manifestStorageProof(manifestEntry)
                .endpointManifest(manifestPreimage)
                .build();
        final Bytes proof = ClprSeiBundlePayload.PROTOBUF.toBytes(payload);

        // Receiver already has up to 4 → only message 5 kept, forcing the re-encode/rebuild path.
        final ParsedBundle parsed = trim(proof, 4L);

        assertThat(parsed.messages()).hasSize(1);
        final ClprSeiBundlePayload rebuilt = readPayload(parsed.rawBundleBytes());
        assertThat(rebuilt.hasManifestStorageProof()).isTrue();
        assertThat(rebuilt.manifestStorageProof()).isEqualTo(manifestEntry);
        assertThat(rebuilt.endpointManifest()).isEqualTo(manifestPreimage);
    }

    private static ClprSeiBundlePayload readPayload(final Bytes bytes) {
        try {
            return ClprSeiBundlePayload.PROTOBUF.parse(bytes);
        } catch (final Exception e) {
            throw new AssertionError(e);
        }
    }
}
