// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hiero.clpr.relay.evm.AbiCodec.rlpBytes;
import static org.hiero.clpr.relay.evm.AbiCodec.rlpList;

import com.hedera.hapi.node.state.clpr.ClprBundleContent;
import com.hedera.hapi.node.state.clpr.ClprMessage;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprQueueMetadata;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.clpr.relay.core.ParsedBundle;
import org.junit.jupiter.api.Test;

class EthereumPosProofCodecTest {

    private static final EthereumPosProofCodec CODEC = new EthereumPosProofCodec();

    private static byte[] item(final int seed, final int len) {
        final byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[i] = (byte) (seed + i);
        }
        return rlpBytes(b);
    }

    /**
     * Builds the standard ten-item Ethereum-PoS RLP bundle (no manifest proof) whose element at index
     * {@code 8} wraps {@code content} — matching {@link EthereumPosBundleConstructor}'s steady-state
     * output {@code [attestedHeader, syncAggregate, executionStateRoot, executionBranch, nextCommittee,
     * nextCommitteeBranch, accountProof, storageProof, bundleContent, nonSignerProofs]}.
     */
    private static Bytes tenItemBundle(final ClprBundleContent content) {
        final byte[] serialized = ClprBundleContent.PROTOBUF.toBytes(content).toByteArray();
        return Bytes.wrap(rlpList(
                item(0x01, 8), // [0] attestedHeader (placeholder)
                item(0x02, 8), // [1] syncAggregate
                item(0x03, 32), // [2] executionStateRoot
                item(0x04, 8), // [3] executionBranch
                item(0x05, 0), // [4] nextCommittee (empty in steady state)
                rlpList(), // [5] nextCommitteeBranch (empty list in steady state)
                item(0x06, 8), // [6] accountProof
                item(0x07, 8), // [7] storageProof
                rlpBytes(serialized), // [8] bundleContent
                rlpList())); // [9] nonSignerProofs (empty: all committee members signed)
    }

    /**
     * Builds the twelve-item Ethereum-PoS RLP bundle (with manifest proof): the ten-item shape plus the
     * two trailing manifest elements {@link EthereumPosBundleConstructor} appends when a local
     * endpoint-manifest proof is attached. The bundle content stays at index {@code 8}.
     */
    private static Bytes twelveItemBundle(final ClprBundleContent content) {
        final byte[] serialized = ClprBundleContent.PROTOBUF.toBytes(content).toByteArray();
        return Bytes.wrap(rlpList(
                item(0x01, 8), // [0] attestedHeader (placeholder)
                item(0x02, 8), // [1] syncAggregate
                item(0x03, 32), // [2] executionStateRoot
                item(0x04, 8), // [3] executionBranch
                item(0x05, 0), // [4] nextCommittee (empty in steady state)
                rlpList(), // [5] nextCommitteeBranch (empty list in steady state)
                item(0x06, 8), // [6] accountProof
                item(0x07, 8), // [7] storageProof
                rlpBytes(serialized), // [8] bundleContent
                rlpList(), // [9] nonSignerProofs
                rlpList(item(0x09, 8)), // [10] manifestProof (entry-wrapped placeholder)
                item(0x0A, 8))); // [11] manifestPreimage (placeholder)
    }

    private static ClprBundleContent contentWithOneMessage() {
        final ClprMessage message = ClprMessage.newBuilder()
                .messageData(Bytes.wrap(new byte[] {0x11, 0x22}))
                .build();
        final ClprMessagePayload payload =
                ClprMessagePayload.newBuilder().message(message).build();
        final ClprQueueMetadata metadata =
                ClprQueueMetadata.newBuilder().nextMessageId(42L).build();
        return ClprBundleContent.newBuilder()
                .metadata(metadata)
                .messages(List.of(payload))
                .build();
    }

    @Test
    void decodeBundle_extractsMetadataAndMessagesFromIndexEight() {
        final ClprBundleContent content = contentWithOneMessage();

        final Bytes bundle = tenItemBundle(content);
        final ParsedBundle result = CODEC.decodeBundle(bundle);

        assertThat(result.metadata().nextMessageId()).isEqualTo(42L);
        assertThat(result.messages()).hasSize(1);
        assertThat(result.messages().getFirst().message().messageData()).isEqualTo(Bytes.wrap(new byte[] {0x11, 0x22}));
        assertThat(result.rawBundleBytes()).isEqualTo(bundle);
    }

    @Test
    void decodeBundle_extractsContentFromTwelveItemBundleWithManifestProof() {
        final ClprBundleContent content = contentWithOneMessage();

        final Bytes bundle = twelveItemBundle(content);
        final ParsedBundle result = CODEC.decodeBundle(bundle);

        assertThat(result.metadata().nextMessageId()).isEqualTo(42L);
        assertThat(result.messages()).hasSize(1);
        assertThat(result.rawBundleBytes()).isEqualTo(bundle);
    }

    @Test
    void decodeBundle_rejectsWrongTupleSize() {
        final Bytes fiveItem = Bytes.wrap(rlpList(item(1, 4), item(2, 4), item(3, 4), item(4, 4), item(5, 4)));
        assertThatThrownBy(() -> CODEC.decodeBundle(fiveItem))
                .isInstanceOf(IllegalArgumentException.class)
                .hasStackTraceContaining("expected 10 or 12 RLP elements");
    }

    @Test
    void parseBundle_returnsVerbatimRegardlessOfReceivedMessageId() {
        final ClprQueueMetadata metadata =
                ClprQueueMetadata.newBuilder().nextMessageId(5L).build();
        final ClprBundleContent content =
                ClprBundleContent.newBuilder().metadata(metadata).build();
        final Bytes bundle = tenItemBundle(content);

        // Read-only format: parseBundle must return the same raw bytes regardless of receivedMessageId.
        final ParsedBundle result = CODEC.parseBundle(bundle, 3L);
        assertThat(result.rawBundleBytes()).isEqualTo(bundle);
        assertThat(result.metadata().nextMessageId()).isEqualTo(5L);
    }
}
