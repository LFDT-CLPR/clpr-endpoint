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

    /** Builds a 9-item Ethereum-PoS RLP bundle whose 9th element wraps {@code content}. */
    private static Bytes nineItemBundle(final ClprBundleContent content) {
        final byte[] serialized = ClprBundleContent.PROTOBUF.toBytes(content).toByteArray();
        return Bytes.wrap(rlpList(
                item(0x01, 8), // attestedHeader (placeholder)
                item(0x02, 8), // syncAggregate
                item(0x03, 32), // executionStateRoot
                item(0x04, 8), // executionBranch
                item(0x05, 0), // nextCommittee (empty)
                rlpList(), // nextCommitteeBranch (empty list)
                item(0x06, 8), // accountProof
                item(0x07, 8), // storageProof
                rlpBytes(serialized))); // bundleContent
    }

    @Test
    void decodeBundle_extractsMetadataAndMessagesFromIndexEight() {
        final ClprMessage message = ClprMessage.newBuilder()
                .messageData(Bytes.wrap(new byte[] {0x11, 0x22}))
                .build();
        final ClprMessagePayload payload =
                ClprMessagePayload.newBuilder().message(message).build();
        final ClprQueueMetadata metadata =
                ClprQueueMetadata.newBuilder().nextMessageId(42L).build();
        final ClprBundleContent content = ClprBundleContent.newBuilder()
                .metadata(metadata)
                .messages(List.of(payload))
                .build();

        final Bytes bundle = nineItemBundle(content);
        final ParsedBundle result = CODEC.decodeBundle(bundle);

        assertThat(result.metadata().nextMessageId()).isEqualTo(42L);
        assertThat(result.messages()).hasSize(1);
        assertThat(result.messages().getFirst().message().messageData()).isEqualTo(Bytes.wrap(new byte[] {0x11, 0x22}));
        assertThat(result.rawProofBytes()).isEqualTo(bundle);
    }

    @Test
    void decodeBundle_rejectsWrongTupleSize() {
        final Bytes fiveItem = Bytes.wrap(rlpList(item(1, 4), item(2, 4), item(3, 4), item(4, 4), item(5, 4)));
        assertThatThrownBy(() -> CODEC.decodeBundle(fiveItem))
                .isInstanceOf(IllegalArgumentException.class)
                .hasStackTraceContaining("expected 9 RLP elements");
    }

    @Test
    void parseBundle_returnsVerbatimRegardlessOfReceivedMessageId() {
        final ClprQueueMetadata metadata =
                ClprQueueMetadata.newBuilder().nextMessageId(5L).build();
        final ClprBundleContent content =
                ClprBundleContent.newBuilder().metadata(metadata).build();
        final Bytes bundle = nineItemBundle(content);

        // Read-only format: parseBundle must return the same raw bytes regardless of receivedMessageId.
        final ParsedBundle result = CODEC.parseBundle(bundle, 3L);
        assertThat(result.rawProofBytes()).isEqualTo(bundle);
        assertThat(result.metadata().nextMessageId()).isEqualTo(5L);
    }
}
