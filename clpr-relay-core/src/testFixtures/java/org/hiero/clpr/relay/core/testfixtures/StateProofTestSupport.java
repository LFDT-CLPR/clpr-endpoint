// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core.testfixtures;

import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprMessageValue;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds Hiero block-stream {@code StateProof} wire bytes for tests, without depending on the
 * block-stream proto family. Mirrors the nesting a Hiero node emits:
 *
 * <pre>
 *   StateProof { paths(1) = [ MerklePath { state_item_leaf(4) =
 *       StateItem { value(3) = StateValue { ClprChannel=60 | ClprMessageValue=62 } } } ... ],
 *                signed_block_proof(2) = TssSignedBlockProof { block_signature(1) = &lt;opaque&gt; } }
 * </pre>
 *
 * <p>Only the leaf values are real protobuf ({@link ClprChannel} / {@link ClprMessageValue}); the
 * surrounding structure is hand-encoded so the decoder under test sees exactly the shape Hiero sends.
 *
 * <p>Promoted to {@code testFixtures} so the stub-peer test harness can craft inbound bundle bytes the
 * {@code CompositeProofParser} accepts (Hiero lead bytes), in addition to the {@code clpr-relay-core}
 * unit tests that originally owned it.
 */
public final class StateProofTestSupport {

    private static final int WIRE_LENGTH_DELIMITED = 2;
    private static final int STATE_PROOF_PATHS_FIELD = 1;
    private static final int STATE_PROOF_SIGNED_BLOCK_PROOF_FIELD = 2;
    private static final int MERKLE_PATH_STATE_ITEM_LEAF_FIELD = 4;
    private static final int STATE_ITEM_VALUE_FIELD = 3;
    private static final int STATE_VALUE_CLPR_CHANNEL_FIELD = 60;
    private static final int STATE_VALUE_CLPR_MESSAGE_VALUE_FIELD = 62;
    private static final int SIGNED_BLOCK_PROOF_BLOCK_SIGNATURE_FIELD = 1;

    private StateProofTestSupport() {}

    /** A full bundle proof: a channel leaf plus one message leaf per payload. */
    public static Bytes bundleProof(final ClprChannel channel, final List<ClprMessagePayload> payloads) {
        final List<byte[]> parts = new ArrayList<>();
        parts.add(channelPath(channel));
        for (final ClprMessagePayload payload : payloads) {
            parts.add(messagePath(payload));
        }
        // A real StateProof also carries a signed_block_proof (field 2), a TssSignedBlockProof whose
        // block_signature (field 1) is opaque to us. It must be well-formed protobuf: the on-chain
        // decoder descends into signed_block_proof, so a bare byte string trips its "unsupported wire
        // type" guard (a leading 0x01 byte decodes as wire type 1). Wrap the dummy signature in field 1.
        final byte[] signedBlockProof =
                lengthDelimited(SIGNED_BLOCK_PROOF_BLOCK_SIGNATURE_FIELD, new byte[] {0x01, 0x02, 0x03});
        parts.add(lengthDelimited(STATE_PROOF_SIGNED_BLOCK_PROOF_FIELD, signedBlockProof));
        return Bytes.wrap(concat(parts));
    }

    /** A malformed-for-our-purposes proof: only a message leaf, no channel leaf. */
    public static Bytes messageOnlyProof(final ClprMessagePayload payload) {
        return Bytes.wrap(messagePath(payload));
    }

    private static byte[] channelPath(final ClprChannel channel) {
        final byte[] stateValue = lengthDelimited(
                STATE_VALUE_CLPR_CHANNEL_FIELD,
                ClprChannel.PROTOBUF.toBytes(channel).toByteArray());
        return path(stateValue);
    }

    private static byte[] messagePath(final ClprMessagePayload payload) {
        final ClprMessageValue value =
                ClprMessageValue.newBuilder().payload(payload).build();
        final byte[] stateValue = lengthDelimited(
                STATE_VALUE_CLPR_MESSAGE_VALUE_FIELD,
                ClprMessageValue.PROTOBUF.toBytes(value).toByteArray());
        return path(stateValue);
    }

    /** Wrap a serialized StateValue as StateItem.value → MerklePath.state_item_leaf → StateProof.paths. */
    private static byte[] path(final byte[] stateValue) {
        final byte[] stateItem = lengthDelimited(STATE_ITEM_VALUE_FIELD, stateValue);
        final byte[] merklePath = lengthDelimited(MERKLE_PATH_STATE_ITEM_LEAF_FIELD, stateItem);
        return lengthDelimited(STATE_PROOF_PATHS_FIELD, merklePath);
    }

    private static byte[] lengthDelimited(final int field, final byte[] value) {
        final byte[] tag = varint((long) (field << 3) | WIRE_LENGTH_DELIMITED);
        final byte[] len = varint(value.length);
        return concat(List.of(tag, len, value));
    }

    private static byte[] varint(final long v) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        long value = v;
        do {
            int b = (int) (value & 0x7f);
            value >>>= 7;
            if (value != 0) {
                b |= 0x80;
            }
            out.write(b);
        } while (value != 0);
        return out.toByteArray();
    }

    private static byte[] concat(final List<byte[]> parts) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (final byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }
}
