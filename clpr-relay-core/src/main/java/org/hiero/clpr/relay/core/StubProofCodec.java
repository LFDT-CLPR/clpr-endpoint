// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import com.hedera.hapi.node.state.clpr.ClprBundleContent;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprQueueMetadata;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Arrays;
import java.util.List;

/**
 * Codec for the no-crypto <b>stub proof</b> format {@code "CLPRSTUB" || protobuf(ClprBundleContent)}.
 *
 * <p>Used only for integration testing against the {@code StubClprVerifier} contract: a channel
 * configured with {@code peerProofType=}{@code Stub} treats an inbound peer bundle as its own bundle
 * content — there is no state proof, so {@link #parseBundle} returns the bytes verbatim and the on-chain
 * stub verifier re-derives the same metadata. The matching outbound constructor is
 * {@link StubBundleConstructor}, and {@link #encode} produces the bytes a simulated peer sends.
 */
public final class StubProofCodec implements BundlePayloadCodec {

    /** The 8-byte magic prefix that tags the stub proof format. */
    static final Bytes MAGIC_PREFIX = StubBundleConstructor.MAGIC_PREFIX;

    /**
     * Encode {@code metadata} + {@code messages} as the stub proof {@code "CLPRSTUB" ||
     * protobuf(ClprBundleContent)} — the inverse of {@link #decodeBundle} and the format
     * {@code StubClprVerifier} accepts.
     *
     * @param metadata the queue metadata to carry
     * @param messages the ordered message payloads to carry
     * @return the stub proof bytes
     */
    public static Bytes encode(final ClprQueueMetadata metadata, final List<ClprMessagePayload> messages) {
        final ClprBundleContent content = ClprBundleContent.newBuilder()
                .metadata(metadata)
                .messages(messages)
                .build();
        final byte[] prefix = MAGIC_PREFIX.toByteArray();
        final byte[] proto = ClprBundleContent.PROTOBUF.toBytes(content).toByteArray();
        final byte[] combined = new byte[prefix.length + proto.length];
        System.arraycopy(prefix, 0, combined, 0, prefix.length);
        System.arraycopy(proto, 0, combined, prefix.length, proto.length);
        return Bytes.wrap(combined);
    }

    @Override
    public ParsedBundle decodeBundle(final Bytes proofBytes) {
        final byte[] all = proofBytes.toByteArray();
        final byte[] prefix = MAGIC_PREFIX.toByteArray();
        if (all.length < prefix.length || !hasPrefix(all, prefix)) {
            throw new IllegalArgumentException("Stub bundle proof missing the CLPRSTUB prefix");
        }
        try {
            final ClprBundleContent content =
                    ClprBundleContent.PROTOBUF.parse(Bytes.wrap(Arrays.copyOfRange(all, prefix.length, all.length)));
            final ClprQueueMetadata metadata =
                    content.metadata() != null ? content.metadata() : ClprQueueMetadata.DEFAULT;
            return new ParsedBundle(metadata, List.copyOf(content.messages()), proofBytes);
        } catch (final Exception e) {
            throw new IllegalArgumentException("Failed to parse stub bundle proof", e);
        }
    }

    @Override
    public ParsedBundle parseBundle(final Bytes proofBytes, final long receivedMessageId) {
        // The stub format carries its metadata + messages verbatim (no state proof to trim), so the
        // bundle is submitted unchanged; the on-chain replay guard drops any already-received message.
        return decodeBundle(proofBytes);
    }

    private static boolean hasPrefix(final byte[] data, final byte[] prefix) {
        for (int i = 0; i < prefix.length; i++) {
            if (data[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }
}
