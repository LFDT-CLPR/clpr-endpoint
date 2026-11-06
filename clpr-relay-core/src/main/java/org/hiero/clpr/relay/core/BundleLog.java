// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import com.hedera.hapi.node.state.clpr.ClprQueueMetadata;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.HexFormat;
import org.jspecify.annotations.Nullable;

/**
 * Shared formatting helpers for tracing a single bundle as it moves through the relay.
 *
 * <p>A bundle passes through several log sites on its way across the wire — constructed and cached,
 * dispatched outbound and received back, received inbound, verified, and submitted on-chain. To follow
 * one bundle end to end, every site logs the same two artefacts:
 *
 * <ul>
 *   <li>{@link #tag(Bytes)} — a short, content-derived identity {@code len=<n> bundle=<8hex>}.
 *       The id is the leading bytes of {@code keccak256(payload)}, so the <em>same wire bytes</em>
 *       produce the <em>same id</em> on both the sending and the receiving relay. Grep one
 *       {@code bundle=<id>} across a log (or across both relays' interleaved logs in an integration
 *       test) to see that bundle's whole lifecycle.</li>
 *   <li>{@link #coords(ClprQueueMetadata, int)} — the logical payload the bundle carries
 *       ({@code next=<> recv=<> status=<> msgs=<>}), available wherever the metadata has been
 *       decoded.</li>
 * </ul>
 *
 * <p>The wire payload, the cached proof package, and {@link ParsedBundle#rawBundleBytes()} are
 * byte-identical for a given bundle, so tagging any of them yields the same id. The keccak digest
 * is only computed when a statement is actually emitted (callers gate on log level), so the cost stays
 * off the hot path.
 */
public final class BundleLog {

    private BundleLog() {}

    /**
     * Short, stable content tag for a wire bundle payload: {@code len=<n> bundle=<8hex>}.
     *
     * @param payload the bundle payload bytes (the wire payload, the cached proof package, or
     *                {@link ParsedBundle#rawBundleBytes()} — all equivalent)
     * @return a {@code len=… bundle=…} fragment; {@code len=0 bundle=empty} for an empty/null payload
     */
    public static String tag(@Nullable final Bytes payload) {
        if (payload == null || payload.length() == 0) {
            return "len=0 bundle=empty";
        }
        final byte[] digest = Secp256k1Utils.keccak256(payload.toByteArray());
        return "len=" + payload.length() + " bundle=" + HexFormat.of().formatHex(digest, 0, 4);
    }

    /**
     * Logical coordinates a bundle carries: {@code next=<> recv=<> status=<> msgs=<>}.
     *
     * @param metadata     the verified queue metadata
     * @param messageCount number of application messages in the bundle (0 for ack/status-only)
     * @return a {@code next=… recv=… status=… msgs=…} fragment
     */
    public static String coords(final ClprQueueMetadata metadata, final int messageCount) {
        return "next=" + metadata.nextMessageId()
                + " recv=" + metadata.receivedMessageId()
                + " status=" + metadata.status()
                + " msgs=" + messageCount;
    }
}
