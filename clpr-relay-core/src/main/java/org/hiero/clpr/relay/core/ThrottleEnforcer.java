// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.logging.api.Logger;
import com.swirlds.logging.api.Loggers;
import java.util.List;
import org.jspecify.annotations.NonNull;

/**
 * Enforces inbound bundle size and message-count limits for a single CLPR channel (spec §3.1.1).
 *
 * <p>Each limit is independently configurable; a value of {@code <= 0} disables that particular
 * check. All three checks are applied cheapest-first (bundle byte count → message count →
 * per-message payload size) so that oversized payloads are rejected before any per-message work.
 */
public class ThrottleEnforcer {

    private static final Logger LOG = Loggers.getLogger(ThrottleEnforcer.class);

    private final long maxSyncBytes;
    private final int maxMessagesPerBundle;
    private final int maxMessagePayloadBytes;

    /**
     * Full constructor wiring every spec-defined size/count throttle this enforcer covers.
     *
     * @param maxSyncBytes           spec §3.1.1 / §3.2.5 — maximum bundle-exchange payload size in bytes; {@code <= 0}
     *                               means unthrottled
     * @param maxMessagesPerBundle   spec §3.1.1 — hard cap on messages per bundle; {@code <= 0} means unthrottled
     * @param maxMessagePayloadBytes spec §3.1.1 / §3.2.5 — maximum size of a single message payload in bytes;
     *                               {@code <= 0} means unthrottled
     */
    public ThrottleEnforcer(final long maxSyncBytes, final int maxMessagesPerBundle, final int maxMessagePayloadBytes) {
        this.maxSyncBytes = maxSyncBytes;
        this.maxMessagesPerBundle = maxMessagesPerBundle;
        this.maxMessagePayloadBytes = maxMessagePayloadBytes;
    }

    /**
     * Size (in bytes) of the user payload carried by one queued message, per spec §3.1.1's "message payload"
     * definition. {@code ClprMessage.message_data} and {@code ClprMessageReply.message_reply_data} are the only fields
     * the spec calls user payload; a {@code ClprControlMessage} is a protocol-defined structure and is reported as size
     * 0 so the user-set limit doesn't reject it.
     */
    private static long userPayloadBytes(@NonNull final ClprMessagePayload p) {
        if (p.hasMessage()) {
            return p.messageOrThrow().messageData().length();
        }
        if (p.hasMessageReply()) {
            return p.messageReplyOrThrow().messageReplyData().length();
        }
        return 0L;
    }

    /**
     * Apply every spec-defined size/count throttle to one inbound sync and return whether it should be processed.
     *
     * <p>Order of checks: bundle size → message count → per-message payload size.
     * The specific reason for any rejection is logged at WARN here so callers only need to react to a boolean.
     *
     * @param bundleBytes the full inbound bundle payload; its {@link Bytes#length()} is compared against
     *                    {@code maxSyncBytes}
     * @param messages    parsed messages from the verified bundle; count and per-message payload size
     *                    are checked against {@code maxMessagesPerBundle} and {@code maxMessagePayloadBytes}
     * @return {@code true} if every configured limit accepts the sync, {@code false} (with WARN log) otherwise
     */
    public boolean shouldAccept(final Bytes bundleBytes, @NonNull final List<ClprMessagePayload> messages) {
        if (maxSyncBytes > 0 && bundleBytes.length() > maxSyncBytes) {
            LOG.warn("bundle shunned: size {} bytes exceeds MaxSyncBytes={}", bundleBytes.length(), maxSyncBytes);
            return false;
        }
        if (maxMessagesPerBundle > 0 && messages.size() > maxMessagesPerBundle) {
            LOG.warn(
                    "bundle shunned: {} messages exceeds MaxMessagesPerBundle={}",
                    messages.size(),
                    maxMessagesPerBundle);
            return false;
        }
        if (maxMessagePayloadBytes > 0) {
            for (int i = 0; i < messages.size(); i++) {
                // Per spec §3.1.1: MaxMessagePayloadBytes caps the user-data payload of a single
                // message — message_data for ClprMessage, message_reply_data for ClprMessageReply.
                // Control messages are protocol-defined structures (not user payload), so the
                // user-set limit doesn't apply to them.
                final long sz = userPayloadBytes(messages.get(i));
                if (sz > maxMessagePayloadBytes) {
                    LOG.warn(
                            "bundle shunned: message[{}] payload {} bytes exceeds MaxMessagePayloadBytes={}",
                            i,
                            sz,
                            maxMessagePayloadBytes);
                    return false;
                }
            }
        }
        return true;
    }
}
