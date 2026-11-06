// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.test.harness;

import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprConfigUpdate;
import com.hedera.hapi.node.state.clpr.ClprControlMessage;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.hapi.node.state.clpr.ClprMessage;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprMessageReply;
import com.hedera.hapi.node.state.clpr.ClprMessageReplyStatus;
import com.hedera.hapi.node.state.clpr.ClprQueueMetadata;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.hiero.clpr.relay.core.StubProofCodec;

/**
 * Crafts {@code bundle_payload} bytes the endpoint-under-test's verifier accepts (the no-crypto
 * <b>stub proof</b> format {@code "CLPRSTUB" || protobuf(ClprBundleContent)}), with chosen queue
 * metadata and messages.
 *
 * <p>This builder emits the stub-proof shape via {@link StubProofCodec#encode} — the same format the
 * relay parses with {@code StubProofCodec} ({@code peerProofType=Stub}) and the integration-test
 * {@code StubClprVerifier} accepts. The queue metadata carries the simulated peer's view; each message
 * rides along in {@code ClprBundleContent.messages}.
 *
 * <p><b>How the fields map to verified metadata</b>:
 * <ul>
 *   <li>{@code next_message_id = acked_message_id + 1 + #messages}</li>
 *   <li>{@code sent_running_hash}, {@code received_message_id}, {@code received_running_hash},
 *       {@code status}, {@code trust_anchor_id} are carried straight through.</li>
 * </ul>
 *
 * <p><b>Contract-acceptance coupling.</b> When the endpoint submits this bundle, the real CLPR Service
 * (the oracle) re-checks it. To pass:
 * <ul>
 *   <li><b>Replay</b> — the first message id must equal {@code local.received_message_id + 1}; set
 *       {@link #ackedMessageId(long)} to the destination's current {@code received_message_id}.</li>
 *   <li><b>Running hash</b> — the verified {@code sent_running_hash} must equal the SHA-256 chain over
 *       the included messages starting from {@code local.received_running_hash}; pass that base to
 *       {@link #chainFrom(Bytes)} and the builder computes the leaf's {@code sent_running_hash}.</li>
 *   <li><b>Ack</b> — {@link #receivedMessageId(long)} (the peer's ack of our outbound) updates the
 *       destination's {@code acked_message_id}; it must not exceed {@code next_message_id - 1}.</li>
 * </ul>
 */
public final class StubBundles {

    private final List<ClprMessagePayload> messages = new ArrayList<>();
    private long ackedMessageId;
    private long receivedMessageId;
    private Bytes chainFrom = zeros();
    private Bytes receivedRunningHash = zeros();
    private ClprChannelStatus status = ClprChannelStatus.ACTIVE;
    private Bytes trustAnchorId = Bytes.EMPTY;

    private StubBundles() {}

    /** Start a stub-proof ({@code CLPRSTUB}) bundle builder. */
    public static StubBundles stubProof() {
        return new StubBundles();
    }

    /**
     * The leaf's {@code acked_message_id}, which sets the bundle's message-id frontier
     * ({@code next_message_id = acked_message_id + 1 + #messages}). For the bundle to pass the
     * destination's replay check, set this to the destination channel's current
     * {@code received_message_id}.
     */
    public StubBundles ackedMessageId(final long value) {
        this.ackedMessageId = value;
        return this;
    }

    /** The leaf's {@code received_message_id} — the peer's ack of our outbound; updates local {@code acked_message_id}. */
    public StubBundles receivedMessageId(final long value) {
        this.receivedMessageId = value;
        return this;
    }

    /**
     * The destination channel's current {@code received_running_hash}: the base from which the
     * builder computes the leaf's {@code sent_running_hash} by chaining the included messages. Defaults
     * to 32 zero bytes (a fresh channel).
     */
    public StubBundles chainFrom(final Bytes localReceivedRunningHash) {
        this.chainFrom = localReceivedRunningHash;
        return this;
    }

    /** The leaf's {@code received_running_hash} (the peer's view of our sent hash; optional cross-validation). */
    public StubBundles receivedRunningHash(final Bytes value) {
        this.receivedRunningHash = value;
        return this;
    }

    /** The peer's channel status carried in the metadata (ACTIVE / CLOSING / DRAINED / CLOSED …). */
    public StubBundles status(final ClprChannelStatus value) {
        this.status = value;
        return this;
    }

    /** The leaf's {@code trust_anchor_id}, propagated in the verified metadata (§3.2). */
    public StubBundles trustAnchorId(final Bytes value) {
        this.trustAnchorId = value;
        return this;
    }

    /** Append a DATA message from the peer. */
    public StubBundles message(
            final byte[] connectorId, final byte[] targetApplication, final byte[] sender, final byte[] messageData) {
        messages.add(ClprMessagePayload.newBuilder()
                .message(ClprMessage.newBuilder()
                        .connectorId(Bytes.wrap(connectorId.clone()))
                        .targetApplication(Bytes.wrap(targetApplication.clone()))
                        .sender(Bytes.wrap(sender.clone()))
                        .messageData(Bytes.wrap(messageData.clone()))
                        .build())
                .build());
        return this;
    }

    /** Append a REPLY message (a response to one of our outbound DATA messages). */
    public StubBundles reply(final long messageId, final ClprMessageReplyStatus replyStatus, final byte[] replyData) {
        messages.add(ClprMessagePayload.newBuilder()
                .messageReply(ClprMessageReply.newBuilder()
                        .messageId(messageId)
                        .status(replyStatus)
                        .messageReplyData(Bytes.wrap(replyData.clone()))
                        .build())
                .build());
        return this;
    }

    /**
     * Append a CONTROL message carrying a {@link ClprLedgerConfiguration} config update. When the
     * destination CLPR Service processes it (§4.2 step 6), it replaces the Channel's peer endpoint
     * roster from {@code config.endpoints()} and advances {@code peer_config_timestamp} to
     * {@code config.timestamp()} (§2.4.2) — which the relay's listener detects as a roster refresh.
     */
    public StubBundles controlConfigUpdate(final ClprLedgerConfiguration config) {
        messages.add(ClprMessagePayload.newBuilder()
                .control(ClprControlMessage.newBuilder()
                        .configUpdate(ClprConfigUpdate.newBuilder()
                                .configuration(config)
                                .build())
                        .build())
                .build());
        return this;
    }

    /** Build the stub-proof ({@code "CLPRSTUB" || protobuf(ClprBundleContent)}) {@code bundle_payload} bytes. */
    public Bytes build() {
        final ClprQueueMetadata metadata = ClprQueueMetadata.newBuilder()
                .nextMessageId(ackedMessageId + 1L + messages.size())
                .sentRunningHash(computeSentRunningHash())
                .receivedMessageId(receivedMessageId)
                .receivedRunningHash(receivedRunningHash)
                .status(status)
                .trustAnchorId(trustAnchorId)
                .build();
        return StubProofCodec.encode(metadata, List.copyOf(messages));
    }

    /**
     * SHA-256 running-hash chain over the included messages, seeded from {@link #chainFrom}. Each
     * step is {@code sha256(previousHash ‖ sha256(payload))}: the payload is hashed before being
     * folded into the chain, matching the on-chain queue running-hash computation.
     */
    private Bytes computeSentRunningHash() {
        byte[] running = chainFrom.toByteArray();
        for (final ClprMessagePayload payload : messages) {
            running =
                    sha256(running, ClprMessagePayload.PROTOBUF.toBytes(payload).toByteArray());
        }
        return Bytes.wrap(running);
    }

    private static byte[] sha256(final byte[] previous, final byte[] serializedPayload) {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(previous);
            digest.update(sha256(serializedPayload));
            return digest.digest();
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static byte[] sha256(final byte[] input) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(input);
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static Bytes zeros() {
        return Bytes.wrap(new byte[32]);
    }
}
