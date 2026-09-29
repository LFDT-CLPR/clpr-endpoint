// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.grpc.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.hiero.clpr.relay.core.BundleConstructor;
import org.hiero.clpr.relay.core.BundlePayloadCodec;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.ParsedBundle;
import org.hiero.clpr.relay.core.PeerManifestVersionCache;
import org.hiero.clpr.relay.core.TransactionSubmitter;
import org.hiero.clpr.relay.core.metrics.SimpleMetrics;
import org.hiero.clpr.relay.core.testfixtures.PassThroughCodec;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ClprSyncHandler}: channel state guards, inbound-bundle throttling, and
 * submission. (Peer authentication is handled by the mTLS transport, not this handler.)
 */
class ClprSyncHandlerTest {

    private static final Bytes CHANNEL_ID = Bytes.wrap(new byte[] {0x01, 0x02, 0x03});
    private static final Bytes BUNDLE_PAYLOAD = Bytes.wrap("bundle-data".getBytes());

    // -------------------------------------------------------------------------
    // Tracking stubs
    // -------------------------------------------------------------------------

    static class TrackingSubmitter implements TransactionSubmitter {
        boolean called = false;

        @Override
        public void submitBundle(@NonNull ClprChannel channel, @NonNull ParsedBundle bundle) {
            called = true;
        }
    }

    static class RevertingSubmitter implements TransactionSubmitter {
        boolean called = false;

        @Override
        public void submitBundle(@NonNull ClprChannel channel, @NonNull ParsedBundle bundle) {
            called = true;
        }
    }

    static class FixedBundleConstructor implements BundleConstructor {
        @NonNull
        @Override
        public Optional<Bytes> getLatestBundlePayload(@NonNull Bytes id) {
            return Optional.of(Bytes.wrap(new byte[] {0x01}));
        }

        @Override
        public void onStateChanged(
                @NonNull final BigInteger blockNumber,
                @NonNull Bytes id,
                @NonNull ClprChannel s,
                @NonNull List<ContractStateReader.QueuedMessage> m) {}
    }

    static class StubStateReader implements ContractStateReader {
        ClprChannelStatus status;

        StubStateReader(ClprChannelStatus status) {
            this.status = status;
        }

        @NonNull
        @Override
        public Optional<ClprChannel> readChannelState(@NonNull Bytes channelId, @NonNull String blockTag) {
            // serviceAddress here is the PEER's service address; the handler does not consume it (it
            // returns the cached proof unsigned), so any value works for these tests.
            final byte[] peerAddr = new byte[20];
            Arrays.fill(peerAddr, (byte) 0xDD);
            return Optional.of(ClprChannel.newBuilder()
                    .channelId(channelId)
                    .status(status)
                    .nextMessageId(1L)
                    .ackedMessageId(0L)
                    .serviceAddress(Bytes.wrap(peerAddr))
                    .build());
        }

        @NonNull
        @Override
        public List<QueuedMessage> readQueuedMessages(
                @NonNull Bytes channelId, long fromId, long toId, @NonNull String blockTag) {
            return List.of();
        }

        @NonNull
        @Override
        public ClprLedgerConfiguration readLedgerConfiguration(@NonNull CommitmentLevel level) {
            return ClprLedgerConfiguration.DEFAULT;
        }
    }

    // -------------------------------------------------------------------------
    // Test fixtures
    // -------------------------------------------------------------------------

    private TrackingSubmitter submitter;
    private PassThroughCodec proofParser;

    @BeforeEach
    void setUp() {
        submitter = new TrackingSubmitter();
        proofParser = new PassThroughCodec();
    }

    /** Build a handler with the given channel status and peer cache. */
    private ClprSyncHandler buildHandler(ClprChannelStatus status) {
        return buildHandler(status, new SimpleMetrics());
    }

    /** Build a handler with a custom metrics instance. */
    private ClprSyncHandler buildHandler(ClprChannelStatus status, SimpleMetrics metrics) {
        // The response signer uses a dev key (all 0x01) so it won't interfere with peer key checks
        return new ClprSyncHandler(
                id -> proofParser,
                new FixedBundleConstructor(),
                submitter,
                id -> new ThrottleEnforcer(Long.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE),
                new StubStateReader(status),
                new PeerManifestVersionCache(),
                metrics,
                "");
    }

    /** Build a handler using the given submitter (rather than the default {@link #submitter}). */
    private ClprSyncHandler buildHandlerWithSubmitter(
            ClprChannelStatus status, TransactionSubmitter txSubmitter, SimpleMetrics metrics) {
        return new ClprSyncHandler(
                _ -> proofParser,
                new FixedBundleConstructor(),
                txSubmitter,
                id -> new ThrottleEnforcer(0, 0, 0),
                new StubStateReader(status),
                new PeerManifestVersionCache(),
                metrics,
                "");
    }

    /**
     * Build an inbound payload signed with the peer's ECDSA key.
     */
    private ClprSyncPayload syncPayload(Bytes bundle) {
        return ClprSyncPayload.newBuilder()
                .channelId(CHANNEL_ID)
                .bundlePayload(bundle)
                .build();
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Test
    void handleSync_doesNotRejectClosedChannel() {
        // CLOSED → skips parse and submission; non-empty-proof response
        final var handler = buildHandler(ClprChannelStatus.CLOSED);
        final var response = handler.handleSync(
                ClprSyncPayload.newBuilder()
                        .channelId(CHANNEL_ID)
                        .bundlePayload(BUNDLE_PAYLOAD)
                        .build(),
                null);

        assertThat(submitter.called).isFalse();
        assertThat(proofParser.isParseCalled()).isFalse();
        assertThat(response.bundlePayload()).isNotEqualTo(Bytes.EMPTY);
        assertThat(response.channelId()).isEqualTo(CHANNEL_ID);
    }

    @Test
    void handleSync_repliesWhenSubmissionReverts() {
        // The handler delegates the submission (fire-and-forget) and returns a normal (non-empty-proof)
        // response regardless of the eventual on-chain outcome. The reverted-outcome metric is owned by
        // AccountTransactionSubmitter and covered in its own test; here we only assert the handler's own
        // behaviour.
        final var metrics = new SimpleMetrics();
        final var revertingSubmitter = new RevertingSubmitter();
        final var handler = buildHandlerWithSubmitter(ClprChannelStatus.ACTIVE, revertingSubmitter, metrics);
        final var response = handler.handleSync(
                ClprSyncPayload.newBuilder()
                        .channelId(CHANNEL_ID)
                        .bundlePayload(BUNDLE_PAYLOAD)
                        .build(),
                null);

        assertThat(revertingSubmitter.called).isTrue();
        assertThat(response.channelId()).isEqualTo(CHANNEL_ID);
        assertThat(response.bundlePayload()).isNotEqualTo(Bytes.EMPTY);
    }

    @Test
    void handleSync_skipsSubmission_whenThrottleRejectsBundle() {
        // ThrottleEnforcer with maxSyncBytes=1: BUNDLE_PAYLOAD ("bundle-data", 11 bytes) exceeds it.
        // Parse is still called (throttle check happens after proof parsing), but submission
        // is skipped and the handler still returns our cached proof.

        final var tightThrottle = new ThrottleEnforcer(1, 0, 0); // maxSyncBytes=1, others unlimited
        final var handler = new ClprSyncHandler(
                id -> proofParser,
                new FixedBundleConstructor(),
                submitter,
                id -> tightThrottle,
                new StubStateReader(ClprChannelStatus.ACTIVE),
                new PeerManifestVersionCache(),
                new SimpleMetrics(),
                "");

        final var response = handler.handleSync(
                ClprSyncPayload.newBuilder()
                        .channelId(CHANNEL_ID)
                        .bundlePayload(BUNDLE_PAYLOAD)
                        .build(),
                null);

        assertThat(proofParser.isParseCalled()).isTrue();
        assertThat(submitter.called).isFalse();
        assertThat(response.channelId()).isEqualTo(CHANNEL_ID);
        assertThat(response.bundlePayload()).isNotEqualTo(Bytes.EMPTY); // our cached proof is still returned
    }

    @Test
    void handleSync_noChannelRecord_returnsEmptyProof() {
        // No on-chain Channel record (PENDING commit-only or never registered) → empty-proof reply.
        final var handler = new ClprSyncHandler(
                id -> proofParser,
                new FixedBundleConstructor(),
                submitter,
                id -> new ThrottleEnforcer(Long.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE),
                new StubStateReader(ClprChannelStatus.ACTIVE) {
                    @Override
                    public Optional<ClprChannel> readChannelState(Bytes channelId, String blockTag) {
                        return Optional.empty();
                    }
                },
                new PeerManifestVersionCache(),
                new SimpleMetrics(),
                "");

        final var response = handler.handleSync(syncPayload(BUNDLE_PAYLOAD), null);

        assertThat(response.bundlePayload()).isEqualTo(Bytes.EMPTY);
        assertThat(response.channelId()).isEqualTo(CHANNEL_ID);
        assertThat(submitter.called).isFalse();
        assertThat(proofParser.isParseCalled()).isFalse();
    }

    @Test
    void handleSync_pendingChannel_returnsEmptyProof() {
        // PENDING → the channel is not yet usable; reply empty-proof and skip parse/submit.
        final var handler = buildHandler(ClprChannelStatus.PENDING);

        final var response = handler.handleSync(syncPayload(BUNDLE_PAYLOAD), null);

        assertThat(response.bundlePayload()).isEqualTo(Bytes.EMPTY);
        assertThat(submitter.called).isFalse();
        assertThat(proofParser.isParseCalled()).isFalse();
    }

    @Test
    void handleSync_parseFailure_stillRepliesWithoutSubmitting() {
        // A codec that throws while parsing the peer bundle → parse_error is trapped; submission is
        // skipped (parsed == null) but we still return our latest cached proof.
        final BundlePayloadCodec throwingCodec = new BundlePayloadCodec() {
            @Override
            public ParsedBundle decodeBundle(Bytes proofBytes) {
                throw new IllegalStateException("malformed peer proof");
            }

            @Override
            public ParsedBundle parseBundle(Bytes proofBytes, long receivedMessageId) {
                throw new IllegalStateException("malformed peer proof");
            }
        };
        final var handler = new ClprSyncHandler(
                id -> throwingCodec,
                new FixedBundleConstructor(),
                submitter,
                id -> new ThrottleEnforcer(Long.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE),
                new StubStateReader(ClprChannelStatus.ACTIVE),
                new PeerManifestVersionCache(),
                new SimpleMetrics(),
                "");

        final var response = handler.handleSync(syncPayload(BUNDLE_PAYLOAD), null);

        assertThat(submitter.called).isFalse();
        assertThat(response.bundlePayload()).isNotEqualTo(Bytes.EMPTY);
        assertThat(response.channelId()).isEqualTo(CHANNEL_ID);
    }

    @Test
    void handleSync_submitFailure_stillReplies() {
        // The submitter throwing must be trapped (submit_error) so the peer still receives our proof.
        final TransactionSubmitter throwingSubmitter = new TransactionSubmitter() {
            @Override
            public void submitBundle(@NonNull ClprChannel channel, @NonNull ParsedBundle bundle) {
                throw new IllegalStateException("submit blew up");
            }
        };
        final var handler = new ClprSyncHandler(
                id -> proofParser,
                new FixedBundleConstructor(),
                throwingSubmitter,
                id -> new ThrottleEnforcer(Long.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE),
                new StubStateReader(ClprChannelStatus.ACTIVE),
                new PeerManifestVersionCache(),
                new SimpleMetrics(),
                "");

        final var response = handler.handleSync(syncPayload(BUNDLE_PAYLOAD), null);

        assertThat(response.bundlePayload()).isNotEqualTo(Bytes.EMPTY);
        assertThat(response.channelId()).isEqualTo(CHANNEL_ID);
    }
}
