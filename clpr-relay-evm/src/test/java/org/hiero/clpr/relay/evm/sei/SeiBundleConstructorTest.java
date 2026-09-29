// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.sei;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.ENDPOINT_MANIFEST_COMMITMENT_SLOT;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.buildEvmStorageAbciKey;

import com.hedera.hapi.node.state.clpr.ClprBundleContent;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprEndpointManifest;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprMessageValue;
import com.hedera.hapi.node.state.clpr.ClprSeiBundlePayload;
import com.hedera.hapi.node.state.clpr.SeiHeader;
import com.hedera.hapi.node.state.clpr.SeiSignedHeader;
import com.hedera.hapi.node.state.clpr.SeiValidatorEntry;
import com.hedera.hapi.node.state.clpr.SeiValidatorSet;
import com.hedera.hapi.node.state.clpr.SeiValidatorSetUpdate;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.hiero.clpr.relay.core.BundleConstructor;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.PeerManifestVersionCache;
import org.hiero.clpr.relay.evm.BundleConstructorManifestContract;
import org.hiero.clpr.relay.evm.model.Address;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for {@link SeiBundleConstructor}, exercising every branch of {@code onStateChanged}
 * against a stubbed {@link CometBftRpcClient} (no Docker required).
 */
class SeiBundleConstructorTest {

    private static final Address ADDR = Address.fromHexString("0x" + "cc".repeat(20));
    private static final Bytes CONN_ID = Bytes.wrap(new byte[32]);
    private static final Bytes OTHER_CONN_ID = Bytes.fromHex("00".repeat(31) + "ff");

    private static final Bytes VALIDATORS_HASH = Bytes.fromHex("aa".repeat(32));
    private static final Bytes NEXT_VALIDATORS_HASH_DIFFERENT = Bytes.fromHex("bb".repeat(32));

    private static final Bytes RUNNING_HASH =
            Bytes.fromHex("cafecafe000000000000000000000000000000000000000000000000cafecafe");
    private static final Bytes IAVL_PROOF = Bytes.fromHex("aabb");
    private static final Bytes MULTISTORE_PROOF = Bytes.fromHex("ccdd");

    private static final BigInteger STATE_HEIGHT = BigInteger.valueOf(100);

    // ── cache / construction ────────────────────────────────────────────────────

    @Test
    void getLatestBundlePayload_emptyBeforeAnyStateChange() {
        final var constructor = new SeiBundleConstructor(ADDR, 10, 10, new StubCometBftRpcClient());
        assertThat(constructor.getLatestBundlePayload(CONN_ID)).isEmpty();
    }

    @Test
    void constructor_rejectsNonPositiveMaxMessages() {
        assertThatThrownBy(() -> new SeiBundleConstructor(ADDR, 0, 10, new StubCometBftRpcClient()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ── happy paths ──────────────────────────────────────────────────────────────

    @Test
    void onStateChanged_withMessage_cachesDeserializablePayloadWithFiveProofs() throws Exception {
        final var cometBftRpcClient = stubNoRotation();
        final var constructor = new SeiBundleConstructor(ADDR, 10, 10, cometBftRpcClient);

        constructor.onStateChanged(STATE_HEIGHT, CONN_ID, channel(0L), List.of(message(1L, RUNNING_HASH)));

        final var raw = constructor.getLatestBundlePayload(CONN_ID).orElseThrow();
        final var payload = ClprSeiBundlePayload.PROTOBUF.parse(raw);

        assertThat(payload.hasStateProof()).isTrue();
        assertThat(payload.stateProof().signedHeader()).isEqualTo(cometBftRpcClient.signedHeaderResponse);
        assertThat(payload.stateProof().multistoreProof()).isEqualTo(MULTISTORE_PROOF);
        assertThat(payload.stateProof().storageProofs()).hasSize(6); // 5 channel fields + 1 message hash
        assertThat(payload.hasNextValidatorSet()).isFalse();
        assertThat(payload.bundleContent().length()).isGreaterThan(0);
        assertThat(ClprBundleContent.PROTOBUF.parse(payload.bundleContent()).messages())
                .hasSize(1);

        // Header is anchored at stateHeight + 1; proofs are queried at stateHeight.
        assertThat(cometBftRpcClient.lastSignedHeaderHeight).isEqualTo(101L);
        assertThat(cometBftRpcClient.lastAbciHeight).isEqualTo(100L);
        assertThat(cometBftRpcClient.abciCallCount).isEqualTo(6);
        // No rotation → validator set must not be fetched.
        assertThat(cometBftRpcClient.lastValidatorSetHeight).isEqualTo(-1L);

        // The cache is keyed per channel.
        assertThat(constructor.getLatestBundlePayload(OTHER_CONN_ID)).isEmpty();
    }

    @Test
    void onStateChanged_ackOnlyNoMessages_cachesFourProofs() throws Exception {
        final var cometBftRpcClient = stubNoRotation();
        final var constructor = new SeiBundleConstructor(ADDR, 10, 10, cometBftRpcClient);

        // No pending messages, but a received message id > 0 means there is an ACK to prove.
        constructor.onStateChanged(STATE_HEIGHT, CONN_ID, channel(5L), List.of());

        final var payload = ClprSeiBundlePayload.PROTOBUF.parse(
                constructor.getLatestBundlePayload(CONN_ID).orElseThrow());
        assertThat(payload.stateProof().storageProofs()).hasSize(5); // 5 channel fields, no message-hash slot
        assertThat(ClprBundleContent.PROTOBUF.parse(payload.bundleContent()).messages())
                .isEmpty();
        assertThat(cometBftRpcClient.abciCallCount).isEqualTo(5);
    }

    @Test
    void onStateChanged_bundleExceedsMaxBundleSize_notEmitted() {
        final var cometBftRpcClient = stubNoRotation();
        // maxBundleSize = 1 byte → any assembled proof exceeds it, so nothing is cached or emitted.
        final var constructor = new SeiBundleConstructor(ADDR, 10, 10, cometBftRpcClient, null, null, 1L);

        constructor.onStateChanged(STATE_HEIGHT, CONN_ID, channel(5L), List.of());

        assertThat(constructor.getLatestBundlePayload(CONN_ID)).isEmpty();
    }

    @Test
    void onStateChanged_rotation_includesNextValidatorSet() throws Exception {
        final var cometBftRpcClient = stubNoRotation();
        // next_validators_hash != validators_hash → rotation required.
        cometBftRpcClient.signedHeaderResponse = signedHeader(VALIDATORS_HASH, NEXT_VALIDATORS_HASH_DIFFERENT);
        cometBftRpcClient.validatorSetResponse = validatorSet(100L);
        final var constructor = new SeiBundleConstructor(ADDR, 10, 10, cometBftRpcClient);

        // Rotation alone justifies a bundle even with no messages and no ACKs.
        constructor.onStateChanged(STATE_HEIGHT, CONN_ID, channel(0L), List.of());

        final var payload = ClprSeiBundlePayload.PROTOBUF.parse(
                constructor.getLatestBundlePayload(CONN_ID).orElseThrow());
        assertThat(payload.hasNextValidatorSet()).isTrue();
        assertThat(payload.nextValidatorSet()).isEqualTo(cometBftRpcClient.validatorSetResponse);
        // next_validators_hash at H+1 is the hash of validators at H+2.
        assertThat(cometBftRpcClient.lastValidatorSetHeight).isEqualTo(102L);
    }

    // ── deferral / skip branches (nothing cached) ────────────────────────────────

    @Test
    void onStateChanged_skipsWhenNothingToProve() {
        final var cometBftRpcClient = stubNoRotation();
        final var constructor = new SeiBundleConstructor(ADDR, 10, 10, cometBftRpcClient);

        // No messages, no ACKs (receivedMessageId 0), no rotation → nothing to prove.
        constructor.onStateChanged(STATE_HEIGHT, CONN_ID, channel(0L), List.of());

        assertThat(constructor.getLatestBundlePayload(CONN_ID)).isEmpty();
        assertThat(cometBftRpcClient.abciCallCount).isEqualTo(0);
    }

    @Test
    void onStateChanged_defersWhenRotationValidatorFetchFails() {
        final var cometBftRpcClient = stubNoRotation();
        cometBftRpcClient.signedHeaderResponse = signedHeader(VALIDATORS_HASH, NEXT_VALIDATORS_HASH_DIFFERENT);
        cometBftRpcClient.failValidatorSet = true;
        final var constructor = new SeiBundleConstructor(ADDR, 10, 10, cometBftRpcClient);

        constructor.onStateChanged(STATE_HEIGHT, CONN_ID, channel(0L), List.of());

        assertThat(constructor.getLatestBundlePayload(CONN_ID)).isEmpty();
        assertThat(cometBftRpcClient.abciCallCount).isEqualTo(0);
    }

    @Test
    void onStateChanged_defersOnRunningHashMismatch() {
        final var cometBftRpcClient = stubNoRotation();
        // The proven message-hash slot value will not match the message's running hash.
        cometBftRpcClient.abciProofResult =
                new CometBftRpcClient.SeiAbciProofResult(Bytes.fromHex("dd".repeat(32)), IAVL_PROOF, MULTISTORE_PROOF);
        final var constructor = new SeiBundleConstructor(ADDR, 10, 10, cometBftRpcClient);

        constructor.onStateChanged(STATE_HEIGHT, CONN_ID, channel(0L), List.of(message(1L, RUNNING_HASH)));

        assertThat(constructor.getLatestBundlePayload(CONN_ID)).isEmpty();
    }

    @Test
    void onStateChanged_defersOnEmptyMultistoreProof() {
        final var cometBftRpcClient = stubNoRotation();
        cometBftRpcClient.abciProofResult =
                new CometBftRpcClient.SeiAbciProofResult(RUNNING_HASH, IAVL_PROOF, Bytes.EMPTY);
        final var constructor = new SeiBundleConstructor(ADDR, 10, 10, cometBftRpcClient);

        constructor.onStateChanged(STATE_HEIGHT, CONN_ID, channel(0L), List.of(message(1L, RUNNING_HASH)));

        assertThat(constructor.getLatestBundlePayload(CONN_ID)).isEmpty();
    }

    @Test
    void onStateChanged_defersWhenAbciQueryFails() {
        final var cometBftRpcClient = stubNoRotation();
        cometBftRpcClient.failAbci = true;
        final var constructor = new SeiBundleConstructor(ADDR, 10, 10, cometBftRpcClient);

        constructor.onStateChanged(STATE_HEIGHT, CONN_ID, channel(0L), List.of(message(1L, RUNNING_HASH)));

        assertThat(constructor.getLatestBundlePayload(CONN_ID)).isEmpty();
    }

    // ── truncation ───────────────────────────────────────────────────────────────

    @Test
    void onStateChanged_truncatesToMaxMessagesPerBundle() throws Exception {
        final var cometBftRpcClient = stubNoRotation();
        final var constructor = new SeiBundleConstructor(ADDR, 2, 10, cometBftRpcClient);

        // Five pending messages, but maxMessagesPerBundle is 2 → only the first two are included.
        // Every message shares RUNNING_HASH so the last *included* one passes the consistency check.
        final var messages = List.of(
                message(1L, RUNNING_HASH),
                message(2L, RUNNING_HASH),
                message(3L, RUNNING_HASH),
                message(4L, RUNNING_HASH),
                message(5L, RUNNING_HASH));

        constructor.onStateChanged(STATE_HEIGHT, CONN_ID, channel(0L), messages);

        final var payload = ClprSeiBundlePayload.PROTOBUF.parse(
                constructor.getLatestBundlePayload(CONN_ID).orElseThrow());
        assertThat(ClprBundleContent.PROTOBUF.parse(payload.bundleContent()).messages())
                .hasSize(2);
        assertThat(payload.stateProof().storageProofs()).hasSize(6); // 5 channel fields + 1 message hash
        assertThat(cometBftRpcClient.abciCallCount).isEqualTo(6);
    }

    // ── endpoint-manifest proof branch (spec §4.2 Step 1b) ───────────────────────

    @Test
    void onStateChanged_manifestDue_attachesManifestProof() throws Exception {
        final var cometBftRpcClient = stubNoRotation();
        final var manifest = ClprEndpointManifest.newBuilder()
                .version(1L)
                .serviceAddress(Bytes.wrap(new byte[20]))
                .build();
        final var constructor = new SeiBundleConstructor(
                ADDR, 10, 10, cometBftRpcClient, new StubManifestReader(manifest), new PeerManifestVersionCache());

        // Ack-only bundle (5 queue slots) + the manifest commitment slot = 6 ABCI queries.
        constructor.onStateChanged(STATE_HEIGHT, CONN_ID, channel(5L), List.of());

        final var payload = ClprSeiBundlePayload.PROTOBUF.parse(
                constructor.getLatestBundlePayload(CONN_ID).orElseThrow());
        assertThat(payload.hasManifestStorageProof()).isTrue();
        assertThat(payload.manifestStorageProof().key())
                .isEqualTo(Bytes.wrap(buildEvmStorageAbciKey(ADDR, ENDPOINT_MANIFEST_COMMITMENT_SLOT)));
        assertThat(payload.endpointManifest()).isEqualTo(ClprEndpointManifest.PROTOBUF.toBytes(manifest));
        assertThat(cometBftRpcClient.abciCallCount).isEqualTo(6);
    }

    @Test
    void onStateChanged_peerAlreadyCurrent_skipsManifestProof() throws Exception {
        final var cometBftRpcClient = stubNoRotation();
        final var manifest = ClprEndpointManifest.newBuilder()
                .version(1L)
                .serviceAddress(Bytes.wrap(new byte[20]))
                .build();
        // The peer has already reported holding version 1, so 1 > 1 is false → no manifest proof.
        final var peerVersions = new PeerManifestVersionCache();
        peerVersions.record(CONN_ID, 1L);
        final var constructor = new SeiBundleConstructor(
                ADDR, 10, 10, cometBftRpcClient, new StubManifestReader(manifest), peerVersions);

        constructor.onStateChanged(STATE_HEIGHT, CONN_ID, channel(5L), List.of());

        final var payload = ClprSeiBundlePayload.PROTOBUF.parse(
                constructor.getLatestBundlePayload(CONN_ID).orElseThrow());
        assertThat(payload.hasManifestStorageProof()).isFalse();
        assertThat(payload.endpointManifest()).isEqualTo(Bytes.EMPTY);
        assertThat(cometBftRpcClient.abciCallCount).isEqualTo(5); // queue slots only, no manifest slot
    }

    @Test
    void onStateChanged_emptyServiceAddressManifest_skipsManifestProof() throws Exception {
        final var cometBftRpcClient = stubNoRotation();
        // A version >= 1 manifest with an empty serviceAddress must not be proven (it would be rejected
        // on-chain by ManifestServiceAddressMismatch).
        final var manifest = ClprEndpointManifest.newBuilder().version(1L).build();
        final var constructor = new SeiBundleConstructor(
                ADDR, 10, 10, cometBftRpcClient, new StubManifestReader(manifest), new PeerManifestVersionCache());

        constructor.onStateChanged(STATE_HEIGHT, CONN_ID, channel(5L), List.of());

        final var payload = ClprSeiBundlePayload.PROTOBUF.parse(
                constructor.getLatestBundlePayload(CONN_ID).orElseThrow());
        assertThat(payload.hasManifestStorageProof()).isFalse();
        assertThat(cometBftRpcClient.abciCallCount).isEqualTo(5);
    }

    @Test
    void onStateChanged_noStateReader_attachesNoManifestProof() throws Exception {
        final var cometBftRpcClient = stubNoRotation();
        final var constructor = new SeiBundleConstructor(ADDR, 10, 10, cometBftRpcClient); // no manifest reader wired

        constructor.onStateChanged(STATE_HEIGHT, CONN_ID, channel(5L), List.of());

        final var payload = ClprSeiBundlePayload.PROTOBUF.parse(
                constructor.getLatestBundlePayload(CONN_ID).orElseThrow());
        assertThat(payload.hasManifestStorageProof()).isFalse();
        assertThat(cometBftRpcClient.abciCallCount).isEqualTo(5);
    }

    // ── fixtures ─────────────────────────────────────────────────────────────────

    private static StubCometBftRpcClient stubNoRotation() {
        final var stub = new StubCometBftRpcClient();
        stub.signedHeaderResponse = signedHeader(VALIDATORS_HASH, VALIDATORS_HASH); // equal → no rotation
        stub.abciProofResult = new CometBftRpcClient.SeiAbciProofResult(RUNNING_HASH, IAVL_PROOF, MULTISTORE_PROOF);
        return stub;
    }

    private static SeiSignedHeader signedHeader(final Bytes validatorsHash, final Bytes nextValidatorsHash) {
        return SeiSignedHeader.newBuilder()
                .header(SeiHeader.newBuilder()
                        .height(101L)
                        .chainId("sei-local")
                        .validatorsHash(validatorsHash)
                        .nextValidatorsHash(nextValidatorsHash)
                        .build())
                .build();
    }

    private static SeiValidatorSet validatorSet(final long votingPower) {
        return SeiValidatorSet.newBuilder()
                .validators(List.of(SeiValidatorEntry.newBuilder()
                        .ed25519PubKey(Bytes.wrap(new byte[32]))
                        .votingPower(votingPower)
                        .build()))
                .build();
    }

    private static ClprChannel channel(final long receivedMessageId) {
        return ClprChannel.newBuilder()
                .channelId(CONN_ID)
                .status(ClprChannelStatus.ACTIVE)
                .nextMessageId(2L)
                .ackedMessageId(0L)
                .sentRunningHash(Bytes.wrap(new byte[32]))
                .receivedMessageId(receivedMessageId)
                .receivedRunningHash(Bytes.wrap(new byte[32]))
                .trustAnchorId(Bytes.EMPTY)
                .remoteTrustAnchorId(VALIDATORS_HASH.append(
                        Bytes.wrap(ByteBuffer.allocate(8).putLong(101L).array())))
                .build();
    }

    private static ContractStateReader.QueuedMessage message(final long id, final Bytes runningHash) {
        final var value = ClprMessageValue.newBuilder()
                .payload(ClprMessagePayload.newBuilder().build())
                .runningHashAfterProcessing(runningHash)
                .build();
        return new ContractStateReader.QueuedMessage(BigInteger.valueOf(id), value);
    }

    /** Minimal {@link ContractStateReader} that only supplies a fixed local endpoint manifest. */
    private record StubManifestReader(ClprEndpointManifest manifest) implements ContractStateReader {
        @Override
        public ClprEndpointManifest readEndpointManifest(final String blockTag) {
            return manifest;
        }

        @Override
        public Optional<ClprChannel> readChannelState(final Bytes channelId, final String blockTag) {
            return Optional.empty();
        }

        @Override
        public List<QueuedMessage> readQueuedMessages(
                final Bytes channelId, final long fromId, final long toId, final String blockTag) {
            return List.of();
        }

        @Override
        public ClprLedgerConfiguration readLedgerConfiguration(final CommitmentLevel commitmentLevel) {
            return ClprLedgerConfiguration.DEFAULT;
        }
    }

    /** Stub CometBFT client returning pre-configured responses and recording call arguments. */
    private static final class StubCometBftRpcClient extends CometBftRpcClient {
        SeiSignedHeader signedHeaderResponse = SeiSignedHeader.DEFAULT;
        SeiValidatorSet validatorSetResponse = SeiValidatorSet.DEFAULT;
        CometBftRpcClient.SeiAbciProofResult abciProofResult =
                new CometBftRpcClient.SeiAbciProofResult(Bytes.EMPTY, Bytes.EMPTY, Bytes.EMPTY);
        boolean failValidatorSet;
        boolean failAbci;

        long lastSignedHeaderHeight = -1L;
        long lastValidatorSetHeight = -1L;
        long lastAbciHeight = -1L;
        int abciCallCount;

        StubCometBftRpcClient() {
            super("http://localhost:1", 0, Duration.ofSeconds(1));
        }

        @Override
        public SeiSignedHeader getSignedHeader(final long height) {
            this.lastSignedHeaderHeight = height;
            return signedHeaderResponse;
        }

        @Override
        public SeiValidatorSet getValidatorSet(final long height) {
            this.lastValidatorSetHeight = height;
            if (failValidatorSet) {
                throw new RuntimeException("validator-set fetch failed");
            }
            return validatorSetResponse;
        }

        @Override
        public SeiAbciProofResult abciQuery(final byte[] key, final long height) {
            this.lastAbciHeight = height;
            this.abciCallCount++;
            if (failAbci) {
                throw new RuntimeException("abci query failed");
            }
            return abciProofResult;
        }
    }

    // ── parametrized trust-anchor rotation scenarios ─────────────────────────────

    private enum Outcome {
        /** onStateChanged returns without caching anything. */
        NO_BUNDLE,
        /** A full bundle: state proof + content, optionally next_validator_set and/or prior updates. */
        REGULAR,
        /** A rotation-only bundle: prior_validator_set_updates only, no state proof or content. */
        ROTATION_ONLY
    }

    /**
     * One rotation scenario. {@code changeIntoHeights} lists the CometBFT heights at which the
     * validator set changes (the set active at {@code h} differs from {@code h-1}); every header and
     * validator set is then derived deterministically by {@link ChainStub}. {@code priorAnnounceHeights}
     * are the heights whose headers are expected, in order, in {@code prior_validator_set_updates};
     * {@code nextValidatorSetHeight} is the height of the expected head-block {@code next_validator_set}
     * (or {@code null} when none).
     */
    private record Scenario(
            String name,
            long[] changeIntoHeights,
            Bytes remoteTrustAnchorId,
            boolean withMessage,
            int maxPriorValidatorSetUpdates,
            Outcome outcome,
            long[] priorAnnounceHeights,
            Long nextValidatorSetHeight) {
        @Override
        public String toString() {
            return name;
        }
    }

    private static Stream<Scenario> rotationScenarios() {
        // STATE_HEIGHT = 100 → proof/header height = 101; the scan covers [anchorHeight, 100).
        return Stream.of(
                // Remote anchor already on the head validator set → regular bundle, no rotations.
                new Scenario(
                        "remote up-to-date, no head change, with message → regular / no rotations",
                        new long[] {},
                        anchorId(epochHash(0), 50L),
                        true,
                        10,
                        Outcome.REGULAR,
                        new long[] {},
                        null),
                // Remote up-to-date, but the set changes at the next block → next_validator_set rides along.
                new Scenario(
                        "remote up-to-date, head changing → regular with next_validator_set",
                        new long[] {102},
                        anchorId(epochHash(0), 101L),
                        false,
                        10,
                        Outcome.REGULAR,
                        new long[] {},
                        102L),
                // Remote two sets behind, both rotations fit → regular bundle carrying the catch-up + content.
                new Scenario(
                        "remote behind by 2, complete catch-up → regular carrying 2 prior updates",
                        new long[] {60, 80},
                        anchorId(epochHash(0), 50L),
                        true,
                        10,
                        Outcome.REGULAR,
                        new long[] {59, 79},
                        null),
                // Remote four sets behind, only maxPrior=2 fit → can't verify content yet → rotation-only.
                new Scenario(
                        "remote behind by 4, capped at 2 → rotation-only bundle",
                        new long[] {60, 70, 80, 90},
                        anchorId(epochHash(0), 50L),
                        false,
                        2,
                        Outcome.ROTATION_ONLY,
                        new long[] {59, 69},
                        null),
                // Remote anchor pinned to a height beyond our head → our RPC is behind → no bundle.
                new Scenario(
                        "remote anchor height ahead of head → no bundle",
                        new long[] {},
                        anchorId(epochHash(1), 200L),
                        false,
                        10,
                        Outcome.NO_BUNDLE,
                        new long[] {},
                        null),
                // Unset/short remoteTrustAnchorId → falls back to the current head anchor → regular bundle.
                new Scenario(
                        "empty remoteTrustAnchorId falls back to default → regular / no rotations",
                        new long[] {},
                        Bytes.EMPTY,
                        true,
                        10,
                        Outcome.REGULAR,
                        new long[] {},
                        null));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rotationScenarios")
    void onStateChanged_producesExpectedRotations(final Scenario s) throws Exception {
        final var stub = new ChainStub(s.changeIntoHeights());
        final var constructor = new SeiBundleConstructor(ADDR, 10, s.maxPriorValidatorSetUpdates(), stub);
        final var messages =
                s.withMessage() ? List.of(message(1L, RUNNING_HASH)) : List.<ContractStateReader.QueuedMessage>of();

        constructor.onStateChanged(STATE_HEIGHT, CONN_ID, channel(0L, s.remoteTrustAnchorId()), messages);

        final var cached = constructor.getLatestBundlePayload(CONN_ID);
        if (s.outcome() == Outcome.NO_BUNDLE) {
            assertThat(cached).as("expected no bundle to be cached").isEmpty();
            return;
        }
        final var payload = ClprSeiBundlePayload.PROTOBUF.parse(cached.orElseThrow());

        // Expected prior validator-set updates: for each change-announcing height h, the header at h
        // (signed by the still-trusted set) paired with the set that becomes active at h+1.
        final var expectedPrior = Arrays.stream(s.priorAnnounceHeights())
                .mapToObj(h -> SeiValidatorSetUpdate.newBuilder()
                        .signedHeader(chainHeader(s.changeIntoHeights(), h))
                        .nextValidatorSet(chainValidatorSet(s.changeIntoHeights(), h + 1))
                        .build())
                .toList();
        assertThat(payload.priorValidatorSetUpdates()).containsExactlyElementsOf(expectedPrior);

        if (s.outcome() == Outcome.ROTATION_ONLY) {
            assertThat(payload.hasStateProof())
                    .as("rotation-only carries no state proof")
                    .isFalse();
            assertThat(payload.bundleContent().length())
                    .as("rotation-only carries no content")
                    .isZero();
            assertThat(payload.hasNextValidatorSet()).isFalse();
        } else { // REGULAR
            assertThat(payload.hasStateProof())
                    .as("regular bundle carries a state proof")
                    .isTrue();
            if (s.nextValidatorSetHeight() == null) {
                assertThat(payload.hasNextValidatorSet())
                        .as("no head-block rotation → no next_validator_set")
                        .isFalse();
            } else {
                assertThat(payload.hasNextValidatorSet()).isTrue();
                assertThat(payload.nextValidatorSet())
                        .isEqualTo(chainValidatorSet(s.changeIntoHeights(), s.nextValidatorSetHeight()));
            }
        }
    }

    // ── chain model helpers (shared by ChainStub and the expected-value builders) ────

    /** The validator set at {@code height} is identified by its "epoch" = number of changes at/below it. */
    private static int epochAt(final long[] changeIntoHeights, final long height) {
        int epoch = 0;
        for (final long c : changeIntoHeights) {
            if (height >= c) {
                epoch++;
            }
        }
        return epoch;
    }

    /** A distinct 32-byte validators hash per epoch. */
    private static Bytes epochHash(final int epoch) {
        return Bytes.fromHex(String.format("%02x", epoch & 0xff).repeat(32));
    }

    private static SeiSignedHeader chainHeader(final long[] changeIntoHeights, final long height) {
        return SeiSignedHeader.newBuilder()
                .header(SeiHeader.newBuilder()
                        .height(height)
                        .chainId("sei-local")
                        .validatorsHash(epochHash(epochAt(changeIntoHeights, height)))
                        .nextValidatorsHash(epochHash(epochAt(changeIntoHeights, height + 1)))
                        .build())
                .build();
    }

    /** A distinct validator set per epoch (encoded in the single validator's pubkey). */
    private static SeiValidatorSet chainValidatorSet(final long[] changeIntoHeights, final long height) {
        final byte[] pubKey = new byte[32];
        pubKey[0] = (byte) epochAt(changeIntoHeights, height);
        return SeiValidatorSet.newBuilder()
                .validators(List.of(SeiValidatorEntry.newBuilder()
                        .ed25519PubKey(Bytes.wrap(pubKey))
                        .votingPower(100L)
                        .build()))
                .build();
    }

    /** Encode a trust anchor id as validators_hash (32 bytes) || height (8 bytes, big-endian). */
    private static Bytes anchorId(final Bytes validatorsHash, final long height) {
        return validatorsHash.append(
                Bytes.wrap(ByteBuffer.allocate(8).putLong(height).array()));
    }

    private static ClprChannel channel(final long receivedMessageId, final Bytes remoteTrustAnchorId) {
        return ClprChannel.newBuilder()
                .channelId(CONN_ID)
                .status(ClprChannelStatus.ACTIVE)
                .nextMessageId(2L)
                .ackedMessageId(0L)
                .sentRunningHash(Bytes.wrap(new byte[32]))
                .receivedMessageId(receivedMessageId)
                .receivedRunningHash(Bytes.wrap(new byte[32]))
                .trustAnchorId(Bytes.EMPTY)
                .remoteTrustAnchorId(remoteTrustAnchorId)
                .build();
    }

    /**
     * Height-aware CometBFT stub: models a chain whose validator set changes at the configured
     * heights. {@code getSignedHeader(h)} and {@code getValidatorSet(h)} are pure functions of the
     * height, so a test can rebuild the exact expected headers/sets independently.
     */
    private static final class ChainStub extends CometBftRpcClient {
        private final long[] changeIntoHeights;

        ChainStub(final long[] changeIntoHeights) {
            super("http://localhost:1", 0, Duration.ofSeconds(1));
            this.changeIntoHeights = changeIntoHeights;
        }

        @Override
        public SeiSignedHeader getSignedHeader(final long height) {
            return chainHeader(changeIntoHeights, height);
        }

        @Override
        public SeiValidatorSet getValidatorSet(final long height) {
            return chainValidatorSet(changeIntoHeights, height);
        }

        @Override
        public SeiAbciProofResult abciQuery(final byte[] key, final long height) {
            // Return the message running hash so the last-message consistency check passes.
            return new CometBftRpcClient.SeiAbciProofResult(RUNNING_HASH, IAVL_PROOF, MULTISTORE_PROOF);
        }
    }

    /**
     * {@link BundleConstructorManifestContract} for the CometBFT/Sei format: the manifest proof rides in
     * the dedicated {@code manifest_storage_proof} field of the bundle payload.
     */
    @Nested
    class ManifestAttachmentContract extends BundleConstructorManifestContract {
        @Override
        protected BundleConstructor newConstructor(
                final ContractStateReader manifestReader, final PeerManifestVersionCache peerVersions) {
            return new SeiBundleConstructor(ADDR, 10, 10, stubNoRotation(), manifestReader, peerVersions);
        }

        @Override
        protected void driveOneBundle(final BundleConstructor constructor) {
            // Ack-only bundle (receivedMessageId > 0, no pending messages) → a bundle is produced.
            constructor.onStateChanged(STATE_HEIGHT, CONN, channel(5L), List.of());
        }

        @Override
        protected boolean manifestAttached(final Bytes payload) throws Exception {
            return ClprSeiBundlePayload.PROTOBUF.parse(payload).hasManifestStorageProof();
        }
    }
}
