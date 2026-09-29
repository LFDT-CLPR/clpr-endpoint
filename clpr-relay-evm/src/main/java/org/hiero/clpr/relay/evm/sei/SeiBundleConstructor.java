// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.sei;

import static org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient.toBlockTag;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.CH_OFFSET_ENDPOINT_MANIFEST_VERSION;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.CH_OFFSET_RECEIVED_MSG_ID;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.CH_OFFSET_RECEIVED_RUNNING_HASH;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.CH_OFFSET_SENT_RUNNING_HASH;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.CH_OFFSET_STATUS_AND_NEXT_MSG_ID;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.ENDPOINT_MANIFEST_COMMITMENT_SLOT;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.STORE_KEY_EVM;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.buildEvmStorageAbciKey;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.calculateChannelFieldStorageSlot;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.calculateMsgRunningHashStorageSlot;

import com.hedera.hapi.node.state.clpr.ClprBundleContent;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprEndpointManifest;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprMessageValue;
import com.hedera.hapi.node.state.clpr.ClprQueueMetadata;
import com.hedera.hapi.node.state.clpr.ClprSeiBundlePayload;
import com.hedera.hapi.node.state.clpr.SeiHeader;
import com.hedera.hapi.node.state.clpr.SeiSignedHeader;
import com.hedera.hapi.node.state.clpr.SeiStateProof;
import com.hedera.hapi.node.state.clpr.SeiStorageProofEntry;
import com.hedera.hapi.node.state.clpr.SeiValidatorSet;
import com.hedera.hapi.node.state.clpr.SeiValidatorSetUpdate;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.logging.api.Logger;
import com.swirlds.logging.api.Loggers;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.hiero.clpr.relay.core.BundleConstructor;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.ManifestProofPolicy;
import org.hiero.clpr.relay.core.PeerManifestVersionCache;
import org.hiero.clpr.relay.evm.ByteUtils;
import org.hiero.clpr.relay.evm.model.Address;
import org.jspecify.annotations.Nullable;

/**
 * An implementation of {@link BundleConstructor} that creates Sei bundles backed by
 * CometBFT ICS-23 state proofs.
 *
 * <p>The contract storage layout (Solidity slot calculations) is identical to the QBFT
 * implementation: both target the same {@code ClprService} contract. What differs is the
 * proof mechanism: instead of EIP-1186 MPT proofs anchored to an RLP block header, each
 * storage slot is proven via an ABCI query returning an ICS-23 IAVL proof, and the whole
 * proof package is anchored to a CometBFT signed header.
 *
 * <p>Trust-anchor rotation is handled by including a {@code next_validator_set} in the bundle
 * when the signed header's {@code next_validators_hash} differs from its {@code validators_hash},
 * indicating the validator set will change at the next block.
 */
public final class SeiBundleConstructor implements BundleConstructor {

    record SeiTrustAnchorId(Bytes validatorsHash, long height) {}

    private static final Logger log = Loggers.getLogger(SeiBundleConstructor.class);

    private static final int VALIDATORS_HASH_LENGTH = 32;

    private static final int ENCODED_TRUST_ANCHOR_ID_LENGTH = VALIDATORS_HASH_LENGTH + Long.BYTES;

    /** Default assembled bundle-proof size ceiling (bytes) when none is configured; matches gRPC 1 MiB. */
    public static final long DEFAULT_MAX_BUNDLE_SIZE = 1_048_576L;

    /** Cache of the latest bundle proof per channel ID. */
    private final ConcurrentHashMap<Bytes, Bytes> bundleProofCache = new ConcurrentHashMap<>();

    private final Address clprServiceAddress;
    private final CometBftRpcClient cometBftRpcClient;
    private final int maxMessagesPerBundle;
    private final int maxPriorValidatorSetUpdates;
    private final long maxBundleSize;

    /** Reader for the local endpoint manifest; {@code null} disables manifest proofs. */
    @Nullable
    private final ContractStateReader stateReader;

    /**
     * Per-channel record of the manifest version each peer has cached of our manifest; {@code null}
     * means "unknown for every channel" (treated as version 0).
     */
    @Nullable
    private final PeerManifestVersionCache peerManifestVersions;

    public SeiBundleConstructor(
            final Address clprServiceAddress,
            final int maxMessagesPerBundle,
            final int maxPriorValidatorSetUpdates,
            final CometBftRpcClient cometBftRpcClient) {
        this(clprServiceAddress, maxMessagesPerBundle, maxPriorValidatorSetUpdates, cometBftRpcClient, null, null);
    }

    public SeiBundleConstructor(
            final Address clprServiceAddress,
            final int maxMessagesPerBundle,
            final int maxPriorValidatorSetUpdates,
            final CometBftRpcClient cometBftRpcClient,
            @Nullable final ContractStateReader stateReader,
            @Nullable final PeerManifestVersionCache peerManifestVersions) {
        this(
                clprServiceAddress,
                maxMessagesPerBundle,
                maxPriorValidatorSetUpdates,
                cometBftRpcClient,
                stateReader,
                peerManifestVersions,
                DEFAULT_MAX_BUNDLE_SIZE);
    }

    public SeiBundleConstructor(
            final Address clprServiceAddress,
            final int maxMessagesPerBundle,
            final int maxPriorValidatorSetUpdates,
            final CometBftRpcClient cometBftRpcClient,
            @Nullable final ContractStateReader stateReader,
            @Nullable final PeerManifestVersionCache peerManifestVersions,
            final long maxBundleSize) {
        this.clprServiceAddress = clprServiceAddress;
        this.cometBftRpcClient = cometBftRpcClient;
        this.stateReader = stateReader;
        this.peerManifestVersions = peerManifestVersions;

        if (maxMessagesPerBundle < 1) {
            throw new IllegalArgumentException("maxMessagesPerBundle must be at least 1");
        }
        this.maxMessagesPerBundle = maxMessagesPerBundle;

        if (maxPriorValidatorSetUpdates < 1) {
            throw new IllegalArgumentException("maxPriorValidatorSetUpdates must be at least 1");
        }
        this.maxPriorValidatorSetUpdates = maxPriorValidatorSetUpdates;

        if (maxBundleSize < 1) {
            throw new IllegalArgumentException("maxBundleSize must be at least 1 byte");
        }
        this.maxBundleSize = maxBundleSize;
    }

    @Override
    public Optional<Bytes> getLatestBundlePayload(final Bytes channelId) {
        return Optional.ofNullable(bundleProofCache.get(channelId));
    }

    private List<SeiValidatorSetUpdate> scanForValidatorSetUpdates(
            final long fromHeight, final long maxHeight, final int maxCount) {
        final var rotations = new ArrayList<SeiValidatorSetUpdate>(maxCount);
        long height = fromHeight;
        while (rotations.size() < maxCount && height < maxHeight) {
            final var signedHeader = cometBftRpcClient.getSignedHeader(height);
            final var seiHeader = signedHeader.header();
            if (!seiHeader.nextValidatorsHash().equals(seiHeader.validatorsHash())) {
                // The validator set changes at height+1. This header (signed by the still-trusted
                // set) commits the new set via next_validators_hash; pair it with that new set so the
                // verifier can authenticate the header against its anchor and then adopt the new set.
                final var nextValidatorSet = cometBftRpcClient.getValidatorSet(height + 1);
                rotations.add(SeiValidatorSetUpdate.newBuilder()
                        .signedHeader(signedHeader)
                        .nextValidatorSet(nextValidatorSet)
                        .build());
            }
            height += 1;
        }
        return rotations;
    }

    private SeiTrustAnchorId decodeTrustAnchorId(Bytes trustAnchorId) {
        final var validatorsHash = trustAnchorId.getBytes(0, VALIDATORS_HASH_LENGTH);
        final var height = ByteBuffer.wrap(trustAnchorId.toByteArray(VALIDATORS_HASH_LENGTH, Long.BYTES))
                .getLong();
        return new SeiTrustAnchorId(validatorsHash, height);
    }

    private SeiTrustAnchorId defaultTrustAnchorId(SeiHeader seiHeader) {
        return new SeiTrustAnchorId(seiHeader.nextValidatorsHash(), seiHeader.height() + 1);
    }

    @Override
    public void onStateChanged(
            final BigInteger blockNumber,
            final Bytes channelId,
            final ClprChannel channel,
            final List<ContractStateReader.QueuedMessage> pendingMessages) {
        final long stateHeight = blockNumber.longValueExact();
        final var proofBlockHeight = stateHeight + 1;
        // Fetch the signed header at stateHeight+1.
        // Per CometBFT semantics the app_hash in header H+1 commits the application state
        // after executing block H, so ICS-23 proofs queried at H are anchored to header H+1.
        final var signedHeader = cometBftRpcClient.getSignedHeader(proofBlockHeight);
        final var seiHeader = signedHeader.header();

        // Check if we need to update the trust anchor on the remote side
        // If remoteTrustAnchorId isn't yet set, default to current head.
        final SeiTrustAnchorId remoteTrustAnchorId;
        if (channel.remoteTrustAnchorId().length() == ENCODED_TRUST_ANCHOR_ID_LENGTH) {
            remoteTrustAnchorId = decodeTrustAnchorId(channel.remoteTrustAnchorId());
        } else {
            log.warn(
                    "Could not decode Sei remoteTrustAnchorId (= {}). Falling back to default (current header).",
                    channel.remoteTrustAnchorId().toHex());
            remoteTrustAnchorId = defaultTrustAnchorId(seiHeader);
        }

        final var currentTrustAnchorMatchesValidators =
                remoteTrustAnchorId.validatorsHash().equals(seiHeader.validatorsHash());

        // The verifier's trust anchor is outdated - let's include
        // the missing validator set rotations in the bundle.
        final List<SeiValidatorSetUpdate> priorValidatorSetUpdates;
        if (!currentTrustAnchorMatchesValidators) {
            if (remoteTrustAnchorId.height() > seiHeader.height()) {
                // Remote side was updated to a _future_ (from our perspective) trust anchor.
                // That must mean that the RPC node we're connected to is behind.
                log.warn("""
                        Remote channel was updated to trust anchor ID (Sei height) {}, but our JSON RPC says
                        that the current height is {} which is behind remote channel's current trust anchor.
                        This might indicate that our JSON RPC node is lagging behind.""", remoteTrustAnchorId.height(), seiHeader.height());
                return;
            }

            priorValidatorSetUpdates = scanForValidatorSetUpdates(
                    remoteTrustAnchorId.height(), Math.max(0, proofBlockHeight - 1), maxPriorValidatorSetUpdates);
        } else {
            priorValidatorSetUpdates = new ArrayList<>();
        }

        final var verifierAbleToVerifyBundleContent =
                // Either the verifier is already up to date
                currentTrustAnchorMatchesValidators
                        ||
                        // Or we've included all necessary rotations so that it can update itself
                        // to the current validator set
                        (!priorValidatorSetUpdates.isEmpty()
                                && priorValidatorSetUpdates
                                        .getLast()
                                        .signedHeader()
                                        .header()
                                        .nextValidatorsHash()
                                        .equals(seiHeader.validatorsHash()));

        // It might happen that the verifier is behind in validator set rotations,
        // and we've run out of bundle space to provide all necessary rotations (e.g. we provide
        // 10 out of 20 missing). In that case the verifier wouldn't be able to verify the content
        // of the bundle.
        // So we're going to create a trust rotation-only bundle, with no other payload, just to push it closer
        // to a state where it can verify messages again.
        ClprSeiBundlePayload bundlePayload;
        if (!verifierAbleToVerifyBundleContent) {
            if (priorValidatorSetUpdates.isEmpty()) {
                log.warn("""
                        Remote verifier's trust anchor is outdated, but we were unable to provide the
                        necessary trust anchor rotations at height {}.""", seiHeader.height());
                return;
            }
            bundlePayload = createTrustRotationOnlyBundle(priorValidatorSetUpdates);
        } else {
            bundlePayload = createRegularBundle(
                    channel, pendingMessages, stateHeight, signedHeader, seiHeader, priorValidatorSetUpdates);
        }

        if (bundlePayload != null) {
            final Bytes payloadBytes = ClprSeiBundlePayload.PROTOBUF.toBytes(bundlePayload);
            if (payloadBytes.length() > maxBundleSize) {
                log.warn(
                        "[Sei] Assembled bundle proof for channel {} is {} bytes, over the configured maxBundleSize"
                                + " of {} bytes; not emitting it (a peer submitting it would risk a gas/size blowup)."
                                + " Lower maxMessagesPerBundle or raise maxBundleSize.",
                        channelId,
                        payloadBytes.length(),
                        maxBundleSize);
                return;
            }
            bundleProofCache.put(channelId, payloadBytes);
        }
    }

    private ClprSeiBundlePayload createRegularBundle(
            final ClprChannel channel,
            final List<ContractStateReader.QueuedMessage> pendingMessages,
            final long stateHeight,
            SeiSignedHeader signedHeader,
            SeiHeader seiHeader,
            List<SeiValidatorSetUpdate> priorValidatorSetUpdates) {
        final var messagesToInclude =
                pendingMessages.subList(0, Math.min(maxMessagesPerBundle, pendingMessages.size()));

        log.info(
                "[Sei] Attempting to create a bundle { state_height={} "
                        + "next_msg_id={} acked_msg_id={} recv_msg_id={} msg_count={}}",
                stateHeight,
                channel.nextMessageId(),
                channel.ackedMessageId(),
                channel.receivedMessageId(),
                messagesToInclude.size());

        final var validatorsChangingNow = !seiHeader.nextValidatorsHash().equals(seiHeader.validatorsHash());

        if (messagesToInclude.isEmpty()
                && priorValidatorSetUpdates.isEmpty()
                && !validatorsChangingNow
                && channel.receivedMessageId() <= 0) {
            log.debug(
                    "[Sei] Skipping bundle creation - no pending messages, ACKs or validator rotation at height={}",
                    stateHeight);
            return null;
        }

        // Step 3: Fetch next_validator_set when rotation is needed.
        // The next_validators_hash at height H+1 is the hash of validators at height H+2.
        final SeiValidatorSet nextValidatorSet;
        if (validatorsChangingNow) {
            try {
                nextValidatorSet = cometBftRpcClient.getValidatorSet(seiHeader.height() + 1);
            } catch (final Exception e) {
                log.warn(
                        "[Sei] Validator rotation required but could not fetch validators at"
                                + " height {}+2: {}. Deferring until next cycle.",
                        stateHeight,
                        e.getMessage());
                return null;
            }
        } else {
            nextValidatorSet = null;
        }

        // Step 4: Compute the storage slots to prove — same six slots as QBFT.
        // 5 mandatory channel-field slots + optional message running-hash slot.
        final var slotsToProve = new ArrayList<BigInteger>(6);
        slotsToProve.add(calculateChannelFieldStorageSlot(channel.channelId(), CH_OFFSET_STATUS_AND_NEXT_MSG_ID));
        slotsToProve.add(calculateChannelFieldStorageSlot(channel.channelId(), CH_OFFSET_RECEIVED_MSG_ID));
        slotsToProve.add(calculateChannelFieldStorageSlot(channel.channelId(), CH_OFFSET_SENT_RUNNING_HASH));
        slotsToProve.add(calculateChannelFieldStorageSlot(channel.channelId(), CH_OFFSET_RECEIVED_RUNNING_HASH));
        slotsToProve.add(calculateChannelFieldStorageSlot(channel.channelId(), CH_OFFSET_ENDPOINT_MANIFEST_VERSION));
        if (!messagesToInclude.isEmpty()) {
            final var lastMessage = messagesToInclude.getLast();
            slotsToProve.add(calculateMsgRunningHashStorageSlot(channel.channelId(), lastMessage.messageId()));
        }

        log.info(
                "[Sei] Creating a proof for bundle { height={} messages=[{}]  msg_count={} num_prior_validator_set_updates={}}",
                seiHeader.height(),
                messagesToInclude.isEmpty()
                        ? ""
                        : (messagesToInclude.getFirst().messageId() + ".."
                                + messagesToInclude.getLast().messageId()),
                messagesToInclude.size(),
                priorValidatorSetUpdates.size());

        // Step 5: ABCI-query each slot for its ICS-23 proof.
        // All slots reside in the same store ("evm") at the same height, so the multistore
        // proof (ops[1]) is identical across queries — we retain only the first one.
        final var storageProofEntries = new ArrayList<SeiStorageProofEntry>(slotsToProve.size());
        Bytes multistoreProof = null;
        for (final var slot : slotsToProve) {
            final var abciKey = buildEvmStorageAbciKey(clprServiceAddress, slot);
            final CometBftRpcClient.SeiAbciProofResult proofResult;
            try {
                proofResult = cometBftRpcClient.abciQuery(abciKey, stateHeight);
            } catch (final Exception e) {
                log.error("[Sei] ABCI query failed for slot {} at height {}: {}", slot, stateHeight, e.getMessage());
                return null;
            }
            storageProofEntries.add(SeiStorageProofEntry.newBuilder()
                    .key(Bytes.wrap(abciKey))
                    .value(proofResult.value())
                    .iavlProof(proofResult.iavlProof())
                    .build());
            if (multistoreProof == null) {
                multistoreProof = proofResult.multistoreProof();
            }
        }

        if (multistoreProof == null || multistoreProof.length() == 0) {
            log.error("[Sei] Multistore proof is empty for at block={}", seiHeader.height());
            return null;
        }

        // Step 6: Validate the last message's running-hash proof value against the in-memory
        // record, mirroring the QBFT consistency check.
        if (!messagesToInclude.isEmpty()) {
            final var lastMessage = messagesToInclude.getLast();
            final var lastMsgSlot = calculateMsgRunningHashStorageSlot(channel.channelId(), lastMessage.messageId());
            final var lastMsgAbciKey = Bytes.wrap(buildEvmStorageAbciKey(clprServiceAddress, lastMsgSlot));
            final var maybeLastProof = storageProofEntries.stream()
                    .filter(e -> e.key().equals(lastMsgAbciKey))
                    .findFirst();
            if (maybeLastProof.isEmpty()) {
                log.error("[Sei] Last message running-hash proof not found in ABCI results");
                return null;
            }
            final var provenValue = ByteUtils.leftPad32(maybeLastProof.get().value());
            final var expectedValue = ByteUtils.leftPad32(lastMessage.value().runningHashAfterProcessing());
            if (!provenValue.equals(expectedValue)) {
                log.error(
                        "[Sei] Last message running-hash mismatch: expected={} proven={}",
                        expectedValue.toHex(),
                        provenValue.toHex());
                return null;
            }
        }

        // Step 7: Assemble the bundle content.
        final ClprQueueMetadata metadata = buildQueueMetadata(channel);
        final List<ClprMessagePayload> payloads = messagesToInclude.stream()
                .map(ContractStateReader.QueuedMessage::value)
                .map(ClprMessageValue::payload)
                .toList();
        final ClprBundleContent bundleContent = ClprBundleContent.newBuilder()
                .metadata(metadata)
                .messages(payloads)
                .build();
        final Bytes serializedContent = ClprBundleContent.PROTOBUF.toBytes(bundleContent);

        // Step 7b (optional): endpoint-manifest proof (spec §4.2 Step 1b) — the CometBFT analog of the
        // QBFT bundle's slot-18 proof. The "attach only when our local manifest is ahead of the peer's
        // known version" decision is the shared ManifestProofPolicy so no codec can forget it; the
        // Sei-specific part proves the same _endpointManifest.commitment slot via an ICS-23 query at the
        // same height/store as the queue slots, so it verifies against the same signed header and
        // multistore proof. It is carried in a dedicated field rather than in storage_proofs so the
        // fixed, positional queue-slot list above is unaffected.
        SeiStorageProofEntry manifestProofEntry = null;
        Bytes manifestPreimage = null;
        final Optional<ClprEndpointManifest> manifestDue = ManifestProofPolicy.manifestToAttach(
                stateReader, peerManifestVersions, channel.channelId(), toBlockTag(stateHeight));
        if (manifestDue.isPresent()) {
            final ClprEndpointManifest manifest = manifestDue.get();
            final var manifestAbciKey = buildEvmStorageAbciKey(clprServiceAddress, ENDPOINT_MANIFEST_COMMITMENT_SLOT);
            try {
                final var manifestProofResult = cometBftRpcClient.abciQuery(manifestAbciKey, stateHeight);
                manifestProofEntry = SeiStorageProofEntry.newBuilder()
                        .key(Bytes.wrap(manifestAbciKey))
                        .value(manifestProofResult.value())
                        .iavlProof(manifestProofResult.iavlProof())
                        .build();
                manifestPreimage = ClprEndpointManifest.PROTOBUF.toBytes(manifest);
            } catch (final Exception e) {
                // A failed manifest query only drops the optional manifest proof; the queue-metadata
                // bundle is still delivered, and the proof is re-attempted on the next advance.
                log.warn(
                        "[Sei] manifest proof unavailable for channel {} (localVersion={}): {}",
                        channel.channelId().toHex(),
                        manifest.version(),
                        e.getMessage());
                manifestProofEntry = null;
                manifestPreimage = null;
            }
        }

        // Step 8: Build SeiStateProof + ClprSeiBundlePayload, then cache.
        final var stateProof = SeiStateProof.newBuilder()
                .signedHeader(signedHeader)
                .storeKey(STORE_KEY_EVM)
                .multistoreProof(multistoreProof)
                .storageProofs(storageProofEntries)
                .build();

        final var payloadBuilder =
                ClprSeiBundlePayload.newBuilder().stateProof(stateProof).bundleContent(serializedContent);
        if (nextValidatorSet != null) {
            payloadBuilder.nextValidatorSet(nextValidatorSet);
        }
        final boolean manifestAttached = manifestProofEntry != null && manifestPreimage != null;
        if (manifestAttached) {
            payloadBuilder.manifestStorageProof(manifestProofEntry).endpointManifest(manifestPreimage);
        }
        if (!priorValidatorSetUpdates.isEmpty()) {
            payloadBuilder.priorValidatorSetUpdates(priorValidatorSetUpdates);
        }

        log.info(
                "[Sei] Bundle created { height={} signedHeaderHeight={}"
                        + " storageProofs={} bundleContentBytes={} has_next_validator_set={}"
                        + " num_prior_validator_set_updates={} manifest={}}",
                stateHeight,
                seiHeader.height(),
                storageProofEntries.size(),
                serializedContent.length(),
                nextValidatorSet != null,
                priorValidatorSetUpdates.size(),
                manifestAttached);

        return payloadBuilder.build();
    }

    private ClprSeiBundlePayload createTrustRotationOnlyBundle(List<SeiValidatorSetUpdate> priorValidatorSetUpdates) {
        log.info(
                "[Sei] Trust-only bundle created { num_prior_validator_set_updates={} }",
                priorValidatorSetUpdates.size());
        return ClprSeiBundlePayload.newBuilder()
                .priorValidatorSetUpdates(priorValidatorSetUpdates)
                .build();
    }

    /**
     * Builds {@link ClprQueueMetadata} from the given channel state.
     */
    private static ClprQueueMetadata buildQueueMetadata(final ClprChannel channel) {
        return ClprQueueMetadata.newBuilder()
                .nextMessageId(channel.nextMessageId())
                .sentRunningHash(channel.sentRunningHash())
                .receivedMessageId(channel.receivedMessageId())
                .receivedRunningHash(channel.receivedRunningHash())
                .status(channel.status())
                .trustAnchorId(channel.trustAnchorId())
                .endpointManifestVersion(channel.endpointManifestVersion())
                .build();
    }
}
