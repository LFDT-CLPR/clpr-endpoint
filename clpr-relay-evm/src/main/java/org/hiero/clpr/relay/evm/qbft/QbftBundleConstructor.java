// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.qbft;

import static org.hiero.clpr.relay.evm.AbiCodec.rlpBytes;
import static org.hiero.clpr.relay.evm.AbiCodec.rlpList;
import static org.hiero.clpr.relay.evm.ByteUtils.leftPad32;
import static org.hiero.clpr.relay.evm.EvmUtils.toSlotHex;
import static org.hiero.clpr.relay.evm.EvmUtils.wrapToUint256;
import static org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient.toBlockTag;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.CH_OFFSET_ENDPOINT_MANIFEST_VERSION;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.CH_OFFSET_RECEIVED_MSG_ID;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.CH_OFFSET_RECEIVED_RUNNING_HASH;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.CH_OFFSET_SENT_RUNNING_HASH;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.CH_OFFSET_STATUS_AND_NEXT_MSG_ID;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.ENDPOINT_MANIFEST_COMMITMENT_SLOT;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.calculateChannelFieldStorageSlot;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.calculateMsgRunningHashStorageSlot;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.hapi.node.state.clpr.ClprBundleContent;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprEndpointManifest;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprMessageValue;
import com.hedera.hapi.node.state.clpr.ClprQueueMetadata;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.logging.api.Logger;
import com.swirlds.logging.api.Loggers;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.hiero.clpr.relay.core.BundleConstructor;
import org.hiero.clpr.relay.core.BundleLog;
import org.hiero.clpr.relay.core.BundleWithConstructionMetadata;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.ManifestProofPolicy;
import org.hiero.clpr.relay.evm.BlockHeaderRlpCodec;
import org.hiero.clpr.relay.evm.ByteUtils;
import org.hiero.clpr.relay.evm.EvmUtils;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.model.BlockHeader;
import org.hiero.clpr.relay.evm.model.ProofResponse;
import org.jspecify.annotations.Nullable;

/**
 * An implementation of {@link BundleConstructor} that creates bundles with Besu's QBFT proofs.
 *
 * <p>In the two-phase sync protocol a bundle is built ad hoc from the peer's {@link BundleRequest}: the
 * requested message range, the peer verifier's current trust anchor ({@code currentTrustAnchorId}, which
 * drives validator-set / epoch catch-up), and the endpoint-manifest version the peer holds of us
 * ({@code currentEndpointManifestVersion}, which gates the local manifest proof).
 */
public final class QbftBundleConstructor implements BundleConstructor {

    private record QbftTrustAnchorId(long epoch) {}

    private static final Logger log = Loggers.getLogger(QbftBundleConstructor.class);

    private static final int ENCODED_TRUST_ANCHOR_ID_LENGTH = Long.BYTES;

    public record QbftStorageProofEntry(Bytes key, List<Bytes> proof) {}

    public record QbftBundlePayload(
            List<BlockHeader> epochBlockHeaders,
            BlockHeader currentBlockHeader,
            List<Bytes> clprServiceAccountProof,
            List<QbftStorageProofEntry> clprServiceStorageProofs,
            Bytes serializedBundleContent,
            @Nullable QbftStorageProofEntry manifestStorageProof,
            @Nullable Bytes serializedEndpointManifest) {}

    /** Default assembled bundle-proof size ceiling (bytes) when none is configured; matches gRPC 1 MiB. */
    public static final long DEFAULT_MAX_BUNDLE_SIZE = 1_048_576L;

    private final Address clprServiceAddress;
    private final EvmJsonRpcClient rpcClient;
    private final ContractStateReader stateReader;
    private final CommitmentLevel commitmentLevel;
    private final long epochLength;
    private final int maxEpochBlockHeadersPerBundle;
    private final int maxMessagesPerBundle;
    private final long maxBundleSize;

    /**
     * Convenience constructor using {@link #DEFAULT_MAX_BUNDLE_SIZE} as the assembled-bundle size ceiling.
     */
    public QbftBundleConstructor(
            Address clprServiceAddress,
            long epochLength,
            int maxEpochBlockHeadersPerBundle,
            int maxMessagesPerBundle,
            EvmJsonRpcClient rpcClient,
            ContractStateReader stateReader,
            CommitmentLevel commitmentLevel) {
        this(
                clprServiceAddress,
                epochLength,
                maxEpochBlockHeadersPerBundle,
                maxMessagesPerBundle,
                rpcClient,
                stateReader,
                commitmentLevel,
                DEFAULT_MAX_BUNDLE_SIZE);
    }

    public QbftBundleConstructor(
            Address clprServiceAddress,
            long epochLength,
            int maxEpochBlockHeadersPerBundle,
            int maxMessagesPerBundle,
            EvmJsonRpcClient rpcClient,
            ContractStateReader stateReader,
            CommitmentLevel commitmentLevel,
            long maxBundleSize) {
        this.clprServiceAddress = clprServiceAddress;
        this.rpcClient = rpcClient;
        this.stateReader = stateReader;
        this.commitmentLevel = commitmentLevel;

        if (epochLength < 1) {
            throw new IllegalArgumentException("epochLength must be at least 1");
        }
        this.epochLength = epochLength;

        if (maxEpochBlockHeadersPerBundle < 1) {
            throw new IllegalArgumentException("maxEpochBlockHeadersPerBundle must be at least 1");
        }
        this.maxEpochBlockHeadersPerBundle = maxEpochBlockHeadersPerBundle;

        if (maxMessagesPerBundle < 1) {
            throw new IllegalArgumentException("maxMessagesPerBundle must be at least 1");
        }
        this.maxMessagesPerBundle = maxMessagesPerBundle;

        if (maxBundleSize < 1) {
            throw new IllegalArgumentException("maxBundleSize must be at least 1 byte");
        }
        this.maxBundleSize = maxBundleSize;
    }

    @Override
    public BundleWithConstructionMetadata createBundle(final Bytes channelId, final BundleRequest request) {
        // Read the local head at the configured commitment level (QBFT proves against the head — no lag).
        final BlockHeader head = rpcClient.ethGetBlockHeaderByNumber(commitmentLevel.toBlockTag());
        final long blockNum = head.number().longValueExact();

        if (ClprChannelStatus.CLOSED.equals(request.currentStatus())) {
            // Nothing to do if the remote side is closed
            return new BundleWithConstructionMetadata(Bytes.EMPTY, blockNum);
        }

        final String blockTag = toBlockTag(blockNum);
        final var maybeChannel = stateReader.readChannelState(channelId, blockTag);
        if (maybeChannel.isEmpty()) {
            return new BundleWithConstructionMetadata(Bytes.EMPTY, blockNum);
        }
        final ClprChannel channel = maybeChannel.get();
        if (ClprChannelStatus.PENDING.equals(channel.status())) {
            return new BundleWithConstructionMetadata(Bytes.EMPTY, blockNum);
        }

        // Sender-side trim: serve messages from the id the peer asked for, up to what we have sent.
        final long fromId = Math.max(1L, request.currentReceivedMessageId() + 1);
        final List<ContractStateReader.QueuedMessage> pending = fromId >= channel.nextMessageId()
                ? List.of()
                : stateReader.readQueuedMessages(channelId, fromId, channel.nextMessageId(), blockTag);

        final Bytes payload = buildBundlePayload(
                        BigInteger.valueOf(blockNum),
                        channelId,
                        channel,
                        pending,
                        request.currentTrustAnchorId(),
                        request.currentEndpointManifestVersion())
                .orElse(Bytes.EMPTY);
        return new BundleWithConstructionMetadata(payload, blockNum);
    }

    private List<BlockHeader> fetchEpochBlockHeaders(long fromEpoch, int maxCount) {
        final var res = new ArrayList<BlockHeader>(maxCount);
        for (var i = 0; i < maxCount; i++) {
            final var epoch = fromEpoch + i;
            final var epochBlockNumber = epoch * epochLength;
            final var epochBlockHeader = this.rpcClient.ethGetBlockHeaderByNumber(toBlockTag(epochBlockNumber));
            res.add(epochBlockHeader);
        }
        return res;
    }

    private Optional<QbftTrustAnchorId> decodeTrustAnchorId(Bytes trustAnchorId) {
        if (trustAnchorId.length() != ENCODED_TRUST_ANCHOR_ID_LENGTH) {
            return Optional.empty();
        }
        try {
            final var epoch = new BigInteger(1, trustAnchorId.toByteArray()).longValueExact();
            return Optional.of(new QbftTrustAnchorId(epoch));
        } catch (final ArithmeticException e) {
            return Optional.empty();
        }
    }

    /**
     * Builds the QBFT proof payload for {@code channel} at {@code blockNumber}, including
     * {@code pendingMessages} (already trimmed to the requested start). Validator-set / epoch catch-up is
     * driven by {@code remoteTrustAnchorId} (the peer's current trust anchor, from its request) and the
     * local endpoint-manifest proof is gated on {@code peerManifestVersion} (the version the peer holds of
     * our manifest, from its request). Returns empty when there is nothing to send (no messages, acks,
     * epoch headers, or lifecycle status) or when proof assembly fails.
     */
    private Optional<Bytes> buildBundlePayload(
            final BigInteger blockNumber,
            final Bytes channelId,
            final ClprChannel channel,
            final List<ContractStateReader.QueuedMessage> pendingMessages,
            final Bytes remoteTrustAnchorId,
            final long peerManifestVersion) {

        final var currentBlockTag = toBlockTag(blockNumber.longValueExact());
        final var currentBlockHeader = this.rpcClient.ethGetBlockHeaderByNumber(currentBlockTag);
        final var currentEpoch = currentBlockHeader.number().longValueExact() / this.epochLength;

        // Decode the remote trust anchor ID (from the peer's request), default to current epoch if not set
        final QbftTrustAnchorId remoteTrustAnchor =
                decodeTrustAnchorId(remoteTrustAnchorId).orElseGet(() -> new QbftTrustAnchorId(currentEpoch));

        log.info(
                "bundle CONSTRUCT attempt next={} acked={} recv={} status={} block={} local_epoch={} remote_epoch={}",
                channel.nextMessageId(),
                channel.ackedMessageId(),
                channel.receivedMessageId(),
                channel.status(),
                blockNumber,
                currentEpoch,
                remoteTrustAnchor.epoch());

        // Step 1: Check if we need to update the trust anchor on the remote side

        // We're using the following strategy as a reasonable starting point:
        // - if the remote side only needs a single epoch block header to update
        //   its trust anchor to the latest (current) epoch:
        //   we include that epoch header and messages up to maxMessagesPerBundle
        // - if they need more than one epoch block header to advance to the
        //   latest (current) epoch: we include up to maxEpochBlockHeadersPerBundle epoch block
        //   headers and NO messages
        //
        // It can later be tweaked (e.g. have a single `maxBundleSize` limit and include
        // as many epoch block headers and messages as we can fit).

        final List<BlockHeader> epochBlockHeaders;
        final int maxMessagesToInclude;
        if (remoteTrustAnchor.epoch() > currentEpoch) {
            // Remote side was updated to a _future_ (from our perspective) trust anchor.
            // That must mean that the RPC node we're connected to is behind.
            log.warn(
                    """
                    Remote channel was updated to trust anchor ID (Besu epoch) {}, but our JSON RPC says
                    that the current block number is {}, so with {} blocks per epoch that results in epoch {},
                    which is behind remote channel's current trust anchor.
                    This might indicate that our JSON RPC node is lagging behind.""",
                    remoteTrustAnchor.epoch(),
                    currentBlockHeader.number().longValueExact(),
                    epochLength,
                    currentEpoch);
            return Optional.empty();
        } else if (remoteTrustAnchor.epoch() < currentEpoch) {
            // Remote side is behind:
            // let's fetch the missing epoch block headers (up to maxEpochBlockHeadersPerBundle)
            // and include them in the bundle

            final var maxEpochHeadersToFetch = Math.min(
                    (int) Math.min(currentEpoch - remoteTrustAnchor.epoch(), Integer.MAX_VALUE),
                    maxEpochBlockHeadersPerBundle);
            epochBlockHeaders = fetchEpochBlockHeaders(remoteTrustAnchor.epoch() + 1, maxEpochHeadersToFetch);
            // As described in the comment above:
            // - at most one header   => include all messages up to the usual limit
            // - more than one header => don't include any messages
            maxMessagesToInclude = maxEpochHeadersToFetch <= 1 ? maxMessagesPerBundle : 0;
        } else {
            // Remote trust anchor matches the current epoch / validator set.
            // All good, no need to include any extra epoch block headers.
            epochBlockHeaders = List.of();
            maxMessagesToInclude = maxMessagesPerBundle; // the usual limit, unaltered
        }

        final var messagesToInclude =
                pendingMessages.subList(0, Math.min(maxMessagesToInclude, pendingMessages.size()));

        // A closing/draining status is itself something the peer must receive: it drives the peer's own
        // lifecycle forward even when no message, ack, or epoch header rides along. So build a status-only
        // bundle in those states; only skip when there is genuinely nothing for the peer to act on (an
        // ACTIVE channel with no pending messages, no acks, and no trust-anchor advance).
        final var status = channel.status();
        final boolean conveysLifecycle = status == ClprChannelStatus.CLOSING || status == ClprChannelStatus.DRAINED;
        if (messagesToInclude.isEmpty()
                && epochBlockHeaders.isEmpty()
                && channel.receivedMessageId() <= 0
                && !conveysLifecycle) {
            // Nothing to deliver and no lifecycle status to convey.
            log.debug(
                    "bundle CONSTRUCT skip (no pending messages, ACKs, epoch headers or lifecycle status) status={} block={}",
                    status,
                    blockNumber);
            return Optional.empty();
        }

        final var hasMessages = !messagesToInclude.isEmpty();
        final var maybeLastMessage = hasMessages ? messagesToInclude.getLast() : null;

        // Step 2: Create the state proofs of metadata components and the running hash of the last message (if present)

        // 5 channel-field slots (always present) + optional message running-hash slot
        final var slotsToProve = new ArrayList<BigInteger>(5);
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
                "bundle CONSTRUCT proof msgs=[{}] msg_count={} epoch_headers={} block={}",
                messagesToInclude.isEmpty()
                        ? ""
                        : (messagesToInclude.getFirst().messageId() + ".."
                                + messagesToInclude.getLast().messageId()),
                messagesToInclude.size(),
                epochBlockHeaders.size(),
                blockNumber);

        final var slotsToProveHex =
                slotsToProve.stream().map(EvmUtils::toSlotHex).toArray(String[]::new);
        final var proofResp =
                this.rpcClient.ethGetProof(clprServiceAddress.toHexString(), slotsToProveHex, currentBlockTag);

        // Sanity check the proof response
        if (proofResp.accountProof().isEmpty()) {
            logBundleCreationError(channelId, maybeLastMessage, "Account proof is empty.");
            return Optional.empty();
        }

        if (proofResp.storageProof().size() != slotsToProve.size()) {
            logBundleCreationError(
                    channelId,
                    maybeLastMessage,
                    String.format(
                            "Storage proof is missing some slots (expected %s, but got %s).",
                            slotsToProve.size(), proofResp.storageProof().size()));
            return Optional.empty();
        }

        if (proofResp.storageProof().stream().anyMatch(e -> e.proof().isEmpty())) {
            logBundleCreationError(channelId, maybeLastMessage, "Some storage proofs are empty.");
            return Optional.empty();
        }

        if (hasMessages) {
            final var lastMessageRunningHashSlotBytes = ByteUtils.fromPrefixedHex(
                    toSlotHex(calculateMsgRunningHashStorageSlot(channel.channelId(), maybeLastMessage.messageId())));
            final var lastMessageRunningHashProof = proofResp.storageProof().stream()
                    .filter(e -> leftPad32(e.key()).equals(lastMessageRunningHashSlotBytes))
                    .findFirst();
            if (lastMessageRunningHashProof.isEmpty()) {
                logBundleCreationError(
                        channelId, maybeLastMessage, "Last message's runningHashAfterProcessing proof is missing.");
                return Optional.empty();
            }

            final var lastMessageRunningHashPaddedProofValue =
                    leftPad32(lastMessageRunningHashProof.get().value());
            final var expectedLastMessageRunningHashPadded =
                    leftPad32(maybeLastMessage.value().runningHashAfterProcessing());

            if (!lastMessageRunningHashPaddedProofValue.equals(expectedLastMessageRunningHashPadded)) {
                logBundleCreationError(
                        channelId,
                        maybeLastMessage,
                        String.format(
                                "Last message's runningHashAfterProcessing proof value does not match expected value"
                                        + "(expected %s, but proof contains %s).",
                                expectedLastMessageRunningHashPadded.toHex(),
                                lastMessageRunningHashPaddedProofValue.toHex()));
                return Optional.empty();
            }
        }

        // Step 3 (optional): local endpoint-manifest proof. The "attach only when our LOCAL manifest is
        // ahead of the version the peer holds of it" decision (spec §4.2 Step 1b) is shared across bundle
        // formats in ManifestProofPolicy so no codec can forget it; here we only supply the QBFT-specific
        // proof once the policy says a manifest is due. The peer-held version comes from the request
        // ({@code peerManifestVersion}); channel.endpointManifestVersion() is the version of the *peer's*
        // manifest cached locally, an unrelated quantity, so it must not gate our own manifest proof.
        // The manifest commitment is proven via ENDPOINT_MANIFEST_COMMITMENT_SLOT (slot 18) — a separate
        // ethGetProof call so the channel-slots sanity check above stays simple.
        QbftStorageProofEntry manifestStorageProof = null;
        Bytes manifestPreimage = null;
        final Optional<ClprEndpointManifest> manifestDue =
                ManifestProofPolicy.manifestToAttach(stateReader, peerManifestVersion, currentBlockTag);
        if (manifestDue.isPresent()) {
            final ClprEndpointManifest manifest = manifestDue.get();
            try {
                final var manifestProofResp = this.rpcClient.ethGetProof(
                        clprServiceAddress.toHexString(),
                        new String[] {toSlotHex(wrapToUint256(ENDPOINT_MANIFEST_COMMITMENT_SLOT))},
                        currentBlockTag);
                if (!manifestProofResp.storageProof().isEmpty()
                        && !manifestProofResp.storageProof().get(0).proof().isEmpty()) {
                    final var manifestSp = manifestProofResp.storageProof().get(0);
                    // Keep the slot key alongside the nodes so the proof can be encoded entry-
                    // wrapped ([slotKey, [nodes]]) — the shape SC-189's verifyProvenSlots expects.
                    manifestStorageProof = new QbftStorageProofEntry(manifestSp.key(), manifestSp.proof());
                    manifestPreimage = ClprEndpointManifest.PROTOBUF.toBytes(manifest);
                }
            } catch (final Exception e) {
                log.warn(
                        "manifest proof unavailable for channel {} (localVersion={}): {}",
                        channelId.toHex(),
                        manifest.version(),
                        e.getMessage());
            }
        }

        // Step 4: Assemble the final bundle
        final ClprQueueMetadata metadata = buildQueueMetadata(channel);

        // Extract payloads from message values
        final List<ClprMessagePayload> payloads = messagesToInclude.stream()
                .map(ContractStateReader.QueuedMessage::value)
                .map(ClprMessageValue::payload)
                .toList();

        // Build bundle content
        final ClprBundleContent content = ClprBundleContent.newBuilder()
                .metadata(metadata)
                .messages(payloads)
                .build();
        final Bytes serializedContent = ClprBundleContent.PROTOBUF.toBytes(content);

        // Step 5: Create the QBFT payload
        final var proofPackage = new QbftBundlePayload(
                epochBlockHeaders,
                currentBlockHeader,
                proofResp.accountProof(),
                toBundleStorageProof(proofResp.storageProof()),
                serializedContent,
                manifestStorageProof,
                manifestPreimage);
        final var proofPackageBytes = serializeBundlePayload(proofPackage);

        if (proofPackageBytes.length() > maxBundleSize) {
            log.warn(
                    "[QBFT] Assembled bundle proof for channel {} is {} bytes, over the configured maxBundleSize"
                            + " of {} bytes; not emitting it (a peer submitting it would risk a gas/size blowup)."
                            + " Lower maxMessagesPerBundle or raise maxBundleSize.",
                    channelId,
                    proofPackageBytes.length(),
                    maxBundleSize);
            return Optional.empty();
        }

        log.info(
                "bundle CONSTRUCTED {} status={} block={} blockHash={} accountProofNodes={} storageProofEntries={} "
                        + "contentBytes={}",
                BundleLog.tag(proofPackageBytes),
                channel.status(),
                blockNumber,
                currentBlockHeader.blockHash(),
                proofResp.accountProof().size(),
                proofResp.storageProof().size(),
                serializedContent.length());

        return Optional.of(proofPackageBytes);
    }

    private void logBundleCreationError(
            Bytes channelId, ContractStateReader.@Nullable QueuedMessage lastMessage, String extraInfo) {
        log.error(
                "[QBFT] Failed to create a proof for channel ID {} and last message ID {}. {}",
                channelId.toHex(),
                lastMessage != null ? lastMessage.messageId() : "<ack-only>",
                extraInfo);
    }

    private List<QbftStorageProofEntry> toBundleStorageProof(List<ProofResponse.StorageProofEntry> entries) {
        return entries.stream()
                .map(e -> new QbftStorageProofEntry(e.key(), e.proof()))
                .toList();
    }

    static Bytes serializeBundlePayload(QbftBundlePayload bundlePayload) {
        return Bytes.wrap(PayloadRlpEncoder.encodeQbftBundlePayload(bundlePayload));
    }

    /**
     * Builds {@link ClprQueueMetadata} from the given channel state.
     *
     * @param channel the current channel state
     * @return the queue metadata derived from the channel
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

    /*
    TODO: what we really need is a base RLP encoder/decoder and a system of codecs for custom types.
    E.g.:
    ```
    final var payload = new QbftBundlePayload(...);
    final var rlpEncoded = RLPEncoder.encode(payload, QbftBundlePayloadCodec.class);
    ```
     */
    private static class PayloadRlpEncoder {
        public static byte[] encodeQbftBundlePayload(QbftBundlePayload payload) {
            final byte[] blockHeaderElem = BlockHeaderRlpCodec.encodeRlpBytes(payload.currentBlockHeader());
            final byte[] epochHeadersElem = rlpList(payload.epochBlockHeaders().stream()
                    .map(BlockHeaderRlpCodec::encodeRlpBytes)
                    .toArray(byte[][]::new));
            final byte[] accountProofElem = rlpList(payload.clprServiceAccountProof().stream()
                    .map(b -> rlpBytes(b.toByteArray()))
                    .toArray(byte[][]::new));
            final byte[] storageProofsElem = rlpList(payload.clprServiceStorageProofs().stream()
                    .map(PayloadRlpEncoder::encodeStorageProofEntry)
                    .toArray(byte[][]::new));
            final byte[] contentElem =
                    rlpBytes(payload.serializedBundleContent().toByteArray());

            if (payload.manifestStorageProof() != null && payload.serializedEndpointManifest() != null) {
                // SC-189's ClprEvmStateProof.verifyProvenSlots (via _verifyEndpointManifest) decodes the
                // manifest proof as a list of [slotKey, [nodes]] ENTRIES — the same shape as the
                // channel storage proof — and calls RLP.readList on each entry. Encode it entry-
                // wrapped (a single [commitmentSlotKey, [nodes]] entry), NOT a flat list of node
                // strings: a string element makes the on-chain readList revert RLPInvalidEncoding.
                final byte[] manifestProofElem = rlpList(encodeStorageProofEntry(payload.manifestStorageProof()));
                final byte[] manifestPreimageElem =
                        rlpBytes(payload.serializedEndpointManifest().toByteArray());
                return rlpList(
                        blockHeaderElem,
                        epochHeadersElem,
                        accountProofElem,
                        storageProofsElem,
                        contentElem,
                        manifestProofElem,
                        manifestPreimageElem);
            }
            return rlpList(blockHeaderElem, epochHeadersElem, accountProofElem, storageProofsElem, contentElem);
        }

        private static byte[] encodeStorageProofEntry(QbftStorageProofEntry e) {
            return rlpList(
                    rlpBytes(e.key().toByteArray()),
                    rlpList(e.proof().stream()
                            .map(b -> rlpBytes(b.toByteArray()))
                            .toArray(byte[][]::new)));
        }
    }
}
