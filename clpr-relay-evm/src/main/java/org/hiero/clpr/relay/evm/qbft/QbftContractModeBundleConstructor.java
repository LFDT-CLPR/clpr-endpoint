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
import org.hiero.clpr.relay.evm.model.EvmStorageMerkleProof;
import org.hiero.clpr.relay.evm.model.ProofResponse;
import org.hiero.clpr.relay.evm.qbft.ValidatorContractProofSource.ContractProof;
import org.hiero.clpr.relay.evm.qbft.ValidatorContractProofSource.ProvenValidatorSet;
import org.hiero.clpr.relay.evm.qbft.ValidatorContractProofSource.ValidatorSet;
import org.jspecify.annotations.Nullable;

/**
 * A {@link BundleConstructor} for Besu QBFT chains running "contract" validator-selection mode
 * (issue #105) — where the validator set is held by a validator smart contract (e.g. the Consensys
 * <a href="https://github.com/Consensys/validator-smart-contracts">validator-smart-contracts</a>
 * reference implementation) rather than encoded in every block header's {@code extraData}.
 *
 * <p>Feeds the on-chain {@code QbftContractModeVerifier} (mode {@code 1}) in
 * {@code clpr-smart-contracts}. This is a sibling to {@link QbftBundleConstructor} (Besu's
 * "header" validator-selection mode, mode {@code 0}), not an extension of it: the two modes produce
 * structurally different bundle proofs, and per {@code qbft-contract-mode.md} (the sibling
 * contracts repo's wire-format note) a channel keeps one mode for its full life, so a relay never
 * needs to pick between them for the same channel. The channel-slots and endpoint-manifest proof
 * steps mirror {@link QbftBundleConstructor}'s — the same pattern every {@code BundleConstructor}
 * implementation follows for that shared, mode-independent part of a bundle — rather than being
 * shared code between the two classes.
 *
 * <p>The trust anchor id is a QBFT block number (not an epoch, unlike header mode): every bundle
 * must resend the full validator member list behind the anchor's on-chain {@code setCommitment}
 * hash (the anchor stores only the hash, not the list), plus one "hop" — a change-block header and
 * a proof of the validator contract's array at that block — for every rotation the peer's anchor
 * must cross to reach the current block. Rotation ("hop") boundaries are located with a bisection
 * search between the anchor block and the block before the current one, since — unlike header
 * mode, where a rotation is announced in every intervening header — a validator-contract rotation
 * leaves no trace in blocks other than the one it happened in.
 *
 * <p>In the two-phase sync protocol a bundle is built ad hoc from the peer's {@link BundleRequest}: the
 * requested message range, the peer verifier's current trust anchor ({@code currentTrustAnchorId} — the
 * block number above, which drives the rotation walk), and the endpoint-manifest version the peer holds
 * of us ({@code currentEndpointManifestVersion}, which gates the local manifest proof).
 *
 * <p>Requires the JSON-RPC node to serve {@code eth_getProof} for historical blocks back to the
 * peer's trust anchor (an archive node); see {@code docs/configuration.md}'s
 * {@code localNetworks[].qbftContractMode} section for this and the peer-chain gas-sizing
 * requirement.
 */
public final class QbftContractModeBundleConstructor implements BundleConstructor {

    private record ContractModeTrustAnchorId(long blockNumber) {}

    /**
     * One validator-set rotation the peer's verifier must cross between its trust-anchor block and
     * the current block — an entry of {@code hops[]} in {@code qbft-contract-mode.md} §4. The
     * verifier checks the change block's seals against the old set (the one-block lag means the
     * change block is still sealed by it), then adopts the new set proven from that block's state;
     * once it has crossed hops, it returns a new trust anchor.
     *
     * @param header        the change block — the last block sealed by the old set
     * @param contractProof the validator contract's array (the new set) under {@code header}'s state root
     */
    private record Hop(BlockHeader header, ContractProof contractProof) {}

    private record ContractModeBundlePayload(
            List<Address> trustedSet,
            List<Hop> hops,
            ContractProof currentContractProof,
            BlockHeader currentBlockHeader,
            List<Bytes> clprServiceAccountProof,
            List<EvmStorageMerkleProof> clprServiceStorageProofs,
            Bytes serializedBundleContent,
            @Nullable EvmStorageMerkleProof manifestStorageProof,
            @Nullable Bytes serializedEndpointManifest) {}

    private static final Logger log = Loggers.getLogger(QbftContractModeBundleConstructor.class);

    private static final int ENCODED_TRUST_ANCHOR_ID_LENGTH = Long.BYTES;

    private final Address clprServiceAddress;
    private final EvmJsonRpcClient rpcClient;
    private final ContractStateReader stateReader;
    private final CommitmentLevel commitmentLevel;
    private final ValidatorContractProofSource validatorSource;
    private final int maxMessagesPerBundle;

    public QbftContractModeBundleConstructor(
            final Address clprServiceAddress,
            final ValidatorContractProofSource validatorSource,
            final int maxMessagesPerBundle,
            final EvmJsonRpcClient rpcClient,
            final ContractStateReader stateReader,
            final CommitmentLevel commitmentLevel) {
        this.clprServiceAddress = clprServiceAddress;
        this.validatorSource = validatorSource;
        this.rpcClient = rpcClient;
        this.stateReader = stateReader;
        this.commitmentLevel = commitmentLevel;

        if (maxMessagesPerBundle < 1) {
            throw new IllegalArgumentException("maxMessagesPerBundle must be at least 1");
        }
        this.maxMessagesPerBundle = maxMessagesPerBundle;
    }

    @Override
    public BundleWithConstructionMetadata createBundle(final Bytes channelId, final BundleRequest request) {
        // Read the local head at the configured commitment level (contract mode proves against the head —
        // no lag, like header mode).
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

    private Optional<ContractModeTrustAnchorId> decodeTrustAnchorId(final Bytes trustAnchorId) {
        if (trustAnchorId.length() != ENCODED_TRUST_ANCHOR_ID_LENGTH) {
            return Optional.empty();
        }
        try {
            final var blockNumber = new BigInteger(1, trustAnchorId.toByteArray()).longValueExact();
            return Optional.of(new ContractModeTrustAnchorId(blockNumber));
        } catch (final ArithmeticException e) {
            return Optional.empty();
        }
    }

    /**
     * Builds the contract-mode proof payload for {@code channel} at {@code blockNumber}, including
     * {@code pendingMessages} (already trimmed to the requested start). The rotation walk starts at
     * {@code remoteTrustAnchorIdBytes} (the peer's current trust anchor, from its request) and the local
     * endpoint-manifest proof is gated on {@code peerManifestVersion} (the version the peer holds of our
     * manifest, from its request). Returns empty when there is nothing to send (no messages, acks,
     * rotation hops, or lifecycle status) or when proof assembly fails.
     */
    private Optional<Bytes> buildBundlePayload(
            final BigInteger blockNumber,
            final Bytes channelId,
            final ClprChannel channel,
            final List<ContractStateReader.QueuedMessage> pendingMessages,
            final Bytes remoteTrustAnchorIdBytes,
            final long peerManifestVersion) {

        final var currentBlockTag = toBlockTag(blockNumber.longValueExact());
        final var currentBlockHeader = this.rpcClient.ethGetBlockHeaderByNumber(currentBlockTag);
        final var currentBlockNumber = currentBlockHeader.number();
        final long current = currentBlockNumber.longValueExact();

        final Optional<ContractModeTrustAnchorId> remoteTrustAnchorId = decodeTrustAnchorId(remoteTrustAnchorIdBytes);

        log.info(
                "bundle CONSTRUCT attempt (contract mode) next={} acked={} recv={} status={} block={}"
                        + " remote_anchor_block={}",
                channel.nextMessageId(),
                channel.ackedMessageId(),
                channel.receivedMessageId(),
                channel.status(),
                blockNumber,
                remoteTrustAnchorId.map(a -> Long.toString(a.blockNumber())).orElse("unknown"));

        if (remoteTrustAnchorId.isPresent() && remoteTrustAnchorId.get().blockNumber() >= current) {
            log.warn(
                    "Remote channel was updated to trust anchor block {}, but our JSON RPC says the current block"
                            + " number is {}. The verifier requires the current block to be strictly after the"
                            + " anchor block; this might indicate that our JSON RPC node is lagging behind.",
                    remoteTrustAnchorId.get().blockNumber(),
                    current);
            return Optional.empty();
        }

        // Step 1: resolve the trusted set behind the anchor's setCommitment (resent in full every
        // bundle — the on-chain anchor stores only its hash) and the hops needed to walk the peer
        // forward to the set that seals the current block.
        //
        // When the peer's request carries no decodable anchor, fall back to the steady-state
        // assumption: the set behind the anchor is whatever set seals the block immediately before
        // the current one. That is exactly the set required by QbftContractModeVerifier._walkContractMode's
        // verifyQuorum(currentSet, header) call (in clpr-smart-contracts), so the fallback only
        // produces an invalid bundle if a rotation happened since the peer's last bundle — the
        // same limitation the epoch-based header-mode fallback has.
        final long trustedSetBlock =
                remoteTrustAnchorId.map(ContractModeTrustAnchorId::blockNumber).orElse(current - 1);
        if (trustedSetBlock < 0) {
            // Only the synthetic fallback at block 0 can reach this — the anchor check above
            // already rejected any real anchor at or after current, and an anchor is never negative.
            return Optional.empty();
        }

        final ValidatorSet trustedSet = validatorSource
                .proveValidatorSet(BigInteger.valueOf(trustedSetBlock))
                .set();
        final List<Hop> hops =
                findHops(BigInteger.valueOf(trustedSetBlock), currentBlockNumber.subtract(BigInteger.ONE), trustedSet);
        final ProvenValidatorSet currentProven = validatorSource.proveValidatorSet(currentBlockNumber);

        final var messagesToInclude =
                pendingMessages.subList(0, Math.min(maxMessagesPerBundle, pendingMessages.size()));

        final var status = channel.status();
        final boolean conveysLifecycle = status == ClprChannelStatus.CLOSING || status == ClprChannelStatus.DRAINED;
        if (messagesToInclude.isEmpty() && hops.isEmpty() && channel.receivedMessageId() <= 0 && !conveysLifecycle) {
            log.debug(
                    "bundle CONSTRUCT skip (no pending messages, ACKs or rotation hops) status={} block={}",
                    status,
                    blockNumber);
            return Optional.empty();
        }

        final var hasMessages = !messagesToInclude.isEmpty();
        final var maybeLastMessage = hasMessages ? messagesToInclude.getLast() : null;

        // Step 2: prove the channel-state slots (and, if present, the last message's running hash).
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
                "bundle CONSTRUCT proof (contract mode) msgs=[{}] msg_count={} hops={} block={}",
                messagesToInclude.isEmpty()
                        ? ""
                        : (messagesToInclude.getFirst().messageId() + ".."
                                + messagesToInclude.getLast().messageId()),
                messagesToInclude.size(),
                hops.size(),
                blockNumber);

        final var slotsToProveHex =
                slotsToProve.stream().map(EvmUtils::toSlotHex).toArray(String[]::new);
        final var proofResp =
                this.rpcClient.ethGetProof(clprServiceAddress.toHexString(), slotsToProveHex, currentBlockTag);

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
            final var lastMessageRunningHashSlotBytes = leftPad32(ByteUtils.fromPrefixedHex(
                    toSlotHex(calculateMsgRunningHashStorageSlot(channel.channelId(), maybeLastMessage.messageId()))));
            final var lastMessageRunningHashProof = proofResp.storageProof().stream()
                    .filter(e -> leftPad32(e.key()).equals(lastMessageRunningHashSlotBytes))
                    .findFirst();
            if (lastMessageRunningHashProof.isEmpty()) {
                logBundleCreationError(
                        channelId, maybeLastMessage, "Last message's runningHashAfterProcessing proof is missing.");
                return Optional.empty();
            }
            final var provenValue = leftPad32(lastMessageRunningHashProof.get().value());
            final var expectedValue = leftPad32(maybeLastMessage.value().runningHashAfterProcessing());
            if (!provenValue.equals(expectedValue)) {
                logBundleCreationError(
                        channelId,
                        maybeLastMessage,
                        String.format(
                                "Last message's runningHashAfterProcessing proof value does not match expected value"
                                        + " (expected %s, but proof contains %s).",
                                expectedValue.toHex(), provenValue.toHex()));
                return Optional.empty();
            }
        }

        // Step 3 (optional): local endpoint-manifest proof — see QbftBundleConstructor for the
        // shared policy this mirrors (spec §4.2 Step 1b), gated on the version the peer's request
        // reports holding of our manifest.
        EvmStorageMerkleProof manifestStorageProof = null;
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
                    manifestStorageProof = new EvmStorageMerkleProof(manifestSp.key(), manifestSp.proof());
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

        // Step 4: assemble the bundle.
        final ClprQueueMetadata metadata = buildQueueMetadata(channel);
        final List<ClprMessagePayload> payloads = messagesToInclude.stream()
                .map(ContractStateReader.QueuedMessage::value)
                .map(ClprMessageValue::payload)
                .toList();
        final ClprBundleContent content = ClprBundleContent.newBuilder()
                .metadata(metadata)
                .messages(payloads)
                .build();
        final Bytes serializedContent = ClprBundleContent.PROTOBUF.toBytes(content);

        final var proofPackage = new ContractModeBundlePayload(
                trustedSet.members(),
                hops,
                currentProven.proof(),
                currentBlockHeader,
                proofResp.accountProof(),
                toBundleStorageProof(proofResp.storageProof()),
                serializedContent,
                manifestStorageProof,
                manifestPreimage);
        final var proofPackageBytes = serializeBundlePayload(proofPackage);

        log.info(
                "bundle CONSTRUCTED (contract mode) {} status={} block={} blockHash={} hops={} trustedSetSize={}"
                        + " accountProofNodes={} storageProofEntries={} contentBytes={}",
                BundleLog.tag(proofPackageBytes),
                channel.status(),
                blockNumber,
                currentBlockHeader.blockHash(),
                hops.size(),
                trustedSet.members().size(),
                proofResp.accountProof().size(),
                proofResp.storageProof().size(),
                serializedContent.length());

        return Optional.of(proofPackageBytes);
    }

    /**
     * Locates the validator-contract rotations strictly after {@code trustedSetBlock} and up to
     * {@code toBlock} (the caller passes {@code current - 1}: the block whose state seals the
     * current block, per the one-block lag below) that bisection actually finds, in ascending
     * order. Every rotation found is included — a partial walk leaves the walked set short of what
     * actually seals {@code current} and is rejected ({@code QuorumNotReached}) — so there is no
     * bundle-size limit to cap this at.
     *
     * <p>Besu's contract-mode consensus lags the seal by exactly one block: the state of a change
     * block N already holds the new set, but block N is still sealed by the OLD set, and only block
     * N+1 is sealed by the new one (qbft-contract-mode.md §1). So a rotation's change block is found
     * by bisection — a plain binary search for the first block whose read set differs from
     * {@code knownSet} — in O(log(range)) probes per hop, and each found change block is itself the
     * hop.
     *
     * <p>Bisection can skip a rotation that is later undone within the searched range (e.g. A→B then
     * B→A before {@code toBlock}) — harmless, since only the set that actually seals the next
     * examined block matters, not the full history of every intervening block.
     */
    private List<Hop> findHops(
            final BigInteger trustedSetBlock, final BigInteger toBlock, final ValidatorSet trustedSet) {
        final List<Hop> hops = new ArrayList<>();
        BigInteger searchFrom = trustedSetBlock;
        ValidatorSet knownSet = trustedSet;
        while (true) {
            final BigInteger changeBlock = findFirstChangeBlock(searchFrom, toBlock, knownSet);
            if (changeBlock == null) {
                break;
            }
            final ProvenValidatorSet proven = validatorSource.proveValidatorSet(changeBlock);
            final var header = this.rpcClient.ethGetBlockHeaderByNumber(toBlockTag(changeBlock.longValueExact()));
            hops.add(new Hop(header, proven.proof()));
            knownSet = proven.set();
            searchFrom = changeBlock;
        }
        return hops;
    }

    /**
     * Binary search for the smallest block in {@code (searchFromExclusive, toInclusive]} whose
     * validator set differs from {@code knownSet}, or {@code null} if none does.
     */
    @Nullable
    private BigInteger findFirstChangeBlock(
            final BigInteger searchFromExclusive, final BigInteger toInclusive, final ValidatorSet knownSet) {
        if (searchFromExclusive.compareTo(toInclusive) >= 0) {
            return null;
        }
        if (validatorSource.proveValidatorSet(toInclusive).set().equals(knownSet)) {
            return null;
        }
        BigInteger lo = searchFromExclusive;
        BigInteger hi = toInclusive;
        while (hi.subtract(lo).compareTo(BigInteger.ONE) > 0) {
            final BigInteger mid = lo.add(hi).shiftRight(1);
            if (validatorSource.proveValidatorSet(mid).set().equals(knownSet)) {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        return hi;
    }

    private void logBundleCreationError(
            final Bytes channelId,
            final ContractStateReader.@Nullable QueuedMessage lastMessage,
            final String extraInfo) {
        log.error(
                "[QBFT contract-mode] Failed to create a proof for channel ID {} and last message ID {}. {}",
                channelId.toHex(),
                lastMessage != null ? lastMessage.messageId() : "<ack-only>",
                extraInfo);
    }

    private List<EvmStorageMerkleProof> toBundleStorageProof(final List<ProofResponse.StorageProofEntry> entries) {
        return entries.stream()
                .map(e -> new EvmStorageMerkleProof(e.key(), e.proof()))
                .toList();
    }

    private static Bytes serializeBundlePayload(final ContractModeBundlePayload bundlePayload) {
        return Bytes.wrap(PayloadRlpEncoder.encodeContractModeBundlePayload(bundlePayload));
    }

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

    /**
     * RLP-encodes the Contract-mode bundle payload matching {@code QbftContractModeVerifier}'s
     * expected wire shape: a top-level 5 (or 7, with a manifest update) element tuple
     * {@code [currentBlockHeader, validatorSource, accountProof, storageProof, bundleContent,
     * manifestProof?, manifestPreimage?]}, where {@code validatorSource} is itself
     * {@code [trustedSet, hops[], contractProof]} and a hop is {@code [blockHeader, contractProof]}.
     * A {@code contractProof} (the 4-item {@code [validatorContract, accountProof, arraySlot,
     * storageProof]} list) is always additionally wrapped as an RLP byte string — the verifier
     * strips that envelope in place before reading the list.
     */
    private static final class PayloadRlpEncoder {
        static byte[] encodeContractModeBundlePayload(final ContractModeBundlePayload payload) {
            final byte[] blockHeaderElem = BlockHeaderRlpCodec.encodeRlpBytes(payload.currentBlockHeader());
            final byte[] validatorSourceElem = encodeValidatorSource(payload);
            final byte[] accountProofElem = rlpList(payload.clprServiceAccountProof().stream()
                    .map(b -> rlpBytes(b.toByteArray()))
                    .toArray(byte[][]::new));
            final byte[] storageProofsElem = rlpList(payload.clprServiceStorageProofs().stream()
                    .map(EvmStorageMerkleProof::encodeRlp)
                    .toArray(byte[][]::new));
            final byte[] contentElem =
                    rlpBytes(payload.serializedBundleContent().toByteArray());

            if (payload.manifestStorageProof() != null && payload.serializedEndpointManifest() != null) {
                final byte[] manifestProofElem =
                        rlpList(payload.manifestStorageProof().encodeRlp());
                final byte[] manifestPreimageElem =
                        rlpBytes(payload.serializedEndpointManifest().toByteArray());
                return rlpList(
                        blockHeaderElem,
                        validatorSourceElem,
                        accountProofElem,
                        storageProofsElem,
                        contentElem,
                        manifestProofElem,
                        manifestPreimageElem);
            }
            return rlpList(blockHeaderElem, validatorSourceElem, accountProofElem, storageProofsElem, contentElem);
        }

        private static byte[] encodeValidatorSource(final ContractModeBundlePayload payload) {
            final byte[] trustedSetElem = rlpList(
                    payload.trustedSet().stream().map(a -> rlpBytes(a.value())).toArray(byte[][]::new));
            final byte[] hopsElem = rlpList(
                    payload.hops().stream().map(PayloadRlpEncoder::encodeHop).toArray(byte[][]::new));
            final byte[] currentContractProofElem =
                    ValidatorContractProofRlp.encodeWrapped(payload.currentContractProof());
            return rlpList(trustedSetElem, hopsElem, currentContractProofElem);
        }

        private static byte[] encodeHop(final Hop hop) {
            return rlpList(
                    BlockHeaderRlpCodec.encodeRlpBytes(hop.header()),
                    ValidatorContractProofRlp.encodeWrapped(hop.contractProof()));
        }
    }
}
