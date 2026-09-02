// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import static org.hiero.clpr.relay.evm.AbiCodec.rlpBigInt;
import static org.hiero.clpr.relay.evm.AbiCodec.rlpBytes;
import static org.hiero.clpr.relay.evm.AbiCodec.rlpList;
import static org.hiero.clpr.relay.evm.ByteUtils.leftPad32;
import static org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient.toBlockTag;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.CH_OFFSET_ENDPOINT_MANIFEST_VERSION;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.CH_OFFSET_RECEIVED_MSG_ID;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.CH_OFFSET_RECEIVED_RUNNING_HASH;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.CH_OFFSET_SENT_RUNNING_HASH;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.CH_OFFSET_STATUS_AND_NEXT_MSG_ID;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.ENDPOINT_MANIFEST_COMMITMENT_SLOT;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.calculateChannelFieldStorageSlot;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.calculateMsgRunningHashStorageSlot;

import com.hedera.cryptography.hints.HintsLibraryBridge;
import com.hedera.hapi.node.state.clpr.ClprBundleContent;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprEndpointManifest;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprMessageValue;
import com.hedera.hapi.node.state.clpr.ClprQueueMetadata;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.logging.api.Logger;
import com.swirlds.logging.api.Loggers;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.hiero.clpr.relay.core.BundleConstructor;
import org.hiero.clpr.relay.core.BundleLog;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.ManifestProofPolicy;
import org.hiero.clpr.relay.core.PeerManifestVersionCache;
import org.hiero.clpr.relay.evm.beacon.BeaconApiClient;
import org.hiero.clpr.relay.evm.beacon.LightClientBootstrap;
import org.hiero.clpr.relay.evm.beacon.LightClientHeader;
import org.hiero.clpr.relay.evm.beacon.LightClientUpdate;
import org.hiero.clpr.relay.evm.beacon.SyncAggregate;
import org.hiero.clpr.relay.evm.beacon.SyncCommittee;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.model.ProofResponse;
import org.hiero.clpr.relay.evm.ssz.Ssz;

/**
 * A {@link BundleConstructor} that builds bundles verifiable by the Hiero-native Ethereum-PoS
 * sync-committee verifier ({@code EthereumSyncCommitteeProofVerifier} in {@code clpr-hiero}).
 *
 * <p>The produced {@code proofBytes} is a top-level RLP list of ten items — twelve when carrying an
 * optional endpoint-manifest proof (mirrors that verifier's {@code PayloadDecoder.decodeBundle}):
 *
 * <pre>
 *   [0] attestedHeader      [slot, proposerIndex, parentRoot, stateRoot, bodyRoot]
 *   [1] syncAggregate       [bits(64), signature(96)]
 *   [2] executionStateRoot  32 bytes (execution-layer state root)
 *   [3] executionBranch     SSZ branch, depth 9 (state_root under bodyRoot)
 *   [4] nextCommittee       empty (steady state) OR [pubkeys[512], aggregatePubkey] (rotation)
 *   [5] nextCommitteeBranch  empty list (steady state) OR depth-6 branch (rotation)
 *   [6] accountProof        EIP-1186 account proof for the ClprService contract
 *   [7] storageProof        five [key, proofNodes] entries, six when the bundle carries outbound
 *                           messages (the sixth proves the last included message's running hash);
 *                           verifier sorts by key, value from MPT
 *   [8] bundleContent       protobuf {@link ClprBundleContent}
 *   [9] nonSignerProofs     RLP list of N 416-byte entries, one per non-signing committee member:
 *                           uncompressedPubkey(128) ‖ merkleProof(9 × 32 = 288); the proof is a
 *                           depth-9 SSZ inclusion branch against the pubkeys-vector Merkle root
 *   [10] manifestProof      OPTIONAL, present only when items 10/11 are both emitted: entry-wrapped
 *                           [slotKey, [proofNodes]] for {@code ENDPOINT_MANIFEST_COMMITMENT_SLOT}
 *   [11] manifestPreimage   OPTIONAL: protobuf {@link ClprEndpointManifest}, paired with item 10
 * </pre>
 *
 * <p><b>Endpoint-manifest proof:</b> items 10/11 are attached only when {@link ManifestProofPolicy}
 * decides the local manifest is due to this channel's peer (spec §4.2 Step 1b), mirroring the QBFT and
 * Sei bundle constructors' Step-3 proof. The verifier tells "attached vs not" by list length (10 vs
 * 12), the same convention the QBFT codec uses (5 vs 7).
 *
 * <p>Real beacon-chain data is fetched from the consensus layer via {@link BeaconApiClient}: the
 * per-period light-client update supplies the attested header + sync aggregate (the latest
 * finality update is used only to learn the chain's current sync-committee period). The execution
 * state root and the depth-9 branch come from that attested header; EIP-1186 account/storage proofs
 * are read from the execution node at the attested execution block via
 * {@link EvmJsonRpcClient#ethGetProof}.
 * Storage-slot derivation is shared with the other EVM constructors via
 * {@link org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout}.
 *
 * <p><b>Sync-committee rotation:</b> the attested header is sourced from the per-period light-client
 * update (<code>/eth/v1/beacon/light_client/updates</code>) for the period the remote verifier's
 * trust anchor is on, so it is always signed by a committee the verifier still trusts. When the chain
 * advances past that period, the same update carries the {@code next_sync_committee} (+ branch); the
 * bundle includes them (items 4/5) to advance the anchor exactly one period, and the relay tracks the
 * new period per connection. Multi-period catch-up happens one step per bundle.
 *
 * <p><b>Assumption / limitation:</b> the remote anchor's period is tracked locally (seeded to the
 * chain's current period on the first bundle), because the Ethereum trust anchor exposes no period
 * field. If the relay restarts after the remote anchor has fallen behind, it re-assumes "current" and
 * may need an out-of-band anchor refresh.
 */
public final class EthereumPosBundleConstructor implements BundleConstructor {

    private static final Logger log = Loggers.getLogger(EthereumPosBundleConstructor.class);

    /**
     * The verifier requires five fixed channel-metadata storage entries, plus a sixth (the last
     * included message's running hash) when the bundle carries outbound messages; it sorts them by
     * slot key. This constant sizes the fixed-slot list; see {@link #channelProofSlots}.
     */
    private static final int STORAGE_PROOF_ENTRY_COUNT = 5;

    /** Depth of the sync-committee pubkeys Merkle tree: log2(512) = 9. */
    private static final int PUBKEY_TREE_DEPTH = Integer.numberOfTrailingZeros(Ssz.SYNC_COMMITTEE_SIZE);

    /** Bytes per non-signer proof entry: 128-byte EIP-2537 uncompressed G1 + 9 × 32-byte siblings. */
    private static final int NON_SIGNER_ENTRY_BYTES = 128 + PUBKEY_TREE_DEPTH * 32;

    /** One {@code [key, proofNodes]} storage entry, as the Hiero verifier expects (no value). */
    private record EthStorageProofEntry(Bytes key, List<Bytes> proof) {}

    private record EthBundlePayload(
            LightClientHeader attestedHeader,
            SyncAggregate syncAggregate,
            @Nullable SyncCommittee nextCommittee,
            @Nullable List<Bytes> nextCommitteeBranch,
            List<Bytes> clprServiceAccountProof,
            List<EthStorageProofEntry> clprServiceStorageProofs,
            Bytes serializedBundleContent,
            List<byte[]> nonSignerProofs,
            @Nullable EthStorageProofEntry manifestStorageProof,
            @Nullable Bytes serializedEndpointManifest) {}

    /**
     * The per-channel rotation bookkeeping {@code onStateChanged} needs before it can source an
     * attested header: which sync-committee periods the local chain and the remote anchor are on,
     * and the committee currently used to identify non-signers.
     */
    private record RotationState(long currentPeriod, long remotePeriod, SyncCommittee currentCommittee) {}

    /** The optional Step-3 endpoint-manifest proof; {@link #NONE} when none is due or available. */
    private record ManifestProof(
            @Nullable EthStorageProofEntry storageProof,
            @Nullable Bytes preimage) {
        private static final ManifestProof NONE = new ManifestProof(null, null);
    }

    private final ConcurrentHashMap<Bytes, Bytes> bundleProofCache = new ConcurrentHashMap<>();

    /**
     * The sync-committee period the remote verifier's trust anchor is believed to be on, per
     * connection. Seeded to the chain's current period on the first bundle (we assume the connection
     * was registered with an up-to-date bootstrap); advanced by one each time we emit a rotation
     * proof. Used to decide when a rotation is due and which committee period's update to source the
     * attested header from.
     */
    private final ConcurrentHashMap<Bytes, Long> remotePeriodByConnection = new ConcurrentHashMap<>();

    /**
     * The active sync committee per connection — needed to identify non-signer pubkeys and build their
     * depth-9 Merkle inclusion proofs. Seeded from the finalized bootstrap on the first bundle;
     * advanced to {@code nextCommittee} whenever a rotation bundle is emitted.
     */
    private final ConcurrentHashMap<Bytes, SyncCommittee> currentCommitteeByConnection = new ConcurrentHashMap<>();

    private final Address clprServiceAddress;
    private final EvmJsonRpcClient rpcClient;
    private final BeaconApiClient beaconClient;
    private final int maxMessagesPerBundle;

    /** Reader for the local endpoint manifest; {@code null} disables manifest proofs. */
    @Nullable
    private final ContractStateReader stateReader;

    /**
     * Per-channel record of the manifest version each peer has cached of our manifest; {@code null}
     * means "unknown for every channel" (treated as version 0), so a local manifest proof is
     * attached whenever a local manifest exists.
     */
    @Nullable
    private final PeerManifestVersionCache peerManifestVersions;

    public EthereumPosBundleConstructor(
            final Address clprServiceAddress,
            final int maxMessagesPerBundle,
            final EvmJsonRpcClient rpcClient,
            final BeaconApiClient beaconClient) {
        this(clprServiceAddress, maxMessagesPerBundle, rpcClient, beaconClient, null, null);
    }

    public EthereumPosBundleConstructor(
            final Address clprServiceAddress,
            final int maxMessagesPerBundle,
            final EvmJsonRpcClient rpcClient,
            final BeaconApiClient beaconClient,
            @Nullable final ContractStateReader stateReader,
            @Nullable final PeerManifestVersionCache peerManifestVersions) {
        this.clprServiceAddress = clprServiceAddress;
        this.rpcClient = rpcClient;
        this.beaconClient = beaconClient;
        this.stateReader = stateReader;
        this.peerManifestVersions = peerManifestVersions;
        if (maxMessagesPerBundle < 1) {
            throw new IllegalArgumentException("maxMessagesPerBundle must be at least 1");
        }
        this.maxMessagesPerBundle = maxMessagesPerBundle;
    }

    @Override
    public Optional<Bytes> getLatestBundlePayload(final Bytes connectionId) {
        return Optional.ofNullable(bundleProofCache.get(connectionId));
    }

    @Override
    public void onStateChanged(
            final BigInteger blockNumber,
            final Bytes connectionId,
            final ClprChannel channel,
            final List<ContractStateReader.QueuedMessage> pendingMessages) {

        final var messagesToInclude =
                pendingMessages.subList(0, Math.min(maxMessagesPerBundle, pendingMessages.size()));
        if (messagesToInclude.isEmpty() && channel.receivedMessageId() <= 0) {
            log.debug(
                    "bundle CONSTRUCT skip (no pending messages or ACKs) status={} block={}",
                    channel.status(),
                    blockNumber);
            return;
        }

        final RotationState rotation = resolveRotationState(connectionId, channel);
        if (rotation == null) {
            return;
        }

        log.info(
                "bundle CONSTRUCT attempt next={} recv={} status={} block={} local_period={} remote_period={}",
                channel.nextMessageId(),
                channel.receivedMessageId(),
                channel.status(),
                blockNumber,
                rotation.currentPeriod(),
                rotation.remotePeriod());

        final Optional<LightClientUpdate> maybeUpdate = fetchPeriodUpdate(connectionId, rotation.remotePeriod());
        if (maybeUpdate.isEmpty()) {
            return;
        }
        final LightClientUpdate update = maybeUpdate.get();
        final LightClientHeader attested = update.attestedHeader();

        // Rotation is due only once the chain has actually advanced past the anchor's period. The
        // period update for the anchor's period carries next_sync_committee (the committee for
        // remotePeriod + 1) and its branch — advancing the anchor exactly one step.
        final boolean rotationDue = rotation.currentPeriod() > rotation.remotePeriod();
        final boolean rotate = rotationDue && update.hasRotation();
        if (rotationDue && !update.hasRotation()) {
            // Anomalous: per the beacon light-client protocol, the period-remotePeriod update should
            // always carry next_sync_committee once the chain has advanced past that period. Without
            // it we cannot advance the remote anchor, so this cycle's bundle is steady-state only and
            // will not verify against a remote anchor that is more than one period behind.
            log.error(
                    "[ETH-POS] bundle CONSTRUCT anomaly for connection {}: local_period={} > remote_period={} "
                            + "but the period-{} light-client update carries no next_sync_committee; cannot "
                            + "advance the remote trust anchor this cycle.",
                    connectionId.toHex(),
                    rotation.currentPeriod(),
                    rotation.remotePeriod(),
                    rotation.remotePeriod());
        }
        final SyncCommittee nextCommittee = rotate ? update.nextSyncCommittee() : null;
        final List<Bytes> nextCommitteeBranch = rotate ? update.nextSyncCommitteeBranch() : null;

        final long attestedExecBlock = attested.execution().blockNumber();
        final String attestedBlockTag = toBlockTag(attestedExecBlock);

        final var hasMessages = !messagesToInclude.isEmpty();
        final var maybeLastMessage = hasMessages ? messagesToInclude.getLast() : null;

        log.info(
                "bundle CONSTRUCT proof msgs=[{}] msg_count={} attestedExecBlock={} block={}",
                messagesToInclude.isEmpty()
                        ? ""
                        : (messagesToInclude.getFirst().messageId() + ".."
                                + messagesToInclude.getLast().messageId()),
                messagesToInclude.size(),
                attestedExecBlock,
                blockNumber);

        final Optional<ProofResponse> maybeProof = fetchAndValidateChannelProof(
                connectionId, maybeLastMessage, channelProofSlots(channel, messagesToInclude), attestedBlockTag);
        if (maybeProof.isEmpty()) {
            return;
        }
        final ProofResponse proofResp = maybeProof.get();
        final var storageEntries = proofResp.storageProof().stream()
                .map(e -> new EthStorageProofEntry(e.key(), e.proof()))
                .toList();

        final ManifestProof manifestProof = resolveManifestProof(connectionId, attestedBlockTag);

        final Bytes serializedContent = buildSerializedContent(channel, messagesToInclude);

        final List<byte[]> nonSignerProofs = buildNonSignerProofs(rotation.currentCommittee(), update.syncAggregate());

        final var payload = new EthBundlePayload(
                attested,
                update.syncAggregate(),
                nextCommittee,
                nextCommitteeBranch,
                proofResp.accountProof(),
                storageEntries,
                serializedContent,
                nonSignerProofs,
                manifestProof.storageProof(),
                manifestProof.preimage());
        final Bytes proofBytes = Bytes.wrap(PayloadRlpEncoder.encode(payload));
        bundleProofCache.put(connectionId, proofBytes);

        // The bundle advances the verifier's anchor by one period; track that so the next bundle
        // sources from — and, if needed, advances past — the new period.
        if (rotate) {
            remotePeriodByConnection.put(connectionId, rotation.remotePeriod() + 1);
            currentCommitteeByConnection.put(connectionId, nextCommittee);
        }

        log.info(
                "bundle CONSTRUCTED {} status={} block={} attestedExecBlock={} beaconSlot={} period={} "
                        + "rotated={} accountProofNodes={} storageProofEntries={} contentBytes={}",
                BundleLog.tag(proofBytes),
                channel.status(),
                blockNumber,
                attestedExecBlock,
                attested.beacon().slot(),
                rotation.remotePeriod(),
                rotate,
                proofResp.accountProof().size(),
                storageEntries.size(),
                serializedContent.length());
    }

    /**
     * Resolves the sync-committee period bookkeeping for {@code connectionId}: the local chain's
     * current period, the period the remote anchor is believed to be on (seeding it to
     * {@code currentPeriod} on the first bundle), and the committee currently used to identify
     * non-signers (seeded from the finalized bootstrap on the first bundle). Returns {@code null} —
     * having already logged the failure — if no committee is cached yet and the bootstrap didn't
     * supply one.
     */
    private @Nullable RotationState resolveRotationState(final Bytes connectionId, final ClprChannel channel) {
        // Source the attested header from the sync-committee-period light-client update (not just the
        // finality update), because that is the one endpoint that also carries next_sync_committee for
        // rotation. We always source from the period the *remote anchor* trusts, so the attested
        // header is signed by a committee the verifier still accepts.
        final long slotsPerSyncCommitteePeriod = beaconClient.getSlotsPerSyncCommitteePeriod();
        final long currentPeriod =
                beaconClient.getFinalityUpdate().attestedHeader().beacon().slot() / slotsPerSyncCommitteePeriod;
        final long remotePeriod = remotePeriodByConnection.computeIfAbsent(connectionId, k -> {
            // TODO: with 2-phase sync, seed from channel.trustAnchorId() (the remote verifier's
            // actual anchor period) instead of assuming it matches the current local beacon period.
            // Until then this is fragile: a relay restart or a slow remote anchor will produce an
            // incorrect seed and may require an out-of-band anchor refresh.
            log.debug(
                    "bundle CONSTRUCT seed remote_period={} slot={} status={}",
                    currentPeriod,
                    currentPeriod * slotsPerSyncCommitteePeriod,
                    channel.status());
            return currentPeriod;
        });

        if (!currentCommitteeByConnection.containsKey(connectionId)) {
            final LightClientBootstrap bootstrap = beaconClient.getBootstrap(beaconClient.getFinalizedBlockRootHex());
            currentCommitteeByConnection.putIfAbsent(connectionId, bootstrap.currentSyncCommittee());
        }
        final SyncCommittee currentCommittee = currentCommitteeByConnection.get(connectionId);
        if (currentCommittee == null) {
            logBundleCreationError(
                    connectionId, null, "Current sync committee not cached; cannot build non-signer proofs.");
            return null;
        }
        return new RotationState(currentPeriod, remotePeriod, currentCommittee);
    }

    /**
     * Fetches the committee-period light-client update for {@code remotePeriod} (the remote anchor's
     * period) and validates its attested header's execution-state-root branch. Empty — having already
     * logged the failure — if the beacon node has no update for that period or the branch doesn't
     * verify.
     */
    private Optional<LightClientUpdate> fetchPeriodUpdate(final Bytes connectionId, final long remotePeriod) {
        final List<LightClientUpdate> updates = beaconClient.getUpdates(remotePeriod, 1);
        if (updates.isEmpty()) {
            logBundleCreationError(
                    connectionId,
                    null,
                    "No light-client update for committee period " + remotePeriod
                            + " (remote anchor period) — cannot build a verifiable bundle.");
            return Optional.empty();
        }
        final LightClientUpdate update = updates.getFirst();
        if (!update.attestedHeader().executionStateRootBranchVerifies()) {
            logBundleCreationError(
                    connectionId, null, "Beacon attested header's execution-state-root branch does not verify.");
            return Optional.empty();
        }
        return Optional.of(update);
    }

    /**
     * Five channel-metadata slots, always present (the verifier sorts by key, so order here is not
     * load-bearing), plus a sixth slot proving the last included message's running hash, present only
     * when {@code messagesToInclude} is non-empty — mirroring {@code QbftBundleConstructor} and the
     * on-chain verifier's {@code _channelMetadataSlots}/{@code _verifyChannelStorage}.
     */
    private static List<BigInteger> channelProofSlots(
            final ClprChannel channel, final List<ContractStateReader.QueuedMessage> messagesToInclude) {
        final var slotsToProve = new ArrayList<BigInteger>(STORAGE_PROOF_ENTRY_COUNT);
        slotsToProve.add(calculateChannelFieldStorageSlot(channel.channelId(), CH_OFFSET_STATUS_AND_NEXT_MSG_ID));
        slotsToProve.add(calculateChannelFieldStorageSlot(channel.channelId(), CH_OFFSET_RECEIVED_MSG_ID));
        slotsToProve.add(calculateChannelFieldStorageSlot(channel.channelId(), CH_OFFSET_SENT_RUNNING_HASH));
        slotsToProve.add(calculateChannelFieldStorageSlot(channel.channelId(), CH_OFFSET_RECEIVED_RUNNING_HASH));
        slotsToProve.add(calculateChannelFieldStorageSlot(channel.channelId(), CH_OFFSET_ENDPOINT_MANIFEST_VERSION));
        if (!messagesToInclude.isEmpty()) {
            slotsToProve.add(calculateMsgRunningHashStorageSlot(
                    channel.channelId(), messagesToInclude.getLast().messageId()));
        }
        return slotsToProve;
    }

    /**
     * Fetches the EIP-1186 proof for {@code slotsToProve} at {@code attestedBlockTag} and validates
     * it: a non-empty account proof, exactly {@code slotsToProve.size()} storage entries, and every
     * entry carrying a non-empty proof. Empty — having already logged the failure — otherwise.
     */
    private Optional<ProofResponse> fetchAndValidateChannelProof(
            final Bytes connectionId,
            @Nullable final ContractStateReader.QueuedMessage maybeLastMessage,
            final List<BigInteger> slotsToProve,
            final String attestedBlockTag) {
        final var slotsHex = slotsToProve.stream().map(EvmUtils::toSlotHex).toArray(String[]::new);
        final ProofResponse proofResp =
                rpcClient.ethGetProof(clprServiceAddress.toHexString(), slotsHex, attestedBlockTag);

        if (proofResp.accountProof().isEmpty()) {
            logBundleCreationError(connectionId, maybeLastMessage, "Account proof is empty.");
            return Optional.empty();
        }
        if (proofResp.storageProof().size() != slotsToProve.size()) {
            logBundleCreationError(
                    connectionId,
                    maybeLastMessage,
                    String.format(
                            "Storage proof is missing some slots (expected %s, but got %s).",
                            slotsToProve.size(), proofResp.storageProof().size()));
            return Optional.empty();
        }
        if (proofResp.storageProof().stream().anyMatch(e -> e.proof().isEmpty())) {
            logBundleCreationError(connectionId, maybeLastMessage, "Some storage proofs are empty.");
            return Optional.empty();
        }
        return Optional.of(proofResp);
    }

    /**
     * Resolves the optional Step-3 endpoint-manifest proof (spec §4.2 Step 1b): attach only when our
     * LOCAL manifest is ahead of the version the peer has cached of it. That decision is shared across
     * bundle formats in {@link ManifestProofPolicy} so no codec can forget it — here we only supply the
     * Ethereum-PoS-specific proof once the policy says a manifest is due. Proven at the same
     * beacon-attested block as the rest of the bundle, via a separate {@code ethGetProof} call so
     * {@link #fetchAndValidateChannelProof} stays simple. Returns {@link ManifestProof#NONE} when no
     * manifest is due or the proof could not be fetched.
     */
    private ManifestProof resolveManifestProof(final Bytes connectionId, final String attestedBlockTag) {
        final Optional<ClprEndpointManifest> manifestDue =
                ManifestProofPolicy.manifestToAttach(stateReader, peerManifestVersions, connectionId, attestedBlockTag);
        if (manifestDue.isEmpty()) {
            return ManifestProof.NONE;
        }
        final ClprEndpointManifest manifest = manifestDue.get();
        try {
            final var manifestProofResp = rpcClient.ethGetProof(
                    clprServiceAddress.toHexString(),
                    new String[] {EvmUtils.toSlotHex(EvmUtils.wrapToUint256(ENDPOINT_MANIFEST_COMMITMENT_SLOT))},
                    attestedBlockTag);
            if (manifestProofResp.storageProof().isEmpty()
                    || manifestProofResp.storageProof().get(0).proof().isEmpty()) {
                return ManifestProof.NONE;
            }
            final var manifestSp = manifestProofResp.storageProof().get(0);
            return new ManifestProof(
                    new EthStorageProofEntry(manifestSp.key(), manifestSp.proof()),
                    ClprEndpointManifest.PROTOBUF.toBytes(manifest));
        } catch (final Exception e) {
            log.warn(
                    "manifest proof unavailable for channel {} (localVersion={}): {}",
                    connectionId.toHex(),
                    manifest.version(),
                    e.getMessage());
            return ManifestProof.NONE;
        }
    }

    /**
     * Builds the serialized {@link ClprBundleContent} (queue metadata + message payloads) to embed.
     * The verifier ignores this embedded copy and trusts the storage proofs instead; it is carried
     * only for parity with the QBFT bundle.
     */
    private static Bytes buildSerializedContent(
            final ClprChannel channel, final List<ContractStateReader.QueuedMessage> messagesToInclude) {
        final ClprQueueMetadata metadata = buildQueueMetadata(channel);
        final List<ClprMessagePayload> payloads = messagesToInclude.stream()
                .map(ContractStateReader.QueuedMessage::value)
                .map(ClprMessageValue::payload)
                .toList();
        final ClprBundleContent content = ClprBundleContent.newBuilder()
                .metadata(metadata)
                .messages(payloads)
                .build();
        return ClprBundleContent.PROTOBUF.toBytes(content);
    }

    /**
     * Builds the non-signer proof list: one 416-byte entry per non-signing committee member.
     * Each entry is {@code uncompressedG1(128) ‖ merkleProof(9 × 32)} — the member's EIP-2537
     * uncompressed G1 pubkey followed by the depth-9 SSZ Merkle inclusion branch of their pubkey
     * leaf in the 512-leaf pubkeys-vector tree.
     */
    private static List<byte[]> buildNonSignerProofs(final SyncCommittee committee, final SyncAggregate aggregate) {
        final byte[][] chunks = new byte[Ssz.SYNC_COMMITTEE_SIZE][];
        for (int i = 0; i < Ssz.SYNC_COMMITTEE_SIZE; i++) {
            chunks[i] = Ssz.pubkeyHash64(committee.pubkeys().get(i).toByteArray());
        }
        final byte[] bits = aggregate.bits().toByteArray();
        final List<byte[]> entries = new ArrayList<>();
        for (int i = 0; i < Ssz.SYNC_COMMITTEE_SIZE; i++) {
            // SSZ bitvector is little-endian: bit i is at byte i/8, bit i%8.
            final boolean signed = (bits[i / 8] & (1 << (i % 8))) != 0;
            if (signed) {
                continue;
            }
            final byte[] uncompressed = HintsLibraryBridge.getInstance()
                    .decompressG1ToEip2537(committee.pubkeys().get(i).toByteArray());
            if (uncompressed == null) {
                throw new IllegalArgumentException("G1 decompression failed for non-signer at committee index " + i);
            }
            final byte[][] branch = Ssz.generateProof(chunks, i);
            final byte[] entry = new byte[NON_SIGNER_ENTRY_BYTES];
            System.arraycopy(uncompressed, 0, entry, 0, 128);
            for (int j = 0; j < branch.length; j++) {
                System.arraycopy(branch[j], 0, entry, 128 + j * 32, 32);
            }
            entries.add(entry);
        }
        return entries;
    }

    private void logBundleCreationError(
            final Bytes connectionId,
            @Nullable final ContractStateReader.QueuedMessage lastMessage,
            final String extraInfo) {
        log.error(
                "[ETH-POS] Failed to create a proof for connection ID {} and last message ID {}. {}",
                connectionId.toHex(),
                lastMessage != null ? lastMessage.messageId() : "<ack-only>",
                extraInfo);
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

    private static final class PayloadRlpEncoder {
        private static final byte[] EMPTY = new byte[0];

        static byte[] encode(final EthBundlePayload p) {
            final var beacon = p.attestedHeader().beacon();
            final byte[] attestedHeader = rlpList(
                    rlpBigInt(BigInteger.valueOf(beacon.slot())),
                    rlpBigInt(BigInteger.valueOf(beacon.proposerIndex())),
                    rlpBytes(beacon.parentRoot().toByteArray()),
                    rlpBytes(beacon.stateRoot().toByteArray()),
                    rlpBytes(beacon.bodyRoot().toByteArray()));

            final var aggregate = p.syncAggregate();
            final byte[] syncAggregate = rlpList(
                    rlpBytes(aggregate.bits().toByteArray()),
                    rlpBytes(aggregate.signature().toByteArray()));

            final byte[] executionStateRoot =
                    rlpBytes(p.attestedHeader().executionStateRoot().toByteArray());

            final byte[][] branchNodes = p.attestedHeader().executionStateRootBranch();
            final byte[] executionBranch = rlpList(encodeNodes(branchNodes));

            // Rotation pair: both absent (empty string + empty list) for a steady-state bundle, or
            // both present when advancing the trust anchor across a committee-period boundary.
            final byte[] nextCommittee =
                    p.nextCommittee() == null ? rlpBytes(EMPTY) : encodeCommittee(p.nextCommittee());
            final byte[] nextCommitteeBranch =
                    p.nextCommitteeBranch() == null ? rlpList() : rlpList(encodeByteList(p.nextCommitteeBranch()));

            final byte[] accountProof = rlpList(p.clprServiceAccountProof().stream()
                    .map(b -> rlpBytes(b.toByteArray()))
                    .toArray(byte[][]::new));
            final byte[] storageProof = rlpList(p.clprServiceStorageProofs().stream()
                    .map(PayloadRlpEncoder::encodeStorageEntry)
                    .toArray(byte[][]::new));
            final byte[] bundleContent = rlpBytes(p.serializedBundleContent().toByteArray());
            final byte[] nonSignerProofsRlp =
                    rlpList(p.nonSignerProofs().stream().map(AbiCodec::rlpBytes).toArray(byte[][]::new));

            if (p.manifestStorageProof() != null && p.serializedEndpointManifest() != null) {
                // Entry-wrapped ([slotKey, [nodes]]), the same shape the QBFT codec uses for its
                // manifest-commitment proof — a flat list of node strings would make the verifier's
                // RLP.readList revert.
                final byte[] manifestProofElem = rlpList(encodeStorageEntry(p.manifestStorageProof()));
                final byte[] manifestPreimageElem =
                        rlpBytes(p.serializedEndpointManifest().toByteArray());
                return rlpList(
                        attestedHeader,
                        syncAggregate,
                        executionStateRoot,
                        executionBranch,
                        nextCommittee,
                        nextCommitteeBranch,
                        accountProof,
                        storageProof,
                        bundleContent,
                        nonSignerProofsRlp,
                        manifestProofElem,
                        manifestPreimageElem);
            }
            return rlpList(
                    attestedHeader,
                    syncAggregate,
                    executionStateRoot,
                    executionBranch,
                    nextCommittee,
                    nextCommitteeBranch,
                    accountProof,
                    storageProof,
                    bundleContent,
                    nonSignerProofsRlp);
        }

        private static byte[] encodeStorageEntry(final EthStorageProofEntry e) {
            return rlpList(
                    rlpBytes(leftPad32(e.key()).toByteArray()),
                    rlpList(e.proof().stream()
                            .map(b -> rlpBytes(b.toByteArray()))
                            .toArray(byte[][]::new)));
        }

        private static byte[][] encodeNodes(final byte[][] nodes) {
            final byte[][] out = new byte[nodes.length][];
            for (int i = 0; i < nodes.length; i++) {
                out[i] = rlpBytes(nodes[i]);
            }
            return out;
        }

        /** RLP-encodes a sync committee as {@code [pubkeys[512], aggregatePubkey]} (the verifier's shape). */
        private static byte[] encodeCommittee(final SyncCommittee committee) {
            // pubkeys are stored as 48-byte compressed; the Solidity verifier needs EIP-2537 uncompressed
            final List<Bytes> uncompressed = new ArrayList<>(committee.pubkeys().size());
            for (final Bytes pubkey : committee.pubkeys()) {
                final byte[] g1 = HintsLibraryBridge.getInstance().decompressG1ToEip2537(pubkey.toByteArray());
                if (g1 == null) {
                    throw new IllegalArgumentException("G1 decompression failed during committee encoding");
                }
                uncompressed.add(Bytes.wrap(g1));
            }
            return rlpList(
                    rlpList(encodeByteList(uncompressed)),
                    rlpBytes(committee.aggregatePubkey().toByteArray()));
        }

        /** RLP-encodes a list of byte strings as the elements of a list (each via {@code rlpBytes}). */
        private static byte[][] encodeByteList(final List<Bytes> items) {
            return items.stream().map(b -> rlpBytes(b.toByteArray())).toArray(byte[][]::new);
        }
    }
}
