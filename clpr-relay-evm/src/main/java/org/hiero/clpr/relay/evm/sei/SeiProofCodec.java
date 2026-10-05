// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.sei;

import com.hedera.hapi.node.state.clpr.ClprBundleContent;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprSeiBundlePayload;
import com.hedera.hapi.node.state.clpr.SeiStateProof;
import com.hedera.hapi.node.state.clpr.SeiStorageProofEntry;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.List;
import org.hiero.clpr.relay.core.BundlePayloadCodec;
import org.hiero.clpr.relay.core.BundleTrimmer;
import org.hiero.clpr.relay.core.ParsedBundle;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.qbft.QbftProofCodec;
import org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout;
import org.jspecify.annotations.NonNull;

/**
 * Handles the Sei (CometBFT) proof format produced by {@link SeiBundleConstructor}: a
 * {@code ClprSeiBundlePayload} protobuf carrying the CometBFT signed-header + ICS-23 state proof
 * (field 1), the verbatim {@code ClprBundleContent} bytes (field 2), an optional validator-set
 * rotation (field 3), and an optional endpoint-manifest proof + preimage (fields 4/5).
 *
 * <p>Like {@link QbftProofCodec}, <b>parses</b> the bundle
 * — it extracts the queue metadata and messages from the embedded {@link ClprBundleContent} and
 * passes the raw proof bytes through unchanged for on-chain submission. It performs <b>no</b>
 * cryptographic verification: the CometBFT signed header, validator-set quorum and ICS-23 storage
 * proofs are validated by the on-chain verifier contract during {@code submitBundle} (the
 * integration-test {@code StubClprVerifier} under the test wiring's CLPRSTUB re-encoder; the real
 * Sei verifier otherwise).
 *
 * <p>The {@code ClprSeiBundlePayload} protobuf and the Hiero {@code StateProof} both lead with tag
 * {@code 0x0a} (field 1, length-delimited), so leading-byte sniffing cannot tell them apart.
 * Disambiguation is therefore by the channel's {@code peerProofType} in
 * {@code ClprChannelHandler.codecFor()} — a {@code CometBFT} peer routes exclusively here.
 */
public final class SeiProofCodec implements BundlePayloadCodec {

    /** The channel this codec is bound to — used to locate the last message's running-hash slot. */
    private final Bytes channelId;

    public SeiProofCodec(@NonNull final Bytes channelId) {
        this.channelId = channelId;
    }

    @Override
    @NonNull
    public ParsedBundle decodeBundle(@NonNull final Bytes proofBytes) {
        final ClprBundleContent content = extractBundleContent(proofBytes);
        return new ParsedBundle(content.metadata(), content.messages(), proofBytes);
    }

    @Override
    @NonNull
    public ParsedBundle parseBundle(final Bytes proofBytes, final long receivedMessageId) {
        // Decode first, outside the re-encode try, so a malformed proof surfaces as a parse error.
        final ClprSeiBundlePayload payload = parsePayload(proofBytes);
        final ClprBundleContent bundle = parseContent(payload);
        final var metadata = bundle.metadata();
        final List<ClprMessagePayload> messages = bundle.messages();

        final List<ClprMessagePayload> kept = BundleTrimmer.trim(messages, metadata.nextMessageId(), receivedMessageId);
        if (kept == messages) {
            // Nothing already received — submit the peer's bundle verbatim (byte-identical, so the
            // on-chain verifier's proofs still match the content bytes the peer produced).
            return new ParsedBundle(metadata, messages, proofBytes);
        }

        try {
            // Reuse the structural elements (signed header, multistore proof, validator-set rotation)
            // from the original proof; only the bundle content (and, when fully trimmed, the storage
            // proofs) change. Metadata is unchanged; only already-applied messages were removed.
            final ClprBundleContent trimmedContent = ClprBundleContent.newBuilder()
                    .metadata(metadata)
                    .messages(kept)
                    .build();

            // The constructor proves only the last message's running hash. When a tail message survives it
            // is still the last one, so its proof stays valid. When no message survives, drop that
            // now-orphaned storage-proof entry so the proof set matches the content.
            SeiStateProof stateProof = payload.stateProof();
            if (kept.isEmpty() && stateProof != null) {
                stateProof = dropMessageRunningHashProof(stateProof, channelId, metadata.nextMessageId() - 1L);
            }

            final var rebuilt =
                    ClprSeiBundlePayload.newBuilder().bundleContent(ClprBundleContent.PROTOBUF.toBytes(trimmedContent));
            if (stateProof != null) {
                rebuilt.stateProof(stateProof);
            }
            if (payload.hasNextValidatorSet()) {
                rebuilt.nextValidatorSet(payload.nextValidatorSet());
            }
            // Preserve the endpoint-manifest proof (fields 4/5) verbatim — it proves the slot-18
            // commitment, not a message slot, so message trimming never invalidates it. Dropping it here
            // would silently strip the manifest advance from a partially/fully trimmed inbound bundle
            // before on-chain submission (mirrors the QBFT codec preserving RLP elements 5 and 6).
            if (payload.hasManifestStorageProof()) {
                rebuilt.manifestStorageProof(payload.manifestStorageProof())
                        .endpointManifest(payload.endpointManifest());
            }
            return new ParsedBundle(metadata, kept, ClprSeiBundlePayload.PROTOBUF.toBytes(rebuilt.build()));
        } catch (final Exception e) {
            throw new IllegalArgumentException("Failed to re-encode trimmed Sei bundle proof", e);
        }
    }

    /** Re-builds the state proof with the running-hash storage entry for {@code messageId} removed. */
    @NonNull
    private static SeiStateProof dropMessageRunningHashProof(
            @NonNull final SeiStateProof stateProof, @NonNull final Bytes channelId, final long messageId) {
        // Storage-proof keys are 0x03 || 20-byte contract address || 32-byte slot; match on the slot
        // suffix so we don't need the contract address here.
        final Bytes targetSlot = Bytes.wrap(AbiCodec.encodeUint256(
                ClprServiceStorageLayout.calculateMsgRunningHashStorageSlot(channelId, BigInteger.valueOf(messageId))));
        final List<SeiStorageProofEntry> kept = stateProof.storageProofs().stream()
                .filter(e -> !slotSuffixMatches(e.key(), targetSlot))
                .toList();
        return SeiStateProof.newBuilder()
                .signedHeader(stateProof.signedHeader())
                .storeKey(stateProof.storeKey())
                .multistoreProof(stateProof.multistoreProof())
                .storageProofs(kept)
                .build();
    }

    private static boolean slotSuffixMatches(@NonNull final Bytes abciKey, @NonNull final Bytes slot32) {
        final long len = abciKey.length();
        return len >= 32 && abciKey.slice(len - 32, 32).equals(slot32);
    }

    @NonNull
    private static ClprBundleContent extractBundleContent(@NonNull final Bytes proofBytes) {
        return parseContent(parsePayload(proofBytes));
    }

    @NonNull
    private static ClprSeiBundlePayload parsePayload(@NonNull final Bytes proofBytes) {
        try {
            return ClprSeiBundlePayload.PROTOBUF.parse(proofBytes);
        } catch (final Exception e) {
            throw new IllegalArgumentException("Failed to parse Sei bundle proof", e);
        }
    }

    @NonNull
    private static ClprBundleContent parseContent(@NonNull final ClprSeiBundlePayload payload) {
        try {
            return ClprBundleContent.PROTOBUF.parse(payload.bundleContent());
        } catch (final Exception e) {
            throw new IllegalArgumentException("Failed to parse Sei bundle proof", e);
        }
    }
}
