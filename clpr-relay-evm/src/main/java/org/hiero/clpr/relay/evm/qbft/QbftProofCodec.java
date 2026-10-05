// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.qbft;

import com.hedera.hapi.node.state.clpr.ClprBundleContent;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.hiero.clpr.relay.core.BundlePayloadCodec;
import org.hiero.clpr.relay.core.BundleTrimmer;
import org.hiero.clpr.relay.core.ParsedBundle;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.ByteUtils;
import org.hiero.clpr.relay.evm.EvmUtils;
import org.hiero.clpr.relay.evm.RlpReader;
import org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout;

/**
 * Handles the Besu-QBFT proof format produced by {@link QbftBundleConstructor}: an RLP list
 * {@code [currentBlockHeader, epochBlockHeaders, clprServiceAccountProof, clprServiceStorageProofs,
 * rlp(ClprBundleContent)]}.
 *
 * <p>This codec <b>decodes</b> the bundle — it extracts the queue metadata and messages from the
 * embedded {@link ClprBundleContent} (the 5th tuple element) and passes the raw proof bytes through
 * unchanged for on-chain submission. It performs <b>no</b> cryptographic verification: the
 * Merkle-Patricia account/storage proofs and the QBFT committed-seal recovery are validated by the
 * on-chain verifier contract during {@code submitBundle} (pre-flighted gas-free by
 * {@code AccountTransactionSubmitter}'s {@code eth_call} preview).
 *
 * <p>Living in {@code clpr-relay-evm} (the only ledger-aware module) keeps the RLP knowledge next to
 * {@link RlpReader} and {@link QbftBundleConstructor}.
 */
public final class QbftProofCodec implements BundlePayloadCodec {

    /** Number of RLP elements in the standard QBFT bundle tuple (without manifest proof). */
    private static final int QBFT_TUPLE_SIZE = 5;
    /** Number of RLP elements in a QBFT bundle tuple that includes a manifest proof. */
    private static final int QBFT_TUPLE_SIZE_WITH_MANIFEST = 7;
    /** Index of the {@code clprServiceStorageProofs} RLP list within the QBFT bundle tuple. */
    private static final int STORAGE_PROOFS_INDEX = 3;
    /** Index of the RLP-wrapped {@link ClprBundleContent} within the QBFT bundle tuple. */
    private static final int BUNDLE_CONTENT_INDEX = 4;

    /** The channel this codec is bound to — used to locate the last message's running-hash slot. */
    private final Bytes channelId;

    public QbftProofCodec(final Bytes channelId) {
        this.channelId = channelId;
    }

    @Override
    public ParsedBundle decodeBundle(final Bytes proofBytes) {
        final ClprBundleContent content = extractBundleContent(proofBytes);
        return new ParsedBundle(content.metadata(), content.messages(), proofBytes);
    }

    @Override
    public ParsedBundle parseBundle(final Bytes proofBytes, final long receivedMessageId) {
        // Decode first, outside the re-encode try, so a malformed proof surfaces as a parse error.
        final List<Bytes> items = splitTuple(proofBytes);
        final ClprBundleContent bundle = parseContent(items.get(BUNDLE_CONTENT_INDEX));
        final var metadata = bundle.metadata();
        final List<ClprMessagePayload> messages = bundle.messages();

        final List<ClprMessagePayload> kept = BundleTrimmer.trim(messages, metadata.nextMessageId(), receivedMessageId);
        if (kept == messages) {
            // Nothing already received — submit the peer's bundle verbatim (byte-identical, so the
            // on-chain verifier's proofs still match the content bytes the peer produced).
            return new ParsedBundle(metadata, messages, proofBytes);
        }

        try {
            // Reuse the structural elements (block headers, account/storage proofs) from the original
            // proof; only the content element (and, when fully trimmed, the storage-proof list) changes.
            // Metadata (the acknowledgment, running hashes and frontier) is unchanged; only the
            // already-applied message payloads have been removed by BundleTrimmer.
            final ClprBundleContent trimmedContent = ClprBundleContent.newBuilder()
                    .metadata(metadata)
                    .messages(kept)
                    .build();
            final byte[] contentElem = AbiCodec.rlpBytes(
                    ClprBundleContent.PROTOBUF.toBytes(trimmedContent).toByteArray());

            // The constructor proves only the last message's running hash. When a tail message survives
            // it is still the last one, so its proof stays valid and the storage-proof list is untouched.
            // When no message survives, drop that now-orphaned entry so the proof set matches the content.
            final byte[] storageElem = kept.isEmpty()
                    ? dropMessageRunningHashProof(
                            items.get(STORAGE_PROOFS_INDEX), channelId, metadata.nextMessageId() - 1L)
                    : items.get(STORAGE_PROOFS_INDEX).toByteArray();

            final byte[] reencoded;
            if (items.size() == QBFT_TUPLE_SIZE_WITH_MANIFEST) {
                // Preserve manifest proof (elements 5 and 6) when present in the original bundle.
                reencoded = AbiCodec.rlpList(
                        items.get(0).toByteArray(),
                        items.get(1).toByteArray(),
                        items.get(2).toByteArray(),
                        storageElem,
                        contentElem,
                        items.get(5).toByteArray(),
                        items.get(6).toByteArray());
            } else {
                reencoded = AbiCodec.rlpList(
                        items.get(0).toByteArray(),
                        items.get(1).toByteArray(),
                        items.get(2).toByteArray(),
                        storageElem,
                        contentElem);
            }
            return new ParsedBundle(metadata, kept, Bytes.wrap(reencoded));
        } catch (final Exception e) {
            throw new IllegalArgumentException("Failed to re-encode trimmed QBFT bundle proof", e);
        }
    }

    /** Re-encodes the storage-proof list with the running-hash entry for {@code messageId} removed. */
    private static byte[] dropMessageRunningHashProof(
            final Bytes storageProofsRlp, final Bytes channelId, final long messageId) {
        final Bytes targetSlot = ByteUtils.leftPad32(ByteUtils.fromPrefixedHex(
                EvmUtils.toSlotHex(ClprServiceStorageLayout.calculateMsgRunningHashStorageSlot(
                        channelId, BigInteger.valueOf(messageId)))));
        final List<Bytes> entries = RlpReader.splitList(storageProofsRlp);
        final List<byte[]> kept = new ArrayList<>(entries.size());
        for (final Bytes entry : entries) {
            final Bytes key = RlpReader.bytesValue(RlpReader.splitList(entry).getFirst());
            if (!ByteUtils.leftPad32(key).equals(targetSlot)) {
                kept.add(entry.toByteArray());
            }
        }
        return AbiCodec.rlpList(kept.toArray(new byte[0][]));
    }

    private static ClprBundleContent extractBundleContent(final Bytes proofBytes) {
        return parseContent(splitTuple(proofBytes).get(BUNDLE_CONTENT_INDEX));
    }

    /** Parse the RLP-wrapped {@link ClprBundleContent} (the {@value #BUNDLE_CONTENT_INDEX}th tuple element). */
    private static ClprBundleContent parseContent(final Bytes contentElement) {
        try {
            return ClprBundleContent.PROTOBUF.parse(RlpReader.bytesValue(contentElement));
        } catch (final Exception e) {
            throw new IllegalArgumentException("Failed to parse QBFT bundle proof", e);
        }
    }

    private static List<Bytes> splitTuple(final Bytes proofBytes) {
        final List<Bytes> items = RlpReader.splitList(proofBytes);
        if (items.size() != QBFT_TUPLE_SIZE && items.size() != QBFT_TUPLE_SIZE_WITH_MANIFEST) {
            throw new IllegalArgumentException("expected " + QBFT_TUPLE_SIZE + " or " + QBFT_TUPLE_SIZE_WITH_MANIFEST
                    + " RLP elements in QBFT bundle, got " + items.size());
        }
        return items;
    }
}
