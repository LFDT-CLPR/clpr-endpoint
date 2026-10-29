// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import com.hedera.hapi.node.state.clpr.ClprBundleContent;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.clpr.relay.core.BundlePayloadCodec;
import org.hiero.clpr.relay.core.ParsedBundle;

/**
 * Handles the Ethereum-PoS proof format produced by {@link EthereumPosBundleConstructor}: a top-level
 * RLP list of ten items
 * {@code [attestedHeader, syncAggregate, executionStateRoot, executionBranch, nextCommittee,
 * nextCommitteeBranch, accountProof, storageProof, rlp(ClprBundleContent), nonSignerProofs]}, or
 * twelve when a local endpoint-manifest proof is attached (two extra trailing items:
 * {@code manifestProof, manifestPreimage}).
 *
 * <p>This codec <b>parses</b> the bundle — it extracts the queue metadata and messages from the
 * embedded {@link ClprBundleContent} (index {@value #BUNDLE_CONTENT_INDEX}) and passes the raw proof
 * bytes through unchanged for on-chain submission. It performs <b>no</b> cryptographic verification:
 * the BLS sync-committee signature, the SSZ branches, and the Merkle-Patricia account/storage proofs
 * are validated by the peer's verifier during submission.
 *
 * <p>Read-only format: the SSZ proofs are anchored to a specific attested beacon execution block;
 * re-encoding with a trimmed message set would require re-fetching proofs from the beacon node,
 * which this module does not have access to. {@link #parseBundle} therefore returns the original
 * bytes verbatim, leaving already-received messages for the on-chain replay guard.
 *
 * <p>Mirrors {@link org.hiero.clpr.relay.evm.qbft.QbftProofCodec} (which likewise accepts a with- or
 * without-manifest tuple); the structural differences are the tuple sizes
 * ({@value #ETH_POS_TUPLE_SIZE} / {@value #ETH_POS_TUPLE_SIZE_WITH_MANIFEST}) and the bundle-content
 * index ({@value #BUNDLE_CONTENT_INDEX}).
 */
public final class EthereumPosProofCodec implements BundlePayloadCodec {

    /** Number of RLP elements in the standard Ethereum-PoS bundle tuple (without manifest proof). */
    private static final int ETH_POS_TUPLE_SIZE = 10;
    /** Number of RLP elements in an Ethereum-PoS bundle tuple that includes a manifest proof. */
    private static final int ETH_POS_TUPLE_SIZE_WITH_MANIFEST = 12;
    /** Index of the RLP-wrapped {@link ClprBundleContent} within the Ethereum-PoS bundle tuple. */
    private static final int BUNDLE_CONTENT_INDEX = 8;

    @Override
    public ParsedBundle decodeBundle(final Bytes proofBytes) {
        final ClprBundleContent content = extractBundleContent(proofBytes);
        return new ParsedBundle(content.metadata(), content.messages(), proofBytes);
    }

    @Override
    public ParsedBundle parseBundle(final Bytes proofBytes, final long receivedMessageId) {
        // Read-only format: SSZ proofs are anchored to the attested execution block and cannot be
        // re-encoded client-side. Return the bundle verbatim; the on-chain replay guard handles
        // already-received messages.
        return decodeBundle(proofBytes);
    }

    private static ClprBundleContent extractBundleContent(final Bytes proofBytes) {
        try {
            final List<Bytes> items = RlpReader.splitList(proofBytes);
            if (items.size() != ETH_POS_TUPLE_SIZE && items.size() != ETH_POS_TUPLE_SIZE_WITH_MANIFEST) {
                throw new IllegalArgumentException("expected " + ETH_POS_TUPLE_SIZE + " or "
                        + ETH_POS_TUPLE_SIZE_WITH_MANIFEST + " RLP elements in Ethereum-PoS bundle, got "
                        + items.size());
            }
            final Bytes contentBytes = RlpReader.bytesValue(items.get(BUNDLE_CONTENT_INDEX));
            return ClprBundleContent.PROTOBUF.parse(contentBytes);
        } catch (final Exception e) {
            throw new IllegalArgumentException("Failed to parse Ethereum-PoS bundle proof", e);
        }
    }
}
