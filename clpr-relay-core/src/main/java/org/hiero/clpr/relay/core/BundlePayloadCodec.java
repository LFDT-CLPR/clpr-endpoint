// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import com.hedera.pbj.runtime.io.buffer.Bytes;

/**
 * Decodes a chain-specific bundle payload into its CLPR content (queue metadata + messages), and can
 * re-encode a replay-trimmed content back into the same wire format. Each implementation understands
 * exactly one proof wire format.
 *
 * <p>Despite the "proof" in the concrete implementation names, an implementation performs <b>no</b>
 * cryptographic verification: the Merkle-Patricia, QBFT committed-seal and CometBFT signed-header
 * checks all run on-chain in the verifier contract during {@code submitBundle} (and may be pre-flighted
 * gas-free via {@code eth_call}). The client side only decodes the opaque proof bytes far enough to read
 * the metadata and messages, keeping the raw bytes for verbatim submission.
 *
 * <p>Replay-trimming reuses the chain-agnostic message selection in {@link BundleTrimmer#trim}; the
 * format-specific re-encoding of the retained messages is folded into the {@code receivedMessageId}
 * overload of {@link #parseBundle}. Read-only formats (e.g. the Hiero block-stream {@code StateProof})
 * cannot re-encode, so their trimming overload returns the full bundle unchanged.
 *
 * <p>Implementations throw {@link IllegalArgumentException} when the bytes are not a well-formed payload
 * of the expected format.
 */
public interface BundlePayloadCodec {

    /**
     * Decodes a bundle payload into its metadata and messages, keeping the raw bytes verbatim. This is
     * the plain decode with no replay-trimming; the on-chain submit path uses {@link #parseBundle}
     * instead.
     *
     * @param proofBytes the raw proof bytes
     * @return the parsed bundle, containing metadata, messages, and the raw proof bytes
     * @throws IllegalArgumentException if the bytes are not a well-formed payload
     */
    ParsedBundle decodeBundle(final Bytes proofBytes);

    /**
     * Decodes a bundle payload and drops every leading message the receiving ledger has already
     * accepted, retaining only the contiguous suffix whose ids are strictly greater than
     * {@code receivedMessageId}, then re-encodes that suffix back into this codec's proof wire format so
     * the trimmed payload is what lands on chain.
     *
     * <p>When nothing is dropped (or the format is read-only and cannot re-encode) the peer's original
     * bytes are returned verbatim — a codec must never re-serialize an unchanged bundle, since the
     * on-chain verifier checks its proofs against the exact content bytes the peer produced.
     *
     * <p>A codec instance is bound to one channel: the channel id it needs to rebuild the proof
     * (e.g. the storage-slot of the last message's running hash) is supplied at construction, so it is
     * not a parameter here.
     *
     * @param proofBytes the raw proof bytes received from the peer
     * @param receivedMessageId the highest message id the receiving ledger has already accepted;
     *     messages with id {@code <= receivedMessageId} are dropped
     * @return the parsed bundle with already-received messages removed (or the original when nothing was
     *     trimmed), whose {@link ParsedBundle#rawBundleBytes()} are what to submit on chain
     * @throws IllegalArgumentException if the bytes are not a well-formed payload
     */
    ParsedBundle parseBundle(final Bytes proofBytes, final long receivedMessageId);
}
