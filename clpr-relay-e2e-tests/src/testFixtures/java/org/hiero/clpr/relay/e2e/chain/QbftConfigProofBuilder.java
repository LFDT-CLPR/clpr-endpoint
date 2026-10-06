// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.chain;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.BlockHeaderRlpCodec;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.model.BlockHeader;
import org.hiero.clpr.relay.evm.model.ProofResponse;

/**
 * Builds the two RLP proofs {@code QBFTVerifier.verifyConfig} demands when a channel is completed
 * against the <b>real</b> verifier.
 *
 * <p>Both proofs describe the <em>peer</em> chain: chain A's
 * {@code completeChannel} is handed a proof of chain B's state and vice versa.
 *
 * <p>No RLP library and no {@code debug_} RPC is needed. {@code AbiCodec}'s RLP primitives and
 * {@code BlockHeaderRlpCodec} already exist because the relay constructs QBFT bundle proofs with
 * them — and that codec has to reproduce Besu's header serialisation byte-for-byte or the relay's own
 * proofs would not verify, which makes it a better source for the raw header than
 * {@code debug_getRawHeader}.
 */
public final class QbftConfigProofBuilder {

    /**
     * Per-channel throttles advertised in the config proof.
     *
     * <p>Five entries, matching what the verifier reads. {@code ClprTypes.Throttles} carries two more
     * endpoint-limit fields, but {@code verifyConfig} accepts the five-entry list and defaults those
     * to "no limit", so adding them here would make the RLP unparseable.
     */
    public static final long MAX_MESSAGES_PER_BUNDLE = 100L;

    public static final long MAX_MESSAGE_PAYLOAD_BYTES = 16_384L;
    public static final long MAX_GAS_PER_MESSAGE = 5_000_000L;
    public static final long MAX_QUEUE_DEPTH = 1_024L;
    public static final long MAX_SYNC_BYTES = 1_000_000L;

    /** Must match the Besu genesis {@code qbft.epochlength}. */
    public static final long QBFT_EPOCH_LENGTH = 30_000L;

    /**
     * Storage slot of {@code _endpointManifest.commitment}: struct base 17 plus field offset 1. Holds
     * {@code keccak256(protobuf(ClprEndpointManifest))}.
     */
    public static final String ENDPOINT_MANIFEST_COMMITMENT_SLOT = "0x" + "00".repeat(31) + "12";

    private QbftConfigProofBuilder() {}

    /**
     * Build the ten-field config proof for {@code completeChannel}, against a specific
     * {@code ClprService} deployment on the peer chain.
     *
     * <p>Takes the peer network AND the exact service address so a channel wired against a
     * non-primary {@code ClprService} (via {@code BesuNetwork.deployAdditionalService()}) can still
     * produce a valid trust anchor — the anchor names the peer service, and using the primary
     * address for a channel that actually lives at a different deployment would leave the on-chain
     * anchor pointing at the wrong contract.
     *
     * @param peer the chain being proven
     * @param peerServiceAddress the {@code ClprService} on {@code peer} the channel is registered on
     * @return the RLP-encoded proof
     */
    public static byte[] buildConfigProof(final BesuNetwork peer, final String peerServiceAddress) {
        final EvmJsonRpcClient peerRpc = peer.rpc();
        final String peerValidatorAddress = peer.validatorAddress();
        final String peerChainId = peer.caip2();

        final String code = peer.rawRpc().ethGetCode(peerServiceAddress);
        if (code == null || code.isBlank() || "0x".equals(code)) {
            throw new IllegalStateException(
                    "no code at ClprService " + peerServiceAddress + " — cannot build a trust anchor for it");
        }
        final byte[] codeHash = AbiCodec.Keccak256.keccak256(AbiCodec.fromHex(code));

        final BlockHeader header = peerRpc.ethGetBlockHeaderByNumber("latest");
        final byte[] headerRlp = BlockHeaderRlpCodec.encodeRlp(header).toByteArray();

        // Field [6] is a byte STRING whose content is itself RLP([validator, service, codeHash]).
        final byte[] trustAnchorInner = AbiCodec.rlpList(
                AbiCodec.rlpBytes(AbiCodec.fromHex(peerValidatorAddress)),
                AbiCodec.rlpBytes(AbiCodec.fromHex(peerServiceAddress)),
                AbiCodec.rlpBytes(codeHash));

        return AbiCodec.rlpList(
                AbiCodec.rlpBytes(AbiCodec.fromHex(peerValidatorAddress)), // [0] validator
                AbiCodec.rlpBytes(AbiCodec.fromHex(peerServiceAddress)), // [1] peer service address
                AbiCodec.rlpBytes(new byte[32]), // [2] codeHash, unused by verifyConfig
                AbiCodec.rlpBytes(peerChainId.getBytes(StandardCharsets.UTF_8)), // [3] chainId
                AbiCodec.rlpBytes(new byte[0]), // [4] peerConfigNanos = 0
                AbiCodec.rlpList( // [5] throttles
                        AbiCodec.rlpBigInt(BigInteger.valueOf(MAX_MESSAGES_PER_BUNDLE)),
                        AbiCodec.rlpBigInt(BigInteger.valueOf(MAX_MESSAGE_PAYLOAD_BYTES)),
                        AbiCodec.rlpBigInt(BigInteger.valueOf(MAX_GAS_PER_MESSAGE)),
                        AbiCodec.rlpBigInt(BigInteger.valueOf(MAX_QUEUE_DEPTH)),
                        AbiCodec.rlpBigInt(BigInteger.valueOf(MAX_SYNC_BYTES))),
                AbiCodec.rlpBytes(trustAnchorInner), // [6] trust anchor, wrapped as a byte string
                AbiCodec.rlpBytes(new byte[0]), // [7] trustAnchorId, empty
                headerRlp, // [8] header, spliced as a nested list
                AbiCodec.rlpBigInt(BigInteger.valueOf(QBFT_EPOCH_LENGTH))); // [9] epochLength
    }

    /**
     * Build the manifest proof against a specific {@code ClprService} on {@code peer}, for channels
     * wired against an additional deployment rather than the chain's primary.
     *
     * @param peer the chain being proven
     * @param peerServiceAddress the {@code ClprService} address whose manifest slot to read
     * @return the RLP-encoded proof
     */
    public static byte[] buildManifestProof(final BesuNetwork peer, final String peerServiceAddress) {
        final EvmJsonRpcClient peerRpc = peer.rpc();
        final BlockHeader header = peerRpc.ethGetBlockHeaderByNumber("latest");
        // Pin the proof to the same block the header describes. Reading the header at "latest" and the
        // proof at a later "latest" would pair a state root with a state it does not commit to, and
        // the verifier would reject the mismatch as a malformed proof.
        final String blockTag = "0x" + header.number().toString(16);

        final ProofResponse proof =
                peerRpc.ethGetProof(peerServiceAddress, new String[] {ENDPOINT_MANIFEST_COMMITMENT_SLOT}, blockTag);
        if (proof.storageProof().isEmpty()) {
            throw new IllegalStateException(
                    "eth_getProof returned no storage proof for the manifest slot on " + peerServiceAddress);
        }
        final ProofResponse.StorageProofEntry entry = proof.storageProof().getFirst();

        final byte[] preimage = manifestPreimage(peerServiceAddress);
        final byte[] expected = AbiCodec.Keccak256.keccak256(preimage);
        final byte[] proven = leftPad32(entry.value().toByteArray());
        if (!java.util.Arrays.equals(expected, proven)) {
            // Check locally before submitting: a protobuf encoding drift would otherwise surface as an
            // opaque contract revert inside completeChannel, far from its cause.
            throw new IllegalStateException(
                    "manifest preimage mismatch on " + peerServiceAddress + ": keccak(preimage)="
                            + AbiCodec.toHex(expected) + " but the proven slot holds " + AbiCodec.toHex(proven)
                            + ". The live manifest is not the expected {version:1, service_address, endpoints:[]} —"
                            + " has an endpoint been admitted to the manifest already?");
        }

        final byte[] accountProofRlp = rlpOfByteStrings(
                proof.accountProof().stream().map(b -> b.toByteArray()).toList());
        final byte[] storageNodesRlp = rlpOfByteStrings(
                entry.proof().stream().map(b -> b.toByteArray()).toList());
        final byte[] manifestStorageProof = AbiCodec.rlpList(AbiCodec.rlpList(
                AbiCodec.rlpBytes(AbiCodec.fromHex(ENDPOINT_MANIFEST_COMMITMENT_SLOT)), storageNodesRlp));

        return AbiCodec.rlpList(
                BlockHeaderRlpCodec.encodeRlp(header).toByteArray(),
                accountProofRlp,
                manifestStorageProof,
                AbiCodec.rlpBytes(preimage));
    }

    /**
     * The protobuf encoding of {@code ClprEndpointManifest{version: 1, service_address, endpoints: []}}
     * — the manifest state a freshly initialised service holds.
     *
     * <p>Hand-encoded because it is three bytes of framing: field 1 (version) as a varint, then field 2
     * (service_address) as a 20-byte length-delimited value. An empty {@code endpoints} list emits
     * nothing at all.
     */
    private static byte[] manifestPreimage(final String serviceAddress) {
        final byte[] address = AbiCodec.fromHex(serviceAddress);
        if (address.length != 20) {
            throw new IllegalArgumentException("expected a 20-byte service address, got " + address.length);
        }
        return AbiCodec.concat(
                new byte[] {0x08, 0x01}, // field 1, varint, value 1
                new byte[] {0x12, 0x14}, // field 2, length-delimited, length 20
                address);
    }

    /** RLP-encode a list whose every element is a byte string. */
    private static byte[] rlpOfByteStrings(final List<byte[]> items) {
        final byte[][] encoded = new byte[items.size()][];
        for (int i = 0; i < items.size(); i++) {
            encoded[i] = AbiCodec.rlpBytes(items.get(i));
        }
        return AbiCodec.rlpList(encoded);
    }

    /** Left-pad to 32 bytes; a storage value comes back minimally encoded. */
    private static byte[] leftPad32(final byte[] value) {
        if (value.length == 32) {
            return value;
        }
        if (value.length > 32) {
            throw new IllegalArgumentException("storage value longer than 32 bytes: " + value.length);
        }
        final byte[] out = new byte[32];
        System.arraycopy(value, 0, out, 32 - value.length, value.length);
        return out;
    }
}
