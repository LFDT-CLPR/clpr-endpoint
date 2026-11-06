// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import com.swirlds.logging.api.Level;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;

/**
 * Decodes a JSON-RPC revert from a {@code ClprService} call into a classified {@link EvmError},
 * mapping the 4-byte selector ({@code keccak256("Name(types)")[:4]}) back to its Solidity signature.
 * Handles {@link JsonRpcException} only.
 *
 * <p>The table is scoped to errors the relay can actually receive — from {@code submitBundle} and
 * the read views ({@code getChannel}, {@code getMessage}, {@code getLedgerConfiguration},
 * {@code getPeerEndpointRoster}): the bundle-validation {@code Clpr*} errors and the Hiero/QBFT/Sei
 * verifier errors, plus {@code Error(string)}/{@code Panic(uint256)}. Errors from functions the
 * relay never calls (connector/channel/endpoint registration, messaging, admin) and the
 * Ethereum-beacon verifier are excluded; an excluded selector degrades to
 * {@code "unrecognized contract error"}. Two endpoint-manifest errors
 * ({@code ClprEndpointManifestFull()}, {@code ClprEndpointUnauthorized()}) are carried as a
 * deliberate exception to that exclusion — the relay does not trigger them, but they are kept in
 * the table so diagnostics stay aligned with the {@code ClprService} manifest surface.
 */
public final class EvmErrorParser {

    /** Selector for {@code ClprReplayDetected()}. */
    public static final String CLPR_REPLAY_DETECTED = "0x24315cae";

    /** Selector for {@code ClprRunningHashMismatch()}. */
    public static final String CLPR_RUNNING_HASH_MISMATCH = "0xcd21c020";

    /** Matches a 4-byte hex selector (0x + 8 hex chars) anywhere in a revert message. */
    private static final Pattern SELECTOR = Pattern.compile("0x[0-9a-fA-F]{8}");

    /** A recognised contract error's signature and its log level. */
    private record KnownError(String signature, Level level) {}

    private static KnownError warn(final String signature) {
        return new KnownError(signature, Level.WARN);
    }

    /** Selector (lower-case, 0x-prefixed) -> the recognised contract error it denotes. */
    private static final Map<String, KnownError> ERRORS_BY_SELECTOR = Map.ofEntries(
            Map.entry("0x01c942f8", warn("ClprInvalidConfiguration()")),
            Map.entry("0x028aed16", warn("LeftNeighbourNotRightMost()")),
            Map.entry("0x0296952a", warn("ValidatorSealNotFound(address)")),
            Map.entry("0x029a1002", warn("ClprChannelNotFound()")),
            Map.entry("0x034c6daf", warn("ClprModuleNotDeployed()")),
            Map.entry("0x05e28ace", warn("HieroHintsBadBlockRootLen(uint256)")),
            Map.entry("0x07c9047b", warn("StateProofMissingChannel()")),
            Map.entry("0x08c379a0", warn("Error(string)")),
            Map.entry("0x0cb309b4", warn("RightNeighbourNotLeftMost()")),
            Map.entry("0x10980f41", warn("InvalidStoreKey()")),
            Map.entry("0x10c598b8", warn("InvalidProofPayloadShape()")),
            Map.entry("0x11c9d58e", warn("HieroHintsBadVkLen(uint256)")),
            Map.entry("0x1208b21b", warn("EmptyValue()")),
            Map.entry("0x15202318", warn("ClprPayloadTooLarge()")),
            Map.entry("0x1554c064", warn("ClprHieroSignatureLength(uint256)")),
            Map.entry("0x15a8239e", warn("MerkleProofNoBaseHash()")),
            Map.entry("0x169c0eb3", warn("RightNeighbourNotAfterKey()")),
            Map.entry("0x1902c5a7", warn("MissingLedgerConfig()")),
            Map.entry("0x190c8e79", warn("SlotNotProven(bytes32)")),
            Map.entry("0x1e2cd89b", warn("WRAPSGammaAbcIndexOutOfRange()")),
            Map.entry("0x1ec0b2f7", warn("TooManyMessages()")),
            Map.entry("0x1f72b0a6", warn("HieroHintsFinalPairingFailed()")),
            Map.entry("0x1f906cc4", warn("InvalidServiceAddressLength()")),
            Map.entry(CLPR_REPLAY_DETECTED, warn("ClprReplayDetected()")),
            Map.entry("0x24b284f1", warn("InvalidPayloadShape()")),
            Map.entry("0x2be0c1ed", warn("WRAPSInvalidProofLength()")),
            Map.entry("0x2da90387", warn("SignersBitOutOfRange()")),
            Map.entry("0x2df17dae", warn("EmptyValidator()")),
            Map.entry("0x33570aaf", warn("RightNeighbourRootMismatch()")),
            Map.entry("0x339e1ffb", warn("EmptyValidatorSet()")),
            Map.entry("0x36b2ba36", warn("InvalidConfigHeader()")),
            Map.entry("0x3971511f", warn("UnsupportedProofType()")),
            Map.entry("0x3a4605a3", warn("HieroHintsBadN(uint64)")),
            Map.entry("0x3bebc35f", warn("ClprHieroBlockProofMissing()")),
            Map.entry("0x3bfb6a5e", warn("NextValidatorSetHashMismatch()")),
            Map.entry("0x3cd69fac", warn("EmptyKey()")),
            Map.entry("0x3f6069b6", warn("InvalidStorageEntry()")),
            Map.entry("0x40975687", warn("NonExistenceSlotNotEmpty()")),
            Map.entry("0x4223482b", warn("VerifyFailed(string)")),
            Map.entry("0x45c6978d", warn("InvalidEd25519KeyLength()")),
            Map.entry("0x48733073", warn("LeafPrefixMismatch()")),
            Map.entry("0x4a78b08f", warn("ClprHieroNoStateItemLeaf()")),
            Map.entry("0x4b21cde1", warn("WRAPSLedgerIdMismatch()")),
            Map.entry("0x4ba6766c", warn("HashOpNotSha256()")),
            Map.entry("0x4d4b7e1f", warn("HieroHintsBitmapNotBinary()")),
            Map.entry("0x4e487b71", warn("Panic(uint256)")),
            Map.entry("0x4e605b0b", warn("MerkleProofChainHasContent(uint256)")),
            Map.entry("0x4f0cb2bf", warn("LeafOpSpecMismatch()")),
            Map.entry("0x50a3f67a", warn("HieroHintsKzgShiftedFailed()")),
            Map.entry("0x53a49d08", warn("PrefixCollidesWithLeaf()")),
            Map.entry("0x54c4d9f8", warn("MPTInlineNodeUnsupported()")),
            Map.entry("0x558cc8c6", warn("MerkleProofStartIndexOutOfRange(uint256)")),
            Map.entry("0x564cdb7d", warn("LeavesNotNeighbours()")),
            Map.entry("0x56641863", warn("StorageKeyMismatch()")),
            Map.entry("0x5ade0455", warn("RootMismatch()")),
            Map.entry("0x5c427cd9", warn("UnauthorizedCaller()")),
            Map.entry("0x5d1dab4f", warn("ZeroLogicAddress()")),
            Map.entry("0x5d6431f6", warn("BundleTooLarge()")),
            Map.entry("0x662a9f47", warn("OnlyExistenceProofsSupported()")),
            Map.entry("0x69e87257", warn("NonExistenceMissingNeighbour()")),
            Map.entry("0x6a140048", warn("NonExistenceKeyMismatch()")),
            Map.entry("0x6d187b28", warn("InvalidAccount()")),
            Map.entry("0x6e21eb51", warn("InvalidNumberOfSealSignatures()")),
            Map.entry("0x6f2b0fcb", warn("MissingBundleContent()")),
            Map.entry("0x73d6c419", warn("MPTInvalidHashRef()")),
            Map.entry("0x7411a7a0", warn("WRAPSCmENotInfinity()")),
            Map.entry("0x75ca989e", warn("ZeroEd25519Verifier()")),
            Map.entry("0x768d7529", warn("MerkleProofChainOutOfRange(uint256)")),
            Map.entry("0x7ab70596", warn("InvalidExtraData()")),
            Map.entry("0x7d92d3da", warn("MissingStateProof()")),
            Map.entry("0x7dff7ad5", warn("ExtraSignatures()")),
            Map.entry("0x8100442f", warn("HieroHintsBelowThreshold()")),
            Map.entry("0x838ff676", warn("ClprHieroPrecompileFailure(address)")),
            Map.entry("0x839e2ccd", warn("KeyMismatch()")),
            Map.entry("0x8b1075b9", warn("NoProgress()")),
            Map.entry("0x8baa579f", warn("InvalidSignature()")),
            Map.entry("0x8bfbcc84", warn("SealLengthMismatch(uint256)")),
            Map.entry("0x8c29e0ad", warn("LeftNeighbourNotBeforeKey()")),
            Map.entry("0x8d4654b1", warn("InvalidProposerAddressLength()")),
            Map.entry("0x8e400bc6", warn("MPTKeyAbsent()")),
            Map.entry("0x8f3b1e1a", warn("MPTProofEndedEarly()")),
            Map.entry("0x90804f1d", warn("MissingNextValidatorSet()")),
            Map.entry("0x90c0b375", warn("InvalidPrefixLength()")),
            Map.entry("0x9333c519", warn("WRAPSPoseidonMismatch()")),
            Map.entry("0x93c44ee6", warn("CodeHashMismatch()")),
            Map.entry("0x96e5d6cd", warn("HieroHintsBSkIdentityFailed()")),
            Map.entry("0x9a9151e3", warn("InvalidStorageProofShape()")),
            Map.entry("0x9b3e4a9e", warn("MissingValidatorSet()")),
            Map.entry("0x9d3c8752", warn("Load32OutOfBounds()")),
            Map.entry("0x9d4bef2d", warn("InvalidStorageProofCount()")),
            Map.entry("0x9ddb7335", warn("HieroHintsDegreeBoundFailed()")),
            Map.entry("0xa052cfb4", warn("InvalidSuffixLength()")),
            Map.entry("0xa179f8c9", warn("ChainIdMismatch()")),
            Map.entry("0xa495ce62", warn("ClprBundleVerificationFailed()")),
            Map.entry("0xa4e48c3a", warn("MerkleProofChainCycle()")),
            Map.entry("0xa6e84187", warn("ClprBundleRateLimitExceeded()")),
            Map.entry("0xaa24a871", warn("ClprHieroTssVerificationFailed()")),
            Map.entry("0xadc6e799", warn("ClprInvalidChannelStatus()")),
            Map.entry("0xadd76648", warn("WRAPSPairingFailed()")),
            Map.entry("0xae5f8b75", warn("ClprEndpointNotRegistered()")),
            Map.entry("0xaf2cbcf0", warn("EpochMismatch()")),
            Map.entry("0xb098fbc6", warn("ClprDisabled()")),
            Map.entry("0xb1400534", warn("ClprBundleVerificationFailedOneOfVariant()")),
            Map.entry("0xb4506e54", warn("HieroHintsBadSigLen(uint256)")),
            Map.entry("0xb45c9de5", warn("ChainIdTooLong()")),
            Map.entry("0xb60323c7", warn("ClprEndpointManifestFull()")),
            Map.entry("0xb6084610", warn("InvalidStorageValueLength()")),
            Map.entry("0xb83ef9c6", warn("ClprHieroEmptyLedgerId()")),
            Map.entry("0xba5509dc", warn("ClprAckVerificationFailed()")),
            Map.entry("0xbabb01dd", warn("InvalidHeader()")),
            Map.entry("0xbd6c4f90", warn("BN254G2SqrtFailed()")),
            Map.entry("0xbdb86313", warn("EmptyGenesisValidatorSet()")),
            Map.entry("0xc165a74c", warn("ClprEndpointUnauthorized()")),
            Map.entry("0xc35c96d0", warn("InvalidTrustAnchorFields()")),
            Map.entry("0xc4b52691", warn("HieroHintsKzgMergedFailed()")),
            Map.entry("0xc66cb05b", warn("InvalidTrustAnchor()")),
            Map.entry("0xc680a4b2", warn("LeftNeighbourRootMismatch()")),
            Map.entry("0xc993dd80", warn("TooFewSignatures()")),
            Map.entry("0xcbac88a4", warn("MPTEmptyProof()")),
            Map.entry(CLPR_RUNNING_HASH_MISMATCH, warn("ClprRunningHashMismatch()")),
            Map.entry("0xcea7fa17", warn("MultiValidatorNotSupported()")),
            Map.entry("0xd3afa303", warn("ClprUnknownWireField(uint64)")),
            Map.entry("0xd5ff852f", warn("WrongSealCount(uint256)")),
            Map.entry("0xd79e824a", warn("QuorumNotMet()")),
            Map.entry("0xdc7de087", warn("ClprPeerServiceAddressMismatch()")),
            Map.entry("0xdd8e4af7", warn("ValueMismatch()")),
            Map.entry("0xe03c4397", warn("ClprBundleVerificationFailedEmptyPayload()")),
            Map.entry("0xe15c8c44", warn("InvalidConfigThrottleFieldCount()")),
            Map.entry("0xe570050c", warn("ClprHieroWrapsProofRequired()")),
            Map.entry("0xe82e3caa", warn("StateProofPathInvalid()")),
            Map.entry("0xe94aa279", warn("ServiceAddressSlotMismatch()")),
            Map.entry("0xe965e169", warn("ClprEndpointNotInChannelRoster()")),
            Map.entry("0xed93b10b", warn("ValidatorSetHashMismatch()")),
            Map.entry("0xf07b9780", warn("InvalidSignersBitsLength()")),
            Map.entry("0xf2817348", warn("SealRecoverFailed()")),
            Map.entry("0xf3816b0f", warn("StorageProofFailed()")),
            Map.entry("0xf94a1a87", warn("MPTInvalidPathPrefix()")),
            Map.entry("0xfa314034", warn("WRAPSSliceOutOfBounds()")),
            Map.entry("0xfa8cc849", warn("ClprHieroBlockHashLength(uint256)")),
            Map.entry("0xfb8c7da0", warn("HieroHintsParsumBadRecurrence()")),
            Map.entry("0xfc6594b2", warn("MPTInvalidNode()")),
            Map.entry("0xfedc7165", warn("MPTKeyMismatch()")));

    private EvmErrorParser() {}

    /**
     * Decode a JSON-RPC revert into a classified {@link EvmError}.
     *
     * @param error the reverting call's exception, or {@code null} if the reason was not captured
     * @return the decoded error; never {@code null}
     */
    public static EvmError parse(final @Nullable JsonRpcException error) {
        final String message = error == null ? null : error.getMessage();
        final String selector = extractSelector(message);
        final KnownError known = selector == null ? null : ERRORS_BY_SELECTOR.get(selector);
        final String signature = known == null ? null : known.signature();
        final Level level = known == null ? Level.WARN : known.level();
        return new EvmError(selector, signature, level, message);
    }

    /**
     * Resolve a bare 4-byte selector to its Solidity error signature.
     *
     * @param selector a {@code 0x}-prefixed 8-hex-char selector, case-insensitive, or {@code null}
     * @return the signature (e.g. {@code "ClprRunningHashMismatch()"}), or {@code null} if unknown,
     *     malformed, or {@code null}
     */
    public static @Nullable String errorName(final @Nullable String selector) {
        if (selector == null) {
            return null;
        }
        final KnownError known = ERRORS_BY_SELECTOR.get(selector.trim().toLowerCase(Locale.ROOT));
        return known == null ? null : known.signature();
    }

    /**
     * Extract the first 4-byte selector from revert text (e.g. {@code data="0xcd21c020"}).
     *
     * @param revertText the revert message or raw data hex, or {@code null}
     * @return the {@code 0x}-prefixed lower-case selector, or {@code null} if none is present
     */
    public static @Nullable String extractSelector(final @Nullable String revertText) {
        if (revertText == null) {
            return null;
        }
        final Matcher m = SELECTOR.matcher(revertText);
        return m.find() ? m.group().toLowerCase(Locale.ROOT) : null;
    }
}
