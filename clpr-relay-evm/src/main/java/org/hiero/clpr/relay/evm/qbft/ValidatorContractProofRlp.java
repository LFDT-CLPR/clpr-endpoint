// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.qbft;

import static org.hiero.clpr.relay.evm.AbiCodec.rlpBytes;
import static org.hiero.clpr.relay.evm.AbiCodec.rlpList;

import org.hiero.clpr.relay.evm.model.EvmStorageMerkleProof;
import org.hiero.clpr.relay.evm.qbft.ValidatorContractProofSource.ContractProof;

/**
 * RLP encoding shared by every QBFT "contract" validator-selection mode proof that embeds a
 * {@link ContractProof}: the bundle proof's current/hop contract proofs
 * ({@link QbftContractModeBundleConstructor}) and a channel's config proof (built by E2E/ops
 * tooling against {@code QbftContractModeVerifier.verifyConfig}).
 *
 * <p>Matches {@code QbftContractValidatorSource.sol}'s expected wire shape exactly: the 4-item
 * list {@code [validatorContract, accountProof, arraySlot, storageProof]}, itself wrapped as an
 * RLP byte string — the verifier strips that envelope in place ({@code RlpCursor.payloadSlice})
 * before reading the list.
 */
public final class ValidatorContractProofRlp {

    private ValidatorContractProofRlp() {}

    /** {@code rlpBytes(rlpList([validatorContract, accountProof, arraySlot, storageProof]))}. */
    public static byte[] encodeWrapped(final ContractProof proof) {
        final byte[] validatorContractElem = rlpBytes(proof.validatorContract().value());
        final byte[] accountProofElem = rlpList(proof.accountProof().stream()
                .map(b -> rlpBytes(b.toByteArray()))
                .toArray(byte[][]::new));
        final byte[] arraySlotElem = rlpBytes(proof.arraySlot().toByteArray());
        final byte[] storageProofElem = rlpList(proof.storageProof().stream()
                .map(EvmStorageMerkleProof::encodeRlp)
                .toArray(byte[][]::new));
        return rlpBytes(rlpList(validatorContractElem, accountProofElem, arraySlotElem, storageProofElem));
    }
}
