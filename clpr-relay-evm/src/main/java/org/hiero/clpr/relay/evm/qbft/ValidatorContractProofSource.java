// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.qbft;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.List;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.model.EvmStorageMerkleProof;

/**
 * Reads and proves the QBFT validator set held by a validator smart contract (Besu's QBFT
 * "contract" validator-selection mode, e.g. the Consensys
 * <a href="https://github.com/Consensys/validator-smart-contracts">validator-smart-contracts</a>
 * reference implementation).
 *
 * <p>This is the single seam {@link QbftContractModeBundleConstructor} uses to read the validator
 * set from chain state — a network can wire its own implementation (in {@code ClprChannelHandler}'s
 * {@code QBFTContractMode} branch) to change how the array is found and read, but any implementation
 * must still produce
 * the {@link ContractProof} shape below; it is not a license to accept an arbitrary non-standard
 * storage layout on its own — the on-chain verifier only ever checks that one shape.
 *
 * @see StandardValidatorContractProofSource the default implementation, matching the standard
 *      Solidity {@code address[] public validators} storage layout
 */
public interface ValidatorContractProofSource {

    /**
     * A validator set, sorted ascending by address. Must not contain duplicates or the zero
     * address — the on-chain verifier rejects both ({@code DuplicateValidator}, {@code ZeroValidator}
     * in {@code QbftContractValidatorSource.sol}) rather than silently correcting them.
     */
    record ValidatorSet(List<Address> members) {}

    /**
     * An EIP-1186 proof of the validator contract's array, matching the 4-item RLP shape
     * {@code [validatorContract, accountProof, arraySlot, storageProof]} the on-chain
     * {@code QbftContractModeVerifier} expects (one storage-proof entry for the array's length slot,
     * plus one per element, in contract storage order — the verifier sorts the proven elements and
     * reverts if that reveals a duplicate or the zero address; it does not dedupe).
     */
    record ContractProof(
            Address validatorContract,
            List<Bytes> accountProof,
            Bytes arraySlot,
            List<EvmStorageMerkleProof> storageProof) {}

    /** A validator set together with the proof of its contents at the block it was read from. */
    record ProvenValidatorSet(ValidatorSet set, ContractProof proof) {}

    /**
     * Reads and proves the validator set at {@code blockNumber}.
     *
     * @param blockNumber the block whose state root the proof is constructed against
     * @return the validator set (sorted) and its EIP-1186 proof
     * @throws IllegalStateException if the array is empty, the proof response is malformed, or the
     *     array has more than 256 entries, a duplicate, or the zero address
     */
    ProvenValidatorSet proveValidatorSet(BigInteger blockNumber);
}
