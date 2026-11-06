// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.qbft;

import static org.hiero.clpr.relay.evm.ByteUtils.leftPad32;
import static org.hiero.clpr.relay.evm.EvmUtils.toSlotHex;
import static org.hiero.clpr.relay.evm.EvmUtils.wrapToUint256;
import static org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient.toBlockTag;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.ByteUtils;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.model.EvmStorageMerkleProof;

/**
 * The default {@link ValidatorContractProofSource}: reads a validator contract laid out the way
 * Solidity lays out any {@code address[] public validators} state variable — the array's length at
 * {@code arraySlot} and its elements at {@code keccak256(arraySlot) + i} — matching the storage
 * layout the Consensys
 * <a href="https://github.com/Consensys/validator-smart-contracts">validator-smart-contracts</a>
 * reference implementation uses, and what {@code QbftContractValidatorSource.sol} (the on-chain
 * counterpart, in {@code clpr-smart-contracts}) expects to be proven.
 *
 * <p>Reading the set costs two {@code eth_getProof} round trips: the first learns the array length
 * (the number of element slots to request is not known up front), the second is the authoritative
 * read — the length slot again plus every element slot, all under one account proof — so the
 * returned {@link ValidatorContractProofSource.ContractProof} is internally consistent (one account
 * proof, one storage-proof list covering every slot the verifier needs).
 */
public final class StandardValidatorContractProofSource implements ValidatorContractProofSource {

    private static final Comparator<Address> ADDRESS_ORDER = Comparator.comparing(Address::toHexString);

    /** Mirrors {@code ClprQbftQuorum.MAX_VALIDATORS} in clpr-smart-contracts: the verifier's structural cap. */
    private static final BigInteger MAX_VALIDATORS = BigInteger.valueOf(256);

    private final EvmJsonRpcClient rpcClient;
    private final Address validatorContract;
    private final BigInteger arraySlot;

    public StandardValidatorContractProofSource(
            final EvmJsonRpcClient rpcClient, final Address validatorContract, final BigInteger arraySlot) {
        this.rpcClient = rpcClient;
        this.validatorContract = validatorContract;
        this.arraySlot = arraySlot;
    }

    @Override
    public ProvenValidatorSet proveValidatorSet(final BigInteger blockNumber) {
        final String blockTag = toBlockTag(blockNumber.longValueExact());
        final String lengthSlotHex = toSlotHex(arraySlot);

        final var lengthProbe =
                rpcClient.ethGetProof(validatorContract.toHexString(), new String[] {lengthSlotHex}, blockTag);
        if (lengthProbe.storageProof().isEmpty()) {
            throw new IllegalStateException(
                    "Validator contract " + validatorContract + " has no length-slot proof at block " + blockNumber);
        }
        // A misconfigured arraySlotHex can point at any word — an address, a hash, a timestamp — so
        // this value must be range-checked as a BigInteger before it drives any allocation or array
        // index. Casting an out-of-range value to int first risks a negative array size (silent
        // wraparound) or an OutOfMemoryError (a huge but positive value): the latter is not an
        // Exception and would kill the channel's poller thread with no retry.
        final BigInteger lengthValue = new BigInteger(
                1, leftPad32(lengthProbe.storageProof().getFirst().value()).toByteArray());
        if (lengthValue.signum() == 0) {
            throw new IllegalStateException(
                    "Validator contract " + validatorContract + " array is empty at block " + blockNumber);
        }
        if (lengthValue.compareTo(MAX_VALIDATORS) > 0) {
            throw new IllegalStateException("Validator contract " + validatorContract + " array at block "
                    + blockNumber + " has length " + lengthValue
                    + ", exceeding the verifier's structural cap of " + MAX_VALIDATORS
                    + " (arraySlotHex is likely misconfigured — it should name the array's length slot)");
        }
        final int length = lengthValue.intValueExact();

        final BigInteger elementBase =
                new BigInteger(1, AbiCodec.Keccak256.keccak256(AbiCodec.encodeUint256(arraySlot)));
        final String[] elementSlotHexes = new String[length];
        for (int i = 0; i < length; i++) {
            elementSlotHexes[i] = toSlotHex(wrapToUint256(elementBase.add(BigInteger.valueOf(i))));
        }
        final String[] allSlotHexes = new String[elementSlotHexes.length + 1];
        allSlotHexes[0] = lengthSlotHex;
        System.arraycopy(elementSlotHexes, 0, allSlotHexes, 1, elementSlotHexes.length);

        final var proofResp = rpcClient.ethGetProof(validatorContract.toHexString(), allSlotHexes, blockTag);
        if (proofResp.accountProof().isEmpty()) {
            throw new IllegalStateException(
                    "Validator contract " + validatorContract + " account proof is empty at block " + blockNumber);
        }
        if (proofResp.storageProof().size() != allSlotHexes.length) {
            throw new IllegalStateException("Validator contract " + validatorContract + " storage proof at block "
                    + blockNumber + " is missing slots (expected " + allSlotHexes.length + ", got "
                    + proofResp.storageProof().size() + ")");
        }

        // Index by (left-padded) key so entries can be looked up regardless of the order the node
        // returned them in, then re-emit them in the requested (length, then element 0..N-1) order —
        // the canonical "contract storage order" the on-chain proof format expects.
        final Map<Bytes, ProofResponseEntry> byKey = new LinkedHashMap<>();
        for (final var entry : proofResp.storageProof()) {
            byKey.put(leftPad32(entry.key()), new ProofResponseEntry(entry.value(), entry.proof()));
        }

        final List<EvmStorageMerkleProof> storageProof = new ArrayList<>(allSlotHexes.length);
        for (final String slotHex : allSlotHexes) {
            final Bytes slotKey = leftPad32(ByteUtils.fromPrefixedHex(slotHex));
            final var found = byKey.get(slotKey);
            if (found == null || found.proof().isEmpty()) {
                throw new IllegalStateException("Validator contract " + validatorContract + " storage proof at block "
                        + blockNumber + " is missing slot " + slotHex);
            }
            storageProof.add(new EvmStorageMerkleProof(slotKey, found.proof()));
        }

        final List<Address> members = new ArrayList<>(length);
        for (final String elementSlotHex : elementSlotHexes) {
            final Bytes slotKey = leftPad32(ByteUtils.fromPrefixedHex(elementSlotHex));
            final var found = byKey.get(slotKey);
            final byte[] word = leftPad32(found.value()).toByteArray();
            final byte[] addressBytes = new byte[20];
            System.arraycopy(word, 12, addressBytes, 0, 20);
            members.add(new Address(addressBytes));
        }

        // Sorted, not deduped: the verifier itself sorts the proven elements and reverts if that
        // reveals a duplicate or the zero address (DuplicateValidator / ZeroValidator in
        // QbftContractValidatorSource.sol) rather than silently correcting either — so this source
        // must reject the same conditions instead of masking them.
        final List<Address> sorted = members.stream().sorted(ADDRESS_ORDER).toList();
        for (int i = 0; i < sorted.size(); i++) {
            final Address member = sorted.get(i);
            if (member.equals(Address.ZERO)) {
                throw new IllegalStateException("Validator contract " + validatorContract + " array at block "
                        + blockNumber + " contains the zero address");
            }
            if (i > 0 && member.equals(sorted.get(i - 1))) {
                throw new IllegalStateException("Validator contract " + validatorContract + " array at block "
                        + blockNumber + " contains a duplicate: " + member);
            }
        }

        final var proof = new ContractProof(
                validatorContract,
                proofResp.accountProof(),
                Bytes.wrap(AbiCodec.encodeUint256(arraySlot)),
                storageProof);
        return new ProvenValidatorSet(new ValidatorSet(sorted), proof);
    }

    private record ProofResponseEntry(Bytes value, List<Bytes> proof) {}
}
