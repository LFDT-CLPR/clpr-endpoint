// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.qbft;

import static org.hiero.clpr.relay.evm.EvmUtils.toSlotHex;
import static org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient.toBlockTag;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.CONFIG_SERVICE_ADDRESS_SLOT;

import com.hedera.hapi.node.state.clpr.BlockHeader;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfigurationResponse;
import com.hedera.hapi.node.state.clpr.QbftContractModeValidatorSource;
import com.hedera.hapi.node.state.clpr.QbftLedgerConfigurationPayload;
import com.hedera.hapi.node.state.clpr.StorageProofEntry;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.LedgerConfigurationPayloadProvider;
import org.hiero.clpr.relay.evm.BlockHeaderRlpCodec;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.model.EvmStorageMerkleProof;
import org.hiero.clpr.relay.evm.model.ProofResponse;

/**
 * EVM/Besu-QBFT "contract" validator-selection mode implementation of
 * {@link LedgerConfigurationPayloadProvider} — the counterpart to
 * {@link QbftLedgerConfigurationProvider} for a network whose validator set is held by a
 * validator smart contract rather than encoded in each block header's {@code extra_data}.
 *
 * <p>Assembles the same platform-neutral {@code ledger_configuration} and {@code ClprService}
 * account/storage proof {@link QbftLedgerConfigurationProvider} does, pinned to the same block,
 * but omits {@code latest_epoch_block_header} (meaningless in contract mode) and instead attaches a
 * {@link QbftContractModeValidatorSource}: a proof of the validator contract's {@code address[]}
 * array under that same block's state root, plus the contract's address and array slot (not
 * otherwise derivable by the peer). A calling peer uses this to build its own config proof's
 * contract-mode descriptor and validator-contract-proof fields — see {@code clpr-smart-contracts}'s
 * {@code src/verifiers/evm/qbft/qbft-contract-mode.md}.
 */
public final class QbftContractModeLedgerConfigurationProvider implements LedgerConfigurationPayloadProvider {
    private final EvmJsonRpcClient rpcClient;
    private final ContractStateReader stateReader;
    private final Address clprServiceAddress;
    private final ValidatorContractProofSource validatorSource;

    /**
     * @param rpcClient          the JSON-RPC client used to fetch block headers and proofs
     * @param stateReader        the contract state reader used to read the ledger configuration
     * @param clprServiceAddress the ClprService contract address — the account the EIP-1186
     *                           proof is constructed against
     * @param validatorSource    reads and proves the validator contract's array
     */
    public QbftContractModeLedgerConfigurationProvider(
            final EvmJsonRpcClient rpcClient,
            final ContractStateReader stateReader,
            final Address clprServiceAddress,
            final ValidatorContractProofSource validatorSource) {
        this.rpcClient = rpcClient;
        this.stateReader = stateReader;
        this.clprServiceAddress = clprServiceAddress;
        this.validatorSource = validatorSource;
    }

    @Override
    public ClprLedgerConfigurationResponse provide(final CommitmentLevel commitmentLevel) {
        // Resolve the symbolic tag to a concrete block exactly once so every subsequent read
        // (ledger configuration, ClprService proof, validator-contract proof) hits the same block.
        final var currentHeader = rpcClient.ethGetBlockHeaderByNumber(commitmentLevel.toBlockTag());
        final String currentBlockTag = toBlockTag(currentHeader.number().longValueExact());

        final var ledgerConfig = stateReader.readLedgerConfiguration(currentBlockTag);

        final String serviceAddressSlotHex = toSlotHex(CONFIG_SERVICE_ADDRESS_SLOT);
        final ProofResponse proofResp = rpcClient.ethGetProof(
                clprServiceAddress.toHexString(), new String[] {serviceAddressSlotHex}, currentBlockTag);
        final List<StorageProofEntry> storageProofs = proofResp.storageProof().stream()
                .map(QbftContractModeLedgerConfigurationProvider::toProtoStorageProofEntry)
                .toList();

        final var provenValidatorSet = validatorSource.proveValidatorSet(currentHeader.number());
        final var contractProof = provenValidatorSet.proof();
        final var validatorSourcePayload = QbftContractModeValidatorSource.newBuilder()
                .validatorContractAddress(
                        Bytes.wrap(contractProof.validatorContract().value()))
                .arraySlot(contractProof.arraySlot())
                .validatorContractAccountProof(contractProof.accountProof())
                .validatorContractStorageProofs(contractProof.storageProof().stream()
                        .map(QbftContractModeLedgerConfigurationProvider::toProtoStorageProofEntry)
                        .toList())
                .build();

        final var payload = QbftLedgerConfigurationPayload.newBuilder()
                .ledgerConfiguration(ledgerConfig)
                .currentBlockHeader(toPayloadHeader(currentHeader))
                .clprServiceAccountProof(proofResp.accountProof())
                .clprServiceStorageProofs(storageProofs)
                .contractModeValidatorSource(validatorSourcePayload)
                .build();
        return ClprLedgerConfigurationResponse.newBuilder().qbft(payload).build();
    }

    private static StorageProofEntry toProtoStorageProofEntry(final EvmStorageMerkleProof e) {
        return StorageProofEntry.newBuilder().key(e.key()).proof(e.proof()).build();
    }

    private static StorageProofEntry toProtoStorageProofEntry(final ProofResponse.StorageProofEntry e) {
        return StorageProofEntry.newBuilder()
                .key(e.key())
                .value(e.value())
                .proof(e.proof())
                .build();
    }

    private static BlockHeader toPayloadHeader(final org.hiero.clpr.relay.evm.model.BlockHeader parsed) {
        return BlockHeader.newBuilder()
                .rlp(BlockHeaderRlpCodec.encodeRlp(parsed))
                .build();
    }
}
