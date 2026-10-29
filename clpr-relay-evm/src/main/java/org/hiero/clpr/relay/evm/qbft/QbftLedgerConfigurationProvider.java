// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.qbft;

import static org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient.toBlockTag;
import static org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout.CONFIG_SERVICE_ADDRESS_SLOT;

import com.hedera.hapi.node.state.clpr.BlockHeader;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfigurationResponse;
import com.hedera.hapi.node.state.clpr.QbftLedgerConfigurationPayload;
import com.hedera.hapi.node.state.clpr.StorageProofEntry;
import java.util.List;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.LedgerConfigurationPayloadProvider;
import org.hiero.clpr.relay.evm.BlockHeaderRlpCodec;
import org.hiero.clpr.relay.evm.EvmUtils;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.model.ProofResponse;

/**
 * EVM/Besu-QBFT implementation of {@link LedgerConfigurationPayloadProvider}.
 *
 * <p>Assembles the QBFT-specific ledger configuration payload by reading four pieces of EVM
 * state and binding them to a single block:
 *
 * <ol>
 *   <li>The current block header at the requested commitment level — resolved once via
 *       {@code eth_getBlockByNumber}. The header's {@code stateRoot} is the root against which
 *       the EIP-1186 proofs (step 4) verify.</li>
 *   <li>The on-chain {@code ClprLedgerConfiguration} read via {@link ContractStateReader} <b>at
 *       the exact block number from step 1</b>, not at the symbolic tag. This avoids a race
 *       where {@code "finalized"} (or any other symbolic tag) resolves to a different block on
 *       the second call and the configuration value ends up inconsistent with the state proof.</li>
 *   <li>The genesis block header (so the verifier can derive the initial QBFT validator set
 *       from {@code extra_data}).</li>
 *   <li>An EIP-1186 account proof for the ClprService contract plus a storage proof for the
 *       single slot backing {@code _config.serviceAddress}. This first version proves only the
 *       service address as a sanity check for the verifier's proof-format handling; remaining
 *       {@code ClprLedgerConfiguration} fields (protocolVersion, chainId, timestamp, throttles,
 *       endpoints) are a follow-up that extends the slot whitelist.</li>
 * </ol>
 *
 * <p>Why a separate provider rather than decorating {@link ContractStateReader}: internal
 * consumers (sync loop, the {@code protocolVersion} bootstrap check, state-change listener,
 * peer-roster reads) keep using the raw reader. They don't need the headers or the proof, and
 * a decorator would pay the {@code eth_getBlockByNumber} + {@code eth_getProof} round-trips on
 * every cycle. The provider is only invoked from the gRPC {@code getLedgerConfiguration}
 * pipeline.
 */
public final class QbftLedgerConfigurationProvider implements LedgerConfigurationPayloadProvider {
    private final EvmJsonRpcClient rpcClient;
    private final ContractStateReader stateReader;
    private final Address clprServiceAddress;
    private final long qbftEpochLength;

    /**
     * @param rpcClient          the JSON-RPC client used to fetch block headers and proofs
     * @param stateReader        the contract state reader used to read the ledger configuration
     * @param clprServiceAddress the ClprService contract address — the account the EIP-1186
     *                           proof is constructed against
     */
    public QbftLedgerConfigurationProvider(
            final EvmJsonRpcClient rpcClient,
            final ContractStateReader stateReader,
            final Address clprServiceAddress,
            final long qbftEpochLength) {
        this.rpcClient = rpcClient;
        this.stateReader = stateReader;
        this.clprServiceAddress = clprServiceAddress;
        this.qbftEpochLength = qbftEpochLength;
    }

    @Override
    public ClprLedgerConfigurationResponse provide(final CommitmentLevel commitmentLevel) {
        // Resolve the symbolic tag to a concrete block exactly once so every subsequent read
        // hits the same block — see class javadoc for why this matters.
        final var currentHeader = rpcClient.ethGetBlockHeaderByNumber(commitmentLevel.toBlockTag());
        final String currentBlockTag = toBlockTag(currentHeader.number().longValueExact());

        final var ledgerConfig = stateReader.readLedgerConfiguration(currentBlockTag);

        final var latestEpochBlockNumber =
                (currentHeader.number().longValueExact() / qbftEpochLength) * qbftEpochLength;
        final var latestEpochBlockHeader = rpcClient.ethGetBlockHeaderByNumber(toBlockTag(latestEpochBlockNumber));

        final String serviceAddressSlotHex = EvmUtils.toSlotHex(CONFIG_SERVICE_ADDRESS_SLOT);
        final ProofResponse proofResp = rpcClient.ethGetProof(
                clprServiceAddress.toHexString(), new String[] {serviceAddressSlotHex}, currentBlockTag);
        final List<StorageProofEntry> storageProofs = proofResp.storageProof().stream()
                .map(QbftLedgerConfigurationProvider::toProtoStorageProofEntry)
                .toList();

        final var payload = QbftLedgerConfigurationPayload.newBuilder()
                .latestEpochBlockHeader(toPayloadHeader(latestEpochBlockHeader))
                .ledgerConfiguration(ledgerConfig)
                .currentBlockHeader(toPayloadHeader(currentHeader))
                .clprServiceAccountProof(proofResp.accountProof())
                .clprServiceStorageProofs(storageProofs)
                .epochLength(qbftEpochLength)
                .build();
        return ClprLedgerConfigurationResponse.newBuilder().qbft(payload).build();
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
