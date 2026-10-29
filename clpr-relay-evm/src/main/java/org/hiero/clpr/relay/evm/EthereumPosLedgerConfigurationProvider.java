// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import static org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient.toBlockTag;

import com.hedera.hapi.node.state.clpr.ClprLedgerConfigurationResponse;
import com.hedera.hapi.node.state.clpr.EthereumPosLedgerConfigurationPayload;
import com.hedera.hapi.node.state.clpr.EthereumPosSyncCommittee;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.LedgerConfigurationPayloadProvider;
import org.hiero.clpr.relay.evm.beacon.BeaconApiClient;
import org.hiero.clpr.relay.evm.beacon.LightClientBootstrap;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;

/**
 * Ethereum-PoS implementation of {@link LedgerConfigurationPayloadProvider} — the sync-committee
 * analog of {@code SeiLedgerConfigurationProvider}.
 *
 * <p>Assembles the Ethereum-PoS ledger configuration payload by reading:
 *
 * <ol>
 *   <li>The current EVM block header at the requested commitment level — resolved once via
 *       {@code eth_getBlockByNumber} — and the on-chain {@code ClprLedgerConfiguration} read via
 *       {@link ContractStateReader} <b>at that exact block number</b>, not the symbolic tag, so the
 *       configuration is internally consistent (same race-avoidance as the QBFT/Sei providers).</li>
 *   <li>The current sync committee from the beacon node's finalized light-client bootstrap, plus the
 *       genesis validators root and the active fork version — the three components the peer's
 *       Ethereum-PoS verifier needs to seed its initial trust anchor
 *       ({@code RLP[syncCommittee, genesisValidatorsRoot, forkVersion]}).</li>
 * </ol>
 *
 * <p>The payload is thin (committee + chain-pinning fields + ledger configuration), mirroring
 * {@code ClprSeiLedgerConfigurationPayload}: the heavy execution-state proof material lives in the
 * bundle, not the configuration payload. The peer's verifier derives the trust anchor from these
 * fields rather than trusting a pre-computed anchor.
 */
public final class EthereumPosLedgerConfigurationProvider implements LedgerConfigurationPayloadProvider {

    private final EvmJsonRpcClient rpcClient;
    private final ContractStateReader stateReader;
    private final BeaconApiClient beaconClient;

    /**
     * @param rpcClient    the EVM JSON-RPC client used to resolve the commitment level to a concrete
     *                     block number and to read contract state
     * @param stateReader  the contract state reader used to read the ledger configuration
     * @param beaconClient the beacon-node client used to fetch the sync committee, genesis validators
     *                     root, and fork version
     */
    public EthereumPosLedgerConfigurationProvider(
            final EvmJsonRpcClient rpcClient,
            final ContractStateReader stateReader,
            final BeaconApiClient beaconClient) {
        this.rpcClient = rpcClient;
        this.stateReader = stateReader;
        this.beaconClient = beaconClient;
    }

    @Override
    public ClprLedgerConfigurationResponse provide(final CommitmentLevel commitmentLevel) {
        // Resolve the symbolic tag to a concrete block exactly once so the config read is consistent.
        final var currentHeader = rpcClient.ethGetBlockHeaderByNumber(commitmentLevel.toBlockTag());
        final String currentBlockTag = toBlockTag(currentHeader.number().longValueExact());
        final var ledgerConfig = stateReader.readLedgerConfiguration(currentBlockTag);

        // Trust-anchor seed material from the consensus layer: the current sync committee (from the
        // finalized light-client bootstrap) plus the chain-pinning genesis validators root and fork
        // version. The verifier composes RLP[syncCommittee, genesisValidatorsRoot, forkVersion].
        final LightClientBootstrap bootstrap = beaconClient.getBootstrap(beaconClient.getFinalizedBlockRootHex());
        final var committee = bootstrap.currentSyncCommittee();

        final var syncCommittee = EthereumPosSyncCommittee.newBuilder()
                .pubkeys(committee.pubkeys())
                .aggregatePubkey(committee.aggregatePubkey())
                .build();

        final var payload = EthereumPosLedgerConfigurationPayload.newBuilder()
                .syncCommittee(syncCommittee)
                .ledgerConfiguration(ledgerConfig)
                .genesisValidatorsRoot(beaconClient.getGenesisValidatorsRoot())
                .forkVersion(beaconClient.getCurrentForkVersion())
                // The slot the bootstrap committee is current for; its period (slot / 8192) pins the
                // peer's initial trust anchor and seeds its rotation tracking.
                .slot(bootstrap.header().beacon().slot())
                .build();

        return ClprLedgerConfigurationResponse.newBuilder().ethereumPos(payload).build();
    }
}
