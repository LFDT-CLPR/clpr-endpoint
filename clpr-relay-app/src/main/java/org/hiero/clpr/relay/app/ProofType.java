// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.app;

/**
 * Proof format discriminator for a local network or peer channel.
 *
 * <p>The relay's core abstractions ({@code ContractStateReader}, {@code InboundBundleQueue},
 * {@code BundleConstructor}, {@code BundlePayloadCodec}) are chain-agnostic. This enum selects which
 * concrete implementation bundle to build in {@code ClprChannelHandler.create()} and which
 * {@code getLedgerConfiguration} payload type to serve.
 *
 * <p>New proof types are added by: (1) adding an enum constant here, (2) adding a corresponding
 * network-params record to {@link RelayConfig}, (3) implementing
 * {@link org.hiero.clpr.relay.core.LedgerConfigurationPayloadProvider}, (4) wiring a new
 * {@code BundleConstructor} + {@code LedgerConfigurationPayloadProvider} case in the proof-type
 * switch in {@code ClprChannelHandler.create()}, and (5) wiring a new {@code BundlePayloadCodec}
 * case in {@code ClprChannelHandler.codecFor()}.
 */
public enum ProofType {
    /**
     * EVM chain using QBFT consensus (e.g. Hyperledger Besu) in the default header-based
     * validator-selection mode. {@code getLedgerConfiguration} returns a
     * {@code QbftLedgerConfigurationPayload} carrying the epoch-boundary block header whose
     * {@code extra_data} encodes the validator set.
     */
    QBFT,
    /**
     * EVM chain using QBFT consensus (e.g. Hyperledger Besu) in Besu's "contract"
     * validator-selection mode: the validator set is held by a validator smart contract (e.g. the
     * Consensys <a href="https://github.com/Consensys/validator-smart-contracts">validator-smart-contracts</a>
     * reference implementation) rather than encoded in each block header's {@code extra_data}.
     * Distinct from {@link #QBFT} because the two modes are structurally different "networks" that
     * just happen to both be implemented by Besu — a channel keeps one mode for its full life, and
     * there is no in-place migration between them. {@code getLedgerConfiguration} still returns a
     * {@code QbftLedgerConfigurationPayload}, with its {@code contract_mode_validator_source} field
     * set instead of the header-derived validator set.
     */
    QBFTContractMode,
    CometBFT,
    Hiero,

    /**
     * Ethereum mainnet (post-Merge, proof-of-stake) and L2s anchoring to it.
     * {@code getLedgerConfiguration} returns an {@code EthereumPosLedgerConfigurationPayload} carrying
     * the sync-committee light-client material; bundles are a nine-item RLP verified by the
     * Hiero-native {@code EthereumSyncCommitteeProofVerifier} against a sync-committee BLS finality
     * proof.
     */
    EthereumPOS,

    /**
     * No-crypto stub format {@code "CLPRSTUB" || protobuf(ClprBundleContent)} used only for
     * integration testing against the {@code StubClprVerifier} contract — never a real production
     * chain. Valid on both sides of a test channel:
     *
     * <ul>
     *   <li>As a channel's {@code peerProofType}: the relay parses inbound bundles with
     *       {@code StubProofCodec} and submits them verbatim to the stub verifier. A simulated stub
     *       peer supplies hand-crafted bundles this way.</li>
     *   <li>As a <em>local network's</em> own proof type: {@code StubBundleConstructor} builds
     *       CLPRSTUB bundles from real on-chain channel state, letting two real relays exchange
     *       messages end-to-end over the stub verifier without a real consensus proof.</li>
     * </ul>
     */
    Stub
}
