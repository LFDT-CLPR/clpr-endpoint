// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.app;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Positive;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Top-level configuration record for the CLPR EVM Relay application.
 *
 * <p>Scalar blocks ({@code grpc.*}, {@code backoff.*}) are loaded via the swirlds-config framework.
 * The {@code localNetworks}, {@code clprServices}, and {@code peerProofTypes} sections are parsed
 * from the YAML file by {@code RelayConfigLoader}, because list/map-of-objects shapes cannot be
 * represented by the property-based config model.
 *
 * @param grpc                gRPC server settings (port, max message size)
 * @param localNetworks       named local network entries; each entry declares a proof type and the
 *                            network connection parameters for a chain the relay submits to
 * @param clprServices        {@code ClprService} contract deployments; each references a local
 *                            network, carries its signing key(s), and lists predefined channels
 *                            and/or enables on-chain channel discovery
 * @param backoff             per-channel loop failure-backoff configuration
 * @param peerProofTypes      map of peer CAIP-2 {@code chainId} (e.g. {@code "eip155:1337"},
 *                            {@code "hiero:mainnet"}) to that peer's {@link ProofType}; used to
 *                            resolve a channel's peer proof type. When the config provides none,
 *                            {@code RelayConfigLoader} substitutes {@link #DEFAULT_PEER_PROOF_TYPES}
 */
public record RelayConfig(
        GrpcConfig grpc,
        List<LocalNetworkConfig> localNetworks,
        List<ClprServiceConfig> clprServices,
        BackoffConfig backoff,
        Map<String, ProofType> peerProofTypes) {

    /**
     * Known peer CAIP-2 {@code chainId} → {@link ProofType} mappings, used by {@code RelayConfigLoader}
     * as the fallback when the config supplies no {@code peerProofTypes}. Covers the public networks
     * plus the local/dev chains the test harness uses, so standard deployments need no
     * explicit mapping. Note that a Sei peer's CLPR identity is its <b>CometBFT chain-id</b>
     * (e.g. {@code pacific-1}, {@code sei-local}), not {@code eip155:<evmChainId>}.
     */
    public static final Map<String, ProofType> DEFAULT_PEER_PROOF_TYPES = Map.ofEntries(
            // EVM / QBFT chains (CAIP-2 eip155:<chainId>).
            Map.entry("eip155:1337", ProofType.QBFT), // Anvil / Besu dev + integration tests
            Map.entry("eip155:31337", ProofType.QBFT), // Besu devnet (besu1)
            Map.entry("eip155:31338", ProofType.QBFT), // Besu devnet (besu2)
            // Hiero networks (CAIP-2 hiero:<network>).
            Map.entry("hiero:mainnet", ProofType.Hiero),
            Map.entry("hiero:testnet", ProofType.Hiero),
            Map.entry("hiero:previewnet", ProofType.Hiero),
            Map.entry("hiero:localnet", ProofType.Hiero),
            // Sei (CometBFT) — keyed by the CometBFT chain-id, not the EVM eip155 id.
            Map.entry("pacific-1", ProofType.CometBFT), // Sei mainnet
            Map.entry("atlantic-2", ProofType.CometBFT), // Sei testnet
            Map.entry("arctic-1", ProofType.CometBFT), // Sei devnet
            Map.entry("sei-local", ProofType.CometBFT), // local single-node Sei devnet
            Map.entry("sei-local-b", ProofType.CometBFT)); // second local Sei devnet

    /**
     * A single {@code ClprService} contract deployment the relay operates against.
     *
     * @param defaultSigningPrivateKeyHex        default hex-encoded ECDSA secp256k1 signing key used
     *                                           for every channel on this service that has no
     *                                           per-channel override; required when the service
     *                                           discovers channels or has predefined channels
     *                                           without their own key
     * @param localNetwork                       id of the {@link LocalNetworkConfig} this service is
     *                                           deployed on
     * @param serviceAddress                     hex address of the deployed {@code ClprService}
     *                                           contract
     * @param discoverChannels                when {@code true}, poll the contract for channels
     *                                           that appear on-chain and register them at runtime
     * @param discoveryStartBlock                block number from which channel discovery begins
     *                                           its {@code ChannelCompleted} log scan; set to the
     *                                           contract deployment block to avoid rescanning a large
     *                                           chain from genesis
     * @param predefinedChannels              hex-encoded channel ids registered at startup
     *                                           (used when discovery is disabled, or alongside it)
     * @param perChannelSigningPrivateKeyHex  per-channel signing-key overrides keyed by
     *                                           channel id (predefined or discovered later)
     */
    public record ClprServiceConfig(
            String defaultSigningPrivateKeyHex,
            String localNetwork,
            String serviceAddress,
            boolean discoverChannels,
            long discoveryStartBlock,
            List<String> predefinedChannels,
            Map<String, String> perChannelSigningPrivateKeyHex) {}

    /**
     * A named local network: a chain the relay connects to locally.
     *
     * <p>The {@code proofType} selects the correct {@code BundlePayloadCodec} for inbound sync payloads
     * and the {@code getLedgerConfiguration} payload type. The typed sub-blocks ({@code evm},
     * {@code cometBft}, {@code qbft}, {@code qbftContractMode}, {@code beacon}) carry the network
     * connection and proof parameters required to make RPC calls and build bundles.
     *
     * @param id               unique identifier for this network (referenced by
     *                         {@link ClprServiceConfig})
     * @param proofType        proof format discriminator used to build the implementation bundle and
     *                         to select the {@code BundlePayloadCodec} for inbound bundles
     * @param evm              EVM network connection parameters; non-null when {@code proofType} is
     *                         {@link ProofType#QBFT}, {@link ProofType#QBFTContractMode},
     *                         {@link ProofType#CometBFT}, or {@link ProofType#EthereumPOS}
     * @param cometBft         CometBFT proof parameters; non-null when {@code proofType} is
     *                         {@link ProofType#CometBFT}, otherwise {@code null}
     * @param qbft             QBFT header-mode proof parameters; non-null when {@code proofType} is
     *                         {@link ProofType#QBFT}, otherwise {@code null}
     * @param beacon           Ethereum-PoS beacon (consensus-layer) proof parameters; non-null when
     *                         {@code proofType} is {@link ProofType#EthereumPOS}, otherwise
     *                         {@code null}
     * @param qbftContractMode QBFT contract-mode proof parameters; non-null when {@code proofType} is
     *                         {@link ProofType#QBFTContractMode}, otherwise {@code null}
     */
    public record LocalNetworkConfig(
            String id,
            ProofType proofType,
            CommonEvmParams evm,
            @Nullable CometBftConfig cometBft,
            @Nullable QbftConfig qbft,
            @Nullable BeaconConfig beacon,
            @Nullable QbftContractModeConfig qbftContractMode) {}

    /**
     * Common EVM network connection parameters. Present on {@link LocalNetworkConfig} entries whose
     * {@code proofType} is {@link ProofType#QBFT}, {@link ProofType#QBFTContractMode}, or
     * {@link ProofType#CometBFT} — all run an EVM JSON-RPC and the {@code ClprService} contract.
     * Proof-format-specific parameters live in the separate {@link QbftConfig},
     * {@link QbftContractModeConfig}, and {@link CometBftConfig} blocks.
     *
     * @param jsonRpcUrl          HTTP URL of the EVM JSON-RPC endpoint
     *                            (default: {@code http://localhost:8545})
     * @param chainId             EIP-155 chain identifier (default: {@code 1} — Ethereum mainnet)
     * @param maxGasPriceCap      hard cap on {@code maxFeePerGas} in wei; the default
     *                            {@link Long#MAX_VALUE} imposes no cap
     * @param gasPriorityFee      EIP-1559 priority fee in wei (default: {@code 2000000000} = 2 gwei)
     * @param gasBufferMultiplier multiplier applied to the current {@code baseFeePerGas} when
     *                            computing {@code maxFeePerGas} (default: {@code 1.2})
     * @param pollIntervalMs      interval in milliseconds between successive on-chain state polls by
     *                            the listener (default: {@code 1000} = 1 s)
     * @param requestTimeoutMs    per-request JSON-RPC response timeout in milliseconds; on expiry the
     *                            request is treated as a transient error and retried up to
     *                            {@code maxRpcRetries} times (default: {@code 30000} = 30 s)
     * @param maxRpcRetries       maximum JSON-RPC retry attempts on transient errors (channel
     *                            refused, timeout, HTTP 429/503); total attempts is one more than
     *                            this (default: {@code 3})
     */
    public record CommonEvmParams(
            String jsonRpcUrl,
            long chainId,
            long maxGasPriceCap,
            long gasPriorityFee,
            double gasBufferMultiplier,
            long pollIntervalMs,
            long requestTimeoutMs,
            int maxRpcRetries) {}

    /**
     * QBFT header-mode proof parameters. Present on {@link LocalNetworkConfig} entries whose
     * {@code proofType} is {@link ProofType#QBFT}. These drive the epoch-boundary computation for the
     * {@code getLedgerConfiguration} payload and bound bundle size.
     *
     * @param epochLength                   QBFT epoch length used to compute the epoch-boundary block
     *                                      for the getLedgerConfiguration payload (default:
     *                                      {@code 30000})
     * @param maxEpochBlockHeadersPerBundle maximum number of epoch block headers to include in a
     *                                      single bundle when the remote trust anchor is behind
     *                                      (default: {@code 5})
     * @param maxMessagesPerBundle          maximum number of messages to include in a single bundle
     *                                      (default: {@code 10})
     * @param maxBundleSize                 maximum assembled bundle-proof size, in bytes; a bundle
     *                                      exceeding it is not emitted (defence-in-depth against a
     *                                      gas/size blowup on the peer's {@code submitBundle}), on top
     *                                      of {@code maxMessagesPerBundle} (default: {@code 1048576})
     */
    public record QbftConfig(
            long epochLength, int maxEpochBlockHeadersPerBundle, int maxMessagesPerBundle, long maxBundleSize) {

        public QbftConfig {
            if (maxBundleSize < 1) {
                throw new IllegalArgumentException("qbft.maxBundleSize must be at least 1 byte");
            }
        }
    }

    /**
     * QBFT "contract" validator-selection mode proof parameters. Present on
     * {@link LocalNetworkConfig} entries whose {@code proofType} is
     * {@link ProofType#QBFTContractMode}: the network's validator set is held by a validator smart
     * contract (e.g. the Consensys
     * <a href="https://github.com/Consensys/validator-smart-contracts">validator-smart-contracts</a>
     * reference implementation) rather than encoded in every block header. Wired to a
     * {@code ValidatorContractProofSource} implementation in {@code ClprChannelHandler}'s
     * {@code QBFTContractMode} branch — a customized network can supply its own implementation there
     * without touching this config shape, as long as it produces the {@code ContractProof} form the
     * on-chain verifier checks.
     *
     * <p>{@code arraySlotHex} depends on the deployed validator contract's storage layout — the
     * default {@code StandardValidatorContractProofSource} assumes a plain Solidity
     * {@code address[]} state variable (length at the slot, elements at
     * {@code keccak256(slot) + i}); read the deployed contract's storage layout to find the slot for
     * a customized implementation.
     *
     * @param validatorContractAddress hex address of the deployed validator contract
     * @param arraySlotHex             hex-encoded storage slot of the validator contract's
     *                                 {@code address[]} array (32 bytes, {@code 0x}-prefixed or not)
     * @param maxMessagesPerBundle     maximum number of messages to include in a single bundle
     *                                 (default: {@code 10})
     */
    public record QbftContractModeConfig(
            String validatorContractAddress, String arraySlotHex, int maxMessagesPerBundle) {

        /**
         * Parses {@link #arraySlotHex} into the storage slot it names. The single parse point for
         * this value — both {@code RelayConfigLoader} (validation) and {@code ClprChannelHandler}
         * (wiring) call this rather than parsing the hex string themselves.
         *
         * @return the parsed slot
         * @throws IllegalArgumentException if the value is not valid hex, is negative, or exceeds 256 bits
         */
        public BigInteger arraySlot() {
            final String stripped = arraySlotHex.startsWith("0x") || arraySlotHex.startsWith("0X")
                    ? arraySlotHex.substring(2)
                    : arraySlotHex;
            final BigInteger slot = new BigInteger(stripped, 16);
            if (slot.signum() < 0 || slot.bitLength() > 256) {
                throw new IllegalArgumentException(
                        "arraySlotHex '" + arraySlotHex + "' must be a non-negative value not exceeding 256 bits");
            }
            return slot;
        }
    }

    /**
     * CometBFT proof parameters. Present on {@link LocalNetworkConfig} entries whose
     * {@code proofType} is {@link ProofType#CometBFT}. The EVM side of a CometBFT network (contract
     * reads + {@code submitBundle} transactions) is configured via {@link CommonEvmParams}; these
     * fields configure the CometBFT proof path.
     *
     * @param cometBftRpcUrl       HTTP URL of the CometBFT RPC endpoint used to fetch signed
     *                             headers, validator sets, and ICS-23 ABCI proofs (default:
     *                             {@code http://localhost:26657}, CometBFT's default RPC port)
     * @param maxMessagesPerBundle maximum number of messages to include in a CometBFT bundle
     *                             (default: 10)
     * @param maxPriorValidatorSetUpdates maximum number of prior validator-set updates (trust-anchor
     *                             rotations) to include in a single bundle when advancing a lagging
     *                             peer across validator-set changes (default: 10)
     * @param maxRetries           maximum retry attempts for a CometBFT RPC call on a transient
     *                             failure, in addition to the initial attempt (default: 3)
     * @param requestTimeoutMs     per-request CometBFT RPC response timeout, in milliseconds
     *                             (default: 5000)
     * @param maxBundleSize        maximum assembled bundle-proof size, in bytes; a bundle exceeding it
     *                             is not emitted (defence-in-depth against a gas/size blowup on the
     *                             peer's {@code submitBundle}), on top of {@code maxMessagesPerBundle}
     *                             (default: {@code 1048576})
     */
    public record CometBftConfig(
            String cometBftRpcUrl,
            int maxMessagesPerBundle,
            int maxPriorValidatorSetUpdates,
            int maxRetries,
            long requestTimeoutMs,
            long maxBundleSize) {

        public CometBftConfig {
            if (maxBundleSize < 1) {
                throw new IllegalArgumentException("cometBft.maxBundleSize must be at least 1 byte");
            }
        }
    }

    /**
     * Ethereum-PoS beacon (consensus-layer) configuration. Present on {@link LocalNetworkConfig}
     * entries whose {@code proofType} is {@link ProofType#EthereumPOS}.
     *
     * @param beaconApiUrl         HTTP URL of the beacon node REST API
     *                             (default: {@code http://localhost:5052})
     * @param maxMessagesPerBundle maximum number of messages to include in a single bundle
     *                             (default: {@code 10})
     * @param maxRetries           maximum retry attempts for a beacon API call on a transient
     *                             failure, in addition to the initial attempt (default: {@code 3})
     * @param requestTimeoutMs     per-request beacon API response timeout in milliseconds
     *                             (default: {@code 30000} = 30 s)
     */
    public record BeaconConfig(String beaconApiUrl, int maxMessagesPerBundle, int maxRetries, long requestTimeoutMs) {}

    /**
     * gRPC server configuration. The server hosts {@code sync} and {@code getLedgerConfiguration} on
     * one port.
     *
     * <p>System properties: {@code relay.grpc.port}, {@code relay.grpc.maxMessageSize}
     *
     * @param port           TCP port the gRPC server listens on (default: {@code 9545})
     * @param maxMessageSize maximum gRPC message size in bytes (default: {@code 1048576} = 1 MiB).
     *                       Applied symmetrically to the inbound PBJ-Helidon server and to
     *                       outbound {@code ClprEndpointClient} Netty channels
     */
    @ConfigData("relay.grpc")
    public record GrpcConfig(
            @ConfigProperty(defaultValue = "9545") int port,

            @Positive @ConfigProperty(defaultValue = "1048576")
            int maxMessageSize) {}

    /**
     * Failure-backoff policy for the relay's per-channel loops (the sync loop and the listener
     * poll loop). When a loop iteration throws an unchecked exception the loop survives, logs at
     * ERROR, and waits a bounded, exponentially-growing delay before retrying — never the tight
     * per-iteration cadence. The first clean iteration resets the backoff.
     *
     * <p>System properties: {@code relay.backoff.baseMs}, {@code relay.backoff.capMs}
     *
     * @param baseMs first-failure backoff in milliseconds; the delay doubles per consecutive failure
     *               up to {@code capMs} (default: {@code 1000} = 1 s)
     * @param capMs  ceiling on the per-failure backoff in milliseconds (default: {@code 30000} =
     *               30 s)
     */
    @ConfigData("relay.backoff")
    public record BackoffConfig(
            @Positive @ConfigProperty(defaultValue = "1000") long baseMs,

            @Positive @ConfigProperty(defaultValue = "30000")
            long capMs) {
        public BackoffConfig {
            if (capMs < baseMs)
                throw new IllegalArgumentException(
                        "relay.backoff.capMs (" + capMs + ") must be >= baseMs (" + baseMs + ")");
        }
    }
}
