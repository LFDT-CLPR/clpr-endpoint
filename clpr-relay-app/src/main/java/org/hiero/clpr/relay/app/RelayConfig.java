// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.app;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import com.swirlds.config.api.validation.annotation.Positive;
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
 * @param grpc                shared gRPC server settings (max message size); the {@code info} and
 *                            {@code sync} listeners are separate top-level params to allow the
 *                            config framework to bind them independently
 * @param info                always-plaintext info listener (discovery + ledger configuration)
 * @param sync                sync (data-plane) listener: port plus optional mTLS material
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
        GrpcInfoConfig info,
        GrpcSyncConfig sync,
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
     * {@code cometBft}, {@code qbft}, {@code beacon}) carry the network connection and proof
     * parameters required to make RPC calls and build bundles.
     *
     * @param id        unique identifier for this network (referenced by {@link ClprServiceConfig})
     * @param proofType proof format discriminator used to build the implementation bundle and to
     *                  select the {@code BundlePayloadCodec} for inbound bundles
     * @param evm       EVM network connection parameters; non-null when {@code proofType} is
     *                  {@link ProofType#QBFT}, {@link ProofType#CometBFT}, or
     *                  {@link ProofType#EthereumPOS}
     * @param cometBft  CometBFT proof parameters; non-null when {@code proofType} is
     *                  {@link ProofType#CometBFT}, otherwise {@code null}
     * @param qbft      QBFT proof parameters; non-null when {@code proofType} is
     *                  {@link ProofType#QBFT}, otherwise {@code null}
     * @param beacon    Ethereum-PoS beacon (consensus-layer) proof parameters; non-null when
     *                  {@code proofType} is {@link ProofType#EthereumPOS}, otherwise {@code null}
     */
    public record LocalNetworkConfig(
            String id,
            ProofType proofType,
            CommonEvmParams evm,
            @Nullable CometBftConfig cometBft,
            @Nullable QbftConfig qbft,
            @Nullable BeaconConfig beacon) {}

    /**
     * Common EVM network connection parameters. Present on {@link LocalNetworkConfig} entries whose
     * {@code proofType} is {@link ProofType#QBFT} or {@link ProofType#CometBFT} — both run an EVM
     * JSON-RPC and the {@code ClprService} contract. Proof-format-specific parameters live in the
     * separate {@link QbftConfig} and {@link CometBftConfig} blocks.
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
     * QBFT proof parameters. Present on {@link LocalNetworkConfig} entries whose {@code proofType} is
     * {@link ProofType#QBFT}. These drive the epoch-boundary computation for the
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
     * Shared gRPC settings: the max inbound message size applied symmetrically to every inbound
     * PBJ-Helidon listener and to outbound {@code ClprEndpointClient} Netty channels.
     *
     * <p>System property: {@code relay.grpc.maxMessageSize}
     *
     * @param maxMessageSize maximum gRPC message size in bytes (default: {@code 1048576} = 1 MiB)
     */
    @ConfigData("relay.grpc")
    public record GrpcConfig(
            @Positive @ConfigProperty(defaultValue = "1048576")
            int maxMessageSize) {}

    /**
     * The <b>info</b> listener: always plaintext, always served. Hosts the public metadata RPCs
     * {@code discoverEndpoints} and {@code getLedgerConfiguration}, which return chain-verifiable data, so
     * they are never put behind TLS.
     *
     * <p>System property: {@code relay.grpc.info.port}
     *
     * @param port TCP port for the plaintext info listener (default: {@code 9546})
     */
    @ConfigData("relay.grpc.info")
    public record GrpcInfoConfig(
            @ConfigProperty(defaultValue = "9546") int port) {}

    /**
     * The <b>sync</b> (data-plane) listener: its port and optional mTLS key. When {@code tlsEnabled} is
     * {@code true} the sync listener is mandatory mTLS and {@code tlsKeyPath} is required; when
     * {@code false} the key is ignored and the listener is plaintext.
     *
     * <p>{@code tlsKeyPath} is this endpoint's ECDSA P-384 <b>CA private key</b> — the private half of the
     * CA whose certificate is published on-chain as {@code ClprEndpoint.tls_certificate}. The certificate
     * itself is never loaded from disk: at startup the relay generates an Ed25519 leaf signed by this key
     * and presents it at every handshake. A dialer's leaf must chain to a CA in the on-chain roster.
     *
     * <p>{@code leafCertValiditySeconds} controls how often the in-memory Ed25519 leaf is re-minted.
     * The leaf is regenerated on demand the first time a TLS handshake occurs after the window elapses.
     * Set to {@code 0} to disable rotation (the initial leaf is valid for 10 years). The default of
     * {@code 86400} (24 h) is a reasonable production value; adjust for compliance requirements.
     *
     * <p>System properties: {@code relay.grpc.sync.port}, {@code relay.grpc.sync.tlsEnabled},
     * {@code relay.grpc.sync.tlsKeyPath}, {@code relay.grpc.sync.leafCertValiditySeconds}
     *
     * @param port                     TCP port for the sync listener (default: {@code 9545})
     * @param tlsEnabled               {@code true} makes the sync listener mandatory mTLS (default: {@code false})
     * @param tlsKeyPath               path to this endpoint's CA private key (PKCS#8 PEM/DER); required iff
     *                                 {@code tlsEnabled}
     * @param leafCertValiditySeconds  seconds between leaf re-mints; {@code 0} disables rotation
     *                                 (default: {@code 86400})
     */
    @ConfigData("relay.grpc.sync")
    public record GrpcSyncConfig(
            @ConfigProperty(defaultValue = "9545") int port,
            @ConfigProperty(defaultValue = "false") boolean tlsEnabled,
            @ConfigProperty(defaultValue = "") String tlsKeyPath,
            @ConfigProperty(defaultValue = "86400") @Min(0) long leafCertValiditySeconds) {}

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
