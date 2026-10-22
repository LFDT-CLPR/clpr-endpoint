// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import com.swirlds.config.api.validation.ConfigViolationException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link RelayConfigLoader} against the {@code localNetworks} + {@code clprServices}
 * schema. Signing keys are configured per {@code ClprService} deployment (a service default plus
 * per-channel overrides), so there is no longer a required top-level signing key.
 */
class RelayConfigLoaderTest {

    // -------------------------------------------------------------------------
    // Scalar blocks (grpc / backoff) + empty defaults
    // -------------------------------------------------------------------------

    @Test
    void noConfigFile_yieldsDefaultsAndEmptyTopology() {
        final var config = RelayConfigLoader.load(null, new Properties());

        assertThat(config.grpc().port()).isEqualTo(9545);
        assertThat(config.grpc().maxMessageSize()).isEqualTo(1_048_576);
        assertThat(config.localNetworks()).isEmpty();
        assertThat(config.clprServices()).isEmpty();
        // No peerProofTypes provided anywhere → the built-in defaults.
        assertThat(config.peerProofTypes()).isEqualTo(RelayConfig.DEFAULT_PEER_PROOF_TYPES);
        assertThat(config.backoff().baseMs()).isEqualTo(1_000L);
        assertThat(config.backoff().capMs()).isEqualTo(30_000L);
    }

    @Test
    void noConfigFile_systemPropertiesOverrideGrpcDefaults() {
        final var overrides = new Properties();
        overrides.setProperty("relay.grpc.port", "12345");

        final var config = RelayConfigLoader.load(null, overrides);

        assertThat(config.grpc().port()).isEqualTo(12345);
        assertThat(config.clprServices()).isEmpty();
    }

    @Test
    void systemPropertyOverridesGrpcPort(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("relay-config.yaml");
        Files.writeString(yaml, """
                grpc:
                  port: 9000
                """);
        final var overrides = new Properties();
        overrides.setProperty("relay.grpc.port", "7777");

        final var config = RelayConfigLoader.load(yaml, overrides);

        assertThat(config.grpc().port()).isEqualTo(7777);
    }

    @Test
    void grpcMaxMessageSize_fromYamlFile(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("grpc-max.yaml");
        Files.writeString(yaml, """
                grpc:
                  maxMessageSize: 4194304
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        assertThat(config.grpc().maxMessageSize()).isEqualTo(4_194_304);
    }

    @Test
    void grpcMaxMessageSize_nonPositive_failsFast() {
        final var overrides = new Properties();
        overrides.setProperty("relay.grpc.maxMessageSize", "0");

        assertThatThrownBy(() -> RelayConfigLoader.load(null, overrides))
                .isInstanceOf(ConfigViolationException.class)
                .hasMessageContaining("violations");
    }

    @Test
    void backoff_fromYamlFile(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("backoff.yaml");
        Files.writeString(yaml, """
                backoff:
                  baseMs: 250
                  capMs: 60000
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        assertThat(config.backoff().baseMs()).isEqualTo(250L);
        assertThat(config.backoff().capMs()).isEqualTo(60_000L);
    }

    @Test
    void backoff_systemPropertyOverride() {
        final var overrides = new Properties();
        overrides.setProperty("relay.backoff.capMs", "5000");

        final var config = RelayConfigLoader.load(null, overrides);

        assertThat(config.backoff().capMs()).isEqualTo(5_000L);
    }

    // -------------------------------------------------------------------------
    // localNetworks
    // -------------------------------------------------------------------------

    @Test
    void localNetwork_qbftDefaults_whenFieldsOmitted(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("defaults.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: minimal
                    proofType: QBFT
                    evm: {}
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        final var net = config.localNetworks().getFirst();
        final var evm = net.evm();
        assertThat(evm.jsonRpcUrl()).isEqualTo("http://localhost:8545");
        assertThat(evm.chainId()).isEqualTo(1L);
        assertThat(evm.maxGasPriceCap()).isEqualTo(Long.MAX_VALUE);
        assertThat(evm.gasPriorityFee()).isEqualTo(2_000_000_000L);
        assertThat(evm.gasBufferMultiplier()).isEqualTo(1.2);
        assertThat(evm.pollIntervalMs()).isEqualTo(1_000L);
        assertThat(net.qbft()).isNotNull();
        assertThat(net.qbft().epochLength()).isEqualTo(30_000L);
        assertThat(net.qbft().maxEpochBlockHeadersPerBundle()).isEqualTo(5);
        assertThat(net.qbft().maxMessagesPerBundle()).isEqualTo(10);
        assertThat(net.qbftContractMode()).isNull();
    }

    @Test
    void localNetwork_qbftContractMode_parsed(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("qbft-contract-mode.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: minimal
                    proofType: QBFTContractMode
                    evm: {}
                    qbftContractMode:
                      validatorContractAddress: "0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead"
                      arraySlotHex: "0x02"
                      maxMessagesPerBundle: 7
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        final var net = config.localNetworks().getFirst();
        assertThat(net.proofType()).isEqualTo(ProofType.QBFTContractMode);
        assertThat(net.qbft()).isNull();
        final var qbftContractMode = net.qbftContractMode();
        assertThat(qbftContractMode).isNotNull();
        assertThat(qbftContractMode.validatorContractAddress()).isEqualTo("0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead");
        assertThat(qbftContractMode.arraySlotHex()).isEqualTo("0x02");
        assertThat(qbftContractMode.arraySlot()).isEqualTo(BigInteger.TWO);
        assertThat(qbftContractMode.maxMessagesPerBundle()).isEqualTo(7);
    }

    @Test
    void localNetwork_qbftContractMode_defaultMaxMessagesPerBundle_whenOmitted(@TempDir final Path tmp)
            throws Exception {
        final var yaml = tmp.resolve("qbft-contract-mode-defaults.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: minimal
                    proofType: QBFTContractMode
                    evm: {}
                    qbftContractMode:
                      validatorContractAddress: "0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead"
                      arraySlotHex: "0x02"
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        assertThat(config.localNetworks().getFirst().qbftContractMode().maxMessagesPerBundle())
                .isEqualTo(10);
    }

    @Test
    void localNetwork_qbftContractMode_missingBlock_failsFast(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("qbft-contract-mode-missing-block.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: minimal
                    proofType: QBFTContractMode
                    evm: {}
                """);

        assertThatThrownBy(() -> RelayConfigLoader.load(yaml, new Properties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("qbftContractMode");
    }

    @Test
    void localNetwork_qbftContractMode_missingAddress_failsFast(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("qbft-contract-mode-missing-address.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: minimal
                    proofType: QBFTContractMode
                    evm: {}
                    qbftContractMode:
                      arraySlotHex: "0x02"
                """);

        assertThatThrownBy(() -> RelayConfigLoader.load(yaml, new Properties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("validatorContractAddress");
    }

    @Test
    void localNetwork_qbftContractMode_invalidAddress_failsFast(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("qbft-contract-mode-invalid-address.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: minimal
                    proofType: QBFTContractMode
                    evm: {}
                    qbftContractMode:
                      validatorContractAddress: "not-an-address"
                      arraySlotHex: "0x02"
                """);

        assertThatThrownBy(() -> RelayConfigLoader.load(yaml, new Properties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("validatorContractAddress");
    }

    @Test
    void localNetwork_qbftContractMode_missingArraySlot_failsFast(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("qbft-contract-mode-missing-slot.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: minimal
                    proofType: QBFTContractMode
                    evm: {}
                    qbftContractMode:
                      validatorContractAddress: "0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead"
                """);

        assertThatThrownBy(() -> RelayConfigLoader.load(yaml, new Properties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("arraySlotHex");
    }

    @Test
    void localNetwork_qbftContractMode_invalidArraySlotHex_failsFast(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("qbft-contract-mode-invalid-slot.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: minimal
                    proofType: QBFTContractMode
                    evm: {}
                    qbftContractMode:
                      validatorContractAddress: "0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead"
                      arraySlotHex: "0xzz"
                """);

        assertThatThrownBy(() -> RelayConfigLoader.load(yaml, new Properties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("arraySlotHex")
                .hasMessageContaining("not a valid storage slot");
    }

    @Test
    void localNetwork_qbftContractMode_arraySlotHexTooLarge_failsFast(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("qbft-contract-mode-slot-too-large.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: minimal
                    proofType: QBFTContractMode
                    evm: {}
                    qbftContractMode:
                      validatorContractAddress: "0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead"
                      arraySlotHex: "0x1%s"
                """.formatted("0".repeat(64)));

        assertThatThrownBy(() -> RelayConfigLoader.load(yaml, new Properties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("arraySlotHex")
                .hasMessageContaining("not a valid storage slot");
    }

    @Test
    void localNetwork_qbftContractMode_negativeArraySlot_failsFast(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("qbft-contract-mode-negative-slot.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: minimal
                    proofType: QBFTContractMode
                    evm: {}
                    qbftContractMode:
                      validatorContractAddress: "0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead"
                      arraySlotHex: "-2"
                """);

        assertThatThrownBy(() -> RelayConfigLoader.load(yaml, new Properties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("arraySlotHex")
                .hasMessageContaining("not a valid storage slot");
    }

    @Test
    void cometBftLocalNetwork_parsedWithCometBftBlock(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("cometbft.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: sei-local
                    proofType: CometBFT
                    evm:
                      jsonRpcUrl: "http://sei:8545"
                      chainId: 1329
                    cometBft:
                      cometBftRpcUrl: "http://sei:26657"
                      maxMessagesPerBundle: 7
                      maxRetries: 9
                      requestTimeoutMs: 12000
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        final var net = config.localNetworks().getFirst();
        assertThat(net.proofType()).isEqualTo(ProofType.CometBFT);
        assertThat(net.evm()).isNotNull();
        assertThat(net.evm().jsonRpcUrl()).isEqualTo("http://sei:8545");
        assertThat(net.cometBft()).isNotNull();
        assertThat(net.cometBft().cometBftRpcUrl()).isEqualTo("http://sei:26657");
        assertThat(net.cometBft().maxMessagesPerBundle()).isEqualTo(7);
        assertThat(net.cometBft().maxRetries()).isEqualTo(9);
        assertThat(net.cometBft().requestTimeoutMs()).isEqualTo(12000L);
    }

    @Test
    void cometBftLocalNetwork_cometBftDefaultsWhenBlockAbsentAndProofTypeIsCaseInsensitive(@TempDir final Path tmp)
            throws Exception {
        final var yaml = tmp.resolve("cometbft-defaults.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: sei-local
                    proofType: cometbft
                    evm:
                      jsonRpcUrl: "http://sei:8545"
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        final var net = config.localNetworks().getFirst();
        assertThat(net.proofType()).isEqualTo(ProofType.CometBFT);
        assertThat(net.cometBft()).isNotNull();
        assertThat(net.cometBft().cometBftRpcUrl()).isEqualTo("http://localhost:26657");
        assertThat(net.cometBft().maxMessagesPerBundle()).isEqualTo(10);
        assertThat(net.cometBft().maxRetries()).isEqualTo(3);
        assertThat(net.cometBft().requestTimeoutMs()).isEqualTo(5000L);
    }

    @Test
    void multipleLocalNetworks_parsedInOrder(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("multi-network.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: chain-a
                    proofType: QBFT
                    evm:
                      jsonRpcUrl: "http://chain-a:8545"
                      chainId: 1111
                  - id: chain-b
                    proofType: QBFT
                    evm:
                      jsonRpcUrl: "http://chain-b:8545"
                      chainId: 2222
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        assertThat(config.localNetworks()).hasSize(2);
        assertThat(config.localNetworks().get(0).id()).isEqualTo("chain-a");
        assertThat(config.localNetworks().get(0).evm().chainId()).isEqualTo(1111L);
        assertThat(config.localNetworks().get(1).id()).isEqualTo("chain-b");
        assertThat(config.localNetworks().get(1).evm().chainId()).isEqualTo(2222L);
    }

    @Test
    void localNetwork_missingEvmBlock_failsFast(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("missing-evm.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: no-params
                    proofType: QBFT
                """);

        assertThatThrownBy(() -> RelayConfigLoader.load(yaml, new Properties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no-params")
                .hasMessageContaining("evm");
    }

    @Test
    void ethereumPosLocalNetwork_parsedWithBeaconBlock(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("ethereum-pos.yaml");
        Files.writeString(yaml, """
                signingPrivateKeyHex: "0xaabbccdd"
                localNetworks:
                  - id: ethereum-local
                    proofType: EthereumPOS
                    evm:
                      jsonRpcUrl: "http://eth:8545"
                      chainId: 1
                    beacon:
                      beaconApiUrl: "http://eth:5052"
                      maxMessagesPerBundle: 7
                      maxRetries: 9
                      requestTimeoutMs: 12000
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        final var net = config.localNetworks().getFirst();
        assertThat(net.proofType()).isEqualTo(ProofType.EthereumPOS);
        assertThat(net.evm()).isNotNull();
        assertThat(net.evm().jsonRpcUrl()).isEqualTo("http://eth:8545");
        assertThat(net.cometBft()).isNull();
        assertThat(net.qbft()).isNull();
        assertThat(net.beacon()).isNotNull();
        assertThat(net.beacon().beaconApiUrl()).isEqualTo("http://eth:5052");
        assertThat(net.beacon().maxMessagesPerBundle()).isEqualTo(7);
        assertThat(net.beacon().maxRetries()).isEqualTo(9);
        assertThat(net.beacon().requestTimeoutMs()).isEqualTo(12000L);
    }

    @Test
    void ethereumPosLocalNetwork_beaconDefaultsWhenBlockAbsentAndProofTypeIsCaseInsensitive(@TempDir final Path tmp)
            throws Exception {
        final var yaml = tmp.resolve("ethereum-pos-defaults.yaml");
        Files.writeString(yaml, """
                signingPrivateKeyHex: "0xaabbccdd"
                localNetworks:
                  - id: ethereum-local
                    proofType: ethereumpos
                    evm:
                      jsonRpcUrl: "http://eth:8545"
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        final var net = config.localNetworks().getFirst();
        assertThat(net.proofType()).isEqualTo(ProofType.EthereumPOS);
        // A missing `beacon:` block yields an all-defaults BeaconConfig (never null for EthereumPOS).
        assertThat(net.beacon()).isNotNull();
        assertThat(net.beacon().beaconApiUrl()).isEqualTo("http://localhost:5052");
        assertThat(net.beacon().maxMessagesPerBundle()).isEqualTo(10);
        assertThat(net.beacon().maxRetries()).isEqualTo(3);
        assertThat(net.beacon().requestTimeoutMs()).isEqualTo(30000L);
    }

    @Test
    void localNetwork_invalidProofType_failsFast(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("bad-proof-type.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: bad-chain
                    proofType: UNKNOWN_TYPE
                    evm: {}
                """);

        assertThatThrownBy(() -> RelayConfigLoader.load(yaml, new Properties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("bad-chain")
                .hasMessageContaining("UNKNOWN_TYPE");
    }

    @Test
    void localNetwork_hieroProofType_failsFastForLocalNetwork(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("hiero-local.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: hiero-chain
                    proofType: Hiero
                    evm: {}
                """);

        assertThatThrownBy(() -> RelayConfigLoader.load(yaml, new Properties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("hiero-chain")
                .hasMessageContaining("Hiero");
    }

    // -------------------------------------------------------------------------
    // clprServices
    // -------------------------------------------------------------------------

    @Test
    void fullConfigFile_parsedCorrectlyIncludingServices(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("relay-config.yaml");
        Files.writeString(yaml, """
                grpc:
                  sync:
                    port: 9545
                peerProofTypes:
                  "eip155:1337": QBFT
                localNetworks:
                  - id: besu-local
                    proofType: QBFT
                    evm:
                      jsonRpcUrl: "http://besu:8545"
                      chainId: 1337
                      gasBufferMultiplier: 1.5
                      maxGasPriceCap: 50000000000
                      gasPriorityFee: 1500000000
                clprServices:
                  - serviceAddress: "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
                    localNetwork: besu-local
                    defaultSigningPrivateKeyHex: "0xaabbccdd"
                    discoverChannels: true
                    discoveryStartBlock: 4200
                    predefinedChannels:
                      - "0x00112233"
                      - "0xdeadbeef"
                    perChannelSigningPrivateKeyHex:
                      "0xdeadbeef": "0x99887766"
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        assertThat(config.grpc().port()).isEqualTo(9545);
        assertThat(config.peerProofTypes()).containsEntry("eip155:1337", ProofType.QBFT);

        assertThat(config.localNetworks()).hasSize(1);
        final var net = config.localNetworks().getFirst();
        assertThat(net.id()).isEqualTo("besu-local");
        assertThat(net.proofType()).isEqualTo(ProofType.QBFT);
        assertThat(net.evm().jsonRpcUrl()).isEqualTo("http://besu:8545");
        assertThat(net.evm().chainId()).isEqualTo(1337L);
        assertThat(net.evm().gasBufferMultiplier()).isEqualTo(1.5);
        assertThat(net.evm().maxGasPriceCap()).isEqualTo(50_000_000_000L);
        assertThat(net.evm().gasPriorityFee()).isEqualTo(1_500_000_000L);

        assertThat(config.clprServices()).hasSize(1);
        final var svc = config.clprServices().getFirst();
        assertThat(svc.serviceAddress()).isEqualTo("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
        assertThat(svc.localNetwork()).isEqualTo("besu-local");
        assertThat(svc.defaultSigningPrivateKeyHex()).isEqualTo("0xaabbccdd");
        assertThat(svc.discoverChannels()).isTrue();
        assertThat(svc.discoveryStartBlock()).isEqualTo(4200L);
        assertThat(svc.predefinedChannels()).containsExactly("0x00112233", "0xdeadbeef");
        assertThat(svc.perChannelSigningPrivateKeyHex()).containsEntry("0xdeadbeef", "0x99887766");
    }

    @Test
    void clprServiceDefaults_whenOptionalFieldsOmitted(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("service-defaults.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: besu-local
                    proofType: QBFT
                    evm:
                      jsonRpcUrl: "http://besu:8545"
                clprServices:
                  - serviceAddress: "0x1111111111111111111111111111111111111111"
                    localNetwork: besu-local
                    defaultSigningPrivateKeyHex: "0xaabbccdd"
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        final var svc = config.clprServices().getFirst();
        assertThat(svc.discoverChannels()).isFalse();
        assertThat(svc.discoveryStartBlock()).isZero();
        assertThat(svc.predefinedChannels()).isEmpty();
        assertThat(svc.perChannelSigningPrivateKeyHex()).isEmpty();
    }

    @Test
    void multipleServicesOnSameNetwork_isValid(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("two-services.yaml");
        Files.writeString(yaml, """
                signingPrivateKeyHex: "0xaabbccdd"
                localNetworks:
                  - id: chain-a
                    proofType: QBFT
                    evm:
                      jsonRpcUrl: "http://localhost:8545"
                clprServices:
                  - serviceAddress: "0x1111111111111111111111111111111111111111"
                    localNetwork: chain-a
                    defaultSigningPrivateKeyHex: "0xaabbccdd"
                  - serviceAddress: "0x2222222222222222222222222222222222222222"
                    localNetwork: chain-a
                    defaultSigningPrivateKeyHex: "0xaabbccdd"
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        assertThat(config.clprServices()).hasSize(2);
        assertThat(config.clprServices().get(0).serviceAddress())
                .isEqualTo("0x1111111111111111111111111111111111111111");
        assertThat(config.clprServices().get(1).serviceAddress())
                .isEqualTo("0x2222222222222222222222222222222222222222");
    }

    @Test
    void clprService_missingServiceAddress_fails(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("missing-service-address.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: chain-a
                    proofType: QBFT
                    evm: {}
                clprServices:
                  - localNetwork: chain-a
                    defaultSigningPrivateKeyHex: "0xaabbccdd"
                """);

        assertThatThrownBy(() -> RelayConfigLoader.load(yaml, new Properties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serviceAddress");
    }

    @Test
    void clprService_missingLocalNetwork_fails(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("missing-local.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: chain-a
                    proofType: QBFT
                    evm: {}
                clprServices:
                  - serviceAddress: "0x1111111111111111111111111111111111111111"
                    defaultSigningPrivateKeyHex: "0xaabbccdd"
                """);

        assertThatThrownBy(() -> RelayConfigLoader.load(yaml, new Properties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("localNetwork");
    }

    @Test
    void clprService_unknownLocalNetworkRef_fails(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("unknown-local.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: known-chain
                    proofType: QBFT
                    evm: {}
                clprServices:
                  - serviceAddress: "0x1111111111111111111111111111111111111111"
                    localNetwork: unknown-chain
                    defaultSigningPrivateKeyHex: "0xaabbccdd"
                """);

        assertThatThrownBy(() -> RelayConfigLoader.load(yaml, new Properties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unknown-chain");
    }

    @Test
    void serviceAddress_preservedAsProvided(@TempDir final Path tmp) throws Exception {
        // Loaded verbatim — no case normalisation. (De-dup of the same contract on the same network
        // happens in RelayInstance; see RelayInstanceTest.)
        final var yaml = tmp.resolve("mixed-case-service.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: chain-a
                    proofType: QBFT
                    evm: {}
                clprServices:
                  - serviceAddress: "0xAbCdEf0000000000000000000000000000000001"
                    localNetwork: chain-a
                    defaultSigningPrivateKeyHex: "0xaabbccdd"
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        assertThat(config.clprServices().getFirst().serviceAddress())
                .isEqualTo("0xAbCdEf0000000000000000000000000000000001");
    }

    // -------------------------------------------------------------------------
    // peerProofTypes
    // -------------------------------------------------------------------------

    @Test
    void peerProofTypes_absentInFile_fallsBackToDefault(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("no-peer-proof-types.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: besu-local
                    proofType: QBFT
                    evm:
                      jsonRpcUrl: "http://besu:8545"
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        assertThat(config.peerProofTypes()).isEqualTo(RelayConfig.DEFAULT_PEER_PROOF_TYPES);
    }

    @Test
    void peerProofTypes_emptyBlock_fallsBackToDefault(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("empty-peer-proof-types.yaml");
        Files.writeString(yaml, """
                peerProofTypes: {}
                localNetworks:
                  - id: besu-local
                    proofType: QBFT
                    evm:
                      jsonRpcUrl: "http://besu:8545"
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        assertThat(config.peerProofTypes()).isEqualTo(RelayConfig.DEFAULT_PEER_PROOF_TYPES);
    }

    @Test
    void peerProofTypes_whenProvided_overridesDefault(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("explicit-peer-proof-types.yaml");
        Files.writeString(yaml, """
                peerProofTypes:
                  "eip155:9999": CometBFT
                localNetworks:
                  - id: besu-local
                    proofType: QBFT
                    evm:
                      jsonRpcUrl: "http://besu:8545"
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        // The provided map replaces the defaults wholesale (no merge).
        assertThat(config.peerProofTypes()).containsExactly(entry("eip155:9999", ProofType.CometBFT));
    }

    @Test
    void peerProofTypes_notAMap_failsFast(@TempDir final Path tmp) throws Exception {
        // A scalar where a chainId -> proofType map is expected must fail startup with a clear message.
        final var yaml = tmp.resolve("bad-peer-proof-types.yaml");
        Files.writeString(yaml, """
                peerProofTypes: "not-a-map"
                """);

        assertThatThrownBy(() -> RelayConfigLoader.load(yaml, new Properties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invalid relay config file")
                .hasMessageContaining("not-a-map");
    }

    @Test
    void peerProofTypes_valueNotAProofTypeString_failsFast(@TempDir final Path tmp) throws Exception {
        // A nested object where a proof-type string is expected must fail startup.
        final var yaml = tmp.resolve("bad-peer-proof-value.yaml");
        Files.writeString(yaml, """
                peerProofTypes:
                  "eip155:1":
                    nested: value
                """);

        assertThatThrownBy(() -> RelayConfigLoader.load(yaml, new Properties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invalid relay config file");
    }

    // -------------------------------------------------------------------------
    // Signing-key validation
    // -------------------------------------------------------------------------

    @Test
    void discoveringService_withoutDefaultKey_failsFast(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("discover-no-key.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: chain-a
                    proofType: QBFT
                    evm: {}
                clprServices:
                  - serviceAddress: "0x1111111111111111111111111111111111111111"
                    localNetwork: chain-a
                    discoverChannels: true
                """);

        assertThatThrownBy(() -> RelayConfigLoader.load(yaml, new Properties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("defaultSigningPrivateKeyHex");
    }

    @Test
    void predefinedChannelWithoutKey_andNoDefault_failsFast(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("predefined-no-key.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: chain-a
                    proofType: QBFT
                    evm: {}
                clprServices:
                  - serviceAddress: "0x1111111111111111111111111111111111111111"
                    localNetwork: chain-a
                    predefinedChannels:
                      - "0x01"
                """);

        assertThatThrownBy(() -> RelayConfigLoader.load(yaml, new Properties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("defaultSigningPrivateKeyHex");
    }

    @Test
    void predefinedChannelWithPerChannelKey_andNoDefault_isValid(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("predefined-with-key.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: chain-a
                    proofType: QBFT
                    evm: {}
                clprServices:
                  - serviceAddress: "0x1111111111111111111111111111111111111111"
                    localNetwork: chain-a
                    predefinedChannels:
                      - "0x01"
                    perChannelSigningPrivateKeyHex:
                      "0x01": "0xaabbccdd"
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        final var svc = config.clprServices().getFirst();
        assertThat(svc.defaultSigningPrivateKeyHex()).isEmpty();
        assertThat(svc.predefinedChannels()).containsExactly("0x01");
    }

    // -------------------------------------------------------------------------
    // File-level error handling
    // -------------------------------------------------------------------------

    @Test
    void missingFile_throwsWithClearMessage(@TempDir final Path tmp) {
        final var missing = tmp.resolve("definitely-does-not-exist.yaml");

        assertThatThrownBy(() -> RelayConfigLoader.load(missing, new Properties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not found")
                .hasMessageContaining(missing.toAbsolutePath().toString());
    }

    @Test
    void malformedFile_throwsWithClearMessage(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("bad.yaml");
        Files.writeString(yaml, "grpc:\n  port: 9000\n  : : invalid : yaml :\n  - :\n");

        assertThatThrownBy(() -> RelayConfigLoader.load(yaml, new Properties()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(yaml.toAbsolutePath().toString());
    }

    // -------------------------------------------------------------------------
    // gRPC port validation
    // -------------------------------------------------------------------------

    @Test
    void grpcPortZero_failsFast() {
        final var overrides = new Properties();
        overrides.setProperty("relay.grpc.port", "0");

        assertThatThrownBy(() -> RelayConfigLoader.load(null, overrides))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("relay.grpc.port must be > 0");
    }

    @Test
    void grpcPort_fromYamlFile(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("ports.yaml");
        Files.writeString(yaml, """
                grpc:
                  port: 9700
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        assertThat(config.grpc().port()).isEqualTo(9700);
    }

    @Test
    void maxBundleSize_defaultsAndParsesFromYaml(@TempDir final Path tmp) throws Exception {
        final var yaml = tmp.resolve("maxbundle.yaml");
        Files.writeString(yaml, """
                localNetworks:
                  - id: besu1
                    proofType: QBFT
                    evm: {}
                  - id: sei1
                    proofType: CometBFT
                    evm: {}
                    cometBft:
                      maxBundleSize: 2048
                """);

        final var config = RelayConfigLoader.load(yaml, new Properties());

        final var qbft = config.localNetworks().stream()
                .filter(n -> n.proofType() == ProofType.QBFT)
                .findFirst()
                .orElseThrow();
        final var cometBft = config.localNetworks().stream()
                .filter(n -> n.proofType() == ProofType.CometBFT)
                .findFirst()
                .orElseThrow();
        assertThat(qbft.qbft().maxBundleSize()).isEqualTo(1_048_576L); // default when omitted
        assertThat(cometBft.cometBft().maxBundleSize()).isEqualTo(2_048L); // from yaml
    }
}
