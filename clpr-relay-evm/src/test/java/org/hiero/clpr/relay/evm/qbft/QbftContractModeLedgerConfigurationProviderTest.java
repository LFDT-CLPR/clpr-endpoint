// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.qbft;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.hapi.node.state.clpr.ClprThrottles;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.ByteUtils;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.model.BlockHeader;
import org.hiero.clpr.relay.evm.model.EvmStorageMerkleProof;
import org.hiero.clpr.relay.evm.model.ProofResponse;
import org.hiero.clpr.relay.evm.qbft.ValidatorContractProofSource.ContractProof;
import org.hiero.clpr.relay.evm.qbft.ValidatorContractProofSource.ProvenValidatorSet;
import org.hiero.clpr.relay.evm.qbft.ValidatorContractProofSource.ValidatorSet;
import org.hiero.clpr.relay.evm.testfixtures.TestEvmJsonRpcClient;
import org.junit.jupiter.api.Test;

class QbftContractModeLedgerConfigurationProviderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String ZERO_HASH = "0x" + "00".repeat(32);
    private static final String ONE_HASH = "0x" + "00".repeat(31) + "01";
    private static final String MINER = "0x" + "ab".repeat(20);
    private static final Address CONTRACT_ADDRESS = Address.fromHexString("0x" + "cc".repeat(20));
    private static final Address VALIDATOR_CONTRACT = Address.fromHexString("0x" + "dd".repeat(20));

    private static final class StubRpcClient extends TestEvmJsonRpcClient {
        private final Map<String, BlockHeader> headersByTag = new HashMap<>();
        private ProofResponse proofResponse;
        String[] lastProofStorageKeys;
        String lastProofBlockTag;
        int proofCallCount;

        void setBlock(final String tag, final JsonNode block) {
            headersByTag.put(tag, EvmJsonRpcClient.blockHeaderFromJson(block));
        }

        void setProofResponse(final JsonNode proof) {
            this.proofResponse = new ProofResponse(
                    EvmJsonRpcClient.accountProofFromJson(proof), EvmJsonRpcClient.storageProofFromJson(proof));
        }

        @Override
        public BlockHeader ethGetBlockHeaderByNumber(final String blockTag) {
            final BlockHeader h = headersByTag.get(blockTag);
            if (h == null) {
                throw new AssertionError("unexpected block tag requested: " + blockTag);
            }
            return h;
        }

        @Override
        public ProofResponse ethGetProof(final String address, final String[] storageKeys, final String blockTag) {
            this.lastProofStorageKeys = storageKeys;
            this.lastProofBlockTag = blockTag;
            this.proofCallCount++;
            if (proofResponse == null) {
                throw new AssertionError("no proof response stubbed");
            }
            return proofResponse;
        }
    }

    private static final class StubStateReader implements ContractStateReader {
        private final ClprLedgerConfiguration config;
        String lastBlockTag;

        StubStateReader(final ClprLedgerConfiguration config) {
            this.config = config;
        }

        @Override
        public Optional<ClprChannel> readChannelState(final Bytes channelId, final String blockTag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<QueuedMessage> readQueuedMessages(
                final Bytes channelId, final long fromId, final long toId, final String blockTag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ClprLedgerConfiguration readLedgerConfiguration(final CommitmentLevel commitmentLevel) {
            throw new AssertionError("provider must use the string-tag overload to pin the read to an exact block; got "
                    + commitmentLevel);
        }

        @Override
        public ClprLedgerConfiguration readLedgerConfiguration(final String blockTag) {
            this.lastBlockTag = blockTag;
            return config;
        }
    }

    private static final class StubValidatorSource implements ValidatorContractProofSource {
        BigInteger lastRequestedBlock;

        @Override
        public ProvenValidatorSet proveValidatorSet(final BigInteger blockNumber) {
            this.lastRequestedBlock = blockNumber;
            final var set = new ValidatorSet(List.of(Address.fromHexString("0x" + "11".repeat(20))));
            final var proof = new ContractProof(
                    VALIDATOR_CONTRACT,
                    List.of(ByteUtils.fromPrefixedHex("0xf8518080")),
                    Bytes.wrap(AbiCodec.encodeUint256(BigInteger.TWO)),
                    List.of(new EvmStorageMerkleProof(
                            ByteUtils.leftPad32(ByteUtils.fromPrefixedHex("0x02")),
                            List.of(ByteUtils.fromPrefixedHex("0xf8518080")))));
            return new ProvenValidatorSet(set, proof);
        }
    }

    @Test
    void provide_attachesContractModeValidatorSourceAndOmitsEpochHeader() throws Exception {
        final var config = ClprLedgerConfiguration.newBuilder()
                .protocolVersion(1)
                .chainId("eip155:1337")
                .serviceAddress(Bytes.wrap(new byte[] {0x12, 0x34}))
                .throttles(ClprThrottles.newBuilder().maxMessagesPerBundle(50).build())
                .build();
        final var stateReader = new StubStateReader(config);

        final var rpc = new StubRpcClient();
        rpc.setBlock("finalized", block("0x100", ONE_HASH, "0xdeadbeef", "0x65111111"));
        final String serviceAddressSlotValue = "0x" + "12".repeat(20) + "00".repeat(11) + "28";
        rpc.setProofResponse(proof(serviceAddressSlotValue));

        final var validatorSource = new StubValidatorSource();
        final var provider =
                new QbftContractModeLedgerConfigurationProvider(rpc, stateReader, CONTRACT_ADDRESS, validatorSource);

        final var response = provider.provide(CommitmentLevel.FINALIZED);
        assertThat(response.qbft()).isNotNull();
        final var qbft = response.qbft();

        assertThat(stateReader.lastBlockTag).isEqualTo("0x100");
        assertThat(qbft.ledgerConfiguration()).isEqualTo(config);
        assertThat(qbft.currentBlockHeader()).isNotNull();
        // Meaningless in contract mode — must be left unset, not synthesized.
        assertThat(qbft.latestEpochBlockHeader()).isNull();

        assertThat(validatorSource.lastRequestedBlock).isEqualTo(BigInteger.valueOf(0x100));
        final var validatorSourcePayload = qbft.contractModeValidatorSource();
        assertThat(validatorSourcePayload).isNotNull();
        assertThat(validatorSourcePayload.validatorContractAddress()).isEqualTo(Bytes.wrap(VALIDATOR_CONTRACT.value()));
        assertThat(validatorSourcePayload.arraySlot()).isEqualTo(Bytes.wrap(AbiCodec.encodeUint256(BigInteger.TWO)));
        assertThat(validatorSourcePayload.validatorContractAccountProof()).hasSize(1);
        assertThat(validatorSourcePayload.validatorContractStorageProofs()).hasSize(1);

        assertThat(rpc.proofCallCount).isEqualTo(1);
        assertThat(rpc.lastProofBlockTag).isEqualTo("0x100");
    }

    private static JsonNode block(
            final String number, final String hash, final String extraData, final String timestamp) throws Exception {
        return MAPPER.readTree("""
                {
                  "parentHash":            "%s",
                  "sha3Uncles":            "%s",
                  "miner":                 "%s",
                  "stateRoot":             "%s",
                  "transactionsRoot":      "%s",
                  "receiptsRoot":          "%s",
                  "logsBloom":             "0x%s",
                  "difficulty":            "0x00",
                  "number":                "%s",
                  "gasLimit":              "0x1000",
                  "gasUsed":               "0x800",
                  "timestamp":             "%s",
                  "extraData":             "%s",
                  "mixHash":               "%s",
                  "nonce":                 "0x0000000000000000",
                  "baseFeePerGas":         "0x07",
                  "withdrawalsRoot":       null,
                  "parentBeaconBlockRoot": null,
                  "hash":                  "%s"
                }
                """.formatted(
                        ZERO_HASH,
                        ZERO_HASH,
                        MINER,
                        ZERO_HASH,
                        ZERO_HASH,
                        ZERO_HASH,
                        "00".repeat(256),
                        number,
                        timestamp,
                        extraData,
                        ZERO_HASH,
                        hash));
    }

    private static JsonNode proof(final String storageValue) throws Exception {
        return MAPPER.readTree("""
                {
                  "nonce":       "0x01",
                  "balance":     "0x00",
                  "storageHash": "%s",
                  "codeHash":    "%s",
                  "accountProof": [
                    "0xf8518080",
                    "0xf85180"
                  ],
                  "storageProof": [
                    { "key": "0x0000000000000000000000000000000000000000000000000000000000000019", "value": "%s", "proof": ["0xf8518080", "0xe2a0"] }
                  ]
                }
                """.formatted(ZERO_HASH, ONE_HASH, storageValue));
    }
}
