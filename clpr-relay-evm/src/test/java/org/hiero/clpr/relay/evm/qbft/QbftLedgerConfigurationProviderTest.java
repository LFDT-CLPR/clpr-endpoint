// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.qbft;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.hapi.node.state.clpr.ClprThrottles;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.model.BlockHeader;
import org.hiero.clpr.relay.evm.model.ProofResponse;
import org.hiero.clpr.relay.evm.testfixtures.TestEvmJsonRpcClient;
import org.junit.jupiter.api.Test;

class QbftLedgerConfigurationProviderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String ZERO_HASH = "0x" + "00".repeat(32);
    private static final String ONE_HASH = "0x" + "00".repeat(31) + "01";
    private static final String GENESIS_HASH = "0x" + "00".repeat(31) + "aa";
    private static final String MINER = "0x" + "ab".repeat(20);
    private static final String CONTRACT_HEX = "0x" + "cc".repeat(20);
    private static final Address CONTRACT_ADDRESS = Address.fromHexString(CONTRACT_HEX);

    /**
     * Storage slot for {@code _config.serviceAddress}, mirrored from
     * {@link org.hiero.clpr.relay.evm.storage.ClprServiceStorageLayout#CONFIG_SERVICE_ADDRESS_SLOT}
     * so the assertion fails loudly if either side moves.
     */
    private static final String SERVICE_ADDRESS_SLOT_HEX = "0x" + "0".repeat(62) + "19"; // 0x19 = 25

    /** Stub RPC client that returns a different parsed header per requested block tag. */
    private static final class StubRpcClient extends TestEvmJsonRpcClient {
        private final Map<String, BlockHeader> headersByTag = new HashMap<>();
        private ProofResponse proofResponse;

        String lastProofAddress;
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
            this.lastProofAddress = address;
            this.lastProofStorageKeys = storageKeys;
            this.lastProofBlockTag = blockTag;
            this.proofCallCount++;
            if (proofResponse == null) {
                throw new AssertionError("no proof response stubbed");
            }
            return proofResponse;
        }
    }

    /**
     * Stub state reader that returns a fixed configuration and records the exact block tag
     * the provider asked for via the string-tag overload.
     */
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

    @Test
    void provide_assemblesPayloadWithGenesisAndCurrentHeadersAndServiceAddressProof() throws Exception {
        final var config = ClprLedgerConfiguration.newBuilder()
                .protocolVersion(1)
                .chainId("eip155:1337")
                .serviceAddress(Bytes.wrap(new byte[] {0x12, 0x34}))
                .throttles(ClprThrottles.newBuilder().maxMessagesPerBundle(50).build())
                .build();
        final var stateReader = new StubStateReader(config);

        final var rpc = new StubRpcClient();
        rpc.setBlock("0x0", block("0x01", GENESIS_HASH, "0xc0", "0x65000000"));
        rpc.setBlock("finalized", block("0x100", ONE_HASH, "0xdeadbeef", "0x65111111"));
        // Encodes the inline-bytes layout of `_config.serviceAddress`:
        //   <20-byte address><11 zero bytes><0x28 = length*2 = 40>
        final String serviceAddressSlotValue = "0x" + "12".repeat(20) + "00".repeat(11) + "28";
        rpc.setProofResponse(proof(serviceAddressSlotValue));

        final var provider = new QbftLedgerConfigurationProvider(rpc, stateReader, CONTRACT_ADDRESS, 30_000L);

        final var response = provider.provide(CommitmentLevel.FINALIZED);
        assertThat(response.qbft()).isNotNull();
        final var qbft = response.qbft();

        // Block pinning: the contract read must use the exact hex block number resolved
        // from the symbolic tag, not the symbolic tag itself.
        assertThat(stateReader.lastBlockTag).isEqualTo("0x100");

        assertThat(qbft.ledgerConfiguration()).isEqualTo(config);
        assertThat(qbft.latestEpochBlockHeader()).isNotNull();
        assertThat(qbft.latestEpochBlockHeader().rlp().length()).isGreaterThan(0);
        assertThat(qbft.currentBlockHeader()).isNotNull();
        assertThat(qbft.currentBlockHeader().rlp().length()).isGreaterThan(0);
        assertThat(qbft.latestEpochBlockHeader().rlp())
                .isNotEqualTo(qbft.currentBlockHeader().rlp());

        // eth_getProof must have been issued for the contract account, against the resolved
        // block (not the symbolic tag), with the serviceAddress slot as the sole storage key.
        assertThat(rpc.proofCallCount).isEqualTo(1);
        assertThat(rpc.lastProofAddress).isEqualTo(CONTRACT_ADDRESS.toHexString());
        assertThat(rpc.lastProofStorageKeys).containsExactly(SERVICE_ADDRESS_SLOT_HEX);
        assertThat(rpc.lastProofBlockTag).isEqualTo("0x100");

        // Account proof: two RLP-encoded MPT nodes, parsed in order.
        assertThat(qbft.clprServiceAccountProof()).hasSize(2);
        assertThat(qbft.clprServiceAccountProof().get(0)).isEqualTo(Bytes.fromHex("f8518080"));

        // Storage proof: exactly one entry for the serviceAddress slot.
        assertThat(qbft.clprServiceStorageProofs()).hasSize(1);
        final var entry = qbft.clprServiceStorageProofs().getFirst();
        assertThat(entry.key()).isEqualTo(Bytes.fromHex("0".repeat(62) + "19"));
        assertThat(entry.value()).isEqualTo(Bytes.fromHex("12".repeat(20) + "00".repeat(11) + "28"));
        assertThat(entry.proof()).hasSize(2);
    }

    @Test
    void provide_pinsContractReadAndProofToExactCurrentBlockNumber() throws Exception {
        final var stateReader = new StubStateReader(ClprLedgerConfiguration.DEFAULT);

        final var rpc = new StubRpcClient();
        rpc.setBlock("0x0", block("0x01", GENESIS_HASH, "0xc0", "0x65000000"));
        // Symbolic "latest" tag resolves to block 0xabc.
        rpc.setBlock("latest", block("0xabc", ONE_HASH, "0xdeadbeef", "0x65111111"));
        rpc.setProofResponse(proof("0x00"));

        final var provider = new QbftLedgerConfigurationProvider(rpc, stateReader, CONTRACT_ADDRESS, 30_000L);

        provider.provide(CommitmentLevel.LATEST);

        // The provider must NOT pass "latest" to the contract read; it must pass the
        // resolved hex block number from the eth_getBlockByNumber result.
        assertThat(stateReader.lastBlockTag).isEqualTo("0xabc");
        // The eth_getProof call must also pin to the same resolved block number.
        assertThat(rpc.lastProofBlockTag).isEqualTo("0xabc");
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

    /**
     * Minimal {@code eth_getProof} response with one storage proof entry — matching the
     * single slot ({@code _config.serviceAddress}) this provider currently requests.
     */
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
                    { "key": "%s", "value": "%s", "proof": ["0xf8518080", "0xe2a0"] }
                  ]
                }
                """.formatted(ZERO_HASH, ONE_HASH, SERVICE_ADDRESS_SLOT_HEX, storageValue));
    }
}
