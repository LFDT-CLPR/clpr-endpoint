// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.hapi.node.state.clpr.ClprThrottles;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.evm.beacon.BeaconApiClient;
import org.hiero.clpr.relay.evm.beacon.BeaconBlockHeader;
import org.hiero.clpr.relay.evm.beacon.ExecutionPayloadHeader;
import org.hiero.clpr.relay.evm.beacon.LightClientBootstrap;
import org.hiero.clpr.relay.evm.beacon.LightClientHeader;
import org.hiero.clpr.relay.evm.beacon.SyncCommittee;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.model.BlockHeader;
import org.hiero.clpr.relay.evm.ssz.Ssz;
import org.hiero.clpr.relay.evm.testfixtures.TestEvmJsonRpcClient;
import org.junit.jupiter.api.Test;

class EthereumPosLedgerConfigurationProviderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String ZERO_HASH = "0x" + "00".repeat(32);
    private static final String ONE_HASH = "0x" + "00".repeat(31) + "01";
    private static final String MINER = "0x" + "ab".repeat(20);

    private static final Bytes GENESIS_VALIDATORS_ROOT = bytes(32, 0x4B);
    private static final Bytes FORK_VERSION = Bytes.wrap(new byte[] {0x05, 0x00, 0x00, 0x00});
    private static final Bytes AGGREGATE_PUBKEY = bytes(48, 0x77);
    private static final String FINALIZED_ROOT_HEX = "0x" + "ee".repeat(32);
    private static final long BOOTSTRAP_SLOT = 7_000_000L;

    private static Bytes bytes(final int len, final int seed) {
        final byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[i] = (byte) (seed + i * 31);
        }
        return Bytes.wrap(b);
    }

    private static SyncCommittee sampleCommittee() {
        final List<Bytes> pubkeys = new ArrayList<>(Ssz.SYNC_COMMITTEE_SIZE);
        for (int i = 0; i < Ssz.SYNC_COMMITTEE_SIZE; i++) {
            pubkeys.add(bytes(48, i));
        }
        return new SyncCommittee(pubkeys, AGGREGATE_PUBKEY);
    }

    private static final class StubRpcClient extends TestEvmJsonRpcClient {
        private final Map<String, BlockHeader> headersByTag = new HashMap<>();

        void setBlock(final String tag, final JsonNode block) {
            headersByTag.put(tag, EvmJsonRpcClient.blockHeaderFromJson(block));
        }

        @Override
        public BlockHeader ethGetBlockHeaderByNumber(final String blockTag) {
            final BlockHeader h = headersByTag.get(blockTag);
            if (h == null) {
                throw new AssertionError("unexpected block tag requested: " + blockTag);
            }
            return h;
        }
    }

    private static final class StubStateReader implements ContractStateReader {
        private final ClprLedgerConfiguration config;
        String lastBlockTag;

        StubStateReader(final ClprLedgerConfiguration config) {
            this.config = config;
        }

        @Override
        public Optional<ClprChannel> readChannelState(final Bytes connectionId, final String blockTag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<QueuedMessage> readQueuedMessages(
                final Bytes connectionId, final long fromId, final long toId, final String blockTag) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ClprLedgerConfiguration readLedgerConfiguration(final CommitmentLevel commitmentLevel) {
            throw new AssertionError("provider must use the string-tag overload to pin the read to an exact block");
        }

        @Override
        public ClprLedgerConfiguration readLedgerConfiguration(final String blockTag) {
            this.lastBlockTag = blockTag;
            return config;
        }
    }

    private static final class StubBeaconClient extends BeaconApiClient {
        private final SyncCommittee committee;
        String lastBootstrapRoot;

        StubBeaconClient(final SyncCommittee committee) {
            super("http://localhost:5052", 0, Duration.ofSeconds(1));
            this.committee = committee;
        }

        @Override
        public String getFinalizedBlockRootHex() {
            return FINALIZED_ROOT_HEX;
        }

        @Override
        public LightClientBootstrap getBootstrap(final String blockRootHex) {
            this.lastBootstrapRoot = blockRootHex;
            // The provider only reads currentSyncCommittee; the header is a placeholder.
            final var beacon = new BeaconBlockHeader(BOOTSTRAP_SLOT, 0L, bytes(32, 1), bytes(32, 2), bytes(32, 3));
            final var eph = new ExecutionPayloadHeader(
                    bytes(32, 1),
                    bytes(20, 2),
                    bytes(32, 3),
                    bytes(32, 4),
                    bytes(256, 5),
                    bytes(32, 6),
                    1L,
                    1L,
                    1L,
                    1L,
                    bytes(4, 7),
                    BigInteger.ONE,
                    bytes(32, 8),
                    bytes(32, 9),
                    bytes(32, 10),
                    1L,
                    1L);
            final var header =
                    new LightClientHeader(beacon, eph, List.of(bytes(32, 0), bytes(32, 0), bytes(32, 0), bytes(32, 0)));
            return new LightClientBootstrap(header, committee, List.of(bytes(32, 0)));
        }

        @Override
        public Bytes getGenesisValidatorsRoot() {
            return GENESIS_VALIDATORS_ROOT;
        }

        @Override
        public Bytes getCurrentForkVersion() {
            return FORK_VERSION;
        }
    }

    @Test
    void provide_assemblesPayloadWithSyncCommitteeAndChainPinningFields() throws Exception {
        final var config = ClprLedgerConfiguration.newBuilder()
                .protocolVersion(1)
                .chainId("eip155:1")
                .serviceAddress(Bytes.wrap(new byte[] {0x12, 0x34}))
                .throttles(ClprThrottles.newBuilder().maxMessagesPerBundle(50).build())
                .build();
        final var stateReader = new StubStateReader(config);

        final var rpc = new StubRpcClient();
        rpc.setBlock("finalized", block("0x100", ONE_HASH, "0xdeadbeef", "0x65111111")); // 0x100 = 256

        final var committee = sampleCommittee();
        final var beacon = new StubBeaconClient(committee);

        final var provider = new EthereumPosLedgerConfigurationProvider(rpc, stateReader, beacon);
        final var ethPos = provider.provide(CommitmentLevel.FINALIZED).ethereumPos();

        // Block pinning: contract read must use the exact resolved hex number, not the symbolic tag.
        assertThat(stateReader.lastBlockTag).isEqualTo("0x100");
        // Bootstrap must be fetched at the finalized block root the beacon node reports.
        assertThat(beacon.lastBootstrapRoot).isEqualTo(FINALIZED_ROOT_HEX);

        assertThat(ethPos.ledgerConfiguration()).isEqualTo(config);
        assertThat(ethPos.genesisValidatorsRoot()).isEqualTo(GENESIS_VALIDATORS_ROOT);
        assertThat(ethPos.forkVersion()).isEqualTo(FORK_VERSION);
        assertThat(ethPos.slot())
                .as("slot the committee is current for (pins the period)")
                .isEqualTo(BOOTSTRAP_SLOT);
        assertThat(ethPos.syncCommittee().aggregatePubkey()).isEqualTo(AGGREGATE_PUBKEY);
        assertThat(ethPos.syncCommittee().pubkeys()).hasSize(Ssz.SYNC_COMMITTEE_SIZE);
        assertThat(ethPos.syncCommittee().pubkeys()).isEqualTo(committee.pubkeys());
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
}
