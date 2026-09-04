// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.jsonrpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hiero.clpr.relay.evm.ByteUtils.fromPrefixedHex;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.List;
import org.hiero.clpr.relay.evm.JsonRpcException;
import org.hiero.clpr.relay.evm.QbftBundleConstructor;
import org.hiero.clpr.relay.evm.model.BlockHeader;
import org.hiero.clpr.relay.evm.model.ProofResponse;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class EvmJsonRpcClientTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static final String ZERO_HASH = "0x" + "00".repeat(32);
    static final String ONE_HASH = "0x" + "00".repeat(31) + "01";
    static final Bytes ZERO_BYTES32 = fromPrefixedHex(ZERO_HASH);
    static final String MINER_ADDR = "0x" + "ab".repeat(20);

    // ------------------------------------------------------------------
    // buildRequest tests
    // ------------------------------------------------------------------

    @Test
    void buildRequest_ethCall() throws Exception {
        final String to = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
        final String data = "0x12345678";
        final String blockTag = "finalized";

        // Simulate the call-object approach used by ethCall
        final ObjectNode callObj = MAPPER.createObjectNode();
        callObj.put("to", to);
        callObj.put("data", data);

        final String json = EvmJsonRpcClient.buildRequest("eth_call", callObj, blockTag);
        final JsonNode root = MAPPER.readTree(json);

        assertThat(root.get("jsonrpc").asText()).isEqualTo("2.0");
        assertThat(root.get("method").asText()).isEqualTo("eth_call");
        assertThat(root.get("params").isArray()).isTrue();
        assertThat(root.get("params").size()).isEqualTo(2);

        final JsonNode firstParam = root.get("params").get(0);
        assertThat(firstParam.get("to").asText()).isEqualTo(to);
        assertThat(firstParam.get("data").asText()).isEqualTo(data);

        assertThat(root.get("params").get(1).asText()).isEqualTo(blockTag);
    }

    @Test
    void buildRequest_simpleMethod_noParams() throws Exception {
        final String json = EvmJsonRpcClient.buildRequest("eth_blockNumber");
        final JsonNode root = MAPPER.readTree(json);

        assertThat(root.get("jsonrpc").asText()).isEqualTo("2.0");
        assertThat(root.get("method").asText()).isEqualTo("eth_blockNumber");
        assertThat(root.get("id").isNumber()).isTrue();

        final JsonNode params = root.get("params");
        assertThat(params.isArray()).isTrue();
        assertThat(params.size()).isEqualTo(0);
    }

    @Test
    void buildRequest_withBooleanParam() throws Exception {
        final String json = EvmJsonRpcClient.buildRequest("eth_getBlockByNumber", "latest", true);
        final JsonNode root = MAPPER.readTree(json);

        final JsonNode params = root.get("params");
        assertThat(params.size()).isEqualTo(2);
        assertThat(params.get(0).asText()).isEqualTo("latest");
        assertThat(params.get(1).asBoolean()).isTrue();
    }

    // ------------------------------------------------------------------
    // parseHexLong tests
    // ------------------------------------------------------------------

    @Test
    void parseHexLong_typicalValue() throws Exception {
        assertThat(EvmJsonRpcClient.parseHexLong("0x1a")).isEqualTo(26L);
    }

    @Test
    void parseHexLong_zero() {
        assertThat(EvmJsonRpcClient.parseHexLong("0x0")).isEqualTo(0L);
    }

    @Test
    void parseHexLong_largeValue() {
        // 0x100 = 256
        assertThat(EvmJsonRpcClient.parseHexLong("0x100")).isEqualTo(256L);
    }

    @Test
    void parseHexLong_noPrefix() {
        assertThat(EvmJsonRpcClient.parseHexLong("1a")).isEqualTo(26L);
    }

    // ------------------------------------------------------------------
    // JsonRpcException tests
    // ------------------------------------------------------------------

    @Test
    void jsonRpcException_storesCodeAndMessage() {
        final JsonRpcException ex = new JsonRpcException(-32601, "Method not found");
        assertThat(ex.code()).isEqualTo(-32601);
        assertThat(ex.getMessage()).contains("Method not found");
        assertThat(ex.getMessage()).contains("-32601");
    }

    @Test
    void buildRequest_ethSendRawTransaction() throws Exception {
        final String signedTx = "0xf86b...";
        final String json = EvmJsonRpcClient.buildRequest("eth_sendRawTransaction", signedTx);
        final JsonNode root = MAPPER.readTree(json);

        assertThat(root.get("method").asText()).isEqualTo("eth_sendRawTransaction");
        assertThat(root.get("params").get(0).asText()).isEqualTo(signedTx);
    }

    // ── accountProofFromJson ──────────────────────────────────────────────────

    @Nested
    class AccountProofFromJson {

        @Test
        void parsesTwoNodes() throws Exception {
            final List<Bytes> nodes = EvmJsonRpcClient.accountProofFromJson(minimalProof("0x00"));

            assertThat(nodes).hasSize(2);
            assertThat(nodes.get(0)).isEqualTo(Bytes.fromHex("f8518080"));
        }
    }

    // ── blockHeaderFromJson ───────────────────────────────────────────────────

    @Nested
    class BlockHeaderFromJson {

        @Test
        void parsesAllMandatoryFields() throws Exception {
            final BlockHeader h = EvmJsonRpcClient.blockHeaderFromJson(minimalBlock("0xdeadbeef"));

            assertThat(h.parentHash()).isEqualTo(ZERO_BYTES32);
            assertThat(h.number()).isEqualTo(BigInteger.ONE);
            assertThat(h.gasLimit()).isEqualTo(new BigInteger("1000", 16));
            assertThat(h.blockHash()).isEqualTo(fromPrefixedHex(ONE_HASH));
        }

        @Test
        void parsesBaseFeePerGas() throws Exception {
            assertThat(EvmJsonRpcClient.blockHeaderFromJson(minimalBlock("0x")).baseFeePerGas())
                    .isEqualTo(BigInteger.valueOf(7));
        }

        @Test
        void nullForMissingPostForkFields() throws Exception {
            final BlockHeader h = EvmJsonRpcClient.blockHeaderFromJson(minimalBlock("0x"));

            assertThat(h.withdrawalsRoot()).isNull();
            assertThat(h.parentBeaconBlockRoot()).isNull();
        }

        @Test
        void handlesEmptyExtraData() throws Exception {
            assertThat(EvmJsonRpcClient.blockHeaderFromJson(minimalBlock("0x"))
                            .extraData()
                            .toByteArray())
                    .isEmpty();
        }

        @Test
        void missingMandatoryFieldNamesTheFieldInError() throws Exception {
            final ObjectNode block = (ObjectNode) minimalBlock("0x");
            block.remove("stateRoot");

            assertThatThrownBy(() -> EvmJsonRpcClient.blockHeaderFromJson(block))
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("stateRoot");
        }

        @Test
        void nullMandatoryFieldNamesTheFieldInError() throws Exception {
            final ObjectNode block = (ObjectNode) minimalBlock("0x");
            block.putNull("parentHash");

            assertThatThrownBy(() -> EvmJsonRpcClient.blockHeaderFromJson(block))
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("parentHash");
        }
    }

    // ── storageProofFromJson ──────────────────────────────────────────────────

    @Nested
    class StorageProofFromJson {

        @Test
        void parsesFirstEntry() throws Exception {
            // {@link #minimalProof} now returns five entries (one for the message running hash
            // plus four for the Channel-struct fields). The first entry carries the value
            // passed in; we only assert on that one here.
            final List<ProofResponse.StorageProofEntry> entries =
                    EvmJsonRpcClient.storageProofFromJson(minimalProof("0xabcd"));

            assertThat(entries).hasSize(5);
            assertThat(entries.get(0).key()).isEqualTo(fromPrefixedHex(ZERO_HASH));
            assertThat(entries.get(0).value()).isEqualTo(Bytes.fromHex("abcd"));
            assertThat(entries.get(0).proof()).hasSize(2);
        }

        @Test
        void zeroValue() throws Exception {
            assertThat(EvmJsonRpcClient.storageProofFromJson(minimalProof("0x00"))
                            .get(0)
                            .value())
                    .isEqualTo(Bytes.fromHex("00"));
        }
    }

    // ── eth_getLogs / ChannelCompleted decoding ───────────────────────────

    @Nested
    class GetLogsDecoding {

        /** topics[0] = keccak256("ChannelCompleted(bytes32,string,bytes,address,bytes32)"). */
        static final String CHANNEL_COMPLETED_TOPIC0 = "0x"
                + org.hiero.clpr.relay.evm.AbiCodec.toHexNoPrefix(org.hiero.clpr.relay.evm.AbiCodec.Keccak256.keccak256(
                        "ChannelCompleted(bytes32,string,bytes,address,bytes32)".getBytes()));

        @Test
        void buildRequest_ethGetLogs_rangeAndTopic() throws Exception {
            final com.fasterxml.jackson.databind.node.ObjectNode filter = MAPPER.createObjectNode();
            filter.put("fromBlock", "0x5");
            filter.put("toBlock", "0xa");
            filter.put("address", "0xabc");
            final var topics = MAPPER.createArrayNode();
            topics.add(CHANNEL_COMPLETED_TOPIC0);
            filter.set("topics", topics);

            final String json = EvmJsonRpcClient.buildRequest("eth_getLogs", filter);
            final JsonNode root = MAPPER.readTree(json);

            assertThat(root.get("method").asText()).isEqualTo("eth_getLogs");
            final JsonNode param = root.get("params").get(0);
            assertThat(param.get("fromBlock").asText()).isEqualTo("0x5");
            assertThat(param.get("toBlock").asText()).isEqualTo("0xa");
            assertThat(param.get("topics").get(0).asText()).isEqualTo(CHANNEL_COMPLETED_TOPIC0);
        }

        @Test
        void logsFromJson_parsesTopicsDataAndBlock() throws Exception {
            final JsonNode result = MAPPER.readTree("""
                    [
                      {
                        "address": "0xabc",
                        "topics": ["%s", "%s"],
                        "data": "0x1234",
                        "blockNumber": "0x10"
                      }
                    ]
                    """.formatted(CHANNEL_COMPLETED_TOPIC0, ONE_HASH));

            final var entries = EvmJsonRpcClient.logsFromJson(result);

            assertThat(entries).hasSize(1);
            assertThat(entries.get(0).topics()).containsExactly(CHANNEL_COMPLETED_TOPIC0, ONE_HASH);
            assertThat(entries.get(0).data()).isEqualTo("0x1234");
            assertThat(entries.get(0).blockNumber()).isEqualTo(16L);
        }

        @Test
        void logsFromJson_emptyOrNonArray() throws Exception {
            assertThat(EvmJsonRpcClient.logsFromJson(MAPPER.readTree("[]"))).isEmpty();
            assertThat(EvmJsonRpcClient.logsFromJson(MAPPER.readTree("null"))).isEmpty();
        }

        @Test
        void decodeChannelCompletedChannelIds_extractsTopic1() throws Exception {
            final JsonNode result = MAPPER.readTree(
                    """
                    [
                      { "topics": ["%s", "%s"], "data": "0x", "blockNumber": "0x1" },
                      { "topics": ["%s", "%s"], "data": "0x", "blockNumber": "0x2" }
                    ]
                    """.formatted(CHANNEL_COMPLETED_TOPIC0, ONE_HASH, CHANNEL_COMPLETED_TOPIC0, ZERO_HASH));

            final var ids = EvmJsonRpcClient.decodeChannelCompletedChannelIds(EvmJsonRpcClient.logsFromJson(result));

            // channelId is the indexed bytes32 at topics[1], in log order.
            assertThat(ids).containsExactly(fromPrefixedHex(ONE_HASH), fromPrefixedHex(ZERO_HASH));
        }

        @Test
        void decodeChannelCompletedChannelIds_skipsMalformedEntries() throws Exception {
            // A log with only topic0 (no indexed channelId) is skipped, not an error.
            final JsonNode result =
                    MAPPER.readTree("""
                    [
                      { "topics": ["%s"], "data": "0x", "blockNumber": "0x1" },
                      { "topics": ["%s", "%s"], "data": "0x", "blockNumber": "0x2" }
                    ]
                    """.formatted(CHANNEL_COMPLETED_TOPIC0, CHANNEL_COMPLETED_TOPIC0, ONE_HASH));

            final var ids = EvmJsonRpcClient.decodeChannelCompletedChannelIds(EvmJsonRpcClient.logsFromJson(result));

            assertThat(ids).containsExactly(fromPrefixedHex(ONE_HASH));
        }
    }

    // ── JSON fixtures ─────────────────────────────────────────────────────────

    static JsonNode minimalBlock(String extraData) throws Exception {
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
              "number":                "0x01",
              "gasLimit":              "0x1000",
              "gasUsed":               "0x800",
              "timestamp":             "0x65000000",
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
                        MINER_ADDR,
                        ZERO_HASH,
                        ZERO_HASH,
                        ZERO_HASH,
                        "00".repeat(256),
                        extraData,
                        ZERO_HASH,
                        ONE_HASH));
    }

    /**
     * Build a minimal {@code eth_getProof} response with five storage entries — matching
     * the number of slots {@link QbftBundleConstructor#onStateChanged} now requests
     * (one for the last-message running hash plus four for the Channel-struct fields
     * backing the ClprQueueMetadata). The first entry carries {@code storageValue}, which
     * is what the constructor validates against the queued message's running hash; the
     * remaining four use dummy keys and {@link #ZERO_HASH} as a placeholder value.
     */
    static JsonNode minimalProof(String storageValue) throws Exception {
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
            { "key": "%s", "value": "%s", "proof": ["0xf8518080", "0xe2a0"] },
            { "key": "%s", "value": "%s", "proof": ["0xf8518080", "0xe2a0"] },
            { "key": "%s", "value": "%s", "proof": ["0xf8518080", "0xe2a0"] },
            { "key": "%s", "value": "%s", "proof": ["0xf8518080", "0xe2a0"] },
            { "key": "%s", "value": "%s", "proof": ["0xf8518080", "0xe2a0"] }
          ]
        }
        """.formatted(
                        ZERO_HASH,
                        ONE_HASH,
                        // entry 0: last-message running hash (validated against the queued message)
                        ZERO_HASH,
                        storageValue,
                        // entry 1: channel slot+1 (verifier | status | nextMessageId)
                        ONE_HASH,
                        ZERO_HASH,
                        // entry 2: channel slot+2 (ackedMessageId | receivedMessageId | nextExpectedReplyId)
                        ONE_HASH,
                        ZERO_HASH,
                        // entry 3: channel slot+4 (sentRunningHash)
                        ONE_HASH,
                        ZERO_HASH,
                        // entry 4: channel slot+5 (receivedRunningHash)
                        ONE_HASH,
                        ZERO_HASH));
    }
}
