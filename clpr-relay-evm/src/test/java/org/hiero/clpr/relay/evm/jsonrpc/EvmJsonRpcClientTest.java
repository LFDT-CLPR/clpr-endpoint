// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.jsonrpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hiero.clpr.relay.evm.ByteUtils.fromPrefixedHex;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import org.hiero.clpr.relay.evm.JsonRpcException;
import org.hiero.clpr.relay.evm.QbftBundleConstructor;
import org.hiero.clpr.relay.evm.model.BlockHeader;
import org.hiero.clpr.relay.evm.model.ProofResponse;
import org.junit.jupiter.api.AfterEach;
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

    // ── send() retry + parseResponse() error decoding ─────────────────────────
    //
    // These drive the real HTTP transport against a loopback com.sun.net.httpserver
    // instance so the retry state machine (429/503 retry-then-fail, IOException retry)
    // and the JSON-RPC error-response decoding run end to end — no mocking.

    @Nested
    class SendRetryAndParse {

        private static final String RESULT_0X10 = "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0x10\"}";

        private HttpServer server;

        @AfterEach
        void stopServer() {
            if (server != null) {
                server.stop(0);
                server = null;
            }
        }

        /** A single canned HTTP response: status line + JSON body. */
        private record Canned(int status, String body) {}

        /** Start a loopback server that serves {@code responses} in order (200/result once drained). */
        private int startServer(final Canned... responses) throws IOException {
            final Queue<Canned> queue = new ArrayDeque<>(List.of(responses));
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                exchange.getRequestBody().readAllBytes();
                final Canned c = queue.poll();
                final Canned resp = c != null ? c : new Canned(200, RESULT_0X10);
                final byte[] body = resp.body().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(resp.status(), body.length);
                try (var os = exchange.getResponseBody()) {
                    os.write(body);
                }
            });
            server.start();
            return server.getAddress().getPort();
        }

        private EvmJsonRpcClient clientFor(final int port, final int maxRetries) {
            return new EvmJsonRpcClient("http://127.0.0.1:" + port, maxRetries, Duration.ofSeconds(5));
        }

        @Test
        void send_nonRetryableHttpError_throws() throws IOException {
            final int port = startServer(new Canned(400, "{}"));
            assertThatThrownBy(() -> clientFor(port, 0).ethBlockNumber())
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("HTTP error 400");
        }

        @Test
        void send_rateLimitedThenSucceeds_returnsResult() throws IOException {
            // First attempt gets 429 (retryable), second attempt gets the real result.
            final int port = startServer(new Canned(429, "{}"), new Canned(200, RESULT_0X10));
            assertThat(clientFor(port, 2).ethBlockNumber()).isEqualTo(16L);
        }

        @Test
        void send_serviceUnavailable_exhaustsRetries_throws() throws IOException {
            final int port = startServer(new Canned(503, "{}"), new Canned(503, "{}"));
            assertThatThrownBy(() -> clientFor(port, 1).ethBlockNumber())
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("HTTP 503 after 1 retries");
        }

        @Test
        void send_connectionRefused_exhaustsRetries_throws() throws IOException {
            // Grab a free port, then close it so connections are refused.
            final int deadPort;
            try (ServerSocket s = new ServerSocket(0)) {
                deadPort = s.getLocalPort();
            }
            assertThatThrownBy(() -> clientFor(deadPort, 1).ethBlockNumber())
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("Channel failed after 1 retries");
        }

        @Test
        void parseResponse_invalidJson_throws() throws IOException {
            final int port = startServer(new Canned(200, "{\"oops\""));
            assertThatThrownBy(() -> clientFor(port, 0).ethBlockNumber())
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("-32700")
                    .hasMessageContaining("Invalid JSON in response");
        }

        @Test
        void parseResponse_errorNode_throwsWithCodeMessageAndData() throws IOException {
            final int port = startServer(new Canned(
                    200,
                    "{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":"
                            + "{\"code\":-32000,\"message\":\"execution reverted\",\"data\":\"0xdead\"}}"));
            assertThatThrownBy(() -> clientFor(port, 0).ethBlockNumber())
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("-32000")
                    .hasMessageContaining("execution reverted")
                    .hasMessageContaining("0xdead");
        }

        @Test
        void parseResponse_errorNode_usesDefaultsWhenFieldsMissing() throws IOException {
            final int port = startServer(new Canned(200, "{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{}}"));
            assertThatThrownBy(() -> clientFor(port, 0).ethBlockNumber())
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("-32000")
                    .hasMessageContaining("unknown error");
        }

        @Test
        void parseResponse_neitherResultNorError_throws() throws IOException {
            final int port = startServer(new Canned(200, "{\"jsonrpc\":\"2.0\",\"id\":1}"));
            assertThatThrownBy(() -> clientFor(port, 0).ethBlockNumber())
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("-32603")
                    .hasMessageContaining("neither 'result' nor 'error'");
        }

        // ---- public read/write methods driven through the real transport ----

        private String resultBody(final String resultJson) {
            return "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":" + resultJson + "}";
        }

        @Test
        void ethCall_returnsResultString() throws IOException {
            final int port = startServer(new Canned(200, resultBody("\"0xabcd\"")));
            assertThat(clientFor(port, 0).ethCall("0xto", "0xdata", "latest")).isEqualTo("0xabcd");
        }

        @Test
        void ethCallFrom_returnsResultString() throws IOException {
            final int port = startServer(new Canned(200, resultBody("\"0xbeef\"")));
            assertThat(clientFor(port, 0).ethCallFrom("0xfrom", "0xto", "0xdata", "latest"))
                    .isEqualTo("0xbeef");
        }

        @Test
        void ethSendRawTransaction_returnsTxHash() throws IOException {
            final int port = startServer(new Canned(200, resultBody("\"0xdeadbeef\"")));
            assertThat(clientFor(port, 0).ethSendRawTransaction("0xf86b")).isEqualTo("0xdeadbeef");
        }

        @Test
        void ethGetTransactionCount_parsesHexLong() throws IOException {
            final int port = startServer(new Canned(200, resultBody("\"0x5\"")));
            assertThat(clientFor(port, 0).ethGetTransactionCount("0xaddr", "pending"))
                    .isEqualTo(5L);
        }

        @Test
        void ethGetBalance_parsesHexBigInteger() throws IOException {
            final int port = startServer(new Canned(200, resultBody("\"0x64\"")));
            assertThat(clientFor(port, 0).ethGetBalance("0xaddr", "latest")).isEqualTo(BigInteger.valueOf(100));
        }

        @Test
        void ethGetTransactionReceipt_minedTx_returnsNode() throws IOException {
            final int port = startServer(new Canned(200, resultBody("{\"status\":\"0x1\"}")));
            final var receipt = clientFor(port, 0).ethGetTransactionReceipt("0xhash");
            assertThat(receipt).isNotNull();
            assertThat(receipt.get("status").asText()).isEqualTo("0x1");
        }

        @Test
        void ethGetTransactionReceipt_unminedTx_returnsNull() throws IOException {
            final int port = startServer(new Canned(200, resultBody("null")));
            assertThat(clientFor(port, 0).ethGetTransactionReceipt("0xhash")).isNull();
        }

        @Test
        void ethGetBlockHeaderByNumber_decodesHeader() throws Exception {
            final int port = startServer(new Canned(200, resultBody(MAPPER.writeValueAsString(minimalBlock("0x")))));
            final BlockHeader header = clientFor(port, 0).ethGetBlockHeaderByNumber("latest");
            assertThat(header.number()).isEqualTo(BigInteger.ONE);
            assertThat(header.baseFeePerGas()).isEqualTo(BigInteger.valueOf(7));
        }

        @Test
        void ethGetProof_decodesAccountAndStorageProofs() throws Exception {
            final int port =
                    startServer(new Canned(200, resultBody(MAPPER.writeValueAsString(minimalProof("0xabcd")))));
            final ProofResponse proof = clientFor(port, 0).ethGetProof("0xaddr", new String[] {}, "latest");
            assertThat(proof.accountProof()).hasSize(2);
            assertThat(proof.storageProof()).hasSize(5);
        }

        @Test
        void ethGetLogs_decodesLogEntries() throws IOException {
            final String logsArray = "[{\"topics\":[\"0x01\"],\"data\":\"0x1234\",\"blockNumber\":\"0x10\"}]";
            final int port = startServer(new Canned(200, resultBody(logsArray)));
            final var logs = clientFor(port, 0).ethGetLogs("0xaddr", null, 1L, 2L);
            assertThat(logs).hasSize(1);
            assertThat(logs.get(0).blockNumber()).isEqualTo(16L);
            assertThat(logs.get(0).data()).isEqualTo("0x1234");
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
