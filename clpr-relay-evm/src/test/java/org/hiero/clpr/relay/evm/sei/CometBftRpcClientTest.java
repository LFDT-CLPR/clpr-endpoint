// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.sei;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.hiero.clpr.relay.evm.JsonRpcException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CometBftRpcClientTest {

    @Test
    void getSignedHeader_decodesCometBftHexHeaderFields() throws Exception {
        final var lastBlockHash = "11".repeat(32);
        final var lastBlockPartHash = "22".repeat(32);
        final var lastCommitHash = "33".repeat(32);
        final var dataHash = "44".repeat(32);
        final var validatorsHash = "55".repeat(32);
        final var nextValidatorsHash = "66".repeat(32);
        final var consensusHash = "77".repeat(32);
        final var appHash = "88".repeat(32);
        final var lastResultsHash = "99".repeat(32);
        final var evidenceHash = "aa".repeat(32);
        final var proposerAddress = "bb".repeat(20);
        final var commitPartHash = "cc".repeat(32);
        final var signature = Bytes.fromHex("dd".repeat(64));
        final var signatureBase64 = Base64.getEncoder().encodeToString(signature.toByteArray());
        final var responseBody = """
                {"jsonrpc":"2.0","id":1,"result":{"signed_header":{"header":{
                  "version":{"block":"11","app":"0"},
                  "chain_id":"sei",
                  "height":"5",
                  "time":"2026-06-22T15:21:13.879944805Z",
                  "last_block_id":{"hash":"%s","parts":{"total":1,"hash":"%s"}},
                  "last_commit_hash":"%s",
                  "data_hash":"%s",
                  "validators_hash":"%s",
                  "next_validators_hash":"%s",
                  "consensus_hash":"%s",
                  "app_hash":"%s",
                  "last_results_hash":"%s",
                  "evidence_hash":"%s",
                  "proposer_address":"%s"
                },"commit":{
                  "round":0,
                  "block_id":{"parts":{"total":1,"hash":"%s"}},
                  "signatures":[{"block_id_flag":2,"timestamp":"2026-06-22T15:21:13.826870805Z","signature":"%s"}]
                }}}}
                """.formatted(
                        lastBlockHash,
                        lastBlockPartHash,
                        lastCommitHash,
                        dataHash,
                        validatorsHash,
                        nextValidatorsHash,
                        consensusHash,
                        appHash,
                        lastResultsHash,
                        evidenceHash,
                        proposerAddress,
                        commitPartHash,
                        signatureBase64);

        try (final var server = new OneShotJsonServer(responseBody)) {
            final var client = new CometBftRpcClient(server.url(), 0, Duration.ofSeconds(2));

            final var signedHeader = client.getSignedHeader(5);
            final var header = signedHeader.header();
            final var commit = signedHeader.commit();

            assertThat(header.validatorsHash()).isEqualTo(Bytes.fromHex(validatorsHash));
            assertThat(header.nextValidatorsHash()).isEqualTo(Bytes.fromHex(nextValidatorsHash));
            assertThat(header.appHash()).isEqualTo(Bytes.fromHex(appHash));
            assertThat(header.lastBlockId().hash()).isEqualTo(Bytes.fromHex(lastBlockHash));
            assertThat(header.lastBlockId().partSetHash()).isEqualTo(Bytes.fromHex(lastBlockPartHash));
            assertThat(header.lastCommitHash()).isEqualTo(Bytes.fromHex(lastCommitHash));
            assertThat(header.dataHash()).isEqualTo(Bytes.fromHex(dataHash));
            assertThat(header.consensusHash()).isEqualTo(Bytes.fromHex(consensusHash));
            assertThat(header.lastResultsHash()).isEqualTo(Bytes.fromHex(lastResultsHash));
            assertThat(header.evidenceHash()).isEqualTo(Bytes.fromHex(evidenceHash));
            assertThat(header.proposerAddress()).isEqualTo(Bytes.fromHex(proposerAddress));
            assertThat(commit.partSetHash()).isEqualTo(Bytes.fromHex(commitPartHash));
            assertThat(commit.signersBits()).isEqualTo(Bytes.fromHex("80"));
            assertThat(commit.signatures()).singleElement().satisfies(sig -> assertThat(sig.signature())
                    .isEqualTo(signature));
        }
    }

    // ── rpc() transport retry + result/error decoding, plus domain-level failures ──
    //
    // Drives the real HTTP transport against a loopback com.sun.net.httpserver so the
    // retry state machine (429/503, connection-refused) and the JSON-RPC + ABCI error
    // decoding run end to end — no mocking.
    @Nested
    class TransportAndErrors {

        private HttpServer server;

        @AfterEach
        void stopServer() {
            if (server != null) {
                server.stop(0);
                server = null;
            }
        }

        private record Canned(int status, String body) {}

        private String startServerUrl(final Canned... responses) throws IOException {
            final Queue<Canned> queue = new ArrayDeque<>(List.of(responses));
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                exchange.getRequestBody().readAllBytes();
                final Canned c = queue.poll();
                final Canned resp = c != null ? c : new Canned(200, "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}");
                final byte[] body = resp.body().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(resp.status(), body.length);
                try (var os = exchange.getResponseBody()) {
                    os.write(body);
                }
            });
            server.start();
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        private CometBftRpcClient clientFor(final String url, final int maxRetries) {
            return new CometBftRpcClient(url, maxRetries, Duration.ofSeconds(5));
        }

        // ---- transport ----

        @Test
        void rpc_nonRetryableHttpError_throws() throws IOException {
            final var url = startServerUrl(new Canned(400, "{}"));
            assertThatThrownBy(() -> clientFor(url, 0).getValidatorSet(1))
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("HTTP error 400");
        }

        @Test
        void rpc_rateLimitedThenSucceeds_returnsResult() throws IOException {
            final var url = startServerUrl(
                    new Canned(429, "{}"),
                    new Canned(200, "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"validators\":[],\"total\":\"0\"}}"));
            assertThat(clientFor(url, 2).getValidatorSet(1).validators()).isEmpty();
        }

        @Test
        void rpc_serviceUnavailable_exhaustsRetries_throws() throws IOException {
            final var url = startServerUrl(new Canned(503, "{}"), new Canned(503, "{}"));
            assertThatThrownBy(() -> clientFor(url, 1).getValidatorSet(1))
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("HTTP 503 after 1 retries");
        }

        @Test
        void rpc_connectionRefused_exhaustsRetries_throws() throws IOException {
            final int deadPort;
            try (ServerSocket s = new ServerSocket(0)) {
                deadPort = s.getLocalPort();
            }
            assertThatThrownBy(
                            () -> clientFor("http://127.0.0.1:" + deadPort, 1).getValidatorSet(1))
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("CometBFT RPC failed after 1 retries");
        }

        @Test
        void rpc_invalidJson_throws() throws IOException {
            final var url = startServerUrl(new Canned(200, "{bad"));
            assertThatThrownBy(() -> clientFor(url, 0).getValidatorSet(1))
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("-32700")
                    .hasMessageContaining("Invalid JSON in CometBFT response");
        }

        @Test
        void rpc_errorNode_throws() throws IOException {
            final var url = startServerUrl(new Canned(
                    200,
                    "{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32601,\"message\":\"method not found\"}}"));
            assertThatThrownBy(() -> clientFor(url, 0).getValidatorSet(1))
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("-32601")
                    .hasMessageContaining("method not found");
        }

        @Test
        void rpc_errorNode_usesDefaultsWhenFieldsMissing() throws IOException {
            final var url = startServerUrl(new Canned(200, "{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{}}"));
            assertThatThrownBy(() -> clientFor(url, 0).getValidatorSet(1))
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("unknown CometBFT RPC error");
        }

        @Test
        void rpc_noResultField_throws() throws IOException {
            final var url = startServerUrl(new Canned(200, "{\"jsonrpc\":\"2.0\",\"id\":1}"));
            assertThatThrownBy(() -> clientFor(url, 0).getValidatorSet(1))
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("returned no 'result' field");
        }

        // ---- getValidatorSet domain rules ----

        @Test
        void getValidatorSet_exceedsSinglePage_throws() throws IOException {
            final var url = startServerUrl(new Canned(
                    200, "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"validators\":[{}],\"total\":\"101\"}}"));
            assertThatThrownBy(() -> clientFor(url, 0).getValidatorSet(9))
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("exceeding the single-page limit");
        }

        @Test
        void getValidatorSet_nonEd25519Key_throws() throws IOException {
            final var body = """
                    {"jsonrpc":"2.0","id":1,"result":{"total":"1","validators":[
                      {"pub_key":{"type":"tendermint/PubKeySecp256k1","value":"%s"},"voting_power":"10"}
                    ]}}
                    """.formatted(Base64.getEncoder().encodeToString(new byte[33]));
            final var url = startServerUrl(new Canned(200, body));
            assertThatThrownBy(() -> clientFor(url, 0).getValidatorSet(9))
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("unsupported public key type");
        }

        @Test
        void getValidatorSet_ed25519_parsesEntry() throws IOException {
            final var pubKey = new byte[32];
            Arrays.fill(pubKey, (byte) 0x07);
            final var body = """
                    {"jsonrpc":"2.0","id":1,"result":{"total":"1","validators":[
                      {"pub_key":{"type":"tendermint/PubKeyEd25519","value":"%s"},"voting_power":"10"}
                    ]}}
                    """.formatted(Base64.getEncoder().encodeToString(pubKey));
            final var url = startServerUrl(new Canned(200, body));
            final var set = clientFor(url, 0).getValidatorSet(9);
            assertThat(set.validators()).singleElement().satisfies(v -> {
                assertThat(v.votingPower()).isEqualTo(10L);
                assertThat(v.ed25519PubKey()).isEqualTo(Bytes.wrap(pubKey));
            });
        }

        // ---- abciQuery domain rules ----

        @Test
        void abciQuery_nonZeroCode_throws() throws IOException {
            final var url = startServerUrl(new Canned(
                    200, "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"response\":{\"code\":1,\"log\":\"boom\"}}}"));
            assertThatThrownBy(() -> clientFor(url, 0).abciQuery(new byte[53], 9))
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("ABCI query failed with code 1")
                    .hasMessageContaining("boom");
        }

        @Test
        void abciQuery_insufficientProofOps_throws() throws IOException {
            final var body = """
                    {"jsonrpc":"2.0","id":1,"result":{"response":{
                      "code":0,"value":"","proofOps":{"ops":[{"data":"%s"}]}
                    }}}
                    """.formatted(Base64.getEncoder().encodeToString(new byte[] {0x0a}));
            final var url = startServerUrl(new Canned(200, body));
            assertThatThrownBy(() -> clientFor(url, 0).abciQuery(new byte[53], 9))
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("Expected 2 proof ops");
        }

        @Test
        void abciQuery_success_decodesValueAndProofs() throws IOException {
            final var value = new byte[] {0x01, 0x02};
            final var iavl = new byte[] {0x0a};
            final var multistore = new byte[] {0x0b};
            final var b64 = Base64.getEncoder();
            final var body =
                    """
                    {"jsonrpc":"2.0","id":1,"result":{"response":{
                      "code":0,"value":"%s","proofOps":{"ops":[{"data":"%s"},{"data":"%s"}]}
                    }}}
                    """.formatted(b64.encodeToString(value), b64.encodeToString(iavl), b64.encodeToString(multistore));
            final var url = startServerUrl(new Canned(200, body));
            final var result = clientFor(url, 0).abciQuery(new byte[53], 9);
            assertThat(result.value()).isEqualTo(Bytes.wrap(value));
            assertThat(result.iavlProof()).isEqualTo(Bytes.wrap(iavl));
            assertThat(result.multistoreProof()).isEqualTo(Bytes.wrap(multistore));
        }

        // ---- decodeHexField ----

        @Test
        void getSignedHeader_invalidHexField_throws() throws IOException {
            final var url = startServerUrl(new Canned(
                    200,
                    "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"signed_header\":{\"header\":"
                            + "{\"chain_id\":\"sei\",\"height\":\"5\",\"last_commit_hash\":\"zz\"}}}}"));
            assertThatThrownBy(() -> clientFor(url, 0).getSignedHeader(5))
                    .isInstanceOf(JsonRpcException.class)
                    .hasMessageContaining("not valid hex");
        }
    }

    private static final class OneShotJsonServer implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final ExecutorService executor;

        private OneShotJsonServer(final String responseBody) throws IOException {
            serverSocket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
            executor = Executors.newSingleThreadExecutor();
            executor.submit(() -> serve(responseBody));
        }

        private String url() {
            return "http://" + serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getLocalPort();
        }

        private void serve(final String responseBody) {
            try (final var socket = serverSocket.accept();
                    final var reader =
                            new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                    final var output = socket.getOutputStream()) {
                String line;
                while ((line = reader.readLine()) != null && !line.isEmpty()) {
                    // Drain request headers before writing the response.
                }
                final var responseBytes = responseBody.getBytes(StandardCharsets.UTF_8);
                final var headers = ("HTTP/1.1 200 OK\r\n"
                                + "Content-Type: application/json\r\n"
                                + "Content-Length: "
                                + responseBytes.length
                                + "\r\n"
                                + "Channel: close\r\n\r\n")
                        .getBytes(StandardCharsets.UTF_8);
                output.write(headers);
                output.write(responseBytes);
                output.flush();
            } catch (final IOException ignored) {
                // Test teardown can close the server socket before a request arrives.
            }
        }

        @Override
        public void close() throws Exception {
            serverSocket.close();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
