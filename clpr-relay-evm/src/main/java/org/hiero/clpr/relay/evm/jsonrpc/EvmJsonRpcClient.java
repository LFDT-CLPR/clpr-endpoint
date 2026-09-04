// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.jsonrpc;

import static org.hiero.clpr.relay.evm.ByteUtils.fromPrefixedHex;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.hiero.clpr.relay.evm.JsonRpcException;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.model.BlockHeader;
import org.hiero.clpr.relay.evm.model.ProofResponse;
import org.jspecify.annotations.Nullable;

/**
 * A thin JSON-RPC 2.0 HTTP client for communicating with EVM nodes (Geth, Besu, Reth, etc.).
 *
 * <p>Uses {@link java.net.http.HttpClient} for HTTP transport and Jackson for JSON
 * serialisation/deserialisation. Transient errors (connection refused, timeout, HTTP 429/503) are
 * retried with exponential backoff.
 */
public class EvmJsonRpcClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long INITIAL_BACKOFF_MS = 200L;

    private final URI jsonRpcUri;
    private final HttpClient httpClient;
    private final int maxRetries;
    private final Duration requestTimeout;

    /**
     * Create a client targeting the given JSON-RPC URL with a custom retry limit and per-request
     * timeout.
     *
     * @param jsonRpcUrl     the base URL of the EVM node
     * @param maxRetries     maximum number of retry attempts on transient errors
     * @param requestTimeout per-request response timeout; on expiry the request is treated as a
     *                       transient error and retried up to {@code maxRetries} times
     */
    public EvmJsonRpcClient(final String jsonRpcUrl, final int maxRetries, final Duration requestTimeout) {
        this.jsonRpcUri = URI.create(jsonRpcUrl);
        this.maxRetries = maxRetries;
        this.requestTimeout = requestTimeout;
        // Force HTTP/1.1: Ethereum JSON-RPC servers (Anvil, Besu, Reth, Geth) do not speak
        // HTTP/2, and the JDK default's HTTP/2 upgrade dance fails with "header parser
        // received no bytes" when the server drops the h2 upgrade instead of falling back.
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Perform a read-only contract call ({@code eth_call}).
     *
     * @param to the contract address (hex with 0x prefix)
     * @param data the call data (hex with 0x prefix)
     * @param blockTag block tag, e.g. {@code "latest"} or {@code "finalized"}
     * @return the hex-encoded return data
     */
    public String ethCall(final String to, final String data, final String blockTag) {
        final ObjectNode callObject = MAPPER.createObjectNode();
        callObject.put("to", to);
        callObject.put("data", data);
        final String body = buildRequest("eth_call", callObject, blockTag);
        return send(body).asText();
    }

    /** Overload of {@link #ethCall} that includes a {@code from} address in the call object. */
    public String ethCallFrom(final String from, final String to, final String data, final String blockTag) {
        final ObjectNode callObject = MAPPER.createObjectNode();
        callObject.put("from", from);
        callObject.put("to", to);
        callObject.put("data", data);
        final String body = buildRequest("eth_call", callObject, blockTag);
        return send(body).asText();
    }

    /**
     * Estimate the gas a call from {@code from} to {@code to} with {@code data} would use
     * ({@code eth_estimateGas}). Sent with only the call object (no block tag) for maximum node
     * compatibility; the node estimates against its pending/latest state. A call that would revert
     * surfaces as a {@link JsonRpcException}, exactly like {@link #ethCallFrom}.
     *
     * @param from the sender address ({@code msg.sender}; can affect the executed path and its gas)
     * @param to   the contract address
     * @param data the ABI-encoded call data (hex with 0x prefix)
     * @return the estimated gas as a {@code long}
     */
    public long ethEstimateGas(final String from, final String to, final String data) {
        final ObjectNode callObject = MAPPER.createObjectNode();
        callObject.put("from", from);
        callObject.put("to", to);
        callObject.put("data", data);
        final String body = buildRequest("eth_estimateGas", callObject);
        return parseHexLong(send(body).asText());
    }

    /**
     * Submit a signed raw transaction ({@code eth_sendRawTransaction}).
     *
     * @param signedTxHex the RLP-encoded signed transaction (hex with 0x prefix)
     * @return the transaction hash
     */
    public String ethSendRawTransaction(final String signedTxHex) {
        final String body = buildRequest("eth_sendRawTransaction", signedTxHex);
        return send(body).asText();
    }

    /**
     * Retrieve a transaction receipt ({@code eth_getTransactionReceipt}).
     *
     * @param txHash the transaction hash (hex with 0x prefix)
     * @return the receipt as a {@link JsonNode}, or {@code null} if the transaction is not yet
     *     mined
     */
    @Nullable
    public JsonNode ethGetTransactionReceipt(final String txHash) {
        final String body = buildRequest("eth_getTransactionReceipt", txHash);
        final JsonNode result = send(body);
        return result.isNull() ? null : result;
    }

    /**
     * Get the transaction count (nonce) for an address ({@code eth_getTransactionCount}).
     *
     * @param address the account address (hex with 0x prefix)
     * @param blockTag block tag, e.g. {@code "latest"} or {@code "pending"}
     * @return the transaction count as a {@code long}
     */
    public long ethGetTransactionCount(final String address, final String blockTag) {
        final String body = buildRequest("eth_getTransactionCount", address, blockTag);
        return parseHexLong(send(body).asText());
    }

    /**
     * Get the number of the most recent block ({@code eth_blockNumber}).
     *
     * @return the latest block number as a {@code long}
     */
    public long ethBlockNumber() {
        final String body = buildRequest("eth_blockNumber");
        return parseHexLong(send(body).asText());
    }

    /**
     * Fetch event logs for a single contract address and a single {@code topic0} (event-signature
     * hash) over the inclusive block range {@code [fromBlock, toBlock]} ({@code eth_getLogs}).
     *
     * <p>Each returned {@link LogEntry} carries the raw {@code topics} and {@code data} of a
     * matching log plus the block number it was mined in. Callers decode the indexed/non-indexed
     * fields themselves (see {@link #decodeChannelCompletedChannelIds}).
     *
     * @param address  the contract address whose logs to filter (hex with 0x prefix)
     * @param topic0   the event-signature hash to filter on (hex with 0x prefix), or {@code null}
     *                 to match any event from {@code address}
     * @param fromBlock first block of the inclusive range
     * @param toBlock   last block of the inclusive range
     * @return the matching logs, in chain order
     */
    public List<LogEntry> ethGetLogs(
            final String address, @Nullable final String topic0, final long fromBlock, final long toBlock) {
        final ObjectNode filter = MAPPER.createObjectNode();
        filter.put("fromBlock", toBlockTag(fromBlock));
        filter.put("toBlock", toBlockTag(toBlock));
        filter.put("address", address);
        if (topic0 != null) {
            final ArrayNode topics = MAPPER.createArrayNode();
            topics.add(topic0);
            filter.set("topics", topics);
        }
        final String body = buildRequest("eth_getLogs", filter);
        return logsFromJson(send(body));
    }

    /**
     * A single decoded {@code eth_getLogs} entry: the indexed topics, the non-indexed data, and the
     * block the log was emitted in. Topic 0 is the event-signature hash; subsequent topics are the
     * event's indexed parameters in declaration order.
     *
     * @param topics      the log topics (hex, 0x-prefixed); {@code topics[0]} is the event signature
     * @param data        the non-indexed log data (hex, 0x-prefixed)
     * @param blockNumber the block the log was mined in
     */
    public record LogEntry(List<String> topics, String data, long blockNumber) {}

    /** Decode an {@code eth_getLogs} result array into {@link LogEntry} records. */
    public static List<LogEntry> logsFromJson(final JsonNode result) {
        if (result == null || result.isNull() || !result.isArray()) {
            return List.of();
        }
        final List<LogEntry> entries = new ArrayList<>(result.size());
        for (final JsonNode logNode : result) {
            final List<String> topics = new ArrayList<>();
            final JsonNode topicsNode = logNode.get("topics");
            if (topicsNode != null && topicsNode.isArray()) {
                for (final JsonNode t : topicsNode) {
                    topics.add(t.asText());
                }
            }
            final JsonNode dataNode = logNode.get("data");
            final String data = dataNode != null && !dataNode.isNull() ? dataNode.asText() : "0x";
            final JsonNode blockNode = logNode.get("blockNumber");
            final long blockNumber = blockNode != null && !blockNode.isNull() ? parseHexLong(blockNode.asText()) : 0L;
            entries.add(new LogEntry(List.copyOf(topics), data, blockNumber));
        }
        return List.copyOf(entries);
    }

    /**
     * Extract the {@code channelId} (indexed {@code bytes32} = {@code topics[1]}) from each
     * {@code ChannelCompleted} log in {@code entries}, preserving order and skipping malformed
     * entries (fewer than two topics). The {@code channelId} is the first indexed parameter of
     * {@code event ChannelCompleted(bytes32 indexed channelId, string, bytes, address,
     * bytes32)}.
     *
     * @param entries decoded logs (already filtered to the ChannelCompleted topic0)
     * @return the 32-byte channel ids, in log order
     */
    public static List<Bytes> decodeChannelCompletedChannelIds(final List<LogEntry> entries) {
        final List<Bytes> ids = new ArrayList<>(entries.size());
        for (final LogEntry entry : entries) {
            if (entry.topics().size() < 2) {
                continue;
            }
            ids.add(fromPrefixedHex(entry.topics().get(1)));
        }
        return List.copyOf(ids);
    }

    /**
     * Get a block header by number (using {@code eth_getBlockByNumber} with fullTxs set to false).
     *
     * @param blockTag block tag, e.g. {@code "latest"} or a hex block number
     * @return the block as a {@link JsonNode}
     */
    public BlockHeader ethGetBlockHeaderByNumber(final String blockTag) {
        final var fullTxs = false;
        final String body = buildRequest("eth_getBlockByNumber", blockTag, fullTxs);
        final var blockResp = send(body);
        // eth_getBlockByNumber returns JSON null when the block is not available (unknown tag,
        // node still syncing, requested number beyond head). Surface it as a retryable
        // JSON-RPC error with the tag included, so the caller's backoff loop shows *which*
        // read failed instead of an unqualified NPE deep in blockHeaderFromJson.
        if (blockResp.isNull()) {
            throw new JsonRpcException(-32000, "eth_getBlockByNumber returned null for blockTag=" + blockTag);
        }
        return blockHeaderFromJson(blockResp);
    }

    public static BlockHeader blockHeaderFromJson(final JsonNode block) {
        return new BlockHeader(
                requiredBytes(block, "parentHash"),
                requiredBytes(block, "sha3Uncles"),
                Address.fromHexString(requiredString(block, "miner")),
                requiredBytes(block, "stateRoot"),
                requiredBytes(block, "transactionsRoot"),
                requiredBytes(block, "receiptsRoot"),
                requiredBytes(block, "logsBloom"),
                requiredBigInteger(block, "difficulty"),
                requiredBigInteger(block, "number"),
                requiredBigInteger(block, "gasLimit"),
                requiredBigInteger(block, "gasUsed"),
                requiredBigInteger(block, "timestamp"),
                requiredBytes(block, "extraData"),
                requiredBytes(block, "mixHash"),
                requiredBytes(block, "nonce"),
                hexBigIntegerOrNull(block, "baseFeePerGas"),
                bytesOrNull(block, "withdrawalsRoot"),
                // EIP-4844 blob fields and EIP-4788 parentBeaconBlockRoot must be parsed in
                // Besu's canonical write order so encodeBlockHeader can reproduce the bytes
                // Besu signed when computing the QBFT committed-seal hash.
                hexBigIntegerOrNull(block, "blobGasUsed"),
                hexBigIntegerOrNull(block, "excessBlobGas"),
                bytesOrNull(block, "parentBeaconBlockRoot"),
                bytesOrNull(block, "requestsHash"),
                requiredBytes(block, "hash"));
    }

    private static String requiredString(final JsonNode node, final String field) {
        final JsonNode child = node.get(field);
        if (child == null || child.isNull()) {
            throw new JsonRpcException(-32000, "block header missing required field '" + field + "'");
        }
        return child.asText();
    }

    private static Bytes requiredBytes(final JsonNode node, final String field) {
        return fromPrefixedHex(requiredString(node, field));
    }

    private static BigInteger requiredBigInteger(final JsonNode node, final String field) {
        return parseHexBigInteger(requiredString(node, field));
    }

    private static BigInteger parseHexBigInteger(final String hex) {
        final String stripped = hex.startsWith("0x") || hex.startsWith("0X") ? hex.substring(2) : hex;
        if (stripped.isEmpty()) return BigInteger.ZERO;
        return new BigInteger(stripped, 16);
    }

    private static BigInteger hexBigIntegerOrNull(final JsonNode node, final String field) {
        final JsonNode child = node.get(field);
        if (child == null || child.isNull()) return null;
        return parseHexBigInteger(child.asText());
    }

    private static Bytes bytesOrNull(final JsonNode node, final String field) {
        final JsonNode child = node.get(field);
        if (child == null || child.isNull()) return null;
        return fromPrefixedHex(child.asText());
    }

    /**
     * Get the ETH balance of an account ({@code eth_getBalance}).
     *
     * @param address  the account address (hex with 0x prefix)
     * @param blockTag block tag, e.g. {@code "latest"} or {@code "finalized"}
     * @return the balance in wei
     */
    public BigInteger ethGetBalance(final String address, final String blockTag) {
        final String body = buildRequest("eth_getBalance", address, blockTag);
        return parseHexBigInteger(send(body).asText());
    }

    /**
     * Retrieve the EIP-1186 Merkle proof for an account and optional storage keys
     * ({@code eth_getProof}).
     *
     * <p>The returned object contains:
     * <ul>
     *   <li>{@code accountProof} — RLP-serialised MerkleTree-Nodes from the state trie root to
     *       the account leaf, as a JSON array of hex strings</li>
     *   <li>{@code balance} — account balance in wei (hex)</li>
     *   <li>{@code codeHash} — keccak256 hash of the account code (hex)</li>
     *   <li>{@code nonce} — account nonce (hex)</li>
     *   <li>{@code storageHash} — keccak256 hash of the account's storage trie root (hex)</li>
     *   <li>{@code storageProof} — array of proof objects, one per requested storage key, each
     *       containing {@code key}, {@code value}, and {@code proof} (array of hex strings)</li>
     * </ul>
     *
     * @param address     the account address (hex with 0x prefix)
     * @param storageKeys storage slot keys for which inclusion proofs are requested (each hex
     *                    with 0x prefix); pass an empty array for an account-only proof
     * @param blockTag    block tag, e.g. {@code "latest"} or a hex block number
     * @return the proof object as a {@link JsonNode}
     */
    public ProofResponse ethGetProof(final String address, final String[] storageKeys, final String blockTag) {
        final ArrayNode keysArray = MAPPER.createArrayNode();
        for (final String key : storageKeys) {
            keysArray.add(key);
        }
        final String body = buildRequest("eth_getProof", address, keysArray, blockTag);
        final var respJson = send(body);
        final var accountProof = accountProofFromJson(respJson);
        final var storageProof = storageProofFromJson(respJson);
        return new ProofResponse(accountProof, storageProof);
    }

    public static List<Bytes> accountProofFromJson(final JsonNode proof) {
        final List<Bytes> nodes = new ArrayList<>();
        for (final JsonNode node : proof.get("accountProof")) {
            nodes.add(fromPrefixedHex(node.asText()));
        }
        return List.copyOf(nodes);
    }

    public static List<ProofResponse.StorageProofEntry> storageProofFromJson(final JsonNode proof) {
        final List<ProofResponse.StorageProofEntry> entries = new ArrayList<>();
        for (final JsonNode entry : proof.get("storageProof")) {
            final Bytes key = fromPrefixedHex(entry.get("key").asText());
            final Bytes value = fromPrefixedHex(entry.get("value").asText());
            final List<Bytes> entryProof = new ArrayList<>();
            for (final JsonNode node : entry.get("proof")) {
                entryProof.add(fromPrefixedHex(node.asText()));
            }
            entries.add(new ProofResponse.StorageProofEntry(key, value, List.copyOf(entryProof)));
        }
        return List.copyOf(entries);
    }

    // -------------------------------------------------------------------------
    // Package-visible helpers (also used by tests)
    // -------------------------------------------------------------------------

    /**
     * Build a JSON-RPC 2.0 request body string.
     *
     * @param method the JSON-RPC method name
     * @param params the method parameters (strings, booleans, or {@link JsonNode} objects)
     * @return the JSON string
     */
    static String buildRequest(final String method, final Object... params) {
        final ObjectNode request = MAPPER.createObjectNode();
        request.put("jsonrpc", "2.0");
        // JSON-RPC 2.0 requires an "id" field but does not require uniqueness for non-batched
        // single-request HTTP — each call gets its own response on its own connection, so a
        // constant id is correlation-safe. If we ever switch to batched requests, the ids
        // inside a batch will need to be distinct.
        request.put("id", 1);
        request.put("method", method);

        final ArrayNode paramsArray = MAPPER.createArrayNode();
        for (final Object param : params) {
            switch (param) {
                case final JsonNode jn -> paramsArray.add(jn);
                case final Boolean b -> paramsArray.add(b);
                case final Long l -> paramsArray.add(l);
                case final Integer i -> paramsArray.add(i);
                default -> paramsArray.add(param.toString());
            }
        }
        request.set("params", paramsArray);

        try {
            return MAPPER.writeValueAsString(request);
        } catch (final JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialise JSON-RPC request", e);
        }
    }

    // -------------------------------------------------------------------------
    // Internal HTTP + retry logic
    // -------------------------------------------------------------------------

    private JsonNode send(final String requestBody) {
        IOException lastIoException = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            if (attempt > 0) {
                sleepBackoff(attempt);
            }
            try {
                final HttpRequest request = HttpRequest.newBuilder()
                        .uri(jsonRpcUri)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                        .timeout(requestTimeout)
                        .build();

                final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                final int status = response.statusCode();

                if (status == 429 || status == 503) {
                    // Transient: rate-limited or service unavailable — retry
                    if (attempt < maxRetries) {
                        continue;
                    }
                    throw new JsonRpcException(-32000, "HTTP " + status + " after " + maxRetries + " retries");
                }

                if (status < 200 || status >= 300) {
                    throw new JsonRpcException(-32000, "HTTP error " + status);
                }

                return parseResponse(response.body());

            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new JsonRpcException(-32000, "Request interrupted");
            } catch (final IOException e) {
                lastIoException = e;
                // Channel-level transient error — retry
            }
        }
        throw new JsonRpcException(
                -32000,
                "Channel failed after " + maxRetries + " retries: "
                        + (lastIoException != null ? lastIoException.getMessage() : "unknown"));
    }

    private static JsonNode parseResponse(final String body) {
        final JsonNode root;
        try {
            root = MAPPER.readTree(body);
        } catch (final JsonProcessingException e) {
            throw new JsonRpcException(-32700, "Invalid JSON in response: " + e.getMessage());
        }

        final JsonNode errorNode = root.get("error");
        if (errorNode != null && !errorNode.isNull()) {
            final int code = errorNode.path("code").asInt(-32000);
            final String message = errorNode.path("message").asText("unknown error");
            final JsonNode dataNode = errorNode.get("data");
            final String detail = dataNode != null && !dataNode.isNull() ? " data=" + dataNode : "";
            throw new JsonRpcException(code, message + detail);
        }

        final JsonNode resultNode = root.get("result");
        if (resultNode == null) {
            throw new JsonRpcException(-32603, "Response contains neither 'result' nor 'error'");
        }
        return resultNode;
    }

    static long parseHexLong(final String hex) {
        final String stripped = hex.startsWith("0x") || hex.startsWith("0X") ? hex.substring(2) : hex;
        if (stripped.isEmpty()) {
            return 0L;
        }
        return Long.parseUnsignedLong(stripped, 16);
    }

    private static void sleepBackoff(final int attempt) {
        final long delayMs = INITIAL_BACKOFF_MS * (1L << (attempt - 1));
        try {
            Thread.sleep(delayMs);
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    public static String toBlockTag(long blockNumber) {
        return "0x" + Long.toString(blockNumber, 16);
    }
}
