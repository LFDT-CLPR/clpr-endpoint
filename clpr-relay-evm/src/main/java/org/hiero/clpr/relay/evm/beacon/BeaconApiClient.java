// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.beacon;

import static org.hiero.clpr.relay.evm.ByteUtils.fromPrefixedHex;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hedera.cryptography.hints.HintsLibraryBridge;
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

/**
 * A thin Beacon-node REST client (consensus layer) for the light-client endpoints the Ethereum-PoS
 * bundle/config producer needs. Distinct from {@code EvmJsonRpcClient}: the beacon node is a
 * separate process speaking the Beacon REST API ({@code GET /eth/v1/beacon/...}) rather than
 * execution-layer JSON-RPC.
 *
 * <p>Transport mirrors {@code EvmJsonRpcClient}: {@link java.net.http.HttpClient} forced to HTTP/1.1,
 * with exponential-backoff retry of transient failures (connection errors, timeout, HTTP 429/503).
 */
public class BeaconApiClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long INITIAL_BACKOFF_MS = 200L;

    private final URI baseUri;
    private final HttpClient httpClient;
    private final int maxRetries;
    private final Duration requestTimeout;

    /**
     * Slots per sync-committee period, lazily fetched from {@code /eth/v1/config/spec} and cached for
     * this client's lifetime. {@code -1} means not yet fetched. Guarded by {@code this} rather than
     * {@code responseCache} because it derives from two spec fields combined into one value.
     */
    private volatile long slotsPerSyncCommitteePeriod = -1L;
    /**
     * @param beaconApiUrl   base URL of the beacon node REST API (e.g. {@code http://localhost:5052})
     * @param maxRetries     maximum retry attempts on transient errors
     * @param requestTimeout per-request response timeout
     */
    public BeaconApiClient(final String beaconApiUrl, final int maxRetries, final Duration requestTimeout) {
        // Strip a trailing slash so path concatenation is clean.
        final String trimmed =
                beaconApiUrl.endsWith("/") ? beaconApiUrl.substring(0, beaconApiUrl.length() - 1) : beaconApiUrl;
        this.baseUri = URI.create(trimmed);
        this.maxRetries = maxRetries;
        this.requestTimeout = requestTimeout;
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /** {@code GET /eth/v1/beacon/genesis} → the 32-byte genesis validators root. */
    public Bytes getGenesisValidatorsRoot() {
        final JsonNode data = fetch("/eth/v1/beacon/genesis").get("data");
        requireField(data, "genesis_validators_root", "genesis");
        return fromPrefixedHex(data.get("genesis_validators_root").asText());
    }

    /**
     * {@code GET /eth/v1/beacon/headers/finalized} → the 32-byte beacon block root of the latest
     * finalized header (hex with {@code 0x} prefix), suitable as the {@code block_root} for
     * {@link #getBootstrap(String)}.
     */
    public String getFinalizedBlockRootHex() {
        final JsonNode data = fetch("/eth/v1/beacon/headers/finalized").get("data");
        return requireField(data, "root", "finalized header").asText();
    }

    /**
     * {@code GET /eth/v1/beacon/states/finalized/fork} → the 4-byte consensus fork version in
     * effect at the finalized state ({@code Fork.current_version}) — the fork version component of
     * the Ethereum-PoS trust anchor and sync-committee signing domain.
     */
    public Bytes getCurrentForkVersion() {
        final JsonNode data = fetch("/eth/v1/beacon/states/finalized/fork").get("data");
        requireField(data, "current_version", "fork");
        return fromPrefixedHex(data.get("current_version").asText());
    }

    /** {@code GET /eth/v1/beacon/light_client/finality_update} → the latest finalized light-client update. */
    public LightClientUpdate getFinalityUpdate() {
        return parseUpdate(fetch("/eth/v1/beacon/light_client/finality_update").get("data"));
    }

    /** {@code GET /eth/v1/beacon/light_client/bootstrap/{block_root}} → a bootstrap for the trust anchor. */
    public LightClientBootstrap getBootstrap(final String blockRootHex) {
        final JsonNode data =
                fetch("/eth/v1/beacon/light_client/bootstrap/" + blockRootHex).get("data");
        final LightClientHeader header = parseHeader(requireField(data, "header", "bootstrap"));
        final SyncCommittee committee = parseSyncCommittee(requireField(data, "current_sync_committee", "bootstrap"));
        final List<Bytes> branch = parseBranch(requireField(data, "current_sync_committee_branch", "bootstrap"));
        return new LightClientBootstrap(header, committee, branch);
    }

    /**
     * {@code GET /eth/v1/beacon/light_client/updates?start_period=&count=} → committee-period updates
     * (carry {@code next_sync_committee} for rotation). The endpoint returns a top-level array.
     */
    public List<LightClientUpdate> getUpdates(final long startPeriod, final int count) {
        final JsonNode array =
                fetch("/eth/v1/beacon/light_client/updates?start_period=" + startPeriod + "&count=" + count);
        if (!array.isArray()) {
            throw new BeaconApiException("light_client/updates did not return a JSON array");
        }
        final List<LightClientUpdate> updates = new ArrayList<>(array.size());
        for (final JsonNode entry : array) {
            updates.add(parseUpdate(requireField(entry, "data", "updates[]")));
        }
        return updates;
    }

    /**
     * Slots per sync-committee period ({@code SLOTS_PER_EPOCH × EPOCHS_PER_SYNC_COMMITTEE_PERIOD}),
     * fetched once from {@code GET /eth/v1/config/spec} and cached for this client's lifetime.
     * Mainnet's value is {@code 32 × 256 = 8192}, but devnets running a non-mainnet preset (e.g. the
     * "minimal" preset, which shrinks {@code SLOTS_PER_EPOCH}) can differ — callers must not assume
     * the mainnet value.
     */
    public long getSlotsPerSyncCommitteePeriod() {
        long cached = slotsPerSyncCommitteePeriod;
        if (cached < 0) {
            synchronized (this) {
                cached = slotsPerSyncCommitteePeriod;
                if (cached < 0) {
                    cached = fetchSlotsPerSyncCommitteePeriod();
                    slotsPerSyncCommitteePeriod = cached;
                }
            }
        }
        return cached;
    }

    private long fetchSlotsPerSyncCommitteePeriod() {
        final JsonNode data = fetch("/eth/v1/config/spec").get("data");
        final long slotsPerEpoch = u64(data, "SLOTS_PER_EPOCH");
        final long epochsPerSyncCommitteePeriod = u64(data, "EPOCHS_PER_SYNC_COMMITTEE_PERIOD");
        return slotsPerEpoch * epochsPerSyncCommitteePeriod;
    }

    // -------------------------------------------------------------------------
    // Parsing
    // -------------------------------------------------------------------------

    private LightClientUpdate parseUpdate(final JsonNode data) {
        final LightClientHeader attested = parseHeader(requireField(data, "attested_header", "update"));
        final SyncAggregate aggregate = parseSyncAggregate(requireField(data, "sync_aggregate", "update"));
        final long signatureSlot = u64(data, "signature_slot");

        SyncCommittee nextCommittee = null;
        List<Bytes> nextBranch = null;
        final JsonNode nextCommitteeNode = data.get("next_sync_committee");
        if (nextCommitteeNode != null && !nextCommitteeNode.isNull()) {
            nextCommittee = parseSyncCommittee(nextCommitteeNode);
            nextBranch = parseBranch(requireField(data, "next_sync_committee_branch", "update"));
        }
        return new LightClientUpdate(attested, aggregate, signatureSlot, nextCommittee, nextBranch);
    }

    private LightClientHeader parseHeader(final JsonNode header) {
        final JsonNode beaconNode = requireField(header, "beacon", "header");
        final BeaconBlockHeader beacon = new BeaconBlockHeader(
                u64(beaconNode, "slot"),
                u64(beaconNode, "proposer_index"),
                hex(beaconNode, "parent_root"),
                hex(beaconNode, "state_root"),
                hex(beaconNode, "body_root"));
        final ExecutionPayloadHeader execution = parseExecution(requireField(header, "execution", "header"));
        final List<Bytes> executionBranch = parseBranch(requireField(header, "execution_branch", "header"));
        return new LightClientHeader(beacon, execution, executionBranch);
    }

    private ExecutionPayloadHeader parseExecution(final JsonNode e) {
        return new ExecutionPayloadHeader(
                hex(e, "parent_hash"),
                hex(e, "fee_recipient"),
                hex(e, "state_root"),
                hex(e, "receipts_root"),
                hex(e, "logs_bloom"),
                hex(e, "prev_randao"),
                u64(e, "block_number"),
                u64(e, "gas_limit"),
                u64(e, "gas_used"),
                u64(e, "timestamp"),
                hex(e, "extra_data"),
                bigint(e, "base_fee_per_gas"),
                hex(e, "block_hash"),
                hex(e, "transactions_root"),
                hex(e, "withdrawals_root"),
                u64(e, "blob_gas_used"),
                u64(e, "excess_blob_gas"));
    }

    private SyncCommittee parseSyncCommittee(final JsonNode node) {
        final JsonNode pubkeysNode = requireField(node, "pubkeys", "sync_committee");
        if (!pubkeysNode.isArray()) {
            throw new BeaconApiException("sync_committee.pubkeys is not an array");
        }
        final List<Bytes> pubkeys = new ArrayList<>(pubkeysNode.size());
        for (final JsonNode pk : pubkeysNode) {
            pubkeys.add(fromPrefixedHex(pk.asText()));
        }
        return new SyncCommittee(pubkeys, hex(node, "aggregate_pubkey"));
    }

    private SyncAggregate parseSyncAggregate(final JsonNode node) {
        final byte[] uncompressed = HintsLibraryBridge.getInstance()
                .decompressG2ToEip2537(hex(node, "sync_committee_signature").toByteArray());
        if (uncompressed == null) {
            throw new BeaconApiException("invalid G2 signature in sync_aggregate: G2 decompression failed");
        }
        return new SyncAggregate(hex(node, "sync_committee_bits"), Bytes.wrap(uncompressed));
    }

    private List<Bytes> parseBranch(final JsonNode array) {
        if (!array.isArray()) {
            throw new BeaconApiException("expected a JSON array for an SSZ branch");
        }
        final List<Bytes> branch = new ArrayList<>(array.size());
        for (final JsonNode node : array) {
            branch.add(fromPrefixedHex(node.asText()));
        }
        return branch;
    }

    // -------------------------------------------------------------------------
    // JSON helpers
    // -------------------------------------------------------------------------

    private static JsonNode requireField(final JsonNode node, final String field, final String context) {
        final JsonNode value = node == null ? null : node.get(field);
        if (value == null || value.isNull()) {
            throw new BeaconApiException("beacon response missing field '" + field + "' in " + context);
        }
        return value;
    }

    private static Bytes hex(final JsonNode node, final String field) {
        return fromPrefixedHex(requireField(node, field, "field").asText());
    }

    private static long u64(final JsonNode node, final String field) {
        final String text = requireField(node, field, "field").asText();
        try {
            return Long.parseUnsignedLong(text);
        } catch (final NumberFormatException ex) {
            throw new BeaconApiException("field '" + field + "' is not a uint64: " + text, ex);
        }
    }

    private static BigInteger bigint(final JsonNode node, final String field) {
        final String text = requireField(node, field, "field").asText();
        try {
            return new BigInteger(text);
        } catch (final NumberFormatException ex) {
            throw new BeaconApiException("field '" + field + "' is not an integer: " + text, ex);
        }
    }

    // -------------------------------------------------------------------------
    // HTTP + retry
    // -------------------------------------------------------------------------

    /** Current wall-clock time in millis; overridable so tests can drive cache expiry deterministically. */
    protected long now() {
        return System.currentTimeMillis();
    }

    /** Performs the raw HTTP GET (with retry/backoff) and parses the JSON body. */
    protected JsonNode fetch(final String path) {
        IOException lastIoException = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            if (attempt > 0) {
                sleepBackoff(attempt);
            }
            try {
                final HttpRequest request = HttpRequest.newBuilder()
                        .uri(baseUri.resolve(path))
                        .header("Accept", "application/json")
                        .GET()
                        .timeout(requestTimeout)
                        .build();
                final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                final int status = response.statusCode();

                if (status == 429 || status == 503) {
                    if (attempt < maxRetries) {
                        continue;
                    }
                    throw new BeaconApiException("HTTP " + status + " after " + maxRetries + " retries for " + path);
                }
                if (status < 200 || status >= 300) {
                    throw new BeaconApiException("HTTP error " + status + " for " + path);
                }
                try {
                    return MAPPER.readTree(response.body());
                } catch (final IOException e) {
                    throw new BeaconApiException("invalid JSON in beacon response for " + path, e);
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new BeaconApiException("beacon request interrupted");
            } catch (final IOException e) {
                lastIoException = e;
            }
        }
        throw new BeaconApiException("beacon request to " + path + " failed after " + maxRetries + " retries: "
                + (lastIoException != null ? lastIoException.getMessage() : "unknown"));
    }

    private static void sleepBackoff(final int attempt) {
        try {
            Thread.sleep(INITIAL_BACKOFF_MS * (1L << (attempt - 1)));
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BeaconApiException("beacon backoff interrupted");
        }
    }
}
