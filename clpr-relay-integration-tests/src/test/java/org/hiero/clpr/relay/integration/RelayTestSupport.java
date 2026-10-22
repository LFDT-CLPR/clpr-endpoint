// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.swirlds.metrics.api.Counter;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.hiero.clpr.relay.app.ProofType;
import org.hiero.clpr.relay.app.RelayConfig;
import org.hiero.clpr.relay.app.RelayConfig.BackoffConfig;
import org.hiero.clpr.relay.app.RelayInstance;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Triple;

/**
 * Static helpers shared by the integration tests. {@code IntegrationTestBase} and
 * {@code OneSidedStubPeerTestBase} subclasses inherit thin delegators for these; This is the
 * single home for the relay-config shape, port allocation, ETH-transfer-and-wait, and metric
 * read, so none of those is copy-pasted per test.
 */
public final class RelayTestSupport {

    /** The default policy: 1 s base, 30 s cap. */
    public static final BackoffConfig DEFAULT_BACKOFF = new BackoffConfig(1000L, 30_000L);

    private RelayTestSupport() {}

    /**
     * Build a relay for integration tests. The suite deploys {@code StubClprVerifier}, which accepts
     * only CLPRSTUB-prefixed proofs, so integration relays run with {@code ProofType.Stub} on both the
     * local network (build via {@code StubBundleConstructor}) and the peer side (parse via
     * {@code StubProofCodec}) — see {@link #relayConfig(String, int, String, List, ProofType)}.
     *
     * @param config       the relay configuration
     * @param instanceName worker-loop log-context label
     * @return a not-yet-started relay instance
     */
    public static RelayInstance buildRelay(final RelayConfig config, final String instanceName) {
        return RelayInstance.build(config, instanceName);
    }

    /**
     * Allocate a free TCP port on localhost. Not collision-proof but acceptable for tests.
     *
     * @return a free port number
     * @throws Exception if a socket cannot be opened
     */
    public static int findFreePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /**
     * Sum the {@code sync.exchanges.started{role=initiator}} counters across all channels of a relay,
     * or 0 if absent. Tests use this to assert whether a relay initiated any outbound sync.
     *
     * @param relay the relay to query
     * @return the counter value, or {@code 0}
     */
    public static long outboundAttempts(final RelayInstance relay) {
        // sync.exchanges.started{role=initiator,...} is registered lazily per channel_id — sum the
        // initiator-side entries. The encoded metric name carries labels as a [k=v,...] suffix, so
        // filtering on "role=initiator" isolates outbound (dialed) exchanges from inbound (responder) ones.
        return relay.metrics().findMetricsByCategory("sync").stream()
                .filter(m -> m.getName().startsWith("exchanges.started")
                        && m.getName().contains("role=initiator"))
                .filter(Counter.class::isInstance)
                .mapToLong(m -> ((Counter) m).get())
                .sum();
    }

    /**
     * The single-channel relay config used across the integration suite. Centralises the
     * chain target and channel shape so a tweak lands in one place.
     *
     * @param jsonRpcUrl       the Anvil JSON-RPC URL
     * @param port             the gRPC port to bind
     * @param privateKey       the signing key
     * @param channelParams (channelId, serviceAddress, peerProofType) per channel
     * @return a relay configuration ready for {@link RelayInstance#build(RelayConfig)}
     */
    public static RelayConfig relayConfig(
            final String jsonRpcUrl,
            final int port,
            final String privateKey,
            final List<Triple<String, String, ProofType>> channelParams) {
        return relayConfig(jsonRpcUrl, port, privateKey, channelParams, ProofType.QBFT);
    }

    /**
     * The {@link #relayConfig} variant that lets the caller pick the <em>local</em> network's proof
     * type. Two real relays exchanging messages over the {@code StubClprVerifier} must both build
     * {@code ProofType.Stub} (CLPRSTUB) bundles; single-side stub-peer tests keep the QBFT default.
     *
     * @param localProofType the local network's own proof type ({@code QBFT} or {@code Stub})
     */
    public static RelayConfig relayConfig(
            final String jsonRpcUrl,
            final int port,
            final String privateKey,
            final List<Triple<String, String, ProofType>> channelParams,
            final ProofType localProofType) {
        return new RelayConfig(
                new RelayConfig.GrpcConfig(port, 1_048_576),
                List.of(localNetwork(jsonRpcUrl, localProofType)),
                channels(privateKey, channelParams),
                DEFAULT_BACKOFF,
                peerProofTypes(channelParams));
    }

    /** The QBFT local network targeting the Anvil chain. */
    private static RelayConfig.LocalNetworkConfig localNetwork(final String jsonRpcUrl) {
        return localNetwork(jsonRpcUrl, ProofType.QBFT);
    }

    /**
     * The local network targeting the Anvil chain with the given proof type. {@code QBFT} builds real
     * QBFT bundles; {@code Stub} builds CLPRSTUB bundles from real channel state for the
     * {@code StubClprVerifier} — the format two real relays must use to exchange messages end-to-end
     * over the stub verifier.
     */
    private static RelayConfig.LocalNetworkConfig localNetwork(
            final String jsonRpcUrl, final ProofType localProofType) {
        final var evm = new RelayConfig.CommonEvmParams(
                jsonRpcUrl,
                AnvilContainer.CHAIN_ID,
                Long.MAX_VALUE,
                0L,
                1.2,
                1000L,
                // Short request timeout + single retry so a node outage surfaces as a
                // channel-level failure in seconds rather than the ~2 min the production
                // defaults (30 s × 4 attempts) would take — see TransientNodeOutageRecoveryTest.
                2000L,
                1);
        return switch (localProofType) {
            case QBFT ->
                new RelayConfig.LocalNetworkConfig(
                        "local",
                        ProofType.QBFT,
                        evm,
                        null,
                        new RelayConfig.QbftConfig(30_000L, 5, 10, 1_048_576L),
                        null,
                        null);
            case Stub -> new RelayConfig.LocalNetworkConfig("local", ProofType.Stub, evm, null, null, null, null);
            default ->
                throw new IllegalArgumentException(
                        "unsupported local proof type for integration tests: " + localProofType);
        };
    }

    private static List<RelayConfig.ClprServiceConfig> channels(
            final String privateKey, final List<Triple<String, String, ProofType>> channelParams) {
        // Group predefined channels by their ClprService address; each distinct address becomes
        // one ClprServiceConfig sharing the single signing key.
        final var channelsByService = new LinkedHashMap<String, List<String>>();
        final var peerProofTypes = new LinkedHashMap<String, ProofType>();
        for (final var param : channelParams) {
            channelsByService
                    .computeIfAbsent(param.getMiddle(), k -> new ArrayList<>())
                    .add(param.getLeft());
            // peerProofType is resolved at runtime from the channel's on-chain peer chainId; the
            // integration channels are all registered under the Anvil chain, so map that chainId
            // to the intended peer proof type here.
            peerProofTypes.put("eip155:" + AnvilContainer.CHAIN_ID, param.getRight());
        }

        return channelsByService.entrySet().stream()
                .map(e -> new RelayConfig.ClprServiceConfig(
                        privateKey, "local", e.getKey(), false, 0L, List.copyOf(e.getValue()), Map.of()))
                .toList();
    }

    private static LinkedHashMap<String, ProofType> peerProofTypes(
            final List<Triple<String, String, ProofType>> channelParams) {
        // Group predefined channels by their ClprService address; each distinct address becomes
        // one ClprServiceConfig sharing the single signing key.
        final var peerProofTypes = new LinkedHashMap<String, ProofType>();
        for (final var param : channelParams) {
            // peerProofType is resolved at runtime from the channel's on-chain peer chainId; the
            // integration channels are all registered under the Anvil chain, so map that chainId
            // to the intended peer proof type here.
            peerProofTypes.put("eip155:" + AnvilContainer.CHAIN_ID, param.getRight());
        }

        return peerProofTypes;
    }

    /**
     * Send a plain ETH transfer from the dev account to {@code to} and wait for the receipt.
     *
     * @param submitter the client-side raw-tx submitter (signs as EIP-1559)
     * @param client    the JSON-RPC client (for receipt polling)
     * @param to        the recipient address (hex with {@code 0x} prefix)
     * @param weiAmount the amount of wei to transfer
     * @throws InterruptedException if interrupted while polling for the receipt
     */
    static void sendEthAndWait(
            final AnvilTxSubmitter submitter, final EvmJsonRpcClient client, final String to, final long weiAmount)
            throws InterruptedException {
        final String txHash = submitter.sendRawTx(to, new byte[0], weiAmount, 0xffffffL);
        for (int i = 0; i < 120; i++) {
            final JsonNode receipt = client.ethGetTransactionReceipt(txHash);
            if (receipt != null && !receipt.isNull()) {
                final JsonNode statusNode = receipt.get("status");
                final String status = statusNode != null ? statusNode.asText() : "0x0";
                if ("0x1".equals(status)) {
                    return;
                }
                throw new IllegalStateException("ETH transfer to " + to + " failed, status=" + status);
            }
            Thread.sleep(500L);
        }
        throw new IllegalStateException("ETH transfer receipt not found for tx " + txHash);
    }
}
