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
import org.hiero.clpr.relay.app.RelayConfig.GrpcInfoConfig;
import org.hiero.clpr.relay.app.RelayConfig.GrpcSyncConfig;
import org.hiero.clpr.relay.app.RelayInstance;
import org.hiero.clpr.relay.core.StubReencodingTransactionSubmitter;
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
     * Build a relay for integration tests, injecting the CLPRSTUB re-encoder so each verified bundle
     * is re-encoded to the stub format before submission. The integration suite deploys
     * {@code StubClprVerifier}, which only accepts CLPRSTUB-prefixed proofs, while the relay keeps
     * constructing and parsing real proofs on the wire.
     *
     * @param config       the relay configuration
     * @param instanceName worker-loop log-context label
     * @return a not-yet-started relay instance
     */
    public static RelayInstance buildRelay(final RelayConfig config, final String instanceName) {
        return RelayInstance.build(config, instanceName, StubReencodingTransactionSubmitter::new);
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
     * Read the {@code sync.outbound.attempts} counter from a relay's metrics registry, or 0 if
     * absent. Tests use this to assert whether a relay initiated any outbound sync.
     *
     * @param relay the relay to query
     * @return the counter value, or {@code 0}
     */
    public static long outboundAttempts(final RelayInstance relay) {
        // outbound.attempts is a LabeledCounter registered lazily per channel_id — sum all.
        return relay.metrics().findMetricsByCategory("sync").stream()
                .filter(m -> m.getName().startsWith("outbound.attempts"))
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
        final int infoPort;
        try {
            infoPort = findFreePort(); // distinct per relay (two run in-process)
        } catch (final Exception e) {
            throw new RuntimeException("failed to allocate an info port", e);
        }
        return new RelayConfig(
                new RelayConfig.GrpcConfig(1_048_576),
                new GrpcInfoConfig(infoPort),
                new GrpcSyncConfig(port, false, "", 86400),
                List.of(localNetwork(jsonRpcUrl)),
                channels(privateKey, channelParams),
                DEFAULT_BACKOFF,
                peerProofTypes(channelParams));
    }

    /**
     * The <b>secure</b>-sync variant of {@link #relayConfig}: the sync listener is mandatory mTLS on
     * {@code secureSyncPort}, presenting a leaf signed by the CA key at {@code keyPath} and validating
     * dialers against the on-chain roster. The info listener stays plaintext. Peers dial
     * {@code secureSyncPort} (published in each chain's peer roster) over mutual TLS.
     *
     * @param jsonRpcUrl              the Anvil JSON-RPC URL
     * @param secureSyncPort          the mandatory-mTLS sync port to bind (published to the peer as its target)
     * @param keyPath                 path to this relay's CA PKCS#8 private key (DER/PEM)
     * @param leafCertValiditySeconds seconds between leaf re-mints (0 disables rotation)
     * @param privateKey              the signing key
     * @param channelParams           the reduced ChannelConfig
     * @return a secure-sync relay configuration ready for {@link RelayInstance#build(RelayConfig)}
     */
    public static RelayConfig secureRelayConfig(
            final String jsonRpcUrl,
            final int secureSyncPort,
            final String keyPath,
            final long leafCertValiditySeconds,
            final String privateKey,
            final List<Triple<String, String, ProofType>> channelParams) {
        final int infoPort;
        try {
            infoPort = findFreePort(); // distinct per relay (two run in-process)
        } catch (final Exception e) {
            throw new RuntimeException("failed to allocate an info port", e);
        }
        return new RelayConfig(
                new RelayConfig.GrpcConfig(1_048_576),
                new GrpcInfoConfig(infoPort),
                new GrpcSyncConfig(secureSyncPort, true, keyPath, leafCertValiditySeconds),
                List.of(localNetwork(jsonRpcUrl)),
                channels(privateKey, channelParams),
                DEFAULT_BACKOFF,
                peerProofTypes(channelParams));
    }

    /**
     * The <b>secure</b>-sync variant of {@link #relayConfig} with the default 86400-second leaf
     * validity. Delegates to {@link #secureRelayConfig(String, int, String, long, String, List)}.
     *
     * @param jsonRpcUrl       the Anvil JSON-RPC URL
     * @param secureSyncPort   the mandatory-mTLS sync port to bind (published to the peer as its target)
     * @param keyPath          path to this relay's CA PKCS#8 private key (DER/PEM)
     * @param privateKey       the signing key
     * @param channelParams the reduced ChannelConfig
     * @return a secure-sync relay configuration ready for {@link RelayInstance#build(RelayConfig)}
     */
    public static RelayConfig secureRelayConfig(
            final String jsonRpcUrl,
            final int secureSyncPort,
            final String keyPath,
            final String privateKey,
            final List<Triple<String, String, ProofType>> channelParams) {
        return secureRelayConfig(jsonRpcUrl, secureSyncPort, keyPath, 86400L, privateKey, channelParams);
    }

    /** The single QBFT local network targeting the Anvil chain, shared by both config variants. */
    private static RelayConfig.LocalNetworkConfig localNetwork(final String jsonRpcUrl) {
        return new RelayConfig.LocalNetworkConfig(
                "local",
                ProofType.QBFT,
                new RelayConfig.CommonEvmParams(
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
                        1),
                null,
                new RelayConfig.QbftConfig(30_000L, 5, 10, 1_048_576L),
                null);
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
