// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.endpoint;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Renders the {@code relay.yaml} an endpoint container is started with.
 *
 * <p>Written by hand rather than through a YAML library on purpose: the document's shape is part of
 * the contract with {@code RelayConfigLoader}, and spelling it out keeps the mapping between test
 * intent and config key visible. Every value is a hex string, an integer or an identifier, so there is
 * nothing to escape — and {@link #plain} <em>enforces</em> that rather than assuming it, because a
 * newline or YAML metacharacter slipping into an address or URL would corrupt the document silently
 * and the relay would fail to start with an unrelated-looking parse error.
 *
 * <p>Signing keys are <b>not</b> written into the document. Each service entry carries a
 * {@code __SIGNING_KEY_<slot>__} sentinel that {@code docker-entrypoint.sh} substitutes from the
 * matching {@code SIGNING_KEY_<slot>} environment variable at container start — the same mechanism
 * production uses to keep the key out of a Kubernetes ConfigMap. Reusing it here means the E2E path
 * exercises the real entrypoint rather than a test-only shortcut.
 */
public final class RelayYamlWriter {

    /** One local chain the endpoint talks to. */
    public record LocalNetwork(
            String id,
            long chainId,
            String jsonRpcUrl,
            long pollIntervalMs,
            long requestTimeoutMs,
            int maxRpcRetries,
            long maxGasPriceCapWei,
            int maxMessagesPerBundle,
            @Nullable ContractValidators contractValidators) {

        public LocalNetwork(
                final String id,
                final long chainId,
                final String jsonRpcUrl,
                final long pollIntervalMs,
                final long requestTimeoutMs,
                final int maxRpcRetries,
                final long maxGasPriceCapWei,
                final int maxMessagesPerBundle) {
            this(
                    id,
                    chainId,
                    jsonRpcUrl,
                    pollIntervalMs,
                    requestTimeoutMs,
                    maxRpcRetries,
                    maxGasPriceCapWei,
                    maxMessagesPerBundle,
                    null);
        }
    }

    /**
     * QBFT "contract" validator-selection mode parameters — see
     * {@code RelayConfig.QbftContractModeConfig}. Presence on a {@link LocalNetwork} selects
     * {@code proofType: QBFTContractMode} and a top-level {@code qbftContractMode:} block instead of
     * {@code proofType: QBFT} and a {@code qbft:} block.
     */
    public record ContractValidators(String validatorContractAddress, String arraySlotHex) {}

    /** One {@code ClprService} deployment the endpoint operates, with the channels it serves. */
    public record Service(
            String serviceAddress,
            String localNetworkId,
            String signingKeySlot,
            boolean discoverChannels,
            List<String> predefinedChannelIds) {
        public Service {
            predefinedChannelIds = List.copyOf(predefinedChannelIds);
        }
    }

    private final int grpcPort;
    private final long backoffBaseMs;
    private final long backoffCapMs;
    private final List<LocalNetwork> localNetworks = new ArrayList<>();
    private final List<Service> services = new ArrayList<>();
    private final Map<String, String> peerProofTypes = new LinkedHashMap<>();

    public RelayYamlWriter(final int grpcPort, final long backoffBaseMs, final long backoffCapMs) {
        this.grpcPort = grpcPort;
        this.backoffBaseMs = backoffBaseMs;
        this.backoffCapMs = backoffCapMs;
    }

    public RelayYamlWriter addLocalNetwork(final LocalNetwork network) {
        localNetworks.add(network);
        return this;
    }

    public RelayYamlWriter addService(final Service service) {
        services.add(service);
        return this;
    }

    /**
     * Map a peer's CAIP-2 chain id to a proof type.
     *
     * <p>Needed even for an all-QBFT topology: the relay resolves a channel's peer proof type from
     * the peer chain id it reads on-chain, and an unmapped non-EVM peer is skipped silently. Being
     * explicit avoids a channel that never comes up with no error to explain it.
     *
     * @param peerCaip2 e.g. {@code eip155:31338}
     * @param proofType e.g. {@code QBFT}
     * @return this writer
     */
    public RelayYamlWriter addPeerProofType(final String peerCaip2, final String proofType) {
        peerProofTypes.put(peerCaip2, proofType);
        return this;
    }

    /**
     * Reject a scalar that would need quoting or escaping in YAML.
     *
     * @param what field name, for the failure message
     * @param value the scalar to check
     * @return {@code value}, unchanged
     */
    private static String plain(final String what, final String value) {
        if (value == null) {
            throw new IllegalArgumentException(what + " must not be null");
        }
        if (value.chars().anyMatch(c -> c == '\n' || c == '\r' || c == '\t' || c < 0x20)) {
            throw new IllegalArgumentException(what + " must not contain control characters: '" + value + "'");
        }
        for (final char c :
                new char[] {'"', '\'', ':', '#', '{', '}', '[', ']', ',', '&', '*', '!', '|', '>', '%', '@', '`'}) {
            // ':' and '/' are legitimate inside a URL, so only flag it outside the scheme/host form.
            if (c == ':' && value.matches("^[a-zA-Z0-9+.-]+://[^\\s]*$")) {
                continue;
            }
            if (value.indexOf(c) >= 0) {
                throw new IllegalArgumentException(
                        what + " contains the YAML metacharacter '" + c + "' and would need quoting: '" + value + "'");
            }
        }
        return value;
    }

    /** @return the rendered YAML document. */
    public String render() {
        if (localNetworks.isEmpty()) {
            throw new IllegalStateException("relay.yaml needs at least one localNetworks entry");
        }
        final StringBuilder out = new StringBuilder(2048);
        out.append("# SPDX-License-Identifier: Apache-2.0\n");
        out.append("# Generated by the CLPR E2E framework — do not edit.\n\n");

        out.append("grpc:\n");
        out.append("  port: ").append(grpcPort).append('\n');
        // 1 MiB. The PBJ default of 10 KiB is far too small for a real bundle plus its QBFT proof,
        // and the failure mode is a truncated message rather than a clear error.
        out.append("  maxMessageSize: 1048576\n\n");

        out.append("localNetworks:\n");
        for (final LocalNetwork network : localNetworks) {
            out.append("  - id: ")
                    .append(plain("localNetworks[].id", network.id()))
                    .append('\n');
            final ContractValidators cv = network.contractValidators();
            out.append("    proofType: ")
                    .append(cv != null ? "QBFTContractMode" : "QBFT")
                    .append('\n');
            out.append("    evm:\n");
            out.append("      jsonRpcUrl: ")
                    .append(plain("evm.jsonRpcUrl", network.jsonRpcUrl()))
                    .append('\n');
            out.append("      chainId: ").append(network.chainId()).append('\n');
            out.append("      maxGasPriceCap: ")
                    .append(network.maxGasPriceCapWei())
                    .append('\n');
            out.append("      gasPriorityFee: 2000000000\n");
            out.append("      gasBufferMultiplier: 1.2\n");
            out.append("      pollIntervalMs: ")
                    .append(network.pollIntervalMs())
                    .append('\n');
            out.append("      requestTimeoutMs: ")
                    .append(network.requestTimeoutMs())
                    .append('\n');
            out.append("      maxRpcRetries: ").append(network.maxRpcRetries()).append('\n');
            if (cv == null) {
                out.append("    qbft:\n");
                out.append("      epochLength: 30000\n");
                out.append("      maxEpochBlockHeadersPerBundle: 5\n");
                out.append("      maxMessagesPerBundle: ")
                        .append(network.maxMessagesPerBundle())
                        .append('\n');
            } else {
                out.append("    qbftContractMode:\n");
                out.append("      validatorContractAddress: \"")
                        .append(plain("qbftContractMode.validatorContractAddress", cv.validatorContractAddress()))
                        .append("\"\n");
                out.append("      arraySlotHex: \"")
                        .append(plain("qbftContractMode.arraySlotHex", cv.arraySlotHex()))
                        .append("\"\n");
                out.append("      maxMessagesPerBundle: ")
                        .append(network.maxMessagesPerBundle())
                        .append('\n');
            }
        }
        out.append('\n');

        out.append("clprServices:\n");
        for (final Service service : services) {
            out.append("  - serviceAddress: \"")
                    .append(plain("clprServices[].serviceAddress", service.serviceAddress()))
                    .append("\"\n");
            out.append("    localNetwork: ")
                    .append(plain("clprServices[].localNetwork", service.localNetworkId()))
                    .append('\n');
            out.append("    defaultSigningPrivateKeyHex: \"__SIGNING_KEY_")
                    .append(service.signingKeySlot())
                    .append("__\"\n");
            out.append("    discoverChannels: ")
                    .append(service.discoverChannels())
                    .append('\n');
            out.append("    discoveryStartBlock: 0\n");
            if (service.predefinedChannelIds().isEmpty()) {
                out.append("    predefinedChannels: []\n");
            } else {
                out.append("    predefinedChannels:\n");
                for (final String channelId : service.predefinedChannelIds()) {
                    out.append("      - \"")
                            .append(plain("predefinedChannels[]", channelId))
                            .append("\"\n");
                }
            }
            out.append("    perChannelSigningPrivateKeyHex: {}\n");
        }
        out.append('\n');

        out.append("backoff:\n");
        out.append("  baseMs: ").append(backoffBaseMs).append('\n');
        out.append("  capMs: ").append(backoffCapMs).append('\n');

        if (!peerProofTypes.isEmpty()) {
            out.append('\n').append("peerProofTypes:\n");
            peerProofTypes.forEach((caip2, proofType) -> out.append("  \"")
                    .append(caip2)
                    .append("\": ")
                    .append(proofType)
                    .append('\n'));
        }
        return out.toString();
    }
}
