// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.config.extensions.sources.SimpleConfigSource;
import com.swirlds.config.extensions.sources.SystemPropertiesConfigSource;
import com.swirlds.logging.api.Logger;
import com.swirlds.logging.api.Loggers;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.hiero.clpr.relay.app.RelayConfig.GrpcInfoConfig;
import org.hiero.clpr.relay.app.RelayConfig.GrpcSyncConfig;
import org.jspecify.annotations.Nullable;

/**
 * Loads a {@link RelayConfig} from an optional YAML file, applying system-property overrides.
 *
 * <p>Configuration precedence (highest wins):
 * <ol>
 *   <li>System properties (or explicit overrides in the package-private test overload)</li>
 *   <li>YAML config file values</li>
 *   <li>Property defaults declared via {@link com.swirlds.config.api.ConfigProperty#defaultValue()}</li>
 * </ol>
 *
 * <p>Note that only the {@code grpc} and {@code backoff} scalar blocks are system-property
 * overridable (they are backed by {@code @ConfigData} records). The {@code localNetworks},
 * {@code clprServices}, and {@code peerProofTypes} sections are list/map shapes that can only be set
 * from the YAML file.
 *
 * <p>The config file is resolved in the following order:
 * <ol>
 *   <li>System property {@value #CONFIG_FILE_PROPERTY}</li>
 *   <li>Environment variable {@value #CONFIG_FILE_ENV_VAR}</li>
 *   <li>{@value #DEFAULT_CONFIG_FILENAME} in the current working directory (auto-fallback)</li>
 * </ol>
 *
 * <p>The YAML schema:
 * <pre>{@code
 * grpc:
 *   port: 9545
 * backoff:
 *   baseMs: 1000
 *   capMs: 30000
 * peerProofTypes:
 *   "eip155:1337": QBFT
 * localNetworks:
 *   - id: besu-local
 *     proofType: QBFT
 *     evm:
 *       jsonRpcUrl: "http://besu:8545"
 *       chainId: 1337
 *     qbft:
 *       epochLength: 30000
 *   - id: eth-mainnet            # proofType EthereumPOS: an EVM execution node + a beacon (CL) node
 *     proofType: EthereumPOS
 *     evm:
 *       jsonRpcUrl: "http://geth:8545"
 *       chainId: 1
 *     beacon:
 *       beaconApiUrl: "http://lighthouse:5052"
 *       maxMessagesPerBundle: 10
 *       maxRetries: 3
 *       requestTimeoutMs: 10000
 * clprServices:
 *   - serviceAddress: "0x..."
 *     localNetwork: besu-local
 *     defaultSigningPrivateKeyHex: "0x..."   # signs every channel lacking an override
 *     discoverChannels: false
 *     discoveryStartBlock: 0
 *     predefinedChannels:
 *       - "0x..."
 *     perChannelSigningPrivateKeyHex:
 *       "0x...": "0x..."
 * }</pre>
 */
final class RelayConfigLoader {

    private static final Logger log = Loggers.getLogger(RelayConfigLoader.class);

    /** System property naming the config file to load. */
    public static final String CONFIG_FILE_PROPERTY = "relay.configFile";
    /** Environment variable fallback for {@link #CONFIG_FILE_PROPERTY}. */
    public static final String CONFIG_FILE_ENV_VAR = "RELAY_CONFIG_FILE";
    /**
     * Name of the auto-discovered fallback config file in the working directory.
     * Intentionally NOT named {@code application.yaml} to avoid colliding with Helidon Config's
     * auto-discovery.
     */
    public static final String DEFAULT_CONFIG_FILENAME = "relay.yaml";

    // Ordinals mirror ConfigSourceOrdinalConstants from swirlds-config-extensions.
    private static final int SYSTEM_PROPERTIES_ORDINAL = 400;
    private static final int FILE_CONFIG_ORDINAL = 200;

    private RelayConfigLoader() {}

    /**
     * Load a {@link RelayConfig} using system-property and auto-discovered file sources.
     *
     * @return the merged configuration
     * @throws IllegalStateException if a config file is configured but cannot be read or parsed
     */
    public static RelayConfig load() {
        final var configPath = resolveConfigFilePath();
        final FileConfig fileConfig = configPath == null ? FileConfig.EMPTY : readYaml(configPath);
        final Configuration config = ConfigurationBuilder.create()
                .withConfigDataType(RelayConfig.GrpcConfig.class)
                .withConfigDataType(GrpcInfoConfig.class)
                .withConfigDataType(GrpcSyncConfig.class)
                .withConfigDataType(RelayConfig.BackoffConfig.class)
                .withSource(SystemPropertiesConfigSource.getInstance())
                .withSource(buildFileSource(fileConfig))
                .build();
        return assemble(config, fileConfig);
    }

    /**
     * Load a {@link RelayConfig} from the given file path (may be {@code null}) with explicit
     * overrides supplied as a {@code Properties}-like map. Exposed for testing.
     *
     * @param configPath optional path to a YAML file; if {@code null} only defaults + overrides
     *                   are used
     * @param overrides  system-property-style overrides (e.g. {@code relay.grpc.port=9545});
     *                   applied at the same priority level as system properties
     * @return the merged configuration
     * @throws IllegalStateException if {@code configPath} is non-null but missing, unreadable, or
     *                               malformed
     */
    static RelayConfig load(@Nullable final Path configPath, final Properties overrides) {
        final FileConfig fileConfig = configPath == null ? FileConfig.EMPTY : readYaml(configPath);
        final var overrideSource = new SimpleConfigSource().withOrdinal(SYSTEM_PROPERTIES_ORDINAL);
        overrides.forEach((k, v) -> overrideSource.withValue(k.toString(), v.toString()));
        final Configuration config = ConfigurationBuilder.create()
                .withConfigDataType(RelayConfig.GrpcConfig.class)
                .withConfigDataType(GrpcInfoConfig.class)
                .withConfigDataType(GrpcSyncConfig.class)
                .withConfigDataType(RelayConfig.BackoffConfig.class)
                .withSource(overrideSource)
                .withSource(buildFileSource(fileConfig))
                .build();
        return assemble(config, fileConfig);
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private static RelayConfig assemble(final Configuration config, final FileConfig fileConfig) {
        // Fall back to the built-in defaults when no peerProofTypes were provided — whether the block
        // was null/missing, explicitly empty, or there was no config file at all.
        final var peerProofTypes =
                fileConfig.peerProofTypes.isEmpty() ? RelayConfig.DEFAULT_PEER_PROOF_TYPES : fileConfig.peerProofTypes;
        final var info = config.getConfigData(GrpcInfoConfig.class);
        final var sync = config.getConfigData(GrpcSyncConfig.class);
        validateTransport(info, sync);

        final var grpc = config.getConfigData(RelayConfig.GrpcConfig.class);
        return new RelayConfig(
                grpc,
                info,
                sync,
                fileConfig.localNetworks,
                fileConfig.clprServices,
                config.getConfigData(RelayConfig.BackoffConfig.class),
                peerProofTypes);
    }

    /**
     * Cross-reference validation applied to a parsed file: each service must reference a known local
     * network and be able to sign for every channel it serves. Thrown from within {@link #readYaml}
     * so the failure surfaces as an {@link IllegalStateException} naming the offending file.
     */
    private static void validate(final FileConfig fc) {
        final var networkIds = new HashSet<String>();
        for (final var network : fc.localNetworks) {
            networkIds.add(network.id());
        }
        for (final var service : fc.clprServices) {
            if (!networkIds.contains(service.localNetwork())) {
                throw new IllegalArgumentException("clprServices['" + service.serviceAddress() + "'].localNetwork='"
                        + service.localNetwork() + "' does not reference a known localNetworks id");
            }
            validateSigningKeys(service);
        }
    }

    /**
     * A service must be able to sign for every channel it serves: discovered channels and
     * predefined channels without a per-channel override both fall back to the default key, so
     * the default must be present whenever either is the case.
     */
    private static void validateSigningKeys(final RelayConfig.ClprServiceConfig service) {
        final var overriddenIds = new HashSet<String>();
        service.perChannelSigningPrivateKeyHex()
                .keySet()
                .forEach(id -> overriddenIds.add(ClprServiceHandler.normaliseHex(id)));
        boolean needsDefault = service.discoverChannels();
        for (final var id : service.predefinedChannels()) {
            if (!overriddenIds.contains(ClprServiceHandler.normaliseHex(id))) {
                needsDefault = true;
            }
        }
        final var defaultKey = service.defaultSigningPrivateKeyHex();
        if (needsDefault && (defaultKey == null || defaultKey.isBlank())) {
            throw new IllegalArgumentException("clprServices['" + service.serviceAddress()
                    + "'] requires defaultSigningPrivateKeyHex: it discovers channels or has predefined"
                    + " channels without a per-channel signing key");
        }
    }

    /**
     * Fail-fast validation of the transport configuration. The info and sync listeners are always served
     * (both ports must be {@code > 0}) and must use distinct ports. The sync listener is mandatory mTLS
     * when {@code sync.tlsEnabled} is true, in which case {@code sync.tlsKeyPath} is required (and must be
     * readable); when disabled the key is ignored.
     *
     * <p>The certificate/key presence-and-readability check here is structural only — a best-effort
     * courtesy for a friendly early error. A file readable at load time may be gone or rotated by server
     * start, so it is <em>not</em> the authoritative guard: the certificate and key are fully loaded, and
     * the key matched to the certificate, when the {@code SSLContext} is built at server start.
     */
    private static void validateTransport(final GrpcInfoConfig info, final GrpcSyncConfig sync) {
        if (sync.port() <= 0) {
            throw new IllegalArgumentException("relay.grpc.sync.port must be > 0");
        }
        if (info.port() <= 0) {
            throw new IllegalArgumentException("relay.grpc.info.port must be > 0");
        }
        requireDistinct(sync.port(), info.port());

        // tlsKeyPath is mandatory when sync mTLS is enabled, and ignored otherwise.
        if (sync.tlsEnabled()) {
            if (sync.tlsKeyPath().isBlank()) {
                throw new IllegalArgumentException(
                        "relay.grpc.sync.tlsEnabled is true, so relay.grpc.sync.tlsKeyPath must be set");
            }
            if (!Files.isReadable(Path.of(sync.tlsKeyPath()))) {
                throw new IllegalArgumentException("\"relay.grpc.sync.tlsKeyPath\" does not exist or is not readable");
            }
        }
    }

    /** Configured (non-zero) ports must be pairwise distinct; disabled ports ({@code 0}) never collide. */
    private static void requireDistinct(final int... ports) {
        final var seen = new HashSet<Integer>();
        for (final int port : ports) {
            if (port > 0 && !seen.add(port)) {
                throw new IllegalArgumentException(
                        "all configured gRPC ports must differ; got " + Arrays.toString(ports));
            }
        }
    }

    /**
     * Build a {@link SimpleConfigSource} at {@link #FILE_CONFIG_ORDINAL} from parsed YAML values.
     * Only non-null fields are added so that missing YAML entries naturally fall through to
     * lower-priority sources (i.e. the {@code @ConfigProperty} defaults).
     */
    private static SimpleConfigSource buildFileSource(final FileConfig fc) {
        final var src = new SimpleConfigSource().withOrdinal(FILE_CONFIG_ORDINAL);
        if (fc.grpcMaxMessageSize != null) src.withValue("relay.grpc.maxMessageSize", fc.grpcMaxMessageSize);
        if (fc.infoPort != null) src.withValue("relay.grpc.info.port", fc.infoPort);
        if (fc.syncPort != null) src.withValue("relay.grpc.sync.port", fc.syncPort);
        if (fc.tlsEnabled != null) src.withValue("relay.grpc.sync.tlsEnabled", fc.tlsEnabled);
        if (fc.tlsKeyPath != null) src.withValue("relay.grpc.sync.tlsKeyPath", fc.tlsKeyPath);
        if (fc.backoffBaseMs != null) src.withValue("relay.backoff.baseMs", fc.backoffBaseMs);
        if (fc.backoffCapMs != null) src.withValue("relay.backoff.capMs", fc.backoffCapMs);
        return src;
    }

    @Nullable
    private static Path resolveConfigFilePath() {
        final var fromProp = System.getProperty(CONFIG_FILE_PROPERTY);
        if (fromProp != null && !fromProp.isBlank()) {
            return parsePath(CONFIG_FILE_PROPERTY, fromProp);
        }
        final var fromEnv = System.getenv(CONFIG_FILE_ENV_VAR);
        if (fromEnv != null && !fromEnv.isBlank()) {
            return parsePath(CONFIG_FILE_ENV_VAR, fromEnv);
        }
        final var fallback = Path.of(DEFAULT_CONFIG_FILENAME);
        return Files.exists(fallback) ? fallback : null;
    }

    private static Path parsePath(final String source, final String value) {
        try {
            return Path.of(value);
        } catch (final java.nio.file.InvalidPathException e) {
            throw new IllegalStateException("Invalid path for " + source + "='" + value + "': " + e.getMessage(), e);
        }
    }

    private static FileConfig readYaml(final Path path) {
        if (!Files.exists(path)) {
            throw new IllegalStateException("Relay config file not found: " + path.toAbsolutePath());
        }
        if (!Files.isReadable(path)) {
            throw new IllegalStateException("Relay config file is not readable: " + path.toAbsolutePath());
        }
        final var mapper = new ObjectMapper(new YAMLFactory());
        final Map<String, Object> root;
        try (var in = Files.newInputStream(path)) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> parsed = mapper.readValue(in, Map.class);
            root = parsed == null ? Map.of() : parsed;
        } catch (final IOException e) {
            throw new IllegalStateException(
                    "Failed to read relay config file: " + path.toAbsolutePath() + " (" + e.getMessage() + ")", e);
        } catch (final RuntimeException e) {
            throw new IllegalStateException(
                    "Malformed relay config file: " + path.toAbsolutePath() + " (" + e.getMessage() + ")", e);
        }

        try {
            final var fc = FileConfig.fromMap(root);
            validate(fc);
            return fc;
        } catch (final RuntimeException e) {
            throw new IllegalStateException(
                    "Invalid relay config file: " + path.toAbsolutePath() + " (" + e.getMessage() + ")", e);
        }
    }

    // -------------------------------------------------------------------------
    // Parsed file shape (all scalar fields optional / nullable)
    // -------------------------------------------------------------------------

    private static final class FileConfig {
        static final FileConfig EMPTY = new FileConfig();

        @Nullable
        Integer grpcMaxMessageSize;

        @Nullable
        Integer infoPort;

        @Nullable
        Integer syncPort;

        @Nullable
        Boolean tlsEnabled;

        @Nullable
        String tlsKeyPath;

        @Nullable
        Long backoffBaseMs;

        @Nullable
        Long backoffCapMs;

        List<RelayConfig.LocalNetworkConfig> localNetworks = List.of();
        List<RelayConfig.ClprServiceConfig> clprServices = List.of();
        Map<String, ProofType> peerProofTypes = Map.of();

        static FileConfig fromMap(final Map<String, Object> root) {
            final var fc = new FileConfig();

            final var grpc = asMap(root.get("grpc"));
            if (grpc != null) {
                fc.grpcMaxMessageSize = asInt(grpc.get("maxMessageSize"));
                final var info = asMap(grpc.get("info"));
                if (info != null) {
                    fc.infoPort = asInt(info.get("port"));
                }
                final var sync = asMap(grpc.get("sync"));
                if (sync != null) {
                    fc.syncPort = asInt(sync.get("port"));
                    fc.tlsEnabled = asBool(sync.get("tlsEnabled"));
                    fc.tlsKeyPath = asString(sync.get("tlsKeyPath"));
                }
            }

            final var backoff = asMap(root.get("backoff"));
            if (backoff != null) {
                fc.backoffBaseMs = asLong(backoff.get("baseMs"));
                fc.backoffCapMs = asLong(backoff.get("capMs"));
            }

            fc.localNetworks = parseLocalNetworksList(root.get("localNetworks"));
            fc.clprServices = parseClprServicesList(root.get("clprServices"));
            fc.peerProofTypes = parsePeerProofTypes(root.get("peerProofTypes"));

            return fc;
        }

        /**
         * Parse the optional top-level {@code peerProofTypes} block: a map of peer CAIP-2
         * {@code chainId} to {@link ProofType}. {@code null} maps to an empty map. Proof-type values
         * are resolved case-insensitively (as elsewhere); an unknown value fails loudly.
         */
        private static Map<String, ProofType> parsePeerProofTypes(@Nullable final Object raw) {
            if (raw == null) {
                return Map.of();
            }
            final var map = asMap(raw);
            if (map == null) {
                throw new IllegalArgumentException(
                        "'peerProofTypes' must be a map of chainId -> proofType; got: " + raw);
            }
            final var result = new LinkedHashMap<String, ProofType>();
            for (final var entry : map.entrySet()) {
                final var chainId = entry.getKey();
                final var proofTypeStr = asString(entry.getValue());
                if (proofTypeStr == null) {
                    throw new IllegalArgumentException("peerProofTypes['" + chainId + "'] must be a proof type");
                }
                result.put(chainId, parseProofType(proofTypeStr, "peerProofTypes['" + chainId + "']"));
            }
            return Collections.unmodifiableMap(result);
        }

        private static List<RelayConfig.LocalNetworkConfig> parseLocalNetworksList(@Nullable final Object raw) {
            if (raw == null) {
                return List.of();
            }
            if (!(raw instanceof List<?> list)) {
                throw new IllegalArgumentException("'localNetworks' must be a list; got: " + raw);
            }
            final var result = new ArrayList<RelayConfig.LocalNetworkConfig>(list.size());
            for (final var item : list) {
                final var netMap = asMap(item);
                if (netMap == null) {
                    throw new IllegalArgumentException("localNetworks[] entries must be objects; got: " + item);
                }
                result.add(parseLocalNetwork(netMap));
            }
            return Collections.unmodifiableList(result);
        }

        private static RelayConfig.LocalNetworkConfig parseLocalNetwork(final Map<String, Object> netMap) {
            final var id = asString(netMap.get("id"));
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("localNetworks[].id is required");
            }
            final var proofTypeStr = asString(netMap.get("proofType"));
            if (proofTypeStr == null) {
                throw new IllegalArgumentException("localNetworks['" + id + "'].proofType is required");
            }
            final ProofType proofType = parseProofType(proofTypeStr, "localNetworks['" + id + "'].proofType");
            return switch (proofType) {
                case QBFT ->
                    new RelayConfig.LocalNetworkConfig(
                            id,
                            proofType,
                            parseRequiredEvm(netMap, id),
                            null,
                            parseQbftConfig(asMap(netMap.get("qbft"))),
                            null);
                case CometBFT ->
                    new RelayConfig.LocalNetworkConfig(
                            id,
                            proofType,
                            parseRequiredEvm(netMap, id),
                            parseCometBftConfig(asMap(netMap.get("cometBft"))),
                            null,
                            null);
                case EthereumPOS ->
                    new RelayConfig.LocalNetworkConfig(
                            id,
                            proofType,
                            parseRequiredEvm(netMap, id),
                            null,
                            null,
                            parseBeaconConfig(asMap(netMap.get("beacon"))));
                case Hiero ->
                    throw new IllegalArgumentException(
                            "localNetworks['" + id
                                    + "'].proofType=Hiero is not supported for a local network; the EVM relay only submits to"
                                    + " QBFT, CometBFT or EthereumPOS chains (Hiero may still be used as a channel's peerProofType)");
                case Stub ->
                    // Test-only: a Stub local network builds CLPRSTUB bundles from real channel state
                    // for the StubClprVerifier (integration tests), so it needs only the EVM block —
                    // no qbft/cometBft/beacon proof parameters. Never valid against a real production chain.
                    new RelayConfig.LocalNetworkConfig(id, proofType, parseRequiredEvm(netMap, id), null, null, null);
            };
        }

        /** Parse the required per-network {@code evm} channel block, failing if it is absent. */
        private static RelayConfig.CommonEvmParams parseRequiredEvm(final Map<String, Object> netMap, final String id) {
            final var evmRaw = asMap(netMap.get("evm"));
            if (evmRaw == null) {
                throw new IllegalArgumentException("localNetworks['" + id + "'] requires an 'evm' block");
            }
            return parseCommonEvmParams(evmRaw);
        }

        private static List<RelayConfig.ClprServiceConfig> parseClprServicesList(@Nullable final Object raw) {
            if (raw == null) {
                return List.of();
            }
            if (!(raw instanceof List<?> list)) {
                throw new IllegalArgumentException("'clprServices' must be a list; got: " + raw);
            }
            final var result = new ArrayList<RelayConfig.ClprServiceConfig>(list.size());
            for (final var item : list) {
                final var svcMap = asMap(item);
                if (svcMap == null) {
                    throw new IllegalArgumentException("clprServices[] entries must be objects; got: " + item);
                }
                result.add(parseClprService(svcMap));
            }
            return Collections.unmodifiableList(result);
        }

        private static RelayConfig.ClprServiceConfig parseClprService(final Map<String, Object> svcMap) {
            final var serviceAddress = asString(svcMap.get("serviceAddress"));
            if (serviceAddress == null || serviceAddress.isBlank()) {
                throw new IllegalArgumentException("clprServices[].serviceAddress is required");
            }
            final var localNetwork = asString(svcMap.get("localNetwork"));
            if (localNetwork == null || localNetwork.isBlank()) {
                throw new IllegalArgumentException("clprServices['" + serviceAddress + "'].localNetwork is required");
            }
            final var defaultKey = asString(svcMap.get("defaultSigningPrivateKeyHex"));
            final boolean discoverChannels = Boolean.TRUE.equals(asBool(svcMap.get("discoverChannels")));
            final Long discoveryStartBlockRaw = asLong(svcMap.get("discoveryStartBlock"));
            final long discoveryStartBlock = discoveryStartBlockRaw != null ? discoveryStartBlockRaw : 0L;
            final List<String> predefinedChannels = asStringList(svcMap.get("predefinedChannels"));
            final Map<String, String> perChannelKeys =
                    parseStringMap(svcMap.get("perChannelSigningPrivateKeyHex"), serviceAddress);
            return new RelayConfig.ClprServiceConfig(
                    defaultKey != null ? defaultKey : "",
                    localNetwork,
                    serviceAddress,
                    discoverChannels,
                    discoveryStartBlock,
                    predefinedChannels,
                    perChannelKeys);
        }

        /** Parse a YAML {@code String -> String} map (e.g. per-channel signing keys). */
        private static Map<String, String> parseStringMap(@Nullable final Object raw, final String context) {
            if (raw == null) {
                return Map.of();
            }
            final var map = asMap(raw);
            if (map == null) {
                throw new IllegalArgumentException(
                        "clprServices['" + context + "'].perChannelSigningPrivateKeyHex must be a map; got: " + raw);
            }
            final var result = new LinkedHashMap<String, String>();
            for (final var entry : map.entrySet()) {
                result.put(entry.getKey(), asString(entry.getValue()));
            }
            return Collections.unmodifiableMap(result);
        }

        private static RelayConfig.CommonEvmParams parseCommonEvmParams(final Map<String, Object> evm) {
            final String jsonRpcUrl = asString(evm.get("jsonRpcUrl"));
            final Long chainId = asLong(evm.get("chainId"));
            final Long maxGasPriceCap = asLong(evm.get("maxGasPriceCap"));
            final Long gasPriorityFee = asLong(evm.get("gasPriorityFee"));
            final Double gasBufferMultiplier = asDouble(evm.get("gasBufferMultiplier"));
            final Long pollIntervalMs = asLong(evm.get("pollIntervalMs"));
            final Long requestTimeoutMs = asLong(evm.get("requestTimeoutMs"));
            final Integer maxRpcRetries = asInt(evm.get("maxRpcRetries"));
            return new RelayConfig.CommonEvmParams(
                    jsonRpcUrl != null ? jsonRpcUrl : "http://localhost:8545",
                    chainId != null ? chainId : 1L,
                    maxGasPriceCap != null ? maxGasPriceCap : Long.MAX_VALUE,
                    gasPriorityFee != null ? gasPriorityFee : 2_000_000_000L,
                    gasBufferMultiplier != null ? gasBufferMultiplier : 1.2,
                    pollIntervalMs != null ? pollIntervalMs : 1000L,
                    requestTimeoutMs != null ? requestTimeoutMs : 30_000L,
                    maxRpcRetries != null ? maxRpcRetries : 3);
        }

        /**
         * Parse the optional per-network {@code qbft} block (present for QBFT networks). Missing
         * fields fall back to their defaults; {@code null} input yields an all-defaults block.
         */
        private static RelayConfig.QbftConfig parseQbftConfig(@Nullable final Map<String, Object> qbft) {
            final Map<String, Object> q = qbft != null ? qbft : Map.of();
            final Long epochLength = asLong(q.get("epochLength"));
            final Integer maxEpochBlockHeadersPerBundle = asInt(q.get("maxEpochBlockHeadersPerBundle"));
            final Integer maxMessagesPerBundle = asInt(q.get("maxMessagesPerBundle"));
            final Long maxBundleSize = asLong(q.get("maxBundleSize"));
            return new RelayConfig.QbftConfig(
                    epochLength != null ? epochLength : 30_000L,
                    maxEpochBlockHeadersPerBundle != null ? maxEpochBlockHeadersPerBundle : 5,
                    maxMessagesPerBundle != null ? maxMessagesPerBundle : 10,
                    maxBundleSize != null ? maxBundleSize : 1_048_576L);
        }

        /**
         * Parse the optional per-network {@code cometBft} block (present for CometBFT networks).
         * Missing fields fall back to their defaults; {@code null} input yields an all-defaults block.
         */
        private static RelayConfig.CometBftConfig parseCometBftConfig(@Nullable final Map<String, Object> cometBft) {
            final Map<String, Object> c = cometBft != null ? cometBft : Map.of();
            final String cometBftRpcUrl = asString(c.get("cometBftRpcUrl"));
            final Integer maxMessagesPerBundle = asInt(c.get("maxMessagesPerBundle"));
            final Integer maxPriorValidatorSetUpdates = asInt(c.get("maxPriorValidatorSetUpdates"));
            final Integer maxRetries = asInt(c.get("maxRetries"));
            final Long requestTimeoutMs = asLong(c.get("requestTimeoutMs"));
            final Long maxBundleSize = asLong(c.get("maxBundleSize"));
            return new RelayConfig.CometBftConfig(
                    cometBftRpcUrl != null ? cometBftRpcUrl : "http://localhost:26657",
                    maxMessagesPerBundle != null ? maxMessagesPerBundle : 10,
                    maxPriorValidatorSetUpdates != null ? maxPriorValidatorSetUpdates : 10,
                    maxRetries != null ? maxRetries : 3,
                    requestTimeoutMs != null ? requestTimeoutMs : 5_000L,
                    maxBundleSize != null ? maxBundleSize : 1_048_576L);
        }

        private static RelayConfig.BeaconConfig parseBeaconConfig(@Nullable final Map<String, Object> beacon) {
            final Map<String, Object> b = beacon != null ? beacon : Map.of();
            final String beaconApiUrl = asString(b.get("beaconApiUrl"));
            final Integer maxMessagesPerBundle = asInt(b.get("maxMessagesPerBundle"));
            final Integer maxRetries = asInt(b.get("maxRetries"));
            final Long requestTimeoutMs = asLong(b.get("requestTimeoutMs"));
            return new RelayConfig.BeaconConfig(
                    beaconApiUrl != null ? beaconApiUrl : "http://localhost:5052",
                    maxMessagesPerBundle != null ? maxMessagesPerBundle : 10,
                    maxRetries != null ? maxRetries : 3,
                    requestTimeoutMs != null ? requestTimeoutMs : 30_000L);
        }

        /**
         * Resolve a {@code proofType}/{@code peerProofType} string to a {@link ProofType},
         * case-insensitively (so YAML may use {@code qbft}, {@code QBFT}, {@code CometBFT}, …).
         */
        private static ProofType parseProofType(final String raw, final String context) {
            for (final var ct : ProofType.values()) {
                if (ct.name().equalsIgnoreCase(raw.trim())) {
                    return ct;
                }
            }
            throw new IllegalArgumentException(context + "='" + raw + "' is not a valid proof type (expected one of: "
                    + Arrays.toString(ProofType.values()) + ")");
        }

        @Nullable
        @SuppressWarnings("unchecked")
        private static Map<String, Object> asMap(@Nullable final Object o) {
            if (o == null) {
                return null;
            }
            if (o instanceof Map<?, ?> m) {
                return (Map<String, Object>) m;
            }
            throw new IllegalArgumentException("Expected an object, got: " + o);
        }

        @Nullable
        private static String asString(@Nullable final Object o) {
            if (o == null) {
                return null;
            }
            return o.toString();
        }

        @Nullable
        private static Integer asInt(@Nullable final Object o) {
            if (o == null) {
                return null;
            }
            if (o instanceof Number n) {
                return n.intValue();
            }
            return Integer.parseInt(o.toString().trim());
        }

        @Nullable
        private static Long asLong(@Nullable final Object o) {
            if (o == null) {
                return null;
            }
            if (o instanceof Number n) {
                return n.longValue();
            }
            return Long.parseLong(o.toString().trim());
        }

        @Nullable
        private static Double asDouble(@Nullable final Object o) {
            if (o == null) {
                return null;
            }
            if (o instanceof Number n) {
                return n.doubleValue();
            }
            return Double.parseDouble(o.toString().trim());
        }

        /**
         * Coerce a YAML value into a {@code List<String>}: accept a list (each entry stringified,
         * trimmed, blanks skipped), accept a lone string (wrapped as a singleton), and map
         * {@code null} to an empty list. Never returns {@code null}.
         */
        private static List<String> asStringList(@Nullable final Object o) {
            if (o == null) {
                return List.of();
            }
            final var result = new ArrayList<String>();
            if (o instanceof List<?> list) {
                for (final var item : list) {
                    if (item == null) {
                        continue;
                    }
                    final var s = item.toString().trim();
                    if (!s.isEmpty()) {
                        result.add(s);
                    }
                }
            } else {
                final var s = o.toString().trim();
                if (!s.isEmpty()) {
                    result.add(s);
                }
            }
            return Collections.unmodifiableList(result);
        }

        @Nullable
        private static Boolean asBool(@Nullable final Object o) {
            if (o == null) {
                return null;
            }
            if (o instanceof Boolean b) {
                return b;
            }
            return Boolean.parseBoolean(o.toString().trim());
        }
    }
}
