// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.endpoint;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.hiero.clpr.relay.e2e.LogStream;
import org.hiero.clpr.relay.e2e.chain.BesuNetwork;
import org.hiero.clpr.relay.e2e.chain.TestAccount;
import org.jspecify.annotations.Nullable;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.Transferable;

/**
 * One containerized {@code clpr-evm-endpoint} process — the system under test.
 *
 * <p>Deliberately a black box. The existing integration suite builds a {@code RelayInstance}
 * in-process, which gives it the metrics registry and a submitter-decorator seam; neither exists
 * here. Everything is observed the way an operator would: the Prometheus scrape, the container log,
 * and the chain itself. That is the point — it is also what forces the real {@code QBFTVerifier} to
 * be satisfied rather than a stub.
 *
 * <p>The container's address is pinned, and a restart reuses it. {@code ClprService} validates a
 * channel's seed endpoints as IPv4 literals, so an endpoint's address is baked into the on-chain
 * peer roster at channel-wiring time; if a restart moved it, every peer would keep dialling an
 * address nothing answers on.
 */
public final class EvmEndpoint {

    /** gRPC sync (data plane) port inside the container. */
    public static final int SYNC_PORT = 9545;

    /** gRPC info plane port inside the container. */
    public static final int INFO_PORT = 9546;

    /** Prometheus {@code /metrics} and {@code /healthz} port inside the container. */
    public static final int METRICS_PORT = 9547;

    private final EvmEndpointSpec spec;
    private final TestAccount signer;
    private final String staticIp;
    private final Network network;
    private final ImageFromDockerfile image;
    private final int heapMb;
    private final LogStream logStream;
    private final HttpClient http;

    private final List<RelayYamlWriter.LocalNetwork> localNetworks = new ArrayList<>();
    private final List<RelayYamlWriter.Service> services = new ArrayList<>();
    private final Map<String, String> peerProofTypes = new LinkedHashMap<>();

    @Nullable
    private GenericContainer<?> container;

    EvmEndpoint(
            final EvmEndpointSpec spec,
            final TestAccount signer,
            final String staticIp,
            final Network network,
            final ImageFromDockerfile image,
            final int heapMb) {
        this.spec = spec;
        this.signer = signer;
        this.staticIp = staticIp;
        this.network = network;
        this.image = image;
        this.heapMb = heapMb;
        this.logStream = new LogStream(spec.id());
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    /** @return the endpoint id, also its container alias. */
    public String id() {
        return spec.id();
    }

    /** @return the spec this endpoint was declared with. */
    public EvmEndpointSpec spec() {
        return spec;
    }

    /** @return the account whose key the relay signs transactions and sync payloads with. */
    public TestAccount signerAccount() {
        return signer;
    }

    /** @return the EVM address the relay submits from. */
    public String signingAddress() {
        return signer.address();
    }

    /** @return the pinned container address, which peers dial and the on-chain roster carries. */
    public String containerIp() {
        return staticIp;
    }

    /** @return the captured container output; survives restarts and container removal. */
    public LogStream logs() {
        return logStream;
    }

    // ── configuration (only while stopped) ────────────────────────────────────

    /**
     * Declare a chain this endpoint reads and submits on.
     *
     * @param network the local network entry
     * @return this endpoint
     */
    public EvmEndpoint addLocalNetwork(final RelayYamlWriter.LocalNetwork network) {
        throwIfRunning("addLocalNetwork");
        localNetworks.add(network);
        return this;
    }

    /**
     * Declare a {@code ClprService} deployment this endpoint operates.
     *
     * @param service the service entry
     * @return this endpoint
     */
    public EvmEndpoint addService(final RelayYamlWriter.Service service) {
        throwIfRunning("addService");
        services.add(service);
        return this;
    }

    /**
     * Map a peer chain id to its proof type.
     *
     * @param peerCaip2 peer CAIP-2 chain id
     * @param proofType the proof type name
     * @return this endpoint
     */
    public EvmEndpoint addPeerProofType(final String peerCaip2, final String proofType) {
        throwIfRunning("addPeerProofType");
        peerProofTypes.put(peerCaip2, proofType);
        return this;
    }

    /**
     * Declare that this endpoint operates {@code local}'s {@code ClprService}, serving the given
     * channels.
     *
     * <p>The single place the {@code localNetworks} + {@code clprServices} pair is built, so the
     * channel wiring and a hand-configured endpoint cannot drift apart.
     *
     * <p>Pass an <b>empty</b> list for a discovery endpoint: with
     * {@link EvmEndpointSpec#discoverChannels()} enabled the relay polls the service for
     * {@code ChannelCompleted} events and picks up channels registered after it started, so no ids
     * need pinning up front. With discovery disabled an empty list means the endpoint serves nothing.
     *
     * <p>Peer proof types are separate ({@link #addPeerProofType}) because one endpoint can face
     * several peer chains.
     *
     * @param local the chain whose service this endpoint operates; must already be deployed
     * @param channelIds channel ids to bring up at startup, {@code 0x}-prefixed
     * @return this endpoint
     */
    public EvmEndpoint serve(final BesuNetwork local, final List<String> channelIds) {
        return addLocalNetwork(new RelayYamlWriter.LocalNetwork(
                        local.id(),
                        local.chainId(),
                        // The in-network URL: the relay dials the node from inside the Docker network.
                        // Resolves to the RPC filter proxy's alias when the spec opted in, so a test can
                        // block specific methods without freezing the whole chain.
                        local.endpointRpcUrl(),
                        spec.pollInterval().toMillis(),
                        spec.requestTimeoutMs(),
                        spec.maxRpcRetries(),
                        spec.maxGasPriceCapWei(),
                        spec.maxMessagesPerBundle()))
                .addService(new RelayYamlWriter.Service(
                        local.contracts().clprService(),
                        local.id(),
                        signingKeySlot(),
                        spec.discoverChannels(),
                        channelIds));
    }

    /** @return the signing-key slot name used in the config sentinel and the env var. */
    public String signingKeySlot() {
        return spec.id().replaceAll("[^A-Za-z0-9]", "_").toUpperCase(Locale.ROOT);
    }

    /** @return the {@code relay.yaml} this endpoint would start with. */
    public String renderConfig() {
        final RelayYamlWriter writer =
                new RelayYamlWriter(SYNC_PORT, INFO_PORT, spec.backoffBaseMs(), spec.backoffCapMs());
        localNetworks.forEach(writer::addLocalNetwork);
        services.forEach(writer::addService);
        peerProofTypes.forEach(writer::addPeerProofType);
        return writer.render();
    }

    // ── lifecycle ─────────────────────────────────────────────────────────────

    /** @return {@code true} if the container is up. */
    public boolean isRunning() {
        return container != null && container.isRunning();
    }

    /**
     * Start the container, waiting until the process serves {@code /healthz}.
     *
     * <p>{@code /healthz} means "the JVM is up and the metrics server is listening", not "the
     * channels are live" — the relay starts the metrics server before wiring channels. Channel
     * readiness is a separate, metric-based wait, because a channel whose on-chain protocol version
     * mismatches is skipped silently and would otherwise look like a healthy endpoint.
     */
    public void start() {
        if (isRunning()) {
            return;
        }
        final String configYaml = renderConfig();
        final GenericContainer<?> c = new GenericContainer<>(image)
                .withCreateContainerCmdModifier(
                        cmd -> cmd.withHostName(spec.id()).withIpv4Address(staticIp))
                .withNetwork(network)
                .withNetworkAliases(spec.id())
                .withExposedPorts(SYNC_PORT, INFO_PORT, METRICS_PORT)
                .withCopyToContainer(Transferable.of(configYaml), "/app/relay.yaml")
                // The production entrypoint substitutes SIGNING_KEY_* into the matching
                // __SIGNING_KEY_*__ sentinel, so the key never appears in the config document.
                .withEnv("SIGNING_KEY_" + signingKeySlot(), signer.privateKeyHex())
                .withEnv("JAVA_OPTS", javaOpts())
                .withLogConsumer(logStream.consumer())
                .waitingFor(Wait.forHttp("/healthz")
                        .forPort(METRICS_PORT)
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(2)));
        c.start();
        this.container = c;
    }

    private String javaOpts() {
        final List<String> opts = new ArrayList<>();
        opts.add("-Xmx" + heapMb + "m");
        opts.addAll(spec.jvmOpts());
        return String.join(" ", opts);
    }

    /** Graceful stop (SIGTERM), giving the relay's shutdown hook a chance to run. */
    public void stop() {
        if (container != null) {
            container.stop();
            container = null;
        }
    }

    /** Release the HTTP client behind {@link #metrics()} and {@link #isHealthy()}. */
    void closeHttpClient() {
        http.close();
    }

    /**
     * Stop and start again with the same key, address and configuration.
     *
     * <p>The relay keeps no local state — everything it needs is on chain — so a restart must resume
     * exactly where it left off. That is the property the restart-under-load test checks.
     */
    public void restart() {
        stop();
        start();
    }

    /**
     * SIGKILL. No shutdown hook, no graceful drain — an abrupt crash.
     *
     * <p>Clears the container handle afterwards, matching {@link #stop()}: the process is gone, so
     * leaving the field pointing at a dead container would make {@code isRunning()} the only thing
     * telling the truth. Captured logs survive in the {@link LogStream}. Call {@link #start()} to
     * bring it back.
     */
    public void kill() {
        final GenericContainer<?> c = requireContainer();
        c.getDockerClient().killContainerCmd(c.getContainerId()).exec();
        container = null;
    }

    /** Freeze the process, keeping ports mapped so peers see timeouts rather than refusals. */
    public void pause() {
        final GenericContainer<?> c = requireContainer();
        c.getDockerClient().pauseContainerCmd(c.getContainerId()).exec();
    }

    /** Resume a paused container. */
    public void unpause() {
        final GenericContainer<?> c = requireContainer();
        c.getDockerClient().unpauseContainerCmd(c.getContainerId()).exec();
    }

    // ── observation ───────────────────────────────────────────────────────────

    /**
     * @return the Docker container id, for operations Testcontainers' wrapper does not expose (network
     *     attach/detach, kill, pause)
     */
    public String containerId() {
        return requireContainer().getContainerId();
    }

    /** @return host-mapped sync port. */
    public int syncPort() {
        return requireContainer().getMappedPort(SYNC_PORT);
    }

    /** @return host-mapped info port. */
    public int infoPort() {
        return requireContainer().getMappedPort(INFO_PORT);
    }

    /** @return host-mapped metrics port. */
    public int metricsPort() {
        return requireContainer().getMappedPort(METRICS_PORT);
    }

    /**
     * Scrape and parse {@code /metrics}.
     *
     * <p>A fresh snapshot each call: tests that wait for a counter to move must re-scrape, and
     * returning a cached reading would make such a wait spin forever.
     *
     * @return the parsed snapshot
     */
    public PrometheusScrape metrics() {
        return PrometheusScrape.parse(httpGet("/metrics"));
    }

    /** @return {@code true} if {@code /healthz} answers 200. */
    public boolean isHealthy() {
        try {
            return httpGet("/healthz").contains("ok");
        } catch (final RuntimeException e) {
            return false;
        }
    }

    private String httpGet(final String path) {
        final GenericContainer<?> c = requireContainer();
        final URI uri = URI.create("http://" + c.getHost() + ":" + c.getMappedPort(METRICS_PORT) + path);
        final HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(15))
                .GET()
                .build();
        try {
            final HttpResponse<String> response =
                    http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() != 200) {
                throw new IllegalStateException(
                        "GET " + uri + " returned HTTP " + response.statusCode() + ": " + response.body());
            }
            return response.body();
        } catch (final IOException e) {
            throw new UncheckedIOException("GET " + uri + " failed", e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted during GET " + uri, e);
        }
    }

    private GenericContainer<?> requireContainer() {
        final GenericContainer<?> c = container;
        if (c == null) {
            throw new IllegalStateException("endpoint " + spec.id() + " is not started");
        }
        return c;
    }

    private void throwIfRunning(final String operation) {
        if (isRunning()) {
            throw new IllegalStateException(operation + " on endpoint " + spec.id()
                    + " is only allowed while it is stopped — the relay reads its configuration once at startup,"
                    + " so a change made now would be silently ignored");
        }
    }
}
