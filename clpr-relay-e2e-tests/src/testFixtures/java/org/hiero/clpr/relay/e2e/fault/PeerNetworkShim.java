// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.fault;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpoint;
import org.jspecify.annotations.Nullable;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * A per-environment Toxiproxy sidecar that intercepts endpoint-to-endpoint sync traffic so tests can
 * disconnect two relays without touching their chains.
 *
 * <p>The relay's own {@code partition(EvmEndpoint)} detaches the container from the Docker network
 * entirely, which is fine for "endpoint disappears" but rolls two failures into one: the endpoint
 * also loses its EVM node. Peer-to-peer connection issues need a scalpel that only cuts the peer
 * plane.
 *
 * <p>The trick is the on-chain peer roster. It stores each endpoint's dial address verbatim (spec
 * §2.4.2), so if that address is a Toxiproxy proxy on this container's IP, every peer dials through
 * the proxy and disabling it drops the connection cleanly. Chain traffic keeps its own path
 * (endpoint → besu on the same network) and is unaffected.
 *
 * <p>Opt-in per test via {@code env.enablePeerNetworkShim()} <em>before</em> {@code startAll()}:
 * once the channel wiring has published dial addresses on chain, rerouting is impossible. Existing
 * tests that never call the opt-in pay nothing — no container starts.
 *
 * <p><b>Scope.</b> Toxiproxy proxies are per-<em>destination</em>, so disabling the proxy for
 * endpoint B drops <em>every</em> peer's inbound to B, not just A's. In the current suite's
 * two-endpoint topologies that is exactly bidirectional A↔B, but a future three-endpoint test would
 * need per-source proxies (one Toxiproxy proxy per ordered pair) to isolate a single pair.
 */
public final class PeerNetworkShim implements AutoCloseable {

    /** Toxiproxy image tag. Pinned so the shim behaves the same across CI runs and laptops. */
    private static final String TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.9.0";

    /** Toxiproxy's built-in HTTP control port. */
    private static final int API_PORT = 8474;

    /** First port for a peer proxy inside the container. Above the ephemeral range to avoid clashes. */
    private static final int PROXY_PORT_BASE = 40_000;

    private final Network dockerNetwork;
    private final String staticIp;
    private final HttpClient http;

    /** Endpoints registered with the shim, in registration order. Order fixes the port assignment. */
    private final Map<String, RegisteredEndpoint> registered = new LinkedHashMap<>();

    @Nullable
    private GenericContainer<?> container;

    /** Base URL of Toxiproxy's HTTP API from the test JVM, e.g. {@code http://localhost:32770}. */
    @Nullable
    private String apiBaseUrl;

    public PeerNetworkShim(final Network dockerNetwork, final String staticIp) {
        this.dockerNetwork = dockerNetwork;
        this.staticIp = staticIp;
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    /** @return {@code true} if the Toxiproxy container is running. */
    public boolean isStarted() {
        return container != null && container.isRunning();
    }

    /**
     * Start the Toxiproxy container. Idempotent.
     *
     * <p>Waits on the API's {@code /version} endpoint rather than a log line: the log message
     * predates the API being ready to accept proxy definitions by a few hundred milliseconds, which
     * is long enough for the first {@code registerPeerProxy} call to see connection-refused.
     */
    public void start() {
        if (isStarted()) {
            return;
        }
        final GenericContainer<?> c = new GenericContainer<>(TOXIPROXY_IMAGE)
                .withCreateContainerCmdModifier(
                        cmd -> cmd.withHostName("toxiproxy").withIpv4Address(staticIp))
                .withNetwork(dockerNetwork)
                .withNetworkAliases("toxiproxy")
                .withExposedPorts(API_PORT)
                // Bind the API to all interfaces so the test JVM can reach it via the host-mapped port,
                // AND so proxies bound to `staticIp:<port>` are reachable from other containers.
                .withCommand("-host=0.0.0.0", "-port=" + API_PORT)
                .waitingFor(Wait.forHttp("/version")
                        .forPort(API_PORT)
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofSeconds(60)));
        c.start();
        this.container = c;
        this.apiBaseUrl = "http://" + c.getHost() + ":" + c.getMappedPort(API_PORT);
    }

    /**
     * Register {@code endpoint} with the shim and return the (host, port) peers should dial. Called
     * from the channel wiring once per endpoint. Idempotent — a second call returns the same address.
     *
     * @param endpoint the endpoint to front with a proxy
     * @return the address to publish in the on-chain peer roster
     * @throws IllegalStateException if the shim is not started
     */
    public ProxyAddress registerPeerProxy(final EvmEndpoint endpoint) {
        if (!isStarted()) {
            throw new IllegalStateException("PeerNetworkShim.start() has not been called");
        }
        final RegisteredEndpoint existing = registered.get(endpoint.id());
        if (existing != null) {
            return new ProxyAddress(staticIp, existing.port());
        }
        final int port = PROXY_PORT_BASE + registered.size();
        final String upstream = endpoint.containerIp() + ":" + EvmEndpoint.GRPC_PORT;
        createProxy(endpoint.id(), port, upstream);
        registered.put(endpoint.id(), new RegisteredEndpoint(endpoint.id(), port));
        return new ProxyAddress(staticIp, port);
    }

    /**
     * Toggle the proxy fronting {@code endpoint}.
     *
     * <p>Disabling closes all live connections and refuses new ones until re-enabled.
     *
     * @param endpoint the endpoint whose inbound peer traffic to enable or disable
     * @param enabled {@code true} to allow traffic, {@code false} to block it
     * @throws IllegalArgumentException if the endpoint was never registered with this shim
     */
    public void setEnabled(final EvmEndpoint endpoint, final boolean enabled) {
        final RegisteredEndpoint reg = registered.get(endpoint.id());
        if (reg == null) {
            throw new IllegalArgumentException("endpoint '" + endpoint.id() + "' was not registered with the shim;"
                    + " enable env.enablePeerNetworkShim() before startAll() so its address is proxied");
        }
        updateProxyEnabled(reg.id(), enabled);
    }

    @Override
    public void close() {
        if (container != null) {
            try {
                container.stop();
            } catch (final RuntimeException ignored) {
                // best-effort
            }
            container = null;
        }
        try {
            http.close();
        } catch (final RuntimeException ignored) {
            // best-effort
        }
    }

    // ── Toxiproxy REST client ─────────────────────────────────────────────────

    private void createProxy(final String id, final int listenPort, final String upstream) {
        // POST /proxies with {name, listen, upstream, enabled}. Toxiproxy binds `listen` inside its
        // container, so a bind to 0.0.0.0:port is reachable from other containers via staticIp:port.
        final String body = "{"
                + "\"name\":\"" + id + "\","
                + "\"listen\":\"0.0.0.0:" + listenPort + "\","
                + "\"upstream\":\"" + upstream + "\","
                + "\"enabled\":true"
                + "}";
        final HttpResponse<String> response = post("/proxies", body);
        if (response.statusCode() != 201) {
            throw new IllegalStateException("Toxiproxy rejected proxy creation for '" + id + "': HTTP "
                    + response.statusCode() + " " + response.body());
        }
    }

    private void updateProxyEnabled(final String id, final boolean enabled) {
        // POST /proxies/{name} updates in place. Body carries only the fields being changed.
        final String body = "{\"enabled\":" + enabled + "}";
        final HttpResponse<String> response = post("/proxies/" + id, body);
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Toxiproxy rejected update of proxy '" + id + "' (enabled=" + enabled
                    + "): HTTP " + response.statusCode() + " " + response.body());
        }
    }

    private HttpResponse<String> post(final String path, final String body) {
        final String base = apiBaseUrl;
        if (base == null) {
            throw new IllegalStateException("Toxiproxy API base URL is not set — start() must run first");
        }
        final HttpRequest request = HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        try {
            return http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (final IOException e) {
            throw new UncheckedIOException("POST " + path + " to Toxiproxy failed", e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted during POST " + path + " to Toxiproxy", e);
        }
    }

    /** The address peers dial to reach an endpoint through the shim. Written to the on-chain roster. */
    public record ProxyAddress(String host, int port) {}

    /** One endpoint's slot in this shim: its stable id and the proxy port assigned at registration. */
    private record RegisteredEndpoint(String id, int port) {}
}
