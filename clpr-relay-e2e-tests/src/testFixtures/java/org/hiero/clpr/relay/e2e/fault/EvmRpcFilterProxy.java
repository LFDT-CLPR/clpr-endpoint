// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.fault;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Method-aware JSON-RPC forwarding proxy running as its own container in the docker network. Sits
 * in front of a {@code BesuNode} and forwards every request byte-for-byte except those on a
 * runtime-toggleable blocklist, which return an HTTP 502 with a JSON-RPC {@code -32603} error.
 *
 * <p>Enables submitter-specific outage tests that cannot be expressed with {@code docker pause}:
 * pausing a container freezes every RPC method at once, so the failure surfaces at the earliest
 * caller (state read) and never reaches the transaction submitter. This proxy discriminates by
 * method — e.g. block {@code eth_sendRawTransaction} while leaving {@code eth_call} untouched — so
 * the outage lands exactly where the test needs it to.
 *
 * <p>Opt-in: enabled per network via {@code BesuNetworkSpec.withRpcFilterProxy()}; other networks
 * are unaffected. When enabled the network's {@code endpointRpcUrl()} returns this proxy's alias
 * instead of the Besu node's alias, so the relay dials the proxy without any wiring change in
 * {@code EvmEndpoint}.
 *
 * <p>Two ports:
 * <ul>
 *   <li>{@value #RPC_PORT} — JSON-RPC forwarding surface, the endpoint dials this.</li>
 *   <li>{@value #ADMIN_PORT} — admin surface, the test JVM dials this via {@link #block},
 *       {@link #unblock}, {@link #clear}.</li>
 * </ul>
 */
public final class EvmRpcFilterProxy extends GenericContainer<EvmRpcFilterProxy> {

    public static final int RPC_PORT = 8545;
    public static final int ADMIN_PORT = 8546;

    /** System property naming the proxy jar to mount. Set by the Gradle {@code test} task. */
    private static final String JAR_PROPERTY = "clpr.e2e.rpcFilterJar";

    private static final DockerImageName IMAGE = DockerImageName.parse("eclipse-temurin:25-jre");

    private final String alias;
    private final HttpClient http =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    /**
     * @param alias the docker-network alias the endpoint dials, e.g. {@code besu-0-rpc-filter}
     * @param upstreamAlias the docker-network alias of the Besu node this proxy forwards to
     * @param upstreamPort the JSON-RPC port on {@code upstreamAlias}
     * @param network the shared docker network
     */
    public EvmRpcFilterProxy(
            final String alias, final String upstreamAlias, final int upstreamPort, final Network network) {
        super(IMAGE);
        this.alias = alias;
        withNetwork(network)
                .withNetworkAliases(alias)
                .withExposedPorts(RPC_PORT, ADMIN_PORT)
                .withCopyFileToContainer(MountableFile.forHostPath(proxyJarPath()), "/app/proxy.jar")
                .withCommand(
                        "java",
                        "-jar",
                        "/app/proxy.jar",
                        "--upstream=http://" + upstreamAlias + ":" + upstreamPort,
                        "--port=" + RPC_PORT,
                        "--admin-port=" + ADMIN_PORT)
                // Wait on the admin surface: /admin/health becomes 200 once both listeners are
                // bound. Bindable-port strategies would report ready as soon as the JVM accepts the
                // socket, before the proxy is actually forwarding.
                .waitingFor(
                        Wait.forHttp("/admin/health").forPort(ADMIN_PORT).withStartupTimeout(Duration.ofSeconds(60)));
    }

    /**
     * The in-network URL an endpoint dials to reach this proxy. Stable across container restarts —
     * the alias is what changes, not the port.
     */
    public String internalRpcUrl() {
        return "http://" + alias + ":" + RPC_PORT;
    }

    /**
     * Add methods to the blocklist. Idempotent — a method already blocked stays blocked. Returns
     * once the proxy has acknowledged the update, so a subsequent RPC call is guaranteed to see
     * the new state.
     */
    public void block(final String... methods) {
        postAdmin("/admin/block", methodsJson(methods));
    }

    /** Remove methods from the blocklist. Idempotent — an unlisted method is unaffected. */
    public void unblock(final String... methods) {
        postAdmin("/admin/unblock", methodsJson(methods));
    }

    /** Empty the blocklist. */
    public void clear() {
        postAdmin("/admin/clear", "{}");
    }

    private void postAdmin(final String path, final String body) {
        final URI adminUri = URI.create("http://" + getHost() + ":" + getMappedPort(ADMIN_PORT) + path);
        final HttpRequest req = HttpRequest.newBuilder(adminUri)
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        try {
            final HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new IllegalStateException(
                        "admin " + path + " returned " + resp.statusCode() + ": " + resp.body());
            }
        } catch (final IOException e) {
            throw new IllegalStateException("admin " + path + " failed: " + e.getMessage(), e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("admin " + path + " interrupted", e);
        }
    }

    private static String methodsJson(final String... methods) {
        final StringBuilder sb = new StringBuilder("{\"methods\":[");
        for (int i = 0; i < methods.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('"').append(methods[i]).append('"');
        }
        sb.append("]}");
        return sb.toString();
    }

    /**
     * Path to the proxy jar the Gradle {@code rpcFilterProxyJar} task emits. Fail loudly here — a
     * missing jar surfaces as a wait-timeout on {@code /admin/health} otherwise, with a diagnostic
     * that says nothing about the packaging.
     */
    private static Path proxyJarPath() {
        final String prop = System.getProperty(JAR_PROPERTY);
        if (prop == null || prop.isBlank()) {
            throw new IllegalStateException("system property " + JAR_PROPERTY + " is not set; the E2E Gradle task "
                    + "populates it from the rpcFilterProxyJar output. Run the E2E suite through Gradle, not "
                    + "a direct JVM launch.");
        }
        final Path path = Path.of(prop);
        if (!Files.exists(path)) {
            throw new IllegalStateException("rpc filter proxy jar not found at " + path
                    + " — run ':clpr-relay-e2e-tests:rpcFilterProxyJar' first");
        }
        return path;
    }
}
