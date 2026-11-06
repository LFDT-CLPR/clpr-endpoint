// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.fault;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Method-aware JSON-RPC forwarding proxy used by the E2E suite to inject targeted RPC failures on
 * a running Besu node without freezing the whole node.
 *
 * <p>Runs as its own container in the docker network with two HTTP servers: one on
 * {@code --port} that forwards JSON-RPC POSTs to {@code --upstream}, and one on
 * {@code --admin-port} that toggles a runtime blocklist over HTTP. A request whose {@code method}
 * (or any method in a batch) is on the blocklist returns HTTP 502 with a JSON-RPC {@code -32603}
 * error body; everything else is proxied byte-for-byte.
 *
 * <p>Deliberately single-file, JDK-stdlib-only, no third-party dependencies. Packaged as a slim
 * runnable jar by the Gradle {@code rpcFilterProxyJar} task and copied into an
 * {@code eclipse-temurin:25-jre} container by {@code EvmRpcFilterProxy}. Do not add dependencies —
 * the point is that the jar drops into any recent JDK image and runs unmodified.
 *
 * <p><b>Admin protocol</b>:
 * <ul>
 *   <li>{@code GET /admin/health} — {@code 200 OK} once both servers are bound.</li>
 *   <li>{@code POST /admin/block} — body {@code {"methods":["eth_sendRawTransaction",...]}}. Adds
 *       every listed method to the blocklist. Idempotent.</li>
 *   <li>{@code POST /admin/unblock} — body as above; removes each. Idempotent.</li>
 *   <li>{@code POST /admin/clear} — empties the blocklist. Body ignored.</li>
 * </ul>
 * The admin surface is deliberately minimal so the JVM-side control code is trivial and the wire
 * format is trivial to read in diagnostics.
 */
public final class EvmRpcFilterProxyApp {

    /**
     * Matches the JSON-RPC {@code method} field in the request body. Applied to the raw bytes
     * decoded as UTF-8; the JSON-RPC method values relay clients emit are always ASCII, so a
     * regex over the raw text is safe and avoids pulling in a JSON parser.
     */
    private static final Pattern METHOD_PATTERN = Pattern.compile("\"method\"\\s*:\\s*\"([^\"]+)\"");

    private final Set<String> blocked = ConcurrentHashMap.newKeySet();
    private final URI upstream;
    private final HttpClient client;

    private EvmRpcFilterProxyApp(final URI upstream) {
        this.upstream = upstream;
        this.client =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    public static void main(final String[] args) throws IOException {
        String upstream = null;
        int port = 8545;
        int adminPort = 8546;
        for (final String arg : args) {
            if (arg.startsWith("--upstream=")) {
                upstream = arg.substring("--upstream=".length());
            } else if (arg.startsWith("--port=")) {
                port = Integer.parseInt(arg.substring("--port=".length()));
            } else if (arg.startsWith("--admin-port=")) {
                adminPort = Integer.parseInt(arg.substring("--admin-port=".length()));
            } else {
                throw new IllegalArgumentException(
                        "unknown arg: " + arg + ". Expected: --upstream=<url> --port=<n> --admin-port=<n>");
            }
        }
        if (upstream == null) {
            throw new IllegalArgumentException("--upstream is required");
        }
        new EvmRpcFilterProxyApp(URI.create(upstream)).start(port, adminPort);
    }

    private void start(final int port, final int adminPort) throws IOException {
        // Bind admin first: the readiness probe on /admin/health is what the container fixture
        // waits on, so it must come up before the RPC listener starts accepting bundles.
        final HttpServer admin = HttpServer.create(new InetSocketAddress(adminPort), 0);
        admin.createContext("/admin/health", this::handleHealth);
        admin.createContext("/admin/block", ex -> handleAdmin(ex, /* block */ true));
        admin.createContext("/admin/unblock", ex -> handleAdmin(ex, /* block */ false));
        admin.createContext("/admin/clear", this::handleClear);
        admin.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        admin.start();

        final HttpServer proxy = HttpServer.create(new InetSocketAddress(port), 0);
        proxy.createContext("/", this::handleRpc);
        proxy.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        proxy.start();

        System.out.println("EvmRpcFilterProxy: upstream=" + upstream + " port=" + port + " adminPort=" + adminPort);
    }

    private void handleRpc(final HttpExchange ex) throws IOException {
        try (ex) {
            final byte[] body = ex.getRequestBody().readAllBytes();
            for (final String method : extractMethods(body)) {
                if (blocked.contains(method)) {
                    sendJsonRpcError(ex, method);
                    return;
                }
            }
            final HttpRequest fwd = HttpRequest.newBuilder(upstream)
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", contentType(ex))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .build();
            final HttpResponse<byte[]> resp;
            try {
                resp = client.send(fwd, HttpResponse.BodyHandlers.ofByteArray());
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                sendPlain(ex, 502, "proxy interrupted");
                return;
            } catch (final IOException ioe) {
                // Upstream unreachable is the caller's problem, not ours — surface it verbatim.
                sendPlain(ex, 502, "upstream error: " + ioe.getMessage());
                return;
            }
            ex.getResponseHeaders().set("Content-Type", "application/json");
            final byte[] respBody = resp.body();
            ex.sendResponseHeaders(resp.statusCode(), respBody.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(respBody);
            }
        }
    }

    private void handleAdmin(final HttpExchange ex, final boolean block) throws IOException {
        try (ex) {
            final byte[] body = ex.getRequestBody().readAllBytes();
            final Set<String> methods = extractMethodsArray(body);
            if (block) {
                blocked.addAll(methods);
            } else {
                blocked.removeAll(methods);
            }
            sendPlain(ex, 200, "ok: " + String.join(",", blocked));
        }
    }

    private void handleClear(final HttpExchange ex) throws IOException {
        try (ex) {
            blocked.clear();
            sendPlain(ex, 200, "cleared");
        }
    }

    private void handleHealth(final HttpExchange ex) throws IOException {
        try (ex) {
            sendPlain(ex, 200, "ok");
        }
    }

    private static void sendPlain(final HttpExchange ex, final int status, final String body) throws IOException {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    /**
     * Send a JSON-RPC error response for a blocked method. HTTP status is 502 rather than 200 so
     * a mistuned client that ignores the body still sees a failure; the body carries a proper
     * JSON-RPC error shape so a well-behaved client's parser reports the right diagnostic string.
     */
    private static void sendJsonRpcError(final HttpExchange ex, final String method) throws IOException {
        final String body = "{\"jsonrpc\":\"2.0\",\"id\":null,\"error\":{\"code\":-32603,"
                + "\"message\":\"blocked by test fixture: " + method + "\"}}";
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(502, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static String contentType(final HttpExchange ex) {
        final String ct = ex.getRequestHeaders().getFirst("Content-Type");
        return ct != null ? ct : "application/json";
    }

    /**
     * Extract every JSON-RPC {@code method} in the request body. Handles both the single-call and
     * batch shapes because the same regex matches once per element.
     */
    static Set<String> extractMethods(final byte[] body) {
        final Set<String> methods = new HashSet<>();
        final Matcher m = METHOD_PATTERN.matcher(new String(body, StandardCharsets.UTF_8));
        while (m.find()) {
            methods.add(m.group(1));
        }
        return methods;
    }

    /**
     * Extract a JSON array-of-strings under the {@code methods} key from an admin request body.
     * A minimal parser tolerates whitespace and reject-lists containing a single entry.
     */
    static Set<String> extractMethodsArray(final byte[] body) {
        final Set<String> out = new HashSet<>();
        final String text = new String(body, StandardCharsets.UTF_8);
        final int bracketOpen = text.indexOf('[');
        final int bracketClose = text.lastIndexOf(']');
        if (bracketOpen < 0 || bracketClose <= bracketOpen) {
            return out;
        }
        final String inside = text.substring(bracketOpen + 1, bracketClose);
        for (final String raw : inside.split(",")) {
            final String trimmed = raw.trim();
            if (trimmed.length() >= 2 && trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
                out.add(trimmed.substring(1, trimmed.length() - 1));
            }
        }
        return out;
    }
}
