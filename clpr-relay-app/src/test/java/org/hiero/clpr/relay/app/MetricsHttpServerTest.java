// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import org.hiero.clpr.relay.core.metrics.SimpleMetrics;
import org.junit.jupiter.api.Test;

class MetricsHttpServerTest {

    @Test
    void port_isNegativeBeforeStart() {
        assertThat(new MetricsHttpServer(0, new SimpleMetrics()).port()).isEqualTo(-1);
    }

    @Test
    void servesMetricsAndHealthEndpoints() throws Exception {
        final var server = new MetricsHttpServer(0, new SimpleMetrics());
        server.start();
        try {
            final int port = server.port();
            assertThat(port).isGreaterThan(0);

            final HttpClient http = HttpClient.newHttpClient();
            final var metrics = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:" + port + "/metrics"))
                            .build(),
                    BodyHandlers.ofString());
            assertThat(metrics.statusCode()).isEqualTo(200);

            final var health = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:" + port + "/healthz"))
                            .build(),
                    BodyHandlers.ofString());
            assertThat(health.body()).isEqualTo("ok\n");
        } finally {
            server.stop();
        }
    }
}
