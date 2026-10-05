// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import io.grpc.StatusRuntimeException;
import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Duration;
import javax.net.ssl.X509ExtendedKeyManager;
import org.hiero.clpr.relay.core.LeafCertificate;
import org.hiero.clpr.relay.core.LeafKeyManager;
import org.hiero.clpr.relay.core.metrics.SimpleMetrics;
import org.hiero.clpr.relay.core.testfixtures.CertFixtures;
import org.hiero.clpr.relay.grpc.client.ClprEndpointClient;
import org.hiero.clpr.relay.test.harness.StubPeer;
import org.junit.jupiter.api.Test;

/**
 * Direct TLS-handshake rejection tests: a {@link StubPeer} backed by a static key manager and a
 * {@link ClprEndpointClient} are wired in-process, with no relay or Anvil involved.
 *
 * <p>Each test covers a distinct {@link org.hiero.clpr.relay.core.Certs#chainsTo} failure branch:
 *
 * <ul>
 *   <li>{@link #clientRejectsExpiredServerCert} — {@code checkValidity()} fails
 *   <li>{@link #clientRejectsLeafFromUntrustedCa} — {@code verify(caCert.getPublicKey())} fails
 * </ul>
 *
 * <p>A {@link LeafKeyManager} would rotate an expired cert away on the first {@code
 * getCertificateChain()} call; {@link StubPeer#startWithStaticKeyManager} bypasses that so the
 * expired cert reaches the handshake unchanged.
 */
class ClientTlsDialTest {

    public static final SimpleMetrics METRICS = new SimpleMetrics();

    /**
     * Wrap a fixed {@link LeafCertificate} in a non-rotating {@link X509ExtendedKeyManager}.
     * The returned manager always presents {@code leaf} and never rotates it.
     */
    private static X509ExtendedKeyManager staticKeyManagerFor(final LeafCertificate leaf) {
        return new X509ExtendedKeyManager() {
            @Override
            public String[] getServerAliases(final String keyType, final Principal[] issuers) {
                return new String[] {"clpr"};
            }

            @Override
            public String chooseServerAlias(final String keyType, final Principal[] issuers, final Socket socket) {
                return "clpr";
            }

            @Override
            public String[] getClientAliases(final String keyType, final Principal[] issuers) {
                return null;
            }

            @Override
            public String chooseClientAlias(final String[] keyTypes, final Principal[] issuers, final Socket socket) {
                return null;
            }

            @Override
            public X509Certificate[] getCertificateChain(final String alias) {
                return new X509Certificate[] {leaf.cert()};
            }

            @Override
            public PrivateKey getPrivateKey(final String alias) {
                return leaf.key();
            }
        };
    }

    /**
     * The client must reject a server whose leaf is past its X.509 {@code notAfter} date, even when
     * the certificate is signed by the expected CA. The CA is correct; only expiry causes rejection.
     */
    @Test
    void clientRejectsExpiredServerCert() throws Exception {
        // validity = -3601 s → notAfter = now + (−3601 s) + CLOCK_SKEW(1 h) = now − 1 s
        final var staticKm =
                staticKeyManagerFor(LeafCertificate.generate(CertFixtures.CA_A_KEY, Duration.ofSeconds(-3601)));

        try (final var expiredPeer = StubPeer.startWithStaticKeyManager(staticKm);
                final var client = new ClprEndpointClient(
                        1 << 20, new LeafKeyManager(CertFixtures.CA_A_KEY, Duration.ZERO, METRICS))) {
            assertThatThrownBy(() -> client.sync(
                            "127.0.0.1",
                            expiredPeer.port(),
                            ClprSyncPayload.newBuilder().build(),
                            reply -> ClprSyncPayload.newBuilder().build(),
                            Bytes.wrap(CertFixtures.CA_A_CERT_DER)))
                    .isInstanceOf(StatusRuntimeException.class);
        }
    }

    /**
     * The client must reject a server whose leaf is signed by CA-B when the on-chain trust anchor
     * for that peer is CA-A. The cert is valid and not expired; only the CA mismatch causes rejection.
     */
    @Test
    void clientRejectsLeafFromUntrustedCa() throws Exception {
        final var staticKm = staticKeyManagerFor(CertFixtures.leaf(CertFixtures.CA_B_KEY));

        try (final var wrongCaPeer = StubPeer.startWithStaticKeyManager(staticKm);
                final var client = new ClprEndpointClient(
                        1 << 20, new LeafKeyManager(CertFixtures.CA_A_KEY, Duration.ZERO, METRICS))) {
            assertThatThrownBy(() -> client.sync(
                            "127.0.0.1",
                            wrongCaPeer.port(),
                            ClprSyncPayload.newBuilder().build(),
                            reply -> ClprSyncPayload.newBuilder().build(),
                            Bytes.wrap(CertFixtures.CA_A_CERT_DER)))
                    .isInstanceOf(StatusRuntimeException.class);
        }
    }
}
