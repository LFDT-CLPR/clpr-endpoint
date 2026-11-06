// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprEndpointManifest;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ManifestProofPolicy} — the shared decision (issue #292) for whether an
 * outbound bundle should carry a fresh local endpoint-manifest proof.
 *
 * <p>Since the two-phase sync migration the peer states the version it holds of our manifest directly
 * in its {@code BundleRequest.current_endpoint_manifest_version}, so the policy takes that version as a
 * plain {@code long} — there is no {@code PeerManifestVersionCache} anymore.
 */
class ManifestProofPolicyTest {

    private static ClprEndpointManifest manifest(final long version, final int serviceAddressLen) {
        return ClprEndpointManifest.newBuilder()
                .version(version)
                .serviceAddress(Bytes.wrap(new byte[serviceAddressLen]))
                .build();
    }

    // ── shouldAttach (pure predicate) ───────────────────────────────────────────

    @Test
    void shouldAttach_whenAheadOfPeerAndServiceAddressPresent() {
        assertThat(ManifestProofPolicy.shouldAttach(manifest(2L, 20), 1L)).isTrue();
    }

    @Test
    void shouldNotAttach_whenPeerIsAlreadyCurrent() {
        assertThat(ManifestProofPolicy.shouldAttach(manifest(2L, 20), 2L)).isFalse();
    }

    @Test
    void shouldNotAttach_whenPeerIsAhead() {
        assertThat(ManifestProofPolicy.shouldAttach(manifest(2L, 20), 5L)).isFalse();
    }

    @Test
    void shouldNotAttach_whenServiceAddressEmpty_evenIfVersionAhead() {
        assertThat(ManifestProofPolicy.shouldAttach(manifest(3L, 0), 0L)).isFalse();
    }

    @Test
    void shouldNotAttach_whenVersionZero() {
        assertThat(ManifestProofPolicy.shouldAttach(manifest(0L, 20), 0L)).isFalse();
    }

    // ── manifestToAttach (reader + peer-known-version resolution) ────────────────

    @Test
    void manifestToAttach_nullReaderReturnsEmpty() {
        assertThat(ManifestProofPolicy.manifestToAttach(null, 0L, "latest")).isEmpty();
    }

    @Test
    void manifestToAttach_peerAtVersionZeroReceivesManifest() {
        final var reader = new StubManifestReader(manifest(1L, 20));
        assertThat(ManifestProofPolicy.manifestToAttach(reader, 0L, "latest")).contains(manifest(1L, 20));
    }

    @Test
    void manifestToAttach_skipsWhenPeerReportsCurrent() {
        final var reader = new StubManifestReader(manifest(1L, 20));
        assertThat(ManifestProofPolicy.manifestToAttach(reader, 1L, "latest")).isEmpty();
    }

    /** Minimal reader that only answers {@code readEndpointManifest(String)}. */
    private record StubManifestReader(ClprEndpointManifest manifest) implements ContractStateReader {
        @Override
        public ClprEndpointManifest readEndpointManifest(final String blockTag) {
            return manifest;
        }

        @Override
        public Optional<ClprChannel> readChannelState(final Bytes channelId, final String blockTag) {
            return Optional.empty();
        }

        @Override
        public List<QueuedMessage> readQueuedMessages(
                final Bytes channelId, final long fromId, final long toId, final String blockTag) {
            return List.of();
        }

        @Override
        public ClprLedgerConfiguration readLedgerConfiguration(final CommitmentLevel commitmentLevel) {
            return ClprLedgerConfiguration.DEFAULT;
        }
    }
}
