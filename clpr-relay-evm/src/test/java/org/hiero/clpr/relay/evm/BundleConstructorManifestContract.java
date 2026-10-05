// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprEndpointManifest;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.Optional;
import org.hiero.clpr.relay.core.BundleConstructor;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.ManifestProofPolicy;
import org.junit.jupiter.api.Test;

/**
 * Cross-codec contract for the Step-3 endpoint-manifest proof attachment (spec §4.2 Step 1b).
 *
 * <p>Every {@link BundleConstructor} able to carry an endpoint-manifest proof MUST gate the attachment
 * through {@link ManifestProofPolicy} so the "attach iff the local manifest version is strictly ahead
 * of the version the peer holds AND the manifest carries a service address" rule is identical across
 * wire formats. This base pins that behaviour; each codec supplies the three hooks below.
 *
 * <p>In the two-phase sync protocol the peer states the version it holds of our manifest directly in
 * its {@code BundleRequest.current_endpoint_manifest_version}; the local manifest itself is read from
 * the codec's {@link ContractStateReader}. This contract therefore drives each codec with a
 * peer-known-version {@code long} and a state reader whose {@code readEndpointManifest} returns the
 * manifest under test.
 *
 * <p>A new bundle format (e.g. a future Ethereum proof constructor) plugs in by subclassing this and
 * implementing the hooks — if it forgets to route through the policy, either
 * {@link #attachesManifestWhenLocalStrictlyAheadOfPeer()} or one of the skip cases fails here. That is
 * the forcing function the review of #297 asked for: the decision cannot be re-implemented wrong
 * (it lives only in {@link ManifestProofPolicy}), and a codec's manifest wiring cannot go untested.
 */
public abstract class BundleConstructorManifestContract {

    /** The channel id every hook builds its bundle for (32 zero bytes; matches each codec's fixtures). */
    protected static final Bytes CONN = Bytes.wrap(new byte[32]);

    /**
     * Builds the codec under test wired with {@code stateReader} as its channel-state and local-manifest
     * source, plus whatever codec-specific RPC stubs are needed to produce exactly one bundle for
     * {@link #CONN} via {@link #driveOneBundle}. The reader returns an ack-only channel so a (non-empty)
     * bundle is always produced regardless of whether the manifest proof is attached.
     */
    protected abstract BundleConstructor newConstructor(ContractStateReader stateReader);

    /**
     * Drives a single {@code createBundle} for {@link #CONN}, telling the codec that the peer already
     * holds {@code peerKnownVersion} of our manifest, and returns the resulting bundle payload bytes.
     */
    protected abstract Bytes driveOneBundle(BundleConstructor constructor, long peerKnownVersion);

    /** Parses the bundle payload and reports whether an endpoint-manifest proof is attached. */
    protected abstract boolean manifestAttached(Bytes payload) throws Exception;

    @Test
    void attachesManifestWhenLocalStrictlyAheadOfPeer() throws Exception {
        // Local manifest v1 with a service address; the peer holds version 0 → 1 > 0 → attach.
        assertThat(manifestAttached(buildBundle(manifest(1L, true), 0L)))
                .as("a manifest strictly ahead of the peer and carrying a service address must be attached")
                .isTrue();
    }

    @Test
    void skipsManifestWhenPeerAlreadyCurrent() throws Exception {
        // Peer already holds v1 → 1 > 1 is false → skip (the peer would drop a re-send).
        assertThat(manifestAttached(buildBundle(manifest(1L, true), 1L)))
                .as("a manifest the peer already holds must not be re-attached")
                .isFalse();
    }

    @Test
    void skipsManifestWhenServiceAddressEmpty() throws Exception {
        // v1 but an empty service address (freshly initialized ledger, no admitted endpoint) → not provable.
        assertThat(manifestAttached(buildBundle(manifest(1L, false), 0L)))
                .as("a version>=1 manifest with an empty service address must not be attached")
                .isFalse();
    }

    @Test
    void skipsManifestWhenLocalManifestAtDefaultVersion() throws Exception {
        // The default (version 0, empty) manifest is never strictly ahead of any peer → no proof.
        assertThat(manifestAttached(buildBundle(ClprEndpointManifest.DEFAULT, 0L)))
                .as("a default (version 0) local manifest must not be attached")
                .isFalse();
    }

    private Bytes buildBundle(final ClprEndpointManifest manifest, final long peerKnownVersion) {
        final var constructor = newConstructor(readerOf(manifest));
        final Bytes payload = driveOneBundle(constructor, peerKnownVersion);
        assertThat(payload.length())
                .as("the scenario must produce a bundle so the manifest attachment can be asserted")
                .isGreaterThan(0);
        return payload;
    }

    private static ClprEndpointManifest manifest(final long version, final boolean withServiceAddress) {
        final var builder = ClprEndpointManifest.newBuilder().version(version);
        if (withServiceAddress) {
            builder.serviceAddress(Bytes.wrap(new byte[20]));
        }
        return builder.build();
    }

    private static ContractStateReader readerOf(final ClprEndpointManifest manifest) {
        return new FixedManifestReader(manifest);
    }

    /**
     * Minimal {@link ContractStateReader} that supplies a fixed local endpoint manifest and a single
     * ack-only channel (received message id &gt; 0, no pending messages) so every codec produces a
     * bundle whose only optional element is the manifest proof.
     */
    private static final class FixedManifestReader implements ContractStateReader {
        private final ClprEndpointManifest manifest;

        FixedManifestReader(final ClprEndpointManifest manifest) {
            this.manifest = manifest;
        }

        @Override
        public ClprEndpointManifest readEndpointManifest(final String blockTag) {
            return manifest;
        }

        @Override
        public Optional<ClprChannel> readChannelState(final Bytes channelId, final String blockTag) {
            return Optional.of(ClprChannel.newBuilder()
                    .channelId(CONN)
                    .status(ClprChannelStatus.ACTIVE)
                    .nextMessageId(2L)
                    .ackedMessageId(0L)
                    .sentRunningHash(Bytes.wrap(new byte[32]))
                    .receivedMessageId(5L)
                    .receivedRunningHash(Bytes.wrap(new byte[32]))
                    .trustAnchorId(Bytes.EMPTY)
                    .build());
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
