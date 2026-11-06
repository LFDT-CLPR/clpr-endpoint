// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.pbj.runtime.io.buffer.Bytes;

/**
 * Constructs proofs from EVM state that can be submitted to the Hiero network.
 *
 * <p>Implementations produce proof bytes suitable for on-chain verification by the CLPR EVM
 * Verifier. In the two-phase sync protocol a bundle is built <b>ad hoc</b> from a peer's
 * {@link BundleRequest} (see {@link #createBundle}); the request states which messages the peer wants
 * next, its current status, its verifier's current trust anchor id, and the endpoint-manifest version
 * it currently holds of us, so the same request against the same local chain height always yields the
 * same bundle. The returned {@link BundleWithConstructionMetadata} carries the chain height the bundle
 * was proven against; caching is applied by the {@link CachedBundleConstructor} decorator rather than
 * by each implementation.
 */
public interface BundleConstructor {

    /**
     * Build a bundle for {@code channelId} that answers the peer's {@code request}: it includes the
     * messages from {@code request.currentReceivedMessageId()}+1 onward (trimmed to what the sender actually has),
     * proven against the current local chain state, uses {@code request.currentTrustAnchorId()} as the
     * peer verifier's trust anchor (driving any validator-set / epoch catch-up the proof needs), and
     * attaches a local endpoint-manifest proof when our manifest is newer than
     * {@code request.currentEndpointManifestVersion()} (spec §4.2 Step 1b).
     *
     * <p>Reads the chain at call time and is deterministic for a fixed {@code (request, local chain
     * height)}; the returned metadata reports which height it read, so a decorator can serve repeated
     * identical requests at that height from a cache.
     *
     * @param channelId the CLPR channel identifier
     * @param request   the peer's bundle request (drives message range, trust anchor, manifest, status)
     * @return the opaque proof payload plus the local chain height it was proven against
     */
    BundleWithConstructionMetadata createBundle(final Bytes channelId, final BundleRequest request);
}
