// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.sync;

import com.hedera.hapi.node.state.clpr.ClprEndpoint;
import com.hedera.hapi.node.state.clpr.ClprServiceEndpoint;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.hiero.clpr.relay.core.PeerEndpointCache;
import org.hiero.clpr.relay.core.Peers;
import org.jspecify.annotations.NonNull;

/**
 * Selects which peer to sync with using weighted random selection.
 *
 * <p>Peers with more recorded successes receive proportionally higher weight. Peers currently in
 * an exponential back-off window are excluded from selection; if all peers are backed off, the
 * selector falls back to uniform random selection from the full peer list.
 *
 * <p>Statistics are tracked per advertised service endpoint {@code (host, port)} (see
 * {@link Peers#peerKey(ClprEndpoint)}), not per {@code account_id}.
 */
public class PeerSelector {

    private final PeerEndpointCache cache;
    private final ConcurrentHashMap<ClprServiceEndpoint, PeerStats> stats = new ConcurrentHashMap<>();
    private final Random random = new Random();

    /**
     * Creates a new {@code PeerSelector} backed by the given {@link PeerEndpointCache}.
     *
     * @param cache the peer cache to draw candidates from; must not be {@code null}
     */
    public PeerSelector(@NonNull final PeerEndpointCache cache) {
        this.cache = cache;
    }

    /**
     * Select a peer for syncing.
     *
     * <p>Returns {@link Optional#empty()} when no peers are known. Backed-off peers are excluded;
     * if all peers are in back-off the selection falls back to uniform random from the full list.
     *
     * @return the chosen peer, or empty if none are available
     */
    @NonNull
    public Optional<ClprEndpoint> selectPeer() {
        var peers = cache.allPeers();
        if (peers.isEmpty()) {
            return Optional.empty();
        }

        var eligible = peers.stream().filter(p -> !isBackedOff(p)).toList();
        if (eligible.isEmpty()) {
            // All peers backed off — fall back to uniform random from full list
            return Optional.of(peers.get(random.nextInt(peers.size())));
        }

        return Optional.of(weightedSelect(eligible));
    }

    /**
     * Record a successful sync with the given peer.
     *
     * @param peer the peer that succeeded; must not be {@code null}
     */
    public void recordSuccess(@NonNull final ClprEndpoint peer) {
        getStats(peer).recordSuccess();
    }

    /**
     * Record a failed sync with the given peer. Triggers exponential back-off.
     *
     * @param peer the peer that failed; must not be {@code null}
     */
    public void recordFailure(@NonNull final ClprEndpoint peer) {
        getStats(peer).recordFailure();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private boolean isBackedOff(final ClprEndpoint peer) {
        var s = stats.get(Peers.peerKey(peer));
        return s != null && s.isBackedOff();
    }

    private ClprEndpoint weightedSelect(final List<ClprEndpoint> eligible) {
        double totalWeight =
                eligible.stream().mapToDouble(p -> getStats(p).weight()).sum();
        double r = random.nextDouble() * totalWeight;
        double cumulative = 0;
        for (var peer : eligible) {
            cumulative += getStats(peer).weight();
            if (r <= cumulative) {
                return peer;
            }
        }
        return eligible.getLast(); // numerical-precision fallback
    }

    private PeerStats getStats(final ClprEndpoint peer) {
        return stats.computeIfAbsent(Peers.peerKey(peer), k -> new PeerStats());
    }
}
