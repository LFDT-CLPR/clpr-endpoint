// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import com.hedera.hapi.node.state.clpr.ClprEndpoint;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-channel, thread-safe, in-memory cache of known peer endpoints.
 *
 * <p>Keyed by {@code accountId} ({@link Bytes}) for uniqueness. The cache mirrors the on-ledger
 * peer endpoint roster (spec §2.4.2) — the authoritative set the chain enforces; the relay does not
 * use gossip-based discovery to populate it, so it is the single source of truth for reachable peer
 * endpoints. The roster is installed at startup and replaced wholesale when it changes on chain.
 *
 * <p>Reads observe an atomic snapshot: a reader sees either the complete set before a mutation or
 * the complete set after it, never a partial or empty intermediate, and never a torn view across
 * threads. Writes are serialized.
 */
public class PeerEndpointCache {

    /** Immutable snapshot of the current peers, swapped atomically on every mutation. */
    private volatile Map<Bytes, PeerRosterEntry> entries = Map.of();

    /**
     * Atomically replace the entire set of known peers with the given roster.
     *
     * <p>After this returns the cache reflects exactly {@code roster}, keyed by {@code accountId}:
     * peers absent from it are dropped, peers present are added or updated. The replacement is
     * atomic with respect to readers — a concurrent {@link #allPeers()} observes either the complete
     * previous set or the complete new one, never a partial or empty intermediate. An empty
     * collection clears the cache.
     *
     * @param roster the peer set to install; must not be {@code null} and must not contain {@code null}
     */
    public synchronized void replaceAll(final Collection<ClprEndpoint> roster) {
        final var next = new HashMap<Bytes, PeerRosterEntry>(roster.size());
        for (final var peer : roster) {
            next.put(peer.accountId(), PeerRosterEntry.parse(peer));
        }
        entries = Map.copyOf(next);
    }

    /**
     * Get all known peers as an immutable snapshot (raw endpoints only).
     *
     * @return an immutable list of the current peer endpoints
     */
    public List<ClprEndpoint> allPeers() {
        return entries.values().stream().map(PeerRosterEntry::endpoint).toList();
    }

    /**
     * Get all known peers as an immutable snapshot of {@link PeerRosterEntry} items, each carrying
     * the endpoint and its pre-parsed TLS certificate.
     *
     * @return an immutable list of the current peer roster entries
     */
    public List<PeerRosterEntry> allPeerEntries() {
        return List.copyOf(entries.values());
    }

    /**
     * Returns {@code true} if the cache currently holds at least one peer.
     *
     * @return whether any peers are known
     */
    public boolean hasPeers() {
        return !entries.isEmpty();
    }

    public boolean hasPeerEndpoint(ClprEndpoint peer) {
        return entries.values().stream().anyMatch(entry -> entry.endpoint().equals(peer));
    }

    /**
     * Returns the number of peers currently held.
     *
     * @return the peer count
     */
    public int size() {
        return entries.size();
    }
}
