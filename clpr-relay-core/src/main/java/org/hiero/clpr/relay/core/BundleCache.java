// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A small bounded LRU cache of prepared bundles, keyed by {@code (channelId, request, localChainHeight)}.
 *
 * <p>A {@link BundleConstructor} builds a bundle ad hoc for each peer {@link BundleRequest}; because
 * the same request against the same local chain height always yields the same bundle, a burst of
 * identical requests (e.g. the two-phase peer retrying, or several peers requesting the same range)
 * can be served without re-reading the chain and rebuilding the proof. This cache is owned by the
 * {@link CachedBundleConstructor} decorator, which reads the current chain height once per call and
 * uses that single probed value as the key for both the lookup and the store — deliberately
 * <em>not</em> {@link BundleWithConstructionMetadata#constructedAtHeight()}. That is an independent
 * chain read that can differ when the head advances mid-build, and keying by it would file the entry
 * under a height no later probe reads back — a guaranteed miss.
 *
 * <p>Bounded to {@link #MAX_ENTRIES}: inserting a new entry once the cache is full evicts the
 * least-recently-used one (a {@link #get} counts as a use), so hot entries are retained while stale
 * ones — e.g. entries for a chain height that has since advanced — age out. Thread-safe: every access
 * goes through this instance's monitor (the backing {@link LinkedHashMap} is access-ordered, so a
 * {@code get} is itself a structural mutation and must be synchronized too).
 */
public final class BundleCache {

    /** Maximum entries retained; inserting past this evicts the least-recently-used entry. */
    private static final int MAX_ENTRIES = 64;

    private record Key(Bytes channelId, BundleRequest request, long height) {}

    /** Access-ordered LRU map: {@code removeEldestEntry} evicts the least-recently-used entry past the cap. */
    private static final class LruMap extends LinkedHashMap<Key, BundleWithConstructionMetadata> {
        private static final long serialVersionUID = 1L;
        private final int maxEntries;

        LruMap(final int maxEntries) {
            super(16, 0.75f, true); // access order → true LRU
            this.maxEntries = maxEntries;
        }

        @Override
        protected boolean removeEldestEntry(final Map.Entry<Key, BundleWithConstructionMetadata> eldest) {
            return size() > maxEntries;
        }
    }

    private final LruMap entries = new LruMap(MAX_ENTRIES);

    /**
     * Returns the cached bundle for {@code (channelId, request, height)}, if present, marking it as the
     * most-recently-used entry.
     *
     * @param channelId the CLPR channel identifier
     * @param request   the peer's bundle request
     * @param height    the probed local chain height used as the cache key
     * @return the cached {@link BundleWithConstructionMetadata}, or empty
     */
    public synchronized Optional<BundleWithConstructionMetadata> get(
            final Bytes channelId, final BundleRequest request, final long height) {
        return Optional.ofNullable(entries.get(new Key(channelId, request, height)));
    }

    /**
     * Caches {@code bundle} for {@code (channelId, request, height)} as the most-recently-used entry,
     * evicting the least-recently-used entry if the cache is at capacity.
     *
     * @param channelId the CLPR channel identifier
     * @param request   the peer's bundle request
     * @param height    the probed local chain height used as the cache key
     * @param bundle    the prepared bundle
     */
    public synchronized void put(
            final Bytes channelId,
            final BundleRequest request,
            final long height,
            final BundleWithConstructionMetadata bundle) {
        entries.put(new Key(channelId, request, height), bundle);
    }
}
