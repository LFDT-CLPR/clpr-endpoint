// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * A {@link BundleConstructor} decorator that memoizes built bundles in a bounded LRU
 * {@link BundleCache}, sparing the delegate from re-reading the chain and rebuilding the proof for a
 * burst of identical requests (the two-phase peer retrying, or several peers requesting the same
 * range) at the same chain height.
 *
 * <p>This decorator takes a {@code localChainHeight} supplier — a cheap read of the same height the
 * delegate proves against (the commitment-level head, minus any proof lag the delegate applies). On
 * each call it reads that height once and probes the cache; a hit returns immediately without invoking
 * the delegate. On a miss it delegates, then stores the result under the height it <em>probed</em> with
 * — not the delegate's independently read {@link BundleWithConstructionMetadata#constructedAtHeight()},
 * which can differ when the head advances mid-build and would file the entry under a height no future
 * probe ever reads back. Once the chain advances the probe height no longer matches any stored entry,
 * so a fresh bundle is built and the stale entries age out of the LRU.
 */
public final class CachedBundleConstructor implements BundleConstructor {

    private final BundleConstructor delegate;
    private final LongSupplier localChainHeight;
    private final BundleCache cache = new BundleCache();

    /**
     * @param delegate         the underlying constructor that reads the chain and builds the proof
     * @param localChainHeight a cheap read of the chain height the delegate proves against; probed on
     *                         every call to key the cache before the (expensive) delegate is invoked
     */
    public CachedBundleConstructor(final BundleConstructor delegate, final LongSupplier localChainHeight) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.localChainHeight = Objects.requireNonNull(localChainHeight, "localChainHeight");
    }

    @Override
    public BundleWithConstructionMetadata createBundle(final Bytes channelId, final BundleRequest request) {
        final long height = localChainHeight.getAsLong();
        final var cached = cache.get(channelId, request, height);
        if (cached.isPresent()) {
            return cached.get();
        }
        final BundleWithConstructionMetadata built = delegate.createBundle(channelId, request);
        // Store under the height we PROBED with, not the (independently read) height the delegate proved
        // against: those are two separate chain reads, and if the head advanced between them the entry
        // would be filed under a height no future probe reads back — a guaranteed miss. Keying by the
        // probe height is what the next probe compares against, so the cache actually hits.
        cache.put(channelId, request, height, built);
        return built;
    }
}
