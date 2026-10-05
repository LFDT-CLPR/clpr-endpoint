// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link BundleCache}'s bounded LRU behaviour. */
class BundleCacheTest {

    /** Cap kept in sync with {@code BundleCache.MAX_ENTRIES}. */
    private static final int MAX_ENTRIES = 64;

    private static final Bytes CHANNEL = Bytes.wrap(new byte[32]);
    private static final BundleRequest REQUEST =
            BundleRequest.newBuilder().currentReceivedMessageId(0L).build();

    /** Distinct bundle per height, so identity checks are unambiguous. */
    private static BundleWithConstructionMetadata response(final long height) {
        return new BundleWithConstructionMetadata(Bytes.wrap(new byte[] {(byte) height}), height);
    }

    @Test
    void getReturnsThePutValue() {
        final var cache = new BundleCache();
        assertThat(cache.get(CHANNEL, REQUEST, 5L)).isEmpty();

        cache.put(CHANNEL, REQUEST, 5L, response(5));

        assertThat(cache.get(CHANNEL, REQUEST, 5L)).contains(response(5));
    }

    @Test
    void insertingPastCapacityEvictsLeastRecentlyUsedNotEverything() {
        final var cache = new BundleCache();
        // Fill exactly to capacity, keyed by height.
        for (long h = 0; h < MAX_ENTRIES; h++) {
            cache.put(CHANNEL, REQUEST, h, response(h));
        }

        // Touch the oldest entry so it becomes the most-recently-used; height 1 is now the LRU.
        assertThat(cache.get(CHANNEL, REQUEST, 0L)).contains(response(0));

        // One more insert overflows the cap: only the single LRU entry (height 1) is evicted.
        cache.put(CHANNEL, REQUEST, MAX_ENTRIES, response(MAX_ENTRIES));

        assertThat(cache.get(CHANNEL, REQUEST, 1L)).as("LRU entry evicted").isEmpty();
        assertThat(cache.get(CHANNEL, REQUEST, 0L))
                .as("recently-used entry retained (not cleared wholesale)")
                .contains(response(0));
        assertThat(cache.get(CHANNEL, REQUEST, MAX_ENTRIES))
                .as("new entry present")
                .contains(response(MAX_ENTRIES));
        // A mid entry that was never touched but is not the eldest still survives — the cache was not cleared.
        assertThat(cache.get(CHANNEL, REQUEST, 40L)).contains(response(40));
    }

    @Test
    void keyDistinguishesChannelRequestAndHeight() {
        final var cache = new BundleCache();
        cache.put(CHANNEL, REQUEST, 7L, response(7));

        final BundleRequest otherRequest =
                BundleRequest.newBuilder().currentReceivedMessageId(1L).build();
        final Bytes otherChannel = Bytes.wrap(new byte[] {9});

        assertThat(cache.get(CHANNEL, REQUEST, 8L)).as("different height").isEmpty();
        assertThat(cache.get(CHANNEL, otherRequest, 7L)).as("different request").isEmpty();
        assertThat(cache.get(otherChannel, REQUEST, 7L)).as("different channel").isEmpty();
        assertThat(cache.get(CHANNEL, REQUEST, 7L)).contains(response(7));
    }
}
