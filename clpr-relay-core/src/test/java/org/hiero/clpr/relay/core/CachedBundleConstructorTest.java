// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link CachedBundleConstructor}: it memoizes per {@code (channelId, request, height)}. */
class CachedBundleConstructorTest {

    private static final Bytes CHANNEL = Bytes.wrap(new byte[32]);

    private static BundleRequest requestFrom(final long fromMessageId) {
        return BundleRequest.newBuilder()
                .currentReceivedMessageId(Math.max(0L, fromMessageId - 1))
                .build();
    }

    /** Counts invocations and builds a payload tagged with the call index, at {@code heightAtBuild}. */
    private static final class CountingConstructor implements BundleConstructor {
        final AtomicInteger calls = new AtomicInteger();
        private final LongSupplier heightAtBuild;

        CountingConstructor(final LongSupplier heightAtBuild) {
            this.heightAtBuild = heightAtBuild;
        }

        @Override
        public BundleWithConstructionMetadata createBundle(final Bytes channelId, final BundleRequest request) {
            final int n = calls.incrementAndGet();
            return new BundleWithConstructionMetadata(Bytes.wrap(new byte[] {(byte) n}), heightAtBuild.getAsLong());
        }
    }

    @Test
    void identicalRequestAtSameHeightServedFromCache() {
        final AtomicLong height = new AtomicLong(100L);
        final var delegate = new CountingConstructor(height::get);
        final var cached = new CachedBundleConstructor(delegate, height::get);

        final var first = cached.createBundle(CHANNEL, requestFrom(1L));
        final var second = cached.createBundle(CHANNEL, requestFrom(1L));

        assertThat(delegate.calls).hasValue(1);
        assertThat(second).isEqualTo(first);
    }

    @Test
    void rebuildsAfterHeightAdvances() {
        final AtomicLong height = new AtomicLong(100L);
        final var delegate = new CountingConstructor(height::get);
        final var cached = new CachedBundleConstructor(delegate, height::get);

        cached.createBundle(CHANNEL, requestFrom(1L));
        height.incrementAndGet();
        cached.createBundle(CHANNEL, requestFrom(1L));

        assertThat(delegate.calls).hasValue(2);
    }

    @Test
    void differentRequestRebuilds() {
        final AtomicLong height = new AtomicLong(100L);
        final var delegate = new CountingConstructor(height::get);
        final var cached = new CachedBundleConstructor(delegate, height::get);

        cached.createBundle(CHANNEL, requestFrom(1L));
        cached.createBundle(CHANNEL, requestFrom(2L));

        assertThat(delegate.calls).hasValue(2);
    }

    @Test
    void entryIsKeyedByTheProbedHeightNotTheDelegatesReportedHeight() {
        // Regression: the probe height and the height the delegate builds at can differ (two separate
        // chain reads; the head may advance between them). The entry must be keyed by the PROBED height
        // so a subsequent probe at that same height hits — filing it under the delegate's reported height
        // would be a guaranteed miss (no probe ever reads that height back).
        final AtomicLong probeHeight = new AtomicLong(100L);
        final var delegate = new CountingConstructor(() -> 200L); // always "builds at" a different height
        final var cached = new CachedBundleConstructor(delegate, probeHeight::get);

        final var first = cached.createBundle(CHANNEL, requestFrom(1L)); // probe 100 → stored at 100
        assertThat(delegate.calls).hasValue(1);

        // Same probe height, identical request → hit; the delegate is not invoked again.
        final var second = cached.createBundle(CHANNEL, requestFrom(1L));
        assertThat(delegate.calls).hasValue(1);
        assertThat(second).isEqualTo(first);

        // The delegate's reported height (200) is NOT a cache key: probing at 200 misses and rebuilds.
        probeHeight.set(200L);
        cached.createBundle(CHANNEL, requestFrom(1L));
        assertThat(delegate.calls).hasValue(2);
    }
}
