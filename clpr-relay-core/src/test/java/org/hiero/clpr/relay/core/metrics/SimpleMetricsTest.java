// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.swirlds.metrics.api.Counter;
import com.swirlds.metrics.api.LongGauge;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SimpleMetricsTest {

    @Test
    void getOrCreate_isIdempotentForSameConfig() {
        final var m = new SimpleMetrics();
        final Counter first = m.getOrCreate(new Counter.Config("cat", "n"));
        final Counter second = m.getOrCreate(new Counter.Config("cat", "n"));
        assertThat(second).isSameAs(first);
    }

    @Test
    void getMetric_returnsRegistered_andNullForUnknown() {
        final var m = new SimpleMetrics();
        final Counter c = m.getOrCreate(new Counter.Config("cat", "n"));
        assertThat(m.getMetric("cat", "n")).isSameAs(c);
        assertThat(m.getMetric("cat", "missing")).isNull();
    }

    @Test
    void getOrCreate_incompatibleType_throws() {
        final var m = new SimpleMetrics();
        m.getOrCreate(new Counter.Config("cat", "n"));
        // Re-registering the same key with a different metric type must fail fast.
        assertThatThrownBy(() -> m.getOrCreate(new LongGauge.Config("cat", "n")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("incompatible type");
    }

    @Test
    void findMetricsByCategory_andGetAll() {
        final var m = new SimpleMetrics();
        m.getOrCreate(new Counter.Config("a", "one"));
        m.getOrCreate(new Counter.Config("a", "two"));
        m.getOrCreate(new Counter.Config("b", "three"));

        assertThat(m.findMetricsByCategory("a")).hasSize(2);
        assertThat(m.findMetricsByCategory("nope")).isEmpty();
        assertThat(m.getAll()).hasSize(3);
    }

    @Test
    void remove_byNameMetricAndConfig() {
        final var m = new SimpleMetrics();

        m.getOrCreate(new Counter.Config("cat", "byName"));
        m.remove("cat", "byName");
        assertThat(m.getMetric("cat", "byName")).isNull();

        final Counter byMetric = m.getOrCreate(new Counter.Config("cat", "byMetric"));
        m.remove(byMetric);
        assertThat(m.getMetric("cat", "byMetric")).isNull();

        final var cfg = new Counter.Config("cat", "byConfig");
        m.getOrCreate(cfg);
        m.remove(cfg);
        assertThat(m.getMetric("cat", "byConfig")).isNull();
    }

    @Test
    void runUpdaters_invokesThenStopsAfterRemoval() {
        final var m = new SimpleMetrics();
        final var count = new AtomicInteger();
        final Runnable updater = count::incrementAndGet;

        m.addUpdater(updater);
        m.runUpdaters();
        assertThat(count.get()).isEqualTo(1);

        m.removeUpdater(updater);
        m.runUpdaters();
        assertThat(count.get()).isEqualTo(1); // not run again after removal
    }

    @Test
    void runUpdatersSafely_swallowsThrowingUpdater() {
        final var m = new SimpleMetrics();
        final var good = new AtomicInteger();
        m.addUpdater(() -> {
            throw new RuntimeException("boom");
        });
        m.addUpdater(good::incrementAndGet);

        m.runUpdatersSafely(); // one updater throws; must not propagate
        assertThat(good.get()).isEqualTo(1);
    }

    @Test
    void start_isNoOp() {
        final var m = new SimpleMetrics();
        m.start();
        assertThat(m.getAll()).isEmpty();
    }
}
