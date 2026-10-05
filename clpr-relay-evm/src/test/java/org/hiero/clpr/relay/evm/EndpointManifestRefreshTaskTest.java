// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprEndpoint;
import com.hedera.hapi.node.state.clpr.ClprEndpointManifest;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.PeerEndpointCache;
import org.hiero.clpr.relay.core.PeerEndpointTlsRegistry;
import org.hiero.clpr.relay.core.metrics.SimpleMetrics;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link EndpointManifestRefreshTask}. Each poll reads the on-ledger peer endpoint
 * manifest and, whenever its {@code version} differs from the one last applied — including the first
 * observation — replaces the peer cache + mTLS registry. It reads only the manifest: no channel/queue
 * state, no lifecycle branching.
 */
class EndpointManifestRefreshTaskTest {

    private static final Bytes CHAN = Bytes.wrap(new byte[] {1});

    /**
     * Serves manifests from a queue (re-serving the last once drained, so a single element is a stable
     * steady state) and counts reads. Optionally throws on the first {@code throwCount} reads to exercise
     * the resilience loop.
     */
    private static final class StubManifestReader implements ContractStateReader {
        private final Queue<ClprEndpointManifest> manifests = new ArrayDeque<>();
        private volatile ClprEndpointManifest last = ClprEndpointManifest.DEFAULT;
        private final int throwCount;
        final AtomicInteger reads = new AtomicInteger();

        StubManifestReader() {
            this(0);
        }

        StubManifestReader(final int throwCount) {
            this.throwCount = throwCount;
        }

        StubManifestReader serve(final ClprEndpointManifest... ms) {
            manifests.addAll(List.of(ms));
            return this;
        }

        @Override
        public ClprEndpointManifest readPeerEndpointManifest(final Bytes id, final CommitmentLevel level) {
            final int n = reads.incrementAndGet();
            if (n <= throwCount) {
                // Same throw site every call → stable fingerprint, mirroring a real persistent fault.
                throw new IllegalStateException("simulated RPC failure");
            }
            final var next = manifests.poll();
            if (next != null) {
                last = next;
            }
            return last;
        }

        @Override
        public Optional<ClprChannel> readChannelState(final Bytes id, final String blockTag) {
            return Optional.empty();
        }

        @Override
        public List<QueuedMessage> readQueuedMessages(final Bytes id, final long f, final long t, final String tag) {
            return List.of();
        }

        @Override
        public ClprLedgerConfiguration readLedgerConfiguration(final CommitmentLevel level) {
            return ClprLedgerConfiguration.DEFAULT;
        }
    }

    private static ClprEndpointManifest manifest(final long version, final ClprEndpoint... endpoints) {
        return ClprEndpointManifest.newBuilder()
                .version(version)
                .endpoints(List.of(endpoints))
                .build();
    }

    private static ClprEndpoint endpoint(final int accountId) {
        return ClprEndpoint.newBuilder()
                .accountId(Bytes.wrap(new byte[] {(byte) accountId}))
                .build();
    }

    private static EndpointManifestRefreshTask task(
            final ContractStateReader reader, final PeerEndpointCache cache, final SimpleMetrics metrics) {
        return task(reader, cache, metrics, 1_000L, 30_000L);
    }

    private static EndpointManifestRefreshTask task(
            final ContractStateReader reader,
            final PeerEndpointCache cache,
            final SimpleMetrics metrics,
            final long backoffBaseMs,
            final long backoffCapMs) {
        return new EndpointManifestRefreshTask(
                CHAN,
                reader,
                CommitmentLevel.LATEST,
                cache,
                new PeerEndpointTlsRegistry(),
                metrics,
                50L, // pollIntervalMs — small so tests poll promptly
                backoffBaseMs,
                backoffCapMs);
    }

    private static void awaitUntil(final long timeoutMs, final BooleanSupplier cond) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("condition not met within " + timeoutMs + " ms");
    }

    // -------------------------------------------------------------------------

    @Test
    void refreshesOnFirstObservation() throws Exception {
        final var reader = new StubManifestReader().serve(manifest(1L, endpoint(7)));
        final var cache = new PeerEndpointCache();
        final var metrics = new SimpleMetrics();
        final var task = task(reader, cache, metrics);

        final var thread = Thread.startVirtualThread(task::run);
        try {
            awaitUntil(2_000L, cache::hasPeers);
            assertThat(cache.allPeers())
                    .extracting(ClprEndpoint::accountId)
                    .containsExactly(endpoint(7).accountId());
            final var refreshed = (Counter) metrics.getMetric("evm.listener", "manifest.refreshed[channel_id=01]");
            assertThat(refreshed.get()).isEqualTo(1L);
        } finally {
            task.stop();
            thread.join(2_000L);
        }
    }

    @Test
    void refreshesWhenVersionAdvances() throws Exception {
        final var reader = new StubManifestReader().serve(manifest(1L, endpoint(7)), manifest(2L, endpoint(8)));
        final var cache = new PeerEndpointCache();
        final var metrics = new SimpleMetrics();
        final var task = task(reader, cache, metrics);

        final var thread = Thread.startVirtualThread(task::run);
        try {
            // First observation refreshes to v1, the version advance refreshes again to v2.
            awaitUntil(2_000L, () -> {
                final var c = (Counter) metrics.getMetric("evm.listener", "manifest.refreshed[channel_id=01]");
                return c != null && c.get() == 2L;
            });
            assertThat(cache.allPeers())
                    .extracting(ClprEndpoint::accountId)
                    .containsExactly(endpoint(8).accountId());
        } finally {
            task.stop();
            thread.join(2_000L);
        }
    }

    @Test
    void doesNotRefreshWhenVersionUnchanged() throws Exception {
        // One manifest, re-served forever: after the first-observation refresh the stable version must
        // never trigger another refresh, no matter how many times it is polled.
        final var reader = new StubManifestReader().serve(manifest(1L, endpoint(7)));
        final var metrics = new SimpleMetrics();
        final var task = task(reader, new PeerEndpointCache(), metrics);

        final var thread = Thread.startVirtualThread(task::run);
        try {
            awaitUntil(2_000L, () -> reader.reads.get() >= 4);
            final var refreshed = (Counter) metrics.getMetric("evm.listener", "manifest.refreshed[channel_id=01]");
            assertThat(refreshed.get()).isEqualTo(1L);
        } finally {
            task.stop();
            thread.join(2_000L);
        }
    }

    // -------------------------------------------------------------------------
    // Resilience: the poll loop must survive unchecked exceptions, back off, and exit promptly on
    // interrupt. Backoff math is covered by FailStateTest; here we only assert resilience.
    // -------------------------------------------------------------------------

    @Test
    void survivesAndRecoversWhenReadThrows() throws Exception {
        // Three faults then a clean manifest forever.
        final var reader = new StubManifestReader(3).serve(manifest(1L, endpoint(7)));
        final var metrics = new SimpleMetrics();
        final var task = task(reader, new PeerEndpointCache(), metrics, 5L, 50L);

        final var thread = Thread.startVirtualThread(task::run);
        try {
            awaitUntil(5_000L, () -> {
                final var c = (Counter) metrics.getMetric("evm.listener", "poll.failed[channel_id=01]");
                return c != null && c.get() == 3L;
            });
            assertThat(task.isRunning()).isTrue();
        } finally {
            task.stop();
            thread.join(2_000L);
        }
    }

    @Test
    void exitsPromptlyOnInterruptDuringBackoff() throws Exception {
        // Always throws, with a long backoff so the loop is parked in Thread.sleep when interrupted.
        final var reader = new StubManifestReader(Integer.MAX_VALUE);
        final var metrics = new SimpleMetrics();
        final var task = task(reader, new PeerEndpointCache(), metrics, 60_000L, 60_000L);

        final var thread = Thread.startVirtualThread(task::run);
        try {
            awaitUntil(5_000L, () -> {
                final var c = (Counter) metrics.getMetric("evm.listener", "poll.failed[channel_id=01]");
                return c != null && c.get() >= 1L; // first failure → now parked in backoff
            });
            task.stop(); // interrupts the backoff sleep
            thread.join(2_000L);
            assertThat(thread.isAlive()).isFalse();
        } finally {
            thread.interrupt();
            thread.join(2_000L);
        }
    }
}
