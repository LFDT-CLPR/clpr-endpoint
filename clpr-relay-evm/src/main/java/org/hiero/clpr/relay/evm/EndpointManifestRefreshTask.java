// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import com.hedera.hapi.node.state.clpr.ClprEndpointManifest;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.logging.api.Logger;
import com.swirlds.logging.api.Loggers;
import com.swirlds.metrics.api.Metrics;
import java.util.HexFormat;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.FailState;
import org.hiero.clpr.relay.core.PeerEndpointCache;
import org.hiero.clpr.relay.core.PeerEndpointTlsRegistry;
import org.hiero.clpr.relay.core.metrics.LabeledCounter;
import org.jspecify.annotations.Nullable;

/**
 * Per-channel poller that keeps the channel's peer endpoint set current. Intended to run on its own
 * virtual thread.
 *
 * <p>Each cycle it reads the on-ledger peer {@link ClprEndpointManifest} at the configured commitment
 * level and compares its {@code version} against the version it last applied. When they differ — including
 * the first observation, which repopulates a cache empty at startup — it replaces the channel's
 * {@link PeerEndpointCache} and the mTLS {@link PeerEndpointTlsRegistry} wholesale from the fresh manifest
 * (replace, not merge; spec §2.4.2). Between cycles it sleeps a fixed interval.
 *
 * <p>This is all the relay needs from on-chain polling in the two-phase protocol: bundles are built ad hoc
 * from a peer's request (see {@code BundleConstructor.createBundle}), so there is no queue or lifecycle
 * state to observe here — only the peer roster. It reads nothing but the manifest.
 *
 * <p>The loop terminates when {@link #stop()} is called (e.g. relay shutdown). Any unchecked exception is
 * routed through {@link FailState} — logged, backed off (bounded/capped, never the tight cadence), and
 * retried — so the poller neither dies silently nor tight-loops on a persistent fault.
 */
public final class EndpointManifestRefreshTask implements Runnable {
    private static final Logger log = Loggers.getLogger(EndpointManifestRefreshTask.class);

    /** Sentinel "no manifest applied yet", so the first poll always refreshes (repopulating startup). */
    private static final long NO_VERSION_APPLIED = -1L;

    private final Bytes channelId;
    private final ContractStateReader stateReader;
    private final CommitmentLevel commitmentLevel;
    private final PeerEndpointCache peerCache;
    private final PeerEndpointTlsRegistry tlsRegistry;
    /** Interval in milliseconds between successive manifest reads. */
    private final long pollIntervalMs;

    private final String channelIdLabel;
    private final LabeledCounter pollFailed;
    private final LabeledCounter manifestRefreshed;
    private final FailState failState;

    @Nullable
    private volatile Thread thread;

    /**
     * Version of the peer manifest last applied to the cache, or {@link #NO_VERSION_APPLIED} before the
     * first refresh. Only read/written on the task's own poll thread.
     */
    private long appliedManifestVersion = NO_VERSION_APPLIED;

    /**
     * Flag used to request graceful shutdown of the poll loop. Declared {@code volatile} so writes are
     * visible across virtual threads.
     */
    private volatile boolean running = true;

    /**
     * Create a new {@code EndpointManifestRefreshTask} for a single channel.
     *
     * @param channelId       the CLPR channel whose peer manifest to track
     * @param stateReader     reads the on-ledger peer endpoint manifest
     * @param commitmentLevel the finality level at which to read the manifest
     * @param peerCache       the channel's peer endpoint cache, replaced from the on-ledger manifest
     *                        whenever its version advances
     * @param tlsRegistry     global TLS registry updated in parallel with the peer cache on every
     *                        manifest change so the mTLS trust manager stays current
     * @param metrics         registry for the {@code evm.listener.poll.failed} and
     *                        {@code evm.listener.manifest.refreshed} counters (labeled by {@code channel_id})
     * @param pollIntervalMs  interval in ms between successive manifest reads
     * @param backoffBaseMs   first-failure backoff in ms (doubled per consecutive failure)
     * @param backoffCapMs    ceiling on the per-failure backoff in ms
     */
    public EndpointManifestRefreshTask(
            final Bytes channelId,
            final ContractStateReader stateReader,
            final CommitmentLevel commitmentLevel,
            final PeerEndpointCache peerCache,
            final PeerEndpointTlsRegistry tlsRegistry,
            final Metrics metrics,
            final long pollIntervalMs,
            final long backoffBaseMs,
            final long backoffCapMs) {
        this.channelId = channelId;
        this.stateReader = stateReader;
        this.commitmentLevel = commitmentLevel;
        this.peerCache = peerCache;
        this.tlsRegistry = tlsRegistry;
        this.pollIntervalMs = pollIntervalMs;
        this.channelIdLabel = HexFormat.of().formatHex(channelId.toByteArray());
        this.pollFailed = new LabeledCounter(
                "evm.listener", "poll.failed", "Manifest-refresh poll iterations that threw an exception", metrics);
        this.manifestRefreshed = new LabeledCounter(
                "evm.listener",
                "manifest.refreshed",
                "Peer manifest cache refreshes applied after an on-ledger manifest-version advance",
                metrics);
        this.failState = new FailState(backoffBaseMs, backoffCapMs);
    }

    /** Run the poll loop. Designed to be called on a virtual thread. */
    @Override
    public void run() {
        this.thread = Thread.currentThread();
        while (running) {
            try {
                pollOnce();
                final int cleared = failState.onSuccess();
                if (cleared > 0) {
                    log.info("recovered after {} consecutive failure(s)", cleared);
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("manifest-refresh loop interrupted; stopping");
                break;
            } catch (final Exception e) {
                if (!running) {
                    // Shutting down: a read that fails because the JSON-RPC client was interrupted or its
                    // endpoint went away is expected teardown, not a fault. Exit quietly.
                    break;
                }
                pollFailed.increment("channel_id", channelIdLabel);
                final var decision = failState.onFailure(e);
                if (decision.logFullDetail()) {
                    log.error(
                            "failure #{} fp={}: {}",
                            e,
                            decision.consecutiveFailures(),
                            decision.fingerprint(),
                            e.getMessage());
                } else {
                    log.error("failure #{} fp={} (repeat)", decision.consecutiveFailures(), decision.fingerprint());
                }
                try {
                    //noinspection BusyWait — periodic poll cadence;
                    Thread.sleep(decision.backoffMs());
                } catch (final InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /**
     * Execute a single poll: read the current peer manifest and, if its version differs from the one last
     * applied, refresh the peer cache. Then sleep the success-path cadence.
     *
     * @throws InterruptedException if the thread is interrupted during the cadence sleep
     */
    private void pollOnce() throws InterruptedException {
        final ClprEndpointManifest manifest = stateReader.readPeerEndpointManifest(channelId, commitmentLevel);
        if (manifest.version() != appliedManifestVersion) {
            refresh(manifest);
        }
        Thread.sleep(pollIntervalMs);
    }

    /**
     * Replace the channel's peer cache and the mTLS trust registry from {@code manifest} (replace, not
     * merge) and record its version as applied.
     */
    private void refresh(final ClprEndpointManifest manifest) {
        if (appliedManifestVersion == NO_VERSION_APPLIED) {
            log.info("initial manifest sync version={}", manifest.version());
        } else {
            log.info(
                    "manifest version advanced {} -> {}; refreshing cache", appliedManifestVersion, manifest.version());
        }

        // The manifest endpoints carry the on-chain TLS certs, so one read feeds both the peer cache and
        // the mTLS trust registry (replace, not merge).
        peerCache.replaceAll(manifest.endpoints());
        tlsRegistry.update(channelId, manifest.endpoints());
        appliedManifestVersion = manifest.version();

        if (manifest.endpoints().isEmpty()) {
            log.warn("peer manifest is now empty after refresh; sync will stall until it is repopulated");
            return;
        }
        log.info(
                "manifest cache refreshed: {} endpoint(s) (version={})",
                manifest.endpoints().size(),
                manifest.version());
        manifestRefreshed.increment("channel_id", channelIdLabel);
    }

    /**
     * Request the poll loop to stop after the current iteration completes. The interrupt only shortens the
     * wait.
     */
    public void stop() {
        running = false;
        final var t = thread;
        if (t != null) {
            t.interrupt();
        }
    }

    /**
     * Returns {@code true} if the poll loop is still running.
     *
     * @return whether the task is running
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Returns the channel identifier tracked by this task.
     *
     * @return the channel ID
     */
    public Bytes channelId() {
        return channelId;
    }

    /**
     * Returns this loop's failure state machine.
     *
     * @return the {@link FailState}
     */
    public FailState failState() {
        return failState;
    }
}
