// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.grpc.server;

import com.hedera.hapi.node.state.clpr.ClprSyncPayload;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.swirlds.metrics.api.Metrics;
import org.hiero.clpr.relay.core.ChannelLookup;
import org.hiero.clpr.relay.core.metrics.LabeledCounter;

/**
 * Factory for {@link ClprSyncSession} instances, one per inbound two-phase {@code sync}
 * bidirectional-streaming call. Holds the channel-scoped collaborators (resolved by channel id)
 * and the shared metrics counters, injecting them into each stateful, single-use session.
 */
public class ClprSyncHandler {

    private final ChannelLookup channelLookup;
    private final String instanceName;
    private final LabeledCounter exchangesStarted;
    private final LabeledCounter exchangesFailed;
    private final LabeledCounter exchangesFailedAfterSend;
    private final LabeledCounter syncErrors;

    /**
     * @param metrics           metrics registry
     * @param instanceName      relay label stamped onto the session's log context
     */
    public ClprSyncHandler(final ChannelLookup channelLookup, final Metrics metrics, final String instanceName) {
        this.channelLookup = channelLookup;
        this.instanceName = instanceName;
        // The session/exchange lifecycle counters share the unified sync.exchanges.* family with the
        // outbound ChannelSyncTask; the responder increments them with role=responder.
        this.exchangesStarted = new LabeledCounter(
                "sync",
                "exchanges.started",
                "Two-phase sync exchanges started (role=initiator dialed a peer | responder received a phase-1 request)",
                metrics);
        this.exchangesFailed = new LabeledCounter(
                "sync", "exchanges.failed", "Two-phase sync exchanges that failed before completing", metrics);
        this.exchangesFailedAfterSend = new LabeledCounter(
                "sync",
                "exchanges.failed_after_send",
                "Two-phase exchanges that failed after we had already sent a non-empty bundle (possible non-reciprocity)",
                metrics);
        this.syncErrors = new LabeledCounter(
                "grpc",
                "sync.errors",
                "Inbound sync RPCs whose build/parse/submit step threw, labelled by reason",
                metrics);
    }

    /**
     * Creates a fresh session bound to the outbound response stream for one call.
     *
     * @param responses the response pipeline the session writes replies to
     * @return a new, stateful two-phase session
     */
    public ClprSyncSession newSession(final Pipeline<? super ClprSyncPayload> responses) {
        return new ClprSyncSession(
                responses,
                channelLookup,
                instanceName,
                exchangesStarted,
                exchangesFailed,
                exchangesFailedAfterSend,
                syncErrors);
    }
}
