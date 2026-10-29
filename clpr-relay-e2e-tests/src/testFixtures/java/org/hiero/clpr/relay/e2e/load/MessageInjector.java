// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.load;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.hiero.clpr.relay.e2e.chain.Accounts;
import org.hiero.clpr.relay.e2e.chain.BesuNetwork;
import org.hiero.clpr.relay.e2e.chain.TestAccount;
import org.hiero.clpr.relay.e2e.channel.ChannelSide;
import org.hiero.clpr.relay.e2e.channel.ClprAbi;
import org.hiero.clpr.relay.evm.AbiCodec;

/**
 * Queues messages on a channel side, fast enough to build a real backlog.
 *
 * <p>Two properties make this more than a loop around {@code sendMessage}. Sends are pipelined —
 * signed against a locally tracked nonce and fired without waiting for receipts — because a
 * receipt-per-send loop tops out at one message per block and can never produce a queue. And the
 * batch is spread over several sender accounts, since a node bounds how many future transactions one
 * sender may have pooled (200 in Besu by default) and silently drops the rest.
 *
 * <p>Sender accounts are separate from the endpoints' signing keys and from the channel operator. A
 * shared account would interleave two independent nonce sequences on one address and produce
 * {@code nonce too low} rejections that look exactly like the relay-side submission bug this suite
 * exists to detect.
 */
public final class MessageInjector {

    /** Gas limit per send. Generous: the service writes queue state and emits events. */
    private static final long SEND_GAS_LIMIT = 0xffffffL;

    /** Sender accounts per chain. Four times Besu's 200-per-sender pool depth is ample headroom. */
    private static final int SENDERS_PER_CHAIN = 4;

    private final Accounts accounts;
    private final Map<Long, List<PipelinedTxSubmitter>> sendersByChain = new LinkedHashMap<>();

    public MessageInjector(final Accounts accounts) {
        this.accounts = accounts;
    }

    /**
     * Queue {@code count} messages on {@code side}, addressed to the peer's application.
     *
     * <p>Returns as soon as every send has been accepted by the node; call
     * {@link InjectionRun#awaitAllConfirmed} to reconcile receipts.
     *
     * @param side the sending side
     * @param count how many messages
     * @param shape what the messages look like
     * @return the run handle
     */
    public InjectionRun enqueue(final ChannelSide side, final int count, final MessageShape shape) {
        final BesuNetwork net = side.network();
        final InjectionRun run = new InjectionRun(net.rpc(), side.channelId());
        final List<PipelinedTxSubmitter> senders = sendersFor(net);
        final byte[] targetApp = AbiCodec.fromHex(side.peer().applicationAddress());

        for (int i = 0; i < count; i++) {
            final byte[] callData =
                    ClprAbi.sendMessage(side.channelId(), side.connector().id(), targetApp, shape.payload(i));
            final PipelinedTxSubmitter sender = senders.get(i % senders.size());
            try {
                run.recordSubmitted(sender.submit(side.clprServiceAddress(), callData, 0L, SEND_GAS_LIMIT));
            } catch (final RuntimeException e) {
                // A send-side rejection never reaches the pool, so it has no receipt to reconcile.
                // Recording it separately keeps "the send side failed" distinct from "the relay
                // failed", which is the whole point of the error histogram.
                run.recordRejected(shortReason(e));
                sender.resetNonce();
            }
        }
        return run;
    }

    /**
     * Queue {@code total} messages while holding at most {@code maxInFlight} of them unacknowledged.
     *
     * <p>The mode to use whenever a test is not deliberately building a backlog. The relay currently
     * cannot drain a queue deeper than its per-bundle batch — it bundles the first batch but carries a
     * running hash for the whole queue, so the peer rejects every bundle and the channel wedges
     * permanently (clpr-hiero#171). Keeping the in-flight window below that batch size is what makes
     * a sustained load test possible at all today.
     *
     * @param side the sending side
     * @param total how many messages in total
     * @param maxInFlight ceiling on queued-but-unacknowledged messages
     * @param timeout overall budget
     * @param shape what the messages look like
     * @return the run handle
     */
    public InjectionRun enqueueWithBackpressure(
            final ChannelSide side,
            final int total,
            final int maxInFlight,
            final Duration timeout,
            final MessageShape shape) {
        if (maxInFlight < 1) {
            throw new IllegalArgumentException("maxInFlight must be >= 1, got " + maxInFlight);
        }
        final BesuNetwork net = side.network();
        final InjectionRun run = new InjectionRun(net.rpc(), side.channelId());
        final List<PipelinedTxSubmitter> senders = sendersFor(net);
        final byte[] targetApp = AbiCodec.fromHex(side.peer().applicationAddress());
        final long deadline = System.currentTimeMillis() + timeout.toMillis();

        // Baseline the peer's watermark: it is a per-channel counter, so an earlier run on the same
        // channel would otherwise make the first window look already drained.
        final long deliveredBefore = side.peer().state().receivedMessageId();

        int sent = 0;
        while (sent < total) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("only queued " + sent + " of " + total + " messages within " + timeout
                        + " while holding the in-flight window at " + maxInFlight + "; the peer has processed "
                        + (side.peer().state().receivedMessageId() - deliveredBefore)
                        + " so far, so the channel is draining slower than the budget allows");
            }
            // In-flight is measured as "fired minus processed by the peer", NOT from the sender's
            // on-chain queue depth. Sends are pipelined and unmined for a moment, so the queue depth
            // still reads zero right after firing — using it would let the loop empty the whole batch
            // in one pass and build exactly the backlog this mode exists to avoid.
            final long delivered = side.peer().state().receivedMessageId() - deliveredBefore;
            final int room = (int) Math.min(maxInFlight - (sent - delivered), total - sent);
            if (room <= 0) {
                sleep(250L);
                continue;
            }
            for (int i = 0; i < room; i++) {
                final byte[] callData =
                        ClprAbi.sendMessage(side.channelId(), side.connector().id(), targetApp, shape.payload(sent));
                final PipelinedTxSubmitter sender = senders.get(sent % senders.size());
                try {
                    run.recordSubmitted(sender.submit(side.clprServiceAddress(), callData, 0L, SEND_GAS_LIMIT));
                } catch (final RuntimeException e) {
                    run.recordRejected(shortReason(e));
                    sender.resetNonce();
                }
                sent++;
            }
        }
        return run;
    }

    /**
     * Sender accounts for a chain, created on first use.
     *
     * <p>Cached per chain so a test that injects several times reuses the same nonce sequences; fresh
     * submitters would each re-read {@code pending} and collide with their predecessors' in-flight
     * transactions.
     */
    private List<PipelinedTxSubmitter> sendersFor(final BesuNetwork net) {
        return sendersByChain.computeIfAbsent(net.chainId(), chainId -> {
            final List<PipelinedTxSubmitter> senders = new ArrayList<>(SENDERS_PER_CHAIN);
            for (int i = 0; i < SENDERS_PER_CHAIN; i++) {
                final TestAccount account = accounts.allocate(net.id() + " message sender #" + i);
                senders.add(new PipelinedTxSubmitter(net.rpc(), account, chainId));
            }
            return senders;
        });
    }

    private static String shortReason(final RuntimeException e) {
        final String message = e.getMessage();
        if (message == null) {
            return e.getClass().getSimpleName();
        }
        return message.length() > 160 ? message.substring(0, 160) + "..." : message;
    }

    private static void sleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while applying backpressure", e);
        }
    }
}
