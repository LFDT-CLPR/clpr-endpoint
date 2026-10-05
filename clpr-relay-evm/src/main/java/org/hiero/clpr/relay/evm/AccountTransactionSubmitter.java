// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import com.fasterxml.jackson.databind.JsonNode;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprEndpoint;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.logging.api.Logger;
import com.swirlds.logging.api.Loggers;
import com.swirlds.metrics.api.DoubleGauge;
import com.swirlds.metrics.api.Metrics;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.hiero.clpr.relay.core.BundleLog;
import org.hiero.clpr.relay.core.EvmSubmissionConfig;
import org.hiero.clpr.relay.core.ParsedBundle;
import org.hiero.clpr.relay.core.metrics.LabeledCounter;
import org.hiero.clpr.relay.core.metrics.MetricLabels;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.jspecify.annotations.Nullable;

/**
 * Fully-serialised, queue-based transaction submitter for a single EVM signing account (EOA) on one
 * chain. One instance is shared by every channel/service that signs from the same key on the same
 * network.
 *
 * <p><b>Why this shape.</b> A single dedicated worker thread drains bounded per-channel FIFO
 * queues (round-robin; see below) and drives each request to a <em>definite on-chain verdict</em> — a mined receipt (success or revert) or a
 * definite pre-mempool rejection — before taking the next one, so <em>at most one transaction per
 * account is ever in flight</em>. That property is what removes the need for any nonce tracking: the
 * nonce is read once from the account's {@code latest} (mined) transaction count immediately before
 * signing, and because nothing of ours is unconfirmed at that point, {@code latest} is always the
 * correct next nonce. There is no local nonce cache to drift, no mempool backlog to bound, and no
 * cross-channel nonce collision to serialise against.
 *
 * <p><b>Drive to a verdict, then a bounded safety valve.</b> The transaction is signed at the fixed
 * nonce and re-sent each unmined round. Rather than bump the fee blindly on a timeout — with a large
 * gas limit that would inflate {@code maxFee * gasLimit} toward the account balance and the node's
 * per-tx fee cap and strand the transaction — each round re-reads the <em>live market fee</em>: if it
 * now exceeds what we last signed, the tx is re-priced up to it (clamped to the gas-price cap) and
 * re-signed at the same nonce; if it is the same or lower, the current tx is re-sent verbatim. This
 * clears a base-fee rise that occurs after the tx is pooled (which re-appears as "already known",
 * never "underpriced") by tracking the market instead of over-bumping. The fee is also raised when
 * the node explicitly reports it under-priced. The worker stops driving a request when it observes a
 * mined receipt, a definite pre-mempool rejection, a confirmed nonce advance past ours (some other tx
 * took the slot), or — the safety valve — after {@code MAX_SUBMIT_ROUNDS} without a verdict. That cap is essential: the single worker advances
 * round-robin only once a request finishes, so one permanently unmineable transaction would otherwise
 * wedge every channel sharing the key. Abandoning does not drop the bundle — the sync loop
 * reconstructs and re-submits it from fresh on-chain state.
 *
 * <p><b>Per-channel queues, round-robin drain.</b> Requests are held in one bounded FIFO
 * <em>per channel</em>, and the single worker drains them round-robin — one request per
 * channel per turn — so a high-volume or misbehaving channel cannot starve the others that
 * share this account. Within a channel, order is preserved and requests are never coalesced (a
 * re-poke never evicts an already-queued bundle); when one channel's queue is full its
 * <em>new</em> request is dropped (WARN + {@code evm.tx.queue_dropped}), which bounds memory and
 * cannot block the sync loops or affect any other channel.
 *
 * <p><b>Duplicate suppression.</b> A bundle whose <em>exact bytes</em> are already pending in a
 * channel's queue is suppressed at enqueue ({@code evm.tx.queue_deduped}). The pull
 * {@code ChannelSyncTask} and push {@code ClprSyncHandler} paths both re-offer the same bundle
 * repeatedly while it is undelivered; without this the FIFO fills with identical copies and a
 * genuinely-new bundle is dropped at enqueue. This is content-exact, not counter-keyed — so unlike the
 * removed {@code (next, acked, status)} fingerprint it can never suppress a distinct PAUSE-recovery
 * bundle carrying the same counters as the offending one, and it keeps the existing entry rather than
 * letting a re-offer displace it. It is only a queue-hygiene guard: the true duplicate gate remains
 * the gas-free {@code eth_call} preview run just before each paid submission — a bundle the chain has
 * already applied reverts the preview (replay / running-hash mismatch) and is skipped without spending
 * gas. Because the worker is serial and confirm-before-advance, the second of two overlapping bundles
 * is previewed only after the first has mined, so it reverts. The one thing skipped without a preview
 * is an empty <em>ACTIVE</em>
 * initial-state bundle ({@code nextMessageId ≤ 1}, {@code receivedMessageId == 0}, status
 * {@code ACTIVE}) — it would revert {@code EmptyBundle}. A status-only close (CLOSING/DRAINED/CLOSED)
 * at those same counters is <em>not</em> empty and is always submitted so the lifecycle advances.
 *
 * <p>This class owns the per-channel bundle-outcome metrics (tagged {@code channel_id}): a
 * preview-rejected bundle increments {@code sync.bundle.skipped} ({@code reason=rejected}); a
 * receipt-confirmed submission increments {@code sync.bundle.submissions} plus the
 * {@code messages_submitted} / {@code bytes_submitted} volume counters and {@code evm.tx.submissions};
 * a reverted submission increments {@code sync.bundle.reverted} and {@code evm.tx.reverts}; a
 * fee-computation or send failure increments {@code evm.tx.failures}; a queue-full drop increments
 * {@code evm.tx.queue_dropped}; and an exact-duplicate re-offer suppressed at enqueue increments
 * {@code evm.tx.queue_deduped}.
 */
public final class AccountTransactionSubmitter {

    private static final Logger LOGGER = Loggers.getLogger(AccountTransactionSubmitter.class);

    /**
     * Cap (and fallback) gas limit for submitBundle calls. Each transaction is normally sized by
     * {@code eth_estimateGas} plus a safety margin; this value is the upper bound applied to that
     * estimate and the value used if estimation fails. 28M: the Hiero on-chain bundle verifier
     * re-walks the SHA-384 merkle spine in pure Solidity (~9M base + ~4M per message leaf), so a
     * multi-message inbound bundle needs ~17M gas. Stays under the 30M block gas limit. NOTE:
     * requires the chain NOT to be on EVM Osaka, whose EIP-7825 caps any single tx/eth_call at
     * 16,777,216 gas (the e2e Besu genesis disables Osaka for exactly this reason).
     */
    private static final long DEFAULT_GAS_LIMIT = 28_000_000L;

    /**
     * Safety margin applied to the {@code eth_estimateGas} result (numerator/denominator = +50%),
     * covering small differences between the estimate and the real execution's gas. Sizing the
     * transaction to its actual cost (rather than a flat 28M) keeps the node's upfront reservation
     * ({@code maxFee * gasLimit}) proportional to real usage, so a modestly funded account is not
     * stranded once its balance drops below a 28M-sized reservation.
     */
    private static final long GAS_ESTIMATE_MARGIN_NUMERATOR = 3L;

    private static final long GAS_ESTIMATE_MARGIN_DENOMINATOR = 2L;

    /**
     * Fee bump applied per unmined round (numerator/denominator = +25%). Must exceed the node's
     * minimum replacement price bump (Besu's default is 10%) so a re-signed same-nonce transaction
     * is accepted as a replacement rather than rejected as under-priced.
     */
    private static final long FEE_BUMP_NUMERATOR = 5L;

    private static final long FEE_BUMP_DENOMINATOR = 4L;

    /** Once a transaction has gone this many rounds unmined, each further round logs at WARN. */
    private static final int RESEND_WARN_AFTER_ROUNDS = 2;

    /**
     * Bounded safety valve: after this many rounds without a definite verdict, abandon the request
     * and let the channel's sync loop re-drive it. The single worker advances round-robin only
     * once a request reaches a verdict, so without this cap one permanently unmineable transaction
     * would wedge the account and starve every other channel sharing the key. Abandoning frees the
     * worker; the bundle is not lost — the sync loop reconstructs and re-submits it from fresh
     * on-chain state. Each unmined round is ~one receipt-poll window, so this bounds the wedge to a
     * few minutes.
     */
    private static final int MAX_SUBMIT_ROUNDS = 60;

    /**
     * A self-contained snapshot of one submit request. Holds no live {@link ClprChannel} — every
     * value needed at send time is captured at enqueue time, so the worker never touches shared,
     * mutable channel state.
     *
     * @param contractAddress    hex address of the {@code ClprService} contract to submit against
     * @param channelId       the 32-byte channel id
     * @param bundleBytes         the raw bundle bytes submitted verbatim to {@code submitBundle}
     * @param messageCount       number of application messages in the bundle (for volume metrics)
     * @param channelIdLabel  the lower-case hex channel id used as the {@code channel_id} metric label
     * @param peer               the peer the bundle came from, used to label the submission-outcome
     *                           metrics ({@code null} when the peer is unknown)
     */
    record SubmitRequest(
            String contractAddress,
            Bytes channelId,
            Bytes bundleBytes,
            int messageCount,
            String channelIdLabel,
            @Nullable ClprEndpoint peer) {}

    /** Metric label for the peer a bundle came from — its roster account id, or {@code anonymous}. */
    private static String peerLabel(@Nullable final ClprEndpoint peer) {
        return peer != null ? peer.accountId().toHex() : "anonymous";
    }

    private final EvmJsonRpcClient rpcClient;
    private final EthSigner signer;
    private final String fromAddress;
    private final long chainId;
    private final Eip1559GasStrategy gasStrategy;
    private final Sleeper sleeper;

    private final int perChannelQueueCapacity;

    // Per-channel FIFO queues drained round-robin by the single worker so no channel starves
    // another. {@code byChannel} maps channel-id label -> its pending requests; {@code
    // readyOrder} is the round-robin rotation of channel labels that currently have work (each
    // present at most once); {@code queuedBundleKeys} mirrors each channel's queued bundle bytes so
    // exact-duplicate re-offers are detected in O(1) without scanning the FIFO (kept in lockstep with
    // {@code byChannel} on every enqueue/dequeue). All four are guarded by {@code lock}; {@code
    // notEmpty} wakes the worker when work arrives.
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Map<String, ArrayDeque<SubmitRequest>> byChannel = new HashMap<>();
    private final ArrayDeque<String> readyOrder = new ArrayDeque<>();
    private final Map<String, Set<Bytes>> queuedBundleKeys = new HashMap<>();

    private volatile boolean running;

    private volatile Thread worker;

    // Bundle-outcome metrics (category "sync"), tagged channel_id.
    private final LabeledCounter bundleSkipped;
    private final LabeledCounter bundleSubmissions;
    private final LabeledCounter bundleMessagesSubmitted;
    private final LabeledCounter bundleBytesSubmitted;
    private final LabeledCounter bundleReverted;

    // EVM transaction metrics (category "evm"), tagged channel_id.
    private final LabeledCounter txSubmissions;
    private final LabeledCounter txReverts;
    private final LabeledCounter txFailures;
    private final LabeledCounter txQueueDropped;
    private final LabeledCounter txQueueDeduped;

    /**
     * Convenience constructor that uses the default {@link Sleeper}. Prefer this one in production;
     * the full constructor is intended for tests that want to skip the receipt-polling delay.
     *
     * @param rpcClient     JSON-RPC client for this account's chain
     * @param signer        the ECDSA signer for this account (reused as-is; never mutated here)
     * @param chainId       EIP-155 chain id
     * @param networkId     local-network id, used only as the {@code network} label on the balance gauge
     * @param gasStrategy   EIP-1559 fee strategy
     * @param metrics       metrics registry
     * @param perChannelQueueCapacity bounded capacity of each per-channel submit queue
     */
    public AccountTransactionSubmitter(
            final EvmJsonRpcClient rpcClient,
            final EthSigner signer,
            final long chainId,
            final String networkId,
            final Eip1559GasStrategy gasStrategy,
            final Metrics metrics,
            final int perChannelQueueCapacity) {
        this(rpcClient, signer, chainId, networkId, gasStrategy, metrics, perChannelQueueCapacity, Sleeper.DEFAULT);
    }

    /**
     * Full constructor including a custom {@link Sleeper} for tests.
     *
     * @param rpcClient     JSON-RPC client for this account's chain
     * @param signer        the ECDSA signer for this account (reused as-is; never mutated here)
     * @param chainId       EIP-155 chain id
     * @param networkId     local-network id, used only as the {@code network} label on the balance gauge
     * @param gasStrategy   EIP-1559 fee strategy
     * @param metrics       metrics registry
     * @param perChannelQueueCapacity bounded capacity of each per-channel submit queue
     * @param sleeper       sleep abstraction used between receipt polls
     */
    AccountTransactionSubmitter(
            final EvmJsonRpcClient rpcClient,
            final EthSigner signer,
            final long chainId,
            final String networkId,
            final Eip1559GasStrategy gasStrategy,
            final Metrics metrics,
            final int perChannelQueueCapacity,
            final Sleeper sleeper) {
        if (perChannelQueueCapacity <= 0) {
            throw new IllegalArgumentException(
                    "perChannelQueueCapacity must be positive, got " + perChannelQueueCapacity);
        }
        this.rpcClient = rpcClient;
        this.signer = signer;
        this.fromAddress = signer.address();
        this.chainId = chainId;
        this.gasStrategy = gasStrategy;
        this.sleeper = sleeper;
        this.perChannelQueueCapacity = perChannelQueueCapacity;

        this.bundleSkipped = new LabeledCounter(
                "sync", "bundle.skipped", "Peer bundles skipped before submission, by reason", metrics);
        this.bundleSubmissions =
                new LabeledCounter("sync", "bundle.submissions", "Peer bundles confirmed submitted on-chain", metrics);
        this.bundleMessagesSubmitted = new LabeledCounter(
                "sync", "bundle.messages_submitted", "Messages carried in confirmed peer bundles", metrics);
        this.bundleBytesSubmitted = new LabeledCounter(
                "sync", "bundle.bytes_submitted", "Total bytes of confirmed peer bundle payloads", metrics);
        this.bundleReverted =
                new LabeledCounter("sync", "bundle.reverted", "Bundles whose submission reverted on-chain", metrics);

        this.txSubmissions = new LabeledCounter(
                "evm", "tx.submissions", "Bundle transactions confirmed submitted by receipt", metrics);
        this.txReverts = new LabeledCounter("evm", "tx.reverts", "Bundle transactions that reverted on-chain", metrics);
        this.txFailures = new LabeledCounter("evm", "tx.failures", "Bundle submissions that failed outright", metrics);
        this.txQueueDropped = new LabeledCounter(
                "evm", "tx.queue_dropped", "Submit requests dropped because the per-channel queue was full", metrics);
        this.txQueueDeduped = new LabeledCounter(
                "evm",
                "tx.queue_deduped",
                "Submit requests suppressed because an identical bundle was already queued for the channel",
                metrics);

        // Per-account balance gauge, tagged account + network, refreshed on each metrics scrape (a
        // low-balance account can no longer land transactions, so this is a key ops signal).
        final DoubleGauge balanceGauge = metrics.getOrCreate(new DoubleGauge.Config(
                        "evm",
                        MetricLabels.labeled(
                                "account.balance_eth", Map.of("account", fromAddress, "network", networkId)))
                .withDescription("ETH balance of the signing account"));
        metrics.addUpdater(() -> {
            try {
                final BigInteger wei = rpcClient.ethGetBalance(fromAddress, "latest");
                balanceGauge.set(new BigDecimal(wei)
                        .divide(BigDecimal.TEN.pow(18), 18, RoundingMode.HALF_UP)
                        .doubleValue());
            } catch (final RuntimeException e) {
                LOGGER.warn("failed to read balance for account {}", e, fromAddress);
            }
        });
    }

    /**
     * Start the dedicated worker thread. Idempotent: a second call while already running is a no-op.
     */
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        worker = Thread.ofVirtual().name("clpr-acct-submitter-" + fromAddress).start(this::runLoop);
    }

    /**
     * Signal the worker to stop and interrupt any in-flight {@link #takeNext()} wait or receipt-poll
     * sleep. Requests still queued are discarded.
     */
    public synchronized void stop() {
        running = false;
        final Thread w = worker;
        if (w != null) {
            w.interrupt();
        }
    }

    /**
     * Enqueue a bundle for submission to {@code contractAddress} on behalf of {@code channelId},
     * attributing its submission-outcome metrics to {@code peer}. The bundle joins the signing account's
     * serial FIFO queue; the worker previews it with a gas-free {@code eth_call} and skips (unpaid)
     * anything the contract would revert, so the enqueue never blocks on-chain.
     *
     * @param contractAddress the {@code ClprService} contract address (hex) to submit against
     * @param channelId       the 32-byte CLPR channel identifier
     * @param bundle          the parsed bundle to submit
     * @param peer the peer the bundle came from, used to label the {@code sync.bundle.submissions} /
     *             {@code sync.bundle.reverted} counters ({@code null} → {@code anonymous})
     */
    public void submit(
            final String contractAddress,
            final Bytes channelId,
            final ParsedBundle bundle,
            @Nullable final ClprEndpoint peer) {
        final String connIdLabel = HexFormat.of().formatHex(channelId.toByteArray());
        final long next = bundle.metadata().nextMessageId();
        final long acked = bundle.metadata().receivedMessageId();

        // Empty initial-state bundle (post-completeChannel): nothing to deliver AND no lifecycle
        // change to convey, so it would revert on-chain with EmptyBundle — skip it here rather than
        // pay for a preview. The status guard is load-bearing: a status-only close (CLOSING/DRAINED/
        // CLOSED) rides at next=1/acked=0 on a channel that never carried data, and it MUST land to
        // advance the lifecycle, so only an ACTIVE bundle is dropped here. A stale bundle whose exact
        // bytes are already queued is suppressed at enqueue (below); any other stale one is left for
        // the send-time preview to reject (replay / running-hash).
        if (next <= 1 && acked == 0 && bundle.metadata().status() == ClprChannelStatus.ACTIVE) {
            bundleSkipped.increment("channel_id", connIdLabel, "peer", peerLabel(peer), "reason", "empty");
            LOGGER.debug(
                    "submitBundle: skipping empty initial-state bundle for {} (next={} acked={})",
                    connIdLabel,
                    next,
                    acked);
            return;
        }

        final SubmitRequest request = new SubmitRequest(
                contractAddress,
                channelId,
                bundle.rawBundleBytes(),
                bundle.messages().size(),
                connIdLabel,
                peer);
        lock.lock();
        try {
            final ArrayDeque<SubmitRequest> connQueue = byChannel.computeIfAbsent(connIdLabel, k -> new ArrayDeque<>());
            final Set<Bytes> queuedKeys = queuedBundleKeys.computeIfAbsent(connIdLabel, k -> new HashSet<>());
            final Bytes bundleKey = request.bundleBytes();
            // Suppress exact-duplicate bundles already pending for this channel. The pull
            // (ChannelSyncTask) and push (ClprSyncHandler) sync paths both re-offer the same bundle
            // repeatedly while it is undelivered; without this the FIFO fills with identical copies and
            // genuinely-new bundles are dropped at enqueue. The per-channel hash set detects it in
            // O(1) (no FIFO scan; {@link Bytes} hashes/compares by content). Content-exact (not
            // counter-keyed) so it can never suppress a distinct PAUSE-recovery bundle, and it keeps
            // the existing entry rather than letting a re-offer displace it.
            if (queuedKeys.contains(bundleKey)) {
                txQueueDeduped.increment("channel_id", connIdLabel);
                LOGGER.trace(
                        "submitBundle: identical bundle already queued for {}; skipping re-enqueue {}",
                        connIdLabel,
                        BundleLog.tag(bundle.rawBundleBytes()));
                return;
            }
            if (connQueue.size() >= perChannelQueueCapacity) {
                txQueueDropped.increment("channel_id", connIdLabel);
                LOGGER.warn(
                        "submitBundle: per-channel submit queue full (capacity={}) for {} channel {}; dropping {}",
                        perChannelQueueCapacity,
                        fromAddress,
                        connIdLabel,
                        BundleLog.tag(bundle.rawBundleBytes()));
                return;
            }
            final boolean wasEmpty = connQueue.isEmpty();
            connQueue.addLast(request);
            queuedKeys.add(bundleKey);
            if (wasEmpty) {
                // First pending request for this channel: join the round-robin rotation.
                readyOrder.addLast(connIdLabel);
            }
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    private void runLoop() {
        while (running) {
            final SubmitRequest request;
            try {
                request = takeNext();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            try {
                process(request);
            } catch (final RuntimeException e) {
                // Defensive: process() should reach a definite outcome on its own, but never let an
                // unexpected fault kill the account's only worker thread.
                LOGGER.error("submitBundle worker: unexpected error for channel {}", e, request.channelIdLabel());
            }
        }
    }

    /**
     * Block until work is available, then return the head request of the next channel in
     * round-robin order. Serving one request per channel per turn guarantees a high-volume (or
     * misbehaving) channel cannot starve the others sharing this account.
     */
    private SubmitRequest takeNext() throws InterruptedException {
        lock.lock();
        try {
            while (readyOrder.isEmpty()) {
                notEmpty.await();
            }
            final String connIdLabel = readyOrder.pollFirst();
            final ArrayDeque<SubmitRequest> connQueue = byChannel.get(connIdLabel);
            final SubmitRequest request = connQueue.pollFirst();
            // Keep the dedup index in lockstep: the dequeued bundle is no longer pending, so its key
            // must leave the set (its content may legitimately be re-offered and re-enqueued later).
            final Set<Bytes> queuedKeys = queuedBundleKeys.get(connIdLabel);
            if (queuedKeys != null) {
                queuedKeys.remove(request.bundleBytes());
            }
            if (connQueue.isEmpty()) {
                // Drop the now-empty deque (and its key set) so the maps don't grow unbounded with
                // discovery-driven channel churn; submit() re-creates them via computeIfAbsent.
                byChannel.remove(connIdLabel);
                queuedBundleKeys.remove(connIdLabel);
            } else {
                // Still has work: rotate to the back so every other channel is served first.
                readyOrder.addLast(connIdLabel);
            }
            return request;
        } finally {
            lock.unlock();
        }
    }

    /** Outcome of a receipt-poll round. */
    private enum Outcome {
        SUCCESS,
        REVERTED,
        NONE
    }

    /** Classification of a {@code eth_sendRawTransaction} error. */
    private enum SendClass {
        /** Our identical transaction is already in the pool — proceed to poll. */
        ALREADY_KNOWN,
        /** A tx at this nonce already mined — usually one we sent; if not, confirm via the on-chain nonce. */
        NONCE_ALREADY_MINED,
        /** The node rejected the fee as under-priced (base fee rose) — raise it once and retry. */
        UNDERPRICED,
        /** The fee exceeds the node's per-tx cap or what the balance can reserve — stop raising it. */
        FEE_TOO_HIGH,
        /** The node rejected the transaction outright, before it reached the pool. */
        DEFINITE_REJECT,
        /** A transient error (transport, pool-full, …) — retry the same transaction. */
        TRANSIENT
    }

    /**
     * Drive one request to a definite on-chain verdict: preview, then sign once at a fixed
     * {@code latest} nonce and submit/poll in a loop, re-sending the same nonce until the node
     * returns a mined receipt, a definite pre-mempool rejection, or a confirmed nonce advance — or
     * until the {@code MAX_SUBMIT_ROUNDS} safety valve abandons it for the sync loop to re-drive.
     * Keeps at most one transaction per account in flight. Package-visible so tests can drive it
     * synchronously without the worker thread.
     *
     * @param request the request snapshot to process
     */
    void process(final SubmitRequest request) {
        final String connIdLabel = request.channelIdLabel();
        final String peerLabel = peerLabel(request.peer());
        final String contractAddress = request.contractAddress();
        final Bytes bundle = request.bundleBytes();

        // 1. ABI-encode the call. An encoding failure is definite — skip.
        final byte[] callData;
        try {
            callData = AbiCodec.encodeSubmitBundle(
                    request.channelId().toByteArray(), request.bundleBytes().toByteArray());
        } catch (final RuntimeException e) {
            txFailures.increment("channel_id", connIdLabel, "reason", "encode");
            LOGGER.warn("submitBundle: cannot encode call data for {}: {}", connIdLabel, e.getMessage());
            return;
        }
        final String callDataHex = AbiCodec.toHex(callData);

        // 2. Gas-free preview. A contract revert is a definite rejection (stale, replay, wrong
        //    status, throttled) — skip without spending gas. Transient RPC errors are retried.
        if (!previewPasses(contractAddress, callDataHex, connIdLabel, peerLabel, bundle)) {
            return;
        }

        // 3. Resolve the nonce ONCE from the mined (latest) count. Correct because we only reach here
        //    with none of our own transactions unconfirmed, so latest is exactly the next nonce.
        final Long resolvedNonce = readLatestNonce(connIdLabel);
        if (resolvedNonce == null) {
            return; // interrupted
        }
        final long nonce = resolvedNonce;

        // 4. Initial fee. A gas-cap breach is definite — skip; a transient RPC error is retried.
        Eip1559GasStrategy.Fees fees = computeInitialFees(connIdLabel);
        if (fees == null) {
            return;
        }

        // 4b. Size the transaction to its actual gas via eth_estimateGas (+ margin, capped), so the
        //     node's upfront reservation is proportional to real usage rather than a flat 28M.
        final long gasLimit = estimateGasLimit(contractAddress, callDataHex, connIdLabel);

        // 5. Sign at the fixed nonce and drive to a definite verdict: broadcast, poll, re-broadcast.
        //    Each unmined round re-reads the live market fee: if it exceeds what we last signed we
        //    re-price up to it and re-sign; otherwise we re-send the same tx (never bumping blindly,
        //    which with a large gas limit would inflate maxFee*gasLimit into the balance / node fee
        //    cap and strand it). The fee is also raised when the node reports it under-priced. Both
        //    paths clamp to the cap. A request that never reaches a verdict is abandoned after
        //    MAX_SUBMIT_ROUNDS so it cannot wedge the worker.
        final long cap = gasStrategy.maxGasPriceCap();
        final List<String> sentHashes = new ArrayList<>();
        SignedTx signed = signSubmit(nonce, fees, gasLimit, contractAddress, callData);
        String rawTxHex = signed.rawHex();
        String txHash = signed.hash();
        sentHashes.add(txHash);
        String affordableRawHex = null; // the last raw tx the node accepted into its pool
        String affordableTxHash = null;
        boolean nonceConsumed = false; // a "nonce too low" told us this slot is already used on-chain
        boolean feeCeilingReached = false; // node/balance won't accept a higher fee — stop bumping
        int round = 0;

        while (!Thread.currentThread().isInterrupted()) {
            round++;
            if (round > MAX_SUBMIT_ROUNDS) {
                // Safety valve: give up so the single worker isn't wedged on one request forever.
                // The bundle isn't lost — the channel's sync loop re-drives it from fresh state.
                txFailures.increment("channel_id", connIdLabel, "reason", "abandoned");
                LOGGER.error(
                        "submitBundle: abandoning nonce {} for {} after {} rounds without a verdict; sync loop will re-drive",
                        nonce,
                        connIdLabel,
                        MAX_SUBMIT_ROUNDS);
                return;
            }

            boolean pooled = false;
            try {
                rpcClient.ethSendRawTransaction(rawTxHex);
                pooled = true;
                if (round == 1) {
                    LOGGER.info("submitBundle from={} to={} txHash={}", fromAddress, contractAddress, txHash);
                }
            } catch (final JsonRpcException e) {
                switch (classifySend(e)) {
                    case ALREADY_KNOWN -> pooled = true;
                    case NONCE_ALREADY_MINED -> {
                        // Our nonce is already used on-chain. Usually one of our sent versions mined
                        // (the poll below finds it); the nonce-advance check in the not-mined branch
                        // handles the case where it was some other tx at this nonce.
                        pooled = true;
                        nonceConsumed = true;
                    }
                    case UNDERPRICED -> {
                        // Base fee rose above our maxFee: raise the fee once (clamped) and re-sign.
                        final Eip1559GasStrategy.Fees bumped = bump(fees, cap);
                        if (bumped.equals(fees)) {
                            feeCeilingReached = true;
                        } else {
                            fees = bumped;
                            signed = signSubmit(nonce, fees, gasLimit, contractAddress, callData);
                            rawTxHex = signed.rawHex();
                            txHash = signed.hash();
                            if (!sentHashes.contains(txHash)) {
                                sentHashes.add(txHash);
                            }
                        }
                        if (!sleepQuietly(EvmSubmissionConfig.SUBMIT_RETRY_SLEEP_MS)) {
                            return;
                        }
                        continue;
                    }
                    case FEE_TOO_HIGH -> {
                        // Our fee exceeds the node's per-tx cap or the balance can't reserve it. Stop
                        // raising it and fall back to the last version the node accepted, if any.
                        feeCeilingReached = true;
                        if (affordableRawHex != null && !affordableRawHex.equals(rawTxHex)) {
                            rawTxHex = affordableRawHex;
                            txHash = affordableTxHash;
                            if (!sleepQuietly(EvmSubmissionConfig.SUBMIT_RETRY_SLEEP_MS)) {
                                return;
                            }
                            continue;
                        }
                        // Nothing affordable was ever pooled — we cannot land this transaction at all.
                        txFailures.increment("channel_id", connIdLabel, "reason", "send");
                        LOGGER.warn(
                                "submitBundle: fee too high / insufficient funds, nothing affordable pooled for {}: {}",
                                connIdLabel,
                                e.getMessage());
                        return;
                    }
                    case DEFINITE_REJECT -> {
                        txFailures.increment("channel_id", connIdLabel, "reason", "send");
                        LOGGER.warn("submitBundle: definite rejection for {}: {}", connIdLabel, e.getMessage());
                        return;
                    }
                    case TRANSIENT -> {
                        if (round >= RESEND_WARN_AFTER_ROUNDS) {
                            LOGGER.warn(
                                    "submitBundle: transient send error for {} (round {}), retrying: {}",
                                    connIdLabel,
                                    round,
                                    e.getMessage());
                        }
                        if (!sleepQuietly(EvmSubmissionConfig.SUBMIT_RETRY_SLEEP_MS)) {
                            return;
                        }
                        continue;
                    }
                }
            }
            if (pooled) {
                affordableRawHex = rawTxHex;
                affordableTxHash = txHash;
            }

            final Outcome outcome = pollForReceipt(sentHashes, contractAddress, callDataHex, bundle);
            if (outcome == Outcome.SUCCESS) {
                bundleSubmissions.increment("channel_id", connIdLabel, "peer", peerLabel);
                bundleMessagesSubmitted.add(request.messageCount(), "channel_id", connIdLabel);
                bundleBytesSubmitted.add(request.bundleBytes().length(), "channel_id", connIdLabel);
                txSubmissions.increment("channel_id", connIdLabel);
                return;
            }
            if (outcome == Outcome.REVERTED) {
                bundleReverted.increment("channel_id", connIdLabel, "peer", peerLabel);
                txReverts.increment("channel_id", connIdLabel);
                return;
            }

            // Not mined this round. If the node told us the nonce is already used but none of the
            // versions WE sent has a receipt (a narrowly-missed timeout + reorg, or another actor
            // signing this key), re-read the on-chain nonce: once it has advanced past ours the slot
            // is definitively consumed, so stop and let the sync loop reconcile — otherwise we would
            // re-send into an endless "nonce too low".
            if (nonceConsumed) {
                final Long latest = readLatestNonce(connIdLabel);
                if (latest == null) {
                    return; // interrupted
                }
                if (latest > nonce) {
                    LOGGER.warn(
                            "submitBundle: nonce {} for {} already consumed on-chain (not by a tx we tracked); treating as done",
                            nonce,
                            connIdLabel);
                    return;
                }
            }

            if (round >= RESEND_WARN_AFTER_ROUNDS) {
                LOGGER.warn(
                        "submitBundle: nonce {} for {} still unmined after {} round(s); re-sending (txHash {})",
                        nonce,
                        connIdLabel,
                        round,
                        txHash);
            }

            // A pooled tx can become unmineable if the base fee rose after it was accepted (re-sends
            // then return "already known", never "underpriced"). Rather than bump blindly, re-read the
            // live market fee each round: if it now wants more than we last signed, re-price up to it
            // (clamped to the cap) and re-sign at the same nonce; if it is the same or lower, re-send
            // the current tx unchanged. A node that rejects the re-priced replacement as under-priced
            // (a sub-10% delta over the pooled tx) is handled by the UNDERPRICED branch above, which
            // bumps over our own last fee to clear the replacement rule.
            if (!feeCeilingReached) {
                final Eip1559GasStrategy.Fees latest = latestFeesCapped(connIdLabel);
                if (latest != null && latest.maxFeePerGas() > fees.maxFeePerGas()) {
                    fees = latest;
                    signed = signSubmit(nonce, fees, gasLimit, contractAddress, callData);
                    rawTxHex = signed.rawHex();
                    txHash = signed.hash();
                    if (!sentHashes.contains(txHash)) {
                        sentHashes.add(txHash);
                    }
                }
            }
        }
    }

    /** A signed transaction's raw hex and its keccak256 hash. */
    private record SignedTx(String rawHex, String hash) {}

    /** Sign the {@code submitBundle} transaction at the fixed nonce and fees; returns its hex + hash. */
    private SignedTx signSubmit(
            final long nonce,
            final Eip1559GasStrategy.Fees fees,
            final long gasLimit,
            final String contractAddress,
            final byte[] callData) {
        final byte[] rawTx = signer.signEip1559Transaction(
                nonce,
                fees.maxPriorityFeePerGas(),
                fees.maxFeePerGas(),
                gasLimit,
                contractAddress,
                0L,
                callData,
                chainId);
        return new SignedTx(
                AbiCodec.toHex(rawTx), "0x" + HexFormat.of().formatHex(AbiCodec.Keccak256.keccak256(rawTx)));
    }

    /**
     * Preview the call at {@code pending}. Returns {@code true} if it would succeed; on a contract
     * revert it records the skip metric and returns {@code false}; transient RPC errors are retried.
     */
    private boolean previewPasses(
            final String contractAddress,
            final String callDataHex,
            final String connIdLabel,
            final String peerLabel,
            final Bytes bundle) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                rpcClient.ethCallFrom(fromAddress, contractAddress, callDataHex, "pending");
                return true;
            } catch (final JsonRpcException e) {
                if (isTransientRpc(e)) {
                    if (!sleepQuietly(EvmSubmissionConfig.SUBMIT_RETRY_SLEEP_MS)) {
                        return false;
                    }
                    continue;
                }
                EvmErrorParser.parse(e)
                        .log(LOGGER, "bundle PREVIEW reverted (skipping submission)", BundleLog.tag(bundle));
                bundleSkipped.increment("channel_id", connIdLabel, "peer", peerLabel, "reason", "rejected");
                return false;
            } catch (final RuntimeException e) {
                LOGGER.error("bundle PREVIEW failed unexpectedly for {} {}", e, connIdLabel, BundleLog.tag(bundle));
                if (!sleepQuietly(EvmSubmissionConfig.SUBMIT_RETRY_SLEEP_MS)) {
                    return false;
                }
            }
        }
        return false;
    }

    /** Read the account's {@code latest} nonce, retrying transient RPC errors. */
    @Nullable
    private Long readLatestNonce(final String connIdLabel) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                return rpcClient.ethGetTransactionCount(fromAddress, "latest");
            } catch (final RuntimeException e) {
                LOGGER.warn("submitBundle: nonce read failed for {}, retrying: {}", connIdLabel, e.getMessage());
                if (!sleepQuietly(EvmSubmissionConfig.SUBMIT_RETRY_SLEEP_MS)) {
                    return null;
                }
            }
        }
        return null;
    }

    /**
     * Compute the initial fee. Returns {@code null} (after a failure metric) on a definite gas-cap
     * breach; retries transient RPC errors.
     */
    private Eip1559GasStrategy.@Nullable Fees computeInitialFees(final String connIdLabel) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                return gasStrategy.computeFees();
            } catch (final JsonRpcException e) {
                if (isGasCapBreach(e)) {
                    txFailures.increment("channel_id", connIdLabel, "reason", "gas");
                    LOGGER.warn("submitBundle: fee computation failed for {}: {}", connIdLabel, e.getMessage());
                    return null;
                }
                LOGGER.warn("submitBundle: base-fee read failed for {}, retrying: {}", connIdLabel, e.getMessage());
                if (!sleepQuietly(EvmSubmissionConfig.SUBMIT_RETRY_SLEEP_MS)) {
                    return null;
                }
            }
        }
        return null;
    }

    /**
     * Read the live market fee for a per-round re-price decision, clamped to the gas-price cap (never
     * throwing on a cap breach — the caller already holds a valid pooled tx). Returns {@code null} on
     * any RPC error so the caller simply re-sends the current transaction this round and re-checks the
     * fee next round rather than blocking the worker on a transient fault.
     */
    private Eip1559GasStrategy.@Nullable Fees latestFeesCapped(final String connIdLabel) {
        try {
            return gasStrategy.computeFeesCapped();
        } catch (final RuntimeException e) {
            LOGGER.trace("submitBundle: live fee re-read failed for {}, keeping current fee: {}", e, connIdLabel);
            return null;
        }
    }

    /**
     * Size the transaction via {@code eth_estimateGas} plus a {@value #GAS_ESTIMATE_MARGIN_NUMERATOR}/
     * {@value #GAS_ESTIMATE_MARGIN_DENOMINATOR} margin, capped at {@link #DEFAULT_GAS_LIMIT}. Any
     * failure falls back to the default limit: the preview has already gated reverts, so this call
     * should succeed; a transient RPC fault only costs a larger (but safe) reservation this round.
     */
    private long estimateGasLimit(final String contractAddress, final String callDataHex, final String connIdLabel) {
        try {
            final long estimated = rpcClient.ethEstimateGas(fromAddress, contractAddress, callDataHex);
            final long withMargin = estimated <= Long.MAX_VALUE / GAS_ESTIMATE_MARGIN_NUMERATOR
                    ? estimated * GAS_ESTIMATE_MARGIN_NUMERATOR / GAS_ESTIMATE_MARGIN_DENOMINATOR
                    : DEFAULT_GAS_LIMIT;
            return Math.min(withMargin, DEFAULT_GAS_LIMIT);
        } catch (final RuntimeException e) {
            LOGGER.debug(
                    "submitBundle: gas estimate failed for {}, using default limit {}: {}",
                    connIdLabel,
                    DEFAULT_GAS_LIMIT,
                    e.getMessage());
            return DEFAULT_GAS_LIMIT;
        }
    }

    /**
     * Poll for a receipt on any of the transaction hashes broadcast at this nonce (a fee bump changes
     * the hash, so several versions may momentarily coexist; the node keeps only the highest-fee one).
     */
    private Outcome pollForReceipt(
            final List<String> sentHashes, final String contractAddress, final String callDataHex, final Bytes bundle) {
        for (int i = 0; i < EvmSubmissionConfig.RECEIPT_POLL_ATTEMPTS; i++) {
            for (final String hash : sentHashes) {
                final JsonNode receipt;
                try {
                    receipt = rpcClient.ethGetTransactionReceipt(hash);
                } catch (final RuntimeException e) {
                    continue; // transient read error — try the next hash / iteration
                }
                if (receipt != null && !receipt.isNull()) {
                    final JsonNode status = receipt.get("status");
                    final String statusStr = status != null ? status.asText() : null;
                    if ("0x1".equals(statusStr)) {
                        LOGGER.info("submitBundle tx {} succeeded", hash);
                        return Outcome.SUCCESS;
                    }
                    JsonRpcException revertError = null;
                    try {
                        rpcClient.ethCallFrom(fromAddress, contractAddress, callDataHex, "latest");
                    } catch (final JsonRpcException ex) {
                        revertError = ex;
                    } catch (final RuntimeException ex) {
                        // diagnostic-only; ignore
                    }
                    EvmErrorParser.parse(revertError)
                            .log(
                                    LOGGER,
                                    "submitBundle tx %s REVERTED (status=%s)".formatted(hash, statusStr),
                                    BundleLog.tag(bundle));
                    return Outcome.REVERTED;
                }
            }
            if (!sleepQuietly(EvmSubmissionConfig.RECEIPT_POLL_SLEEP_MS)) {
                return Outcome.NONE;
            }
        }
        return Outcome.NONE;
    }

    /** Classify a send error by the node's message (Besu returns {@code -32000} for most of these). */
    private static SendClass classifySend(final JsonRpcException e) {
        final String m = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
        if (m.contains("known transaction") || m.contains("already known") || m.contains("already imported")) {
            return SendClass.ALREADY_KNOWN;
        }
        if (m.contains("nonce too low") || m.contains("nonce below") || m.contains("old nonce")) {
            return SendClass.NONCE_ALREADY_MINED;
        }
        if (m.contains("underpriced") || m.contains("gas price too low to replace")) {
            return SendClass.UNDERPRICED;
        }
        if (m.contains("fee cap exceeded")
                || m.contains("feecap")
                || m.contains("tx fee cap")
                || m.contains("exceeds the configured cap")
                || m.contains("insufficient funds")
                || m.contains("upfront cost")
                || m.contains("exceeds account balance")) {
            return SendClass.FEE_TOO_HIGH;
        }
        if (m.contains("intrinsic gas")
                || m.contains("exceeds block gas limit")
                || m.contains("exceeds the block gas limit")
                || m.contains("gas limit too high")) {
            return SendClass.DEFINITE_REJECT;
        }
        // Transport, pool-full, nonce-too-distant, or anything unrecognised: retry, never skip.
        return SendClass.TRANSIENT;
    }

    /** True if the exception is a transport/protocol error from our own RPC client, not a revert. */
    private static boolean isTransientRpc(final JsonRpcException e) {
        final String m = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
        return m.contains("retries")
                || m.contains("channel failed")
                || m.contains("request interrupted")
                || m.contains("http ")
                || m.contains("invalid json")
                || m.contains("neither 'result'");
    }

    /** True if the fee-computation error is a definite gas-price-cap breach (not a transient RPC fault). */
    private static boolean isGasCapBreach(final JsonRpcException e) {
        final String m = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
        return m.contains("gas price cap exceeded");
    }

    /** Bump both fee fields by {@code FEE_BUMP} (25%), clamped to {@code cap}; preserves priority ≤ maxFee. */
    private static Eip1559GasStrategy.Fees bump(final Eip1559GasStrategy.Fees fees, final long cap) {
        final long maxFee = bumpValue(fees.maxFeePerGas(), cap);
        final long priority = Math.min(bumpValue(fees.maxPriorityFeePerGas(), cap), maxFee);
        return new Eip1559GasStrategy.Fees(priority, maxFee);
    }

    private static long bumpValue(final long value, final long cap) {
        if (value >= cap) {
            return cap;
        }
        final long bumped =
                value <= Long.MAX_VALUE / FEE_BUMP_NUMERATOR ? value * FEE_BUMP_NUMERATOR / FEE_BUMP_DENOMINATOR : cap;
        return Math.min(bumped, cap);
    }

    /** Sleep via the configured {@link Sleeper}; returns {@code false} if interrupted. */
    private boolean sleepQuietly(final long ms) {
        try {
            sleeper.sleep(ms);
            return true;
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
