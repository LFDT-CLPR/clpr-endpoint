// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprEndpoint;
import com.hedera.hapi.node.state.clpr.ClprEndpointManifest;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprMessageValue;
import com.hedera.hapi.node.state.clpr.ClprServiceEndpoint;
import com.hedera.hapi.node.state.clpr.ClprThrottles;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.logging.api.Logger;
import com.swirlds.logging.api.Loggers;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.metrics.LabeledCounter;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.jspecify.annotations.Nullable;

/**
 * EVM-backed implementation of {@link ContractStateReader}.
 *
 * <p>Reads CLPR Service contract state via {@code eth_call} JSON-RPC. Calls are dispatched
 * at the block tag corresponding to the requested {@link CommitmentLevel}.
 */
public class EvmContractStateReader implements ContractStateReader {

    private static final Logger LOGGER = Loggers.getLogger(EvmContractStateReader.class);

    /**
     * ABI selector for the Solidity error {@code ClprChannelNotFound()} =
     * {@code keccak256("ClprChannelNotFound()")[:4]}. When {@code getChannel} reverts with
     * this selector the chain has no Channel record for the id (PENDING or unregistered), which
     * {@link #readChannelState} maps to {@link Optional#empty()}.
     */
    private static final String CLPR_CHANNEL_NOT_FOUND_SELECTOR = "0x029a1002";

    private final EvmJsonRpcClient rpcClient;
    private final String contractAddress;

    /** Counts endpoint-manifest read failures, labeled {@code scope=local|peer, reason=rpc_error|decode_error}. */
    @Nullable
    private final LabeledCounter readFailures;

    /** WARN throttle for read failures (metric always increments); avoids flooding the hot poll path. */
    private static final long READ_FAILURE_WARN_THROTTLE_NANOS = 30_000_000_000L;

    private final AtomicLong lastReadFailureWarnNanos = new AtomicLong(Long.MIN_VALUE);

    /**
     * Create a new {@code EvmContractStateReader}.
     *
     * @param rpcClient       the JSON-RPC client used to communicate with the EVM node
     * @param contractAddress the hex address of the deployed CLPR Service contract
     */
    public EvmContractStateReader(final EvmJsonRpcClient rpcClient, final String contractAddress) {
        this(rpcClient, contractAddress, null);
    }

    /**
     * As {@link #EvmContractStateReader(EvmJsonRpcClient, String)}, additionally emitting the
     * {@code evm.manifest.read.failed} counter on endpoint-manifest read failures.
     *
     * @param rpcClient       the JSON-RPC client used to communicate with the EVM node
     * @param contractAddress the hex address of the deployed CLPR Service contract
     * @param readFailures    counter incremented on manifest read failure (nullable = no metric)
     */
    public EvmContractStateReader(
            final EvmJsonRpcClient rpcClient,
            final String contractAddress,
            @Nullable final LabeledCounter readFailures) {
        this.rpcClient = rpcClient;
        this.contractAddress = contractAddress;
        this.readFailures = readFailures;
    }

    /**
     * Records an endpoint-manifest read failure: always increments {@code evm.manifest.read.failed}
     * (labeled {@code scope}/{@code reason}) so a persistent failure is observable, plus a WARN
     * throttled to at most once per {@value #READ_FAILURE_WARN_THROTTLE_NANOS} ns so the hot
     * poll/bundle-construction path can't flood the log.
     */
    private void recordManifestReadFailure(
            final String scope, final String reason, final String message, final Object... args) {
        if (readFailures != null) {
            readFailures.increment("scope", scope, "reason", reason);
        }
        final long now = System.nanoTime();
        final long last = lastReadFailureWarnNanos.get();
        if (now - last >= READ_FAILURE_WARN_THROTTLE_NANOS && lastReadFailureWarnNanos.compareAndSet(last, now)) {
            LOGGER.warn(message, args);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Calls {@code getChannel(bytes32)} on the CLPR Service contract and decodes
     * the returned {@code Channel} struct. Populates all fields needed by the sync loop
     * and peer discovery: {@code channelId}, {@code serviceAddress}, {@code status},
     * {@code nextMessageId}, {@code ackedMessageId}, {@code receivedMessageId},
     * {@code sentRunningHash}, {@code receivedRunningHash}, {@code trustAnchor},
     * {@code trustAnchorId}. ({@code remoteTrustAnchorId} was removed from the on-chain struct and
     * is left empty — see the slot map below.)
     */
    @Override
    public Optional<ClprChannel> readChannelState(final Bytes channelId, final String blockTag) {
        final byte[] connIdBytes = toBytes32(channelId);
        final byte[] callData = AbiCodec.encodeGetChannel(connIdBytes);

        final byte[] result;
        try {
            final String hex = rpcClient.ethCall(contractAddress, AbiCodec.toHex(callData), blockTag);
            result = AbiCodec.fromHex(hex);
        } catch (final JsonRpcException e) {
            final String message = e.getMessage();
            if (message != null && message.contains(CLPR_CHANNEL_NOT_FOUND_SELECTOR)) {
                // Routine lifecycle state: PENDING (commitment-only) or never registered.
                // DEBUG so the per-poll listener loop doesn't flood logs.
                LOGGER.debug("getChannel: no Channel record yet for {} (PENDING or unregistered)", channelId);
                return Optional.empty();
            }
            // Any other JSON-RPC failure is a real error (wrong contract, RPC down, ABI mismatch).
            LOGGER.warn("getChannel eth_call failed: {}", message);
            throw e;
        }

        // Minimum: 1 outer-offset word + 27 head slots (0..26, incl. endpointManifestVersion@26)
        // = 28 × 32 = 896 bytes.
        if (result.length < 896) {
            // Empty / malformed return — treat as "no record".
            return Optional.empty();
        }

        // The return value is a dynamic struct (contains string / bytes), so the ABI
        // encoding is wrapped with an offset pointer at word 0 pointing at the struct head.
        // In practice this offset is always 0x20, but we read it dynamically for safety.
        // decodeOffsetOrLength rejects an offset that would not fit a non-negative int (a wrapped or
        // out-of-range uint256), so the head-slot reads below can't index off a silently-truncated base.
        final int base;
        try {
            base = AbiCodec.decodeOffsetOrLength(result, 0);
        } catch (final RuntimeException e) {
            LOGGER.warn("getChannel returned an unreadable struct offset: {}", e.getMessage());
            return Optional.empty();
        }
        // The 27-slot head (0..26) must lie within the return buffer before any slot read; long math so
        // a near-MAX_INT base can't wrap the bound.
        if ((long) base + 27 * 32 > result.length) {
            return Optional.empty();
        }

        // Channel tuple head layout (each slot is 32 bytes, offsets from {@code base}):
        //   0:  bytes32 channelId
        //   1:  address verifier               (left-padded to 32 bytes)
        //   2:  uint8 status                   (enum ChannelStatus)
        //   3:  uint64 nextMessageId
        //   4:  uint64 ackedMessageId
        //   5:  uint64 receivedMessageId
        //   6:  uint64 nextExpectedReplyId
        //   7:  uint96 peerConfigTimestamp
        //   8:  uint96 lastConfigTimestamp
        //   9:  bytes32 sentRunningHash
        //   10: bytes32 receivedRunningHash
        //   11: bytes32 ownershipCommitment
        //   12: bytes32 salt
        //   13: string chainId                  (offset to tail, relative to base)
        //   14: bytes peerServiceAddress        (offset to tail, relative to base)
        //   15: uint64 Throttles.maxMessagesPerBundle   (inlined)
        //   16: uint64 Throttles.maxMessagePayloadBytes  (inlined)
        //   17: uint64 Throttles.maxGasPerMessage       (inlined)
        //   18: uint64 Throttles.maxQueueDepth          (inlined)
        //   19: uint64 Throttles.maxSyncBytes           (inlined)
        //   20: uint32 Throttles.maxLocalEndpoints      (inlined)
        //   21: uint32 Throttles.maxPeerEndpoints       (inlined)
        //   22: bytes trustAnchor               (offset to tail, relative to base)
        //   23: uint64 lastDataMessageId        (inline; not modelled by ClprChannel — skipped)
        //   24: bytes trustAnchorId             (offset to tail, relative to base)
        //   25: bytes channelContext         (offset to tail; not modelled by ClprChannel — skipped)
        //   26: uint64 endpointManifestVersion
        final byte[] connIdSlot = AbiCodec.decodeBytes32(result, base);
        final long statusOrd = AbiCodec.decodeUint64(result, base + 2 * 32);
        final long nextMessageId = AbiCodec.decodeUint64(result, base + 3 * 32);
        final long ackedMessageId = AbiCodec.decodeUint64(result, base + 4 * 32);
        final long receivedMessageId = AbiCodec.decodeUint64(result, base + 5 * 32);
        final byte[] sentRunningHash = AbiCodec.decodeBytes32(result, base + 9 * 32);
        final byte[] receivedRunningHash = AbiCodec.decodeBytes32(result, base + 10 * 32);

        final ClprChannelStatus status = ClprChannelStatus.fromProtobufOrdinal((int) statusOrd);

        // Decode serviceAddress (slot 14, dynamic bytes — offset is relative to base).
        Bytes serviceAddress = Bytes.EMPTY;
        try {
            final int saRelOffset = AbiCodec.decodeOffsetOrLength(result, base + 14 * 32);
            final int saAbsPos = base + saRelOffset;
            final int saLen = AbiCodec.decodeOffsetOrLength(result, saAbsPos);
            if (saLen > 0 && (long) saAbsPos + 32 + saLen <= result.length) {
                serviceAddress = Bytes.wrap(Arrays.copyOfRange(result, saAbsPos + 32, saAbsPos + 32 + saLen));
            }
        } catch (final Exception e) {
            LOGGER.warn("Failed to decode serviceAddress from channel record: {}", e.getMessage());
        }

        // Decode chainId (slot 13, dynamic UTF-8 string — offset relative to base). The peer chain's
        // CAIP-2 id (e.g. "eip155:31338", "hiero:localnet"); used by channel discovery to tell EVM
        // (eip155:) peers from non-EVM peers sharing the same ClprService.
        String chainId = "";
        try {
            final int ciRelOffset = AbiCodec.decodeOffsetOrLength(result, base + 13 * 32);
            final int ciAbsPos = base + ciRelOffset;
            final int ciLen = AbiCodec.decodeOffsetOrLength(result, ciAbsPos);
            if (ciLen > 0 && (long) ciAbsPos + 32 + ciLen <= result.length) {
                chainId = new String(
                        Arrays.copyOfRange(result, ciAbsPos + 32, ciAbsPos + 32 + ciLen),
                        java.nio.charset.StandardCharsets.UTF_8);
            }
        } catch (final Exception e) {
            LOGGER.warn("Failed to decode chainId from channel record: {}", e.getMessage());
        }

        // Decode peerThrottles from the inline Throttles block (slots 15–21).
        final ClprThrottles peerThrottles = ClprThrottles.newBuilder()
                .maxMessagesPerBundle((int) AbiCodec.decodeUint64(result, base + 15 * 32))
                .maxMessagePayloadBytes((int) AbiCodec.decodeUint64(result, base + 16 * 32))
                .maxGasPerMessage(AbiCodec.decodeUint64(result, base + 17 * 32))
                .maxQueueDepth((int) AbiCodec.decodeUint64(result, base + 18 * 32))
                .maxSyncBytes(AbiCodec.decodeUint64(result, base + 19 * 32))
                .maxLocalEndpoints((int) AbiCodec.decodeUint64(result, base + 20 * 32))
                .maxPeerEndpoints((int) AbiCodec.decodeUint64(result, base + 21 * 32))
                .build();

        // Decode trustAnchor (slot 22, dynamic bytes — offset relative to base).
        Bytes trustAnchor = Bytes.EMPTY;
        try {
            final int taRelOffset = AbiCodec.decodeOffsetOrLength(result, base + 22 * 32);
            final int taAbsPos = base + taRelOffset;
            final int taLen = AbiCodec.decodeOffsetOrLength(result, taAbsPos);
            if (taLen > 0 && (long) taAbsPos + 32 + taLen <= result.length) {
                trustAnchor = Bytes.wrap(Arrays.copyOfRange(result, taAbsPos + 32, taAbsPos + 32 + taLen));
            }
        } catch (final Exception e) {
            LOGGER.warn("Failed to decode trustAnchor from channel record: {}", e.getMessage());
        }

        // Slot 7: peerConfigTimestamp (uint96 inline) — nanosSinceEpoch = seconds×1e9 + nanos. The
        // monotonic marker the Service advances when it applies an inbound peer config change and
        // replaces the peer endpoint roster (spec §2.4.2).
        Timestamp peerConfigTimestamp = null;
        final BigInteger peerConfigNanos = AbiCodec.decodeUint96(result, base + 7 * 32);
        if (!peerConfigNanos.equals(BigInteger.ZERO)) {
            peerConfigTimestamp = Timestamp.newBuilder()
                    .seconds(peerConfigNanos
                            .divide(BigInteger.valueOf(1_000_000_000L))
                            .longValue())
                    .nanos(peerConfigNanos
                            .mod(BigInteger.valueOf(1_000_000_000L))
                            .intValue())
                    .build();
        }

        // Decode trustAnchorId (slot 24, dynamic bytes — offset relative to base).
        Bytes trustAnchorId = Bytes.EMPTY;
        try {
            final int taiRelOffset = AbiCodec.decodeOffsetOrLength(result, base + 24 * 32);
            final int taiAbsPos = base + taiRelOffset;
            final int taiLen = AbiCodec.decodeOffsetOrLength(result, taiAbsPos);
            if (taiLen > 0 && (long) taiAbsPos + 32 + taiLen <= result.length) {
                trustAnchorId = Bytes.wrap(Arrays.copyOfRange(result, taiAbsPos + 32, taiAbsPos + 32 + taiLen));
            }
        } catch (final Exception e) {
            LOGGER.warn("Failed to decode trustAnchorId from channel record: {}", e.getMessage());
        }

        // Slot 26: endpointManifestVersion (uint64 inline).
        long endpointManifestVersion = 0L;
        try {
            endpointManifestVersion = AbiCodec.decodeUint64(result, base + 26 * 32);
        } catch (final Exception e) {
            LOGGER.warn("Failed to decode endpointManifestVersion from channel record: {}", e.getMessage());
        }

        return Optional.of(ClprChannel.newBuilder()
                .channelId(Bytes.wrap(connIdSlot))
                .chainId(chainId)
                .serviceAddress(serviceAddress)
                .status(status)
                .nextMessageId(nextMessageId)
                .ackedMessageId(ackedMessageId)
                .sentRunningHash(Bytes.wrap(sentRunningHash))
                .receivedMessageId(receivedMessageId)
                .receivedRunningHash(Bytes.wrap(receivedRunningHash))
                .peerConfigTimestamp(peerConfigTimestamp)
                .peerThrottles(peerThrottles)
                .trustAnchor(trustAnchor)
                .trustAnchorId(trustAnchorId)
                .endpointManifestVersion(endpointManifestVersion)
                .build());
    }

    /**
     * {@inheritDoc}
     *
     * <p>For each {@code messageId} in the half-open interval {@code [fromId, toId)}, calls
     * {@code getMessage(bytes32,uint64)} and decodes the returned
     * {@code MessageValue { bytes payload; bytes32 runningHashAfterProcessing; }} tuple
     * into a {@link ClprMessageValue}. Messages that fail to decode are skipped with a
     * warning log; they will be retried on the next sync tick.
     */
    @Override
    public List<QueuedMessage> readQueuedMessages(
            final Bytes channelId, final long fromId, final long toId, final String blockTag) {
        if (toId <= fromId) {
            return List.of();
        }
        final byte[] connIdBytes = toBytes32(channelId);
        final List<QueuedMessage> out = new ArrayList<>((int) (toId - fromId));

        for (long id = fromId; id < toId; id++) {
            final byte[] callData = AbiCodec.encodeGetMessage(connIdBytes, id);
            try {
                final String hex = rpcClient.ethCall(contractAddress, AbiCodec.toHex(callData), blockTag);
                final byte[] result = AbiCodec.fromHex(hex);
                if (result.length < 96) {
                    continue;
                }
                // Return is a dynamic struct wrapped with a 32-byte offset pointer.
                // decodeOffsetOrLength rejects a wrapped/out-of-range offset (RuntimeException →
                // caught below → message skipped) rather than indexing off a truncated base.
                final int base = AbiCodec.decodeOffsetOrLength(result, 0);
                // Struct head: [offset payload][bytes32 runningHash]
                //   Slot 0 (base+0):  offset to payload bytes (relative to base)
                //   Slot 1 (base+32): runningHashAfterProcessing (bytes32, inline)
                final byte[] runningHash = AbiCodec.decodeBytes32(result, base + 32);

                // Decode the payload bytes: read the offset, then the length-prefixed data.
                ClprMessagePayload messagePayload = ClprMessagePayload.DEFAULT;
                final int payloadRelOffset = AbiCodec.decodeOffsetOrLength(result, base);
                final int payloadAbsBase = base + payloadRelOffset;
                if ((long) payloadAbsBase + 32 <= result.length) {
                    final int payloadLen = AbiCodec.decodeOffsetOrLength(result, payloadAbsBase);
                    if (payloadLen > 0 && (long) payloadAbsBase + 32 + payloadLen <= result.length) {
                        final byte[] rawPayload =
                                Arrays.copyOfRange(result, payloadAbsBase + 32, payloadAbsBase + 32 + payloadLen);
                        try {
                            messagePayload = ClprMessagePayload.PROTOBUF.parse(Bytes.wrap(rawPayload));
                        } catch (final ParseException e) {
                            LOGGER.warn("getMessage({}) payload parse failed: {}", id, e.getMessage());
                        }
                    }
                }

                out.add(new QueuedMessage(
                        BigInteger.valueOf(id),
                        ClprMessageValue.newBuilder()
                                .payload(messagePayload)
                                .runningHashAfterProcessing(Bytes.wrap(runningHash))
                                .build()));
            } catch (final JsonRpcException e) {
                LOGGER.warn("getMessage({}) failed: {}", id, e.getMessage());
            } catch (final RuntimeException e) {
                LOGGER.warn("getMessage({}) decode failed: {}", id, e.getMessage());
            }
        }
        return out;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Calls {@code getLedgerConfiguration()} on the local CLPR Service contract and decodes
     * the returned struct. The endpoint manifest is now maintained separately; use
     * {@link #readEndpointManifest(String)} to obtain the current endpoint set.
     */
    @Override
    public ClprLedgerConfiguration readLedgerConfiguration(final CommitmentLevel commitmentLevel) {
        return readLedgerConfiguration(commitmentLevel.toBlockTag());
    }

    @Override
    public ClprLedgerConfiguration readLedgerConfiguration(final String blockTag) {
        final String callDataHex = AbiCodec.toHex(AbiCodec.encodeGetLedgerConfiguration());
        try {
            final String hex = rpcClient.ethCall(contractAddress, callDataHex, blockTag);
            return decodeLedgerConfiguration(AbiCodec.fromHex(hex));
        } catch (final JsonRpcException e) {
            LOGGER.warn("getLedgerConfiguration eth_call failed: {}", e.getMessage());
            return ClprLedgerConfiguration.DEFAULT;
        } catch (final Exception e) {
            LOGGER.warn("getLedgerConfiguration decode failed: {}", e.getMessage());
            return ClprLedgerConfiguration.DEFAULT;
        }
    }

    public ClprEndpointManifest readEndpointManifest(final String blockTag) {
        final String callDataHex = AbiCodec.toHex(AbiCodec.encodeGetEndpointManifest());
        try {
            final String hex = rpcClient.ethCall(contractAddress, callDataHex, blockTag);
            return decodeEndpointManifest(AbiCodec.fromHex(hex));
        } catch (final JsonRpcException e) {
            recordManifestReadFailure("local", "rpc_error", "getEndpointManifest eth_call failed: {}", e.getMessage());
            return ClprEndpointManifest.DEFAULT;
        } catch (final Exception e) {
            recordManifestReadFailure("local", "decode_error", "getEndpointManifest decode failed: {}", e.getMessage());
            return ClprEndpointManifest.DEFAULT;
        }
    }

    @Override
    public ClprEndpointManifest readPeerEndpointManifest(final Bytes channelId, final CommitmentLevel commitmentLevel) {
        return readPeerEndpointManifest(channelId, commitmentLevel.toBlockTag());
    }

    /**
     * Reads the Channel's cached peer {@code ClprEndpointManifest} pinned to {@code blockTag}.
     *
     * <p>A clean read of an absent/empty manifest returns an empty (version 0) manifest — that is a
     * valid on-chain state. A read <em>failure</em> (RPC error or malformed return) is recorded on
     * {@code evm.manifest.read.failed} and then <b>thrown</b> rather than degraded to a sentinel:
     * the result drives a destructive peer-cache replace, so fabricating an empty manifest would
     * silently wipe the roster.
     *
     * @param channelId the CLPR channel identifier
     * @param blockTag the block to read against
     * @return the peer endpoint manifest at that block (possibly empty)
     * @throws IllegalStateException if the {@code eth_call} fails or the return cannot be decoded
     */
    public ClprEndpointManifest readPeerEndpointManifest(final Bytes channelId, final String blockTag) {
        final byte[] connIdBytes = toBytes32(channelId);
        final String callDataHex = AbiCodec.toHex(AbiCodec.encodeGetPeerEndpointManifest(connIdBytes));
        try {
            final String hex = rpcClient.ethCall(contractAddress, callDataHex, blockTag);
            return decodeEndpointManifest(AbiCodec.fromHex(hex));
        } catch (final Exception e) {
            // Unlike the local readEndpointManifest (whose failure just skips an optional outbound
            // proof), a failed PEER read must NOT degrade to the empty DEFAULT sentinel: both callers
            // feed the result into a destructive PeerEndpointCache/tlsRegistry *replace*, so a
            // transient RPC/decode failure would wipe an authoritative roster and stall sync until the
            // next manifest-version advance (which may never come). Record the failure metric, then
            // rethrow so the listener poll loop backs off and retries and channel bootstrap aborts
            // rather than registering a peer-less, inert handler — the same throw-don't-fabricate
            // contract readChannelState/readChannelCount already follow.
            final String reason = (e instanceof JsonRpcException) ? "rpc_error" : "decode_error";
            recordManifestReadFailure(
                    "peer", reason, "getPeerEndpointManifest failed for {}: {}", channelId, e.getMessage());
            throw new IllegalStateException(
                    "getPeerEndpointManifest read failed for channel " + channelId + " on " + contractAddress, e);
        }
    }

    /**
     * Read {@code channelCount()} from the CLPR Service contract — the number of completed
     * channels currently registered. Used by the discovery poller as a cheap change signal: an
     * increase triggers a {@code ChannelCompleted} log scan.
     *
     * <p>A transient RPC failure or a malformed return <b>throws</b> rather than returning a
     * sentinel: the caller uses the return value for change-detection ({@code count !=
     * lastChannelCount}), so a fabricated {@code 0} on failure would flap the count (e.g.
     * {@code 6 → 0 → 6}), trigger spurious full scans, and never let the discovery loop's backoff
     * engage. Letting it throw routes the failure into that loop's bounded retry instead.
     *
     * @param blockTag block tag, e.g. {@code "latest"} or {@code "finalized"}
     * @return the channel count
     * @throws JsonRpcException if the {@code eth_call} fails or reverts
     * @throws IllegalStateException if the return value is malformed (shorter than one 32-byte word)
     */
    public long readChannelCount(final String blockTag) {
        final String callDataHex = AbiCodec.toHex(AbiCodec.encodeChannelCount());
        final String hex = rpcClient.ethCall(contractAddress, callDataHex, blockTag);
        final byte[] result = AbiCodec.fromHex(hex);
        if (result.length < 32) {
            throw new IllegalStateException("channelCount() on " + contractAddress + " returned malformed data ("
                    + result.length + " bytes; expected >= 32)");
        }
        return AbiCodec.decodeUint64(result, 0);
    }

    /**
     * Decodes the ABI-encoded return value of {@code getLedgerConfiguration()}.
     *
     * <p>ABI return layout — word 0 is the outer offset pointer to the struct head (always
     * {@code 0x20} in practice). The struct head is 13 slots (each 32 bytes), then the
     * dynamic tails follow:
     *
     * <pre>
     *   word 0:     outer offset → struct head start (= base, always 0x20 = 32)
     *
     *   Struct head (offsets from base):
     *   base + 0×32   [slot  0]  protocolVersion (uint32, inline)
     *   base + 1×32   [slot  1]  offset to chainId string       (relative to base)
     *   base + 2×32   [slot  2]  offset to serviceAddress bytes  (relative to base)
     *   base + 3×32   [slot  3]  nanosSinceEpoch (uint96, inline) — seconds×1e9 + nanos
     *   base + 4×32   [slot  4]  Throttles.maxMessagesPerBundle   (uint64)
     *   base + 5×32   [slot  5]  Throttles.maxMessagePayloadBytes  (uint64)
     *   base + 6×32   [slot  6]  Throttles.maxGasPerMessage       (uint64)
     *   base + 7×32   [slot  7]  Throttles.maxQueueDepth          (uint64)
     *   base + 8×32   [slot  8]  Throttles.maxSyncBytes           (uint64)
     *   base + 9×32   [slot  9]  Throttles.maxLocalEndpoints      (uint32)
     *   base + 10×32  [slot 10]  Throttles.maxPeerEndpoints       (uint32)
     *   base + 11×32  [slot 11]  offset to trustAnchor bytes      (relative to base)
     *   base + 12×32  [slot 12]  offset to trustAnchorId bytes    (relative to base)
     * </pre>
     *
     * Dynamic tails follow the head (chainId, serviceAddress, trustAnchor, trustAnchorId).
     * All offsets stored in head slots are relative to {@code base}.
     */
    private ClprLedgerConfiguration decodeLedgerConfiguration(final byte[] result) {
        // Need at least: 1 outer-offset word + 13 struct-head slots = 14 × 32 = 448 bytes.
        if (result.length < 14 * 32) {
            return ClprLedgerConfiguration.DEFAULT;
        }
        // Outer offset (always 0x20 in practice, but read dynamically for safety). A wrapped/out-of-
        // range offset is rejected up front rather than indexing off a truncated base.
        final int base;
        try {
            base = AbiCodec.decodeOffsetOrLength(result, 0);
        } catch (final RuntimeException e) {
            LOGGER.warn("getLedgerConfiguration returned an unreadable struct offset: {}", e.getMessage());
            return ClprLedgerConfiguration.DEFAULT;
        }
        if ((long) base + 13 * 32 > result.length) {
            return ClprLedgerConfiguration.DEFAULT;
        }

        // Slot 0: protocolVersion (uint32 inline).
        final int protocolVersion = (int) AbiCodec.decodeUint64(result, base);

        // Slot 1: offset to chainId string (relative to base).
        String chainId = "";
        try {
            final int chainIdRelOffset = AbiCodec.decodeOffsetOrLength(result, base + 32);
            final int chainIdAbsBase = base + chainIdRelOffset;
            if ((long) chainIdAbsBase + 32 <= result.length) {
                final int chainIdLen = AbiCodec.decodeOffsetOrLength(result, chainIdAbsBase);
                if (chainIdLen > 0 && (long) chainIdAbsBase + 32 + chainIdLen <= result.length) {
                    chainId = new String(result, chainIdAbsBase + 32, chainIdLen, StandardCharsets.UTF_8);
                }
            }
        } catch (final Exception e) {
            LOGGER.warn("Failed to decode chainId from ledger configuration: {}", e.getMessage());
        }

        // Slot 2: offset to serviceAddress bytes (relative to base).
        Bytes serviceAddress = Bytes.EMPTY;
        try {
            final int saRelOffset = AbiCodec.decodeOffsetOrLength(result, base + 2 * 32);
            final int saAbsBase = base + saRelOffset;
            if ((long) saAbsBase + 32 <= result.length) {
                final int saLen = AbiCodec.decodeOffsetOrLength(result, saAbsBase);
                if (saLen > 0 && (long) saAbsBase + 32 + saLen <= result.length) {
                    serviceAddress = Bytes.wrap(Arrays.copyOfRange(result, saAbsBase + 32, saAbsBase + 32 + saLen));
                }
            }
        } catch (final Exception e) {
            LOGGER.warn("Failed to decode serviceAddress from ledger configuration: {}", e.getMessage());
        }

        // Slot 3: nanosSinceEpoch (uint96 inline) — seconds×1_000_000_000 + nanos.
        // We read only the low 64 bits; values that exceed Long.MAX_VALUE would be out of range
        // for any plausible timestamp.
        Timestamp timestamp = null;
        try {
            final long nanosSinceEpoch = AbiCodec.decodeUint64(result, base + 3 * 32);
            if (nanosSinceEpoch != 0L) {
                final long seconds = nanosSinceEpoch / 1_000_000_000L;
                final int nanos = (int) (nanosSinceEpoch % 1_000_000_000L);
                timestamp = Timestamp.newBuilder().seconds(seconds).nanos(nanos).build();
            }
        } catch (final Exception e) {
            LOGGER.warn("Failed to decode timestamp from ledger configuration: {}", e.getMessage());
        }

        // Slots 4–10: Throttles struct (7 fields, all inline).
        final long maxMessagesPerBundle = AbiCodec.decodeUint64(result, base + 4 * 32);
        final long maxMessagePayloadBytes = AbiCodec.decodeUint64(result, base + 5 * 32);
        final long maxGasPerMessage = AbiCodec.decodeUint64(result, base + 6 * 32);
        final long maxQueueDepth = AbiCodec.decodeUint64(result, base + 7 * 32);
        final long maxSyncBytes = AbiCodec.decodeUint64(result, base + 8 * 32);
        final long maxLocalEndpoints = AbiCodec.decodeUint64(result, base + 9 * 32);
        final long maxPeerEndpoints = AbiCodec.decodeUint64(result, base + 10 * 32);

        final ClprThrottles throttles = ClprThrottles.newBuilder()
                .maxMessagesPerBundle((int) maxMessagesPerBundle)
                .maxMessagePayloadBytes((int) maxMessagePayloadBytes)
                .maxGasPerMessage(maxGasPerMessage)
                .maxQueueDepth((int) maxQueueDepth)
                .maxSyncBytes(maxSyncBytes)
                .maxLocalEndpoints((int) maxLocalEndpoints)
                .maxPeerEndpoints((int) maxPeerEndpoints)
                .build();

        // Slots 11–12: trustAnchor and trustAnchorId (dynamic bytes offsets, relative to base).
        // TODO: decode and include in ClprLedgerConfiguration when initialTrustAnchor and
        // initialTrustAnchorId fields are added to the proto.

        return ClprLedgerConfiguration.newBuilder()
                .protocolVersion(protocolVersion)
                .chainId(chainId)
                .serviceAddress(serviceAddress)
                .timestamp(timestamp)
                .throttles(throttles)
                .build();
    }

    private ClprEndpointManifest decodeEndpointManifest(final byte[] result) {
        if (result.length < 4 * 32) {
            return ClprEndpointManifest.DEFAULT;
        }
        final int base;
        try {
            base = AbiCodec.decodeOffsetOrLength(result, 0);
        } catch (final RuntimeException e) {
            LOGGER.warn("getEndpointManifest returned an unreadable struct offset: {}", e.getMessage());
            return ClprEndpointManifest.DEFAULT;
        }
        if ((long) base + 3 * 32 > result.length) {
            return ClprEndpointManifest.DEFAULT;
        }

        final long version = AbiCodec.decodeUint64(result, base);

        Bytes serviceAddress = Bytes.EMPTY;
        try {
            final int saRelOffset = AbiCodec.decodeOffsetOrLength(result, base + 32);
            final int saAbsBase = base + saRelOffset;
            if ((long) saAbsBase + 32 <= result.length) {
                final int saLen = AbiCodec.decodeOffsetOrLength(result, saAbsBase);
                if (saLen > 0 && (long) saAbsBase + 32 + saLen <= result.length) {
                    serviceAddress = Bytes.wrap(Arrays.copyOfRange(result, saAbsBase + 32, saAbsBase + 32 + saLen));
                }
            }
        } catch (final Exception e) {
            LOGGER.warn("Failed to decode serviceAddress from endpoint manifest: {}", e.getMessage());
        }

        final List<ClprEndpoint> endpoints = new ArrayList<>();
        try {
            final int epsRelOffset = AbiCodec.decodeOffsetOrLength(result, base + 2 * 32);
            final int epsAbsBase = base + epsRelOffset;
            if ((long) epsAbsBase + 32 <= result.length) {
                final int n = AbiCodec.decodeOffsetOrLength(result, epsAbsBase);
                if (n > 0 && (long) epsAbsBase + 32 + (long) n * 32 <= result.length) {
                    for (int i = 0; i < n; i++) {
                        final int elemRelFromArr = AbiCodec.decodeOffsetOrLength(result, epsAbsBase + 32 + i * 32);
                        final ClprEndpoint ep = decodeEndpoint(result, epsAbsBase + 32 + elemRelFromArr);
                        if (ep != null) {
                            endpoints.add(ep);
                        }
                    }
                }
            }
        } catch (final Exception e) {
            LOGGER.warn("Failed to decode endpoints from endpoint manifest: {}", e.getMessage());
        }

        return ClprEndpointManifest.newBuilder()
                .version(version)
                .serviceAddress(serviceAddress)
                .endpoints(endpoints)
                .build();
    }

    /**
     * Decodes a single {@code Endpoint} tuple from absolute offset {@code elemBase} in
     * {@code result}.
     *
     * <p>Tuple: {@code (string ipAddress, uint32 port, bytes tlsCertificate,
     * bytes accountId)}. All offsets in the element head are relative to {@code elemBase}.
     */
    @Nullable
    private static ClprEndpoint decodeEndpoint(final byte[] result, final int elemBase) {
        if (elemBase < 0 || (long) elemBase + 4 * 32 > result.length) {
            return null;
        }
        try {
            // Slot 0: offset to ipAddress string (relative to elemBase).
            final int ipRelOffset = AbiCodec.decodeOffsetOrLength(result, elemBase);
            // Slot 1: port (inline uint32).
            final long portLong = AbiCodec.decodeUint64(result, elemBase + 32);

            final int ipAbsBase = elemBase + ipRelOffset;
            if ((long) ipAbsBase + 32 > result.length) {
                return null;
            }
            final int ipLen = AbiCodec.decodeOffsetOrLength(result, ipAbsBase);
            if ((long) ipAbsBase + 32 + ipLen > result.length) {
                return null;
            }
            final String ip = new String(result, ipAbsBase + 32, ipLen, StandardCharsets.UTF_8);

            // Slot 2: offset to tlsCertificate bytes (relative to elemBase).
            final int tlsRelOffset = AbiCodec.decodeOffsetOrLength(result, elemBase + 2 * 32);
            final byte[] tlsCert = decodeBytesAt(result, elemBase + tlsRelOffset);

            // Slot 3: offset to accountId bytes (relative to elemBase).
            final int accountRelOffset = AbiCodec.decodeOffsetOrLength(result, elemBase + 3 * 32);
            byte[] accountId = decodeBytesAt(result, elemBase + accountRelOffset);

            // When the on-chain accountId is empty, synthesise an ip:port key so the
            // PeerSelector stats map can distinguish different peers.
            if (accountId.length == 0) {
                accountId = (ip + ":" + portLong).getBytes(StandardCharsets.UTF_8);
            }

            return new ClprEndpoint(
                    new ClprServiceEndpoint(ip, (int) portLong), Bytes.wrap(tlsCert), Bytes.wrap(accountId));
        } catch (final Exception e) {
            LOGGER.warn("Failed to decode endpoint at offset {}: {}", elemBase, e.getMessage());
            return null;
        }
    }

    /** Reads a dynamic {@code bytes} value whose length word sits at {@code absBase}. */
    private static byte[] decodeBytesAt(final byte[] result, final int absBase) {
        if (absBase < 0 || (long) absBase + 32 > result.length) {
            return new byte[0];
        }
        final int len = AbiCodec.decodeOffsetOrLength(result, absBase);
        if (len == 0 || (long) absBase + 32 + len > result.length) {
            return new byte[0];
        }
        return Arrays.copyOfRange(result, absBase + 32, absBase + 32 + len);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static byte[] toBytes32(final Bytes value) {
        final byte[] raw = value.toByteArray();
        if (raw.length != 32) {
            throw new IllegalArgumentException("bytes32 value must be exactly 32 bytes: " + raw.length);
        }
        return raw;
    }
}
