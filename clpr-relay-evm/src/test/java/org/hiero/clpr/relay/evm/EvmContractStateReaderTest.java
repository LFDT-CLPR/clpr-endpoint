// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprEndpoint;
import com.hedera.hapi.node.state.clpr.ClprEndpointManifest;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.hapi.node.state.clpr.ClprServiceEndpoint;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.core.metrics.LabeledCounter;
import org.hiero.clpr.relay.core.metrics.SimpleMetrics;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.testfixtures.TestEvmJsonRpcClient;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link EvmContractStateReader} — covers {@code decodeLedgerConfiguration},
 * {@code readChannelState}, and {@code readQueuedMessages} paths exercised through a stub
 * RPC client.
 *
 * <p>ABI encoding of {@code LedgerConfiguration} return value (from
 * {@code ClprTypes.sol}):
 * <pre>
 *   word 0:        outer offset → struct head start (0x20 = 32)
 *   base + 0×32    slot  0  protocolVersion (uint32)
 *   base + 1×32    slot  1  offset to chainId string (relative to base)
 *   base + 2×32    slot  2  offset to serviceAddress bytes (relative to base)
 *   base + 3×32    slot  3  nanosSinceEpoch (uint96) = seconds×1e9 + nanos
 *   base + 4×32    slot  4  Throttles.maxMessagesPerBundle   (uint64)
 *   base + 5×32    slot  5  Throttles.maxMessagePayloadBytes (uint64)
 *   base + 6×32    slot  6  Throttles.maxGasPerMessage       (uint64)
 *   base + 7×32    slot  7  Throttles.maxQueueDepth          (uint64)
 *   base + 8×32    slot  8  Throttles.maxSyncBytes           (uint64)
 *   base + 9×32    slot  9  Throttles.maxLocalEndpoints      (uint64)
 *   base + 10×32   slot 10  Throttles.maxPeerEndpoints       (uint64)
 *   base + 11×32   slot 11  offset to trustAnchor bytes (relative to base)
 *   base + 12×32   slot 12  offset to trustAnchorId bytes (relative to base)
 * </pre>
 * Dynamic tails follow the 13-slot head.
 */
class EvmContractStateReaderTest {

    // -----------------------------------------------------------------------
    // Encoder helpers
    // -----------------------------------------------------------------------

    /**
     * Builds a minimal ABI-encoded {@code getLedgerConfiguration()} return value.
     *
     * @param protocolVersion     value for slot 0
     * @param chainId             string for slot 1 (dynamic)
     * @param serviceAddressBytes bytes for serviceAddress (slot 2, dynamic)
     * @param nanosSinceEpoch     value for slot 3
     * @param throttle1           Throttles.maxMessagesPerBundle   (slot 4)
     * @param throttle2           Throttles.maxMessagePayloadBytes (slot 5)
     * @param throttle3           Throttles.maxGasPerMessage       (slot 6)
     * @param throttle4           Throttles.maxQueueDepth          (slot 7)
     * @param throttle5           Throttles.maxSyncBytes           (slot 8)
     * @param throttle6           Throttles.maxLocalEndpoints      (slot 9)
     * @param throttle7           Throttles.maxPeerEndpoints       (slot 10)
     * @return ABI-encoded bytes ready to pass to {@code decodeLedgerConfiguration}
     */
    private static byte[] encodeGetLedgerConfigurationReturn(
            final long protocolVersion,
            final String chainId,
            final byte[] serviceAddressBytes,
            final long nanosSinceEpoch,
            final long throttle1,
            final long throttle2,
            final long throttle3,
            final long throttle4,
            final long throttle5,
            final long throttle6,
            final long throttle7) {
        /*
         * Layout:
         *
         *   word 0:  outer offset = 0x20 (= 32, pointing at "base")
         *
         *   --- struct head (13 × 32 bytes = 416 bytes, starting at offset 32) ---
         *   base + 0:    protocolVersion
         *   base + 32:   offset to chainId     (relative to base)
         *   base + 64:   offset to serviceAddr  (relative to base)
         *   base + 96:   nanosSinceEpoch
         *   base + 128:  throttle1  (maxMessagesPerBundle)
         *   base + 160:  throttle2  (maxMessagePayloadBytes)
         *   base + 192:  throttle3  (maxGasPerMessage)
         *   base + 224:  throttle4  (maxQueueDepth)
         *   base + 256:  throttle5  (maxSyncBytes)
         *   base + 288:  throttle6  (maxLocalEndpoints)
         *   base + 320:  throttle7  (maxPeerEndpoints)
         *   base + 352:  offset to trustAnchor  (relative to base) — empty tail
         *   base + 384:  offset to trustAnchorId (relative to base) — empty tail
         *
         *   --- dynamic tails ---
         *   chainId tail, serviceAddress tail, trustAnchor tail (empty), trustAnchorId tail (empty)
         */
        final byte[] chainIdBytes = chainId.getBytes(StandardCharsets.UTF_8);

        final int HEAD_SLOTS = 13; // slots 0..12
        final int HEAD_BYTES = HEAD_SLOTS * 32; // 416

        final int chainIdTailOffset = HEAD_BYTES;
        final int chainIdTailSize = 32 + padded32(chainIdBytes.length);

        final int serviceAddrTailOffset = chainIdTailOffset + chainIdTailSize;
        final int serviceAddrTailSize = 32 + padded32(serviceAddressBytes.length);

        final int trustAnchorTailOffset = serviceAddrTailOffset + serviceAddrTailSize;
        final int trustAnchorIdTailOffset = trustAnchorTailOffset + 32; // empty bytes tail = 1 word

        final ByteArrayOutputStream buf = new ByteArrayOutputStream();

        // word 0: outer offset = 0x20
        buf.writeBytes(AbiCodec.encodeUint(0x20L));

        // struct head (13 slots)
        buf.writeBytes(AbiCodec.encodeUint(protocolVersion)); // slot 0
        buf.writeBytes(AbiCodec.encodeUint(chainIdTailOffset)); // slot 1: chainId offset
        buf.writeBytes(AbiCodec.encodeUint(serviceAddrTailOffset)); // slot 2: serviceAddr offset
        buf.writeBytes(AbiCodec.encodeUint(nanosSinceEpoch)); // slot 3
        buf.writeBytes(AbiCodec.encodeUint(throttle1)); // slot 4: maxMessagesPerBundle
        buf.writeBytes(AbiCodec.encodeUint(throttle2)); // slot 5: maxMessagePayloadBytes
        buf.writeBytes(AbiCodec.encodeUint(throttle3)); // slot 6: maxGasPerMessage
        buf.writeBytes(AbiCodec.encodeUint(throttle4)); // slot 7: maxQueueDepth
        buf.writeBytes(AbiCodec.encodeUint(throttle5)); // slot 8: maxSyncBytes
        buf.writeBytes(AbiCodec.encodeUint(throttle6)); // slot 9: maxLocalEndpoints
        buf.writeBytes(AbiCodec.encodeUint(throttle7)); // slot 10: maxPeerEndpoints
        buf.writeBytes(AbiCodec.encodeUint(trustAnchorTailOffset)); // slot 11: trustAnchor offset
        buf.writeBytes(AbiCodec.encodeUint(trustAnchorIdTailOffset)); // slot 12: trustAnchorId offset

        // chainId tail
        buf.writeBytes(AbiCodec.encodeUint(chainIdBytes.length));
        if (chainIdBytes.length > 0) {
            buf.writeBytes(AbiCodec.padRight32(chainIdBytes));
        }

        // serviceAddress tail
        buf.writeBytes(AbiCodec.encodeUint(serviceAddressBytes.length));
        if (serviceAddressBytes.length > 0) {
            buf.writeBytes(AbiCodec.padRight32(serviceAddressBytes));
        }

        // trustAnchor tail (empty)
        buf.writeBytes(AbiCodec.encodeUint(0L));

        // trustAnchorId tail (empty)
        buf.writeBytes(AbiCodec.encodeUint(0L));

        return buf.toByteArray();
    }

    /** Returns the number of bytes needed to hold {@code len} bytes padded to a 32-byte boundary. */
    private static int padded32(final int len) {
        if (len == 0) {
            return 0;
        }
        return ((len + 31) / 32) * 32;
    }

    // -----------------------------------------------------------------------
    // Convenience: build an encoded response with typical throttle/endpoint values
    // and retrieve the configuration via a stub reader.
    // -----------------------------------------------------------------------

    private static ClprLedgerConfiguration decode(
            final long protocolVersion,
            final String chainId,
            final byte[] serviceAddressBytes,
            final long nanosSinceEpoch,
            final long t1,
            final long t2,
            final long t3,
            final long t4,
            final long t5,
            final long t6,
            final long t7) {
        final byte[] encoded = encodeGetLedgerConfigurationReturn(
                protocolVersion, chainId, serviceAddressBytes, nanosSinceEpoch, t1, t2, t3, t4, t5, t6, t7);
        final EvmJsonRpcClient stub = TestEvmJsonRpcClient.newStubClient(encoded);
        final EvmContractStateReader reader = new EvmContractStateReader(stub, "0xdeadbeef");
        return reader.readLedgerConfiguration(CommitmentLevel.FINALIZED);
    }

    // -----------------------------------------------------------------------
    // Tests — protocolVersion
    // -----------------------------------------------------------------------

    @Test
    void decodeLedgerConfiguration_extractsProtocolVersion() {
        final ClprLedgerConfiguration cfg = decode(42L, "eip155:1", new byte[0], 0L, 1L, 3L, 4L, 5L, 6L, 0L, 0L);

        assertThat(cfg.protocolVersion()).isEqualTo(42);
    }

    @Test
    void decodeLedgerConfiguration_protocolVersionZeroIsPreserved() {
        final ClprLedgerConfiguration cfg = decode(0L, "", new byte[0], 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);

        assertThat(cfg.protocolVersion()).isEqualTo(0);
    }

    // -----------------------------------------------------------------------
    // Tests — chainId and serviceAddress
    // -----------------------------------------------------------------------

    @Test
    void decodeLedgerConfiguration_extractsChainIdAndServiceAddress() {
        final byte[] saBytes = new byte[] {0x01, 0x02, 0x03, 0x04};
        final ClprLedgerConfiguration cfg = decode(1L, "hedera:mainnet", saBytes, 0L, 1L, 3L, 4L, 5L, 6L, 0L, 0L);

        assertThat(cfg.chainId()).isEqualTo("hedera:mainnet");
        assertThat(cfg.serviceAddress()).isEqualTo(Bytes.wrap(saBytes));
    }

    @Test
    void decodeLedgerConfiguration_emptyChainIdAndServiceAddress() {
        final ClprLedgerConfiguration cfg = decode(1L, "", new byte[0], 0L, 1L, 3L, 4L, 5L, 6L, 0L, 0L);

        assertThat(cfg.chainId()).isEqualTo("");
        assertThat(cfg.serviceAddress()).isEqualTo(Bytes.EMPTY);
    }

    // -----------------------------------------------------------------------
    // Tests — all 7 throttle fields
    // -----------------------------------------------------------------------

    @Test
    void decodeLedgerConfiguration_extractsAllSevenThrottleFields() {
        final ClprLedgerConfiguration cfg = decode(
                1L,
                "eip155:1",
                new byte[0],
                0L,
                // throttle fields in order: 1..7
                100L, // maxMessagesPerBundle
                1_000_000L, // maxMessagePayloadBytes
                500_000L, // maxGasPerMessage
                200L, // maxQueueDepth
                2_000_000L, // maxSyncBytes
                11L, // maxLocalEndpoints
                22L); // maxPeerEndpoints

        assertThat(cfg.hasThrottles()).isTrue();
        final var t = cfg.throttles();
        assertThat(t.maxMessagesPerBundle()).isEqualTo(100);
        assertThat(t.maxMessagePayloadBytes()).isEqualTo(1_000_000);
        assertThat(t.maxGasPerMessage()).isEqualTo(500_000L);
        assertThat(t.maxQueueDepth()).isEqualTo(200);
        assertThat(t.maxSyncBytes()).isEqualTo(2_000_000L);
        assertThat(t.maxLocalEndpoints()).isEqualTo(11);
        assertThat(t.maxPeerEndpoints()).isEqualTo(22);
    }

    // -----------------------------------------------------------------------
    // Tests — short/empty payloads return DEFAULT
    // -----------------------------------------------------------------------

    @Test
    void decodeLedgerConfiguration_tooShortReturnsDefault() {
        final EvmJsonRpcClient stub = TestEvmJsonRpcClient.newStubClient(new byte[10]);
        final EvmContractStateReader reader = new EvmContractStateReader(stub, "0xdeadbeef");
        assertThat(reader.readLedgerConfiguration(CommitmentLevel.FINALIZED))
                .isEqualTo(ClprLedgerConfiguration.DEFAULT);
    }

    @Test
    void decodeLedgerConfiguration_emptyPayloadReturnsDefault() {
        final EvmJsonRpcClient stub = TestEvmJsonRpcClient.newStubClient(new byte[0]);
        final EvmContractStateReader reader = new EvmContractStateReader(stub, "0xdeadbeef");
        assertThat(reader.readLedgerConfiguration(CommitmentLevel.FINALIZED))
                .isEqualTo(ClprLedgerConfiguration.DEFAULT);
    }

    // =========================================================================
    // Encoder helpers for Channel struct
    // =========================================================================

    /**
     * Builds a minimal ABI-encoded {@code getChannel(bytes32)} return value.
     *
     * <p>Layout (outer offset = 0x20, then 29-slot head starting at base = 32):
     * <pre>
     *   slot  0: channelId (bytes32)
     *   slot  1: verifier address (zero)
     *   slot  2: status (uint8)
     *   slot  3: nextMessageId (uint64)
     *   slot  4: ackedMessageId (uint64)
     *   slot  5: receivedMessageId (uint64)
     *   slot  6: nextExpectedReplyId (unused)
     *   slot  7: peerConfigTimestamp (unused)
     *   slot  8: lastConfigTimestamp (unused)
     *   slot  9: sentRunningHash (bytes32)
     *   slot 10: receivedRunningHash (bytes32)
     *   slot 11: ownershipCommitment (bytes32, zero)
     *   slot 12: salt (bytes32, zero)
     *   slot 13: chainId offset (dynamic, points to zero-length tail)
     *   slot 14: peerServiceAddress offset (dynamic)
     *   slot 15–21: throttle fields (zero) — 7 fields including maxLocalEndpoints/maxPeerEndpoints
     *   slot 22: trustAnchor offset (dynamic)
     *   slot 23: lastDataMessageId (uint64, zero)
     *   slot 24: trustAnchorId offset (dynamic, points to zero-length tail)
     *   slot 25: channelContext offset (dynamic, points to zero-length tail)
     *   slot 26: endpointManifestVersion (uint64, zero)
     * </pre>
     */
    private static byte[] encodeGetChannelReturn(
            final byte[] channelId32,
            final long statusOrdinal,
            final long nextMessageId,
            final long ackedMessageId,
            final long receivedMessageId,
            final byte[] sentRunningHash32,
            final byte[] receivedRunningHash32,
            final byte[] serviceAddressBytes,
            final byte[] trustAnchorBytes) {

        final int HEAD_SLOTS = 27;
        final int HEAD_BYTES = HEAD_SLOTS * 32; // 864

        // Dynamic tails layout (all offsets relative to base):
        //   chainId:           HEAD_BYTES + 0   → length=0 (1 word)
        //   serviceAddress:    chainId + 32     → length word + padded data
        //   trustAnchor:       serviceAddress + 32 + padded32(serviceAddressBytes.length)
        //   trustAnchorId:     trustAnchor + 32 + padded32(trustAnchorBytes.length) → length=0 (1 word)
        //   channelContext: trustAnchorId + 32 → length=0 (1 word)
        final int chainIdRelOffset = HEAD_BYTES;
        final int serviceAddrRelOffset = chainIdRelOffset + 32; // chainId tail = 1 word (length=0)
        final int trustAnchorRelOffset = serviceAddrRelOffset + 32 + padded32(serviceAddressBytes.length);
        final int trustAnchorIdRelOffset = trustAnchorRelOffset + 32 + padded32(trustAnchorBytes.length);
        final int channelContextRelOffset = trustAnchorIdRelOffset + 32; // trustAnchorId tail = 1 word

        final ByteArrayOutputStream buf = new ByteArrayOutputStream();

        // word 0: outer offset = 0x20
        buf.writeBytes(AbiCodec.encodeUint(0x20L));

        // struct head (27 slots)
        buf.writeBytes(channelId32); // slot  0
        buf.writeBytes(AbiCodec.encodeUint(0L)); // slot  1: verifier (zero)
        buf.writeBytes(AbiCodec.encodeUint(statusOrdinal)); // slot  2: status
        buf.writeBytes(AbiCodec.encodeUint(nextMessageId)); // slot  3
        buf.writeBytes(AbiCodec.encodeUint(ackedMessageId)); // slot  4
        buf.writeBytes(AbiCodec.encodeUint(receivedMessageId)); // slot  5
        buf.writeBytes(AbiCodec.encodeUint(0L)); // slot  6: nextExpectedReplyId
        buf.writeBytes(AbiCodec.encodeUint(0L)); // slot  7: peerConfigTimestamp
        buf.writeBytes(AbiCodec.encodeUint(0L)); // slot  8: lastConfigTimestamp
        buf.writeBytes(sentRunningHash32); // slot  9
        buf.writeBytes(receivedRunningHash32); // slot 10
        buf.writeBytes(AbiCodec.encodeUint(0L)); // slot 11: ownershipCommitment
        buf.writeBytes(AbiCodec.encodeUint(0L)); // slot 12: salt
        buf.writeBytes(AbiCodec.encodeUint(chainIdRelOffset)); // slot 13: chainId offset
        buf.writeBytes(AbiCodec.encodeUint(serviceAddrRelOffset)); // slot 14: serviceAddress offset
        for (int i = 15; i <= 21; i++) {
            buf.writeBytes(AbiCodec.encodeUint(0L)); // slots 15–21: throttles (7 fields, zero)
        }
        buf.writeBytes(AbiCodec.encodeUint(trustAnchorRelOffset)); // slot 22: trustAnchor offset
        buf.writeBytes(AbiCodec.encodeUint(0L)); // slot 23: lastDataMessageId
        buf.writeBytes(AbiCodec.encodeUint(trustAnchorIdRelOffset)); // slot 24: trustAnchorId offset
        buf.writeBytes(AbiCodec.encodeUint(channelContextRelOffset)); // slot 25: channelContext offset
        buf.writeBytes(AbiCodec.encodeUint(0L)); // slot 26: endpointManifestVersion

        // dynamic tails (chainId empty, serviceAddress, trustAnchor, trustAnchorId empty, channelContext empty)
        buf.writeBytes(AbiCodec.encodeUint(0L)); // chainId length=0
        buf.writeBytes(AbiCodec.encodeUint(serviceAddressBytes.length)); // serviceAddress length
        if (serviceAddressBytes.length > 0) {
            buf.writeBytes(AbiCodec.padRight32(serviceAddressBytes));
        }
        buf.writeBytes(AbiCodec.encodeUint(trustAnchorBytes.length)); // trustAnchor length
        if (trustAnchorBytes.length > 0) {
            buf.writeBytes(AbiCodec.padRight32(trustAnchorBytes));
        }
        buf.writeBytes(AbiCodec.encodeUint(0L)); // trustAnchorId length=0
        buf.writeBytes(AbiCodec.encodeUint(0L)); // channelContext length=0

        return buf.toByteArray();
    }

    /** Convenience wrapper: decode a channel via a one-shot stub reader. */
    private static ClprChannel decodeChannel(
            final byte[] channelId32,
            final long statusOrdinal,
            final long nextMessageId,
            final long ackedMessageId,
            final long receivedMessageId,
            final byte[] sentRunningHash32,
            final byte[] receivedRunningHash32,
            final byte[] serviceAddressBytes,
            final byte[] trustAnchorBytes) {
        final byte[] encoded = encodeGetChannelReturn(
                channelId32,
                statusOrdinal,
                nextMessageId,
                ackedMessageId,
                receivedMessageId,
                sentRunningHash32,
                receivedRunningHash32,
                serviceAddressBytes,
                trustAnchorBytes);
        final EvmJsonRpcClient stub = TestEvmJsonRpcClient.newStubClient(encoded);
        final EvmContractStateReader reader = new EvmContractStateReader(stub, "0xdeadbeef");
        return reader.readChannelState(Bytes.wrap(channelId32), "0x01")
                .orElseThrow(() -> new AssertionError("expected non-empty channel state for decode test"));
    }

    // =========================================================================
    // Tests — readChannelState / decodeChannel
    // =========================================================================

    @Test
    void decodeChannel_typicalLayout() {
        final byte[] connId = new byte[32];
        connId[31] = 0x01;
        final byte[] sentHash = new byte[32];
        sentHash[0] = 0x11;
        final byte[] recvHash = new byte[32];
        recvHash[0] = 0x22;
        final byte[] serviceAddr = new byte[] {0x0a, 0x00, 0x00, 0x01};
        final byte[] trustAnchor = new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef};

        final ClprChannel conn = decodeChannel(
                connId,
                ClprChannelStatus.ACTIVE.protoOrdinal(),
                42L,
                10L,
                7L,
                sentHash,
                recvHash,
                serviceAddr,
                trustAnchor);

        assertThat(conn.channelId()).isEqualTo(Bytes.wrap(connId));
        assertThat(conn.status()).isEqualTo(ClprChannelStatus.ACTIVE);
        assertThat(conn.nextMessageId()).isEqualTo(42L);
        assertThat(conn.ackedMessageId()).isEqualTo(10L);
        assertThat(conn.receivedMessageId()).isEqualTo(7L);
        assertThat(conn.sentRunningHash()).isEqualTo(Bytes.wrap(sentHash));
        assertThat(conn.receivedRunningHash()).isEqualTo(Bytes.wrap(recvHash));
        assertThat(conn.serviceAddress()).isEqualTo(Bytes.wrap(serviceAddr));
        assertThat(conn.trustAnchor()).isEqualTo(Bytes.wrap(trustAnchor));
    }

    @Test
    void decodeChannel_emptyOptionalFields() {
        final byte[] connId = new byte[32];
        final byte[] zeroHash = new byte[32];

        final ClprChannel conn = decodeChannel(
                connId,
                ClprChannelStatus.PENDING.protoOrdinal(),
                0L,
                0L,
                0L,
                zeroHash,
                zeroHash,
                new byte[0],
                new byte[0]);

        assertThat(conn.status()).isEqualTo(ClprChannelStatus.PENDING);
        assertThat(conn.nextMessageId()).isEqualTo(0L);
        assertThat(conn.serviceAddress()).isEqualTo(Bytes.EMPTY);
        assertThat(conn.trustAnchor()).isEqualTo(Bytes.EMPTY);
    }

    @Test
    void decodeChannel_statusMappedCorrectly() {
        final byte[] connId = new byte[32];
        final byte[] zeroHash = new byte[32];

        for (final ClprChannelStatus s : new ClprChannelStatus[] {
            ClprChannelStatus.ACTIVE,
            ClprChannelStatus.PAUSED,
            ClprChannelStatus.CLOSING,
            ClprChannelStatus.DRAINED,
            ClprChannelStatus.CLOSED
        }) {
            final ClprChannel conn =
                    decodeChannel(connId, s.protoOrdinal(), 0L, 0L, 0L, zeroHash, zeroHash, new byte[0], new byte[0]);
            assertThat(conn.status()).as("status %s", s).isEqualTo(s);
        }
    }

    @Test
    void decodeChannel_shortPayloadReturnsDefault() {
        // 895 bytes — one byte short of the 896-byte minimum
        final EvmJsonRpcClient stub = TestEvmJsonRpcClient.newStubClient(new byte[895]);
        final EvmContractStateReader reader = new EvmContractStateReader(stub, "0xdeadbeef");
        assertThat(reader.readChannelState(Bytes.wrap(new byte[32]), "0x01")).isEmpty();
    }

    @Test
    void decodeChannel_emptyPayloadReturnsEmpty() {
        final EvmJsonRpcClient stub = TestEvmJsonRpcClient.newStubClient(new byte[0]);
        final EvmContractStateReader reader = new EvmContractStateReader(stub, "0xdeadbeef");
        assertThat(reader.readChannelState(Bytes.wrap(new byte[32]), "0x01")).isEmpty();
    }

    @Test
    void decodeChannel_clprChannelNotFoundReturnsEmpty() {
        final EvmJsonRpcClient throwing = new TestEvmJsonRpcClient("http://unused") {
            @Override
            public String ethCall(final String to, final String data, final String blockTag) {
                throw new JsonRpcException(3, "execution reverted: custom error 0x029a1002 data=\"0x029a1002\"");
            }
        };
        final EvmContractStateReader reader = new EvmContractStateReader(throwing, "0xdeadbeef");
        assertThat(reader.readChannelState(Bytes.wrap(new byte[32]), "0x01")).isEmpty();
    }

    @Test
    void decodeChannel_unrelatedJsonRpcErrorPropagates() {
        final EvmJsonRpcClient throwing = new TestEvmJsonRpcClient("http://unused") {
            @Override
            public String ethCall(final String to, final String data, final String blockTag) {
                throw new JsonRpcException(-32000, "execution reverted: custom error 0xdeadbeef");
            }
        };
        final EvmContractStateReader reader = new EvmContractStateReader(throwing, "0xdeadbeef");
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> reader.readChannelState(Bytes.wrap(new byte[32]), "0x01"))
                .isInstanceOf(JsonRpcException.class)
                .hasMessageContaining("0xdeadbeef");
    }

    // =========================================================================
    // Encoder helpers for MessageValue struct
    // =========================================================================

    /**
     * Builds a minimal ABI-encoded {@code getMessage(bytes32,uint64)} return value.
     *
     * <p>Layout (outer offset = 0x20, then 2-slot struct head at base):
     * <pre>
     *   slot 0 (base+0):  offset to payload bytes (relative to base)
     *   slot 1 (base+32): runningHashAfterProcessing (bytes32, inline)
     * </pre>
     * Dynamic tail: payload length word + padded data.
     */
    private static byte[] encodeGetMessageReturn(final byte[] payloadBytes, final byte[] runningHash32) {

        // Struct head = 2 slots = 64 bytes.
        // payload offset relative to base = 64 (immediately after the 2-slot head).
        final int payloadRelOffset = 2 * 32;

        final ByteArrayOutputStream buf = new ByteArrayOutputStream();

        // word 0: outer offset = 0x20
        buf.writeBytes(AbiCodec.encodeUint(0x20L));

        // struct head
        buf.writeBytes(AbiCodec.encodeUint(payloadRelOffset)); // slot 0: payload offset
        buf.writeBytes(runningHash32); // slot 1: runningHash (bytes32)

        // payload tail
        buf.writeBytes(AbiCodec.encodeUint(payloadBytes.length));
        if (payloadBytes.length > 0) {
            buf.writeBytes(AbiCodec.padRight32(payloadBytes));
        }

        return buf.toByteArray();
    }

    /** Convenience wrapper: decode one message via a stub reader. */
    private static List<ContractStateReader.QueuedMessage> decodeMessage(
            final byte[] payloadBytes, final byte[] runningHash32) {
        final byte[] encoded = encodeGetMessageReturn(payloadBytes, runningHash32);
        final EvmJsonRpcClient stub = TestEvmJsonRpcClient.newStubClient(encoded);
        final EvmContractStateReader reader = new EvmContractStateReader(stub, "0xdeadbeef");
        // fromId=0, toId=1 → one getMessage call
        return reader.readQueuedMessages(Bytes.wrap(new byte[32]), 0L, 1L, "0x01");
    }

    // =========================================================================
    // Tests — readQueuedMessages / decodeMessage
    // =========================================================================

    @Test
    void decodeMessage_typicalLayout() {
        final byte[] runningHash = new byte[32];
        runningHash[0] = 0x42;
        // A valid empty ClprMessagePayload serialised as protobuf = zero bytes (all defaults).
        final byte[] payload = new byte[0];

        final List<ContractStateReader.QueuedMessage> messages = decodeMessage(payload, runningHash);

        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).value().runningHashAfterProcessing()).isEqualTo(Bytes.wrap(runningHash));
    }

    @Test
    void decodeMessage_emptyPayload() {
        final byte[] runningHash = new byte[32];
        runningHash[31] = 0x07;

        final List<ContractStateReader.QueuedMessage> messages = decodeMessage(new byte[0], runningHash);

        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).value().runningHashAfterProcessing()).isEqualTo(Bytes.wrap(runningHash));
    }

    @Test
    void decodeMessage_zeroRunningHash() {
        final byte[] zeroHash = new byte[32];

        final List<ContractStateReader.QueuedMessage> messages = decodeMessage(new byte[0], zeroHash);

        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).value().runningHashAfterProcessing()).isEqualTo(Bytes.wrap(zeroHash));
    }

    @Test
    void decodeMessage_shortPayloadSkipsEntry() {
        // Return fewer than 96 bytes — the production code requires at least 96.
        final EvmJsonRpcClient stub = TestEvmJsonRpcClient.newStubClient(new byte[95]);
        final EvmContractStateReader reader = new EvmContractStateReader(stub, "0xdeadbeef");
        final List<ContractStateReader.QueuedMessage> messages =
                reader.readQueuedMessages(Bytes.wrap(new byte[32]), 0L, 1L, "0x01");

        assertThat(messages).isEmpty();
    }

    @Test
    void decodeMessage_emptyRangeReturnsEmpty() {
        final EvmJsonRpcClient stub = TestEvmJsonRpcClient.newStubClient(new byte[0]);
        final EvmContractStateReader reader = new EvmContractStateReader(stub, "0xdeadbeef");
        final List<ContractStateReader.QueuedMessage> messages =
                reader.readQueuedMessages(Bytes.wrap(new byte[32]), 5L, 5L, "0x01");

        assertThat(messages).isEmpty();
    }

    @Test
    void decodeMessage_multipleMessages() {
        final byte[] hash1 = new byte[32];
        hash1[0] = 0x01;
        final byte[] hash2 = new byte[32];
        hash2[0] = 0x02;

        final byte[] encoded1 = encodeGetMessageReturn(new byte[0], hash1);
        final byte[] encoded2 = encodeGetMessageReturn(new byte[0], hash2);

        // Alternate responses for successive calls
        final EvmJsonRpcClient twoResponseStub = new TestEvmJsonRpcClient() {
            private int call = 0;

            @Override
            public String ethCall(final String to, final String data, final String blockTag) {
                return AbiCodec.toHex(call++ == 0 ? encoded1 : encoded2);
            }
        };
        final EvmContractStateReader reader = new EvmContractStateReader(twoResponseStub, "0xdeadbeef");
        final List<ContractStateReader.QueuedMessage> messages =
                reader.readQueuedMessages(Bytes.wrap(new byte[32]), 0L, 2L, "0x01");

        assertThat(messages).hasSize(2);
        assertThat(messages.get(0).value().runningHashAfterProcessing()).isEqualTo(Bytes.wrap(hash1));
        assertThat(messages.get(1).value().runningHashAfterProcessing()).isEqualTo(Bytes.wrap(hash2));
    }

    // -----------------------------------------------------------------------
    // ClprEndpointManifest decode (getEndpointManifest / getPeerEndpointManifest)
    // -----------------------------------------------------------------------

    /**
     * Builds a minimal ABI-encoded {@code getEndpointManifest()} return value with an empty
     * endpoints array (a valid on-ledger state for version &ge; 1). Layout:
     * <pre>
     *   word 0:      outer offset → base = 32
     *   base + 0×32  version (uint64)
     *   base + 1×32  offset to serviceAddress bytes (rel to base) = 96
     *   base + 2×32  offset to endpoints array (rel to base) = 160
     *   base + 96    serviceAddress tail: length word + 32-byte right-padded data
     *   base + 160   endpoints array: count = 0
     * </pre>
     *
     * @param version         the manifest version (slot 0)
     * @param serviceAddress  the service address bytes (length &le; 32)
     * @return ABI-encoded bytes ready to pass to {@code decodeEndpointManifest}
     */
    private static byte[] encodeGetEndpointManifestReturn(final long version, final byte[] serviceAddress) {
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.writeBytes(AbiCodec.encodeUint(0x20L)); // outer offset; base = 32
        buf.writeBytes(AbiCodec.encodeUint(version)); // slot 0: version
        buf.writeBytes(AbiCodec.encodeUint(96L)); // slot 1: serviceAddress tail offset (rel to base)
        buf.writeBytes(AbiCodec.encodeUint(160L)); // slot 2: endpoints array offset (rel to base)
        buf.writeBytes(AbiCodec.encodeUint(serviceAddress.length)); // serviceAddress length word
        buf.writeBytes(AbiCodec.padRight32(serviceAddress)); // serviceAddress data (32-byte word)
        buf.writeBytes(AbiCodec.encodeUint(0L)); // endpoints array count = 0
        return buf.toByteArray();
    }

    @Test
    void readEndpointManifest_decodesVersionAndServiceAddress() {
        final byte[] serviceAddr = new byte[20];
        serviceAddr[19] = 0x2a;
        final EvmJsonRpcClient stub =
                TestEvmJsonRpcClient.newStubClient(encodeGetEndpointManifestReturn(7L, serviceAddr));
        final EvmContractStateReader reader = new EvmContractStateReader(stub, "0xdeadbeef");

        final ClprEndpointManifest manifest = reader.readEndpointManifest(CommitmentLevel.FINALIZED.toBlockTag());
        assertThat(manifest.version()).isEqualTo(7L);
        assertThat(manifest.serviceAddress()).isEqualTo(Bytes.wrap(serviceAddr));
        assertThat(manifest.endpoints()).isEmpty();
    }

    @Test
    void readPeerEndpointManifest_decodesVersionWithEmptyEndpoints() {
        final EvmJsonRpcClient stub =
                TestEvmJsonRpcClient.newStubClient(encodeGetEndpointManifestReturn(3L, new byte[20]));
        final EvmContractStateReader reader = new EvmContractStateReader(stub, "0xdeadbeef");

        final ClprEndpointManifest manifest =
                reader.readPeerEndpointManifest(Bytes.wrap(new byte[32]), CommitmentLevel.FINALIZED);
        assertThat(manifest.version()).isEqualTo(3L);
        assertThat(manifest.endpoints()).isEmpty();
    }

    @Test
    void readEndpointManifest_rpcFailure_incrementsReadFailedCounter() {
        // A failing eth_call is observable via evm.manifest.read.failed{scope=local,reason=rpc_error}
        // (and still returns DEFAULT so callers degrade gracefully).
        final var metrics = new SimpleMetrics();
        final var readFailures =
                new LabeledCounter("evm.manifest", "read.failed", "Endpoint-manifest read failures", metrics);
        final EvmJsonRpcClient throwing = new TestEvmJsonRpcClient("http://unused") {
            @Override
            public String ethCall(final String to, final String data, final String blockTag) {
                throw new JsonRpcException(-32000, "boom");
            }
        };
        final EvmContractStateReader reader = new EvmContractStateReader(throwing, "0xdeadbeef", readFailures);

        assertThat(reader.readEndpointManifest(CommitmentLevel.FINALIZED.toBlockTag()))
                .isEqualTo(ClprEndpointManifest.DEFAULT);
        assertThat(readFailures.counter("scope", "local", "reason", "rpc_error").get())
                .isEqualTo(1L);
    }

    @Test
    void readEndpointManifest_tooShortReturnsDefault() {
        final EvmJsonRpcClient stub = TestEvmJsonRpcClient.newStubClient(new byte[10]);
        final EvmContractStateReader reader = new EvmContractStateReader(stub, "0xdeadbeef");
        assertThat(reader.readEndpointManifest(CommitmentLevel.FINALIZED.toBlockTag()))
                .isEqualTo(ClprEndpointManifest.DEFAULT);
    }

    @Test
    void readEndpointManifest_malformedServiceAddressLength_degradesToEmptyServiceAddress() {
        // A serviceAddress length word with bits set above int range must not wrap into a bogus
        // in-range length and drive an out-of-bounds copy (issue #294); the hardened decode rejects
        // the word, leaves the service address empty, and still returns the valid version.
        final byte[] encoded = encodeGetEndpointManifestReturn(4L, new byte[20]);
        // The serviceAddress length word sits at absolute offset 128 (base=32; slot-1 offset=96 → 32+96).
        Arrays.fill(encoded, 128, 160, (byte) 0xff);
        final EvmJsonRpcClient stub = TestEvmJsonRpcClient.newStubClient(encoded);
        final EvmContractStateReader reader = new EvmContractStateReader(stub, "0xdeadbeef");

        final ClprEndpointManifest manifest = reader.readEndpointManifest(CommitmentLevel.FINALIZED.toBlockTag());
        assertThat(manifest.version()).isEqualTo(4L);
        assertThat(manifest.serviceAddress()).isEqualTo(Bytes.EMPTY);
    }

    /**
     * ABI-encodes a single {@code Endpoint} tuple {@code (string ipAddress, uint32 port,
     * bytes tlsCertificate, bytes accountId)}, self-contained with all head offsets relative to the
     * element base. Byte-length fields are kept &le; 32 so each tail occupies one data word.
     */
    private static byte[] encodeEndpoint(final String ip, final int port, final byte[] tls, final byte[] accountId) {
        final byte[] ipBytes = ip.getBytes(StandardCharsets.UTF_8);
        final int ipTail = 32 + (ipBytes.length > 0 ? 32 : 0);
        final int tlsTail = 32 + (tls.length > 0 ? 32 : 0);
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.writeBytes(AbiCodec.encodeUint(128L)); // slot 0: ipAddress offset (rel to elemBase)
        buf.writeBytes(AbiCodec.encodeUint(port)); // slot 1: port (inline)
        buf.writeBytes(AbiCodec.encodeUint(128L + ipTail)); // slot 2: tlsCertificate offset
        buf.writeBytes(AbiCodec.encodeUint(128L + ipTail + tlsTail)); // slot 3: accountId offset
        writeBytesTail(buf, ipBytes);
        writeBytesTail(buf, tls);
        writeBytesTail(buf, accountId);
        return buf.toByteArray();
    }

    /** Writes an ABI dynamic-bytes tail: a length word followed (when non-empty) by one padded word. */
    private static void writeBytesTail(final ByteArrayOutputStream buf, final byte[] data) {
        buf.writeBytes(AbiCodec.encodeUint(data.length));
        if (data.length > 0) {
            buf.writeBytes(AbiCodec.padRight32(data));
        }
    }

    /**
     * Builds an ABI-encoded {@code getEndpointManifest()} return value carrying a populated
     * {@code Endpoint[]} array (each element produced by {@link #encodeEndpoint}).
     */
    private static byte[] encodeGetEndpointManifestReturnWithEndpoints(
            final long version, final byte[] serviceAddress, final List<byte[]> endpoints) {
        final int saTail = 32 + (serviceAddress.length > 0 ? 32 : 0);
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.writeBytes(AbiCodec.encodeUint(0x20L)); // outer offset; base = 32
        buf.writeBytes(AbiCodec.encodeUint(version)); // slot 0: version
        buf.writeBytes(AbiCodec.encodeUint(96L)); // slot 1: serviceAddress offset (rel to base)
        buf.writeBytes(AbiCodec.encodeUint(96L + saTail)); // slot 2: endpoints array offset (rel to base)
        writeBytesTail(buf, serviceAddress);
        // endpoints array: count word, then one offset word per element (rel to end of the count word),
        // then the element bodies laid out contiguously.
        buf.writeBytes(AbiCodec.encodeUint(endpoints.size()));
        long running = endpoints.size() * 32L;
        for (final byte[] e : endpoints) {
            buf.writeBytes(AbiCodec.encodeUint(running));
            running += e.length;
        }
        for (final byte[] e : endpoints) {
            buf.writeBytes(e);
        }
        return buf.toByteArray();
    }

    @Test
    void readPeerEndpointManifest_decodesPopulatedEndpoints() {
        final byte[] ep0 = encodeEndpoint("10.0.0.1", 9545, new byte[] {(byte) 0xAA, (byte) 0xBB}, new byte[] {0x01});
        // ep1 has an empty on-chain accountId, which the decoder must keep empty (never synthesise).
        final byte[] ep1 = encodeEndpoint("10.0.0.2", 9546, new byte[0], new byte[0]);
        final EvmJsonRpcClient stub = TestEvmJsonRpcClient.newStubClient(
                encodeGetEndpointManifestReturnWithEndpoints(5L, new byte[20], List.of(ep0, ep1)));
        final EvmContractStateReader reader = new EvmContractStateReader(stub, "0xdeadbeef");

        final ClprEndpointManifest manifest =
                reader.readPeerEndpointManifest(Bytes.wrap(new byte[32]), CommitmentLevel.FINALIZED);

        assertThat(manifest.version()).isEqualTo(5L);
        assertThat(manifest.endpoints()).hasSize(2);

        final var e0 = manifest.endpoints().getFirst();
        assertThat(e0.serviceEndpoint().ipAddress()).isEqualTo("10.0.0.1");
        assertThat(e0.serviceEndpoint().port()).isEqualTo(9545);
        assertThat(e0.tlsCertificate()).isEqualTo(Bytes.wrap(new byte[] {(byte) 0xAA, (byte) 0xBB}));
        assertThat(e0.accountId()).isEqualTo(Bytes.wrap(new byte[] {0x01}));

        final var e1 = manifest.endpoints().get(1);
        assertThat(e1.serviceEndpoint().ipAddress()).isEqualTo("10.0.0.2");
        assertThat(e1.serviceEndpoint().port()).isEqualTo(9546);
        assertThat(e1.accountId()).isEqualTo(Bytes.EMPTY);
    }

    @Test
    void readEndpointManifest_reEncodesByteExactWithoutSynthesisedAccountId() {
        // The decoded local manifest is re-encoded as the protobuf preimage of the slot-18
        // commitment. An endpoint registered with an empty accountId must therefore re-encode
        // without field 3, exactly as the contract committed it.
        final byte[] ep = encodeEndpoint("10.0.0.2", 9546, new byte[0], new byte[0]);
        final EvmJsonRpcClient stub = TestEvmJsonRpcClient.newStubClient(
                encodeGetEndpointManifestReturnWithEndpoints(2L, new byte[20], List.of(ep)));
        final EvmContractStateReader reader = new EvmContractStateReader(stub, "0xdeadbeef");

        final ClprEndpointManifest decoded = reader.readEndpointManifest("latest");

        final ClprEndpointManifest committed = ClprEndpointManifest.newBuilder()
                .version(2L)
                .serviceAddress(Bytes.wrap(new byte[20]))
                .endpoints(List.of(ClprEndpoint.newBuilder()
                        .serviceEndpoint(new ClprServiceEndpoint("10.0.0.2", 9546))
                        .build()))
                .build();
        assertThat(ClprEndpointManifest.PROTOBUF.toBytes(decoded))
                .isEqualTo(ClprEndpointManifest.PROTOBUF.toBytes(committed));
    }

    // =========================================================================
    // readChannelCount, bytes32 guard, and eth_call failure fallbacks
    // =========================================================================

    /** A stub whose {@code ethCall} always throws the given RPC error. */
    private static EvmJsonRpcClient rpcThrowing(final JsonRpcException ex) {
        return new TestEvmJsonRpcClient("http://unused") {
            @Override
            public String ethCall(final String to, final String data, final String blockTag) {
                throw ex;
            }
        };
    }

    /** A stub whose {@code ethCall} returns the given (possibly malformed) hex string. */
    private static EvmJsonRpcClient rpcReturningHex(final String hex) {
        return new TestEvmJsonRpcClient("http://unused") {
            @Override
            public String ethCall(final String to, final String data, final String blockTag) {
                return hex;
            }
        };
    }

    @Test
    void readChannelCount_decodesCountFromWord() {
        final EvmJsonRpcClient stub = TestEvmJsonRpcClient.newStubClient(AbiCodec.encodeUint(7));
        final EvmContractStateReader reader = new EvmContractStateReader(stub, "0xdeadbeef");
        assertThat(reader.readChannelCount("latest")).isEqualTo(7L);
    }

    @Test
    void readChannelCount_shortReturn_throwsIllegalState() {
        // A return shorter than one 32-byte word is malformed — must throw, not fabricate 0.
        final EvmJsonRpcClient stub = TestEvmJsonRpcClient.newStubClient(new byte[16]);
        final EvmContractStateReader reader = new EvmContractStateReader(stub, "0xdeadbeef");
        assertThatThrownBy(() -> reader.readChannelCount("latest"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("malformed data");
    }

    @Test
    void readChannelCount_rpcFailure_propagates() {
        // Unlike the read views, channelCount must NOT swallow the error into a sentinel.
        final EvmContractStateReader reader =
                new EvmContractStateReader(rpcThrowing(new JsonRpcException(-32000, "boom")), "0xdeadbeef");
        assertThatThrownBy(() -> reader.readChannelCount("latest"))
                .isInstanceOf(JsonRpcException.class)
                .hasMessageContaining("boom");
    }

    @Test
    void readPeerEndpointManifest_non32ChannelId_throws() {
        final EvmContractStateReader reader =
                new EvmContractStateReader(TestEvmJsonRpcClient.newStubClient(new byte[0]), "0xdeadbeef");
        assertThatThrownBy(() -> reader.readPeerEndpointManifest(Bytes.wrap(new byte[31]), "latest"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bytes32 value must be exactly 32 bytes");
    }

    @Test
    void readLedgerConfiguration_commitmentLevelOverload_mapsToBlockTag() {
        // Empty return decodes to DEFAULT; the point is exercising the CommitmentLevel overload.
        final EvmContractStateReader reader =
                new EvmContractStateReader(TestEvmJsonRpcClient.newStubClient(new byte[0]), "0xdeadbeef");
        assertThat(reader.readLedgerConfiguration(CommitmentLevel.FINALIZED))
                .isEqualTo(ClprLedgerConfiguration.DEFAULT);
    }

    @Test
    void readLedgerConfiguration_rpcFailure_returnsDefault() {
        final EvmContractStateReader reader =
                new EvmContractStateReader(rpcThrowing(new JsonRpcException(-32000, "down")), "0xdeadbeef");
        assertThat(reader.readLedgerConfiguration("latest")).isEqualTo(ClprLedgerConfiguration.DEFAULT);
    }

    @Test
    void readLedgerConfiguration_malformedHexReturn_returnsDefault() {
        // Odd-length hex makes AbiCodec.fromHex throw — a non-RPC decode failure hits the generic catch.
        final EvmContractStateReader reader = new EvmContractStateReader(rpcReturningHex("0xabc"), "0xdeadbeef");
        assertThat(reader.readLedgerConfiguration("latest")).isEqualTo(ClprLedgerConfiguration.DEFAULT);
    }

    @Test
    void readPeerEndpointManifest_rpcFailure_rethrows() {
        // A PEER manifest read must NOT degrade to the empty DEFAULT sentinel: the result feeds a
        // destructive cache replace, so a transient RPC failure rethrows rather than wiping the roster.
        final EvmContractStateReader reader =
                new EvmContractStateReader(rpcThrowing(new JsonRpcException(-32000, "down")), "0xdeadbeef");
        assertThatThrownBy(() -> reader.readPeerEndpointManifest(Bytes.wrap(new byte[32]), "latest"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("getPeerEndpointManifest read failed");
    }

    @Test
    void readPeerEndpointManifest_malformedHexReturn_rethrows() {
        // Odd-length hex makes AbiCodec.fromHex throw — a decode failure on a PEER read also rethrows.
        final EvmContractStateReader reader = new EvmContractStateReader(rpcReturningHex("0xabc"), "0xdeadbeef");
        assertThatThrownBy(() -> reader.readPeerEndpointManifest(Bytes.wrap(new byte[32]), "latest"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("getPeerEndpointManifest read failed");
    }

    @Test
    void readEndpointManifest_rpcFailure_returnsDefault() {
        final EvmContractStateReader reader =
                new EvmContractStateReader(rpcThrowing(new JsonRpcException(-32000, "down")), "0xdeadbeef");
        assertThat(reader.readEndpointManifest("latest")).isEqualTo(ClprEndpointManifest.DEFAULT);
    }
}
