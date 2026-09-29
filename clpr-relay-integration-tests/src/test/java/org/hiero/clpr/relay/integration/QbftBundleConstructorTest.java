// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprMessageValue;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.List;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.evm.QbftBundleConstructor;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.testfixtures.TestEvmJsonRpcClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
@Testcontainers
class QbftBundleConstructorTest {

    @Container
    static final BesuContainer BESU = new BesuContainer();

    @Test
    void constructs_a_valid_bundle_payload_with_proof() {
        final EvmJsonRpcClient rpc = new TestEvmJsonRpcClient(BESU.jsonRpcUrl());

        // This test is quite fragile:
        // besu-genesis.json defines a hardcoded account with a non-empty storage slot
        // at 0xedb38a93e6e2e82dbb40826a878df1d817a37ef13fcaa25248649a90fa47497c
        // which corresponds to running hash field of message 1 of channel 1
        // (_messageQueues at base slot 1; see QbftBundleConstructor.MESSAGE_QUEUES_BASE_SLOT).
        // TODO: we need a test with an actual CLPR Service and Channel data
        //
        // Note on storage-key encoding: this exercises eth_getProof against Besu in --network=dev
        // mode, which leniently left-pads under-width / odd-length storage keys. It therefore does
        // NOT regression-guard EvmSlotUtils.toSlotHex's fixed-width (32-byte) encoding —
        // a strict node would reject a malformed key, but dev-mode Besu silently accepts it. The
        // authoritative guard for that encoding is the unit suite
        // EvmSlotUtilsTest.ToSlotHex in clpr-relay-evm.
        final var accountAddress = Address.fromHexString("0x1234567890123456789012345678901234567890");
        final var channelId = 1L;
        final var messageId = 1l;
        final var runningHashAfterProcessing =
                Bytes.fromHex("2fb88ac06bd872a82e3338db1e6b58c3b517bf8329538dc836b53e69d626c11c");

        final var blockNumber = rpc.ethGetBlockHeaderByNumber("latest").number();
        final var qbftProofConstructor = new QbftBundleConstructor(accountAddress, 30_000L, 10, 10, rpc);
        final var clprChannel = minimalChannel(channelId);
        qbftProofConstructor.onStateChanged(
                blockNumber,
                clprChannel.channelId(),
                clprChannel,
                List.of(minimalMessage(messageId, runningHashAfterProcessing)));
        final var constructedBundle = qbftProofConstructor.getLatestBundlePayload(clprChannel.channelId());
        assertThat(constructedBundle).isNotEmpty();

        // RLP-DIVERGENCE REPRO: decode the REAL proof bytes with a faithful port of OpenZeppelin
        // RLP.sol (the strict decoder the on-chain QBFTVerifier uses). Random-byte synthetics are
        // OZ-valid at every size, so if real Besu block-header / MPT-proof-node bytes trip OZ, this
        // pinpoints it. Diagnostic only: prints the byte offset + reason rather than masking it.
        final byte[] proof = constructedBundle.orElseThrow().toByteArray();
        System.out.printf("[RLP-REPRO] real QBFT proof size=%d bytes%n", proof.length);
        try {
            OzRlp.validate(proof);
            System.out.println("[RLP-REPRO] real proof is OZ-strict VALID");
        } catch (final OzRlp.Invalid e) {
            System.out.printf("[RLP-REPRO] real proof REJECTED by OZ: %s%n", e.getMessage());
            throw e;
        }
    }

    /** Faithful port of OpenZeppelin utils/RLP.sol _decodeLength (strict, exact-consumption, recursive). */
    static final class OzRlp {
        private static final int SHORT_THRESHOLD = 55;
        private static final int SHORT_OFFSET = 0x80;
        private static final int LONG_OFFSET = 0xC0;

        static final class Invalid extends RuntimeException {
            Invalid(final String why, final int pos) {
                super(why + " @pos=" + pos);
            }
        }

        private static void req(final boolean c, final String why, final int pos) {
            if (!c) throw new Invalid(why, pos);
        }

        private static long readBE(final byte[] b, final int off, final int n) {
            long v = 0;
            for (int i = 0; i < n; i++) v = (v << 8) | (b[off + i] & 0xffL);
            return v;
        }

        static void validate(final byte[] b) {
            final int consumed = item(b, 0, b.length);
            req(consumed == b.length, "top item not exactly buffer (" + consumed + "/" + b.length + ")", 0);
        }

        private static int item(final byte[] b, final int start, final int end) {
            final int itemLength = end - start;
            req(itemLength != 0, "empty item", start);
            final int prefix = b[start] & 0xff;
            if (prefix < LONG_OFFSET) {
                if (prefix < SHORT_OFFSET) {
                    return 1;
                } else if (prefix <= SHORT_OFFSET + SHORT_THRESHOLD) {
                    final int strLen = prefix - SHORT_OFFSET;
                    req(
                            itemLength > strLen && (strLen != 1 || (b[start + 1] & 0xff) >= SHORT_OFFSET),
                            "short-str",
                            start);
                    return 1 + strLen;
                } else {
                    final int ll = prefix - SHORT_OFFSET - SHORT_THRESHOLD;
                    req(itemLength > ll, "long-str lenlen", start);
                    req((b[start + 1] & 0xff) != 0x00, "long-str leading-zero", start);
                    final long len = readBE(b, start + 1, ll);
                    req(len > SHORT_THRESHOLD && (long) (itemLength - ll) > len, "long-str len", start);
                    return (int) (1 + ll + len);
                }
            } else {
                if (prefix <= LONG_OFFSET + SHORT_THRESHOLD) {
                    final int listLen = prefix - LONG_OFFSET;
                    req(itemLength > listLen, "short-list", start);
                    listContent(b, start + 1, start + 1 + listLen);
                    return 1 + listLen;
                } else {
                    final int ll = prefix - LONG_OFFSET - SHORT_THRESHOLD;
                    req(itemLength > ll, "long-list lenlen", start);
                    req((b[start + 1] & 0xff) != 0x00, "long-list leading-zero", start);
                    final long len = readBE(b, start + 1, ll);
                    req(len > SHORT_THRESHOLD && (long) (itemLength - ll) > len, "long-list len", start);
                    listContent(b, start + 1 + ll, (int) (start + 1 + ll + len));
                    return (int) (1 + ll + len);
                }
            }
        }

        private static void listContent(final byte[] b, final int cStart, final int cEnd) {
            req(cEnd <= b.length, "list-content exceeds buffer", cStart);
            int pos = cStart;
            while (pos < cEnd) {
                final int consumed = item(b, pos, cEnd);
                req(pos + consumed <= cEnd, "element exceeds list bound", pos);
                pos += consumed;
            }
            req(pos == cEnd, "list not exactly consumed", cStart);
        }
    }

    private static ContractStateReader.QueuedMessage minimalMessage(long id, Bytes runningHashAfterProcessing) {
        final ClprMessagePayload payload = ClprMessagePayload.newBuilder().build();
        final ClprMessageValue value = ClprMessageValue.newBuilder()
                .payload(payload)
                .runningHashAfterProcessing(runningHashAfterProcessing)
                .build();
        return new ContractStateReader.QueuedMessage(BigInteger.valueOf(id), value);
    }

    private static ClprChannel minimalChannel(long channelId) {
        return ClprChannel.newBuilder()
                .channelId(Bytes.wrap(BigInteger.valueOf(channelId).toByteArray()))
                .nextMessageId(1L)
                .sentRunningHash(Bytes.wrap(new byte[32]))
                .receivedMessageId(0L)
                .receivedRunningHash(Bytes.wrap(new byte[32]))
                .status(ClprChannelStatus.ACTIVE)
                .build();
    }
}
