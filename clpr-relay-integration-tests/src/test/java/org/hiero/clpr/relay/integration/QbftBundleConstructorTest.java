// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.BundleRequest;
import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprMessageValue;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.List;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.qbft.QbftBundleConstructor;
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

        final var clprChannel = minimalChannel(channelId);
        final ContractStateReader stateReader =
                new FixedStateReader(clprChannel, List.of(minimalMessage(messageId, runningHashAfterProcessing)));
        final var qbftProofConstructor =
                new QbftBundleConstructor(accountAddress, 30_000L, 10, 10, rpc, stateReader, CommitmentLevel.LATEST);

        // Ad-hoc construction: ask for the channel's messages from id 1 (the peer's request). createBundle
        // reads the fixture channel/message from the state reader and fetches the proof from Besu at head.
        final var request =
                BundleRequest.newBuilder().currentReceivedMessageId(0L).build();
        final var response = qbftProofConstructor.createBundle(clprChannel.channelId(), request);
        assertThat(response.bundlePayload().length()).isGreaterThan(0);
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
                .nextMessageId(2L) // message 1 has been sent, so createBundle serves it for a from-1 request
                .sentRunningHash(Bytes.wrap(new byte[32]))
                .receivedMessageId(0L)
                .receivedRunningHash(Bytes.wrap(new byte[32]))
                .status(ClprChannelStatus.ACTIVE)
                .build();
    }
}
