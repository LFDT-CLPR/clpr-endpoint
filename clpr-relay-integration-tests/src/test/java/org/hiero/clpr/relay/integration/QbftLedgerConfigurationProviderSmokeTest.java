// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.ClprChannel;
import com.hedera.hapi.node.state.clpr.ClprLedgerConfiguration;
import com.hedera.hapi.node.state.clpr.ClprThrottles;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.hiero.clpr.relay.core.CommitmentLevel;
import org.hiero.clpr.relay.core.ContractStateReader;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.qbft.QbftLedgerConfigurationProvider;
import org.hiero.clpr.relay.evm.testfixtures.TestEvmJsonRpcClient;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Smoke test: verifies that {@link QbftLedgerConfigurationProvider} can assemble a valid
 * {@link com.hedera.hapi.node.state.clpr.QbftLedgerConfigurationPayload} against a live Besu
 * node — real block headers (RLP-encoded), a real EIP-1186 account and storage proof returned by
 * Besu, and a stubbed ledger configuration (no deployed ClprService contract required).
 *
 * <p>The proof target is the hardcoded genesis account from {@code besu-genesis.json}
 * ({@code 0x1234...7890}), which exists in Besu's state tree and therefore yields a non-trivial
 * Merkle proof. Slot 23 ({@code _config.serviceAddress}) is zero in this account because no real
 * contract was deployed — the test guards proof structure and provider wiring, not storage
 * semantics.
 */
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
@Testcontainers
class QbftLedgerConfigurationProviderSmokeTest {

    @Container
    static final BesuContainer BESU = new BesuContainer();

    @Test
    void provides_payload_with_real_block_headers_and_proof() {
        final EvmJsonRpcClient rpc = new TestEvmJsonRpcClient(BESU.jsonRpcUrl());

        // Use the hardcoded genesis account from besu-genesis.json as the proof target.
        // It exists in Besu's state trie and yields a real Merkle proof, even though it is
        // not a deployed ClprService contract.
        final var contractAddress = Address.fromHexString("0x1234567890123456789012345678901234567890");

        final var expectedConfig = ClprLedgerConfiguration.newBuilder()
                .protocolVersion(1)
                .chainId("eip155:1337")
                .serviceAddress(Bytes.wrap(new byte[] {0x12, 0x34}))
                .throttles(ClprThrottles.newBuilder().maxMessagesPerBundle(50).build())
                .build();

        final var provider =
                new QbftLedgerConfigurationProvider(rpc, new StubStateReader(expectedConfig), contractAddress, 30_000L);
        final var response = provider.provide(CommitmentLevel.LATEST);

        assertThat(response.qbft()).isNotNull();
        final var payload = response.qbft();

        // Both block headers must have been fetched and RLP-encoded.
        assertThat(payload.latestEpochBlockHeader()).isNotNull();
        assertThat(payload.latestEpochBlockHeader().rlp().length()).isGreaterThan(0L);

        assertThat(payload.currentBlockHeader()).isNotNull();
        assertThat(payload.currentBlockHeader().rlp().length()).isGreaterThan(0L);

        // The ledger configuration is passed through unchanged from the stub.
        assertThat(payload.ledgerConfiguration()).isEqualTo(expectedConfig);

        // Besu returned a real EIP-1186 proof: account proof path and exactly one storage proof
        // entry for slot 23 (_config.serviceAddress).
        assertThat(payload.clprServiceAccountProof()).isNotEmpty();
        assertThat(payload.clprServiceStorageProofs()).hasSize(1);

        // Proof anchor: keccak256 of the first account proof node must equal the stateRoot
        // declared in the current block header. Both live in the same payload so this check
        // is self-consistent — no second network call needed.
        final byte[] stateRoot =
                stateRootFromHeaderRlp(payload.currentBlockHeader().rlp());
        final byte[] accountRootHash = AbiCodec.Keccak256.keccak256(
                payload.clprServiceAccountProof().get(0).toByteArray());
        assertThat(accountRootHash).isEqualTo(stateRoot);
    }

    /**
     * Extracts the 32-byte {@code stateRoot} from a RLP-encoded Ethereum block header.
     *
     * <p>The Ethereum block header is an RLP list whose fields have fixed widths at the positions
     * that matter here: parentHash (32 B), sha3Uncles (32 B), miner (20 B), stateRoot (32 B).
     * The RLP list header itself is the only variable-width prefix, and its length is encoded in
     * the leading byte(s) per the RLP spec.
     */
    private static byte[] stateRootFromHeaderRlp(final Bytes rlp) {
        final byte[] bytes = rlp.toByteArray();
        // Skip the outer RLP list header. First byte 0xf8 means 1 length byte follows;
        // 0xf9 means 2; in general (firstByte - 0xf7) gives the number of length bytes.
        int pos = 1 + (bytes[0] & 0xff) - 0xf7;
        pos += 1 + 32; // parentHash:  0xa0 prefix + 32 bytes
        pos += 1 + 32; // sha3Uncles:  0xa0 prefix + 32 bytes
        pos += 1 + 20; // miner:       0x94 prefix + 20 bytes
        pos += 1; // stateRoot:   skip 0xa0 prefix
        return Arrays.copyOfRange(bytes, pos, pos + 32);
    }

    private static final class StubStateReader implements ContractStateReader {

        private final ClprLedgerConfiguration config;

        StubStateReader(final ClprLedgerConfiguration config) {
            this.config = config;
        }

        @Override
        @NonNull
        public Optional<ClprChannel> readChannelState(@NonNull final Bytes channelId, @NonNull final String blockTag) {
            throw new UnsupportedOperationException();
        }

        @Override
        @NonNull
        public List<QueuedMessage> readQueuedMessages(
                @NonNull final Bytes channelId, final long fromId, final long toId, @NonNull final String blockTag) {
            throw new UnsupportedOperationException();
        }

        @Override
        @NonNull
        public ClprLedgerConfiguration readLedgerConfiguration(@NonNull final CommitmentLevel commitmentLevel) {
            throw new AssertionError("provider must use the string-tag overload; got " + commitmentLevel);
        }

        @Override
        @NonNull
        public ClprLedgerConfiguration readLedgerConfiguration(@NonNull final String blockTag) {
            return config;
        }
    }
}
