// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.state.clpr.ClprGetLedgerConfigurationRequest;
import com.hedera.hapi.node.state.clpr.QbftLedgerConfigurationPayload;
import java.time.Duration;
import java.util.Arrays;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.grpc.client.testfixtures.ClprEndpointInfoClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * End-to-end integration test for the {@code getLedgerConfiguration} gRPC RPC.
 *
 * <p>Exercises the full stack: gRPC client → Helidon server → {@code GetLedgerConfigurationHandler}
 * → {@code QbftLedgerConfigurationProvider} → Anvil JSON-RPC ({@code eth_getBlockByNumber},
 * {@code eth_getProof}).
 *
 * <p>The positive baseline ({@link #getLedgerConfiguration_returnsValidPayloadMatchingOnChainState})
 * verifies structural invariants that a broken provider wiring would violate: both block headers must
 * be RLP-encoded, the ledger configuration must reflect what was configured on-chain, and the
 * EIP-1186 account proof must anchor to the current block's state root.
 *
 * <p>Negative angle: the {@code serviceAddress} field in the response is asserted to match the
 * deployed contract address. A relay returning a stale or misconfigured payload would fail this
 * check — the assertion is not trivially true because the provider reads the address from contract
 * storage and must decode the Solidity short-bytes layout correctly.
 */
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = ".*")
class GetLedgerConfigurationTest extends OneSidedStubPeerTestBase {

    @Test
    void getLedgerConfiguration_returnsValidPayloadMatchingOnChainState() throws Exception {
        // Generous timeout: cold-start Helidon + two Anvil round-trips (eth_getBlockByNumber ×2 +
        // eth_getProof) can exceed the production default on slow CI runners.
        final QbftLedgerConfigurationPayload payload;
        try (var client = new ClprEndpointInfoClient(Duration.ofSeconds(60))) {
            payload = client.getLedgerConfiguration(
                    "127.0.0.1",
                    relay.grpcPort(),
                    ClprGetLedgerConfigurationRequest.newBuilder().build());
        }

        // Both block headers must be present and RLP-encoded.
        assertThat(payload.latestEpochBlockHeader())
                .as("latest epoch block header must be present")
                .isNotNull();
        assertThat(payload.latestEpochBlockHeader().rlp().length())
                .as("latest epoch block RLP must be non-empty")
                .isGreaterThan(0L);
        assertThat(payload.currentBlockHeader())
                .as("current block header must be present")
                .isNotNull();
        assertThat(payload.currentBlockHeader().rlp().length())
                .as("current block RLP must be non-empty")
                .isGreaterThan(0L);

        // Ledger configuration must reflect what was configured on-chain by IntegrationTestBase.
        final var ledgerConfig = payload.ledgerConfiguration();
        assertThat(ledgerConfig.protocolVersion())
                .as("protocol version must be 1")
                .isEqualTo(1);
        assertThat(ledgerConfig.chainId())
                .as("chain ID must match Anvil's chain ID")
                .isEqualTo("eip155:1337");

        // Service address in the payload must match the deployed ClprService contract address.
        // The provider reads this from contract storage and decodes Solidity's short-bytes layout;
        // a wrong decode (e.g. off-by-one, missing length byte) would produce a different value.
        final byte[] expectedServiceAddress = AbiCodec.fromHex(contracts.clprServiceAddress());
        assertThat(ledgerConfig.serviceAddress().toByteArray())
                .as("service address must match the deployed ClprService contract")
                .isEqualTo(expectedServiceAddress);

        // EIP-1186 account proof must be present and non-empty.
        assertThat(payload.clprServiceAccountProof())
                .as("EIP-1186 account proof must be present")
                .isNotEmpty();

        // At least one storage proof entry must be present (for _config.serviceAddress at slot 23).
        assertThat(payload.clprServiceStorageProofs())
                .as("storage proof for _config.serviceAddress must be present")
                .hasSizeGreaterThanOrEqualTo(1);

        // Proof anchor: the first account proof node, when hashed with keccak256, must equal the
        // state root declared in the current block header. This is a self-consistent check —
        // both values come from the same payload, so a provider that forgets to pin both reads
        // to the same block would produce mismatched values here.
        final byte[] stateRoot =
                stateRootFromHeaderRlp(payload.currentBlockHeader().rlp().toByteArray());
        final byte[] accountRootHash = AbiCodec.Keccak256.keccak256(
                payload.clprServiceAccountProof().get(0).toByteArray());
        assertThat(accountRootHash)
                .as("keccak256 of the first account proof node must equal the current block's state root")
                .isEqualTo(stateRoot);
    }

    /**
     * Selecting the deployed ClprService by {@code service_address} returns that deployment's
     * configuration — the same payload the empty (primary) request yields in this single-deployment
     * setup. Exercises the {@code service_address} routing added to the request.
     */
    @Test
    void getLedgerConfiguration_withServiceAddressSelector_returnsMatchingConfiguration() throws Exception {
        final byte[] serviceAddress = AbiCodec.fromHex(contracts.clprServiceAddress());
        final QbftLedgerConfigurationPayload payload;
        try (var client = new ClprEndpointInfoClient(Duration.ofSeconds(60))) {
            payload = client.getLedgerConfiguration(
                    "127.0.0.1",
                    relay.grpcPort(),
                    ClprGetLedgerConfigurationRequest.newBuilder()
                            .serviceAddress(com.hedera.pbj.runtime.io.buffer.Bytes.wrap(serviceAddress))
                            .build());
        }

        assertThat(payload.ledgerConfiguration().serviceAddress().toByteArray())
                .as("service address in the routed response must match the requested deployment")
                .isEqualTo(serviceAddress);
        assertThat(payload.ledgerConfiguration().chainId())
                .as("chain ID must match Anvil's chain ID")
                .isEqualTo("eip155:1337");
    }

    /**
     * Extracts the 32-byte {@code stateRoot} from a RLP-encoded Ethereum block header.
     * See {@code QbftLedgerConfigurationProviderSmokeTest} for field layout details.
     */
    private static byte[] stateRootFromHeaderRlp(final byte[] bytes) {
        int pos = 1 + (bytes[0] & 0xff) - 0xf7;
        pos += 1 + 32; // parentHash
        pos += 1 + 32; // sha3Uncles
        pos += 1 + 20; // miner
        pos += 1; // skip stateRoot 0xa0 prefix
        return Arrays.copyOfRange(bytes, pos, pos + 32);
    }
}
