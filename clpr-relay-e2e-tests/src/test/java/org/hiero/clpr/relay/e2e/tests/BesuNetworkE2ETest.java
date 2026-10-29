// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import org.hiero.clpr.relay.e2e.E2EEnvironment;
import org.hiero.clpr.relay.e2e.E2ETest;
import org.hiero.clpr.relay.e2e.chain.BesuNetwork;
import org.hiero.clpr.relay.e2e.chain.BesuNetworkSpec;
import org.hiero.clpr.relay.e2e.chain.BesuQbftGenesis;
import org.hiero.clpr.relay.e2e.chain.TestAccount;

/**
 * Validates the chain layer of the E2E framework before anything is layered on top: that a generated
 * QBFT genesis actually produces a mining network, that a multi-node network forms, and that the
 * prefunded account pool is really funded.
 *
 * <p>Deliberately asserts on block production rather than on RPC reachability. A QBFT network with a
 * malformed validator set or {@code extraData} serves JSON-RPC perfectly happily and simply never
 * mines, so "the port answers" would let a broken genesis through to the first contract deployment,
 * where it surfaces as an unrelated timeout.
 */
class BesuNetworkE2ETest {

    @E2ETest(timeoutSeconds = 300)
    void single_node_network_mines_blocks_and_funds_the_account_pool(final E2EEnvironment env) {
        final BesuNetwork besu = env.besuNetworks().add(BesuNetworkSpec.singleNode());

        env.startAll();

        assertThat(besu.chainId()).isEqualTo(BesuNetworkSpec.FIRST_CHAIN_ID);
        assertThat(besu.caip2()).isEqualTo("eip155:31337");
        assertThat(besu.nodes()).hasSize(1);
        assertThat(besu.primaryNode().isValidator()).isTrue();

        // Mining, not merely reachable: startAll() already waited for height >= 1, so assert the
        // head keeps advancing rather than re-checking the same condition.
        final long first = besu.inspect().headBlockNumber();
        assertThat(first).isGreaterThanOrEqualTo(1L);

        final TestAccount funded = env.accounts().allocate("smoke-test balance check");
        final BigInteger balance = besu.rpc().ethGetBalance(funded.address(), "latest");
        // Asserted against the constant, not a literal: the balance is deliberately capped below
        // Long.MAX_VALUE wei so a single transfer can sweep it, and duplicating the number here is
        // what let an earlier change to it slip past this test.
        assertThat(balance)
                .as("every account in the pool is funded by the genesis alloc")
                .isEqualTo(BesuQbftGenesis.DEFAULT_BALANCE_WEI);

        // The validator address is what the CLPR config proof's trust anchor is built from, so it
        // has to be the account that actually signs blocks.
        assertThat(besu.validatorAddress())
                .isEqualTo(besu.primaryNode().nodeKey().address());
    }

    /**
     * The restart-resume promise the chain layer is built on: {@code stop()} is a restart point, not
     * a reset.
     *
     * <p>Testcontainers destroys the container on {@code stop()}, so this only holds because the
     * node's chain data lives in a Docker volume that outlives it. Without that, a restarted node
     * would come back at genesis and every node-restart test above it would pass for the wrong
     * reason — the chain would look healthy because it was freshly born, not because it recovered.
     */
    @E2ETest(timeoutSeconds = 420)
    void a_stopped_node_resumes_its_chain_rather_than_resetting_to_genesis(final E2EEnvironment env) {
        final BesuNetwork besu = env.besuNetworks().add(BesuNetworkSpec.singleNode());

        // Chains only: contracts and endpoints would add minutes and prove nothing about this claim.
        env.besuNetworks().startAll();

        final long before = besu.inspect().headBlockNumber();
        assertThat(before).isGreaterThanOrEqualTo(1L);

        besu.stop();
        besu.start();

        assertThat(besu.inspect().headBlockNumber())
                .as("the restarted node resumed the existing chain instead of starting a new one")
                .isGreaterThanOrEqualTo(before);
    }

    @E2ETest(timeoutSeconds = 420)
    void two_independent_networks_get_distinct_chain_ids(final E2EEnvironment env) {
        final BesuNetwork besuA = env.besuNetworks().add(BesuNetworkSpec.singleNode());
        final BesuNetwork besuB = env.besuNetworks().add(BesuNetworkSpec.singleNode());

        env.startAll();

        // Distinct chain ids are a correctness requirement, not a convenience: a channel id is
        // derived from both chains' CAIP-2 ids, and the relay resolves a peer's proof type by chain
        // id. Two chains sharing an id makes channel identity ambiguous.
        assertThat(besuA.chainId()).isNotEqualTo(besuB.chainId());
        assertThat(besuA.caip2()).isEqualTo("eip155:31337");
        assertThat(besuB.caip2()).isEqualTo("eip155:31338");

        assertThat(besuA.inspect().headBlockNumber()).isGreaterThanOrEqualTo(1L);
        assertThat(besuB.inspect().headBlockNumber()).isGreaterThanOrEqualTo(1L);

        // Separate chains, separate validators — a shared node key would mean one chain could sign
        // the other's blocks.
        assertThat(besuA.validatorAddress()).isNotEqualTo(besuB.validatorAddress());
    }

    @E2ETest(timeoutSeconds = 420)
    void multi_node_network_uses_one_validator_and_full_nodes(final E2EEnvironment env) {
        // Three containers, one validator. Not a simplification: QBFTVerifier reverts with
        // MultiValidatorNotSupported() on a multi-validator epoch header, so a second validator
        // would make every cross-chain bundle unverifiable. Full nodes are how a network becomes
        // genuinely multi-node while staying verifiable.
        final BesuNetwork besu = env.besuNetworks().add(3);

        env.startAll();

        assertThat(besu.nodes()).hasSize(3);
        assertThat(besu.nodes().stream().filter(n -> n.isValidator()).count()).isEqualTo(1L);
        assertThat(besu.validator(0).isValidator()).isTrue();
        assertThat(besu.fullNode(0).isValidator()).isFalse();
        assertThat(besu.fullNode(1).isValidator()).isFalse();

        // Every node has its own pinned address, which is what lets static-nodes.json be written
        // before any container starts.
        assertThat(besu.nodes().stream().map(n -> n.ipAddress()).distinct()).hasSize(3);

        assertThat(besu.inspect().headBlockNumber()).isGreaterThanOrEqualTo(1L);
    }
}
