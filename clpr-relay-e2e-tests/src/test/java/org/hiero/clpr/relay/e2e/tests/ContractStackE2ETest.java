// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import org.hiero.clpr.relay.e2e.E2EEnvironment;
import org.hiero.clpr.relay.e2e.E2ETest;
import org.hiero.clpr.relay.e2e.chain.BesuNetwork;
import org.hiero.clpr.relay.e2e.chain.BesuNetworkSpec;
import org.hiero.clpr.relay.e2e.chain.ChainTx;
import org.hiero.clpr.relay.e2e.chain.E2eContracts;

/**
 * Validates that the CLPR contract stack — including the <b>real</b> {@code QBFTVerifier} — deploys
 * and bootstraps on a generated Besu QBFT chain.
 *
 * <p>This is the step where the E2E path diverges from the existing integration suite. That suite
 * deploys {@code StubClprVerifier} and injects a re-encoding submitter into the in-process relay so
 * the stub accepts its proofs. A containerized endpoint has no such seam, so from here on the
 * production verifier has to be satisfied for real. Proving it deploys and that the service accepts
 * mutating calls afterwards de-risks everything built on top.
 */
class ContractStackE2ETest {

    @E2ETest(timeoutSeconds = 420)
    void deploys_the_real_verifier_stack_and_enables_the_service(final E2EEnvironment env) {
        final BesuNetwork besu = env.besuNetworks().add(BesuNetworkSpec.singleNode());

        env.startAll();

        final E2eContracts contracts = besu.contracts();

        // Every address must actually carry code. A reverted deployment can still report a
        // contractAddress in its receipt, so "we got an address back" is not evidence of anything.
        assertThat(besu.rawRpc().ethGetCode(contracts.clprService()))
                .as("ClprService bytecode")
                .isNotEqualTo("0x");
        assertThat(besu.rawRpc().ethGetCode(contracts.qbftVerifier()))
                .as("real QBFTVerifier bytecode")
                .isNotEqualTo("0x");
        assertThat(besu.rawRpc().ethGetCode(contracts.e2eApplication()))
                .as("E2EApplication bytecode")
                .isNotEqualTo("0x");

        // A per-channel verifier wrapper and connector deploy on demand — this is what channel
        // wiring will do once per channel.
        final String channelVerifier =
                besu.deployer().deployChannelVerifier(contracts.qbftVerifier(), ChainTx.DEFAULT_RECEIPT_TIMEOUT);
        assertThat(besu.rawRpc().ethGetCode(channelVerifier))
                .as("QbftE2EVerifier wrapper bytecode")
                .isNotEqualTo("0x");

        // The service starts disabled and every mutating call reverts with ClprDisabled() until the
        // owner enables it. startAll() applied the bootstrap, so registerEndpoint must now succeed —
        // that is the observable proof the service is initialized AND enabled.
        final BigInteger minBond = BigInteger.valueOf(10_000_000_000_000_000L); // 0.01 ether
        besu.interactor().registerEndpoint(minBond);
        assertThat(besu.interactor().isEndpointRegistered(besu.deployerAccount().address()))
                .as("the deployer account is a registered endpoint after bootstrap")
                .isTrue();

        // No transaction in the whole setup may have reverted. Deployment and configuration failures
        // are otherwise easy to miss: a reverted tx is mined and charged like any other.
        assertThat(besu.inspect().revertedTransactions(0, besu.inspect().headBlockNumber()))
                .as("contract deployment and bootstrap must not revert")
                .isEmpty();
    }

    @E2ETest(timeoutSeconds = 600)
    void both_chains_get_independent_service_deployments(final E2EEnvironment env) {
        final BesuNetwork besuA = env.besuNetworks().add(BesuNetworkSpec.singleNode());
        final BesuNetwork besuB = env.besuNetworks().add(BesuNetworkSpec.singleNode());

        env.startAll();

        // Independent deployments are what makes the two sides of a channel genuinely separate
        // ledgers rather than two views of one. The integration suite runs both sides against a
        // single Anvil instance; here each chain is its own network with its own service.
        assertThat(besuA.contracts().clprService())
                .isNotEqualTo(besuB.contracts().clprService());
        assertThat(besuA.contracts().qbftVerifier())
                .isNotEqualTo(besuB.contracts().qbftVerifier());

        assertThat(besuA.rawRpc().ethGetCode(besuA.contracts().clprService())).isNotEqualTo("0x");
        assertThat(besuB.rawRpc().ethGetCode(besuB.contracts().clprService())).isNotEqualTo("0x");

        // Cross-check: neither service exists on the other chain. If both networks accidentally
        // shared an RPC target, these would both resolve — and every later channel assertion would
        // be meaningless.
        assertThat(besuB.rawRpc().ethGetCode(besuA.contracts().clprService()))
                .as("chain A's service must not exist on chain B")
                .isEqualTo("0x");

        for (final BesuNetwork network : env.besuNetworks().all()) {
            assertThat(network.inspect()
                            .revertedTransactions(0, network.inspect().headBlockNumber()))
                    .as("no reverted transactions on " + network.id())
                    .isEmpty();
        }
    }
}
