// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.chain;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.integration.AnvilTxSubmitter;
import org.hiero.clpr.relay.integration.ContractArtifact;
import org.hiero.clpr.relay.integration.ContractDeployer;
import org.hiero.clpr.relay.integration.DeployedContracts;

/**
 * Deploys the CLPR stack an E2E chain needs.
 *
 * <p>Layered on the existing {@link ContractDeployer} rather than replacing it: that class already
 * handles the fiddly part — the six logic modules, then {@code ClprService} with its nine-argument
 * constructor and the CAIP-2 chain-id string — and duplicating it would guarantee drift the first
 * time the contract constructor changes. What is added here is the E2E-specific set the integration
 * suite has no use for: the real {@code QBFTVerifier} and the {@code E2EApplication}.
 *
 * <p>Per-channel contracts ({@code QbftE2EVerifier} wrappers and {@code MockClprConnector}s) are
 * <b>not</b> deployed here. Each channel gets its own verifier wrapper because
 * {@code setSeedEndpoints} is per-wrapper and has to name that channel's peer endpoint, so they can
 * only be created once the channel topology is known.
 */
public final class E2eContractDeployer {

    /**
     * Number of committed seals the deployed {@code QBFTVerifier} requires.
     *
     * <p>The verifier reverts with
     * {@code MultiValidatorNotSupported()} on any epoch header carrying a different validator count,
     * so this constant and {@link BesuNetworkSpec#validatorCount()} have to agree.
     */
    public static final int REQUIRED_COMMITTED_SEALS = 1;

    /**
     * Gas limit for the small contract creates this class issues (per-chain {@code QBFTVerifier} +
     * {@code E2EApplication}; per-channel {@code QbftE2EVerifier} + {@code MockClprConnector}).
     *
     * <p>All four observed at ~1.1M–2.2M actual gas, so 4M is a comfortable buffer. Kept well
     * below {@link ChainTx#DEFAULT_GAS_LIMIT} on purpose: Besu's EIP-1559 balance check reserves
     * {@code gasLimit × maxFeePerGas} at admission ({@code 12M × 100 gwei = 1.2 ETH} per pending
     * tx against a genesis-funded 8-ETH deployer), and at scale that reservation starves the pool
     * long before the deployer's actual balance runs out — the failing "pending / 0 gas" empty
     * blocks in the many-channel wiring path traced back to exactly this. A right-sized limit here
     * mirrors {@link ChainTx#PLAIN_TRANSFER_GAS_LIMIT}'s reasoning: don't declare gas you can't
     * plausibly spend.
     */
    private static final long DEPLOY_GAS_LIMIT = 4_000_000L;

    private final EvmJsonRpcClient rpc;
    private final AnvilTxSubmitter txSubmitter;

    /**
     * @param rpc JSON-RPC client for the target chain
     * @param txSubmitter client-side EIP-1559 signer for the deploying account
     */
    public E2eContractDeployer(final EvmJsonRpcClient rpc, final AnvilTxSubmitter txSubmitter) {
        this.rpc = rpc;
        this.txSubmitter = txSubmitter;
    }

    /**
     * Deploy the per-chain stack.
     *
     * @param caip2ChainId this chain's CAIP-2 id, stored as the service's own {@code _config.chainId}
     * @return the deployed addresses
     * @throws IllegalStateException on any JSON-RPC or deployment failure, naming the contract
     */
    public E2eContracts deployChainStack(final String caip2ChainId) {
        // The service's own chain id has to be this chain's real id. Both sides of a channel derive
        // the channel id from the canonically-ordered pair of the two chains' ids, so a service that
        // misreports its own id makes the two sides disagree and the second completeChannel reverts
        // with ClprInvalidChannelId.
        final DeployedContracts core;
        try {
            core = new ContractDeployer(rpc, txSubmitter).deploy(caip2ChainId);
        } catch (final Exception e) {
            throw new IllegalStateException("failed to deploy the CLPR core on " + caip2ChainId, e);
        }
        final String qbftVerifier =
                deploy("QBFTVerifier", AbiCodec.encodeUint(REQUIRED_COMMITTED_SEALS), ChainTx.DEFAULT_RECEIPT_TIMEOUT);
        final String application = deploy("E2EApplication", new byte[0], ChainTx.DEFAULT_RECEIPT_TIMEOUT);
        return new E2eContracts(core, qbftVerifier, application);
    }

    /**
     * Deploy a {@code QbftE2EVerifier} wrapping the chain's real verifier.
     *
     * <p>One per channel. The wrapper delegates {@code verifyBundle} and {@code verifyConfig}
     * verbatim to the inner verifier — the cryptography is unchanged — and adds only
     * {@code setSeedEndpoints}, which seeds the peer roster that {@code completeChannel} would
     * otherwise leave empty, leaving the relay with no sync target.
     *
     * @param innerVerifier the chain's {@code QBFTVerifier} address
     * @param receiptTimeout how long to wait for the deploy receipt; wiring at scale can push a
     *     single deploy past the default budget even when nothing is actually stuck
     * @return the wrapper address
     * @throws IllegalStateException on any deployment failure
     */
    public String deployChannelVerifier(final String innerVerifier, final Duration receiptTimeout) {
        return deploy("QbftE2EVerifier", AbiCodec.encodeAddress(innerVerifier), receiptTimeout);
    }

    /**
     * Deploy a {@code MockClprConnector}, the stakeable adapter a channel routes messages through.
     *
     * @param receiptTimeout how long to wait for the deploy receipt; wiring at scale can push a
     *     single deploy past the default budget even when nothing is actually stuck
     * @return the connector contract address
     * @throws IllegalStateException on any deployment failure
     */
    public String deployConnector(final Duration receiptTimeout) {
        return deploy("MockClprConnector", new byte[0], receiptTimeout);
    }

    private String deploy(
            final String contractName, final byte[] abiEncodedConstructorArgs, final Duration receiptTimeout) {
        final ContractArtifact artifact;
        try {
            artifact = ContractArtifact.loadByName(contractName);
        } catch (final java.io.IOException e) {
            throw new IllegalStateException(
                    "could not load the Foundry artifact for " + contractName
                            + " — is ../clpr-smart-contracts built (forge build)?",
                    e);
        }
        final String bytecode =
                artifact.bytecode().startsWith("0x") ? artifact.bytecode().substring(2) : artifact.bytecode();
        final byte[] callData = AbiCodec.fromHex("0x" + bytecode + AbiCodec.toHexNoPrefix(abiEncodedConstructorArgs));
        final String txHash = txSubmitter.sendRawTx(null, callData, 0L, DEPLOY_GAS_LIMIT);
        return awaitContractAddress(contractName, txHash, receiptTimeout);
    }

    private String awaitContractAddress(final String contractName, final String txHash, final Duration receiptTimeout) {
        final JsonNode receipt = ChainTx.awaitReceipt(rpc, txHash, contractName + " deployment", receiptTimeout);
        final JsonNode status = receipt.get("status");
        if (status != null && !status.isNull() && !"0x1".equalsIgnoreCase(status.asText())) {
            // A reverted deployment can still report a contractAddress, so the status has to be
            // checked first or callers would happily use an address with no code at it.
            throw new IllegalStateException(
                    contractName + " deployment reverted (tx " + txHash + "), receipt=" + receipt);
        }
        final JsonNode address = receipt.get("contractAddress");
        if (address == null || address.isNull() || address.asText().isBlank()) {
            throw new IllegalStateException(
                    contractName + " deployment produced no contractAddress (tx " + txHash + "), receipt=" + receipt);
        }
        return address.asText();
    }
}
