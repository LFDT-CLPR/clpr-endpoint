// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.chain;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
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
     * Gas limit for the contract creates this class issues (per-chain {@code QBFTVerifier} +
     * {@code E2EApplication}; per-channel {@code QbftE2EVerifier} + {@code MockClprConnector}).
     *
     * <p>{@code QBFTVerifier}'s constructor spawns a nested {@code new QbftContractModeVerifier()}
     * inline, so its deploy is effectively two contracts stacked into one transaction. Runtime
     * code-deposit alone is ~6.67M gas (outer 16545 bytes + inner 16799 bytes, each at 200 gas/byte),
     * plus constructor execution and CREATE overhead observed at roughly 1M–2M — so the true gas
     * floor is ~8M. 10M gives a ~25% buffer.
     *
     * <p>The ceiling is coupled to the deployer's balance and the submitter's fee cap and can't
     * move alone. Besu's EIP-1559 admission check reserves {@code gasLimit × maxFeePerGas} per
     * pending tx against the sender's balance at pool-entry, so the invariant is:
     * {@code DEPLOY_GAS_LIMIT × Eip1559TxSubmitter.MAX_FEE_PER_GAS × N ≤
     * BesuQbftGenesis.DEFAULT_BALANCE_WEI}, where {@code N} is the largest number of concurrently
     * pending deploys any workload holds against a single deployer account. At the current values
     * ({@code 10M × 100 gwei = 1 ETH} against 8 ETH) that ceiling is ~8. Workloads that wire many
     * channels in parallel are the ones that approach the bound; when it is exceeded the pool
     * wedges as "1 pending / 0 gas" empty blocks with a stuck tx.
     *
     * <p>If {@code QBFTVerifier} grows again and this ceiling has to rise, raise
     * {@code BesuQbftGenesis.DEFAULT_BALANCE_WEI} in the same change — or drop
     * {@code Eip1559TxSubmitter.MAX_FEE_PER_GAS} (100 gwei today, well above the ~2 gwei this
     * dev chain settles at) — or lower the largest workload's concurrent-pending-deploy count.
     * Do not push this constant upward alone.
     */
    private static final long DEPLOY_GAS_LIMIT = 10_000_000L;

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

    /**
     * Deploy {@code QbftContractModeVerifier} — the on-chain verifier for QBFT "contract"
     * validator-selection mode. Stateless (pure functions only), so it takes no constructor
     * arguments.
     *
     * @param receiptTimeout how long to wait for the deploy receipt
     * @return the verifier contract address
     * @throws IllegalStateException on any deployment failure
     */
    public String deployQbftContractModeVerifier(final Duration receiptTimeout) {
        return deploy("QbftContractModeVerifier", new byte[0], receiptTimeout);
    }

    /**
     * Deploy the E2E {@link org.hiero.clpr.relay.evm.StandardValidatorContractProofSource}-compatible
     * validator contract: a plain {@code address[]} state variable at slot 0, seeded with
     * {@code initialValidators}. Not part of the {@code clpr-smart-contracts} artifact set — it is a
     * self-contained fixture, precompiled to
     * {@code src/testFixtures/resources/StandardValidatorContract.bin} (see that file's sibling
     * {@code .sol} for the regeneration command) rather than built on demand.
     *
     * @param initialValidators hex addresses of the initial validator set (typically the same
     *     address(es) that actually seal this chain's blocks, so
     *     {@code QbftContractModeVerifier}'s committed-seal quorum check has something real to
     *     check against)
     * @param receiptTimeout how long to wait for the deploy receipt
     * @return the validator contract address
     * @throws IllegalStateException on any deployment failure
     */
    public String deployStandardValidatorContract(final List<String> initialValidators, final Duration receiptTimeout) {
        final String bytecode = loadFixtureBytecode("StandardValidatorContract");
        final byte[] constructorArgs = encodeAddressArrayArg(initialValidators);
        return sendCreateAndAwait("StandardValidatorContract", bytecode, constructorArgs, receiptTimeout);
    }

    /** ABI-encodes a constructor's sole {@code address[]} parameter: offset, length, then each word. */
    private static byte[] encodeAddressArrayArg(final List<String> addresses) {
        final byte[] offset = AbiCodec.encodeUint256(BigInteger.valueOf(32));
        final byte[] length = AbiCodec.encodeUint256(BigInteger.valueOf(addresses.size()));
        final byte[][] words = new byte[addresses.size()][];
        for (int i = 0; i < addresses.size(); i++) {
            words[i] = AbiCodec.encodeAddress(addresses.get(i));
        }
        return AbiCodec.concat(AbiCodec.concat(offset, length), AbiCodec.concat(words));
    }

    /**
     * Loads a fixture's compiled creation bytecode (no {@code 0x} prefix) from this module's own
     * {@code testFixtures} classpath resources — e.g. {@code StandardValidatorContract.bin}.
     */
    private static String loadFixtureBytecode(final String name) {
        try (var in = E2eContractDeployer.class.getResourceAsStream("/" + name + ".bin")) {
            if (in == null) {
                throw new IllegalStateException(name + ".bin not found on the test-fixtures classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8).strip();
        } catch (final IOException e) {
            throw new IllegalStateException("failed to read " + name + ".bin", e);
        }
    }

    private String deploy(
            final String contractName, final byte[] abiEncodedConstructorArgs, final Duration receiptTimeout) {
        final ContractArtifact artifact;
        try {
            artifact = ContractArtifact.loadByName(contractName);
        } catch (final IOException e) {
            throw new IllegalStateException(
                    "could not load the Foundry artifact for " + contractName
                            + " — is ../clpr-smart-contracts built (forge build)?",
                    e);
        }
        final String bytecode =
                artifact.bytecode().startsWith("0x") ? artifact.bytecode().substring(2) : artifact.bytecode();
        return sendCreateAndAwait(contractName, bytecode, abiEncodedConstructorArgs, receiptTimeout);
    }

    /** Assembles a contract creation transaction from creation bytecode and ABI-encoded constructor args, sends it, and awaits its address. */
    private String sendCreateAndAwait(
            final String contractName,
            final String bytecodeHexNoPrefix,
            final byte[] abiEncodedConstructorArgs,
            final Duration receiptTimeout) {
        final byte[] callData =
                AbiCodec.fromHex("0x" + bytecodeHexNoPrefix + AbiCodec.toHexNoPrefix(abiEncodedConstructorArgs));
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
