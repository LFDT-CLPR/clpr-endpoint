// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;

/**
 * Deploys the full CLPR smart-contract bundle (ClprService, MockClprVerifier,
 * StubClprVerifier, MockClprApplication, MockClprConnector) to a running EVM node via raw JSON-RPC.
 *
 * <p>ClprService has a simple constructor with no circular dependencies, so contracts are
 * deployed sequentially without any address prediction.
 *
 * <p>Deployment order:
 * <ol>
 *   <li>ClprService(owner, protocolVersion, chainId)</li>
 *   <li>MockClprVerifier()</li>
 *   <li>StubClprVerifier()</li>
 *   <li>MockClprApplication()</li>
 *   <li>MockClprConnector()</li>
 * </ol>
 *
 * <p>Deployment transactions are client-signed as EIP-1559 raw transactions via the supplied
 * {@link AnvilTxSubmitter} and submitted with {@code eth_sendRawTransaction}.
 */
public final class ContractDeployer {

    private static final long DEFAULT_GAS_LIMIT = 0xffffffL;
    private static final int DEPLOYMENT_POLL_ATTEMPTS = 60;
    private static final long DEPLOYMENT_POLL_INTERVAL_MS = 500L;

    /** The 20-byte zero address, passed as {@code ClprService}'s {@code feeToken} to select native fee mode. */
    private static final String ZERO_ADDRESS = "0x0000000000000000000000000000000000000000";

    private final EvmJsonRpcClient rpcClient;
    private final AnvilTxSubmitter txSubmitter;
    private final String deployerAddress;

    /**
     * Create a new deployer.
     *
     * @param rpcClient   the JSON-RPC client bound to the target EVM node, used for receipt polling
     * @param txSubmitter client-side signing helper used to send deployment transactions; the
     *                    deployer address is taken from {@code txSubmitter.address()}
     */
    public ContractDeployer(final EvmJsonRpcClient rpcClient, final AnvilTxSubmitter txSubmitter) {
        this.rpcClient = rpcClient;
        this.txSubmitter = txSubmitter;
        this.deployerAddress = txSubmitter.address();
    }

    /**
     * Deploy the complete set of CLPR contracts and return their addresses.
     *
     * <p>Deployment order:
     * <ol>
     *   <li>ClprService(owner, protocolVersion, chainId)</li>
     *   <li>MockClprVerifier()</li>
     *   <li>StubClprVerifier()</li>
     *   <li>MockClprApplication()</li>
     *   <li>MockClprConnector()</li>
     * </ol>
     *
     * @return the addresses of all deployed contracts
     * @throws Exception on any JSON-RPC or deployment failure
     */
    public DeployedContracts deploy() throws Exception {
        return deploy(DEFAULT_CHAIN_ID);
    }

    /**
     * Default CAIP-2 chain id baked into the deployed {@code ClprService}. Matches the Anvil chain the
     * integration suite runs both sides of a channel against.
     */
    public static final String DEFAULT_CHAIN_ID = "eip155:1337";

    /**
     * Deploy the complete set of CLPR contracts with an explicit local chain id.
     *
     * <p>The chain id is not cosmetic: {@code ClprService} stores it as its own {@code _config.chainId}
     * and derives every channel id from the canonically-ordered pair of the two chains' ids. Two
     * services that both claim the same chain id therefore sort the pair differently and derive
     * different channel ids for the same operator and salt — which surfaces as
     * {@code ClprInvalidChannelId} on the second {@code completeChannel}. Deployments spanning
     * genuinely distinct chains must pass each chain's real id.
     *
     * @param caip2ChainId the CAIP-2 id of the chain being deployed to, e.g. {@code eip155:31337}
     * @return the addresses of all deployed contracts
     * @throws Exception on any JSON-RPC or deployment failure
     */
    public DeployedContracts deploy(final String caip2ChainId) throws Exception {
        // Load artifacts.
        final ContractArtifact service = ContractArtifact.loadByName("ClprService");
        final ContractArtifact verifier = ContractArtifact.loadByName("MockClprVerifier");
        final ContractArtifact stubVerifier = ContractArtifact.loadByName("StubClprVerifier");
        final ContractArtifact app = ContractArtifact.loadByName("MockClprApplication");
        final ContractArtifact connector = ContractArtifact.loadByName("MockClprConnector");
        final ContractArtifact channelLogic = ContractArtifact.loadByName("ChannelLogic");
        final ContractArtifact messagingLogic = ContractArtifact.loadByName("MessagingLogic");
        final ContractArtifact bundleLogic = ContractArtifact.loadByName("BundleLogic");
        final ContractArtifact connectorLogic = ContractArtifact.loadByName("ConnectorLogic");
        final ContractArtifact adminLogic = ContractArtifact.loadByName("AdminLogic");
        final ContractArtifact bundleDecodeHelper = ContractArtifact.loadByName("BundleDecodeHelper");

        // Pre-deploy the six logic / helper contracts (all have zero-arg constructors).
        // ClprService delegates to these via DELEGATECALL at runtime and verifies in its
        // constructor that each address has non-empty code; the inline-deploy variant was
        // removed in clpr-smart-contracts PR #27/#35 because the combined initcode exceeded
        // the EIP-3860 49,152-byte cap on Shanghai+ EVMs. BundleLogic was split out of
        // MessagingLogic in clpr-smart-contracts PR #52 — submitBundle now delegates to it —
        // adding a sixth module and a ninth constructor parameter. Order mirrors DeployCore.sol.
        final String channelLogicAddr = deployContract(channelLogic.bytecode(), new byte[0]);
        final String messagingLogicAddr = deployContract(messagingLogic.bytecode(), new byte[0]);
        final String bundleLogicAddr = deployContract(bundleLogic.bytecode(), new byte[0]);
        final String connectorLogicAddr = deployContract(connectorLogic.bytecode(), new byte[0]);
        final String adminLogicAddr = deployContract(adminLogic.bytecode(), new byte[0]);
        final String bundleDecodeHelperAddr = deployContract(bundleDecodeHelper.bytecode(), new byte[0]);

        // Deploy ClprService(address owner, uint32 protocolVersion, string chainId,
        //                    address channelLogic, address messagingLogic, address bundleLogic,
        //                    address connectorLogic, address adminLogic, address bundleDecodeHelper,
        //                    address feeToken).
        // ABI encoding head layout (10 slots × 32 bytes = 320 bytes):
        //   [0] owner (address, inline)
        //   [1] protocolVersion (uint32, inline)
        //   [2] offset to chainId string = 320
        //   [3] channelLogic (address, inline)
        //   [4] messagingLogic (address, inline)
        //   [5] bundleLogic (address, inline)
        //   [6] connectorLogic (address, inline)
        //   [7] adminLogic (address, inline)
        //   [8] bundleDecodeHelper (address, inline)
        //   [9] feeToken (address, inline) = address(0) → native fee mode
        //   tail: length of chainId bytes, then padded chainId bytes
        // feeToken is a tenth constructor parameter added in clpr-smart-contracts. A non-zero value
        // makes the constructor probe it as an ERC-20 (SafeERC20.tryGetDecimals) and revert with
        // ClprInvalidFeeToken on a codeless address, so the head MUST carry the zero-address slot — and
        // the chainId offset MUST therefore be 320, not 288 — or the whole service deploy reverts.
        // address(0) ("native value") matches the contracts' own ClprDeployHelper default.
        // Must match every test-fixture peerChainId (verifier mock + channel commitments) so channelId
        // derivation stays consistent across both sides, and so GetLedgerConfigurationTest's chainId
        // assertion matches the deployed _config.chainId.
        final byte[] chainIdBytes = caip2ChainId.getBytes(StandardCharsets.UTF_8);
        final byte[] serviceArgs = concat(
                AbiCodec.encodeAddress(deployerAddress),
                AbiCodec.encodeUint(1L),
                AbiCodec.encodeUint(320L),
                AbiCodec.encodeAddress(channelLogicAddr),
                AbiCodec.encodeAddress(messagingLogicAddr),
                AbiCodec.encodeAddress(bundleLogicAddr),
                AbiCodec.encodeAddress(connectorLogicAddr),
                AbiCodec.encodeAddress(adminLogicAddr),
                AbiCodec.encodeAddress(bundleDecodeHelperAddr),
                AbiCodec.encodeAddress(ZERO_ADDRESS),
                AbiCodec.encodeUint(chainIdBytes.length),
                AbiCodec.padRight32(chainIdBytes));
        final String serviceAddr = deployContract(service.bytecode(), serviceArgs);

        // Deploy the mocks (no constructor args).
        final String verifierAddr = deployContract(verifier.bytecode(), new byte[0]);
        final String stubVerifierAddr = deployContract(stubVerifier.bytecode(), new byte[0]);
        final String appAddr = deployContract(app.bytecode(), new byte[0]);
        final String connectorAddr = deployContract(connector.bytecode(), new byte[0]);

        return new DeployedContracts(serviceAddr, verifierAddr, stubVerifierAddr, appAddr, connectorAddr);
    }

    /**
     * Deploy a single contract. The {@code to} field is omitted and the {@code data} is the
     * deployment bytecode concatenated with the ABI-encoded constructor args.
     *
     * @param bytecodeHex    the deployment bytecode as a hex string (with or without {@code 0x})
     * @param constructorArgs the already-ABI-encoded constructor args (may be empty)
     * @return the deployed contract address
     * @throws Exception if the receipt cannot be fetched or does not contain a contract address
     */
    private String deployContract(final String bytecodeHex, final byte[] constructorArgs) throws Exception {
        final String bc = bytecodeHex.startsWith("0x") ? bytecodeHex.substring(2) : bytecodeHex;
        final byte[] callData = AbiCodec.fromHex("0x" + bc + AbiCodec.toHexNoPrefix(constructorArgs));

        // to=null → contract-creation transaction
        final String txHash = txSubmitter.sendRawTx(null, callData, 0L, DEFAULT_GAS_LIMIT);
        return waitForContractAddress(txHash);
    }

    private String waitForContractAddress(final String txHash) throws InterruptedException {
        for (int i = 0; i < DEPLOYMENT_POLL_ATTEMPTS; i++) {
            final JsonNode receipt = rpcClient.ethGetTransactionReceipt(txHash);
            if (receipt != null && !receipt.isNull()) {
                // Fail fast on revert: some clients still surface contractAddress on failed
                // deployments, so callers shouldn't trust the address until status is 0x1.
                final JsonNode statusNode = receipt.get("status");
                if (statusNode != null && !statusNode.isNull()) {
                    final String status = statusNode.asText();
                    if ("0x0".equalsIgnoreCase(status) || "0".equals(status)) {
                        throw new IllegalStateException(
                                "Contract deployment failed (reverted) for tx " + txHash + " — receipt=" + receipt);
                    }
                }
                final JsonNode addrNode = receipt.get("contractAddress");
                if (addrNode != null && !addrNode.isNull()) {
                    final String contractAddress = addrNode.asText();
                    if (!contractAddress.isBlank()) {
                        return contractAddress;
                    }
                }
            }
            Thread.sleep(DEPLOYMENT_POLL_INTERVAL_MS);
        }
        throw new IllegalStateException("Contract deployment receipt not found for tx " + txHash);
    }

    private static byte[] concat(final byte[]... parts) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (final byte[] p : parts) {
            out.writeBytes(p);
        }
        return out.toByteArray();
    }
}
