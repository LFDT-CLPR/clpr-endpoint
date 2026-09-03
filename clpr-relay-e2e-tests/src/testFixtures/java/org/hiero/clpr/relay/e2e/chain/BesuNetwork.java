// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.chain;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.EthSigner;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.testfixtures.TestEvmJsonRpcClient;
import org.hiero.clpr.relay.integration.AnvilTxSubmitter;
import org.hiero.clpr.relay.integration.ContractInteractor;
import org.hiero.clpr.relay.integration.TestConditions;
import org.jspecify.annotations.Nullable;
import org.testcontainers.containers.Network;

/**
 * One Besu QBFT chain: its containers, its genesis-derived identity, and the JSON-RPC surface the
 * test drives it through.
 *
 * <p>Declared via {@link BesuNetworks#add}, brought up by {@link #start()} (normally through
 * {@code E2EEnvironment.startAll()}). Nothing is created until then, because the genesis has to
 * embed the validator set and prefunded accounts and the static-nodes file has to embed every peer's
 * pinned address — all of which is only complete once the topology has been fully declared.
 */
public final class BesuNetwork {

    /**
     * First host index inside the environment's /24 used for Besu nodes. Endpoints occupy the low
     * indices; starting chains at 40 leaves both room to grow without either renumbering the other.
     * Renumbering matters because endpoint addresses are published in on-chain rosters.
     */
    private static final int NODE_HOST_BASE = 40;

    /** Host indices reserved per network, bounding how many nodes one network may have. */
    private static final int NODE_HOSTS_PER_NETWORK = 20;

    private final BesuNetworkSpec spec;
    private final int networkIndex;
    private final Accounts accounts;
    private final Network dockerNetwork;
    private final IntFunction<String> hostAddresses;
    private final Path workDir;
    private final int heapMb;

    private final List<BesuNode> nodes = new ArrayList<>();
    private final TestAccount deployerAccount;
    private final BesuQbftGenesis genesis;

    @Nullable
    private EvmJsonRpcClient rpc;

    @Nullable
    private RawJsonRpc rawRpc;

    @Nullable
    private ChainInspector inspector;

    @Nullable
    private E2eContracts contracts;

    @Nullable
    private E2eContractDeployer deployer;

    @Nullable
    private AnvilTxSubmitter deployerSubmitter;

    @Nullable
    private ContractInteractor interactor;

    BesuNetwork(
            final BesuNetworkSpec spec,
            final int networkIndex,
            final Accounts accounts,
            final Network dockerNetwork,
            final IntFunction<String> hostAddresses,
            final Path workDir,
            final int heapMb) {
        if (spec.nodeCount() > NODE_HOSTS_PER_NETWORK) {
            throw new IllegalArgumentException("network '" + spec.id() + "' declares " + spec.nodeCount()
                    + " nodes but only " + NODE_HOSTS_PER_NETWORK + " host addresses are reserved per network");
        }
        this.spec = spec;
        this.networkIndex = networkIndex;
        this.accounts = accounts;
        this.dockerNetwork = dockerNetwork;
        this.hostAddresses = hostAddresses;
        this.workDir = workDir;
        this.heapMb = heapMb;

        // Validator keys double as Besu node keys. Claiming them from the shared pool (rather than
        // deriving privately) keeps them out of reach of the load generator and the endpoint
        // signers — two submitters on one account produce nonce collisions that look exactly like
        // the relay bug this suite is meant to detect.
        final List<TestAccount> validatorKeys = accounts.allocate(spec.validatorCount(), spec.id() + " validator");
        // A dedicated account for contract deployment and on-chain administration. Kept separate
        // from the endpoint signers and the load generator's senders: two independent nonce
        // sequences on one address surface as `nonce too low`, which is indistinguishable from the
        // relay-side submission bug this suite exists to catch.
        this.deployerAccount = accounts.allocate(spec.id() + " deployer");
        // Give the deployer a much larger genesis balance than the other accounts. It funds every
        // per-channel connector top-up ({@link ChannelSpec#DEFAULT_CONNECTOR_FUNDING_WEI}, 1 ETH),
        // so at ten channels the total outflow already exceeds a default-balance account.
        this.genesis = new BesuQbftGenesis(
                spec,
                validatorKeys,
                accounts.all(),
                Map.of(deployerAccount.address(), BesuQbftGenesis.DEPLOYER_BALANCE_WEI));

        for (int i = 0; i < spec.nodeCount(); i++) {
            final boolean isValidator = i < spec.validatorCount();
            final TestAccount nodeKey = isValidator
                    ? validatorKeys.get(i)
                    : accounts.allocate(spec.id() + " full-node key #" + (i - spec.validatorCount()));
            final String nodeId = spec.id() + "-" + i;
            nodes.add(new BesuNode(
                    nodeId,
                    spec,
                    nodeKey,
                    isValidator,
                    hostAddresses.apply(NODE_HOST_BASE + networkIndex * NODE_HOSTS_PER_NETWORK + i),
                    dockerNetwork,
                    workDir.resolve(nodeId),
                    heapMb));
        }
    }

    /** @return the spec this network was built from. */
    public BesuNetworkSpec spec() {
        return spec;
    }

    /** @return the network id. */
    public String id() {
        return spec.id();
    }

    /** @return the EIP-155 chain id. */
    public long chainId() {
        return spec.chainId();
    }

    /** @return the CAIP-2 identifier, e.g. {@code eip155:31337}. */
    public String caip2() {
        return spec.caip2();
    }

    /**
     * The sole QBFT validator's address, which the CLPR config proof's trust anchor is built from.
     *
     * @return the validator address
     */
    public String validatorAddress() {
        return genesis.soleValidatorAddress();
    }

    /** @return all nodes, validators first. */
    public List<BesuNode> nodes() {
        return List.copyOf(nodes);
    }

    /**
     * @param index zero-based validator index
     * @return that validator
     */
    public BesuNode validator(final int index) {
        return nodes.get(index);
    }

    /**
     * @param index zero-based full-node index
     * @return that full node
     */
    public BesuNode fullNode(final int index) {
        return nodes.get(spec.validatorCount() + index);
    }

    /** @return the node the test drives by default (node 0, the validator). */
    public BesuNode primaryNode() {
        return nodes.getFirst();
    }

    /** @return host-visible JSON-RPC URL of the primary node. */
    public String jsonRpcUrl() {
        return primaryNode().jsonRpcUrl();
    }

    /** @return in-network JSON-RPC URL of the primary node, for endpoint containers. */
    public String internalJsonRpcUrl() {
        return primaryNode().internalJsonRpcUrl();
    }

    /** @return the relay's JSON-RPC client, pointed at the primary node. */
    public EvmJsonRpcClient rpc() {
        return requireStarted(rpc, "rpc()");
    }

    /** @return the raw JSON-RPC escape hatch, pointed at the primary node. */
    public RawJsonRpc rawRpc() {
        return requireStarted(rawRpc, "rawRpc()");
    }

    /** @return the block/transaction inspector for this chain. */
    public ChainInspector inspect() {
        return requireStarted(inspector, "inspect()");
    }

    /**
     * Write configuration, start every node, and wait until the chain is actually producing blocks.
     *
     * <p>Waiting for height &gt;= 1 rather than just for the RPC port matters: a QBFT network whose
     * validator set or {@code extraData} is malformed still serves JSON-RPC quite happily, it simply
     * never mines. Treating "port open" as ready would defer that failure to the first contract
     * deployment, where it looks like an unrelated timeout.
     */
    public void start() {
        final String genesisJson = genesis.toJson();
        final List<String> enodes = nodes.stream().map(BesuNode::enode).toList();
        final String staticNodes = BesuQbftGenesis.staticNodesJson(enodes);

        for (final BesuNode node : nodes) {
            node.writeConfiguration(genesisJson, staticNodes);
        }
        // Roll back a partial bring-up: if node k fails its wait, leaving 0..k-1 running would leak
        // containers for the rest of the suite when the caller's teardown is not reached.
        for (int i = 0; i < nodes.size(); i++) {
            try {
                nodes.get(i).start();
            } catch (final RuntimeException e) {
                for (int j = i; j >= 0; j--) {
                    try {
                        nodes.get(j).stop();
                    } catch (final RuntimeException ignored) {
                        // best-effort
                    }
                }
                throw e;
            }
        }

        this.rpc = new TestEvmJsonRpcClient(primaryNode().jsonRpcUrl());
        this.rawRpc = new RawJsonRpc(primaryNode().jsonRpcUrl());
        this.inspector = new ChainInspector(rawRpc);

        TestConditions.awaitCondition(Duration.ofMinutes(2), Duration.ofMillis(500), () -> {
            try {
                return inspect().headBlockNumber() >= 1L;
            } catch (final RuntimeException e) {
                return false; // node still coming up
            }
        });
    }

    /** @return the account that deploys contracts and performs on-chain administration. */
    public TestAccount deployerAccount() {
        return deployerAccount;
    }

    /** @return the deployed CLPR stack on this chain. */
    public E2eContracts contracts() {
        return requireStarted(contracts, "contracts()");
    }

    /** @return the deployer for this chain's per-channel contracts. */
    public E2eContractDeployer deployer() {
        return requireStarted(deployer, "deployer()");
    }

    /** @return the deployer account's transaction submitter. */
    public AnvilTxSubmitter deployerSubmitter() {
        return requireStarted(deployerSubmitter, "deployerSubmitter()");
    }

    /** @return the on-chain control surface bound to this chain's service and deployer account. */
    public ContractInteractor interactor() {
        return requireStarted(interactor, "interactor()");
    }

    /**
     * An on-chain control surface bound to an arbitrary account on this chain.
     *
     * <p>Needed because {@code registerEndpoint} admits {@code msg.sender}: each relay submits with
     * its own signing key, so that key — not the deployer's — has to be the registered endpoint, or
     * every {@code submitBundle} reverts.
     *
     * @param account the account to act as
     * @return an interactor signing with that account
     */
    public ContractInteractor interactorFor(final TestAccount account) {
        final EthSigner signer = new EthSigner(account.privateKeyHex());
        return new ContractInteractor(
                rpc(),
                submitterFor(account),
                signer,
                account.publicKey64(),
                contracts().core());
    }

    /**
     * A transaction submitter signing as {@code account} on this chain.
     *
     * @param account the account to sign with
     * @return the submitter
     */
    public AnvilTxSubmitter submitterFor(final TestAccount account) {
        return new AnvilTxSubmitter(rpc(), new EthSigner(account.privateKeyHex()), spec.chainId());
    }

    /**
     * Deploy the CLPR stack and apply the one-time bootstrap.
     *
     * <p>Separate from {@link #start()} so every chain in an environment is mining before any
     * contract is deployed; a deployment against a chain that is not yet producing blocks just
     * blocks on a receipt that will never arrive.
     *
     * <p>{@code bootstrapDefaults} is not optional: a freshly deployed service is disabled, and
     * every mutating call — endpoint registration, channel registration, submitBundle — reverts with
     * {@code ClprDisabled()} until the owner enables it.
     *
     */
    public void deployContracts() {
        final EthSigner signer = new EthSigner(deployerAccount.privateKeyHex());
        this.deployerSubmitter = new AnvilTxSubmitter(rpc(), signer, spec.chainId());
        this.deployer = new E2eContractDeployer(rpc(), deployerSubmitter);
        this.contracts = deployer.deployChainStack(caip2());
        this.interactor = new ContractInteractor(
                rpc(), deployerSubmitter, signer, deployerAccount.publicKey64(), contracts.core());
        interactor.bootstrapDefaults(AbiCodec.fromHex(contracts.clprService()));
    }

    /** Stop every node. Host data directories survive, so the chain can be restarted. */
    public void stop() {
        if (rawRpc != null) {
            try {
                rawRpc.close();
            } catch (final RuntimeException e) {
                // best-effort
            }
            rawRpc = null;
            inspector = null;
        }
        for (final BesuNode node : nodes) {
            try {
                node.stop();
            } catch (final RuntimeException e) {
                // Teardown is best-effort; a node that is already gone must not mask the others.
            }
        }
    }

    private static <T> T requireStarted(@Nullable final T value, final String what) {
        if (value == null) {
            throw new IllegalStateException(
                    what + " is only available after the network is started — call E2EEnvironment.startAll() first");
        }
        return value;
    }
}
