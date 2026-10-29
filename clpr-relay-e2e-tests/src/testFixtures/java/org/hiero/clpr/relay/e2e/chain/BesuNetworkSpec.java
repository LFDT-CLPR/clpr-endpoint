// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.chain;

import java.util.Map;

/**
 * Declarative description of one Besu QBFT network. Immutable; use the {@code with*} methods.
 *
 * @param id stable identifier, also the container network-alias prefix
 * @param chainId EIP-155 chain id. Must be unique across the networks in one environment: a
 *     channel's id is derived from both chains' CAIP-2 ids, and the relay resolves a peer's proof
 *     type by chain id, so two chains sharing an id make both ambiguous
 * @param validatorCount QBFT validators. Leave at 1 for anything asserting cross-chain delivery —
 *     {@code QBFTVerifier} reverts with {@code MultiValidatorNotSupported()} on a multi-validator
 *     epoch header, so a second validator makes every inbound bundle unverifiable
 * @param fullNodeCount non-validator nodes. This is how a network becomes genuinely multi-node
 *     (gossip, independent RPC targets, node-outage scenarios) without breaking verification
 * @param blockPeriodSeconds QBFT block period
 * @param minGasPriceWei {@code --min-gas-price}. Deliberately non-zero by default: the
 *     connector-affordability check in {@code BundleLib} is guarded by {@code tx.gasprice > 0}, so a
 *     zero-price chain makes the underfunded-connector path unreachable and silently untestable
 * @param image Besu image. 26.5.0 mines PoA automatically from the node key and does not need the
 *     {@code --miner-*} flags that were removed after 25.x
 * @param extraEnv extra container environment, merged last so it can override anything
 * @param rpcFilterProxy when {@code true} an {@code EvmRpcFilterProxy} container is started in
 *     front of the primary Besu node and endpoints dial that instead of the node directly. Opt-in
 *     because it costs one extra container per network; the fault it enables (per-method RPC
 *     blocking, e.g. break {@code eth_sendRawTransaction} while leaving {@code eth_call} untouched)
 *     cannot be expressed with {@code docker pause}
 */
public record BesuNetworkSpec(
        String id,
        long chainId,
        int validatorCount,
        int fullNodeCount,
        int blockPeriodSeconds,
        long minGasPriceWei,
        String image,
        Map<String, String> extraEnv,
        boolean rpcFilterProxy) {

    /** Besu release used by the CLPR contract E2E backend; QBFT-capable without the removed flags. */
    public static final String DEFAULT_IMAGE = "hyperledger/besu:26.5.0";

    /** First chain id handed out; the second network gets 31338 */
    public static final long FIRST_CHAIN_ID = 31337L;

    public BesuNetworkSpec {
        if (validatorCount < 1) {
            throw new IllegalArgumentException("validatorCount must be >= 1, got " + validatorCount);
        }
        if (fullNodeCount < 0) {
            throw new IllegalArgumentException("fullNodeCount must be >= 0, got " + fullNodeCount);
        }
        if (blockPeriodSeconds < 1) {
            throw new IllegalArgumentException("blockPeriodSeconds must be >= 1, got " + blockPeriodSeconds);
        }
        if (minGasPriceWei < 0) {
            throw new IllegalArgumentException("minGasPriceWei must be >= 0, got " + minGasPriceWei);
        }
        extraEnv = Map.copyOf(extraEnv);
    }

    /**
     * Defaults for a network of {@code nodes} containers: one validator and the rest full nodes.
     *
     * <p>The 1-validator split is not a simplification, it is the only shape the on-chain verifier
     * accepts (see {@link #validatorCount}). {@code chainId} is left at {@link #FIRST_CHAIN_ID} and
     * renumbered by {@link BesuNetworks} as networks are added, so the common two-network case needs
     * no explicit ids.
     *
     * @param nodes total container count, at least 1
     * @return the spec
     */
    public static BesuNetworkSpec of(final int nodes) {
        if (nodes < 1) {
            throw new IllegalArgumentException("a network needs at least one node, got " + nodes);
        }
        return new BesuNetworkSpec("besu", FIRST_CHAIN_ID, 1, nodes - 1, 1, 1L, DEFAULT_IMAGE, Map.of(), false);
    }

    /** Single-node default. */
    public static BesuNetworkSpec singleNode() {
        return of(1);
    }

    /** @return total container count. */
    public int nodeCount() {
        return validatorCount + fullNodeCount;
    }

    /** @return CAIP-2 identifier, e.g. {@code eip155:31337}. */
    public String caip2() {
        return "eip155:" + chainId;
    }

    public BesuNetworkSpec withId(final String newId) {
        return new BesuNetworkSpec(
                newId,
                chainId,
                validatorCount,
                fullNodeCount,
                blockPeriodSeconds,
                minGasPriceWei,
                image,
                extraEnv,
                rpcFilterProxy);
    }

    public BesuNetworkSpec withChainId(final long newChainId) {
        return new BesuNetworkSpec(
                id,
                newChainId,
                validatorCount,
                fullNodeCount,
                blockPeriodSeconds,
                minGasPriceWei,
                image,
                extraEnv,
                rpcFilterProxy);
    }

    public BesuNetworkSpec withValidators(final int count) {
        return new BesuNetworkSpec(
                id, chainId, count, fullNodeCount, blockPeriodSeconds, minGasPriceWei, image, extraEnv, rpcFilterProxy);
    }

    public BesuNetworkSpec withFullNodes(final int count) {
        return new BesuNetworkSpec(
                id,
                chainId,
                validatorCount,
                count,
                blockPeriodSeconds,
                minGasPriceWei,
                image,
                extraEnv,
                rpcFilterProxy);
    }

    public BesuNetworkSpec withBlockPeriodSeconds(final int seconds) {
        return new BesuNetworkSpec(
                id, chainId, validatorCount, fullNodeCount, seconds, minGasPriceWei, image, extraEnv, rpcFilterProxy);
    }

    public BesuNetworkSpec withMinGasPriceWei(final long wei) {
        return new BesuNetworkSpec(
                id, chainId, validatorCount, fullNodeCount, blockPeriodSeconds, wei, image, extraEnv, rpcFilterProxy);
    }

    public BesuNetworkSpec withImage(final String newImage) {
        return new BesuNetworkSpec(
                id,
                chainId,
                validatorCount,
                fullNodeCount,
                blockPeriodSeconds,
                minGasPriceWei,
                newImage,
                extraEnv,
                rpcFilterProxy);
    }

    public BesuNetworkSpec withExtraEnv(final Map<String, String> env) {
        return new BesuNetworkSpec(
                id,
                chainId,
                validatorCount,
                fullNodeCount,
                blockPeriodSeconds,
                minGasPriceWei,
                image,
                env,
                rpcFilterProxy);
    }

    /**
     * Bring up an {@code EvmRpcFilterProxy} container in front of the primary Besu node and route
     * endpoint traffic through it. See the record javadoc for the motivation.
     */
    public BesuNetworkSpec withRpcFilterProxy() {
        return new BesuNetworkSpec(
                id, chainId, validatorCount, fullNodeCount, blockPeriodSeconds, minGasPriceWei, image, extraEnv, true);
    }
}
