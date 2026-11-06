// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.chain;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.IntFunction;
import org.testcontainers.containers.Network;

/**
 * The Besu chains declared by one test.
 *
 * <p>Networks are added before {@code startAll()} and get their ids and chain ids assigned
 * automatically as they are added — {@code besu-0} at {@code 31337}, {@code besu-1} at
 * {@code 31338}. Distinct chain ids are not cosmetic: a
 * channel's id is derived from both chains' CAIP-2 ids and the relay resolves a peer's proof type by
 * chain id, so two chains sharing an id would make both ambiguous. An explicit
 * {@link BesuNetworkSpec#withChainId} wins over the automatic numbering.
 */
public final class BesuNetworks {

    private final Accounts accounts;
    private final Network dockerNetwork;
    private final IntFunction<String> hostAddresses;
    private final Path workDir;
    private final int heapMb;

    private final List<BesuNetwork> networks = new ArrayList<>();

    public BesuNetworks(
            final Accounts accounts,
            final Network dockerNetwork,
            final IntFunction<String> hostAddresses,
            final Path workDir,
            final int heapMb) {
        this.accounts = accounts;
        this.dockerNetwork = dockerNetwork;
        this.hostAddresses = hostAddresses;
        this.workDir = workDir;
        this.heapMb = heapMb;
    }

    /**
     * Declare a network.
     *
     * @param spec the network description; its {@code id} and {@code chainId} are auto-numbered when
     *     left at the {@link BesuNetworkSpec#of} defaults
     * @return the declared network
     */
    public BesuNetwork add(final BesuNetworkSpec spec) {
        final int index = networks.size();
        BesuNetworkSpec effective = spec;
        if ("besu".equals(spec.id())) {
            effective = effective.withId("besu-" + index);
        }
        if (spec.chainId() == BesuNetworkSpec.FIRST_CHAIN_ID) {
            effective = effective.withChainId(BesuNetworkSpec.FIRST_CHAIN_ID + index);
        }
        assertChainIdUnique(effective);

        final BesuNetwork network = new BesuNetwork(
                effective, index, accounts, dockerNetwork, hostAddresses, workDir.resolve(effective.id()), heapMb);
        networks.add(network);
        return network;
    }

    /**
     * Declare a network of {@code nodes} containers with default settings: one QBFT validator and
     * the rest full nodes.
     *
     * @param nodes total container count
     * @return the declared network
     */
    public BesuNetwork add(final int nodes) {
        return add(BesuNetworkSpec.of(nodes));
    }

    /** @return every declared network, in declaration order. */
    public List<BesuNetwork> all() {
        return List.copyOf(networks);
    }

    /**
     * @param index declaration order index
     * @return that network
     */
    public BesuNetwork get(final int index) {
        return networks.get(index);
    }

    /**
     * @param id the network id
     * @return that network
     * @throws IllegalArgumentException if no network has that id
     */
    public BesuNetwork byId(final String id) {
        return networks.stream()
                .filter(n -> n.id().equals(id))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("no Besu network with id '" + id + "'; declared: "
                        + networks.stream().map(BesuNetwork::id).toList()));
    }

    /** Start every declared network. */
    public void startAll() {
        for (final BesuNetwork network : networks) {
            network.start();
        }
    }

    /**
     * Deploy the CLPR stack on every network.
     *
     * <p>Runs after every network is mining, not interleaved with startup: a deployment against a
     * chain that is not yet producing blocks simply waits for a receipt that never comes.
     *
     */
    public void deployAll() {
        for (final BesuNetwork network : networks) {
            network.deployContracts();
        }
    }

    /** Stop every network, best-effort. */
    public void stopAll() {
        for (int i = networks.size() - 1; i >= 0; i--) {
            networks.get(i).stop();
        }
    }

    /**
     * Stop every network and delete its data volumes, in reverse creation order. Terminal — for
     * environment teardown only.
     *
     * <p>Each network is isolated: one that is already gone must not stop the rest from releasing
     * their volumes, because nothing else reclaims them (Ryuk is disabled for this suite).
     */
    public void destroyAll() {
        for (int i = networks.size() - 1; i >= 0; i--) {
            try {
                networks.get(i).destroy();
            } catch (final RuntimeException e) {
                // best-effort
            }
        }
    }

    private void assertChainIdUnique(final BesuNetworkSpec candidate) {
        final Optional<BesuNetwork> clash = networks.stream()
                .filter(n -> n.chainId() == candidate.chainId())
                .findFirst();
        if (clash.isPresent()) {
            throw new IllegalArgumentException("network '" + candidate.id() + "' reuses chain id "
                    + candidate.chainId() + " already taken by '" + clash.get().id()
                    + "'. CLPR derives channel ids from both chains' CAIP-2 ids and resolves peer proof types by"
                    + " chain id, so duplicate ids make channel identity ambiguous.");
        }
    }
}
