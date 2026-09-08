// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e;

import com.github.dockerjava.api.model.Network.Ipam;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.hiero.clpr.relay.e2e.chain.Accounts;
import org.hiero.clpr.relay.e2e.chain.BesuNetworks;
import org.hiero.clpr.relay.e2e.channel.Channels;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpoints;
import org.hiero.clpr.relay.e2e.fault.Faults;
import org.hiero.clpr.relay.e2e.fault.PeerNetworkShim;
import org.hiero.clpr.relay.e2e.internal.E2ESubnets;
import org.hiero.clpr.relay.e2e.load.MessageInjector;
import org.jspecify.annotations.Nullable;
import org.testcontainers.containers.Network;

/**
 * The concrete per-test environment. Created and closed by {@link E2ETestExtension}.
 *
 * <p>Owns three things that must be released even when a test fails: the Docker network, the subnet
 * lease, and the work directory. {@link #close()} therefore never throws — a teardown failure must
 * not replace the actual test failure in the report.
 */
final class E2EEnvironmentImpl implements E2EEnvironment {

    /** Default pool size; enough for a few networks' validators plus endpoints and load senders. */
    private static final int ACCOUNT_POOL_SIZE = 64;

    /** How many subnet slots to try before giving up (see {@link #allocateNetwork()}). */
    private static final int SUBNET_ATTEMPTS = 16;

    private final E2EProfile profile;
    private final Path outputDirectory;
    private final Path workDirectory;
    private final E2ESubnets.Lease subnet;
    private final Network dockerNetwork;
    private final Accounts accounts;
    private final BesuNetworks besuNetworks;
    private final EvmEndpoints endpoints;
    private final Channels channels;
    private final MessageInjector messages;
    private final Awaiter awaiter;
    private final Faults faults;

    /** Toxiproxy sidecar for peer traffic. Present only after {@link #enablePeerNetworkShim()}. */
    @Nullable
    private PeerNetworkShim peerNetworkShim;

    private boolean started;
    private boolean closed;

    E2EEnvironmentImpl(final E2EProfile profile, final Path outputDirectory) {
        this.profile = profile;
        this.outputDirectory = outputDirectory;
        try {
            // The output directory is deliberately NOT created here. Diagnostics are only written when
            // a test fails, so creating it up front left an empty directory behind for every passing
            // test — and a tree of empty directories reads as "diagnostics are broken". Created on
            // demand by the writer instead, so a directory existing means something failed there.
            //
            // Chain data and generated configuration live outside it: the output directory is an
            // artifact a CI job uploads, and shipping a Besu database with it would balloon it for no
            // diagnostic value.
            this.workDirectory = Files.createTempDirectory("clpr-testing-");
        } catch (final IOException e) {
            throw new UncheckedIOException("failed to prepare the E2E work directory", e);
        }

        final AllocatedNetwork allocated = allocateNetwork();
        this.subnet = allocated.lease();
        this.dockerNetwork = allocated.network();
        try {
            this.accounts = new Accounts(ACCOUNT_POOL_SIZE);
            this.besuNetworks = new BesuNetworks(
                    accounts, dockerNetwork, subnet::host, workDirectory.resolve("chains"), profile.besuHeapMb());
            this.endpoints = new EvmEndpoints(accounts, dockerNetwork, subnet::host, profile.endpointHeapMb());
            this.channels = new Channels(accounts);
            this.messages = new MessageInjector(accounts);
            this.awaiter = new Awaiter(() -> endpoints.all());
            this.faults = new Faults(dockerNetwork, () -> peerNetworkShim);
        } catch (final RuntimeException e) {
            try {
                dockerNetwork.close();
            } catch (final RuntimeException ignored) {
                // best-effort
            }
            subnet.close();
            throw e;
        }
    }

    /** A Docker network together with the subnet lease it occupies. */
    private record AllocatedNetwork(Network network, E2ESubnets.Lease lease) {}

    /**
     * Create the environment's Docker network on an explicitly chosen subnet.
     *
     * <p>The subnet is explicit because endpoint containers need pinned addresses:
     * {@code ClprService} validates a channel's seed endpoints as IPv4 literals and rejects
     * hostnames, so a Docker network alias cannot be published in the on-chain peer roster.
     *
     * <p>Creation walks forward through subnet slots on an overlap. The in-JVM allocator cannot see a
     * subnet still held by a network that some earlier, killed run leaked, and Docker rejects that
     * overlap with a 403 — so a fresh JVM would otherwise fail on the same slot indefinitely.
     * Advancing makes an aborted run cost one wasted slot rather than every subsequent run.
     */
    private static AllocatedNetwork allocateNetwork() {
        final List<E2ESubnets.Lease> overlapping = new ArrayList<>();
        RuntimeException lastFailure = null;
        try {
            for (int attempt = 0; attempt < SUBNET_ATTEMPTS; attempt++) {
                final E2ESubnets.Lease candidate = E2ESubnets.lease();
                try {
                    final Network network = Network.builder()
                            .createNetworkCmdModifier(cmd -> cmd.withIpam(new Ipam()
                                    .withConfig(new Ipam.Config()
                                            .withSubnet(candidate.cidr())
                                            .withGateway(candidate.gateway()))))
                            .build();
                    // Testcontainers creates the network lazily, so force it here: an overlap must
                    // surface now, not at the first container start where the cause is far less clear.
                    network.getId();
                    return new AllocatedNetwork(network, candidate);
                } catch (final RuntimeException e) {
                    if (!isSubnetOverlap(e)) {
                        candidate.close();
                        throw e;
                    }
                    // Hold the lease so the next attempt cannot pick the same slot back up.
                    overlapping.add(candidate);
                    lastFailure = e;
                }
            }
        } finally {
            overlapping.forEach(E2ESubnets.Lease::close);
        }
        throw new IllegalStateException(
                "could not find a free /24 for the E2E environment after " + SUBNET_ATTEMPTS
                        + " attempts. Networks leaked by aborted runs are the usual cause — remove them with"
                        + " `docker network prune`, or drop the ones labelled org.testcontainers=true.",
                lastFailure);
    }

    /** Docker reports an address-space clash as a 403 whose message mentions the pool. */
    private static boolean isSubnetOverlap(final Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            final String message = t.getMessage();
            if (message != null && message.contains("Pool overlaps")) {
                return true;
            }
        }
        return false;
    }

    @Override
    public BesuNetworks besuNetworks() {
        return besuNetworks;
    }

    @Override
    public EvmEndpoints endpoints() {
        return endpoints;
    }

    @Override
    public Channels channels() {
        return channels;
    }

    @Override
    public MessageInjector messages() {
        return messages;
    }

    @Override
    public Awaiter await() {
        return awaiter;
    }

    @Override
    public Faults faults() {
        return faults;
    }

    @Override
    public Accounts accounts() {
        return accounts;
    }

    @Override
    public E2EProfile profile() {
        return profile;
    }

    @Override
    public Network dockerNetwork() {
        return dockerNetwork;
    }

    @Override
    public Path outputDirectory() {
        return outputDirectory;
    }

    @Override
    public void enablePeerNetworkShim() {
        if (started) {
            throw new IllegalStateException("enablePeerNetworkShim() must be called before startAll();"
                    + " the on-chain peer roster is written during channel wiring and cannot be re-routed later");
        }
        if (peerNetworkShim != null) {
            return;
        }
        // Infrastructure slot 0 → .2 on the /24, reserved for Toxiproxy per E2ESubnets.
        this.peerNetworkShim = new PeerNetworkShim(dockerNetwork, subnet.infrastructureHost(0));
        channels.useShim(peerNetworkShim);
    }

    @Override
    public void startAll() {
        if (started) {
            return;
        }
        started = true;
        // The shim must be up before wiring runs so it can hand out proxy addresses to publish on chain.
        if (peerNetworkShim != null) {
            peerNetworkShim.start();
        }
        besuNetworks.startAll();
        besuNetworks.deployAll();
        // Channels before endpoints: ClprChannelHandler performs blocking on-chain reads at startup
        // (protocol version, ledger config, peer roster) and aborts a channel that is not registered,
        // so the wiring has to be complete first. It also fills in each endpoint's configuration.
        channels.wireAll();
        endpoints.startAll();
    }

    /** @return the scratch directory holding chain data and generated configuration. */
    Path workDirectory() {
        return workDirectory;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        // Reverse dependency order, each step isolated: a container that is already gone must not
        // prevent the network and the subnet lease from being released, or the next test in the
        // class starts with a leaked subnet.
        try {
            endpoints.shutdown();
        } catch (final RuntimeException e) {
            // best-effort
        }
        try {
            // destroyAll, not stopAll: this is the end of the environment, and the nodes' data
            // volumes are not reclaimed by anything else (Ryuk is disabled), so a stop here would
            // leak one volume per node per test.
            besuNetworks.destroyAll();
        } catch (final RuntimeException e) {
            // best-effort
        }
        // Toxiproxy after the endpoints and chains: they might otherwise sit in a half-connected state
        // that trips their own shutdown paths.
        if (peerNetworkShim != null) {
            try {
                peerNetworkShim.close();
            } catch (final RuntimeException e) {
                // best-effort
            }
        }
        try {
            dockerNetwork.close();
        } catch (final RuntimeException e) {
            // best-effort
        }
        subnet.close();
    }
}
