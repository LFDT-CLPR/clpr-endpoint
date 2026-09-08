// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e;

import java.nio.file.Path;
import org.hiero.clpr.relay.e2e.chain.Accounts;
import org.hiero.clpr.relay.e2e.chain.BesuNetworks;
import org.hiero.clpr.relay.e2e.channel.Channels;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpoints;
import org.hiero.clpr.relay.e2e.fault.Faults;
import org.hiero.clpr.relay.e2e.load.MessageInjector;
import org.testcontainers.containers.Network;

/**
 * The per-test-method environment: the Docker network everything shares, the Besu chains, the
 * endpoint containers under test, and the account pool.
 *
 * <p>Built and closed by {@link E2ETestExtension}, one instance per test method. Declare it as the
 * single parameter of an {@link E2ETest} method to receive it.
 *
 * <p>Objects are <b>declared</b> first and <b>started</b> by {@link #startAll()}. That split is not
 * ceremony: the QBFT genesis has to embed the validator set and the prefunded accounts, and the
 * on-chain peer roster has to embed each endpoint's IP address — none of which is known until the
 * test has finished describing its topology.
 */
public interface E2EEnvironment extends AutoCloseable {

    /** @return the Besu networks declared for this test. */
    BesuNetworks besuNetworks();

    /** @return the endpoint containers under test. */
    EvmEndpoints endpoints();

    /** @return the channels declared for this test. */
    Channels channels();

    /** @return the load generator. */
    MessageInjector messages();

    /** @return the waiting helpers. */
    Awaiter await();

    /** @return the fault injector. */
    Faults faults();

    /** @return the shared, prefunded account pool. */
    Accounts accounts();

    /** @return the active sizing profile. */
    E2EProfile profile();

    /**
     * The Docker network every container in this environment joins, carrying the environment's
     * dedicated /24.
     *
     * @return the network
     */
    Network dockerNetwork();

    /**
     * Directory for this test's artifacts — container logs, metric scrapes, chain dumps.
     *
     * <p>Populated by the framework according to the {@code clpr.e2e.keepArtifacts} policy:
     * {@code failure} (default; only failed tests write here), {@code always} (every test writes,
     * with {@code success.txt} vs {@code failure.txt} as the summary marker), or {@code never}
     * (nothing written). The directory is created on demand.
     *
     * @return the output directory (created on demand, may not exist)
     */
    Path outputDirectory();

    /**
     * Route endpoint-to-endpoint sync traffic through a Toxiproxy sidecar so tests can disconnect
     * two relays cleanly, without also cutting them off from their chains.
     *
     * <p>Must be called <b>before</b> {@link #startAll()}: the channel wiring publishes each
     * endpoint's dial address in the on-chain peer roster, and once that address is on chain the
     * shim cannot re-route existing peers to itself. Idempotent; a second call is a no-op.
     *
     * <p>Peers keep dialing the same address they always would; the address just points at Toxiproxy
     * instead of the endpoint itself. Chain traffic is not touched — for a chain outage, use
     * {@link org.hiero.clpr.relay.e2e.fault.Faults#pauseChain(org.hiero.clpr.relay.e2e.chain.BesuNetwork)}.
     */
    void enablePeerNetworkShim();

    /**
     * Bring up everything declared so far, in dependency order: chains, then contracts, then
     * channel wiring, then endpoints. Idempotent — objects declared afterwards are started
     * individually.
     */
    void startAll();

    /** Tear everything down. Idempotent; never throws. */
    @Override
    void close();
}
