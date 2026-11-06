// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e;

import org.hiero.clpr.relay.e2e.chain.BesuNetwork;
import org.hiero.clpr.relay.e2e.chain.BesuNetworkSpec;
import org.hiero.clpr.relay.e2e.channel.Channel;
import org.hiero.clpr.relay.e2e.channel.ChannelSpec;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpoint;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpointSpec;

/**
 * The two-chain, two-endpoint, one-channel topology most fault-injection and single-channel tests
 * declare — extracted here so those tests do not each redefine the same {@code Fixture} record.
 *
 * <p>Deliberately narrow. Tests that need a second channel, more endpoints, multi-node Besu
 * networks, or discovery-enabled endpoints declare their topology inline; extending this class to
 * cover those cases turns it into a config bag that reads worse than the inline setup.
 *
 * <p>The environment is not started here — the topology is only <em>declared</em>. A caller that
 * wants to opt into other declare-time settings (a Toxiproxy shim, extra channels) can do so
 * between {@link #declare(E2EEnvironment)} and {@link E2EEnvironment#startAll()}.
 */
public record TwoChainOneChannel(
        BesuNetwork besuA, BesuNetwork besuB, EvmEndpoint epA, EvmEndpoint epB, Channel channel) {

    /**
     * Declare a fresh instance on {@code env}. Endpoints use {@link EvmEndpointSpec#defaults()} with
     * ids {@code ep-a} and {@code ep-b}; both Besu networks are single-node QBFT.
     *
     * @param env the per-method environment
     * @return the declared topology
     */
    public static TwoChainOneChannel declare(final E2EEnvironment env) {
        final BesuNetwork besuA = env.besuNetworks().add(BesuNetworkSpec.singleNode());
        final BesuNetwork besuB = env.besuNetworks().add(BesuNetworkSpec.singleNode());
        final EvmEndpoint epA = env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-a"));
        final EvmEndpoint epB = env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-b"));
        final Channel channel =
                env.channels().create(ChannelSpec.between(besuA, besuB).endpoints(epA, epB));
        return new TwoChainOneChannel(besuA, besuB, epA, epB, channel);
    }
}
