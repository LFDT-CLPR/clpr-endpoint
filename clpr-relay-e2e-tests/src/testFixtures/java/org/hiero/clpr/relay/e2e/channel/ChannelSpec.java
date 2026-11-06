// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.channel;

import java.math.BigInteger;
import java.util.List;
import org.hiero.clpr.relay.e2e.chain.BesuNetwork;
import org.hiero.clpr.relay.e2e.chain.E2eContracts;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpoint;
import org.jspecify.annotations.Nullable;

/**
 * Declarative description of one CLPR channel: the two chains it spans and the endpoints that serve
 * each side.
 *
 * <p>A channel needs an endpoint on <b>each</b> side. Chain A's endpoint reads A, builds a bundle and
 * calls {@code sync} on chain B's endpoint, which submits it to B. One process serving both sides is
 * a known relay gap, not a supported topology.
 *
 * @param networkA one side's chain
 * @param networkB the other side's chain
 * @param endpointsA endpoints serving side A; one entry is the normal sharded shape, several make the
 *     side redundant and the endpoints compete for the same bundle
 * @param endpointsB endpoints serving side B
 * @param connectorStakeWei stake locked when the connector is registered; must meet the service's
 *     minimum
 * @param connectorFundingWeiA plain ETH sent to side A's connector contract
 * @param connectorFundingWeiB plain ETH sent to side B's connector contract. Funded per side because
 *     the underfunded-connector scenario needs exactly one side starved: the service checks the
 *     <em>receiving</em> side's connector balance before dispatching an inbound message, and starving
 *     both would also block the reply travelling the other way — turning one intended failure into two
 *     and making the test prove nothing in particular
 * @param salt32 operator-chosen label mixed into the channel id; {@code null} generates a fresh
 *     random one, so re-running a test never collides with a previous run's channel
 * @param serviceA optional override for the CLPR stack this channel wires against on side A. When
 *     {@code null}, {@code networkA.contracts()} — the chain's primary stack — is used. A non-null
 *     override lets the channel target a second {@code ClprService} deployed via
 *     {@link BesuNetwork#deployAdditionalService()}, so a single endpoint can carry multiple
 *     {@code clprServices[]} entries
 * @param serviceB optional override for side B, same shape as {@code serviceA}
 */
public record ChannelSpec(
        BesuNetwork networkA,
        BesuNetwork networkB,
        List<EvmEndpoint> endpointsA,
        List<EvmEndpoint> endpointsB,
        BigInteger connectorStakeWei,
        BigInteger connectorFundingWeiA,
        BigInteger connectorFundingWeiB,
        @Nullable byte[] salt32,
        @Nullable E2eContracts serviceA,
        @Nullable E2eContracts serviceB) {

    /** Minimum locked stake the service accepts: 0.1 ether. */
    public static final BigInteger DEFAULT_CONNECTOR_STAKE_WEI = BigInteger.valueOf(100_000_000_000_000_000L);

    /** 1 ether of prepaid execution budget. */
    public static final BigInteger DEFAULT_CONNECTOR_FUNDING_WEI = BigInteger.valueOf(1_000_000_000_000_000_000L);

    public ChannelSpec {
        endpointsA = List.copyOf(endpointsA);
        endpointsB = List.copyOf(endpointsB);
        // Clone: an array reaching through a record accessor is otherwise mutable by the caller, and
        // a salt changed after wiring would silently point at a different channel id.
        salt32 = salt32 == null ? null : salt32.clone();
        if (connectorStakeWei.signum() < 0) {
            throw new IllegalArgumentException("connectorStakeWei must not be negative");
        }
        if (connectorFundingWeiA.signum() < 0 || connectorFundingWeiB.signum() < 0) {
            throw new IllegalArgumentException("connector funding must not be negative");
        }
        if (endpointsA.isEmpty() || endpointsB.isEmpty()) {
            throw new IllegalArgumentException("a channel needs at least one endpoint on each side");
        }
        if (networkA.chainId() == networkB.chainId()) {
            throw new IllegalArgumentException("a channel needs two distinct chains; both sides are chain "
                    + networkA.chainId() + ". The channel id is derived from both CAIP-2 ids, so identical"
                    + " chains cannot be told apart.");
        }
    }

    /**
     * Start a spec between two chains. Endpoints still have to be attached.
     *
     * @param a one chain
     * @param b the other chain
     * @return a partially built spec
     */
    public static Builder between(final BesuNetwork a, final BesuNetwork b) {
        return new Builder(a, b);
    }

    /** @return the resolved side-A CLPR stack — the override if set, else the chain's primary. */
    public E2eContracts resolvedServiceA() {
        return serviceA != null ? serviceA : networkA.contracts();
    }

    /** @return the resolved side-B CLPR stack — the override if set, else the chain's primary. */
    public E2eContracts resolvedServiceB() {
        return serviceB != null ? serviceB : networkB.contracts();
    }

    /** Fluent builder. */
    public static final class Builder {
        private final BesuNetwork networkA;
        private final BesuNetwork networkB;
        private List<EvmEndpoint> endpointsA = List.of();
        private List<EvmEndpoint> endpointsB = List.of();
        private BigInteger stake = DEFAULT_CONNECTOR_STAKE_WEI;
        private BigInteger fundingA = DEFAULT_CONNECTOR_FUNDING_WEI;
        private BigInteger fundingB = DEFAULT_CONNECTOR_FUNDING_WEI;

        @Nullable
        private byte[] salt;

        @Nullable
        private E2eContracts serviceA;

        @Nullable
        private E2eContracts serviceB;

        private Builder(final BesuNetwork networkA, final BesuNetwork networkB) {
            this.networkA = networkA;
            this.networkB = networkB;
        }

        /** Sharded: exactly one endpoint per side. */
        public Builder endpoints(final EvmEndpoint a, final EvmEndpoint b) {
            this.endpointsA = List.of(a);
            this.endpointsB = List.of(b);
            return this;
        }

        /** Redundant: several endpoints per side, competing for the same bundle. */
        public Builder redundantEndpoints(final List<EvmEndpoint> a, final List<EvmEndpoint> b) {
            this.endpointsA = List.copyOf(a);
            this.endpointsB = List.copyOf(b);
            return this;
        }

        public Builder connectorStakeWei(final BigInteger wei) {
            this.stake = wei;
            return this;
        }

        /** Fund both sides' connectors equally. */
        public Builder connectorFundingWei(final BigInteger wei) {
            this.fundingA = wei;
            this.fundingB = wei;
            return this;
        }

        /**
         * Fund the two sides' connectors independently.
         *
         * @param weiA side A's execution budget
         * @param weiB side B's execution budget; zero starves inbound dispatch on B
         */
        public Builder connectorFunding(final BigInteger weiA, final BigInteger weiB) {
            this.fundingA = weiA;
            this.fundingB = weiB;
            return this;
        }

        public Builder salt(final byte[] salt32) {
            this.salt = salt32;
            return this;
        }

        /**
         * Wire this channel against specific {@code ClprService} deployments — one per side —
         * rather than the primary stack each chain exposes via {@link BesuNetwork#contracts()}.
         *
         * <p>The channel's endpoints will pick up matching {@code clprServices[]} entries in their
         * relay config, each with its own signing key.
         *
         * @param a side-A CLPR stack; typically the return value of
         *     {@link BesuNetwork#deployAdditionalService()}
         * @param b side-B CLPR stack
         * @return this builder
         */
        public Builder services(final E2eContracts a, final E2eContracts b) {
            this.serviceA = a;
            this.serviceB = b;
            return this;
        }

        public ChannelSpec build() {
            return new ChannelSpec(
                    networkA, networkB, endpointsA, endpointsB, stake, fundingA, fundingB, salt, serviceA, serviceB);
        }
    }
}
