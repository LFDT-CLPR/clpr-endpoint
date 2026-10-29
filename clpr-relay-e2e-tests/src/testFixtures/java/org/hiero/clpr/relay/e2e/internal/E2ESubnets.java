// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.internal;

import java.util.BitSet;

/**
 * Allocator for the fixed /24 subnet each E2E environment runs on.
 *
 * <p>Static container IPs are not a convenience here, they are required: {@code ClprService}
 * validates a channel's seed endpoints as IPv4 literals and rejects hostnames with
 * {@code ClprInvalidEndpointAddress}, so a Docker network alias cannot be published in the on-chain
 * peer roster. Each endpoint container therefore gets a pinned address, which in turn means the
 * environment needs a subnet it fully controls.
 *
 * <p>Hard-coding one subnet would make concurrent environments collide, so slots are handed out
 * here and returned on teardown. Within a JVM this is exact. Across JVMs (Gradle
 * {@code maxParallelForks > 1}) it is not — which is why the E2E test task pins
 * {@code maxParallelForks = 1}; lifting that needs a file-based lock, not a bigger bitmap.
 */
public final class E2ESubnets {

    /**
     * Base second octet. {@code 172.31.x.0/24} sits inside the RFC-1918 172.16/12 block and outside
     * Docker's default pool (172.17-172.20), so it does not fight the daemon for address space.
     */
    private static final int FIRST_OCTET = 172;

    private static final int SECOND_OCTET = 31;

    /** Slots 0..253 map to third octets 1..254, keeping .0 and .255 out of play. */
    private static final int MAX_SLOTS = 254;

    private static final BitSet IN_USE = new BitSet(MAX_SLOTS);

    private E2ESubnets() {}

    /** One allocated /24, released back to the pool on {@link #close()}. */
    public static final class Lease implements AutoCloseable {
        /** Index into {@link #IN_USE}; the third octet is {@code bit + 1}. */
        private final int bit;

        private final int thirdOctet;
        private boolean released;

        private Lease(final int bit) {
            this.bit = bit;
            // Bit 0 would render as 172.31.0.0/24; shift so the third octet starts at 1.
            this.thirdOctet = bit + 1;
        }

        /** @return CIDR of the leased subnet, e.g. {@code 172.31.7.0/24}. */
        public String cidr() {
            return FIRST_OCTET + "." + SECOND_OCTET + "." + thirdOctet + ".0/24";
        }

        /** @return gateway address, e.g. {@code 172.31.7.1}. */
        public String gateway() {
            return FIRST_OCTET + "." + SECOND_OCTET + "." + thirdOctet + ".1";
        }

        /**
         * A stable address inside the leased subnet.
         *
         * <p>Host numbering starts at 10 so the low addresses stay free for the gateway and any
         * infrastructure container (a future Toxiproxy, for instance) without renumbering the
         * endpoints — whose addresses are baked into on-chain rosters and must survive a restart.
         *
         * @param index zero-based host index
         * @return the dotted-quad address
         * @throws IllegalArgumentException if {@code index} would leave the subnet
         */
        public String host(final int index) {
            if (index < 0 || index > 244) {
                throw new IllegalArgumentException("host index out of range for a /24: " + index);
            }
            return FIRST_OCTET + "." + SECOND_OCTET + "." + thirdOctet + "." + (10 + index);
        }

        /**
         * An address in the low range reserved for infrastructure containers (Toxiproxy and friends).
         *
         * <p>Reserved so an infrastructure container can be added without renumbering the endpoints,
         * whose addresses are baked into on-chain rosters and must survive a restart.
         *
         * @param index zero-based infrastructure slot, {@code 0..7} → {@code .2..\.9}
         * @return the dotted-quad address
         * @throws IllegalArgumentException if {@code index} would collide with the gateway or with
         *     the host range starting at {@code .10}
         */
        public String infrastructureHost(final int index) {
            if (index < 0 || index > 7) {
                throw new IllegalArgumentException("infrastructure slot out of range (0..7): " + index);
            }
            return FIRST_OCTET + "." + SECOND_OCTET + "." + thirdOctet + "." + (2 + index);
        }

        @Override
        public void close() {
            // The flag is part of the guarded state: reading it outside the lock would let two
            // concurrent closes both clear the bit, freeing a slot another environment already took.
            synchronized (IN_USE) {
                if (released) {
                    return;
                }
                released = true;
                IN_USE.clear(bit);
            }
        }
    }

    /**
     * Lease a free /24.
     *
     * @return the lease
     * @throws IllegalStateException if every slot is in use (a leak: environments are not closing)
     */
    public static Lease lease() {
        synchronized (IN_USE) {
            final int bit = IN_USE.nextClearBit(0);
            if (bit >= MAX_SLOTS) {
                throw new IllegalStateException("no free E2E subnet slot — " + MAX_SLOTS
                        + " environments are open, which means teardown is leaking");
            }
            IN_USE.set(bit);
            return new Lease(bit);
        }
    }
}
