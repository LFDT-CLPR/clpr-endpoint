// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import com.hedera.hapi.node.state.clpr.ClprEndpoint;
import com.hedera.hapi.node.state.clpr.ClprServiceEndpoint;

public class Peers {
    /**
     * Returns a human-readable label for {@code peer}, suitable for logging and metric tags. Prefers
     * {@code "host:port"} when a service endpoint is known; falls back to {@code "unknown"}.
     */
    public static String peerLabel(final ClprEndpoint peer) {
        final var ep = peer.serviceEndpoint();
        if (ep != null && !ep.ipAddress().isBlank()) {
            return ep.ipAddress() + ":" + ep.port();
        }
        return "unknown";
    }

    /**
     * Returns the identity of {@code peer} within one peer manifest: its advertised service
     * endpoint {@code (host, port)}. {@code ClprEndpoint.account_id} is deliberately not used — the
     * spec does not define it, the network does not rely on it, and it may be empty or duplicated.
     * An endpoint without a service endpoint maps to {@link ClprServiceEndpoint#DEFAULT}; such an
     * endpoint is not dialable, so collapsing all of them into one key is harmless.
     *
     * @param peer the peer endpoint
     * @return the key identifying {@code peer}
     */
    public static ClprServiceEndpoint peerKey(final ClprEndpoint peer) {
        return peer.serviceEndpointOrElse(ClprServiceEndpoint.DEFAULT);
    }
}
