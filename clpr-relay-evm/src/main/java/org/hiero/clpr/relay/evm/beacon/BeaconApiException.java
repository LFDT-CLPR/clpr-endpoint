// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.beacon;

/** Raised when a beacon-node REST request fails or returns a malformed/unexpected response. */
public class BeaconApiException extends RuntimeException {
    public BeaconApiException(final String message) {
        super(message);
    }

    public BeaconApiException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
