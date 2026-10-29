// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import com.hedera.hapi.node.state.clpr.BundleRequest;

public interface BundleRequestConstructor {
    BundleRequest createBundleRequest();
}
