// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import com.hedera.pbj.runtime.io.buffer.Bytes;

/**
 * An interface used to interact with a CLPR channel that is being handled by this instance.
 */
public interface ClprChannelOps {
    Bytes channelId();

    BundleConstructor bundleConstructor();

    BundleRequestConstructor bundleRequestConstructor();

    InboundBundleQueue inboundBundleQueue();

    ContractStateReader contractStateReader();

    PeerEndpointCache peerEndpointCache();
}
