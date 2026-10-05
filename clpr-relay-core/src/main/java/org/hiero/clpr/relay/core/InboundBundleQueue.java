// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import com.hedera.hapi.node.state.clpr.ClprEndpoint;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import org.jspecify.annotations.Nullable;

@FunctionalInterface
public interface InboundBundleQueue {
    void offer(Bytes rawBundleBytes, @Nullable ClprEndpoint peer);
}
