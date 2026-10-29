// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Optional;

@FunctionalInterface
public interface ChannelLookup {
    Optional<ClprChannelOps> findById(final Bytes channelId);
}
