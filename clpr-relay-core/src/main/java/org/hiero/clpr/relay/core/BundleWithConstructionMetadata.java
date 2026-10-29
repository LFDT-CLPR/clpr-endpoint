// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Objects;

/**
 * The output of a {@link BundleConstructor}: the opaque proof payload together with the local chain
 * height it was proven against.
 *
 * <p>The sync layer strips the metadata off and assembles the on-wire {@code BundleResponse} from
 * {@link #bundlePayload()}. The {@link #constructedAtHeight()} is the caching key dimension: because a
 * bundle built for a fixed {@code (channelId, request)} against a fixed local chain height is always
 * identical, {@link CachedBundleConstructor} memoizes on {@code (channelId, request, constructedAtHeight)}
 * so a burst of identical requests at the same height is served without rebuilding the proof.
 *
 * @param bundlePayload      the opaque proof bytes ({@link Bytes#EMPTY} when there is nothing to send)
 * @param constructedAtHeight the local chain height the bundle was proven against
 */
public record BundleWithConstructionMetadata(Bytes bundlePayload, long constructedAtHeight) {

    public BundleWithConstructionMetadata {
        Objects.requireNonNull(bundlePayload, "bundlePayload");
    }
}
