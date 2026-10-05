// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprQueueMetadata;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;

/**
 * A bundle of CLPR messages parsed from a peer's proof, ready for submission.
 *
 * <p>"Parsed" means decoded, not cryptographically checked: the proof is verified on chain by the
 * verifier contract during {@code submitBundle}, not here.
 *
 * @param metadata the queue metadata associated with this bundle
 * @param messages the ordered list of message payloads in this bundle
 * @param rawBundleBytes the proof bytes submitted verbatim to the on-chain {@code submitBundle} call so
 *                      the verifier contract receives the wire format its chain produced
 */
public record ParsedBundle(ClprQueueMetadata metadata, List<ClprMessagePayload> messages, Bytes rawBundleBytes) {}
