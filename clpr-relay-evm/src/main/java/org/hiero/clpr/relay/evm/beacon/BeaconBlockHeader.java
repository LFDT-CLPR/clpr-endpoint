// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.beacon;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import org.hiero.clpr.relay.evm.ssz.Ssz;

/**
 * The 5-field SSZ {@code BeaconBlockHeader} container as served by the beacon-node REST API. The
 * bundle/config payload carries these fields RLP-encoded; the peer verifier SSZ-hashes them into the
 * beacon block root the sync committee signed.
 *
 * @param slot          the beacon slot
 * @param proposerIndex the proposer validator index
 * @param parentRoot    32-byte parent block root
 * @param stateRoot     32-byte beacon state root
 * @param bodyRoot      32-byte beacon block body root (the root the execution branch verifies against)
 */
public record BeaconBlockHeader(long slot, long proposerIndex, Bytes parentRoot, Bytes stateRoot, Bytes bodyRoot) {

    /**
     * {@code hash_tree_root}: the 5 field leaves padded with 3 zero chunks to 8 and merkleized.
     * This is the beacon block root the sync committee signs (via the signing-root wrapper).
     */
    public Bytes hashTreeRoot() {
        final byte[][] leaves = {
            Ssz.uint64Leaf(slot),
            Ssz.uint64Leaf(proposerIndex),
            parentRoot.toByteArray(),
            stateRoot.toByteArray(),
            bodyRoot.toByteArray(),
            new byte[32],
            new byte[32],
            new byte[32]
        };
        return Bytes.wrap(Ssz.merkleize(leaves));
    }
}
