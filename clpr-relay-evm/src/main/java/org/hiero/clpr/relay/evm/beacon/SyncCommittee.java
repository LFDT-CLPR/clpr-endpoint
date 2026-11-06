// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.beacon;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.clpr.relay.evm.ssz.Ssz;

/**
 * The SSZ {@code SyncCommittee} container: the 512 member BLS pubkeys plus their precomputed
 * aggregate. Pinned by the trust anchor and by the {@code BeaconState.{current,next}_sync_committee}
 * branches.
 *
 * @param pubkeys         exactly {@link Ssz#SYNC_COMMITTEE_SIZE} 48-byte ZCash/beacon-chain compressed BLS12-381 G1 public keys
 * @param aggregatePubkey the 48-byte compressed aggregate public key
 */
public record SyncCommittee(List<Bytes> pubkeys, Bytes aggregatePubkey) {

    public SyncCommittee {
        if (pubkeys.size() != Ssz.SYNC_COMMITTEE_SIZE) {
            throw new IllegalArgumentException(
                    "sync committee must have " + Ssz.SYNC_COMMITTEE_SIZE + " pubkeys, got " + pubkeys.size());
        }
        pubkeys = List.copyOf(pubkeys);
    }

    /**
     * {@code hash_tree_root}: the root of the 512-pubkey vector paired with the aggregate pubkey's
     * root — the value the trust anchor and the sync-committee branches pin.
     */
    public Bytes hashTreeRoot() {
        final byte[][] pubkeyRoots = new byte[Ssz.SYNC_COMMITTEE_SIZE][];
        for (int i = 0; i < Ssz.SYNC_COMMITTEE_SIZE; i++) {
            pubkeyRoots[i] = Ssz.pubkeyHash64(pubkeys.get(i).toByteArray());
        }
        final byte[] pubkeysRoot = Ssz.merkleize(pubkeyRoots);
        return Bytes.wrap(Ssz.sha256(pubkeysRoot, Ssz.pubkeyHash64(aggregatePubkey.toByteArray())));
    }
}
