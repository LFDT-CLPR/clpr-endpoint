// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.beacon;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;

/**
 * A beacon {@code LightClientBootstrap}: the attested header, the current sync committee, and the
 * depth-6 {@code current_sync_committee} SSZ branch proving the committee under the header's
 * {@code stateRoot}. The relay uses this to assemble the Ethereum-PoS ledger-config payload and seed
 * the connection's initial trust anchor.
 *
 * @param header                     the {@code LightClientHeader}
 * @param currentSyncCommittee       the current period's sync committee
 * @param currentSyncCommitteeBranch the depth-6 {@code current_sync_committee} branch (bottom-up)
 */
public record LightClientBootstrap(
        LightClientHeader header, SyncCommittee currentSyncCommittee, List<Bytes> currentSyncCommitteeBranch) {

    public LightClientBootstrap {
        currentSyncCommitteeBranch = List.copyOf(currentSyncCommitteeBranch);
    }
}
