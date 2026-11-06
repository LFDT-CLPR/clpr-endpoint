// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.beacon;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * A beacon {@code LightClientUpdate} (or finality/optimistic update): the attested header signed by
 * the current sync committee, the aggregate signature, and — for a sync-committee period boundary —
 * the next committee plus its SSZ branch.
 *
 * <p>For a steady-state bundle the relay uses {@code attestedHeader} + {@code syncAggregate}; for a
 * rotation it additionally carries {@code nextSyncCommittee} + {@code nextSyncCommitteeBranch}
 * (both {@code null} otherwise).
 *
 * @param attestedHeader          the attested {@code LightClientHeader}
 * @param syncAggregate           the committee's aggregate signature over {@code attestedHeader}
 * @param signatureSlot           the slot at which the aggregate was produced
 * @param nextSyncCommittee       the next period's sync committee, or {@code null}
 * @param nextSyncCommitteeBranch the depth-6 {@code next_sync_committee} branch, or {@code null}
 */
public record LightClientUpdate(
        LightClientHeader attestedHeader,
        SyncAggregate syncAggregate,
        long signatureSlot,
        @Nullable SyncCommittee nextSyncCommittee,
        @Nullable List<Bytes> nextSyncCommitteeBranch) {

    public LightClientUpdate {
        nextSyncCommitteeBranch = nextSyncCommitteeBranch == null ? null : List.copyOf(nextSyncCommitteeBranch);
    }

    public boolean hasRotation() {
        return nextSyncCommittee != null;
    }
}
