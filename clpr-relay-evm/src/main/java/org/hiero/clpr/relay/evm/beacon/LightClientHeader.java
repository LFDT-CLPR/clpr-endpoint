// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.beacon;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.ArrayList;
import java.util.List;
import org.hiero.clpr.relay.evm.ssz.Ssz;

/**
 * A Capella+ {@code LightClientHeader}: the beacon block header, its execution payload header, and
 * the depth-4 SSZ branch proving the {@code execution_payload} under the beacon block
 * {@code bodyRoot}.
 *
 * @param beacon          the beacon block header
 * @param execution       the execution payload header
 * @param executionBranch the depth-4 {@code execution_payload}-in-body branch (bottom-up siblings)
 */
public record LightClientHeader(
        BeaconBlockHeader beacon, ExecutionPayloadHeader execution, List<Bytes> executionBranch) {

    public LightClientHeader {
        executionBranch = List.copyOf(executionBranch);
    }

    /**
     * Composes the full depth-9 SSZ branch from {@code execution_payload.state_root} up to the beacon
     * {@code bodyRoot}: the depth-5 {@code state_root}-within-{@code ExecutionPayloadHeader} sub-branch
     * followed by the depth-4 {@code execution_payload}-in-body branch. Folding the execution state
     * root through this branch at leaf index {@link Ssz#EXECUTION_STATE_ROOT_LEAF_INDEX} reproduces
     * {@code beacon.bodyRoot} — the exact check the peer verifier performs.
     */
    public byte[][] executionStateRootBranch() {
        final byte[][] sub = execution.stateRootSubBranch(); // depth 5, bottom
        if (executionBranch.size() + sub.length != Ssz.EXECUTION_STATE_ROOT_BRANCH_DEPTH) {
            throw new IllegalStateException("execution_branch must have "
                    + (Ssz.EXECUTION_STATE_ROOT_BRANCH_DEPTH - sub.length) + " nodes, got " + executionBranch.size());
        }
        final byte[][] branch = new byte[Ssz.EXECUTION_STATE_ROOT_BRANCH_DEPTH][];
        for (int i = 0; i < sub.length; i++) {
            branch[i] = sub[i];
        }
        for (int i = 0; i < executionBranch.size(); i++) {
            branch[sub.length + i] = executionBranch.get(i).toByteArray();
        }
        return branch;
    }

    /**
     * Validates that {@link #executionStateRootBranch()} folds the execution state root to the beacon
     * body root — a self-check the producer runs before emitting, so a malformed branch fails locally
     * instead of at the peer verifier.
     */
    public boolean executionStateRootBranchVerifies() {
        return Ssz.verifyBranch(
                execution.stateRoot().toByteArray(),
                executionStateRootBranch(),
                beacon.bodyRoot().toByteArray(),
                Ssz.EXECUTION_STATE_ROOT_LEAF_INDEX);
    }

    /** Convenience: the proven execution-layer state root carried by this header. */
    public Bytes executionStateRoot() {
        return execution.stateRoot();
    }

    /** Defensive copy accessor for the depth-4 execution branch as raw node arrays. */
    public byte[][] executionBranchNodes() {
        final List<byte[]> nodes = new ArrayList<>(executionBranch.size());
        for (final Bytes b : executionBranch) {
            nodes.add(b.toByteArray());
        }
        return nodes.toArray(new byte[0][]);
    }
}
