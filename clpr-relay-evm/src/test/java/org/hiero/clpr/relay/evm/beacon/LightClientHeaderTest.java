// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.beacon;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.List;
import org.hiero.clpr.relay.evm.ssz.Ssz;
import org.junit.jupiter.api.Test;

class LightClientHeaderTest {

    private static Bytes det(final int len, final int seed) {
        final byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[i] = (byte) (seed + i * 31);
        }
        return Bytes.wrap(b);
    }

    private static ExecutionPayloadHeader sampleEph() {
        return new ExecutionPayloadHeader(
                det(32, 0x01), // parent_hash
                det(20, 0x02), // fee_recipient
                det(32, 0x03), // state_root
                det(32, 0x04), // receipts_root
                det(256, 0x05), // logs_bloom
                det(32, 0x06), // prev_randao
                123_456L, // block_number
                30_000_000L, // gas_limit
                21_000L, // gas_used
                1_700_000_000L, // timestamp
                det(12, 0x07), // extra_data (≤ 32 bytes)
                BigInteger.valueOf(1_000_000_007L), // base_fee_per_gas
                det(32, 0x08), // block_hash
                det(32, 0x09), // transactions_root
                det(32, 0x0A), // withdrawals_root
                262_144L, // blob_gas_used
                131_072L); // excess_blob_gas
    }

    @Test
    void executionStateRootBranch_foldsToBodyRoot() {
        final ExecutionPayloadHeader eph = sampleEph();

        // Body-level depth-4 execution_payload branch (arbitrary siblings).
        final List<Bytes> executionBranch = List.of(det(32, 0x21), det(32, 0x22), det(32, 0x23), det(32, 0x24));

        // Derive the body root the way the consensus tree does: fold the EPH root up the body branch
        // at the execution_payload body index (9), so the composed depth-9 fold must reproduce it.
        final byte[][] branch4 = {
            executionBranch.get(0).toByteArray(),
            executionBranch.get(1).toByteArray(),
            executionBranch.get(2).toByteArray(),
            executionBranch.get(3).toByteArray()
        };
        final byte[] bodyRoot = Ssz.branchRoot(eph.hashTreeRoot().toByteArray(), branch4, 9);

        final BeaconBlockHeader beacon =
                new BeaconBlockHeader(100L, 7L, det(32, 0x31), det(32, 0x32), Bytes.wrap(bodyRoot));
        final LightClientHeader header = new LightClientHeader(beacon, eph, executionBranch);

        assertThat(header.executionStateRootBranch().length).isEqualTo(Ssz.EXECUTION_STATE_ROOT_BRANCH_DEPTH);
        assertThat(header.executionStateRootBranchVerifies())
                .as("depth-9 branch must fold state_root to the beacon bodyRoot")
                .isTrue();

        // Direct check at the verifier's leaf index.
        assertThat(Ssz.verifyBranch(
                        eph.stateRoot().toByteArray(),
                        header.executionStateRootBranch(),
                        bodyRoot,
                        Ssz.EXECUTION_STATE_ROOT_LEAF_INDEX))
                .isTrue();
    }

    @Test
    void tamperedBodyRoot_failsSelfCheck() {
        final ExecutionPayloadHeader eph = sampleEph();
        final List<Bytes> executionBranch = List.of(det(32, 0x21), det(32, 0x22), det(32, 0x23), det(32, 0x24));
        final BeaconBlockHeader beacon =
                new BeaconBlockHeader(100L, 7L, det(32, 0x31), det(32, 0x32), det(32, 0x99)); // wrong bodyRoot
        final LightClientHeader header = new LightClientHeader(beacon, eph, executionBranch);
        assertThat(header.executionStateRootBranchVerifies()).isFalse();
    }
}
