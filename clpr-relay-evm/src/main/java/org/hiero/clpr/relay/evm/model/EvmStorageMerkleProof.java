// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.model;

import static org.hiero.clpr.relay.evm.AbiCodec.rlpBytes;
import static org.hiero.clpr.relay.evm.AbiCodec.rlpList;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;

/**
 * An EIP-1186 Merkle-Patricia proof of one contract storage slot: the slot key and the proof nodes
 * from the account's storage root down to the slot. Unlike {@link ProofResponse.StorageProofEntry}
 * it carries no slot value — the verifiers read the value from the proof's leaf node.
 *
 * <p>Shared by every EVM bundle proof (QBFT header and contract mode, Ethereum PoS) and by the QBFT
 * contract-mode validator-array proof.
 */
public record EvmStorageMerkleProof(Bytes key, List<Bytes> proof) {

    /** RLP-encodes this proof as the {@code [slotKey, [nodes]]} entry, with the key as given. */
    public byte[] encodeRlp() {
        return rlpList(
                rlpBytes(key.toByteArray()),
                rlpList(proof.stream().map(b -> rlpBytes(b.toByteArray())).toArray(byte[][]::new)));
    }
}
