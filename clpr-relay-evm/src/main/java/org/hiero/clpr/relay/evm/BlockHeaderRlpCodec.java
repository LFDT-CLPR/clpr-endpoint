// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm;

import static org.hiero.clpr.relay.evm.AbiCodec.rlpBigInt;
import static org.hiero.clpr.relay.evm.AbiCodec.rlpBytes;
import static org.hiero.clpr.relay.evm.AbiCodec.rlpList;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.ArrayList;
import java.util.List;
import org.hiero.clpr.relay.evm.model.BlockHeader;
import org.hiero.clpr.relay.evm.qbft.QbftBundleConstructor;
import org.hiero.clpr.relay.evm.qbft.QbftLedgerConfigurationProvider;

/**
 * RLP-encodes Ethereum block headers.
 *
 * <p>Produces the canonical RLP byte string whose keccak256 yields the block hash. Mirrors
 * Besu's {@code BlockHeader.writeTo} field order exactly, including the optional fields
 * (EIP-1559, EIP-4895, EIP-4844, EIP-4788, EIP-7685).
 *
 * <p>Consumers: {@link QbftBundleConstructor} (for the bundle proof's first RLP element) and
 * {@link QbftLedgerConfigurationProvider} (for the headers in the getLedgerConfiguration
 * payload). Header parsing from JSON lives in
 * {@link org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient}.
 */
public final class BlockHeaderRlpCodec {

    private BlockHeaderRlpCodec() {}

    /**
     * Encode a {@link BlockHeader} as the canonical RLP byte string whose keccak256 yields the
     * block hash. Mirrors Besu's {@code BlockHeader.writeTo} field order including the
     * post-fork optional fields (EIP-1559, EIP-4895, EIP-4844, EIP-4788, EIP-7685).
     *
     * @param header the header to encode
     * @return the RLP-encoded header bytes
     */
    public static Bytes encodeRlp(final BlockHeader header) {
        return Bytes.wrap(encodeRlpBytes(header));
    }

    /**
     * {@link #encodeRlp} variant that returns the raw {@code byte[]} — useful inside other RLP
     * encoders that compose the header into a larger list.
     */
    public static byte[] encodeRlpBytes(final BlockHeader h) {
        final List<byte[]> fields = new ArrayList<>();
        fields.add(rlpBytes(h.parentHash().toByteArray()));
        fields.add(rlpBytes(h.sha3Uncles().toByteArray()));
        fields.add(rlpBytes(h.miner().value())); // 20 bytes
        fields.add(rlpBytes(h.stateRoot().toByteArray()));
        fields.add(rlpBytes(h.transactionsRoot().toByteArray()));
        fields.add(rlpBytes(h.receiptsRoot().toByteArray()));
        fields.add(rlpBytes(h.logsBloom().toByteArray())); // 256 bytes
        fields.add(rlpBigInt(h.difficulty()));
        fields.add(rlpBigInt(h.number()));
        fields.add(rlpBigInt(h.gasLimit()));
        fields.add(rlpBigInt(h.gasUsed()));
        fields.add(rlpBigInt(h.timestamp()));
        fields.add(rlpBytes(h.extraData().toByteArray()));
        fields.add(rlpBytes(h.mixHash().toByteArray()));
        fields.add(rlpBytes(h.nonce().toByteArray())); // 8 bytes, NOT stripped

        // Optional post-fork fields. Mirror Besu's BlockHeader.writeTo short-circuit
        // semantics exactly: each field requires every preceding optional field to have been
        // written, otherwise we break out. Diverging from this order — or skipping a field
        // that Besu actually serialized — would change the keccak digest and break QBFT
        // committed-seal recovery on the Hiero verifier.
        // Reference: besu/ethereum/core/.../BlockHeader.java#writeTo @ tag 26.5.0.
        do {
            if (h.baseFeePerGas() == null) break; // EIP-1559 (London)
            fields.add(rlpBigInt(h.baseFeePerGas()));

            if (h.withdrawalsRoot() == null) break; // EIP-4895 (Shanghai)
            fields.add(rlpBytes(h.withdrawalsRoot().toByteArray()));

            if (h.blobGasUsed() == null || h.excessBlobGas() == null) break; // EIP-4844 (Cancun)
            fields.add(rlpBigInt(h.blobGasUsed()));
            fields.add(rlpBigInt(h.excessBlobGas()));

            if (h.parentBeaconBlockRoot() == null) break; // EIP-4788 (Cancun)
            fields.add(rlpBytes(h.parentBeaconBlockRoot().toByteArray()));

            if (h.requestsHash() == null) break; // EIP-7685 (Prague)
            fields.add(rlpBytes(h.requestsHash().toByteArray()));
        } while (false);
        // blockHash is derived — NOT included in RLP

        return rlpList(fields.toArray(byte[][]::new));
    }
}
