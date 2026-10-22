// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.chain;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.BlockHeaderRlpCodec;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.model.BlockHeader;
import org.hiero.clpr.relay.evm.qbft.StandardValidatorContractProofSource;
import org.hiero.clpr.relay.evm.qbft.ValidatorContractProofRlp;

/**
 * Builds the 7-field config proof {@code QbftContractModeVerifier.verifyConfig} demands — the
 * counterpart to {@link QbftConfigProofBuilder} for QBFT "contract" validator-selection mode.
 *
 * <p>Reuses {@link StandardValidatorContractProofSource} (the same production class
 * {@code QbftContractModeLedgerConfigurationProvider} and {@code QbftContractModeBundleConstructor}
 * use) to build the validator contract's proof, and {@link ValidatorContractProofRlp} to wrap it —
 * so this test-only proof assembly cannot silently drift from the wire shape the relay itself
 * produces at runtime.
 */
public final class QbftContractModeConfigProofBuilder {

    /** {@code QbftValidatorMode.Contract}, per {@code QbftValidators.sol}. */
    private static final long VALIDATOR_MODE_CONTRACT = 1L;

    private QbftContractModeConfigProofBuilder() {}

    /**
     * Build the config proof for {@code completeChannel} against a contract-mode
     * {@code QbftContractModeVerifier}, for a specific {@code ClprService} deployment on the peer
     * chain (see {@link QbftConfigProofBuilder#buildConfigProof} for why the exact service matters).
     *
     * @param peer the chain being proven — the channel's <em>other</em> side
     * @param peerServiceAddress the {@code ClprService} on {@code peer} the channel is registered on
     * @param validatorContract the peer chain's deployed validator contract
     * @param arraySlot storage slot of the validator contract's {@code address[]} array
     * @return the RLP-encoded proof
     */
    public static byte[] buildConfigProof(
            final BesuNetwork peer,
            final String peerServiceAddress,
            final Address validatorContract,
            final BigInteger arraySlot) {
        final EvmJsonRpcClient peerRpc = peer.rpc();
        final String peerChainId = peer.caip2();

        final String code = peer.rawRpc().ethGetCode(peerServiceAddress);
        if (code == null || code.isBlank() || "0x".equals(code)) {
            throw new IllegalStateException(
                    "no code at ClprService " + peerServiceAddress + " — cannot build a trust anchor for it");
        }
        final byte[] serviceCodeHash = AbiCodec.Keccak256.keccak256(AbiCodec.fromHex(code));

        final BlockHeader header = peerRpc.ethGetBlockHeaderByNumber("latest");
        final byte[] headerRlp = BlockHeaderRlpCodec.encodeRlp(header).toByteArray();

        final var validatorSource = new StandardValidatorContractProofSource(peerRpc, validatorContract, arraySlot);
        final var provenValidatorSet = validatorSource.proveValidatorSet(header.number());
        final byte[] validatorContractProofElem = ValidatorContractProofRlp.encodeWrapped(provenValidatorSet.proof());

        // Field [4] descriptor: RLP([mode=1, serviceAddress, codeHash, validatorContract, arraySlot]),
        // wrapped as a byte string.
        final byte[] descriptorElem = AbiCodec.rlpBytes(AbiCodec.rlpList(
                AbiCodec.rlpBigInt(BigInteger.valueOf(VALIDATOR_MODE_CONTRACT)),
                AbiCodec.rlpBytes(AbiCodec.fromHex(peerServiceAddress)),
                AbiCodec.rlpBytes(serviceCodeHash),
                AbiCodec.rlpBytes(validatorContract.value()),
                AbiCodec.rlpBytes(AbiCodec.encodeUint256(arraySlot))));

        return AbiCodec.rlpList(
                AbiCodec.rlpBytes(AbiCodec.fromHex(peerServiceAddress)), // [0] serviceAddress
                AbiCodec.rlpBytes(peerChainId.getBytes(StandardCharsets.UTF_8)), // [1] chainId
                AbiCodec.rlpBytes(new byte[0]), // [2] peerConfigNanos = 0
                AbiCodec.rlpList( // [3] throttles
                        AbiCodec.rlpBigInt(BigInteger.valueOf(QbftConfigProofBuilder.MAX_MESSAGES_PER_BUNDLE)),
                        AbiCodec.rlpBigInt(BigInteger.valueOf(QbftConfigProofBuilder.MAX_MESSAGE_PAYLOAD_BYTES)),
                        AbiCodec.rlpBigInt(BigInteger.valueOf(QbftConfigProofBuilder.MAX_GAS_PER_MESSAGE)),
                        AbiCodec.rlpBigInt(BigInteger.valueOf(QbftConfigProofBuilder.MAX_QUEUE_DEPTH)),
                        AbiCodec.rlpBigInt(BigInteger.valueOf(QbftConfigProofBuilder.MAX_SYNC_BYTES))),
                descriptorElem, // [4] descriptor, wrapped
                validatorContractProofElem, // [5] validator contract proof, wrapped
                headerRlp); // [6] header, spliced as a nested list
    }
}
