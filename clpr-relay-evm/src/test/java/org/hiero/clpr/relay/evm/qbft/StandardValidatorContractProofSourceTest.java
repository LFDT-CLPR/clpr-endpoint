// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.qbft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.hiero.clpr.relay.evm.ByteUtils;
import org.hiero.clpr.relay.evm.model.Address;
import org.hiero.clpr.relay.evm.model.ProofResponse;
import org.hiero.clpr.relay.evm.testfixtures.TestEvmJsonRpcClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StandardValidatorContractProofSourceTest {

    /**
     * Echoes back whatever slot keys {@link StandardValidatorContractProofSource} actually
     * computes and requests, so the test never needs to reproduce its keccak256 element-slot
     * arithmetic: the first call (length only) returns {@code length}; the second call (length +
     * every element slot) returns {@code length} for the first key and {@code addressWords[i-1]}
     * (a 32-byte, right-aligned address word) for each subsequent key, in request order.
     */
    private static class StubRpcClient extends TestEvmJsonRpcClient {
        private String lengthHex;
        private List<String> addressWordsHex = List.of();
        private boolean emptyAccountProofOnSecondCall;
        private boolean emptySecondResponse;

        final List<String[]> requestedStorageKeys = new ArrayList<>();
        String lastAddress;
        String lastBlockTag;

        void configure(String lengthHex, List<String> addressWordsHex) {
            this.lengthHex = lengthHex;
            this.addressWordsHex = addressWordsHex;
        }

        void makeSecondResponseEmpty() {
            this.emptySecondResponse = true;
        }

        void makeSecondResponseAccountProofEmpty() {
            this.emptyAccountProofOnSecondCall = true;
        }

        @Override
        public ProofResponse ethGetProof(String address, String[] storageKeys, String blockTag) {
            this.lastAddress = address;
            this.lastBlockTag = blockTag;
            this.requestedStorageKeys.add(storageKeys);

            if (storageKeys.length == 1) {
                return new ProofResponse(List.of(), List.of(entry(storageKeys[0], lengthHex)));
            }
            if (emptySecondResponse) {
                return new ProofResponse(List.of(), List.of());
            }
            final var accountProof =
                    emptyAccountProofOnSecondCall ? List.<Bytes>of() : List.of(ByteUtils.fromPrefixedHex("0xf8518080"));
            final var storageProof = new ArrayList<ProofResponse.StorageProofEntry>();
            storageProof.add(entry(storageKeys[0], lengthHex));
            for (int i = 1; i < storageKeys.length; i++) {
                storageProof.add(entry(storageKeys[i], addressWordsHex.get(i - 1)));
            }
            return new ProofResponse(accountProof, storageProof);
        }
    }

    private static ProofResponse.StorageProofEntry entry(final String keyHex, final String valueHex) {
        return new ProofResponse.StorageProofEntry(
                ByteUtils.fromPrefixedHex(keyHex),
                ByteUtils.fromPrefixedHex(valueHex),
                List.of(ByteUtils.fromPrefixedHex("0xf8518080")));
    }

    private static String addressWord(final String addressHex40) {
        return "0x" + "00".repeat(12) + addressHex40;
    }

    static final Address VALIDATOR_CONTRACT = Address.fromHexString("0x" + "aa".repeat(20));
    static final BigInteger ARRAY_SLOT = BigInteger.TWO;

    StubRpcClient rpcClient;
    StandardValidatorContractProofSource source;

    @BeforeEach
    void setUp() {
        rpcClient = new StubRpcClient();
        source = new StandardValidatorContractProofSource(rpcClient, VALIDATOR_CONTRACT, ARRAY_SLOT);
    }

    @Test
    void readsAndSortsTwoValidatorsOutOfOrder() {
        final var addr1 = "bb".repeat(20);
        final var addr2 = "11".repeat(20);
        rpcClient.configure("0x02", List.of(addressWord(addr1), addressWord(addr2)));

        final var proven = source.proveValidatorSet(BigInteger.valueOf(100));

        assertThat(proven.set().members())
                .containsExactly(Address.fromHexString("0x" + addr2), Address.fromHexString("0x" + addr1));
        assertThat(proven.proof().validatorContract()).isEqualTo(VALIDATOR_CONTRACT);
        assertThat(proven.proof().storageProof()).hasSize(3);
        assertThat(rpcClient.lastAddress).isEqualTo(VALIDATOR_CONTRACT.toHexString());
        assertThat(rpcClient.lastBlockTag).isEqualTo("0x64");
    }

    @Test
    void duplicateValidatorThrows() {
        // The verifier itself reverts on a duplicate (DuplicateValidator in
        // QbftContractValidatorSource.sol) rather than deduping — this source must reject the same
        // condition instead of silently correcting it.
        final var addr = "cc".repeat(20);
        rpcClient.configure("0x02", List.of(addressWord(addr), addressWord(addr)));

        assertThatThrownBy(() -> source.proveValidatorSet(BigInteger.ONE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("duplicate");
    }

    @Test
    void zeroAddressThrows() {
        // The verifier reverts on the zero address too (ZeroValidator).
        rpcClient.configure("0x01", List.of(addressWord("00".repeat(20))));

        assertThatThrownBy(() -> source.proveValidatorSet(BigInteger.ONE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("zero address");
    }

    @Test
    void lengthAboveMaxValidatorsThrows() {
        // 0x101 = 257, one past ClprQbftQuorum.MAX_VALIDATORS (256) — must be rejected before any
        // element-slot request is built, so the stub's second-call configuration is irrelevant here.
        rpcClient.configure("0x101", List.of());

        assertThatThrownBy(() -> source.proveValidatorSet(BigInteger.ONE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("257")
                .hasMessageContaining("256");
    }

    @Test
    void emptyArrayThrows() {
        rpcClient.configure("0x00", List.of());

        assertThatThrownBy(() -> source.proveValidatorSet(BigInteger.ONE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("empty");
    }

    @Test
    void emptyAccountProofThrows() {
        rpcClient.configure("0x01", List.of(addressWord("dd".repeat(20))));
        rpcClient.makeSecondResponseAccountProofEmpty();

        assertThatThrownBy(() -> source.proveValidatorSet(BigInteger.ONE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("account proof");
    }

    @Test
    void missingElementSlotsThrows() {
        rpcClient.configure("0x01", List.of(addressWord("dd".repeat(20))));
        rpcClient.makeSecondResponseEmpty();

        assertThatThrownBy(() -> source.proveValidatorSet(BigInteger.ONE)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void requestsLengthSlotThenLengthPlusElementSlots() {
        rpcClient.configure("0x01", List.of(addressWord("dd".repeat(20))));

        source.proveValidatorSet(BigInteger.ONE);

        assertThat(rpcClient.requestedStorageKeys).hasSize(2);
        assertThat(rpcClient.requestedStorageKeys.get(0)).hasSize(1);
        assertThat(rpcClient.requestedStorageKeys.get(1)).hasSize(2);
        // The length slot is requested identically both times.
        assertThat(rpcClient.requestedStorageKeys.get(1)[0])
                .isEqualTo(rpcClient.requestedStorageKeys.get(0)[0]);
    }
}
