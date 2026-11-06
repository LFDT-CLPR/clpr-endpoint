// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.InputStream;
import org.junit.jupiter.api.Test;

/**
 * Parses a real {@code StateProof} captured from a Hiero node ({@code stateProof.bin}) through
 * {@link HieroProofCodec}, to confirm the wire-walk decoder handles production bytes (not just
 * hand-built fixtures).
 */
class HieroProofRealStateProofTest {

    @Test
    void parsesRealStateProofFromHieroNode() throws Exception {
        final byte[] raw;
        try (InputStream in = HieroProofRealStateProofTest.class.getResourceAsStream("/stateProof.bin")) {
            assertThat(in).as("stateProof.bin must be on the test classpath").isNotNull();
            raw = in.readAllBytes();
        }
        final Bytes proof = Bytes.wrap(raw);

        final ParsedBundle verified = new HieroProofCodec().decodeBundle(proof);

        // Surface what we decoded in the assertion description so a failure (or -i run) shows it.
        assertThat(verified.metadata())
                .as(
                        "decoded metadata nextMessageId=%d receivedMessageId=%d state=%s, messages=%d",
                        verified.metadata().nextMessageId(),
                        verified.metadata().receivedMessageId(),
                        verified.metadata().status(),
                        verified.messages().size())
                .isNotNull();
        assertThat(verified.rawBundleBytes()).isEqualTo(proof);
    }
}
