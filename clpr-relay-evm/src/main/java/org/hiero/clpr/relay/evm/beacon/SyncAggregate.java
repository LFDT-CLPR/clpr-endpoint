// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.evm.beacon;

import com.hedera.pbj.runtime.io.buffer.Bytes;

/**
 * A beacon {@code SyncAggregate}: the 512-bit participation bitvector (64 bytes) and the 96-byte
 * BLS12-381 G2 aggregate signature over the attested header's signing root. Relayed verbatim; the
 * peer verifier checks supermajority participation and BLS-verifies the signature.
 *
 * @param bits      64-byte SSZ {@code Bitvector[512]}; bit {@code i} = bit {@code i % 8} of byte {@code i / 8}
 * @param signature 256-byte EIP-2537 uncompressed BLS12-381 G2 aggregate signature
 */
public record SyncAggregate(Bytes bits, Bytes signature) {}
