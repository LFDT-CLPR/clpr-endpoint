// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.app;

import static org.assertj.core.api.Assertions.assertThat;

import org.hiero.clpr.relay.core.metrics.SimpleMetrics;
import org.hiero.clpr.relay.evm.EthSigner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link LocalNetworkAdapter}: the client wiring done by {@link
 * LocalNetworkAdapter#create} and the {@link LocalNetworkAdapter#accountSubmitterFor} sharing rule
 * that the nonce-collision fix (issue #258, PR #283) rests on.
 *
 * <p>{@link org.hiero.clpr.relay.evm.AccountTransactionSubmitter} guarantees that <em>one</em>
 * submitter never reuses a nonce — its single serial worker reads {@code latest} once per request and
 * confirms each transaction before dequeuing the next. That guarantee only removes cross-channel
 * collisions if every channel signing from the same key on the same chain actually resolves to
 * <em>the same submitter instance</em>. Each channel independently calls
 * {@code network.accountSubmitterFor(signer)} during {@code ClprChannelHandler.create()}; if that
 * ever handed back distinct instances, each would start its own worker thread, each would read
 * {@code latest} independently, and the collision would be back regardless of how well-behaved any
 * individual submitter is.
 *
 * <p>The nonce space is per {@code (chain, account)}, so the cache key is the signing <b>address
 * alone</b> — deliberately not the {@code ClprService} address. Two {@code clprServices[]} entries on
 * one network sharing a key must share one submitter; the contract they submit to is layered on top
 * as a {@code forContract(...)} view, not baked into the key. {@link
 * #sameKeyAcrossServices_sharesOneSubmitter} pins that, because narrowing the key to
 * {@code (address, serviceAddress)} is a plausible-looking change that would silently reintroduce the
 * bug for multi-service deployments.
 *
 * <p>No EVM node is needed: {@link LocalNetworkAdapter#create} only builds clients and registers a
 * metrics updater that fires on scrape, and a started submitter's worker blocks on an empty queue
 * without issuing any RPC.
 */
class LocalNetworkAdapterTest {

    /** Anvil dev account 0 — test use only. */
    private static final String KEY_A = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    /** Anvil dev account 1 — a distinct signing account, so a distinct nonce space. */
    private static final String KEY_B = "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";

    private LocalNetworkAdapter adapter;

    @AfterEach
    void tearDown() {
        if (adapter != null) {
            // Releases the worker thread each submitter started.
            adapter.close();
        }
    }

    private static RelayConfig.CommonEvmParams evm(final String url) {
        return new RelayConfig.CommonEvmParams(url, 1L, Long.MAX_VALUE, 0L, 1.2, 1000L, 30_000L, 3);
    }

    private static RelayConfig.LocalNetworkConfig net(final String id) {
        return new RelayConfig.LocalNetworkConfig(
                id,
                ProofType.QBFT,
                evm("http://localhost:8545"),
                null,
                new RelayConfig.QbftConfig(30_000L, 5, 10, 1_048_576L),
                null);
    }

    private LocalNetworkAdapter adapter(final String networkId) {
        adapter = LocalNetworkAdapter.create(net(networkId), new SimpleMetrics());
        return adapter;
    }

    @Test
    void create_qbftNetwork_buildsClientsWithoutCometBft() {
        final var config = net("besu1");
        adapter = LocalNetworkAdapter.create(config, new SimpleMetrics());

        assertThat(adapter.id()).isEqualTo("besu1");
        assertThat(adapter.proofType()).isEqualTo(ProofType.QBFT);
        assertThat(adapter.config()).isSameAs(config);
        assertThat(adapter.rpcClient()).isNotNull();
        assertThat(adapter.gasStrategy()).isNotNull();
        assertThat(adapter.cometBftRpcClient()).isEmpty();
    }

    @Test
    void create_cometBftNetwork_buildsCometBftClient() {
        final var config = new RelayConfig.LocalNetworkConfig(
                "sei1",
                ProofType.CometBFT,
                evm("http://localhost:8545"),
                new RelayConfig.CometBftConfig("http://localhost:26657", 10, 10, 3, 30_000L, 1_048_576L),
                null,
                null);
        adapter = LocalNetworkAdapter.create(config, new SimpleMetrics());

        assertThat(adapter.proofType()).isEqualTo(ProofType.CometBFT);
        assertThat(adapter.cometBftRpcClient()).isPresent();
    }

    @Test
    void sameKey_returnsTheSameSubmitterInstance() {
        final var network = adapter("besu1");

        // Two channels registering independently each build their own EthSigner from the same
        // configured key — exactly what ClprServiceHandler.addChannel does per channel.
        final var first = network.accountSubmitterFor(new EthSigner(KEY_A));
        final var second = network.accountSubmitterFor(new EthSigner(KEY_A));

        // Same instance => one worker thread => one nonce reader for this account.
        assertThat(second).isSameAs(first);
    }

    @Test
    void sameKeyManyChannels_allShareOneSubmitter() {
        final var network = adapter("besu1");
        final var expected = network.accountSubmitterFor(new EthSigner(KEY_A));

        // The production shape: many channels discovered over time, all on one signing key.
        for (int i = 0; i < 100; i++) {
            assertThat(network.accountSubmitterFor(new EthSigner(KEY_A))).isSameAs(expected);
        }
    }

    @Test
    void sameKeyAcrossServices_sharesOneSubmitter() {
        final var network = adapter("besu1");
        final var signer = new EthSigner(KEY_A);
        final var shared = network.accountSubmitterFor(signer);

        assertThat(network.accountSubmitterFor(new EthSigner(KEY_A))).isSameAs(shared);
    }

    @Test
    void keyWrittenWithAndWithoutHexPrefix_sharesOneSubmitter() {
        final var network = adapter("besu1");

        // Same account, key spelled differently in config. Both derive the same address, so both
        // draw from the same on-chain nonce sequence and must not get separate submitters.
        final var unprefixed = network.accountSubmitterFor(new EthSigner(KEY_A));
        final var prefixed = network.accountSubmitterFor(new EthSigner("0x" + KEY_A));

        assertThat(prefixed).isSameAs(unprefixed);
    }

    @Test
    void differentKeys_getDistinctSubmitters() {
        final var network = adapter("besu1");

        // Independent accounts have independent nonce sequences, so they must NOT be serialised
        // behind one worker — that would halve throughput for no correctness benefit.
        final var forA = network.accountSubmitterFor(new EthSigner(KEY_A));
        final var forB = network.accountSubmitterFor(new EthSigner(KEY_B));

        assertThat(forB).isNotSameAs(forA);
    }

    @Test
    void differentNetworks_getDistinctSubmitters() {
        // Separate adapters model separate chains (RelayInstance builds one per localNetworks entry).
        // The same key on two chains has two independent nonce sequences.
        final var besu1 = LocalNetworkAdapter.create(net("besu1"), new SimpleMetrics());
        final var besu2 = LocalNetworkAdapter.create(net("besu2"), new SimpleMetrics());
        try {
            assertThat(besu2.accountSubmitterFor(new EthSigner(KEY_A)))
                    .isNotSameAs(besu1.accountSubmitterFor(new EthSigner(KEY_A)));
        } finally {
            besu1.close();
            besu2.close();
        }
    }
}
