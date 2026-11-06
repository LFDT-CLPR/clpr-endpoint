// SPDX-License-Identifier: Apache-2.0
/**
 * Single-side stub-peer test harness for CLPR endpoint integration tests: a controllable gRPC
 * {@code StubPeer}, an on-chain {@code ChainActor}, crafted-bundle builders, and assertion helpers.
 * Consumed by other modules' test source sets (notably {@code clpr-relay-integration-tests}).
 */
module org.hiero.clpr.relay.test.harness {
    // Exposed in public API signatures → transitive.
    // Used internally only.
    requires transitive com.hedera.pbj.runtime;
    requires transitive com.swirlds.metrics.api;
    requires transitive org.hiero.clpr.relay.core;
    requires transitive org.hiero.clpr.relay.evm;
    requires transitive org.hiero.clpr.relay.proto;
    requires com.hedera.pbj.grpc.helidon;
    requires org.hiero.clpr.relay.grpc.client;
    requires io.helidon.common;
    requires io.helidon.webserver;
    requires static org.jspecify;

    exports org.hiero.clpr.relay.test.harness;
}
