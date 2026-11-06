// SPDX-License-Identifier: Apache-2.0
/**
 * Black-box E2E test framework for the CLPR EVM endpoint: Besu QBFT networks and containerized
 * endpoints on Testcontainers, plus the on-chain control surface, load generation, fault injection
 * and assertions the suites are written against.
 *
 * <p>Consumed by this project's own {@code src/test} suites. Distinct from
 * {@code clpr-relay-integration-tests}, which drives in-process relays against a stub verifier;
 * here the endpoint is a container and the verifier is the real one.
 */
module org.hiero.clpr.relay.e2e.test.fixtures {
    // Exposed in the framework's public API: ChainOps hands back ContractInteractor types, specs and
    // containers surface Testcontainers types, @E2ETest builds on JUnit's @TestTemplate, and
    // RawJsonRpc / ChainInspector return Jackson JsonNode.
    // Internal only.
    // Reached through the integration/harness types the channel-state reads decode.
    // ChannelSide.status() returns ClprChannelStatus, a PBJ type from the proto module.
    // EvmRpcFilterProxyApp: single-file HTTP proxy shipped as a slim runnable jar into the RPC
    // filter container. Uses com.sun.net.httpserver.HttpServer, exported from jdk.httpserver.
    requires transitive org.hiero.clpr.relay.evm;
    requires transitive org.hiero.clpr.relay.integration.tests;
    requires transitive org.hiero.clpr.relay.proto;
    requires transitive org.hiero.clpr.relay.test.harness;
    requires transitive com.fasterxml.jackson.databind;
    requires transitive org.junit.jupiter.api;
    requires transitive org.testcontainers;
    requires com.hedera.pbj.runtime;
    requires org.hiero.clpr.relay.evm.test.fixtures;
    requires com.fasterxml.jackson.core;
    requires com.github.dockerjava.api;
    requires java.net.http;
    requires jdk.httpserver;
    requires org.junit.jupiter.params;
    requires static org.jspecify;

    exports org.hiero.clpr.relay.e2e;
    exports org.hiero.clpr.relay.e2e.chain;
    exports org.hiero.clpr.relay.e2e.channel;
    exports org.hiero.clpr.relay.e2e.endpoint;
    exports org.hiero.clpr.relay.e2e.fault;
    exports org.hiero.clpr.relay.e2e.load;
}
