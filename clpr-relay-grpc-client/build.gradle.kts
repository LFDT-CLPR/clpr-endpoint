// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.module.library")
    id("org.hiero.gradle.feature.test-fixtures")
}

description = "gRPC client (Netty/gRPC-Java)"

dependencies {
    api(project(":clpr-relay-proto"))
    implementation("io.grpc:grpc-api")
    implementation("io.grpc:grpc-stub")
    // Outbound peer sync uses grpc-netty (NettyChannelBuilder in
    // ClprEndpointClient). Helidon's gRPC client omits TE: trailers and
    // grpc-java's Netty server enforces it — switching transports for
    // outbound only avoids the interop issue. See class javadoc on
    // ClprEndpointClient for details.
    implementation("io.grpc:grpc-netty-shaded")
    implementation("com.swirlds:swirlds-logging")

    // ClprEndpointInfoClient is a test-only outbound client for the
    // getLedgerConfiguration RPC. It has no production caller, so it lives in
    // test fixtures rather than the shipped jar. Its dependencies mirror the main source set's.
    testFixturesImplementation("io.grpc:grpc-api")
    testFixturesImplementation("io.grpc:grpc-stub")
    testFixturesImplementation("io.grpc:grpc-netty-shaded")
    testFixturesImplementation("com.swirlds:swirlds-logging")
}

testModuleInfo {
    requires("org.hiero.clpr.relay.grpc.client.test.fixtures")
    runtimeOnly("com.swirlds.config.impl")
}
