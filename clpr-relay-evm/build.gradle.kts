// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.module.library")
    id("org.hiero.gradle.feature.test-fixtures")
}

description = "CLPR EVM Relay — JSON-RPC client for EVM nodes"

dependencies {
    api("com.fasterxml.jackson.core:jackson-databind")
    api(project(":clpr-relay-core"))
    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("com.hedera.cryptography:hedera-cryptography-hints")
}

testModuleInfo {
    requires("org.hiero.clpr.relay.evm.test.fixtures")
    // jdk.httpserver: loopback HTTP server used by EvmJsonRpcClientTest to exercise the
    // real send()/parseResponse() retry + error-decoding state machine without mocking.
    requires("jdk.httpserver")
    requires("org.junit.jupiter.params")
    runtimeOnly("com.swirlds.config.impl")
}

tasks.test {
    jvmArgs("--enable-native-access=com.hedera.common.nativesupport,com.hedera.cryptography.hints")
}
