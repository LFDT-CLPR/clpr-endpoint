// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description =
    "CLPR EVM Relay — single-side stub-peer test harness (StubPeer, ChainTxSubmitter, assertions)"

dependencies {
    // Exposed in the harness's public API (assertions, signers, bundle types, ChainTxSubmitter) →
    // api.
    api(project(":clpr-relay-core"))
    api(project(":clpr-relay-proto"))
    api(project(":clpr-relay-evm"))
    api("com.hedera.pbj:pbj-runtime")
    // Used internally only → implementation.
    implementation(project(":clpr-relay-grpc-client"))
    implementation("com.hedera.pbj:pbj-grpc-helidon")
    implementation("io.helidon.webserver:helidon-webserver")
    runtimeOnly("com.hedera.pbj:pbj-grpc-helidon-config")
}

testModuleInfo {
    requires("org.junit.jupiter.api")
    requires("org.assertj.core")
    runtimeOnly("com.swirlds.config.impl")
}
