// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description = "CLPR EVM Relay — Sync orchestration"

dependencies {
    api(project(":clpr-relay-proto"))
    api(project(":clpr-relay-core"))
    api(project(":clpr-relay-grpc-client"))
    api("com.hedera.pbj:pbj-runtime")
    implementation("com.swirlds:swirlds-logging")
}

testModuleInfo {
    requires("org.junit.jupiter.params")
    requires("org.hiero.clpr.relay.core.test.fixtures")
    runtimeOnly("com.swirlds.config.impl")
}
