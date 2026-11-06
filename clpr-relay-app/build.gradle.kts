// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.application") }

description = "CLPR EVM Relay — Application entry point"

application { mainClass = "org.hiero.clpr.relay.app.ClprRelayMain" }

dependencies {
    implementation(project(":clpr-relay-core"))
    implementation(project(":clpr-relay-grpc-server"))
    implementation(project(":clpr-relay-grpc-client"))
    implementation(project(":clpr-relay-evm"))
    implementation(project(":clpr-relay-sync"))
    implementation(project(":clpr-relay-proto"))
    implementation("com.hedera.pbj:pbj-runtime")
    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml")
    implementation("com.swirlds:swirlds-config-api")
    implementation("com.swirlds:swirlds-config-extensions")
    implementation("com.swirlds:swirlds-logging")
    implementation("com.swirlds:swirlds-metrics-api")
    implementation("io.helidon.webserver:helidon-webserver")
}

mainModuleInfo {
    runtimeOnly("com.swirlds.config.impl")
    runtimeOnly("org.apache.logging.log4j.core")
    runtimeOnly("org.apache.logging.log4j.slf4j2.impl")
}

testModuleInfo {
    requires("org.hiero.clpr.relay.core.test.fixtures")
    // StubLoopbackTest calls getLedgerConfiguration over the loopback server.
    requires("org.hiero.clpr.relay.grpc.client.test.fixtures")
    // java.net.http: MetricsHttpServerTest scrapes the /metrics + /healthz endpoints it serves.
    requires("java.net.http")
}

// Run from the repo root so the relay picks up log.properties from the
// conventional working directory. Gradle would otherwise default
// workingDir to this subproject directory.
tasks.named<JavaExec>("run") { workingDir = rootProject.projectDir }

// Make installDist repeatable.
//
// The hiero convention skips `startScripts`, so installDist stages `lib/` and no `bin/`. The
// application plugin guards the task with a "don't overwrite existing directories" check that only
// recognises a directory as one of its own installations when `bin/` is present — so the first run
// succeeds and every run after it fails with "is neither empty nor does it contain an
// installation".
// Clearing the destination first sidesteps that. `doFirst` prepends, so this runs before the
// plugin's own check.
//
// Docker builds never noticed because each one starts from an empty workspace; local runs and the
// E2E suite (which stages this output into a container image every time) do.
// The directory is captured as a Provider rather than reached through the script, so the action
// stays serializable for the configuration cache.
tasks.named<Sync>("installDist") {
    val installDir = layout.buildDirectory.dir("install/${project.name}")
    doFirst { installDir.get().asFile.deleteRecursively() }
}
