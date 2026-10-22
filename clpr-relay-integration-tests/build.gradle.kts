// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.module.library") }

description = "CLPR EVM Relay — Integration tests"

dependencies {
    api(project(":clpr-relay-evm"))
    // ContractInteractor (ChainTxSubmitter) and AnvilTxSubmitter (extends Eip1559TxSubmitter)
    // expose
    // harness types in their public API → api scope.
    api(project(":clpr-relay-test-harness"))
    api("org.testcontainers:testcontainers")
    compileOnly("org.jspecify:jspecify")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation(project(":clpr-relay-app"))
    testImplementation(project(":clpr-relay-core"))
    testImplementation(testFixtures(project(":clpr-relay-evm")))
    // ClprEndpointInfoClient (getLedgerConfiguration driver); the
    // testFixtures dependency pulls the grpc-client main variant transitively.
    testImplementation(testFixtures(project(":clpr-relay-grpc-client")))
}

testModuleInfo {
    requires("org.testcontainers.junit.jupiter")
    requires("org.hiero.clpr.relay.app")
    requires("org.hiero.clpr.relay.core")
    requires("org.hiero.clpr.relay.evm.test.fixtures")
    requires("org.hiero.clpr.relay.grpc.client.test.fixtures")
    requires("com.swirlds.metrics.api")
    opensTo("org.testcontainers.junit.jupiter")
    // Note: com.github.dockerjava.api is required by the main module-info (used by SeiContainer
    // and TransientNodeOutageRecoveryTest); the test sources patch that same module, so it must
    // NOT be repeated here or checkModuleDirectivesScope flags it as a redundant directive.
}

// Suppress spurious classfile warning.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:-classfile")
    doFirst { options.compilerArgs.remove("-Werror") }
}

// Forward Docker-related environment variables and system properties from the parent
// shell to the Gradle test JVM. Testcontainers reads DOCKER_HOST to locate the Docker
// daemon socket (needed on macOS where Docker Desktop uses a non-standard path), and
// honours the DOCKER_API_VERSION env var so we can clamp to the daemon's max API
// version (Docker 29 + docker-java 3.4.1 disagree on the default). Also forward
// RUN_INTEGRATION_TESTS so @EnabledIfEnvironmentVariable picks it up inside the
// forked test VM.
tasks.withType<Test>().configureEach {
    listOf(
            "DOCKER_HOST",
            "DOCKER_API_VERSION",
            "RUN_INTEGRATION_TESTS",
            "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE",
        )
        .forEach { name -> System.getenv(name)?.let { environment(name, it) } }
    testLogging {
        showStandardStreams = true
        events("passed", "skipped", "failed")
    }
    systemProperty(
        "java.util.logging.SimpleFormatter.format",
        "%1\$tF %1\$tT [%4\$s] %3\$s: %5\$s%6\$s%n",
    )
    systemProperty(
        "java.util.logging.config.file",
        "${projectDir}/src/test/resources/logging.properties",
    )

    // Disable the Testcontainers Ryuk reaper. Ryuk's reverse-handshake to the host JVM
    // is unreliable on macOS + Docker Desktop and hangs container startup, especially
    // when tests are launched from IDEs where the host networking path differs from a
    // terminal. The @Testcontainers JUnit extension already cleans up on normal exit;
    // Ryuk is only useful for crash scenarios. Set via env var so it also applies when
    // IntelliJ runs tests outside Gradle.
    environment("TESTCONTAINERS_RYUK_DISABLED", "true")
}
