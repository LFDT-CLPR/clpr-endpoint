// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.module.library")
    id("org.hiero.gradle.feature.test-fixtures")
}

description = "CLPR EVM Relay — Black-box E2E test framework and suite"

// This module has no `src/main`: the framework lives in `src/testFixtures` and the suites in
// `src/test`. Without this, the test compilation has no module to patch and fails with
// "module not found: null". Declaring testFixtures as the sources under test makes `src/test`
// a whitebox extension of the framework module — the same arrangement consensus-otter-tests uses.
@Suppress("UnstableApiUsage")
testing {
    suites.named<JvmTestSuite>("test") {
        javaModuleTesting.whitebox(this) { sourcesUnderTest = sourceSets.testFixtures }
    }
}

dependencies {
    // Only what the module-info files cannot express. Every `requires` in
    // src/testFixtures/java/module-info.java is resolved to a dependency by the
    // javaModuleDependencies
    // convention, so repeating them here is redundant — and the module-directive scope check
    // reports
    // the duplication against the whitebox test module, where it reads as a bogus junit complaint.
    testFixturesCompileOnly("org.jspecify:jspecify")
}

testModuleInfo {
    // org.junit.jupiter.api is intentionally absent: the fixtures module exports it transitively
    // (@E2ETest is built on @TestTemplate), so declaring it again here is flagged as redundant.
    requires("org.assertj.core")
    requires("org.hiero.clpr.relay.e2e.test.fixtures")
    runtimeOnly("com.swirlds.config.impl")
}

// The module-directive scope check cannot model the whitebox arrangement above. `src/test` has no
// module of its own — it patches the fixtures module — so the JUnit API the fixtures already export
// transitively (@E2ETest is an @TestTemplate) looks to the check like a redundant requires on the
// test
// module, and it demands the removal of a `testModuleInfo { requires("org.junit.jupiter.api") }`
// entry
// that is not declared anywhere. Drop only the `test` source set from the check; `testFixtures` —
// the
// framework itself, and the part worth checking — stays covered.
tasks.named<org.gradlex.javamodule.dependencies.tasks.ModuleDirectivesScopeCheck>(
    "checkModuleDirectivesScope"
) {
    // The map is sourceSetName -> the file its directives come from. Dropping `test` leaves `main`
    // and `testFixtures` checked.
    sourceSets.set(sourceSets.get().filterKeys { it != "test" })
}

// Suppress the spurious classfile warning the annotation-only transitives trigger, matching
// clpr-relay-integration-tests.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:-classfile")
    doFirst { options.compilerArgs.remove("-Werror") }
}

// ── Endpoint container image ──────────────────────────────────────────────────
// A THIN image: stage the relay's runtime jars plus the production entrypoint into a directory and
// let Testcontainers build `FROM eclipse-temurin:25-jre` + COPY from it. The production Dockerfile
// runs the whole Gradle build inside Docker (minutes, cold cache); this builds in seconds from the
// host's warm cache while shipping the SAME jars.
//
// The jars come from `:clpr-relay-app:installDist` rather than from a configuration resolved here:
// the JPMS module-info patches applied by the extra-java-module-info transform (12 jars, including
// the whole gRPC stack) are only present in the install output. A plain project dependency yields
// the
// unpatched originals, and the relay then dies at startup with NoClassDefFoundError on a class that
// the patched module-info would have made readable. installDist is made repeatable in that module.
val stageEndpointImage =
    tasks.register<Sync>("stageEndpointImage") {
        description =
            "Stage the E2E endpoint Docker build context (relay runtime jars + entrypoint)"
        dependsOn(":clpr-relay-app:installDist")
        from(project(":clpr-relay-app").layout.buildDirectory.dir("install/clpr-relay-app/lib")) {
            into("lib")
        }
        from(rootProject.file("docker-entrypoint.sh"))
        from(layout.projectDirectory.file("src/testFixtures/resources/log.e2e.properties")) {
            rename { "log.properties" }
        }
        from(layout.projectDirectory.file("src/testFixtures/docker/Dockerfile.e2e")) {
            rename { "Dockerfile" }
        }
        into(layout.buildDirectory.dir("e2e-image"))
    }

// ── RPC filter proxy jar ──────────────────────────────────────────────────────
// A slim runnable jar containing exactly EvmRpcFilterProxyApp (plus any inner classes) and a
// Main-Class manifest. Copied into the eclipse-temurin:25-jre proxy container by the
// EvmRpcFilterProxy fixture. Deliberately not the whole testFixtures jar: a single-class jar drops
// into any JDK image without dragging in Jackson, testcontainers, or the rest of the framework.
val rpcFilterProxyJar =
    tasks.register<Jar>("rpcFilterProxyJar") {
        description =
            "Slim runnable jar containing EvmRpcFilterProxyApp for the E2E RPC filter fixture"
        dependsOn("compileTestFixturesJava")
        from(sourceSets.testFixtures.get().output) {
            include("org/hiero/clpr/relay/e2e/fault/EvmRpcFilterProxyApp*.class")
        }
        archiveFileName.set("proxy.jar")
        destinationDirectory.set(layout.buildDirectory.dir("rpc-filter"))
        manifest {
            attributes("Main-Class" to "org.hiero.clpr.relay.e2e.fault.EvmRpcFilterProxyApp")
        }
    }

tasks.withType<Test>().configureEach {
    dependsOn(stageEndpointImage, rpcFilterProxyJar)

    // Forward the Docker plumbing the same way clpr-relay-integration-tests does: Testcontainers
    // needs DOCKER_HOST on macOS, and DOCKER_API_VERSION clamps the docker-java/daemon mismatch.
    // CLPR_SMART_CONTRACTS_DIR lets a non-sibling contracts checkout be pointed at explicitly.
    listOf(
            "DOCKER_HOST",
            "DOCKER_API_VERSION",
            "RUN_E2E_TESTS",
            "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE",
            "CLPR_SMART_CONTRACTS_DIR",
        )
        .forEach { name -> System.getenv(name)?.let { environment(name, it) } }

    // Ryuk's reverse handshake is unreliable on macOS + Docker Desktop and hangs container
    // startup. The environment's AutoCloseable teardown covers the normal path.
    environment("TESTCONTAINERS_RYUK_DISABLED", "true")

    systemProperty("clpr.e2e.imageDir", layout.buildDirectory.dir("e2e-image").get().asFile.path)
    systemProperty("clpr.e2e.outputDir", layout.buildDirectory.dir("e2e-output").get().asFile.path)
    systemProperty(
        "clpr.e2e.rpcFilterJar",
        layout.buildDirectory.file("rpc-filter/proxy.jar").get().asFile.path,
    )
    systemProperty(
        "clpr.e2e.profile",
        providers.systemProperty("clpr.e2e.profile").getOrElse("local"),
    )
    // Second opt-in switch alongside the RUN_E2E_TESTS env var. IDEs do not pass the shell
    // environment to their Gradle runs, so without this the suite is silently disabled there and a
    // --tests filter then fails the build with an unhelpful "no tests found". Settable once via
    // `clpr.e2e.enabled=true` in gradle.properties.
    //
    // Resolved to a String here: `systemProperty` takes an Object and stringifies it, so handing it
    // a
    // Provider forwards the provider's toString() ("or(provider(?), ...)") rather than its value.
    systemProperty(
        "clpr.e2e.enabled",
        providers
            .gradleProperty("clpr.e2e.enabled")
            .orElse(providers.systemProperty("clpr.e2e.enabled"))
            .getOrElse("false"),
    )

    // Artifact-retention policy: `failure` (default), `always`, or `never`. Forwarded so a run can
    // be triggered with `-Pclpr.e2e.keepArtifacts=always` from the command line, or preset in
    // gradle.properties. Set from an IDE via -Dclpr.e2e.keepArtifacts=always.
    systemProperty(
        "clpr.e2e.keepArtifacts",
        providers
            .gradleProperty("clpr.e2e.keepArtifacts")
            .orElse(providers.systemProperty("clpr.e2e.keepArtifacts"))
            .getOrElse("failure"),
    )

    testLogging {
        showStandardStreams = true
        events("passed", "skipped", "failed")
    }
    systemProperty(
        "java.util.logging.SimpleFormatter.format",
        "%1\$tF %1\$tT [%4\$s] %3\$s: %5\$s%6\$s%n",
    )

    // E2E tests bring up ~6 containers each; running them in parallel would collide on the
    // fixed docker subnets (see E2ESubnets). Keep it strictly sequential for now.
    maxParallelForks = 1
}
