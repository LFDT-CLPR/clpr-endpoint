// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.module.library")
    id("org.hiero.gradle.feature.test-fixtures")
}

description = "CLPR EVM Relay — Core interfaces and types"

dependencies {
    api("com.swirlds:swirlds-metrics-api")
    implementation("com.swirlds:swirlds-metrics-impl")
    compileOnly("org.jspecify:jspecify")
}

testModuleInfo {
    requires("org.junit.jupiter.api")
    requires("org.assertj.core")
    requires("org.hiero.clpr.relay.core.test.fixtures")
    runtimeOnly("com.swirlds.config.impl")
}

// Suppress spurious classfile warning from spotbugs-annotations referencing jsr305 internals.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:-classfile")
    doFirst { options.compilerArgs.remove("-Werror") }
}

// The javadoc convention adds '-Xwerror'; override to disable it so that the
// spurious classfile warning from spotbugs-annotations does not fail the build.
tasks.javadoc {
    (options as StandardJavadocDocletOptions).apply {
        addStringOption("Xdoclint:all,-missing", "-quiet")
    }
}
