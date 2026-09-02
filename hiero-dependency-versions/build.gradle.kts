// SPDX-License-Identifier: Apache-2.0
plugins {
    id("org.hiero.gradle.base.lifecycle")
    id("org.hiero.gradle.base.jpms-modules")
    id("org.hiero.gradle.check.spotless")
    id("org.hiero.gradle.check.spotless-kotlin")
}

dependencies.constraints {
    val pbjVersion = pluginVersions.version("com.hedera.pbj.pbj-compiler")
    val helidonVersion = "4.4.1"
    val hieroCryptoVersion = "3.14.0"
    val jacksonVersion = "2.18.3"
    val swirldsVersion = "0.56.5"
    val log4jVersion = "2.24.3"
    val mockitoVersion = "5.15.2"
    val grpcIoVersion = "1.73.0"
    val protobufVersion = "4.31.1"
    val bouncyCastleVersion = "1.78.1"

    api("com.hedera.pbj:pbj-runtime:${pbjVersion}") { because("com.hedera.pbj.runtime") }
    api("com.hedera.pbj:pbj-grpc-helidon:${pbjVersion}") { because("com.hedera.pbj.grpc.helidon") }
    api("com.hedera.pbj:pbj-grpc-helidon-config:${pbjVersion}") {
        because("com.hedera.pbj.grpc.helidon.config")
    }
    api("com.hedera.pbj:pbj-grpc-client-helidon:${pbjVersion}") {
        because("com.hedera.pbj.grpc.client.helidon")
    }

    api("io.helidon.webclient:helidon-webclient:$helidonVersion") {
        because("io.helidon.webclient")
    }
    api("io.helidon.webclient:helidon-webclient-grpc:$helidonVersion") {
        because("io.helidon.webclient.grpc")
    }
    api("io.helidon.webserver:helidon-webserver:$helidonVersion") {
        because("io.helidon.webserver")
    }
    api("io.helidon.logging:helidon-logging-jul:$helidonVersion") {
        because("io.helidon.logging.jul")
    }

    api("com.fasterxml.jackson.core:jackson-core:$jacksonVersion") {
        because("com.fasterxml.jackson.core")
    }
    api("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion") {
        because("com.fasterxml.jackson.databind")
    }
    api("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jacksonVersion") {
        because("com.fasterxml.jackson.dataformat.yaml")
    }

    api("com.swirlds:swirlds-base:$swirldsVersion") { because("com.swirlds.base") }
    api("com.swirlds:swirlds-config-api:$swirldsVersion") { because("com.swirlds.config.api") }
    api("com.swirlds:swirlds-config-extensions:$swirldsVersion") {
        because("com.swirlds.config.extensions")
    }
    api("com.swirlds:swirlds-config-impl:$swirldsVersion") { because("com.swirlds.config.impl") }
    api("com.swirlds:swirlds-logging:$swirldsVersion") { because("com.swirlds.logging") }
    api("com.swirlds:swirlds-metrics-api:$swirldsVersion") { because("com.swirlds.metrics.api") }
    api("com.swirlds:swirlds-metrics-impl:$swirldsVersion") { because("com.swirlds.metrics.impl") }
    api("com.google.auto.service:auto-service-annotations:1.1.1") {
        because("com.google.auto.service")
    }
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion") {
        because("com.fasterxml.jackson.datatype.jsr310")
    }
    api("org.apache.logging.log4j:log4j-api:$log4jVersion") { because("org.apache.logging.log4j") }

    api("org.apache.logging.log4j:log4j-core:$log4jVersion") {
        because("org.apache.logging.log4j.core")
    }
    api("org.apache.logging.log4j:log4j-slf4j2-impl:$log4jVersion") {
        because("org.apache.logging.log4j.slf4j2.impl")
    }

    // gRPC dependencies (transitive from pbj-grpc-helidon and pbj-grpc-client-helidon)
    api("io.grpc:grpc-api:$grpcIoVersion") { because("io.grpc") }
    api("io.grpc:grpc-core:$grpcIoVersion") { because("io.grpc.internal") }
    api("io.grpc:grpc-stub:$grpcIoVersion") { because("io.grpc.stub") }
    api("io.grpc:grpc-protobuf:$grpcIoVersion") { because("io.grpc.protobuf") }
    api("io.grpc:grpc-netty:$grpcIoVersion") { because("io.grpc.netty") }
    api("io.grpc:grpc-netty-shaded:$grpcIoVersion") { because("io.grpc.netty.shaded") }
    api("io.grpc:grpc-inprocess:$grpcIoVersion") { because("io.grpc.inprocess") }
    api("io.grpc:grpc-util:$grpcIoVersion") { because("io.grpc.util") }

    // Protobuf and related (transitive from grpc)
    api("com.google.protobuf:protobuf-java:$protobufVersion") { because("com.google.protobuf") }
    api("com.google.protobuf:protobuf-java-util:$protobufVersion") {
        because("com.google.protobuf.util")
    }
    api("com.google.api.grpc:proto-google-common-protos:2.51.0") {
        because("com.google.api.grpc.common")
    }
    api("com.google.guava:guava:33.4.8-jre") { because("com.google.common") }
    api("com.google.guava:failureaccess:1.0.3") {
        because("com.google.common.util.concurrent.internal")
    }
    api("org.jspecify:jspecify:1.0.0") { because("org.jspecify") }

    // hiero-cryptography — native BLS12-381 G1/G2 decompression
    api("com.hedera.cryptography:hedera-cryptography-hints:$hieroCryptoVersion") {
        because("com.hedera.cryptography.hints")
    }
    api("com.hedera.common:hedera-common-nativesupport:$hieroCryptoVersion") {
        because("com.hedera.common.nativesupport")
    }

    // BouncyCastle — X.509 leaf-certificate construction for the two-tier mTLS trust model.
    api("org.bouncycastle:bcprov-jdk18on:$bouncyCastleVersion") {
        because("org.bouncycastle.provider")
    }
    api("org.bouncycastle:bcutil-jdk18on:$bouncyCastleVersion") { because("org.bouncycastle.util") }
    api("org.bouncycastle:bcpkix-jdk18on:$bouncyCastleVersion") { because("org.bouncycastle.pkix") }

    // Testing
    api("org.junit.jupiter:junit-jupiter-api:5.14.0") { because("org.junit.jupiter.api") }
    api("org.junit.jupiter:junit-jupiter-engine:5.14.0") { because("org.junit.jupiter.engine") }
    api("org.junit.jupiter:junit-jupiter-params:5.14.0") { because("org.junit.jupiter.params") }
    api("org.assertj:assertj-core:3.27.3") { because("org.assertj.core") }
    api("org.mockito:mockito-core:${mockitoVersion}") { because("org.mockito") }
    api("org.mockito:mockito-junit-jupiter:${mockitoVersion}") {
        because("org.mockito.junit.jupiter")
    }

    val testcontainersVersion = "1.21.4"
    api("org.testcontainers:testcontainers:$testcontainersVersion") {
        because("org.testcontainers")
    }
    api("org.testcontainers:junit-jupiter:$testcontainersVersion") {
        because("org.testcontainers.junit.jupiter")
    }
}

// Exclude version constraints for future modules not yet used in the skeleton build.
// Remove these exclusions when the corresponding modules are implemented.
tasks.checkVersionConsistency {
    excludes.add("com.hedera.pbj:pbj-runtime")
    excludes.add("com.hedera.pbj:pbj-grpc-helidon")
    excludes.add("com.hedera.pbj:pbj-grpc-helidon-config")
    excludes.add("com.hedera.pbj:pbj-grpc-client-helidon")
    excludes.add("io.helidon.webclient:helidon-webclient")
    excludes.add("io.helidon.webclient:helidon-webclient-grpc")
    excludes.add("io.helidon.webserver:helidon-webserver")
    excludes.add("io.helidon.logging:helidon-logging-jul")
    excludes.add("com.fasterxml.jackson.core:jackson-core")
    excludes.add("com.fasterxml.jackson.core:jackson-databind")
    excludes.add("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml")
    excludes.add("com.swirlds:swirlds-base")
    excludes.add("com.swirlds:swirlds-config-api")
    excludes.add("com.swirlds:swirlds-config-extensions")
    excludes.add("com.swirlds:swirlds-config-impl")
    excludes.add("com.swirlds:swirlds-logging")
    excludes.add("com.swirlds:swirlds-metrics-api")
    excludes.add("com.swirlds:swirlds-metrics-impl")
    excludes.add("com.google.auto.service:auto-service-annotations")
    excludes.add("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    excludes.add("org.apache.logging.log4j:log4j-api")
    excludes.add("io.grpc:grpc-api")
    excludes.add("io.grpc:grpc-core")
    excludes.add("io.grpc:grpc-stub")
    excludes.add("io.grpc:grpc-protobuf")
    excludes.add("io.grpc:grpc-netty")
    excludes.add("io.grpc:grpc-inprocess")
    excludes.add("io.grpc:grpc-util")
    excludes.add("com.google.protobuf:protobuf-java")
    excludes.add("com.google.protobuf:protobuf-java-util")
    excludes.add("com.google.api.grpc:proto-google-common-protos")
    excludes.add("com.google.guava:guava")
    excludes.add("com.google.guava:failureaccess")
    excludes.add("org.jspecify:jspecify")
    excludes.add("org.mockito:mockito-core")
    excludes.add("org.mockito:mockito-junit-jupiter")
    excludes.add("org.junit.jupiter:junit-jupiter-api")
    excludes.add("org.junit.jupiter:junit-jupiter-params")
    excludes.add("org.assertj:assertj-core")
    excludes.add("org.testcontainers:testcontainers")
    excludes.add("org.testcontainers:junit-jupiter")
}
