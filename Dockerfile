# SPDX-License-Identifier: Apache-2.0

# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jdk AS builder

# git is required by the hiero Spotless plugin at Gradle configuration time.
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Gradle wrapper + settings files — copied first so dependency resolution is a
# separate cached layer that survives source-only changes.
COPY gradlew gradlew.bat ./
COPY gradle/ gradle/
COPY settings.gradle.kts build.gradle.kts gradle.properties ./
COPY hiero-dependency-versions/ hiero-dependency-versions/

# Subproject build files (needed for dependency resolution).
COPY clpr-relay-proto/build.gradle.kts   clpr-relay-proto/
COPY clpr-relay-core/build.gradle.kts    clpr-relay-core/
COPY clpr-relay-evm/build.gradle.kts     clpr-relay-evm/
COPY clpr-relay-grpc-server/build.gradle.kts    clpr-relay-grpc-server/
COPY clpr-relay-grpc-client/build.gradle.kts    clpr-relay-grpc-client/
COPY clpr-relay-sync/build.gradle.kts    clpr-relay-sync/
COPY clpr-relay-app/build.gradle.kts     clpr-relay-app/

# Warm up the Gradle dependency cache without compiling source.
RUN ./gradlew :clpr-relay-app:dependencies --no-daemon -q 2>/dev/null || true

# Full source — only invalidates the build layer, not the dependency layer.
COPY clpr-relay-proto/ clpr-relay-proto/
COPY clpr-relay-core/  clpr-relay-core/
COPY clpr-relay-evm/   clpr-relay-evm/
COPY clpr-relay-grpc-server/  clpr-relay-grpc-server/
COPY clpr-relay-grpc-client/  clpr-relay-grpc-client/
COPY clpr-relay-sync/  clpr-relay-sync/
COPY clpr-relay-app/   clpr-relay-app/

# Build and assemble all JARs under build/install/clpr-relay-app/lib/.
RUN ./gradlew :clpr-relay-app:installDist --no-daemon -q

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jre AS runtime
WORKDIR /app

# Dedicated non-root user.
RUN groupadd -r clpr && useradd -r -g clpr -d /app clpr

# All JARs — module-path runtime.
COPY --from=builder /workspace/clpr-relay-app/build/install/clpr-relay-app/lib/ /app/lib/

# Swirlds logging config — must be present in the working directory so that
# FileSystemUtils.waitForPathPresence() returns immediately instead of blocking
# Copied directly from the repo root
COPY log.properties /app/log.properties

# Entrypoint script — expands RELAY_SIGNING_KEY and JAVA_OPTS at container start.
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh && chown -R clpr:clpr /app

USER clpr

# gRPC port (9545) and Prometheus metrics port (9547).
EXPOSE 9545 9547

ENTRYPOINT ["/app/docker-entrypoint.sh"]
