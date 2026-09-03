// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Stream;
import org.hiero.clpr.relay.e2e.chain.BesuNetwork;
import org.hiero.clpr.relay.e2e.chain.BesuNode;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpoint;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.testcontainers.DockerClientFactory;

/**
 * JUnit 5 extension backing {@link E2ETest}.
 *
 * <p>Four responsibilities, each for a concrete reason:
 *
 * <ul>
 *   <li>{@link ExecutionCondition} — skip rather than fail when Docker, the contract artifacts, or
 *       the profile's capabilities are missing. An E2E suite that hard-fails on a laptop without
 *       Docker is a suite nobody runs.
 *   <li>{@link TestTemplateInvocationContextProvider} — make a bare {@code @E2ETest} behave like
 *       {@code @Test}, so no method needs both annotations.
 *   <li>{@link ParameterResolver} — build the {@link E2EEnvironment} and register it in the
 *       <em>method</em>-scoped store, so JUnit closes it after every test method. Per-method
 *       isolation is mandatory here: a channel whose backlog exceeded the relay's bundle batch is
 *       wedged for good, so a shared environment would let one test poison the rest.
 *   <li>{@code TestWatcher} — dump container logs and chain state on failure. Without that a red
 *       E2E run is close to undiagnosable after the containers are gone.
 * </ul>
 */
public final class E2ETestExtension
        implements ExecutionCondition,
                ParameterResolver,
                TestTemplateInvocationContextProvider,
                TestExecutionExceptionHandler {

    /** Environment variable gating the whole suite. */
    public static final String ENABLE_ENV_VAR = "RUN_E2E_TESTS";

    /**
     * System property gating the whole suite, as an alternative to {@link #ENABLE_ENV_VAR}.
     *
     * <p>Exists because IDEs do not pass the shell's environment to their Gradle runs, and a disabled
     * suite combined with a {@code --tests} filter fails the build with "no tests found" — which says
     * nothing about the missing switch. A property can be set once in {@code gradle.properties}
     * ({@code clpr.e2e.enabled=true}) and then works from the IDE and the command line alike.
     */
    public static final String ENABLE_PROPERTY = "clpr.e2e.enabled";

    /** System property holding the staged endpoint image build context. */
    public static final String IMAGE_DIR_PROPERTY = "clpr.e2e.imageDir";

    /** System property holding the root output directory. */
    public static final String OUTPUT_DIR_PROPERTY = "clpr.e2e.outputDir";

    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(E2ETestExtension.class);

    private static final String ENVIRONMENT_KEY = "environment";

    // ── ExecutionCondition ────────────────────────────────────────────────────

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(final ExtensionContext context) {
        final Optional<Method> method = context.getTestMethod();
        if (method.isEmpty()) {
            return ConditionEvaluationResult.enabled("not a test method");
        }
        final E2ETest annotation = method.get().getAnnotation(E2ETest.class);
        if (annotation == null) {
            return ConditionEvaluationResult.enabled("not an @E2ETest method");
        }

        if (!isEnabled()) {
            return ConditionEvaluationResult.disabled("E2E tests are opt-in (they need Docker and take minutes)."
                    + " Enable them with the " + ENABLE_ENV_VAR + "=1 environment variable, or with -D"
                    + ENABLE_PROPERTY + "=true / " + ENABLE_PROPERTY + "=true in gradle.properties when"
                    + " running from an IDE.");
        }
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            return ConditionEvaluationResult.disabled("Docker is not available");
        }

        final E2EProfile profile = E2EProfile.active();
        final Capability[] required = annotation.requires();
        if (!profile.supports(required)) {
            return ConditionEvaluationResult.disabled("profile " + profile + " does not offer " + List.of(required)
                    + " (offers " + profile.capabilities() + ")");
        }
        return ConditionEvaluationResult.enabled("profile " + profile + " satisfies " + List.of(required));
    }

    /** @return {@code true} if either opt-in switch is set. */
    private static boolean isEnabled() {
        return System.getenv(ENABLE_ENV_VAR) != null || Boolean.parseBoolean(System.getProperty(ENABLE_PROPERTY));
    }

    // ── TestTemplateInvocationContextProvider ─────────────────────────────────

    @Override
    public boolean supportsTestTemplate(final ExtensionContext context) {
        // Only synthesize an invocation when no other JUnit test annotation already drives the
        // method; otherwise @RepeatedTest / @ParameterizedTest would run twice.
        final Method method = context.getRequiredTestMethod();
        return !isDrivenByAnotherAnnotation(method);
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(final ExtensionContext context) {
        return Stream.of(new TestTemplateInvocationContext() {
            @Override
            public String getDisplayName(final int invocationIndex) {
                return context.getRequiredTestMethod().getName();
            }

            @Override
            public List<Extension> getAdditionalExtensions() {
                return List.of();
            }
        });
    }

    // ── ParameterResolver ─────────────────────────────────────────────────────

    @Override
    public boolean supportsParameter(final ParameterContext parameterContext, final ExtensionContext context)
            throws ParameterResolutionException {
        return E2EEnvironment.class.equals(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(final ParameterContext parameterContext, final ExtensionContext context)
            throws ParameterResolutionException {
        return environmentFor(context);
    }

    // ── failure diagnostics ───────────────────────────────────────────────────

    /**
     * Capture diagnostics while the containers are still alive, then rethrow.
     *
     * <p>{@code TestWatcher.testFailed} would be too late: JUnit closes the method-scoped store —
     * which stops every container — before calling it, so a dump taken there finds nothing running and
     * reports "endpoint is not started" instead of the log and scrape that explain the failure. This
     * hook runs inside the test invocation, before any teardown.
     */
    @Override
    public void handleTestExecutionException(final ExtensionContext context, final Throwable throwable)
            throws Throwable {
        final E2EEnvironment environment = existingEnvironment(context);
        if (environment != null) {
            try {
                dumpDiagnostics(environment, context, throwable);
            } catch (final RuntimeException e) {
                // Diagnostics are an aid; failing to write them must not replace the real cause.
                System.err.println("[e2e] failed to write diagnostics: " + e);
            }
        }
        throw throwable;
    }

    // ── internals ─────────────────────────────────────────────────────────────

    private E2EEnvironment environmentFor(final ExtensionContext context) {
        // CloseableResource in the METHOD-scoped store: JUnit closes it when the method's context is
        // torn down, which is what gives every test a fresh set of containers.
        return context.getStore(NAMESPACE)
                .getOrComputeIfAbsent(
                        ENVIRONMENT_KEY,
                        key -> new CloseableEnvironment(newEnvironment(context)),
                        CloseableEnvironment.class)
                .environment;
    }

    @Nullable
    private E2EEnvironment existingEnvironment(final ExtensionContext context) {
        final CloseableEnvironment holder =
                context.getStore(NAMESPACE).get(ENVIRONMENT_KEY, CloseableEnvironment.class);
        return holder == null ? null : holder.environment;
    }

    private static E2EEnvironment newEnvironment(final ExtensionContext context) {
        final E2EProfile profile = E2EProfile.active();
        return new E2EEnvironmentImpl(profile, outputDirectoryFor(context));
    }

    private static Path outputDirectoryFor(final ExtensionContext context) {
        final String root = System.getProperty(OUTPUT_DIR_PROPERTY, "build/e2e-output");
        final String className =
                context.getTestClass().map(Class::getSimpleName).orElse("unknown");
        final String methodName = context.getTestMethod().map(Method::getName).orElse("unknown");
        return Path.of(root, className, methodName);
    }

    /**
     * Wraps the environment so the JUnit store closes it. The store closes {@link AutoCloseable}
     * values it owns, which is what ties the environment's lifetime to the test method.
     */
    private static final class CloseableEnvironment implements AutoCloseable {
        private final E2EEnvironment environment;

        private CloseableEnvironment(final E2EEnvironment environment) {
            this.environment = environment;
        }

        @Override
        public void close() {
            environment.close();
        }
    }

    private static boolean isDrivenByAnotherAnnotation(final Method method) {
        return method.isAnnotationPresent(org.junit.jupiter.api.Test.class)
                || method.isAnnotationPresent(org.junit.jupiter.api.RepeatedTest.class)
                || method.isAnnotationPresent(org.junit.jupiter.api.TestFactory.class)
                || method.isAnnotationPresent(org.junit.jupiter.params.ParameterizedTest.class);
    }

    private static void dumpDiagnostics(
            final E2EEnvironment environment, final ExtensionContext context, final Throwable cause) {
        final Path dir = environment.outputDirectory();
        write(dir.resolve("failure.txt"), renderFailure(context, cause));

        // Endpoint logs and the final scrape first: for a black-box SUT these are the primary
        // evidence, and a failing wait for a metric is unreadable without them.
        for (final EvmEndpoint endpoint : environment.endpoints().all()) {
            write(
                    dir.resolve("endpoint-" + endpoint.id() + ".log"),
                    endpoint.logs().text());
            try {
                write(dir.resolve("endpoint-" + endpoint.id() + "-config.yaml"), endpoint.renderConfig());
            } catch (final RuntimeException e) {
                writeUnavailable(dir.resolve("endpoint-" + endpoint.id() + "-config.yaml"), e);
            }
            try {
                write(
                        dir.resolve("endpoint-" + endpoint.id() + "-metrics.txt"),
                        endpoint.metrics().raw());
            } catch (final RuntimeException e) {
                writeUnavailable(dir.resolve("endpoint-" + endpoint.id() + "-metrics.txt"), e);
            }
        }

        for (final BesuNetwork network : environment.besuNetworks().all()) {
            for (final BesuNode node : network.nodes()) {
                write(dir.resolve("besu-" + node.id() + ".log"), node.logs().text());
            }
            try {
                final long head = network.inspect().headBlockNumber();
                final StringBuilder chain = new StringBuilder();
                chain.append("network=").append(network.id()).append('\n');
                chain.append("chainId=").append(network.chainId()).append('\n');
                chain.append("head=").append(head).append('\n');
                chain.append("validator=")
                        .append(network.validatorAddress())
                        .append('\n')
                        .append('\n');
                network.inspect().transactions(0, head).forEach(t -> chain.append(t)
                        .append('\n'));
                write(dir.resolve("chain-" + network.id() + ".txt"), chain.toString());
            } catch (final RuntimeException e) {
                writeUnavailable(dir.resolve("chain-" + network.id() + ".txt"), e);
            }
            // Mined-block history says nothing about a transaction the pool is holding but never
            // included, which is exactly the state a wedged wiring transaction leaves behind.
            try {
                write(
                        dir.resolve("txpool-" + network.id() + ".json"),
                        network.rawRpc().call("txpool_content").toPrettyString());
            } catch (final RuntimeException e) {
                writeUnavailable(dir.resolve("txpool-" + network.id() + ".json"), e);
            }
        }
        System.err.println("[e2e] diagnostics written to " + dir.toAbsolutePath());
    }

    private static String renderFailure(final ExtensionContext context, final Throwable cause) {
        final StringBuilder out = new StringBuilder();
        out.append("test=")
                .append(context.getTestClass().map(Class::getName).orElse("?"))
                .append('#')
                .append(context.getTestMethod().map(Method::getName).orElse("?"))
                .append('\n');
        out.append("profile=")
                .append(E2EProfile.active().name().toLowerCase(Locale.ROOT))
                .append('\n');
        out.append("cause=").append(cause).append("\n\n");
        for (final StackTraceElement frame : cause.getStackTrace()) {
            out.append("\tat ").append(frame).append('\n');
        }
        return out.toString();
    }

    private static void write(final Path target, final String content) {
        try {
            Files.createDirectories(target.getParent());
            Files.writeString(target, content, StandardCharsets.UTF_8);
        } catch (final IOException e) {
            throw new UncheckedIOException("failed to write " + target, e);
        }
    }

    // Writes the "unavailable: <exception>" placeholder to the artifact AND surfaces the same
    // failure on stderr with a stack trace, so a reviewer inspecting a failed run sees the
    // diagnostic-collection bug (not just its silent placeholder in the artifact bundle).
    private static void writeUnavailable(final Path target, final Throwable cause) {
        final StringBuilder body =
                new StringBuilder("unavailable: ").append(cause).append('\n');
        for (final StackTraceElement frame : cause.getStackTrace()) {
            body.append("\tat ").append(frame).append('\n');
        }
        write(target, body.toString());
        System.err.println("[e2e] diagnostic collection failed for " + target.getFileName() + ": " + cause);
        cause.printStackTrace(System.err);
    }
}
