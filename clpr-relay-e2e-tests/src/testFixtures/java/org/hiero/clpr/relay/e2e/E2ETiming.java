// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e;

import java.time.Duration;

/**
 * Timing helpers used by fault-injection tests.
 *
 * <p>Almost every wait in this suite is condition-driven and goes through {@link Awaiter}. A raw
 * pause is the exception: it belongs where a test deliberately holds the system in a broken state
 * for long enough that whatever the fault triggers (backoff, gauge changes, dropped connections)
 * has time to happen, without there being a condition to poll on. The pattern was already open-coded
 * in more than one test with the same interrupt handling, so it lives here now.
 *
 * <p>Reach for {@code env.await()} first. Use {@link #pause} only when there is no observable
 * condition to key off — otherwise the test is timing-coupled to whatever knob you tuned this
 * pause to, and it will flake the first time that knob moves.
 */
public final class E2ETiming {

    private E2ETiming() {}

    /**
     * Sleep for {@code duration}, converting a thread interrupt into an {@link AssertionError} that
     * names {@code reason}. The reason string is required so a sleep in a test always documents
     * <em>why</em> it is there (the failure message on interrupt is the diagnostic a reader gets).
     *
     * @param duration how long to sleep; must be non-negative
     * @param reason short description of the outage window, e.g. {@code "outage window"}
     * @throws AssertionError if the current thread is interrupted while sleeping
     */
    public static void pause(final Duration duration, final String reason) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted during " + reason);
        }
    }
}
