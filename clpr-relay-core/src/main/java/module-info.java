// SPDX-License-Identifier: Apache-2.0
module org.hiero.clpr.relay.core {
    requires transitive com.hedera.pbj.runtime;
    requires transitive com.swirlds.config.api;
    requires transitive com.swirlds.logging;
    requires transitive com.swirlds.metrics.api;
    requires transitive org.hiero.clpr.relay.proto;
    requires com.swirlds.metrics.impl;
    requires static org.jspecify;

    exports org.hiero.clpr.relay.core;
    exports org.hiero.clpr.relay.core.metrics;

    provides com.swirlds.logging.api.extensions.handler.LogHandlerFactory with
            org.hiero.clpr.relay.core.ImmediateConsoleHandlerFactory;
}
