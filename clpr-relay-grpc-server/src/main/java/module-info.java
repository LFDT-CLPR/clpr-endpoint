// SPDX-License-Identifier: Apache-2.0
module org.hiero.clpr.relay.grpc.server {
    requires transitive com.hedera.pbj.runtime;
    requires transitive com.swirlds.metrics.api;
    requires transitive org.hiero.clpr.relay.core;
    requires transitive org.hiero.clpr.relay.proto;
    requires com.hedera.pbj.grpc.helidon;
    requires com.swirlds.base;
    requires com.swirlds.logging;
    requires io.helidon.common;
    requires io.helidon.webserver;
    requires static org.jspecify;

    exports org.hiero.clpr.relay.grpc.server;
}
