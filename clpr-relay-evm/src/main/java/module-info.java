// SPDX-License-Identifier: Apache-2.0
module org.hiero.clpr.relay.evm {
    requires transitive com.hedera.pbj.runtime;
    requires transitive com.swirlds.logging;
    requires transitive com.swirlds.metrics.api;
    requires transitive org.hiero.clpr.relay.core;
    requires transitive org.hiero.clpr.relay.proto;
    requires transitive com.fasterxml.jackson.databind;
    requires com.hedera.cryptography.hints;
    requires com.fasterxml.jackson.core;
    requires java.net.http;
    requires static org.jspecify;

    exports org.hiero.clpr.relay.evm;
    exports org.hiero.clpr.relay.evm.jsonrpc;
    exports org.hiero.clpr.relay.evm.model;
    exports org.hiero.clpr.relay.evm.sei;
    exports org.hiero.clpr.relay.evm.storage;
    exports org.hiero.clpr.relay.evm.beacon;
}
