// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.grpc.server;

import com.hedera.pbj.grpc.helidon.PbjRouting;
import com.hedera.pbj.grpc.helidon.config.PbjConfig;
import com.swirlds.logging.api.Logger;
import com.swirlds.logging.api.Loggers;
import io.helidon.webserver.WebServer;

/**
 * Helidon-based gRPC server hosting the CLPR endpoint service on a single port: the two-phase
 * {@code sync} RPC ({@link ClprSyncService}) and {@code getLedgerConfiguration} ({@link ClprInfoService}),
 * which returns chain-verifiable public data.
 */
public class ClprGrpcServer {

    private static final Logger LOG = Loggers.getLogger(ClprGrpcServer.class);

    private final int port;
    private final int maxMessageSize;
    private final String instanceName;

    private final ClprSyncService syncService;
    private final ClprInfoService infoService;

    private volatile WebServer server;

    /**
     * Creates a new gRPC server.
     *
     * @param port                          the port to listen on
     * @param maxMessageSize                maximum inbound gRPC message size in bytes; overrides PBJ's 10 KiB default
     * @param syncHandler                   the handler for sync RPCs
     * @param getLedgerConfigurationHandler the handler for {@code getLedgerConfiguration} RPCs
     * @param instanceName                  free-form label used in the server name and logs
     */
    public ClprGrpcServer(
            final int port,
            final int maxMessageSize,
            final ClprSyncHandler syncHandler,
            final GetLedgerConfigurationHandler getLedgerConfigurationHandler,
            final String instanceName) {
        this.port = port;
        this.maxMessageSize = maxMessageSize;
        this.instanceName = instanceName;
        this.syncService = new ClprSyncService(syncHandler);
        this.infoService = new ClprInfoService(getLedgerConfigurationHandler);
    }

    /**
     * Starts the gRPC server. Blocks until the server is ready to accept connections.
     */
    public void start() {
        server = WebServer.builder()
                .name(instanceName + " clpr-grpc")
                .port(port)
                .addProtocol(PbjConfig.builder()
                        .name("pbj")
                        .maxMessageSizeBytes(maxMessageSize)
                        .build())
                .addRouting(PbjRouting.builder().service(syncService).service(infoService))
                .build()
                .start();
        LOG.info("CLPR gRPC server started on port {} (maxMessageSize={} bytes)", server.port(), maxMessageSize);
    }

    /**
     * Stops the gRPC server.
     */
    public void stop() {
        if (server != null) {
            server.stop();
            LOG.info("CLPR gRPC server stopped");
        }
    }

    /**
     * Returns the actual port the server is listening on, or {@code -1} if not started.
     *
     * @return the listening port
     */
    public int port() {
        return server != null ? server.port() : -1;
    }

    /**
     * Returns whether the server is currently running.
     *
     * @return {@code true} if the server is running
     */
    public boolean isRunning() {
        return server != null && server.isRunning();
    }
}
