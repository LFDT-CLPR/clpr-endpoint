// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.chain;

import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Volume;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.hiero.clpr.relay.e2e.LogStream;
import org.jspecify.annotations.Nullable;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

/**
 * One Besu container in a {@link BesuNetwork}: either the QBFT validator or a non-validator full
 * node.
 *
 * <p>Chain state lives in a <b>named Docker volume</b> and the configuration is <b>copied</b> into
 * the container. Testcontainers destroys a container on {@code stop()}, so an in-container data
 * directory would mean {@link #restart()} silently resets the chain to genesis — which would make a
 * node-restart test pass for the wrong reason. A volume outlives the container, so a restart is a
 * genuine process restart against existing state.
 *
 * <p>Neither mechanism hands the Docker daemon a host path, and that is deliberate: on a
 * Docker-in-Docker runner (the ARC runners this repo's CI uses) the daemon's filesystem is not the
 * test JVM's, so a bind mount resolves to an empty directory inside the daemon. Besu then cannot
 * find its genesis file and exits with code 2 before the JSON-RPC service ever starts. Both a
 * volume and a file copy go through the daemon API instead, so they behave the same locally and
 * under DinD. Do not reintroduce {@code withFileSystemBind} here.
 */
public final class BesuNode {

    /** JSON-RPC port inside the container. */
    public static final int RPC_PORT = 8545;

    /** P2P port inside the container. */
    public static final int P2P_PORT = 30303;

    /** Container path holding the copied genesis file and node key. */
    private static final String CONFIG_PATH = "/config";

    /** Container path backed by the node's data volume. */
    private static final String DATA_PATH = "/data";

    private final String id;
    private final BesuNetworkSpec spec;
    private final TestAccount nodeKey;
    private final boolean validator;
    private final String staticIp;
    private final Network network;
    private final Path configDir;
    private final Path dataDir;
    private final String dataVolume;
    private final int heapMb;

    private final LogStream logStream;

    @Nullable
    private GenericContainer<?> container;

    BesuNode(
            final String id,
            final BesuNetworkSpec spec,
            final TestAccount nodeKey,
            final boolean validator,
            final String staticIp,
            final Network network,
            final Path nodeDir,
            final int heapMb) {
        this.id = id;
        this.spec = spec;
        this.nodeKey = nodeKey;
        this.validator = validator;
        this.staticIp = staticIp;
        this.network = network;
        this.configDir = nodeDir.resolve("config");
        this.dataDir = nodeDir.resolve("data");
        // Scoped to this instance, not to the node id: restarts of this node share the volume, but a
        // later environment reusing the same node id can never resume this chain's state. Node ids
        // are sanitised because Docker only accepts [a-zA-Z0-9][a-zA-Z0-9_.-]* as a volume name.
        this.dataVolume = "clpr-testing-" + id.replaceAll("[^a-zA-Z0-9_.-]", "-") + "-"
                + UUID.randomUUID().toString().substring(0, 8);
        this.heapMb = heapMb;
        this.logStream = new LogStream(id);
        try {
            // Host-side staging only: these directories are read by the test JVM to build the tar it
            // copies into the container. The daemon never sees them, so their permissions do not
            // matter — the image's entrypoint chowns the paths it needs before dropping to the besu
            // user.
            Files.createDirectories(configDir);
            Files.createDirectories(dataDir);
        } catch (final IOException e) {
            throw new UncheckedIOException("failed to create staging directories for Besu node " + id, e);
        }
    }

    /** Write genesis, node key and static-nodes before first start. */
    void writeConfiguration(final String genesisJson, final String staticNodesJson) {
        try {
            Files.writeString(configDir.resolve("genesis.json"), genesisJson, StandardCharsets.UTF_8);
            // Besu accepts the raw hex with or without a 0x prefix.
            Files.writeString(configDir.resolve("node-key"), nodeKey.privateKeyHex() + "\n", StandardCharsets.UTF_8);
            // Besu reads static-nodes.json from the DATA directory, not the config directory.
            // Peer discovery is disabled, so this file is the only way nodes find each other.
            Files.writeString(dataDir.resolve("static-nodes.json"), staticNodesJson, StandardCharsets.UTF_8);
        } catch (final IOException e) {
            throw new UncheckedIOException("failed to write Besu configuration for node " + id, e);
        }
    }

    /** @return the node id, which is also its network alias. */
    public String id() {
        return id;
    }

    /** @return {@code true} if this node is a QBFT validator. */
    public boolean isValidator() {
        return validator;
    }

    /** @return the node's pinned IP on the environment network. */
    public String ipAddress() {
        return staticIp;
    }

    /** @return the node's enode URL, computable without the container running. */
    public String enode() {
        return BesuQbftGenesis.enodeUrl(nodeKey, staticIp, P2P_PORT);
    }

    /** @return the funded account whose key is this node's identity. */
    public TestAccount nodeKey() {
        return nodeKey;
    }

    /** @return host-visible JSON-RPC URL (mapped port), for use from the test JVM. */
    public String jsonRpcUrl() {
        return "http://" + requireContainer().getHost() + ":"
                + requireContainer().getMappedPort(RPC_PORT);
    }

    /** @return in-network JSON-RPC URL, for use from other containers (e.g. the endpoints). */
    public String internalJsonRpcUrl() {
        return "http://" + id + ":" + RPC_PORT;
    }

    /** @return {@code true} if the container is up. */
    public boolean isRunning() {
        return container != null && container.isRunning();
    }

    /** Start the container (no-op if already running). */
    public void start() {
        if (isRunning()) {
            return;
        }
        final GenericContainer<?> c = new GenericContainer<>(spec.image())
                // Pin the address. Endpoint containers publish their peers' addresses on-chain and
                // ClprService only accepts IPv4 literals there, so the whole environment is built on
                // stable IPs rather than aliases. withIpv4Address applies to the network selected by
                // withNetwork below, which Testcontainers turns into the container's network mode
                // before create-command modifiers run.
                .withCreateContainerCmdModifier(cmd -> {
                    cmd.withHostName(id).withIpv4Address(staticIp);
                    mountDataVolume(cmd.getHostConfig());
                })
                .withNetwork(network)
                .withNetworkAliases(id)
                .withExposedPorts(RPC_PORT)
                // Copied, not bind-mounted: see the class javadoc. The copy happens after the
                // container is created and before it starts, so files land in the already-mounted
                // data volume. Both files are rewritten on every start, which is harmless — the
                // configuration is immutable once writeConfiguration has run.
                .withCopyFileToContainer(
                        MountableFile.forHostPath(configDir.resolve("genesis.json")), CONFIG_PATH + "/genesis.json")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(configDir.resolve("node-key")), CONFIG_PATH + "/node-key")
                // Besu reads static-nodes.json from the data directory, so this one goes into the
                // volume rather than next to the genesis file.
                .withCopyFileToContainer(
                        MountableFile.forHostPath(dataDir.resolve("static-nodes.json")),
                        DATA_PATH + "/static-nodes.json")
                .withEnv("BESU_OPTS", "-Xmx" + heapMb + "m")
                .withCommand(commandArgs())
                // Capture output into a buffer that outlives the container. Testcontainers deletes
                // the container when a wait strategy fails, taking getLogs() with it — precisely
                // when the log is the only thing that explains the failure.
                .withLogConsumer(logStream.consumer())
                // Wait on the log line, not on an HTTP probe. Besu drops the connection on an empty
                // POST (and answers 415 to a GET), so the HTTP strategies that work for Anvil report
                // a socket error here and time out against a perfectly healthy node.
                .waitingFor(Wait.forLogMessage(".*JSON-RPC service started.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)));
        spec.extraEnv().forEach(c::withEnv);
        c.start();
        this.container = c;
    }

    private String[] commandArgs() {
        final List<String> args = new ArrayList<>(List.of(
                "--genesis-file=" + CONFIG_PATH + "/genesis.json",
                "--node-private-key-file=" + CONFIG_PATH + "/node-key",
                "--data-path=" + DATA_PATH,
                // FOREST keeps startup cheap for a throwaway chain; BONSAI's extra bookkeeping
                // buys nothing when the whole state lives for minutes.
                "--data-storage-format=FOREST",
                "--network-id=" + spec.chainId(),
                "--rpc-http-enabled",
                "--rpc-http-host=0.0.0.0",
                "--rpc-http-port=" + RPC_PORT,
                "--rpc-http-cors-origins=all",
                // DEBUG is required: the CLPR config proof needs eth_getProof plus raw header
                // access, and TXPOOL lets a load test see whether sends are queueing or landing.
                "--rpc-http-api=ETH,NET,WEB3,DEBUG,TXPOOL,MINER,QBFT",
                // Uncapped eth_call gas: reading contract state and previewing submitBundle both
                // exceed the default cap on a bundle of any size.
                "--rpc-gas-cap=0",
                "--host-allowlist=*",
                "--min-gas-price=" + spec.minGasPriceWei(),
                "--p2p-host=" + staticIp,
                "--p2p-port=" + P2P_PORT,
                // static-nodes.json is authoritative; discovery would only add noise on a
                // throwaway bridge network.
                "--discovery-enabled=false",
                "--logging=INFO"));
        return args.toArray(String[]::new);
    }

    /** Graceful stop. The data volume survives, so {@link #start()} resumes the chain. */
    public void stop() {
        if (container != null) {
            container.stop();
            container = null;
        }
    }

    /** Stop and start again against the retained data volume. */
    public void restart() {
        stop();
        start();
    }

    /** SIGKILL — an abrupt crash, with no chance to flush state. */
    public void kill() {
        final GenericContainer<?> c = requireContainer();
        c.getDockerClient().killContainerCmd(c.getContainerId()).exec();
    }

    /**
     * Freeze the process. Unlike {@link #stop()} this keeps the port mapping and in-memory chain, so
     * callers see RPC timeouts rather than connection refusals — the shape of a real node outage.
     */
    public void pause() {
        final GenericContainer<?> c = requireContainer();
        c.getDockerClient().pauseContainerCmd(c.getContainerId()).exec();
    }

    /** Resume a paused container. */
    public void unpause() {
        final GenericContainer<?> c = requireContainer();
        c.getDockerClient().unpauseContainerCmd(c.getContainerId()).exec();
    }

    /** @return the captured output, which survives container removal and restarts. */
    public LogStream logs() {
        return logStream;
    }

    private GenericContainer<?> requireContainer() {
        final GenericContainer<?> c = container;
        if (c == null) {
            throw new IllegalStateException("Besu node " + id + " is not started");
        }
        return c;
    }

    /**
     * Attach the node's data volume, preserving any bind Testcontainers has already configured.
     *
     * <p>Docker creates the volume on first use, so there is no explicit create step. Replacing the
     * bind list outright would be enough today, but appending keeps this correct if the container
     * ever gains another mount.
     */
    private void mountDataVolume(@Nullable final HostConfig hostConfig) {
        if (hostConfig == null) {
            throw new IllegalStateException("no host config on the create command for Besu node " + id);
        }
        final List<Bind> binds = new ArrayList<>();
        final Bind[] existing = hostConfig.getBinds();
        if (existing != null) {
            binds.addAll(Arrays.asList(existing));
        }
        binds.add(new Bind(dataVolume, new Volume(DATA_PATH)));
        hostConfig.withBinds(binds);
    }

    /**
     * Delete the node's data volume. Terminal: the chain cannot be resumed afterwards, so this is
     * for environment teardown only, never for {@link #stop()}.
     *
     * <p>Best-effort by design. Ryuk is disabled for this suite, so nothing else reclaims the
     * volume — but a volume that was never created (a node that failed before its first start) must
     * not turn a teardown into a second failure that masks the first.
     */
    void discardState() {
        try {
            DockerClientFactory.instance().client().removeVolumeCmd(dataVolume).exec();
        } catch (final RuntimeException e) {
            // best-effort
        }
    }
}
