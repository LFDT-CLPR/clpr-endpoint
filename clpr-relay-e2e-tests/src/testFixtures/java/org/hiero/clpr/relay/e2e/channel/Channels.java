// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.channel;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import org.hiero.clpr.relay.e2e.chain.Accounts;
import org.hiero.clpr.relay.e2e.chain.BesuNetwork;
import org.hiero.clpr.relay.e2e.chain.ChainTx;
import org.hiero.clpr.relay.e2e.chain.E2eContracts;
import org.hiero.clpr.relay.e2e.chain.QbftConfigProofBuilder;
import org.hiero.clpr.relay.e2e.chain.TestAccount;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpoint;
import org.hiero.clpr.relay.e2e.endpoint.EvmEndpointSpec;
import org.hiero.clpr.relay.e2e.fault.PeerNetworkShim;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.EthSigner;
import org.jspecify.annotations.Nullable;

/**
 * The channels declared by one test, and the on-chain wiring that brings them up.
 *
 * <p>Declaration is separate from wiring because the wiring needs things that only exist once the
 * environment is partly up: live chains to build config proofs against, deployed services to register
 * with, and the endpoints' pinned addresses to publish as each side's peer roster. {@code declare} is
 * therefore cheap and {@link #wireAll()} does the work, called from {@code startAll()} between
 * contract deployment and endpoint startup.
 */
public final class Channels {

    /** gRPC sync port every endpoint listens on inside the Docker network. */
    private static final int PEER_SYNC_PORT = EvmEndpoint.GRPC_PORT;

    /** Minimum bond an endpoint must post to be admitted: 0.01 ether. */
    private static final BigInteger ENDPOINT_BOND_WEI = BigInteger.valueOf(10_000_000_000_000_000L);

    private final SecureRandom random = new SecureRandom();
    private final List<Channel> channels = new ArrayList<>();
    private final Set<Channel> wired = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
    private final Accounts accounts;
    private Duration wiringReceiptTimeout = ChainTx.DEFAULT_RECEIPT_TIMEOUT;

    /** Optional Toxiproxy shim. When set, seed_endpoints publish the shim address, not the endpoint's own. */
    @Nullable
    private PeerNetworkShim peerNetworkShim;

    public Channels(final Accounts accounts) {
        this.accounts = accounts;
    }

    /**
     * Override the receipt-wait budget used for every on-chain wiring transaction.
     *
     * <p>Wiring many channels means many sequential transactions on the same chain, each still bound
     * by the same fixed budget as a lone call — {@link ChainTx#DEFAULT_RECEIPT_TIMEOUT} is generous
     * for one transaction but a single transient stall late in a long sequential run can exceed it
     * even though the chain isn't actually stuck. Tests wiring at scale should raise this.
     *
     * @param timeout the new receipt-wait budget for subsequent {@link #wireAll()} /
     *     {@link #wireForDiscovery} / {@link #registerFreshConnector} calls
     * @return this, for chaining before {@code createMany}/{@code startAll()}
     */
    public Channels withWiringReceiptTimeout(final Duration timeout) {
        this.wiringReceiptTimeout = timeout;
        return this;
    }

    /**
     * Route every seed endpoint published from this point onward through {@code shim}.
     *
     * <p>Call before {@code startAll()}, before {@link #wireAll()} runs. Wiring a channel writes the
     * dial address on chain, and the address is immutable afterwards — installing the shim later
     * would leave the roster pointing at the endpoint directly and the shim would never see traffic.
     *
     * @param shim the (already-started) shim
     */
    public void useShim(final PeerNetworkShim shim) {
        this.peerNetworkShim = shim;
    }

    /**
     * Declare a channel.
     *
     * @param spec the channel description
     * @return the declared channel, unwired until {@code startAll()}
     */
    public Channel create(final ChannelSpec spec) {
        final byte[] salt = spec.salt32() != null ? spec.salt32() : freshSalt();
        final Channel channel = new Channel(spec, salt);
        channels.add(channel);
        return channel;
    }

    /**
     * Declare a channel from a builder.
     *
     * @param builder the partially built spec
     * @return the declared channel
     */
    public Channel create(final ChannelSpec.Builder builder) {
        return create(builder.build());
    }

    /**
     * Declare {@code count} channels between the same pair of chains.
     *
     * <p>Each gets its own salt, hence its own id — the service keys everything by channel id, so
     * reusing one would collide on registration.
     *
     * @param count how many
     * @param template the shared shape
     * @return the declared channels
     */
    public List<Channel> createMany(final int count, final ChannelSpec template) {
        final List<Channel> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(create(new ChannelSpec(
                    template.networkA(),
                    template.networkB(),
                    template.endpointsA(),
                    template.endpointsB(),
                    template.connectorStakeWei(),
                    template.connectorFundingWeiA(),
                    template.connectorFundingWeiB(),
                    freshSalt(),
                    template.serviceA(),
                    template.serviceB())));
        }
        return out;
    }

    /** @return every declared channel. */
    public List<Channel> all() {
        return List.copyOf(channels);
    }

    /**
     * @param index declaration order index
     * @return that channel
     */
    public Channel get(final int index) {
        return channels.get(index);
    }

    /**
     * Wire every not-yet-wired channel on chain, then feed the results into the endpoints' configs.
     *
     * <p>Skips channels already wired, so calling it twice is safe — re-running {@code registerChannel}
     * for an existing id would revert.
     */
    public void wireAll() {
        for (final Channel channel : channels) {
            if (wired.add(channel)) {
                wire(channel);
            }
        }
        configureEndpoints();
    }

    /**
     * Wire one channel on chain <b>without</b> touching any endpoint configuration.
     *
     * <p>For channels created after the endpoints are already running. The relay reads its config once
     * at startup, so a running endpoint can only pick this channel up through on-chain discovery —
     * which is why every serving endpoint must have {@link EvmEndpointSpec#discoverChannels()}
     * enabled. Without it the channel would exist on chain and simply never be served, and the test
     * would fail much later with no hint as to why.
     *
     * @param channel a declared, not-yet-wired channel
     * @throws IllegalStateException if the channel is already wired, or a serving endpoint cannot
     *     discover it
     */
    public void wireForDiscovery(final Channel channel) {
        if (!channels.contains(channel)) {
            throw new IllegalStateException("channel was not created through this Channels instance");
        }
        if (!wired.add(channel)) {
            throw new IllegalStateException("channel " + channel.idHex() + " is already wired");
        }
        for (final EvmEndpoint endpoint : servingEndpoints(channel.spec())) {
            if (!endpoint.spec().discoverChannels()) {
                throw new IllegalStateException("endpoint " + endpoint.id() + " has channel discovery disabled,"
                        + " so it would never serve a channel wired after it started. Declare it with"
                        + " EvmEndpointSpec.defaults().withDiscoverChannels(true), or wire this channel"
                        + " before startAll().");
            }
        }
        wire(channel);
    }

    private List<EvmEndpoint> servingEndpoints(final ChannelSpec spec) {
        final List<EvmEndpoint> all = new ArrayList<>(spec.endpointsA());
        all.addAll(spec.endpointsB());
        return all;
    }

    /**
     * Register one channel on both chains and give it a funded connector on each side.
     *
     * <p>Order is dictated by the contracts. Endpoints must be registered before they can be named in
     * a channel; the verifier wrapper's peer roster must be seeded before {@code completeChannel}
     * reads it; and the commit must land before the reveal that proves knowledge of the same
     * {@code (id, pubKey, salt)} triple.
     */
    private void wire(final Channel channel) {
        final ChannelSpec spec = channel.spec();
        final BesuNetwork netA = spec.networkA();
        final BesuNetwork netB = spec.networkB();
        // Resolve which CLPR stack each side wires against — the chain's primary by default, or an
        // additional one deployed via BesuNetwork.deployAdditionalService() when a test wants two
        // channels on the same chain pair to target distinct ClprService addresses.
        final E2eContracts contractsA = spec.resolvedServiceA();
        final E2eContracts contractsB = spec.resolvedServiceB();
        final byte[] salt = channel.salt();

        // ONE operator identity for both sides. The channel id is keccak over the two chain ids, the
        // operator public key and the salt — so if each chain used its own key, chain B would re-derive
        // a different id from chain A's and reject completeChannel with ClprInvalidChannelId. The
        // account is prefunded on every chain because the whole pool is in each genesis alloc.
        final TestAccount operator = accounts.allocate("channel operator");

        // The operator has to be an admitted endpoint on both chains before it may register a channel.
        // registerEndpoint only posts the bond — it does NOT touch the endpoint manifest, which matters
        // because the manifest proof below asserts the manifest is still the pristine
        // {version:1, service_address, endpoints:[]}.
        registerEndpoint(netA, contractsA, operator);
        registerEndpoint(netB, contractsB, operator);

        final byte[] channelId = ClprIds.deriveChannelId(
                netA.rpc(), contractsA.clprService(), netB.caip2(), operator.publicKey64(), salt);
        // Both chains must agree on the id; the derivation sorts the chain ids canonically, so this is
        // a cheap guard against a drift in that ordering rather than a tautology.
        final byte[] channelIdFromB = ClprIds.deriveChannelId(
                netB.rpc(), contractsB.clprService(), netA.caip2(), operator.publicKey64(), salt);
        if (!java.util.Arrays.equals(channelId, channelIdFromB)) {
            throw new IllegalStateException("the two chains derive different channel ids for the same operator and"
                    + " salt: " + AbiCodec.toHex(channelId) + " on " + netA.id() + " vs "
                    + AbiCodec.toHex(channelIdFromB) + " on " + netB.id());
        }

        // Per-channel contracts. A verifier wrapper per channel because setSeedEndpoints is per
        // wrapper and has to name that channel's peer endpoint. The inner QBFTVerifier is the one
        // belonging to the CLPR stack this channel is wired against, not necessarily the chain's
        // primary — for override channels the primary verifier and the override verifier are two
        // separate deployments.
        final String verifierA = deploy(netA, contractsA.qbftVerifier());
        final String verifierB = deploy(netB, contractsB.qbftVerifier());
        final String connectorContractA = deployConnector(netA);
        final String connectorContractB = deployConnector(netB);

        // Seed each side's peer roster with the address of the endpoint serving the OTHER side. The
        // production verifier returns an empty seed list, which would leave the relay with nobody to
        // sync to; this is the one thing the E2E wrapper adds.
        setSeedEndpoints(netA, verifierA, spec.endpointsB());
        setSeedEndpoints(netB, verifierB, spec.endpointsA());

        // Commit on both chains, with the same commitment since the operator key is shared.
        final byte[] commitment = ClprIds.channelCommitment(channelId, operator.publicKey64());
        ChainTx.sendAndAwait(
                netA.rpc(),
                netA.rawRpc(),
                netA.submitterFor(operator),
                contractsA.clprService(),
                ClprAbi.registerChannel(channelId, commitment),
                BigInteger.ZERO,
                "registerChannel on " + netA.id(),
                wiringReceiptTimeout);
        ChainTx.sendAndAwait(
                netB.rpc(),
                netB.rawRpc(),
                netB.submitterFor(operator),
                contractsB.clprService(),
                ClprAbi.registerChannel(channelId, commitment),
                BigInteger.ZERO,
                "registerChannel on " + netB.id(),
                wiringReceiptTimeout);

        // Cross-wired proofs: each side is completed with a proof of the OTHER chain's state. The
        // proofs must reference the peer chain's ACTUAL service — the one this channel is
        // registered on — not the peer's primary. Otherwise a channel wired against an override
        // service would carry a trust anchor pointing at the primary, and every bundle the peer
        // relay produces would fail to verify.
        final byte[] proofOfA = QbftConfigProofBuilder.buildConfigProof(netA, contractsA.clprService());
        final byte[] proofOfB = QbftConfigProofBuilder.buildConfigProof(netB, contractsB.clprService());
        final byte[] manifestOfA = QbftConfigProofBuilder.buildManifestProof(netA, contractsA.clprService());
        final byte[] manifestOfB = QbftConfigProofBuilder.buildManifestProof(netB, contractsB.clprService());

        final EthSigner operatorSigner = new EthSigner(operator.privateKeyHex());
        // The reveal digest binds the id to the service address, so each chain needs its own signature.
        final byte[] sigA = operatorSigner.signMessage(ClprIds.revealDigest(channelId, contractsA.clprService()));
        final byte[] sigB = operatorSigner.signMessage(ClprIds.revealDigest(channelId, contractsB.clprService()));

        ChainTx.sendAndAwait(
                netA.rpc(),
                netA.rawRpc(),
                netA.submitterFor(operator),
                contractsA.clprService(),
                ClprAbi.completeChannel(
                        channelId, operator.publicKey64(), sigA, salt, verifierA, proofOfB, manifestOfB),
                BigInteger.ZERO,
                "completeChannel on " + netA.id(),
                wiringReceiptTimeout);
        ChainTx.sendAndAwait(
                netB.rpc(),
                netB.rawRpc(),
                netB.submitterFor(operator),
                contractsB.clprService(),
                ClprAbi.completeChannel(
                        channelId, operator.publicKey64(), sigB, salt, verifierB, proofOfA, manifestOfA),
                BigInteger.ZERO,
                "completeChannel on " + netB.id(),
                wiringReceiptTimeout);

        // completeChannel succeeding already proves the manifest proof passed the inner verifier;
        // this confirms the channel actually carries a peer manifest rather than the version-0
        // sentinel, which is what the relay reads to find its peer.
        requirePeerManifest(netA, contractsA.clprService(), channelId);
        requirePeerManifest(netB, contractsB.clprService(), channelId);

        final ChannelSide sideA =
                new ChannelSide(netA, contractsA, spec.endpointsA(), channelId, verifierA, contractsA.e2eApplication());
        final ChannelSide sideB =
                new ChannelSide(netB, contractsB, spec.endpointsB(), channelId, verifierB, contractsB.e2eApplication());

        sideA.peer(sideB);
        sideB.peer(sideA);

        // Endpoint-signer registration is deferred to Channels.configureEndpoints(), where the
        // (endpoint, service) → signer assignment is decided. Registering the primary here on every
        // service the endpoint touches would leave stray registerEndpoint transactions from the
        // primary signer on services it never actually serves — noise that a signer-scoped routing
        // assertion cannot distinguish from a real cross-service bundle submission.

        sideA.connector(registerConnector(
                netA,
                contractsA.clprService(),
                operator,
                channelId,
                salt,
                connectorContractA,
                spec.connectorStakeWei(),
                spec.connectorFundingWeiA()));
        sideB.connector(registerConnector(
                netB,
                contractsB.clprService(),
                operator,
                channelId,
                salt,
                connectorContractB,
                spec.connectorStakeWei(),
                spec.connectorFundingWeiB()));

        channel.wired(channelId, operator, sideA, sideB);
    }

    private Connector registerConnector(
            final BesuNetwork net,
            final String clprServiceAddress,
            final TestAccount operator,
            final byte[] channelId,
            final byte[] salt,
            final String connectorContract,
            final BigInteger stakeWei,
            final BigInteger fundingWei) {
        final byte[] connectorId =
                ClprIds.deriveConnectorId(net.rpc(), clprServiceAddress, channelId, operator.publicKey64(), salt);

        ChainTx.sendAndAwait(
                net.rpc(),
                net.rawRpc(),
                net.submitterFor(operator),
                clprServiceAddress,
                ClprAbi.registerConnector(ClprIds.connectorCommitment(connectorId, operator.publicKey64())),
                BigInteger.ZERO,
                "registerConnector on " + net.id(),
                wiringReceiptTimeout);

        final byte[] sig = new EthSigner(operator.privateKeyHex())
                .signMessage(ClprIds.revealDigest(connectorId, clprServiceAddress));
        ChainTx.sendAndAwait(
                net.rpc(),
                net.rawRpc(),
                net.submitterFor(operator),
                clprServiceAddress,
                ClprAbi.completeConnector(
                        connectorId,
                        operator.publicKey64(),
                        sig,
                        salt,
                        channelId,
                        connectorContract,
                        operator.address()),
                stakeWei,
                "completeConnector on " + net.id(),
                wiringReceiptTimeout);

        final Connector connector = new Connector(net, connectorId, connectorContract, wiringReceiptTimeout);
        // The stake is locked in the service; this is the separate prepaid budget the connector
        // contract spends on inbound message execution. Zero is a legitimate request — it is how the
        // underfunded-connector scenario is set up — so skip the transfer rather than sending nothing.
        if (fundingWei.signum() > 0) {
            connector.fund(fundingWei);
        }
        return connector;
    }

    private void setSeedEndpoints(final BesuNetwork net, final String verifier, final List<EvmEndpoint> peers) {
        final List<ClprAbi.Endpoint> endpoints =
                peers.stream().map(this::dialAddressFor).toList();
        ChainTx.sendAndAwait(
                net.rpc(),
                net.rawRpc(),
                net.deployerSubmitter(),
                verifier,
                ClprAbi.setSeedEndpoints(endpoints),
                BigInteger.ZERO,
                "setSeedEndpoints on " + net.id(),
                wiringReceiptTimeout);
    }

    private void requirePeerManifest(final BesuNetwork net, final String clprServiceAddress, final byte[] channelId) {
        final String result = net.rpc()
                .ethCall(clprServiceAddress, AbiCodec.toHex(ClprAbi.getPeerEndpointManifest(channelId)), "latest");
        final byte[] decoded = AbiCodec.fromHex(result);
        if (decoded.length < 32) {
            throw new IllegalStateException(
                    "getPeerEndpointManifest on " + net.id() + " returned " + decoded.length + " bytes: " + result);
        }
        // The struct's first word is `version`; 0 is the uninitialised sentinel.
        final long version = AbiCodec.decodeUint64(decoded, 0);
        if (version < 1L) {
            throw new IllegalStateException("peer endpoint-manifest version on " + net.id() + " is " + version
                    + " after completeChannel, expected >= 1. The relay reads this manifest to find its peer,"
                    + " so a version-0 sentinel means the channel would never sync.");
        }
    }

    /**
     * The dial address to publish in {@code endpoint}'s peer roster: the endpoint's own IP by default,
     * or a Toxiproxy proxy address when the shim is active. Registering with the shim is idempotent, so
     * calling this for the same endpoint from both sides' wiring returns the same address.
     */
    private ClprAbi.Endpoint dialAddressFor(final EvmEndpoint endpoint) {
        final PeerNetworkShim shim = peerNetworkShim;
        if (shim == null) {
            return ClprAbi.Endpoint.of(endpoint.containerIp(), PEER_SYNC_PORT);
        }
        final PeerNetworkShim.ProxyAddress proxy = shim.registerPeerProxy(endpoint);
        return ClprAbi.Endpoint.of(proxy.host(), proxy.port());
    }

    /**
     * Admit an account as an endpoint on a specific {@code ClprService} deployment on {@code net}.
     * Idempotent per (account, service).
     *
     * @param net the chain
     * @param contracts the CLPR stack the endpoint is being registered against — the chain's primary
     *     or an additional one deployed via {@link BesuNetwork#deployAdditionalService()}
     * @param account the account to admit
     */
    private void registerEndpoint(final BesuNetwork net, final E2eContracts contracts, final TestAccount account) {
        final var interactor = net.interactorFor(account, contracts.core());
        if (!interactor.isEndpointRegistered(account.address())) {
            interactor.registerEndpoint(ENDPOINT_BOND_WEI);
        }
    }

    private String deploy(final BesuNetwork net, final String innerVerifier) {
        return net.deployer().deployChannelVerifier(innerVerifier, wiringReceiptTimeout);
    }

    private String deployConnector(final BesuNetwork net) {
        return net.deployer().deployConnector(wiringReceiptTimeout);
    }

    /**
     * Give every endpoint the configuration it needs for the channels it serves: the chain it reads,
     * the service it operates, the channel ids to bring up, and each peer's proof type.
     *
     * <p>Grouped by (endpoint, chain, service): an endpoint serving several channels on the same
     * (chain, service) pair gets one {@code localNetworks} entry and one {@code clprServices} entry
     * carrying all those channel ids. Two channels sharing the endpoint and chain but pointing at
     * different {@code ClprService} deployments produce two separate {@code clprServices[]} entries
     * — with distinct signing keys — which is exactly the shape the multi-service isolation test
     * asserts against.
     */
    private void configureEndpoints() {
        record ServiceKey(EvmEndpoint endpoint, BesuNetwork network, String serviceAddress) {}
        final var channelIdsByService = new LinkedHashMap<ServiceKey, List<String>>();
        final var contractsByService = new LinkedHashMap<ServiceKey, E2eContracts>();

        for (final Channel channel : channels) {
            for (final ChannelSide side : List.of(channel.a(), channel.b())) {
                final ChannelSide peer = channel.peerOf(side);
                for (final EvmEndpoint endpoint : side.endpoints()) {
                    final ServiceKey key = new ServiceKey(endpoint, side.network(), side.clprServiceAddress());
                    channelIdsByService
                            .computeIfAbsent(key, k -> new ArrayList<>())
                            .add(channel.idHex());
                    contractsByService.putIfAbsent(key, side.contracts());
                    // The relay resolves a channel's peer proof type from the peer chain id it reads
                    // on-chain; an unmapped peer is skipped without an error.
                    endpoint.addPeerProofType(peer.network().caip2(), "QBFT");
                }
            }
        }

        // Track which of an endpoint's signers has already been assigned to a service, so the FIRST
        // service on an endpoint keeps using the primary signer (backward compat) and each SUBSEQUENT
        // service is given its own freshly-allocated key. Filtering by from-address on-chain then
        // maps every submitBundle transaction back to a single (endpoint, service) pair — the
        // strongest attribution check available without instrumenting the relay.
        record ServiceSlot(String slotName, TestAccount account) {}
        final var slotByEndpointService = new HashMap<ServiceKey, ServiceSlot>();
        final var endpointUsageCount = new IdentityHashMap<EvmEndpoint, Integer>();

        for (final ServiceKey key : channelIdsByService.keySet()) {
            final int usage = endpointUsageCount.getOrDefault(key.endpoint(), 0);
            final ServiceSlot slot;
            if (usage == 0) {
                slot = new ServiceSlot(
                        key.endpoint().signingKeySlot(), key.endpoint().signerAccount());
            } else {
                final TestAccount extra =
                        accounts.allocate(key.endpoint().id() + " signer for " + key.serviceAddress());
                final String slotName = key.endpoint().registerAdditionalSigner(extra);
                slot = new ServiceSlot(slotName, extra);
            }
            // Admit this endpoint's chosen signer on the target service before the container starts.
            // submitBundle is gated on getEndpointEntry(from) != NONE, so a signer that is not
            // admitted has every bundle it submits revert. Idempotent. Running this for every
            // signer — primary and additional alike — keeps the on-chain footprint of each signer
            // scoped to its own service, which is what the routing attribution check depends on.
            registerEndpoint(key.network(), contractsByService.get(key), slot.account());
            slotByEndpointService.put(key, slot);
            endpointUsageCount.put(key.endpoint(), usage + 1);
        }

        channelIdsByService.forEach((key, channelIds) -> {
            final ServiceSlot slot = slotByEndpointService.get(key);
            key.endpoint().serve(key.network(), key.serviceAddress(), slot.slotName(), channelIds);
        });
    }

    /**
     * Register a replacement connector on one side of a live channel.
     *
     * <p>The recovery path after a connector is banned: a banned connector cannot be revived, and
     * re-funding it changes nothing, so the channel needs a new one. The new connector gets a fresh
     * salt because its id is derived from {@code (channelId, operator, salt)} — reusing the salt would
     * derive the banned id again.
     *
     * @param channel the live channel
     * @param side the side to give a new connector
     * @param stakeWei stake to lock
     * @param fundingWei prepaid execution budget
     * @return the new connector, also installed on {@code side}
     */
    public Connector registerFreshConnector(
            final Channel channel, final ChannelSide side, final BigInteger stakeWei, final BigInteger fundingWei) {
        final BesuNetwork net = side.network();
        final String contract = deployConnector(net);
        final Connector connector = registerConnector(
                net,
                side.clprServiceAddress(),
                channel.operator(),
                side.channelId(),
                freshSalt(),
                contract,
                stakeWei,
                fundingWei);
        side.connector(connector);
        return connector;
    }

    private byte[] freshSalt() {
        final byte[] salt = new byte[32];
        random.nextBytes(salt);
        return salt;
    }
}
