// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.hedera.hapi.node.state.clpr.ClprBundleContent;
import com.hedera.hapi.node.state.clpr.ClprChannelStatus;
import com.hedera.hapi.node.state.clpr.ClprMessage;
import com.hedera.hapi.node.state.clpr.ClprMessagePayload;
import com.hedera.hapi.node.state.clpr.ClprMessageReply;
import com.hedera.hapi.node.state.clpr.ClprMessageReplyStatus;
import com.hedera.hapi.node.state.clpr.ClprQueueMetadata;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import org.hiero.clpr.relay.evm.AbiCodec;
import org.hiero.clpr.relay.evm.EthSigner;
import org.hiero.clpr.relay.evm.jsonrpc.EvmJsonRpcClient;
import org.hiero.clpr.relay.test.harness.ChainTxSubmitter;
import org.jspecify.annotations.Nullable;

/**
 * A helper for configuring and querying the deployed CLPR contracts from integration tests.
 *
 * <p>All transactions are client-signed as EIP-1559 raw transactions via the supplied
 * {@link AnvilTxSubmitter} and submitted via {@code eth_sendRawTransaction}. Endpoint
 * commit-reveal signatures (channel and connector setup) are also produced client-side
 * via {@link EthSigner#signMessage(byte[])}, applying the EIP-191 {@code "Ethereum
 * Signed Message:\n32"} prefix exactly as Anvil's {@code eth_sign} JSON-RPC method would.
 */
public final class ContractInteractor {

    private static final long DEFAULT_GAS_LIMIT = 0xffffffL;
    private static final int RECEIPT_POLL_ATTEMPTS = 120;
    private static final long RECEIPT_POLL_INTERVAL_MS = 500L;

    private final EvmJsonRpcClient rpc;
    private final ChainTxSubmitter txSubmitter;
    private final EthSigner signer;
    private final byte[] signerPubKey64;
    private final String fromAddress;
    private final DeployedContracts contracts;

    /**
     * Create a new interactor.
     *
     * @param rpc            the JSON-RPC client for the target node, used for queries and
     *                       receipt polling
     * @param txSubmitter    client-side signing helper used to send every transaction
     * @param signer         the {@link EthSigner} used to produce endpoint commit-reveal
     *                       signatures via {@link EthSigner#signMessage(byte[])}; its address
     *                       must match {@code txSubmitter.address()}
     * @param signerPubKey64 the 64-byte uncompressed secp256k1 public key (X||Y) for the signer
     * @param contracts      the deployed contract set
     */
    public ContractInteractor(
            final EvmJsonRpcClient rpc,
            final ChainTxSubmitter txSubmitter,
            final EthSigner signer,
            final byte[] signerPubKey64,
            final DeployedContracts contracts) {
        if (signerPubKey64.length != 64) {
            throw new IllegalArgumentException("signerPubKey64 must be 64 bytes, got " + signerPubKey64.length);
        }
        if (!signer.address().equalsIgnoreCase(txSubmitter.address())) {
            throw new IllegalArgumentException("signer.address() must equal txSubmitter.address()");
        }
        this.rpc = rpc;
        this.txSubmitter = txSubmitter;
        this.signer = signer;
        this.signerPubKey64 = signerPubKey64.clone();
        this.fromAddress = txSubmitter.address();
        this.contracts = contracts;
    }

    /** @return the underlying JSON-RPC client */
    public EvmJsonRpcClient rpc() {
        return rpc;
    }

    /** @return the sender address used by this interactor */
    public String fromAddress() {
        return fromAddress;
    }

    /** @return the deployed contract addresses */
    public DeployedContracts contracts() {
        return contracts;
    }

    // -------------------------------------------------------------------------
    // Configuration: economics + ledger
    // -------------------------------------------------------------------------

    /**
     * Call {@code ClprService.updateEconomicConfiguration} with sensible test defaults.
     *
     * <p>The {@code EconomicConfig} struct has 11 fields (all static, padded to 32 bytes each
     * in ABI encoding):
     * <ol>
     *   <li>messageExecutionCost (uint256)</li>
     *   <li>endpointMarginPercent (uint256)</li>
     *   <li>minLockedStake (uint256)</li>
     *   <li>minEndpointBond (uint256)</li>
     *   <li>basePenalty (uint256)</li>
     *   <li>penaltyMultiplier (uint256)</li>
     *   <li>slashBanThreshold (uint32)</li>
     *   <li>connectorQueueQuotaPct (uint32)</li>
     *   <li>connectorInboundGasStipend (uint64)</li>
     *   <li>maxChannels (uint32)</li>
     *   <li>maxConnectors (uint32)</li>
     * </ol>
     */
    public void configureDefaultEconomics() {
        final byte[] selector = AbiCodec.functionSelector(
                "updateEconomicConfiguration((uint256,uint256,uint256,uint256,uint256,uint256,uint32,uint32,uint64,uint32,uint32))");
        final byte[] callData = AbiCodec.concat(selector, defaultEconomicConfigStruct());
        sendTxAndWait(contracts.clprServiceAddress(), callData, null);
    }

    /** The default {@code EconomicConfig} as its 11-word all-static ABI tuple (see field list above). */
    private static byte[] defaultEconomicConfigStruct() {
        return AbiCodec.concat(
                AbiCodec.encodeUint256(new BigInteger("1000000000000000")), // messageExecutionCost = 0.001 ether
                AbiCodec.encodeUint256(new BigInteger("10")), // endpointMarginPercent
                AbiCodec.encodeUint256(new BigInteger("100000000000000000")), // minLockedStake = 0.1 ether
                AbiCodec.encodeUint256(new BigInteger("10000000000000000")), // minEndpointBond = 0.01 ether
                AbiCodec.encodeUint256(new BigInteger("10000000000000000")), // basePenalty = 0.01 ether
                AbiCodec.encodeUint256(new BigInteger("2")), // penaltyMultiplier
                AbiCodec.encodeUint(5L), // slashBanThreshold (uint32)
                AbiCodec.encodeUint(50L), // connectorQueueQuotaPct (uint32)
                AbiCodec.encodeUint(50000L), // connectorInboundGasStipend (uint64)
                AbiCodec.encodeUint(0L), // maxChannels (uint32, 0 = no limit)
                AbiCodec.encodeUint(0L)); // maxConnectors (uint32, 0 = no limit)
    }

    /**
     * An endpoint to register in the ledger configuration.
     *
     * @param ipAddress     numeric IPv4/IPv6 address (no DNS names)
     * @param port          gRPC port
     */
    public record EndpointInfo(String ipAddress, int port) {}

    /**
     * Call {@code ClprService.updateLedgerConfiguration(bytes,(uint32,uint64,uint64,uint32,uint64,uint32,uint32),bytes,bytes)}
     * with sensible test defaults.
     *
     * <p>The trailing {@code trustAnchor} / {@code trustAnchorId} config-proof args are sent empty;
     * the contract accepts empty/empty (the pair must be both-empty or both-set).
     *
     * @param serviceAddr this ledger's own service address (contract address bytes)
     */
    public void configureDefaultLedger(final byte[] serviceAddr) {
        // Build the dynamic tails first so we can compute the trailing offsets.
        final byte[] paddedServiceAddr = AbiCodec.padRight32(serviceAddr);
        final byte[] tailServiceAddr = AbiCodec.concat(AbiCodec.encodeUint(serviceAddr.length), paddedServiceAddr);
        // trustAnchor and trustAnchorId are left empty for tests. AdminLogic enforces the spec
        // invariant that trustAnchorId is non-empty iff trustAnchor is non-empty, so empty/empty
        // is valid. Empty `bytes` ABI-encodes as a single 32-byte length word of 0 (no data).
        final byte[] tailTrustAnchor = AbiCodec.encodeUint(0L);
        final byte[] tailTrustAnchorId = AbiCodec.encodeUint(0L);

        // Head layout (10 slots × 32 bytes = 320 bytes):
        //   [0]    offset to serviceAddress (bytes)
        //   [1]    throttles.maxMessagesPerBundle (uint32)
        //   [2]    throttles.maxMessagePayloadBytes (uint64)
        //   [3]    throttles.maxGasPerMessage (uint64)
        //   [4]    throttles.maxQueueDepth (uint32)
        //   [5]    throttles.maxSyncBytes (uint64)
        //   [6]    throttles.maxLocalEndpoints (uint32)
        //   [7]    throttles.maxPeerEndpoints (uint32)
        //   [8]    offset to trustAnchor (bytes)
        //   [9]    offset to trustAnchorId (bytes)
        final long headSize = 10L * 32L;
        final long serviceAddrOffset = headSize;
        final long trustAnchorOffset = serviceAddrOffset + tailServiceAddr.length;
        final long trustAnchorIdOffset = trustAnchorOffset + tailTrustAnchor.length;

        final byte[] selector = AbiCodec.functionSelector(
                "updateLedgerConfiguration(bytes,(uint32,uint64,uint64,uint32,uint64,uint32,uint32),bytes,bytes)");

        final byte[] head = AbiCodec.concat(
                AbiCodec.encodeUint(serviceAddrOffset),
                defaultThrottlesStruct(),
                AbiCodec.encodeUint(trustAnchorOffset),
                AbiCodec.encodeUint(trustAnchorIdOffset));

        final byte[] callData = AbiCodec.concat(selector, head, tailServiceAddr, tailTrustAnchor, tailTrustAnchorId);
        sendTxAndWait(contracts.clprServiceAddress(), callData, null);
    }

    /** The default {@code Throttles} as its 7-word all-static ABI tuple, shared by the config calls. */
    private static byte[] defaultThrottlesStruct() {
        return AbiCodec.concat(
                AbiCodec.encodeUint(100L), // maxMessagesPerBundle (uint32)
                AbiCodec.encodeUint(1024L), // maxMessagePayloadBytes (uint64)
                AbiCodec.encodeUint(1_000_000L), // maxGasPerMessage (uint64)
                AbiCodec.encodeUint(1000L), // maxQueueDepth (uint32)
                AbiCodec.encodeUint(1_048_576L), // maxSyncBytes (uint64)
                AbiCodec.encodeUint(0L), // maxLocalEndpoints (uint32, 0 = no limit)
                AbiCodec.encodeUint(0L)); // maxPeerEndpoints (uint32, 0 = no limit)
    }

    /**
     * One-time bootstrap for a freshly deployed, default-disabled {@code ClprService}: apply the
     * default ledger + economic configuration via {@code initialize(...)} (permitted while the
     * service is still disabled) and then {@code setClprEnabled(true)}.
     *
     * <p>SC-189 deploys the {@code ClprService} <b>default-disabled</b>: {@code _clprEnabled} starts
     * {@code false} and every mutating op — {@code updateLedgerConfiguration},
     * {@code updateEconomicConfiguration}, {@code registerEndpoint}, {@code submitBundle}, … —
     * reverts with {@code ClprDisabled()} until the owner enables it, and {@code setClprEnabled} is
     * itself gated by {@code whenInitialized}. So this replaces the
     * {@code configureDefaultEconomics()} + {@code configureDefaultLedger(...)} pair, which revert on
     * a fresh, still-disabled service.
     *
     * @param serviceAddr this ledger's own service address (contract address bytes)
     */
    public void bootstrapDefaults(final byte[] serviceAddr) {
        initialize(serviceAddr);
        setClprEnabled(true);
    }

    /**
     * Call {@code ClprService.initialize(bytes,(uint32,uint64,uint64,uint32,uint64,uint32,uint32),
     * bytes,bytes,(uint256,uint256,uint256,uint256,uint256,uint256,uint32,uint32,uint64,uint32,uint32))}
     * with the same default throttles / economic config the {@code configureDefault*} helpers apply.
     *
     * <p>{@code initialize} is the one-time bootstrap that applies the first ledger + economic
     * configuration while the service is still disabled; {@code trustAnchor} / {@code trustAnchorId}
     * are sent empty (the pair must be both-empty or both-set). It does not enable the service — call
     * {@link #setClprEnabled(boolean)} afterwards (as {@link #bootstrapDefaults(byte[])} does).
     *
     * @param serviceAddr this ledger's own service address (contract address bytes)
     */
    public void initialize(final byte[] serviceAddr) {
        // Dynamic tails: serviceAddress bytes, then empty trustAnchor / trustAnchorId.
        final byte[] paddedServiceAddr = AbiCodec.padRight32(serviceAddr);
        final byte[] tailServiceAddr = AbiCodec.concat(AbiCodec.encodeUint(serviceAddr.length), paddedServiceAddr);
        final byte[] tailTrustAnchor = AbiCodec.encodeUint(0L);
        final byte[] tailTrustAnchorId = AbiCodec.encodeUint(0L);

        // Head layout (21 slots × 32 bytes = 672 bytes):
        //   [0]     offset to serviceAddress (bytes)
        //   [1..7]  throttles struct (7 static words)
        //   [8]     offset to trustAnchor (bytes)
        //   [9]     offset to trustAnchorId (bytes)
        //   [10..20] economicConfig struct (11 static words)
        final long headSize = 21L * 32L;
        final long serviceAddrOffset = headSize;
        final long trustAnchorOffset = serviceAddrOffset + tailServiceAddr.length;
        final long trustAnchorIdOffset = trustAnchorOffset + tailTrustAnchor.length;

        final byte[] selector = AbiCodec.functionSelector(
                "initialize(bytes,(uint32,uint64,uint64,uint32,uint64,uint32,uint32),bytes,bytes,"
                        + "(uint256,uint256,uint256,uint256,uint256,uint256,uint32,uint32,uint64,uint32,uint32))");

        final byte[] head = AbiCodec.concat(
                AbiCodec.encodeUint(serviceAddrOffset),
                defaultThrottlesStruct(),
                AbiCodec.encodeUint(trustAnchorOffset),
                AbiCodec.encodeUint(trustAnchorIdOffset),
                defaultEconomicConfigStruct());

        final byte[] callData = AbiCodec.concat(selector, head, tailServiceAddr, tailTrustAnchor, tailTrustAnchorId);
        sendTxAndWait(contracts.clprServiceAddress(), callData, null);
    }

    /** Enable or disable all mutating ops on the {@code ClprService} ({@code setClprEnabled(bool)}, owner-only). */
    public void setClprEnabled(final boolean enabled) {
        final byte[] callData = AbiCodec.concat(
                AbiCodec.functionSelector("setClprEnabled(bool)"), AbiCodec.encodeUint(enabled ? 1L : 0L));
        sendTxAndWait(contracts.clprServiceAddress(), callData, null);
    }

    /**
     * ABI-encode a single {@code (string ip, uint32 port, bytes tls, bytes account)}
     * element. TLS is empty; {@code account} is set to the endpoint's {@code ip:port} bytes.
     *
     * <p>The {@code accountId} MUST be non-empty: the contract keys the per-channel peer roster
     * by {@code keccak256(accountId)} and silently skips endpoints with an empty accountId
     * (clpr-smart-contracts {@code ChannelLogic._populatePeerEndpointRoster}). {@code ip:port}
     * makes each endpoint unique and matches the relay's own decode-time fallback key.
     */
    private static byte[] encodeEndpointElement(final EndpointInfo ep) {
        final byte[] ipBytes = ep.ipAddress().getBytes(StandardCharsets.UTF_8);
        final byte[] paddedIp = AbiCodec.padRight32(ipBytes);
        final byte[] accountBytes = (ep.ipAddress() + ":" + ep.port()).getBytes(StandardCharsets.UTF_8);
        final byte[] paddedAccount = AbiCodec.padRight32(accountBytes);

        // Head is 4 slots (128 bytes), relative to element start:
        //   slot 0: offset to ip string
        //   slot 1: port (inline uint32)
        //   slot 2: offset to tls bytes     (tls always empty)
        //   slot 3: offset to account bytes  (ip:port identifier)
        final long ipOffset = 4L * 32L;
        final long tlsOffset = ipOffset + 32L + paddedIp.length;
        final long accountOffset = tlsOffset + 32L; // tls: length word only (empty)

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(AbiCodec.encodeUint(ipOffset));
        out.writeBytes(AbiCodec.encodeUint(ep.port()));
        out.writeBytes(AbiCodec.encodeUint(tlsOffset));
        out.writeBytes(AbiCodec.encodeUint(accountOffset));
        out.writeBytes(AbiCodec.encodeUint(ipBytes.length));
        out.writeBytes(paddedIp);
        out.writeBytes(AbiCodec.encodeUint(0L)); // tls: empty
        out.writeBytes(AbiCodec.encodeUint(accountBytes.length));
        out.writeBytes(paddedAccount);
        return out.toByteArray();
    }

    // -------------------------------------------------------------------------
    // Endpoint registration
    // -------------------------------------------------------------------------

    /**
     * Call {@code ClprService.registerEndpoint((string,uint32,bytes,bytes))} payable with the
     * given bond and a placeholder endpoint tuple (ip=127.0.0.1, port=0). Registration enters
     * PENDING state; this is sufficient for {@code submitBundle} callers — the contract does not
     * require LIVE status for bundle submission.
     *
     * @param bondWei the bond amount in wei (must be >= minEndpointBond from economics config)
     */
    public void registerEndpoint(final BigInteger bondWei) {
        // Encode a placeholder endpoint tuple: (ipAddress="127.0.0.1", port=0, tls=0x, accountId=0x)
        final byte[] endpointEncoding = AbiCodec.concat(
                AbiCodec.encodeUint(32L), // top-level offset to the tuple (after this word)
                encodeEndpointElement(new EndpointInfo("127.0.0.1", 0)));
        final byte[] selector = AbiCodec.functionSelector("registerEndpoint((string,uint32,bytes,bytes))");
        sendTxAndWait(
                contracts.clprServiceAddress(),
                AbiCodec.concat(selector, endpointEncoding),
                "0x" + bondWei.toString(16));
        // Verify registration succeeded (status must be PENDING or LIVE, i.e. non-zero).
        if (!isEndpointRegistered(fromAddress)) {
            throw new IllegalStateException("registerEndpoint succeeded on-chain but getEndpointEntry(" + fromAddress
                    + ") returned NONE on " + contracts.clprServiceAddress());
        }
    }

    /**
     * Call {@code ClprService.getEndpointEntry(address)} via {@code eth_call} and return
     * {@code true} when the account has been registered (status PENDING or LIVE).
     *
     * <p>ABI return type: {@code EndpointManifestEntry { Endpoint endpoint; uint256 bond; EndpointStatus status; }}.
     * The return is a single dynamic struct, so the ABI encoder wraps it in a 1-element output tuple.
     * Full layout (bytes from the start of the return data):
     * <pre>
     *   [0-31]   outer pointer = 32 (ABI 1-tuple wrapper for the dynamic struct)
     *   [32-63]  inner pointer to endpoint data = 96 (3 struct head slots × 32)
     *   [64-95]  bond (uint256)
     *   [96-127] status (uint8 padded to 32); non-zero = PENDING or LIVE
     *   [128+]   Endpoint tuple encoding
     * </pre>
     */
    public boolean isEndpointRegistered(final String address) {
        final byte[] selector = AbiCodec.functionSelector("getEndpointEntry(address)");
        final byte[] callData = AbiCodec.concat(selector, AbiCodec.encodeAddress(address));
        final String dataHex = "0x" + AbiCodec.toHexNoPrefix(callData);
        try {
            final String result = rpc.ethCall(contracts.clprServiceAddress(), dataHex, "latest");
            final byte[] decoded = AbiCodec.fromHex(result);
            // status = decoded[96..127]; last byte (decoded[127]) is non-zero for PENDING or LIVE
            if (decoded.length < 128) return false;
            return decoded[127] != 0;
        } catch (final RuntimeException e) {
            throw new IllegalStateException("getEndpointEntry eth_call failed: " + e.getMessage(), e);
        }
    }

    /**
     * Call {@code ClprService.addEndpoint(address,(string,uint32,bytes,bytes))} as the contract
     * owner to admit {@code registrant} to the live endpoint manifest. If a PENDING entry exists
     * it is promoted to LIVE; otherwise it is added directly with no bond. Either path increments
     * the on-chain {@code endpointManifestVersion}.
     *
     * @param registrant address to admit (must not already be LIVE)
     * @param ep         endpoint tuple (used only when no PENDING entry exists for {@code registrant})
     */
    public void addEndpointToManifest(final String registrant, final EndpointInfo ep) {
        // ABI: addEndpoint(address,(string,uint32,bytes,bytes))
        // Head: 2 slots = 64 bytes
        //   slot 0: registrant address (static)
        //   slot 1: offset to Endpoint tuple = 64 (past the 2-slot head)
        final byte[] selector = AbiCodec.functionSelector("addEndpoint(address,(string,uint32,bytes,bytes))");
        final byte[] callData = AbiCodec.concat(
                selector, AbiCodec.encodeAddress(registrant), AbiCodec.encodeUint(64L), encodeEndpointElement(ep));
        sendTxAndWait(contracts.clprServiceAddress(), callData, null);
    }

    // -------------------------------------------------------------------------
    // Channel registration (commit-reveal)
    // -------------------------------------------------------------------------

    /**
     * Derive the channel id by calling {@code ClprService.deriveChannelId(string,bytes,bytes32)}
     * via {@code eth_call}.
     *
     * @param peerChainId the chain id of the peer (as configured in MockClprVerifier)
     * @param salt32      the 32-byte operator salt
     * @return the 32-byte channel id
     */
    private byte[] deriveChannelId(final String peerChainId, final byte[] salt32) {
        if (salt32.length != 32) {
            throw new IllegalArgumentException("salt32 must be 32 bytes, got " + salt32.length);
        }
        // ABI: deriveChannelId(string peerChainId, bytes pubKey, bytes32 salt)
        // head: [offset to string][offset to bytes][salt32 inline]
        // head is 3 slots = 96 bytes
        // tail: peerChainId encoded as (length + padded), pubKey encoded as (length + padded)
        final byte[] chainIdBytes = peerChainId.getBytes(StandardCharsets.UTF_8);
        final byte[] paddedChainId = AbiCodec.padRight32(chainIdBytes);
        final byte[] paddedPubKey = AbiCodec.padRight32(signerPubKey64);

        final long headSize = 3L * 32L; // 3 head slots
        final long chainIdOffset = headSize; // offset to chainId string
        final long pubKeyOffset = chainIdOffset + 32L + paddedChainId.length; // offset to pubKey bytes

        final byte[] selector = AbiCodec.functionSelector("deriveChannelId(string,bytes,bytes32)");
        final byte[] head = AbiCodec.concat(
                AbiCodec.encodeUint(chainIdOffset), AbiCodec.encodeUint(pubKeyOffset), AbiCodec.encodeBytes32(salt32));
        final byte[] tail = AbiCodec.concat(
                AbiCodec.encodeUint(chainIdBytes.length),
                paddedChainId,
                AbiCodec.encodeUint(signerPubKey64.length),
                paddedPubKey);

        final byte[] callData = AbiCodec.concat(selector, head, tail);
        final String resultHex =
                rpc.ethCall(contracts.clprServiceAddress(), "0x" + AbiCodec.toHexNoPrefix(callData), "latest");
        final byte[] result = AbiCodec.fromHex(resultHex);
        if (result.length < 32) {
            throw new IllegalStateException("deriveChannelId returned " + result.length + " bytes, expected 32");
        }
        // Result is bytes32 — just the 32-byte value
        final byte[] channelId = new byte[32];
        System.arraycopy(result, 0, channelId, 0, 32);
        return channelId;
    }

    /**
     * Register an active channel on {@link DeployedContracts#clprServiceAddress()} using
     * the commit-reveal pattern:
     * <ol>
     *   <li>Derive {@code channelId} via {@code deriveChannelId(peerChainId, signerPubKey64, salt32)}.</li>
     *   <li>Compute {@code commitment = keccak256(channelId || signerPubKey64)}.</li>
     *   <li>Call {@code registerChannel(commitment)}.</li>
     *   <li>Sign {@code keccak256(channelId)} client-side via {@link EthSigner#signMessage},
     *       which applies the EIP-191 {@code "\\x19Ethereum Signed Message:\\n32"} prefix.</li>
     *   <li>Call {@code completeChannel(bytes32,bytes,bytes,bytes32,address,bytes,bytes)} with 7 head slots.</li>
     * </ol>
     *
     * @param peerChainId     the chain id the peer verifier is configured to report
     * @param salt32          the 32-byte operator salt for channel id derivation
     * @param verifierAddress the verifier contract (typically the peer's)
     * @return the channel id (32 bytes)
     */
    public byte[] registerActiveChannel(final String peerChainId, final byte[] salt32, final String verifierAddress) {
        // 1) Derive channelId on-chain
        final byte[] channelId = deriveChannelId(peerChainId, salt32);

        // 2) commitment = keccak256(channelId || signerPubKey64)
        final byte[] commitment = AbiCodec.Keccak256.keccak256(AbiCodec.concat(channelId, signerPubKey64));

        // 3) registerChannel(bytes32 channelId, bytes32 commitment)
        final byte[] registerData = AbiCodec.concat(
                AbiCodec.functionSelector("registerChannel(bytes32,bytes32)"),
                AbiCodec.encodeBytes32(channelId),
                AbiCodec.encodeBytes32(commitment));
        sendTxAndWait(contracts.clprServiceAddress(), registerData, null);

        // 4) Sign keccak256(channelId || address(clprService)) with the EIP-191 prefix
        //    applied client-side; matches what ChannelLogic._verifySignature recovers on chain:
        //    msgHash = keccak256(channelId, address(this))
        final byte[] clprServiceBytes = AbiCodec.fromHex(contracts.clprServiceAddress());
        final byte[] messageHash = AbiCodec.Keccak256.keccak256(AbiCodec.concat(channelId, clprServiceBytes));
        final byte[] signature = signer.signMessage(messageHash);
        if (signature.length != 65) {
            throw new IllegalStateException("expected 65-byte signature, got " + signature.length);
        }

        // 5) completeChannel(bytes32,bytes,bytes,bytes32,address,bytes,bytes)
        //    Head layout (7 slots × 32 bytes = 224 bytes):
        //      [0] channelId (bytes32, inline)
        //      [1] offset to pubKey (bytes, dynamic)
        //      [2] offset to sig (bytes, dynamic)
        //      [3] salt32 (bytes32, inline — static)
        //      [4] verifier (address, inline — static)
        //      [5] offset to configProof (bytes, dynamic)
        //      [6] offset to endpointManifestProof (bytes, dynamic)
        final byte[] configProof = new byte[] {(byte) 0x00, (byte) 0x01};
        // Empty manifest proof → verifier returns version-0 manifest (bring-up mode); the relay
        // populates the manifest via the first proven bundle's Step 1b.
        final byte[] manifestProof = new byte[0];
        final byte[] paddedPubKey = AbiCodec.padRight32(signerPubKey64);
        final byte[] paddedSig = AbiCodec.padRight32(signature);
        final byte[] paddedProof = AbiCodec.padRight32(configProof);

        // Tail contains: pubKey data, sig data, configProof data, manifestProof data
        // Offsets are from start of head (i.e., from byte 0 of the encoded args, after selector)
        final long headSize = 7L * 32L; // 224 bytes
        final long pubKeyOffset = headSize;
        final long sigOffset = pubKeyOffset + 32L + paddedPubKey.length;
        final long proofOffset = sigOffset + 32L + paddedSig.length;
        final long manifestProofOffset = proofOffset + 32L + paddedProof.length;

        final byte[] selector =
                AbiCodec.functionSelector("completeChannel(bytes32,bytes,bytes,bytes32,address,bytes,bytes)");
        final byte[] head = AbiCodec.concat(
                AbiCodec.encodeBytes32(channelId),
                AbiCodec.encodeUint(pubKeyOffset),
                AbiCodec.encodeUint(sigOffset),
                AbiCodec.encodeBytes32(salt32),
                AbiCodec.encodeAddress(verifierAddress),
                AbiCodec.encodeUint(proofOffset),
                AbiCodec.encodeUint(manifestProofOffset));
        final byte[] tail = AbiCodec.concat(
                AbiCodec.encodeUint(signerPubKey64.length),
                paddedPubKey,
                AbiCodec.encodeUint(signature.length),
                paddedSig,
                AbiCodec.encodeUint(configProof.length),
                paddedProof,
                AbiCodec.encodeUint(manifestProof.length));

        final byte[] callData = AbiCodec.concat(selector, head, tail);
        sendTxAndWait(contracts.clprServiceAddress(), callData, null);

        return channelId;
    }

    // -------------------------------------------------------------------------
    // Connector registration (commit-reveal)
    // -------------------------------------------------------------------------

    /**
     * Register a connector on {@link DeployedContracts#clprServiceAddress()} using the
     * commit-reveal pattern:
     * <ol>
     *   <li>Compute {@code connectorId = keccak256(channelId || signerPubKey64 || salt32)} (as bytes).</li>
     *   <li>Compute {@code commitment = keccak256(connectorId || signerPubKey64)}.</li>
     *   <li>Call {@code registerConnector(bytes32 commitment)}.</li>
     *   <li>Sign {@code keccak256(connectorId || address(clprService))} client-side via
     *       {@link EthSigner#signMessage} (EIP-191 prefix applied).</li>
     *   <li>Call {@code completeConnector(bytes32,bytes,bytes,bytes32,bytes32,address,address)} payable.</li>
     * </ol>
     *
     * @param channelId          the 32-byte channel id
     * @param salt32                the 32-byte operator salt
     * @param connectorContractAddress the on-chain connector contract address
     * @param adminAddress          the connector admin address (usually {@code fromAddress})
     * @param stakeWei              the stake to lock (must meet {@code minLockedStake} from economics)
     * @return the connector id (32-byte keccak hash)
     */
    public byte[] registerConnector(
            final byte[] channelId,
            final byte[] salt32,
            final String connectorContractAddress,
            final String adminAddress,
            final BigInteger stakeWei) {
        if (channelId.length != 32) {
            throw new IllegalArgumentException("channelId must be 32 bytes, got " + channelId.length);
        }
        if (salt32.length != 32) {
            throw new IllegalArgumentException("salt32 must be 32 bytes, got " + salt32.length);
        }

        // 1) connectorId = keccak256(channelId || signerPubKey64 || salt32)
        final byte[] connectorIdBytes =
                AbiCodec.Keccak256.keccak256(AbiCodec.concat(channelId, signerPubKey64, salt32));

        // 2) commitment = keccak256(connectorId || signerPubKey64)
        final byte[] commitment = AbiCodec.Keccak256.keccak256(AbiCodec.concat(connectorIdBytes, signerPubKey64));

        // 3) registerConnector(bytes32 commitment)
        final byte[] registerData = AbiCodec.concat(
                AbiCodec.functionSelector("registerConnector(bytes32)"), AbiCodec.encodeBytes32(commitment));
        sendTxAndWait(contracts.clprServiceAddress(), registerData, null);

        // 4) Sign keccak256(connectorId || address(clprService)) with the EIP-191 prefix applied
        //    client-side; matches what ConnectorLib._verifyConnectorSig recovers on chain:
        //    msgHash = keccak256(abi.encodePacked(connectorId, address(this)))
        final byte[] clprServiceBytes = AbiCodec.fromHex(contracts.clprServiceAddress());
        final byte[] messageHash = AbiCodec.Keccak256.keccak256(AbiCodec.concat(connectorIdBytes, clprServiceBytes));
        final byte[] signature = signer.signMessage(messageHash);
        if (signature.length != 65) {
            throw new IllegalStateException("expected 65-byte signature, got " + signature.length);
        }

        // 5) completeConnector(bytes32,bytes,bytes,bytes32,bytes32,address,address) payable
        //    connectorId is now a static bytes32 (clpr-smart-contracts #172 changed it back from
        //    the dynamic bytes that #170 had introduced).
        //    Head layout (7 slots × 32 bytes = 224 bytes):
        //      [0] connectorId (bytes32, inline — static)
        //      [1] offset to pubKey (bytes, dynamic)
        //      [2] offset to sig (bytes, dynamic)
        //      [3] salt32 (bytes32, inline — static)
        //      [4] channelId (bytes32, inline — static)
        //      [5] connectorContract (address, inline — static)
        //      [6] admin (address, inline — static)
        //    Tail: pubKey data, sig data
        final byte[] paddedPubKey = AbiCodec.padRight32(signerPubKey64);
        final byte[] paddedSig = AbiCodec.padRight32(signature);

        final long headSize = 7L * 32L; // 224 bytes
        final long pubKeyOffset = headSize;
        final long sigOffset = pubKeyOffset + 32L + paddedPubKey.length;

        final byte[] selector =
                AbiCodec.functionSelector("completeConnector(bytes32,bytes,bytes,bytes32,bytes32,address,address)");
        final byte[] head = AbiCodec.concat(
                AbiCodec.encodeBytes32(connectorIdBytes),
                AbiCodec.encodeUint(pubKeyOffset),
                AbiCodec.encodeUint(sigOffset),
                AbiCodec.encodeBytes32(salt32),
                AbiCodec.encodeBytes32(channelId),
                AbiCodec.encodeAddress(connectorContractAddress),
                AbiCodec.encodeAddress(adminAddress));
        final byte[] tail = AbiCodec.concat(
                AbiCodec.encodeUint(signerPubKey64.length),
                paddedPubKey,
                AbiCodec.encodeUint(signature.length),
                paddedSig);

        final byte[] callData = AbiCodec.concat(selector, head, tail);
        sendTxAndWait(contracts.clprServiceAddress(), callData, "0x" + stakeWei.toString(16));

        return connectorIdBytes;
    }

    // -------------------------------------------------------------------------
    // Mock verifier / mock app configuration
    // -------------------------------------------------------------------------

    /**
     * Configure the mock verifier's {@code verifyConfig} result and seed the per-Channel peer
     * endpoint roster with the <em>peer</em> relay's reachable endpoint (IP + gRPC port), so the
     * local relay can discover and dial it. The {@code ecdsaSigningKey} field was removed from the
     * {@code Endpoint} struct along with endpoint signatures on {@code ClprSyncPayload}
     * (clpr-smart-contracts #167), so no signing key is carried in the roster.
     */
    public void configureVerifierConfig(
            final String verifierAddress,
            final String chainId,
            final byte[] peerServiceAddr,
            final long configTimestamp,
            final int peerPort) {
        configureVerifierConfig(verifierAddress, chainId, peerServiceAddr, configTimestamp, peerPort, new byte[0]);
    }

    /**
     * Overload of {@link #configureVerifierConfig(String, String, byte[], long, int)} that also
     * publishes the peer relay's {@code tls_certificate} (DER) in the per-Channel peer endpoint
     * roster. A non-empty certificate makes the local relay dial that peer over mutual TLS: it pins the
     * peer's server certificate to these bytes and presents its own client certificate, which the peer
     * validates against <em>its</em> roster. Pass an empty array for the plaintext path.
     *
     * @param peerTlsCert the peer relay's certificate DER to publish on-chain (empty → plaintext)
     */
    public void configureVerifierConfig(
            final String verifierAddress,
            final String chainId,
            final byte[] peerServiceAddr,
            final long configTimestamp,
            final int peerPort,
            final byte[] peerTlsCert) {
        final byte[] chainIdBytes = chainId.getBytes(StandardCharsets.UTF_8);
        final byte[] paddedChainId = AbiCodec.padRight32(chainIdBytes);
        final byte[] paddedServiceAddr = AbiCodec.padRight32(peerServiceAddr);

        final long headSize = 3L * 32L;
        final long chainIdOffset = headSize;
        final long serviceAddrOffset = chainIdOffset + 32L + paddedChainId.length;

        final byte[] selector = AbiCodec.functionSelector("setVerifyConfigResult(string,bytes,uint96)");
        final byte[] head = AbiCodec.concat(
                AbiCodec.encodeUint(chainIdOffset),
                AbiCodec.encodeUint(serviceAddrOffset),
                AbiCodec.encodeUint(configTimestamp));
        final byte[] tail = AbiCodec.concat(
                AbiCodec.encodeUint(chainIdBytes.length),
                paddedChainId,
                AbiCodec.encodeUint(peerServiceAddr.length),
                paddedServiceAddr);

        final byte[] callData = AbiCodec.concat(selector, head, tail);
        sendTxAndWait(verifierAddress, callData, null);

        // Peer-channel binding is now captured at channel setup as conn.channelContext:
        // verifyConfig returns it (derived from the peer service address + channelId) and
        // submitBundle passes it into verifyBundle. The old proven-peer-service-address binding and
        // its BundleLib ClprPeerServiceAddressMismatch cross-check were removed, so no separate
        // setProvenBindings call is needed here.

        // Also configure the throttles that verifyConfig returns — these represent the peer's
        // ledger-level throttles and must be non-zero or sendMessage will revert with
        // ClprPayloadTooLarge. Values mirror configureDefaultLedger so the two stay in sync.
        final byte[] throttleSelector =
                AbiCodec.functionSelector("setPeerThrottles((uint32,uint64,uint64,uint32,uint64,uint32,uint32))");
        final byte[] throttleStruct = AbiCodec.concat(
                AbiCodec.encodeUint(100L), // maxMessagesPerBundle (uint32)
                AbiCodec.encodeUint(1024L), // maxMessagePayloadBytes (uint64)
                AbiCodec.encodeUint(1_000_000L), // maxGasPerMessage (uint64)
                AbiCodec.encodeUint(1000L), // maxQueueDepth (uint32)
                AbiCodec.encodeUint(1_048_576L), // maxSyncBytes (uint64)
                AbiCodec.encodeUint(0L), // maxLocalEndpoints (uint32, 0 = no limit)
                AbiCodec.encodeUint(0L)); // maxPeerEndpoints (uint32, 0 = no limit)
        sendTxAndWait(verifierAddress, AbiCodec.concat(throttleSelector, throttleStruct), null);

        // Populate the per-Channel peer endpoint roster (spec §2.4.2) with the PEER relay's
        // reachable endpoint. This roster is authoritative for on-chain submitBundle sender
        // validation AND for the relay's outbound sync target (the relay reads the roster, not
        // seed_endpoints). The port is the PEER relay's gRPC port so the local relay dials the
        // right place. (The endpoint signing key was removed along with endpoint signatures on
        // ClprSyncPayload — see clpr-smart-contracts #167.)
        // Endpoint tuple: (string ipAddress, uint32 port, bytes tls, bytes accountId). The accountId
        // MUST be non-empty: the contract keys the per-channel roster by keccak256(accountId) and
        // silently skips endpoints with an empty accountId (clpr-smart-contracts
        // ChannelLogic._populatePeerEndpointRoster), which would leave the relay with an empty
        // roster ("no peer available"). Use ip:port so the entry is unique and matches the relay's
        // own decode-time fallback key.
        final byte[] ipBytes = "127.0.0.1".getBytes(StandardCharsets.UTF_8);
        final byte[] paddedIp = AbiCodec.padRight32(ipBytes);
        final byte[] accountBytes = ("127.0.0.1:" + peerPort).getBytes(StandardCharsets.UTF_8);
        final byte[] paddedAccount = AbiCodec.padRight32(accountBytes);
        final byte[] paddedTls = AbiCodec.padRight32(peerTlsCert); // empty array pads to nothing

        // Head is 4 slots (128 bytes), relative to element start:
        //   slot 0: offset to ipAddress string
        //   slot 1: port (uint32, inline)
        //   slot 2: offset to tls bytes
        //   slot 3: offset to accountId bytes
        final long epHeadSize = 4L * 32L; // 128 bytes
        final long epIpOffset = epHeadSize;
        final long epTlsOffset = epIpOffset + 32L + paddedIp.length;
        final long epAccOffset = epTlsOffset + 32L + paddedTls.length;

        final byte[] endpointElement = AbiCodec.concat(
                AbiCodec.encodeUint(epIpOffset),
                AbiCodec.encodeUint(Integer.toUnsignedLong(peerPort)), // port = peer relay's gRPC port
                AbiCodec.encodeUint(epTlsOffset),
                AbiCodec.encodeUint(epAccOffset),
                AbiCodec.encodeUint(ipBytes.length),
                paddedIp,
                AbiCodec.encodeUint(peerTlsCert.length), // tls length (0 → plaintext)
                paddedTls,
                AbiCodec.encodeUint(accountBytes.length),
                paddedAccount);

        // setSeedEndpoints((string,uint32,bytes,bytes)[]) — 1-element array
        // Top-level dynamic arg: args start with offset-to-array (=32), then the array encoding.
        // Array encoding: [length][element_offsets...][element_data...]
        final byte[] seedSelector = AbiCodec.functionSelector("setSeedEndpoints((string,uint32,bytes,bytes)[])");
        final byte[] seedCallData = AbiCodec.concat(
                seedSelector,
                AbiCodec.encodeUint(32L), // top-level offset to array (after this word)
                AbiCodec.encodeUint(1L), // array length = 1
                AbiCodec.encodeUint(32L), // offset to element 0 (relative to start of element offset table)
                endpointElement);
        sendTxAndWait(verifierAddress, seedCallData, null);
    }

    /**
     * Configure the {@code MockClprVerifier} so its {@code verifyConfig} returns a valid, version-1
     * {@code ClprEndpointManifest} with an <em>empty</em> endpoint list. Call this <b>after</b>
     * {@link #configureVerifierConfig} (which seeds one endpoint) and <b>before</b>
     * {@code registerActiveChannel}, so the channel is completed against an empty peer manifest.
     * {@code MockClprVerifier.setSeedEndpoints} replaces the manifest (delete + re-push) and stamps
     * {@code version = 1}, so an empty array yields a valid-but-empty manifest.
     *
     * <p>ABI: {@code setSeedEndpoints((string,uint32,bytes,bytes)[])} with a zero-length array.
     */
    public void setEmptySeedEndpoints(final String verifierAddress) {
        final byte[] selector = AbiCodec.functionSelector("setSeedEndpoints((string,uint32,bytes,bytes)[])");
        final byte[] callData = AbiCodec.concat(
                selector,
                AbiCodec.encodeUint(32L), // top-level offset to array
                AbiCodec.encodeUint(0L)); // array length = 0
        sendTxAndWait(verifierAddress, callData, null);
    }

    /**
     * Configure the {@code MockClprVerifier} to return the given {@code QueueMetadata} from
     * {@code verifyBundle}. This must be called before poking a bundle if the default zero metadata
     * would fail the contract's step-4 replay check:
     * <pre>
     *   expectedCount = metadata.nextMessageId - (conn.receivedMessageId + 1)
     *   if (expectedCount &lt; 0) revert ClprReplayDetected()
     * </pre>
     * For a fresh channel ({@code conn.receivedMessageId = 0}) set {@code nextMessageId = 1} so
     * {@code expectedCount = 0} (no new messages, no replay). Pass {@code status = 1} (ACTIVE) to
     * avoid triggering an unintended status transition.
     *
     * <p>ABI: {@code setVerifyBundleResult((uint64,bytes32,uint64,bytes32,uint8,uint64),bytes[])}.
     * {@code QueueMetadata} is a fully static struct (6 × 32-byte ABI slots). The {@code bytes[]}
     * message-payload list is always empty.
     *
     * @param verifierAddress   the {@code MockClprVerifier} contract address
     * @param nextMessageId     the peer's next-message-id (≥ {@code conn.receivedMessageId + 1})
     * @param receivedMessageId the peer's ack of our outbound (0 = no ack)
     * @param statusOrdinal     {@code ChannelStatus} ordinal (0=PENDING, 1=ACTIVE, 2=CLOSING…)
     */
    public void configureVerifierBundleMetadata(
            final String verifierAddress,
            final long nextMessageId,
            final long receivedMessageId,
            final int statusOrdinal) {
        // QueueMetadata is a static struct encoded as 6 × 32-byte slots in-place.
        // Then the bytes[] offset points to the empty-array tail (6×32 + 32 = 224).
        final byte[] selector = AbiCodec.functionSelector(
                "setVerifyBundleResult((uint64,bytes32,uint64,bytes32,uint8,uint64),bytes[])");
        final byte[] callData = AbiCodec.concat(
                selector,
                AbiCodec.encodeUint(nextMessageId), // nextMessageId (uint64)
                new byte[32], // sentRunningHash (bytes32 = 0)
                AbiCodec.encodeUint(receivedMessageId), // receivedMessageId (uint64)
                new byte[32], // receivedRunningHash (bytes32 = 0)
                AbiCodec.encodeUint(statusOrdinal), // state (uint8 ordinal)
                AbiCodec.encodeUint(0L), // endpointManifestVersion (uint64)
                AbiCodec.encodeUint(224L), // offset to bytes[] = 6×32 + 32 = 224
                AbiCodec.encodeUint(0L)); // bytes[].length = 0 (empty)
        sendTxAndWait(verifierAddress, callData, null);
    }

    /**
     * Configure the {@code MockClprVerifier} to return a new endpoint manifest (with the given version)
     * from {@code verifyBundle}. When the relay submits a bundle that the verifier processes, it returns
     * this manifest. If the returned version exceeds the channel's current {@code endpointManifestVersion},
     * the contract updates the per-channel version and the relay listener refreshes the manifest cache.
     *
     * <p>Uses an empty serviceAddress and an empty endpoints list — sufficient to advance the version counter
     * in an integration-test context where the relay reads the on-ledger manifest separately.
     *
     * @param verifierAddress the {@code MockClprVerifier} contract address
     * @param version         the version to set; must be greater than the channel's current
     *                        {@code endpointManifestVersion} to trigger an update
     */
    public void configureVerifierBundleManifest(final String verifierAddress, final long version) {
        // ABI: setNewEndpointManifest((uint64,bytes,(string,uint32,bytes,bytes)[]))
        // Outer head: 1 slot = offset to the tuple = 32
        // Tuple head (3 slots = 96 bytes, relative to tuple start):
        //   slot 0: version (uint64, static, inline)
        //   slot 1: offset to serviceAddress (dynamic bytes) = 96
        //   slot 2: offset to endpoints (dynamic array) = 96 + 32 (empty serviceAddress takes 1 length slot)
        // Tail: serviceAddress length=0 (empty), endpoints length=0 (empty)
        final byte[] selector =
                AbiCodec.functionSelector("setNewEndpointManifest((uint64,bytes,(string,uint32,bytes,bytes)[]))");
        final byte[] callData = AbiCodec.concat(
                selector,
                AbiCodec.encodeUint(32L), // outer offset to tuple
                AbiCodec.encodeUint(version), // slot 0: version
                AbiCodec.encodeUint(96L), // slot 1: offset to serviceAddress
                AbiCodec.encodeUint(128L), // slot 2: offset to endpoints (96+32=128)
                AbiCodec.encodeUint(0L), // serviceAddress: length=0
                AbiCodec.encodeUint(0L)); // endpoints: length=0
        sendTxAndWait(verifierAddress, callData, null);
    }

    /**
     * Like {@link #configureVerifierBundleManifest(String, long)} but includes one peer endpoint in
     * the manifest. Required when the relay's listener must see a non-empty manifest after the
     * version advance — the listener skips the {@code manifest.refreshed} metric increment when the
     * manifest is empty.
     *
     * <p>ABI: {@code setNewEndpointManifest((uint64,bytes,(string,uint32,bytes,bytes)[]))}.
     * The struct is dynamic (contains {@code bytes} and a dynamic array), so an outer offset word
     * precedes the tuple. The endpoint element is encoded the same way as in
     * {@link #configureVerifierConfig}.
     *
     * @param verifierAddress the {@code MockClprVerifier} contract address
     * @param version         the version to set (> current {@code endpointManifestVersion})
     * @param peerIp          the peer's reachable IP address (e.g. {@code "127.0.0.1"})
     * @param peerPort        the peer's gRPC port
     */
    public void configureVerifierBundleManifest(
            final String verifierAddress, final long version, final String peerIp, final int peerPort) {
        // Encode the single endpoint element (string,uint32,bytes,bytes).
        final byte[] endpointElement = encodeEndpointElement(new EndpointInfo(peerIp, peerPort));

        // Tuple layout (all offsets relative to tuple start):
        //   slot 0:  version (uint64, static)
        //   slot 1:  offset to serviceAddress = 96 (3 head slots × 32)
        //   slot 2:  offset to endpoints = 128 (96 + 32 for empty serviceAddress)
        //   [96]    serviceAddress: length = 0
        //   [128]   endpoints[]: count = 1
        //   [160]   endpoints[0] offset (relative to offsets start) = 32
        //   [192..] endpoint element data
        final byte[] selector =
                AbiCodec.functionSelector("setNewEndpointManifest((uint64,bytes,(string,uint32,bytes,bytes)[]))");
        final byte[] callData = AbiCodec.concat(
                selector,
                AbiCodec.encodeUint(32L), // outer offset to the tuple
                AbiCodec.encodeUint(version), // slot 0: version
                AbiCodec.encodeUint(96L), // slot 1: serviceAddress offset within tuple
                AbiCodec.encodeUint(128L), // slot 2: endpoints offset within tuple
                AbiCodec.encodeUint(0L), // serviceAddress: length = 0
                AbiCodec.encodeUint(1L), // endpoints[]: count = 1
                AbiCodec.encodeUint(32L), // offset to element 0 (relative to element offsets start)
                endpointElement);
        sendTxAndWait(verifierAddress, callData, null);
    }

    /**
     * Configure the response returned by {@code MockClprApplication.setResponse(bytes)}.
     *
     * @param appAddress   the mock application address
     * @param responseData the response data to return on the next call
     */
    public void configureAppResponse(final String appAddress, final byte[] responseData) {
        final byte[] selector = AbiCodec.functionSelector("setResponse(bytes)");
        final byte[] head = AbiCodec.encodeUint(32L); // offset to bytes tail = 32 (only one arg)
        final byte[] tail =
                AbiCodec.concat(AbiCodec.encodeUint(responseData.length), AbiCodec.padRight32(responseData));
        final byte[] callData = AbiCodec.concat(selector, head, tail);
        sendTxAndWait(appAddress, callData, null);
    }

    // -------------------------------------------------------------------------
    // Send / read
    // -------------------------------------------------------------------------

    /**
     * Call {@code ClprService.sendMessage(bytes32,bytes32,bytes,bytes)}.
     *
     * @param channelId      the 32-byte channel id
     * @param sourceConnectorId the source connector id (32-byte keccak hash, ABI-encoded as {@code bytes32})
     * @param targetApp         the target application address (bytes)
     * @param messageData       the message payload
     * @return the assigned message id (inferred post-send from on-chain state)
     */
    public long sendMessage(
            final byte[] channelId, final byte[] sourceConnectorId, final byte[] targetApp, final byte[] messageData) {
        // Head (4 slots):
        //   [0] channelId (bytes32, inline)
        //   [1] sourceConnectorId (bytes32, inline — clpr-smart-contracts #172 changed this back
        //       from the dynamic bytes that #170 had introduced)
        //   [2] offset to targetApp (bytes, dynamic)
        //   [3] offset to messageData (bytes, dynamic)
        // sender is derived on-chain as msg.sender (spec §4.3 step 6)
        final byte[] paddedTarget = AbiCodec.padRight32(targetApp);
        final byte[] paddedData = AbiCodec.padRight32(messageData);

        final long headSize = 4L * 32L;
        final long offTarget = headSize;
        final long offData = offTarget + 32L + paddedTarget.length;

        final byte[] selector = AbiCodec.functionSelector("sendMessage(bytes32,bytes32,bytes,bytes)");
        final byte[] head = AbiCodec.concat(
                AbiCodec.encodeBytes32(channelId),
                AbiCodec.encodeBytes32(sourceConnectorId),
                AbiCodec.encodeUint(offTarget),
                AbiCodec.encodeUint(offData));
        final byte[] tail = AbiCodec.concat(
                AbiCodec.encodeUint(targetApp.length),
                paddedTarget,
                AbiCodec.encodeUint(messageData.length),
                paddedData);

        final byte[] callData = AbiCodec.concat(selector, head, tail);
        sendTxAndWait(contracts.clprServiceAddress(), callData, null);

        // Infer assigned message id from the post-send channel state: nextMessageId-1 is
        // the id that was just assigned. A successful sendMessage requires a live Channel
        // record, so the Optional must be present.
        final ChannelState state = readChannelState(channelId)
                .orElseThrow(() -> new IllegalStateException(
                        "sendMessage succeeded but getChannel returned no record for " + AbiCodec.toHex(channelId)));
        return Math.max(1L, state.nextMessageId() - 1L);
    }

    /**
     * Read the channel state, requiring the on-chain record to exist. Use after an operation
     * whose success guarantees a live {@code Channel} (e.g. a confirmed {@code sendMessage} or
     * {@code registerChannel}); the must-exist contract is expressed once here instead of an
     * ad-hoc {@code .orElseThrow(...)} at every call site.
     *
     * @param channelId the 32-byte channel id
     * @return the decoded channel state
     * @throws IllegalStateException if {@code getChannel} reports no record
     */
    public ChannelState requireChannelState(final byte[] channelId) {
        return readChannelState(channelId)
                .orElseThrow(() -> new IllegalStateException("expected a Channel record for "
                        + AbiCodec.toHex(channelId) + " but getChannel returned none"));
    }

    /** A projection of the key fields returned by {@code ClprService.getChannel}. */
    public record ChannelState(
            int status,
            long nextMessageId,
            long ackedMessageId,
            long receivedMessageId,
            long nextExpectedReplyId,
            byte[] sentRunningHash,
            byte[] receivedRunningHash) {}

    /**
     * ABI selector for the Solidity error {@code ClprChannelNotFound()} =
     * {@code keccak256("ClprChannelNotFound()")[:4]}. Mirrors the production-side constant in
     * {@code EvmContractStateReader}; lets {@link #readChannelState} report "no record" without
     * crafting a try/catch around every probe.
     */
    private static final String CLPR_CHANNEL_NOT_FOUND_SELECTOR = "0x029a1002";

    /**
     * Read the key fields from {@code ClprService.getChannel(bytes32)} via {@code eth_call}.
     *
     * <p>Returns {@link java.util.Optional#empty()} when the chain reverts with
     * {@code ClprChannelNotFound} (the id is a PENDING commit-only entry, or was never
     * registered — indistinguishable on chain). Any other RPC failure is rethrown.
     *
     * <p>The returned {@code Channel} struct head layout (fields in ABI slot order):
     * <ol start="0">
     *   <li>channelId (bytes32)</li>
     *   <li>verifier (address)</li>
     *   <li>status (uint8)</li>
     *   <li>nextMessageId (uint64)</li>
     *   <li>ackedMessageId (uint64)</li>
     *   <li>receivedMessageId (uint64)</li>
     *   <li>nextExpectedReplyId (uint64)</li>
     *   <li>peerConfigTimestamp (uint96)</li>
     *   <li>lastConfigTimestamp (uint96)</li>
     *   <li>sentRunningHash (bytes32)</li>
     *   <li>receivedRunningHash (bytes32)</li>
     *   <li>... (ownershipCommitment, salt, chainId, peerServiceAddress, peerThrottles×7, trustAnchor, currentSecondStart, bundlesThisSecond, lastDataMessageId, trustAnchorId, channelContext, endpointManifestVersion)</li>
     * </ol>
     *
     * @param channelId the 32-byte channel id
     * @return the channel state projection, or empty if no record exists
     */
    public java.util.Optional<ChannelState> readChannelState(final byte[] channelId) {
        final byte[] callData = AbiCodec.encodeGetChannel(channelId);
        final String resultHex;
        try {
            resultHex = rpc.ethCall(contracts.clprServiceAddress(), "0x" + AbiCodec.toHexNoPrefix(callData), "latest");
        } catch (final org.hiero.clpr.relay.evm.JsonRpcException e) {
            final String message = e.getMessage();
            if (message != null && message.contains(CLPR_CHANNEL_NOT_FOUND_SELECTOR)) {
                return java.util.Optional.empty();
            }
            throw e;
        }
        final byte[] data = AbiCodec.fromHex(resultHex);
        // Minimum: 1 outer-offset word + 27 head slots = 28 × 32 = 896 bytes (matches
        // EvmContractStateReader.readChannelState after the throttle- and rate-limiting-field
        // removal).
        if (data.length < 896) {
            return java.util.Optional.empty();
        }

        // Return is a dynamic struct wrapped with a 32-byte offset pointer.
        final int base = (int) AbiCodec.decodeUint64(data, 0);
        final int status = (int) AbiCodec.decodeUint64(data, base + 2 * 32);
        final long nextMessageId = AbiCodec.decodeUint64(data, base + 3 * 32);
        final long ackedMessageId = AbiCodec.decodeUint64(data, base + 4 * 32);
        final long receivedMessageId = AbiCodec.decodeUint64(data, base + 5 * 32);
        final long nextExpectedReplyId = AbiCodec.decodeUint64(data, base + 6 * 32);
        final byte[] sentRunningHash = AbiCodec.decodeBytes32(data, base + 9 * 32);
        final byte[] receivedRunningHash = AbiCodec.decodeBytes32(data, base + 10 * 32);
        return java.util.Optional.of(new ChannelState(
                status,
                nextMessageId,
                ackedMessageId,
                receivedMessageId,
                nextExpectedReplyId,
                sentRunningHash,
                receivedRunningHash));
    }

    /**
     * Encode a DATA message payload the same way the on-chain {@code ClprProtobuf.encodeDataMessage}
     * would, so that an off-chain test helper can pre-configure a mock verifier with bytes that
     * decode correctly on chain.
     *
     * @param connectorSource     the source connector id bytes (field 1)
     * @param targetApplication   the destination application address bytes (field 2)
     * @param sender              the source sender address bytes (field 3)
     * @param messageData         the opaque payload bytes (field 4)
     * @return the protobuf-encoded {@code ClprMessagePayload} bytes
     */
    public static byte[] encodeDataMessagePayload(
            final byte[] connectorSource,
            final byte[] targetApplication,
            final byte[] sender,
            final byte[] messageData) {
        final ClprMessage inner = ClprMessage.newBuilder()
                .connectorId(Bytes.wrap(connectorSource))
                .targetApplication(Bytes.wrap(targetApplication))
                .sender(Bytes.wrap(sender))
                .messageData(Bytes.wrap(messageData))
                .build();
        // The PBJ generator exposes a Builder `message(ClprMessage)` setter that assigns the
        // OneOf variant to MESSAGE (field 1), matching the Solidity protobuf decoder.
        final ClprMessagePayload payload =
                ClprMessagePayload.newBuilder().message(inner).build();
        return ClprMessagePayload.PROTOBUF.toBytes(payload).toByteArray();
    }

    /**
     * Encode a REPLY message payload the same way the on-chain {@code ClprProtobuf.encodeReplyMessage}
     * would, so that an off-chain test helper can pre-configure a mock verifier with bytes that
     * decode correctly on chain.
     *
     * <p>Wire format: {@code ClprMessagePayload { ClprMessageReply message_reply = 2; }} where
     * {@code ClprMessageReply} carries {@code messageId} (field 1), {@code status} (field 2),
     * and {@code messageReplyData} (field 3).
     *
     * @param messageId  the originating DATA message id being replied to
     * @param status     the reply status (0 = SUCCESS)
     * @param replyData  the opaque application response bytes
     * @return the protobuf-encoded {@code ClprMessagePayload} bytes
     */
    public static byte[] encodeReplyMessagePayload(final long messageId, final int status, final byte[] replyData) {
        final ClprMessageReplyStatus replyStatus = ClprMessageReplyStatus.fromProtobufOrdinal(status);
        final ClprMessageReply inner = ClprMessageReply.newBuilder()
                .messageId(messageId)
                .status(replyStatus)
                .messageReplyData(Bytes.wrap(replyData))
                .build();
        final ClprMessagePayload payload =
                ClprMessagePayload.newBuilder().messageReply(inner).build();
        return ClprMessagePayload.PROTOBUF.toBytes(payload).toByteArray();
    }

    /**
     * Compute {@code sha256(prev || sha256(payload))} — the running-hash step used by
     * {@code BundleProcessor} to verify inbound bundles. The payload is hashed before being folded
     * into the chain, matching the on-chain queue running-hash computation.
     *
     * @param prev     previous 32-byte running hash
     * @param payload  the protobuf-encoded payload bytes
     * @return the 32-byte updated running hash
     */
    public static byte[] runningHashStep(final byte[] prev, final byte[] payload) {
        try {
            final byte[] payloadHash = MessageDigest.getInstance("SHA-256").digest(payload);
            final MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(prev);
            md.update(payloadHash);
            return md.digest();
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    // -------------------------------------------------------------------------
    // Admin operations + direct bundle submission (test-only)
    // -------------------------------------------------------------------------

    /**
     * Call {@code ClprService.closeChannel(bytes32)} as the contract owner. For an existing
     * ACTIVE/PAUSED channel this transitions it to CLOSING; for a pending (commit-only)
     * channel it deletes the commitment.
     *
     * @param channelId the 32-byte channel id
     */
    public void closeChannel(final byte[] channelId) {
        if (channelId.length != 32) {
            throw new IllegalArgumentException("channelId must be 32 bytes, got " + channelId.length);
        }
        final byte[] selector = AbiCodec.functionSelector("closeChannel(bytes32)");
        final byte[] callData = AbiCodec.concat(selector, AbiCodec.encodeBytes32(channelId));
        sendTxAndWait(contracts.clprServiceAddress(), callData, null);
    }

    /**
     * Call {@code registerChannel(bytes32,bytes32)} only — do <strong>not</strong> follow up
     * with {@code completeChannel}. Leaves the id in the PENDING (commit-only) phase per
     * {@code clpr-service.md §3.1.3}.
     *
     * @param peerChainId the chain id the peer verifier is configured to report
     * @param salt32      the 32-byte operator salt for channel id derivation
     * @return the derived 32-byte channel id
     */
    public byte[] registerChannelCommitOnly(final String peerChainId, final byte[] salt32) {
        if (salt32.length != 32) {
            throw new IllegalArgumentException("salt32 must be 32 bytes, got " + salt32.length);
        }
        final byte[] channelId = deriveChannelId(peerChainId, salt32);
        final byte[] commitment = AbiCodec.Keccak256.keccak256(AbiCodec.concat(channelId, signerPubKey64));
        final byte[] callData = AbiCodec.concat(
                AbiCodec.functionSelector("registerChannel(bytes32,bytes32)"),
                AbiCodec.encodeBytes32(channelId),
                AbiCodec.encodeBytes32(commitment));
        sendTxAndWait(contracts.clprServiceAddress(), callData, null);
        return channelId;
    }

    /**
     * Call {@code ClprService.submitBundle(bytes32,bytes)} directly as the registered dev
     * endpoint, bypassing the relay sync flow. Used to deterministically apply a crafted bundle.
     *
     * @param channelId the 32-byte channel id
     * @param proofBytes   the bundle proof bytes, interpreted by the channel's on-chain verifier
     */
    public void submitBundle(final byte[] channelId, final byte[] proofBytes) {
        if (channelId.length != 32) {
            throw new IllegalArgumentException("channelId must be 32 bytes, got " + channelId.length);
        }
        final byte[] callData = AbiCodec.encodeSubmitBundle(channelId, proofBytes);
        sendTxAndWait(contracts.clprServiceAddress(), callData, null);
    }

    /**
     * Build a {@code CLPRSTUB}-prefixed bundle carrying a single DATA message that, applied
     * against a channel with the supplied local queue state, satisfies §4.2 replay and
     * running-hash checks and would advance the channel's {@code receivedMessageId} by 1.
     * Returns the raw payload bytes; the caller submits via {@link #submitBundle(byte[], byte[])}.
     *
     * <p>The local state is passed explicitly (not read from chain) so a bundle crafted to be
     * valid against channel A's state can then be submitted under channel B's id — the
     * mismatch is what {@code SubmitBundleChannelMismatchTest} asserts the contract must catch.
     *
     * @param localState        the destination channel's current queue state as it should be
     *                          reflected in the bundle metadata (used to satisfy §4.2 checks)
     * @param connectorSource   DATA-message source connector id
     * @param targetApplication DATA-message destination application address
     * @param sender            DATA-message source sender address
     * @param messageData       DATA-message opaque payload
     */
    public byte[] craftDataBundleBytes(
            final ChannelState localState,
            final byte[] connectorSource,
            final byte[] targetApplication,
            final byte[] sender,
            final byte[] messageData) {
        final byte[] payload = encodeDataMessagePayload(connectorSource, targetApplication, sender, messageData);
        final byte[] newRunningHash = runningHashStep(localState.receivedRunningHash(), payload);

        final ClprQueueMetadata metadata = ClprQueueMetadata.newBuilder()
                .nextMessageId(localState.receivedMessageId() + 2L) // one new inbound message above receivedMessageId
                .sentRunningHash(Bytes.wrap(newRunningHash))
                .receivedMessageId(localState.ackedMessageId()) // no new acks
                .receivedRunningHash(Bytes.wrap(localState.sentRunningHash()))
                .status(ClprChannelStatus.ACTIVE)
                .build();

        final ClprMessagePayload msg;
        try {
            msg = ClprMessagePayload.PROTOBUF.parse(Bytes.wrap(payload));
        } catch (final com.hedera.pbj.runtime.ParseException e) {
            throw new IllegalStateException("Failed to round-trip ClprMessagePayload", e);
        }
        final ClprBundleContent content = ClprBundleContent.newBuilder()
                .metadata(metadata)
                .messages(List.of(msg))
                .build();
        final byte[] contentBytes = ClprBundleContent.PROTOBUF.toBytes(content).toByteArray();
        final byte[] stubPrefix = "CLPRSTUB".getBytes(StandardCharsets.US_ASCII);
        return AbiCodec.concat(stubPrefix, contentBytes);
    }

    /**
     * Build a {@code CLPRSTUB}-prefixed bundle whose single message is a REPLY with an
     * out-of-order {@code message_id}, and submit it via {@link #submitBundle}. Synthesises the
     * metadata to satisfy replay defense, running-hash continuity, and ack monotonicity so
     * processing reaches the §4.5 response-ordering pre-scan and trips PAUSED on an ACTIVE/PAUSED
     * channel. The channel must be wired to the {@code StubClprVerifier}.
     *
     * @param channelId      the 32-byte channel id (state read from this interactor's chain)
     * @param mismatchedReplyId the {@code message_id} for the crafted REPLY; choose
     *                          {@code >= readChannelState(channelId).nextMessageId()}
     */
    public void submitBundleWithMismatchedReply(final byte[] channelId, final long mismatchedReplyId) {
        final ChannelState state = readChannelState(channelId)
                .orElseThrow(() -> new IllegalStateException(
                        "submitBundleWithMismatchedReply requires an existing Channel record for "
                                + AbiCodec.toHex(channelId)));
        final byte[] payload = encodeReplyMessagePayload(mismatchedReplyId, 0, new byte[0]);
        final byte[] newSentHash = runningHashStep(state.receivedRunningHash(), payload);

        final ClprQueueMetadata metadata = ClprQueueMetadata.newBuilder()
                .nextMessageId(state.receivedMessageId() + 2L) // one new outbound message above receivedMessageId
                .sentRunningHash(Bytes.wrap(newSentHash))
                .receivedMessageId(state.ackedMessageId()) // no new acks
                .receivedRunningHash(Bytes.wrap(state.sentRunningHash()))
                .status(ClprChannelStatus.ACTIVE)
                .build();

        final ClprMessagePayload msg;
        try {
            msg = ClprMessagePayload.PROTOBUF.parse(Bytes.wrap(payload));
        } catch (final com.hedera.pbj.runtime.ParseException e) {
            throw new IllegalStateException("Failed to round-trip ClprMessagePayload", e);
        }
        final ClprBundleContent content = ClprBundleContent.newBuilder()
                .metadata(metadata)
                .messages(List.of(msg))
                .build();
        final byte[] contentBytes = ClprBundleContent.PROTOBUF.toBytes(content).toByteArray();
        final byte[] stubPrefix = "CLPRSTUB".getBytes(StandardCharsets.US_ASCII);
        submitBundle(channelId, AbiCodec.concat(stubPrefix, contentBytes));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void sendTxAndWait(final String to, final byte[] callData, @Nullable final String valueHex) {
        final String dataHex = "0x" + AbiCodec.toHexNoPrefix(callData);
        final long valueWei = (valueHex == null) ? 0L : Long.parseUnsignedLong(stripHexPrefix(valueHex), 16);
        final String txHash = txSubmitter.sendRawTx(to, callData, valueWei, DEFAULT_GAS_LIMIT);
        waitForSuccessfulReceipt(txHash, to, dataHex);
    }

    private static String stripHexPrefix(final String hex) {
        return hex.startsWith("0x") || hex.startsWith("0X") ? hex.substring(2) : hex;
    }

    private void waitForSuccessfulReceipt(final String txHash, final String to, final String dataHex) {
        for (int i = 0; i < RECEIPT_POLL_ATTEMPTS; i++) {
            final JsonNode receipt = rpc.ethGetTransactionReceipt(txHash);
            if (receipt != null && !receipt.isNull()) {
                final JsonNode statusNode = receipt.get("status");
                final String status = statusNode != null ? statusNode.asText() : "0x0";
                if ("0x1".equals(status)) {
                    return;
                }
                // Replay via eth_call to surface the revert reason.
                String revertReason = "<unknown>";
                try {
                    rpc.ethCallFrom(fromAddress, to, dataHex, "latest");
                } catch (final RuntimeException ex) {
                    revertReason = ex.getMessage();
                }
                throw new IllegalStateException(
                        "Transaction " + txHash + " failed, status=" + status + ", revert: " + revertReason);
            }
            try {
                Thread.sleep(RECEIPT_POLL_INTERVAL_MS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for receipt of " + txHash);
            }
        }
        throw new IllegalStateException("Transaction receipt not found for " + txHash);
    }
}
