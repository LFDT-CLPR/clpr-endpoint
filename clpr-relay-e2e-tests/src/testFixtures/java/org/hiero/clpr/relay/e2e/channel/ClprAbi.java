// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.channel;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.hiero.clpr.relay.evm.AbiCodec;

/**
 * ABI encoders for the CLPR calls the channel wiring makes.
 *
 * <p>Hand-encoded, following the repo's convention of routing everything through {@code AbiCodec}
 * rather than pulling in a contract-binding library. The calls here are the awkward ones — four of
 * {@code completeChannel}'s seven parameters are dynamic, and {@code setSeedEndpoints} takes a dynamic
 * array of dynamic structs — so each layout is spelled out at its call site.
 */
public final class ClprAbi {

    private ClprAbi() {}

    /**
     * A CLPR endpoint tuple: {@code (string ipAddress, uint32 port, bytes tlsCertificate, bytes accountId)}.
     *
     * @param ipAddress must be a numeric IPv4 literal — {@code EndpointValidation} rejects hostnames
     *     with {@code ClprInvalidEndpointAddress}
     * @param port the endpoint's gRPC sync port
     * @param tlsCertificate always empty
     * @param accountId keys the per-channel peer roster; the contract silently skips an endpoint whose
     *     accountId is empty, so this must be set to something unique
     */
    public record Endpoint(String ipAddress, int port, byte[] tlsCertificate, byte[] accountId) {

        /** An endpoint keyed by {@code ip:port}, matching the relay's own decode-time fallback key. */
        public static Endpoint of(final String ipAddress, final int port) {
            return new Endpoint(
                    ipAddress, port, new byte[0], (ipAddress + ":" + port).getBytes(StandardCharsets.UTF_8));
        }
    }

    /** {@code registerChannel(bytes32 channelId, bytes32 commitment)} */
    public static byte[] registerChannel(final byte[] channelId, final byte[] commitment) {
        return AbiCodec.concat(
                AbiCodec.functionSelector("registerChannel(bytes32,bytes32)"),
                AbiCodec.encodeBytes32(channelId),
                AbiCodec.encodeBytes32(commitment));
    }

    /**
     * {@code completeChannel(bytes32 channelId, bytes pubKey, bytes sig, bytes32 salt, address verifier,
     * bytes configProof, bytes endpointManifestProof)}
     *
     * <p>Head is seven words: channelId, offset(pubKey), offset(sig), salt, verifier, offset(configProof),
     * offset(manifestProof). Tails follow in that order.
     */
    public static byte[] completeChannel(
            final byte[] channelId,
            final byte[] pubKey64,
            final byte[] signature,
            final byte[] salt32,
            final String verifierAddress,
            final byte[] configProof,
            final byte[] manifestProof) {
        final byte[] tailPubKey = AbiCodec.encodeBytes(pubKey64);
        final byte[] tailSig = AbiCodec.encodeBytes(signature);
        final byte[] tailConfig = AbiCodec.encodeBytes(configProof);
        final byte[] tailManifest = AbiCodec.encodeBytes(manifestProof);

        final long head = 7L * 32L;
        final long offPubKey = head;
        final long offSig = offPubKey + tailPubKey.length;
        final long offConfig = offSig + tailSig.length;
        final long offManifest = offConfig + tailConfig.length;

        return AbiCodec.concat(
                AbiCodec.functionSelector("completeChannel(bytes32,bytes,bytes,bytes32,address,bytes,bytes)"),
                AbiCodec.encodeBytes32(channelId),
                AbiCodec.encodeUint(offPubKey),
                AbiCodec.encodeUint(offSig),
                AbiCodec.encodeBytes32(salt32),
                AbiCodec.encodeAddress(verifierAddress),
                AbiCodec.encodeUint(offConfig),
                AbiCodec.encodeUint(offManifest),
                tailPubKey,
                tailSig,
                tailConfig,
                tailManifest);
    }

    /** {@code registerConnector(bytes32 commitment)} */
    public static byte[] registerConnector(final byte[] commitment) {
        return AbiCodec.concat(
                AbiCodec.functionSelector("registerConnector(bytes32)"), AbiCodec.encodeBytes32(commitment));
    }

    /**
     * {@code completeConnector(bytes32 connectorId, bytes pubKey, bytes sig, bytes32 salt,
     * bytes32 channelId, address connectorContract, address admin)} — payable, the stake is the value.
     */
    public static byte[] completeConnector(
            final byte[] connectorId,
            final byte[] pubKey64,
            final byte[] signature,
            final byte[] salt32,
            final byte[] channelId,
            final String connectorContract,
            final String admin) {
        final byte[] tailPubKey = AbiCodec.encodeBytes(pubKey64);
        final byte[] tailSig = AbiCodec.encodeBytes(signature);

        final long head = 7L * 32L;
        final long offPubKey = head;
        final long offSig = offPubKey + tailPubKey.length;

        return AbiCodec.concat(
                AbiCodec.functionSelector("completeConnector(bytes32,bytes,bytes,bytes32,bytes32,address,address)"),
                AbiCodec.encodeBytes32(connectorId),
                AbiCodec.encodeUint(offPubKey),
                AbiCodec.encodeUint(offSig),
                AbiCodec.encodeBytes32(salt32),
                AbiCodec.encodeBytes32(channelId),
                AbiCodec.encodeAddress(connectorContract),
                AbiCodec.encodeAddress(admin),
                tailPubKey,
                tailSig);
    }

    /**
     * {@code setSeedEndpoints(Endpoint[] seedEndpoints)} on the {@code QbftE2EVerifier} wrapper.
     *
     * <p>A dynamic array of dynamic structs, which is the fiddliest shape in the ABI. Layout:
     *
     * <pre>
     *   selector
     *   [0] offset to the array          (= 32, the array is the only parameter)
     *   array: count
     *          offset(struct_0) ... offset(struct_{n-1})   relative to the first offset word
     *          struct_0 ... struct_{n-1}
     * </pre>
     *
     * Each struct in turn is {@code offset(ip), port, offset(tls), offset(accountId)} followed by its
     * three tails, with offsets relative to the struct's own start.
     */
    public static byte[] setSeedEndpoints(final List<Endpoint> endpoints) {
        final ByteArrayOutputStream offsets = new ByteArrayOutputStream();
        final ByteArrayOutputStream bodies = new ByteArrayOutputStream();

        // Offsets are measured from the start of the offsets block, so the first struct sits right
        // after all n offset words.
        long running = endpoints.size() * 32L;
        for (final Endpoint endpoint : endpoints) {
            final byte[] encoded = encodeEndpointStruct(endpoint);
            offsets.writeBytes(AbiCodec.encodeUint(running));
            bodies.writeBytes(encoded);
            running += encoded.length;
        }

        return AbiCodec.concat(
                AbiCodec.functionSelector("setSeedEndpoints((string,uint32,bytes,bytes)[])"),
                AbiCodec.encodeUint(32L),
                AbiCodec.encodeUint(endpoints.size()),
                offsets.toByteArray(),
                bodies.toByteArray());
    }

    private static byte[] encodeEndpointStruct(final Endpoint endpoint) {
        final byte[] tailIp = AbiCodec.encodeBytes(endpoint.ipAddress().getBytes(StandardCharsets.UTF_8));
        final byte[] tailTls = AbiCodec.encodeBytes(endpoint.tlsCertificate());
        final byte[] tailAccount = AbiCodec.encodeBytes(endpoint.accountId());

        final long head = 4L * 32L;
        return AbiCodec.concat(
                AbiCodec.encodeUint(head), // offset to ipAddress
                AbiCodec.encodeUint(endpoint.port()), // port, inline
                AbiCodec.encodeUint(head + tailIp.length), // offset to tlsCertificate
                AbiCodec.encodeUint(head + tailIp.length + tailTls.length), // offset to accountId
                tailIp,
                tailTls,
                tailAccount);
    }

    /**
     * {@code sendMessage(bytes32 channelId, bytes32 sourceConnectorId, bytes targetApplication,
     * bytes messageData)}
     *
     * <p>Head is four words: channelId, connectorId, offset(targetApplication), offset(messageData).
     * The sender is not a parameter — the service records {@code msg.sender}.
     */
    public static byte[] sendMessage(
            final byte[] channelId,
            final byte[] sourceConnectorId,
            final byte[] targetApplication,
            final byte[] messageData) {
        final byte[] tailTarget = AbiCodec.encodeBytes(targetApplication);
        final byte[] tailData = AbiCodec.encodeBytes(messageData);
        final long head = 4L * 32L;
        return AbiCodec.concat(
                AbiCodec.functionSelector("sendMessage(bytes32,bytes32,bytes,bytes)"),
                AbiCodec.encodeBytes32(channelId),
                AbiCodec.encodeBytes32(sourceConnectorId),
                AbiCodec.encodeUint(head),
                AbiCodec.encodeUint(head + tailTarget.length),
                tailTarget,
                tailData);
    }

    /**
     * {@code topic0} of {@code MessageQueued(bytes32 indexed channelId, uint64 messageId, uint8 messageType)}.
     *
     * <p>The event is the only reliable source of a sent message's id under load. Reading
     * {@code nextMessageId - 1} back from state after the send is a race the moment two sends are in
     * flight, and silently attributes another sender's id to this one.
     */
    public static byte[] messageQueuedTopic() {
        return AbiCodec.Keccak256.keccak256("MessageQueued(bytes32,uint64,uint8)".getBytes(StandardCharsets.UTF_8));
    }

    /** {@code getPeerEndpointManifest(bytes32 channelId)} — used to assert the config path ran. */
    public static byte[] getPeerEndpointManifest(final byte[] channelId) {
        return AbiCodec.concat(
                AbiCodec.functionSelector("getPeerEndpointManifest(bytes32)"), AbiCodec.encodeBytes32(channelId));
    }
}
