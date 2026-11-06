// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.chain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.hiero.clpr.relay.evm.AbiCodec;

/**
 * Builds a Besu QBFT genesis document plus the derived per-node material (validator keys, enode
 * URLs) for one {@link BesuNetworkSpec}.
 *
 * <p>Two details there are load-bearing and easy to
 * get subtly wrong, so they are called out at their implementation sites below: the exact shape of
 * the QBFT {@code extraData} RLP, and the fork schedule.
 */
public final class BesuQbftGenesis {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Balance every prefunded account starts with: 8 ETH.
     *
     * <p>Chosen to stay under {@code Long.MAX_VALUE} wei (~9.22 ETH), which is a hard ceiling rather
     * than a preference: {@code sendRawTx} and {@code signEip1559Transaction} carry {@code valueWei}
     * as a {@code long}, so a balance above it cannot be swept in one transaction — and
     * {@code Faults.drainSigningAccount} exists to do exactly that.
     *
     * <p>8 ETH is ample for a test: at the ~2 gwei this dev chain settles at, a submitBundle costs
     * well under a milli-ETH, so this covers thousands of them plus every deploy, stake and connector
     * top-up the framework performs.
     */
    public static final BigInteger DEFAULT_BALANCE_WEI = BigInteger.valueOf(8L).multiply(BigInteger.TEN.pow(18));

    /**
     * Balance the per-chain deployer account starts with: 100 ETH.
     *
     * <p>The deployer runs the wiring for every channel: each per-side connector gets a
     * {@link org.hiero.clpr.relay.e2e.channel.ChannelSpec#DEFAULT_CONNECTOR_FUNDING_WEI} 1-ETH top-up
     * from this same account, and at ten channels that alone is 20 ETH — well past what a
     * {@link #DEFAULT_BALANCE_WEI} 8-ETH account can carry. Safe to raise: endpoint-signer drain
     * faults do not touch deployer accounts, so sizing this to the test load does not undermine
     * those faults' invariant.
     */
    public static final BigInteger DEPLOYER_BALANCE_WEI =
            BigInteger.valueOf(100L).multiply(BigInteger.TEN.pow(18));

    /** 30,000,000 gas — room for the CLPR contract deploys and multi-message bundles. */
    private static final String GAS_LIMIT = "0x1C9C380";

    /**
     * Osaka activation timestamp, set far in the future to keep the fork <b>off</b>.
     *
     * <p>Osaka's EIP-7825 caps any single transaction at 16,777,216 gas. Inbound CLPR bundles can
     * legitimately exceed that, so activating Osaka would make large bundles fail with
     * out-of-gas rather than a CLPR-level error — an extremely confusing failure mode.
     */
    private static final long OSAKA_TIME_DISABLED = 4102444800L;

    private final BesuNetworkSpec spec;
    private final List<TestAccount> validatorKeys;
    private final List<TestAccount> fundedAccounts;
    private final Map<String, BigInteger> balanceOverrides;

    /**
     * @param spec the network being built
     * @param validatorKeys one derived account per validator; its key becomes the Besu node key and
     *     its address a genesis validator
     * @param fundedAccounts accounts written into {@code alloc} with {@link #DEFAULT_BALANCE_WEI}
     *     unless overridden
     * @param balanceOverrides accounts whose {@code alloc} balance should differ from
     *     {@link #DEFAULT_BALANCE_WEI}, keyed by {@code 0x}-prefixed address. Deployer-like
     *     accounts that fund per-channel connectors need this because 10 × 1-ETH funding easily
     *     exceeds the 8-ETH default.
     */
    public BesuQbftGenesis(
            final BesuNetworkSpec spec,
            final List<TestAccount> validatorKeys,
            final List<TestAccount> fundedAccounts,
            final Map<String, BigInteger> balanceOverrides) {
        if (validatorKeys.size() != spec.validatorCount()) {
            throw new IllegalArgumentException(
                    "expected " + spec.validatorCount() + " validator keys, got " + validatorKeys.size());
        }
        this.spec = spec;
        this.validatorKeys = List.copyOf(validatorKeys);
        this.fundedAccounts = List.copyOf(fundedAccounts);
        // Normalize override keys to lowercase so a caller passing an EIP-55 checksummed
        // address does not silently miss the lookup against TestAccount.address() (which is
        // lowercase). Without this, the account would fall through to DEFAULT_BALANCE_WEI and
        // the deployer would run out of ETH mid-wiring — exactly what the override exists to
        // prevent.
        this.balanceOverrides = balanceOverrides.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(e -> e.getKey().toLowerCase(Locale.ROOT), Map.Entry::getValue));
    }

    /**
     * The single QBFT validator's address, which the CLPR config proof's trust anchor is built from.
     *
     * @return the validator address, {@code 0x}-prefixed lowercase
     * @throws IllegalStateException if the network has more than one validator, since there is then
     *     no single trust anchor and {@code QBFTVerifier} would reject the chain anyway
     */
    public String soleValidatorAddress() {
        if (validatorKeys.size() != 1) {
            throw new IllegalStateException("network '" + spec.id() + "' has " + validatorKeys.size()
                    + " validators; CLPR's QBFTVerifier only accepts a single-validator chain, so there is no"
                    + " single trust anchor to derive");
        }
        return validatorKeys.getFirst().address();
    }

    /** @return the validator accounts, in declaration order. */
    public List<TestAccount> validatorKeys() {
        return validatorKeys;
    }

    /**
     * Render the genesis JSON.
     *
     * @return pretty-printed genesis document
     */
    public String toJson() {
        final ObjectNode root = MAPPER.createObjectNode();

        final ObjectNode config = root.putObject("config");
        config.put("chainId", spec.chainId());
        // Every fork through Prague at genesis. The CLPR contracts compile for a modern EVM and
        // need PUSH0, MCOPY/TSTORE and the EIP-2537 BLS precompiles; without them the contract
        // initcode hits invalid opcodes and reverts during deployment. Osaka stays off — see
        // OSAKA_TIME_DISABLED.
        for (final String block : List.of(
                "homesteadBlock",
                "eip150Block",
                "eip155Block",
                "eip158Block",
                "byzantiumBlock",
                "constantinopleBlock",
                "petersburgBlock",
                "istanbulBlock",
                "muirGlacierBlock",
                "berlinBlock",
                "londonBlock")) {
            config.put(block, 0);
        }
        for (final String time : List.of("shanghaiTime", "cancunTime", "pragueTime")) {
            config.put(time, 0);
        }
        config.put("osakaTime", OSAKA_TIME_DISABLED);

        final ObjectNode qbft = config.putObject("qbft");
        qbft.put("blockperiodseconds", spec.blockPeriodSeconds());
        qbft.put("epochlength", 30000);
        qbft.put("requesttimeoutseconds", 4);
        // Empty blocks keep the chain head advancing while no transactions flow. The relay polls
        // contract state at a block tag and its listener needs the head to move; without this a
        // quiet chain looks indistinguishable from a stalled one.
        qbft.put("createemptyblocks", true);

        root.put("nonce", "0x0");
        root.put("timestamp", "0x0");
        root.put("gasLimit", GAS_LIMIT);
        root.put("difficulty", "0x1");
        root.put("mixHash", "0x63746963616c2062797a616e74696e65206661756c7420746f6c6572616e6365");
        root.put("coinbase", "0x0000000000000000000000000000000000000000");
        root.put("extraData", extraData());

        final ObjectNode alloc = root.putObject("alloc");
        for (final TestAccount account : fundedAccounts) {
            alloc.putObject(account.address())
                    .put("balance", "0x" + balanceFor(account.address()).toString(16));
        }
        // Validator keys double as node keys; fund them too so a validator can also send
        // transactions if a test wants it to.
        for (final TestAccount validator : validatorKeys) {
            if (!alloc.has(validator.address())) {
                alloc.putObject(validator.address())
                        .put("balance", "0x" + balanceFor(validator.address()).toString(16));
            }
        }

        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root);
        } catch (final com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("failed to render Besu genesis for '" + spec.id() + "'", e);
        }
    }

    private BigInteger balanceFor(final String address) {
        return balanceOverrides.getOrDefault(address.toLowerCase(Locale.ROOT), DEFAULT_BALANCE_WEI);
    }

    /**
     * QBFT {@code extraData}: {@code RLP([vanity32, [validators...], [], 0x, []])}.
     *
     * <p>Two traps. The 32-byte vanity is <b>inside</b> the outer list — IBFT2 prepends it as raw
     * bytes instead, and mixing the two yields a genesis Besu accepts but whose header the CLPR
     * verifier cannot parse. And the validator addresses must be sorted ascending by their raw
     * bytes; an unsorted list makes Besu reject the genesis outright.
     */
    private String extraData() {
        final byte[] vanity = new byte[32];

        final List<String> sorted =
                new ArrayList<>(validatorKeys.stream().map(TestAccount::address).toList());
        sorted.sort(Comparator.naturalOrder());

        final byte[][] validatorItems = new byte[sorted.size()][];
        for (int i = 0; i < sorted.size(); i++) {
            validatorItems[i] = AbiCodec.rlpBytes(AbiCodec.fromHex(sorted.get(i)));
        }

        final byte[] encoded = AbiCodec.rlpList(
                AbiCodec.rlpBytes(vanity),
                AbiCodec.rlpList(validatorItems),
                AbiCodec.rlpList(), // vote: none
                AbiCodec.rlpBytes(new byte[0]), // round: 0, encoded as the empty string
                AbiCodec.rlpList()); // committed seals: none at genesis
        return AbiCodec.toHex(encoded);
    }

    /**
     * Build the {@code static-nodes.json} document so every node dials the others from the first
     * block. Peer discovery is disabled in the container command, so this list is the only way the
     * nodes find each other — and because the framework pins container IPs, it can be computed
     * before anything starts.
     *
     * @param enodeUrls one enode URL per node
     * @return the JSON array document
     */
    public static String staticNodesJson(final List<String> enodeUrls) {
        final ArrayNode array = MAPPER.createArrayNode();
        enodeUrls.forEach(array::add);
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(array);
        } catch (final com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("failed to render static-nodes.json", e);
        }
    }

    /**
     * Compose an enode URL. Besu identifies a peer by the 64-byte uncompressed secp256k1 public key
     * of its node key, hex-encoded without the {@code 0x04} prefix.
     *
     * @param account the node's key
     * @param ipAddress the node's pinned address on the environment network
     * @param p2pPort the P2P port
     * @return the enode URL
     */
    public static String enodeUrl(final TestAccount account, final String ipAddress, final int p2pPort) {
        return "enode://" + AbiCodec.toHexNoPrefix(account.publicKey64()) + "@" + ipAddress + ":" + p2pPort;
    }
}
