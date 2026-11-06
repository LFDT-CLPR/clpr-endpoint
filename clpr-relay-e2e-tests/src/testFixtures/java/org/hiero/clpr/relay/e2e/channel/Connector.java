// SPDX-License-Identifier: Apache-2.0
package org.hiero.clpr.relay.e2e.channel;

import java.math.BigInteger;
import java.time.Duration;
import org.hiero.clpr.relay.e2e.chain.BesuNetwork;
import org.hiero.clpr.relay.e2e.chain.ChainTx;
import org.hiero.clpr.relay.evm.AbiCodec;

/**
 * A registered connector on one side of a channel: the staked adapter the service dispatches inbound
 * messages through.
 *
 * <p>Two balances matter and they are not the same thing. The <b>stake</b> is locked in the service
 * and gets slashed when the connector misbehaves or cannot pay. The contract's own <b>ETH balance</b>
 * is the prepaid execution budget: before dispatching, the service checks whether that balance covers
 * {@code maxGasPerMessage} at the current gas price, and if it does not, the message comes back as
 * {@code CONNECTOR_UNDERFUNDED} and the connector is slashed.
 */
public final class Connector {

    private final BesuNetwork network;
    private final byte[] id;
    private final String contractAddress;
    private final Duration fundTimeout;

    Connector(final BesuNetwork network, final byte[] id, final String contractAddress, final Duration fundTimeout) {
        this.network = network;
        this.id = id.clone();
        this.contractAddress = contractAddress;
        this.fundTimeout = fundTimeout;
    }

    /** @return the 32-byte connector id. */
    public byte[] id() {
        return id.clone();
    }

    /** @return the connector id as {@code 0x}-prefixed hex. */
    public String idHex() {
        return AbiCodec.toHex(id);
    }

    /** @return the {@code MockClprConnector} contract address. */
    public String contractAddress() {
        return contractAddress;
    }

    /** @return the contract's ETH balance, i.e. its remaining execution budget. */
    public BigInteger balance() {
        return network.rpc().ethGetBalance(contractAddress, "latest");
    }

    /**
     * Top the execution budget up.
     *
     * @param wei amount to send
     */
    public void fund(final BigInteger wei) {
        ChainTx.fund(
                network.rpc(),
                network.deployerSubmitter(),
                contractAddress,
                wei,
                "fund connector " + idHex(),
                fundTimeout);
    }

    @Override
    public String toString() {
        return "Connector[" + idHex() + " at " + contractAddress + " on " + network.id() + "]";
    }
}
