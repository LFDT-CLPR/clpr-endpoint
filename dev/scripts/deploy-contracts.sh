#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# deploy-contracts.sh — Deploy and configure two independent CLPR contract sets on a local
# Anvil node for kind/Kubernetes dev.
#
# What it does:
#   1. Derives secp256k1 public keys for relay A and relay B's signing accounts.
#   2. Deploys two independent contract sets (ClprService + logic libraries + mocks),
#      each simulating one ledger, using Foundry's forge and cast tools.
#   3. Configures economics, ledger, endpoint registration, verifier, and mock apps
#      on both contract sets so the two relay pods can sync bidirectionally.
#   4. Registers a channel and a connector on both chains via the commit-reveal
#      pattern, using the same EIP-191 signing that the relay itself uses.
#   5. Sends one initial message in each direction.
#   6. Writes a sourceable env file at $OUTPUT_FILE.
#
# Prerequisites:
#   - Anvil running on $ANVIL_URL (default http://localhost:8545)
#     anvil --chain-id 1337 --block-time 1 --hardfork cancun --disable-code-size-limit
#   - Foundry (forge, cast): https://book.getfoundry.sh/
#   - clpr-smart-contracts repo checked out as a sibling of this repo, with artifacts
#     already built: cd ../clpr-smart-contracts && forge build
#
# Environment variables (all optional):
#   ANVIL_URL         JSON-RPC URL for Anvil (default: http://localhost:8545)
#   RELAY_A_HOST      K8s ClusterIP hostname for relay A's gRPC service
#                     (default: relay-a-clpr-evm-endpoint-grpc)
#   RELAY_B_HOST      K8s ClusterIP hostname for relay B's gRPC service
#                     (default: relay-b-clpr-evm-endpoint-grpc)
#   RELAY_GRPC_PORT   gRPC port on both relay services (default: 9545)
#   OUTPUT_FILE       Path to write the sourceable env file
#                     (default: /tmp/clpr-kind-setup.env)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTRACTS_DIR="${CONTRACTS_DIR:-$(cd "$SCRIPT_DIR/../../../clpr-smart-contracts" && pwd)}"

# ── Configuration ──────────────────────────────────────────────────────────────

ANVIL_URL="${ANVIL_URL:-http://localhost:8545}"
RELAY_A_HOST="${RELAY_A_HOST:-relay-a-clpr-evm-endpoint-grpc}"
RELAY_B_HOST="${RELAY_B_HOST:-relay-b-clpr-evm-endpoint-grpc}"
RELAY_GRPC_PORT="${RELAY_GRPC_PORT:-9545}"
# IP literals for the updateLedgerConfiguration seed endpoints.
# EndpointValidation.validateEndpointAddress() accepts only IPv4/IPv6 literals, not hostnames.
# Default to RELAY_{A,B}_HOST so Docker Compose (which already passes static IPs) needs no change.
# kind-setup.sh overrides these to 0.0.0.0 because it passes K8s DNS names in RELAY_{A,B}_HOST.
RELAY_A_SEED_IP="${RELAY_A_SEED_IP:-${RELAY_A_HOST}}"
RELAY_B_SEED_IP="${RELAY_B_SEED_IP:-${RELAY_B_HOST}}"
OUTPUT_FILE="${OUTPUT_FILE:-/tmp/clpr-kind-setup.env}"
CHAIN_ID=1337

# Anvil deterministic dev accounts.
# Account 0: relay A's signing identity and the deployer/owner for all contracts.
# Account 1: relay B's signing identity (registers on CONTRACT_B as the B endpoint).
# Account 2: dedicated message sender (separate nonce sequence from relay signers).
KEY_A="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
KEY_B="0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
KEY_SENDER="0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a"
ADDR_A="0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
ADDR_B="0x70997970C51812dc3A010C7d01b50e0d17dc79C8"

# 32-byte operator salt (all zeros); same in both commit-reveal rounds so the
# channel and connector IDs are deterministic and match between chain A and B.
SALT="0x0000000000000000000000000000000000000000000000000000000000000000"

# configProof passed to completeChannel; AdminLogic accepts any non-empty bytes.
CONFIG_PROOF="0x0001"

# ── Helpers ────────────────────────────────────────────────────────────────────

err() { echo "$*" >&2; }

# Strip 0x prefix for raw byte concatenation.
strip_0x() { echo "${1#0x}"; }

# Compute keccak256 of raw bytes produced by concatenating one or more hex strings
# (each with or without 0x prefix). Used for commitment and message-hash derivation.
keccak_concat() {
    local concat="0x"
    for arg in "$@"; do
        concat="${concat}$(strip_0x "$arg")"
    done
    cast keccak "$concat"
}

# Deploy a contract from the clpr-smart-contracts directory.
# Echoes the deployed contract address; exits 1 on failure.
deploy_contract() {
    local contract_path="$1"; shift
    local out addr
    out=$(
        cd "$CONTRACTS_DIR"
        forge create "$contract_path" \
            --rpc-url "$ANVIL_URL" \
            --private-key "$KEY_A" \
            --broadcast \
            "$@" 2>&1
    )
    addr=$(echo "$out" | awk '/Deployed to:/{print $NF}')
    if [[ -z "$addr" ]]; then
        err "ERROR: deployment of $contract_path failed:"
        err "$out"
        exit 1
    fi
    echo "$addr"
}

# Send a transaction, discard the receipt hash, abort on revert.
# Usage: tx <private_key> <to> <sig> [args...] [--value <wei>]
tx() {
    local key="$1"; shift
    cast send --rpc-url "$ANVIL_URL" --private-key "$key" "$@" >/dev/null
}

# ── Main ───────────────────────────────────────────────────────────────────────

err "=== CLPR Contract Deployer ==="
err "Anvil:    $ANVIL_URL"
err "Relay A:  $RELAY_A_HOST:$RELAY_GRPC_PORT"
err "Relay B:  $RELAY_B_HOST:$RELAY_GRPC_PORT"
err "Output:   $OUTPUT_FILE"
err ""

# ── [1/8] Derive signing public keys ──────────────────────────────────────────
# cast wallet public-key returns the 64-byte uncompressed secp256k1 key (X||Y)
# as 0x<128 hex chars> — the same format EthSigner.derivePublicKey() produces.
err "[1/8] Deriving signing public keys..."
PUB_KEY_A=$(cast wallet public-key --private-key "$KEY_A")
PUB_KEY_B=$(cast wallet public-key --private-key "$KEY_B")
err "  Relay A pub: ${PUB_KEY_A:0:18}..."
err "  Relay B pub: ${PUB_KEY_B:0:18}..."

PUB_KEY_A_RAW=$(strip_0x "$PUB_KEY_A")
PUB_KEY_B_RAW=$(strip_0x "$PUB_KEY_B")

# ── [2/8] Deploy chain A contracts ────────────────────────────────────────────
err ""
err "[2/8] Deploying chain A contracts..."

CONN_LOGIC_A=$(deploy_contract  "src/logic/ChannelLogic.sol:ChannelLogic")
MSG_LOGIC_A=$(deploy_contract   "src/logic/MessagingLogic.sol:MessagingLogic")
BUNDLE_LOGIC_A=$(deploy_contract "src/logic/BundleLogic.sol:BundleLogic")
CONNECTOR_LOGIC_A=$(deploy_contract "src/logic/ConnectorLogic.sol:ConnectorLogic")
ADMIN_LOGIC_A=$(deploy_contract  "src/logic/AdminLogic.sol:AdminLogic")
BUNDLE_DECODE_A=$(deploy_contract "src/libraries/codec/BundleDecodeHelper.sol:BundleDecodeHelper")

CONTRACT_A=$(deploy_contract "src/ClprService.sol:ClprService" \
    --constructor-args "$ADDR_A" 1 "eip155:1" \
    "$CONN_LOGIC_A" "$MSG_LOGIC_A" "$BUNDLE_LOGIC_A" \
    "$CONNECTOR_LOGIC_A" "$ADMIN_LOGIC_A" "$BUNDLE_DECODE_A")

QBFT_VERIFIER_A=$(deploy_contract "test/mocks/MockClprVerifier.sol:MockClprVerifier")
MOCK_APP_A=$(deploy_contract       "test/mocks/MockClprApplication.sol:MockClprApplication")
MOCK_CONNECTOR_A=$(deploy_contract "test/mocks/MockClprConnector.sol:MockClprConnector")

err "  ClprService A:     $CONTRACT_A"
err "  QbftVerifier A:    $QBFT_VERIFIER_A"
err "  MockApp A:         $MOCK_APP_A"
err "  MockConnector A:   $MOCK_CONNECTOR_A"

# ── [3/8] Deploy chain B contracts ────────────────────────────────────────────
err ""
err "[3/8] Deploying chain B contracts..."

CONN_LOGIC_B=$(deploy_contract  "src/logic/ChannelLogic.sol:ChannelLogic")
MSG_LOGIC_B=$(deploy_contract   "src/logic/MessagingLogic.sol:MessagingLogic")
BUNDLE_LOGIC_B=$(deploy_contract "src/logic/BundleLogic.sol:BundleLogic")
CONNECTOR_LOGIC_B=$(deploy_contract "src/logic/ConnectorLogic.sol:ConnectorLogic")
ADMIN_LOGIC_B=$(deploy_contract  "src/logic/AdminLogic.sol:AdminLogic")
BUNDLE_DECODE_B=$(deploy_contract "src/libraries/codec/BundleDecodeHelper.sol:BundleDecodeHelper")

CONTRACT_B=$(deploy_contract "src/ClprService.sol:ClprService" \
    --constructor-args "$ADDR_A" 1 "eip155:1" \
    "$CONN_LOGIC_B" "$MSG_LOGIC_B" "$BUNDLE_LOGIC_B" \
    "$CONNECTOR_LOGIC_B" "$ADMIN_LOGIC_B" "$BUNDLE_DECODE_B")

QBFT_VERIFIER_B=$(deploy_contract "test/mocks/MockClprVerifier.sol:MockClprVerifier")
MOCK_APP_B=$(deploy_contract       "test/mocks/MockClprApplication.sol:MockClprApplication")
MOCK_CONNECTOR_B=$(deploy_contract "test/mocks/MockClprConnector.sol:MockClprConnector")

err "  ClprService B:     $CONTRACT_B"
err "  QbftVerifier B:    $QBFT_VERIFIER_B"
err "  MockApp B:         $MOCK_APP_B"
err "  MockConnector B:   $MOCK_CONNECTOR_B"

CONTRACT_A_RAW=$(strip_0x "$CONTRACT_A")
CONTRACT_B_RAW=$(strip_0x "$CONTRACT_B")

# ── [4/8] Configure economics and ledger on both chains ───────────────────────
err ""
err "[4/8] Configuring economics and ledger..."

# 11-field EconomicConfig struct; field order matches the Solidity definition.
ECON="(1000000000000000,10,100000000000000000,10000000000000000,10000000000000000,2,5,50,50000,0,0)"
tx "$KEY_A" "$CONTRACT_A" \
    "updateEconomicConfiguration((uint256,uint256,uint256,uint256,uint256,uint256,uint32,uint32,uint64,uint32,uint32))" \
    "$ECON"
tx "$KEY_A" "$CONTRACT_B" \
    "updateEconomicConfiguration((uint256,uint256,uint256,uint256,uint256,uint256,uint32,uint32,uint64,uint32,uint32))" \
    "$ECON"
err "  Economics configured on both chains."

# Throttles struct (5 uint64 fields) used in ledger config and verifier configuration:
# maxMessagesPerBundle, maxMessagePayloadBytes, maxGasPerMessage, maxQueueDepth, maxSyncBytes.
THROTTLES="(100,1048576,10000000,1000,1048576)"
SIG_LEDGER="updateLedgerConfiguration(bytes,(uint64,uint64,uint64,uint64,uint64),(string,uint32,bytes,bytes)[],bytes,bytes)"

# Chain A ledger: service address = CONTRACT_A; seed endpoint = relay B's IP + pub key B.
tx "$KEY_A" "$CONTRACT_A" "$SIG_LEDGER" \
    "$CONTRACT_A" "$THROTTLES" \
    "[(\"$RELAY_B_SEED_IP\",$RELAY_GRPC_PORT,0x,$ADDR_B)]" \
    "0x" "0x"
err "  Ledger configured on chain A (seed: $RELAY_B_SEED_IP)."

# Chain B ledger: service address = CONTRACT_B; seed endpoint = relay A's IP + pub key A.
tx "$KEY_A" "$CONTRACT_B" "$SIG_LEDGER" \
    "$CONTRACT_B" "$THROTTLES" \
    "[(\"$RELAY_A_SEED_IP\",$RELAY_GRPC_PORT,0x,$ADDR_A)]" \
    "0x" "0x"
err "  Ledger configured on chain B (seed: $RELAY_A_SEED_IP)."

# ── [5/8] Register endpoints ───────────────────────────────────────────────────
err ""
err "[5/8] Registering endpoints..."

BOND=10000000000000000  # 0.01 ETH minimum bond

# Relay A registers on CONTRACT_A using KEY_A. Registration is permissionless and
# bond-only now — no pub key argument.
tx "$KEY_A" "$CONTRACT_A" "registerEndpoint()" --value "$BOND"
err "  Relay A registered on contract A."

# Relay B registers on CONTRACT_B using KEY_B.
tx "$KEY_B" "$CONTRACT_B" "registerEndpoint()" --value "$BOND"
err "  Relay B registered on contract B."

# ── [6/8] Configure verifiers and mock apps ────────────────────────────────────
err ""
err "[6/8] Configuring verifiers and mock apps..."

# QbftPassThroughVerifier A:
#   - verifyConfig reports CONTRACT_B as the peer service address (chain ID "eip155:1")
#   - seed endpoint points to relay B's gRPC service with pub key B
#   - peer throttles match the ledger defaults so sendMessage does not revert
tx "$KEY_A" "$QBFT_VERIFIER_A" "setVerifyConfigResult(string,bytes,uint96)" "eip155:1" "$CONTRACT_B" 1000
tx "$KEY_A" "$QBFT_VERIFIER_A" "setPeerThrottles((uint64,uint64,uint64,uint64,uint64))" "$THROTTLES"
tx "$KEY_A" "$QBFT_VERIFIER_A" "setSeedEndpoints((string,uint32,bytes,bytes)[])" \
    "[(\"$RELAY_B_HOST\",$RELAY_GRPC_PORT,0x,$ADDR_B)]"
err "  Verifier A configured (peer: $CONTRACT_B)."

# QbftPassThroughVerifier B:
#   - verifyConfig reports CONTRACT_A as the peer service address
#   - seed endpoint points to relay A's gRPC service with pub key A
tx "$KEY_A" "$QBFT_VERIFIER_B" "setVerifyConfigResult(string,bytes,uint96)" "eip155:1" "$CONTRACT_A" 1000
tx "$KEY_A" "$QBFT_VERIFIER_B" "setPeerThrottles((uint64,uint64,uint64,uint64,uint64))" "$THROTTLES"
tx "$KEY_A" "$QBFT_VERIFIER_B" "setSeedEndpoints((string,uint32,bytes,bytes)[])" \
    "[(\"$RELAY_A_HOST\",$RELAY_GRPC_PORT,0x,$ADDR_A)]"
err "  Verifier B configured (peer: $CONTRACT_A)."

# Pre-load PONG responses in mock apps so inbound messages are handled.
PONG="0x$(printf 'PONG' | od -An -tx1 | tr -d ' \n')"
tx "$KEY_A" "$MOCK_APP_A" "setResponse(bytes)" "$PONG"
tx "$KEY_A" "$MOCK_APP_B" "setResponse(bytes)" "$PONG"
err "  Mock apps configured with PONG response."

# Fund mock connectors so they can pay the inbound gas stipend.
cast send "$MOCK_CONNECTOR_A" --rpc-url "$ANVIL_URL" --private-key "$KEY_A" \
    --value 1000000000000000000 >/dev/null
cast send "$MOCK_CONNECTOR_B" --rpc-url "$ANVIL_URL" --private-key "$KEY_A" \
    --value 1000000000000000000 >/dev/null
err "  Mock connectors funded (1 ETH each)."

# ── [7/8] Register channel (commit-reveal) on both chains ──────────────────
err ""
err "[7/8] Registering channel (commit-reveal)..."

# Both chains use the same channelId (derived from the same pub key + salt pair).
# Both commit-reveal rounds use KEY_A / PUB_KEY_A because ContractInteractor was
# constructed with signerA on both chains in the original Java setup.

# Derive channelId on-chain: deriveChannelId(peerChainId, pubKey, salt) → bytes32.
# Uses CONTRACT_A; the result is identical on CONTRACT_B because the inputs are the same.
CHANNEL_ID=$(cast call "$CONTRACT_A" \
    "deriveChannelId(string,bytes,bytes32)(bytes32)" \
    "eip155:1" "$PUB_KEY_A" "$SALT" \
    --rpc-url "$ANVIL_URL")
CONN_ID_RAW=$(strip_0x "$CHANNEL_ID")
err "  Channel ID: $CHANNEL_ID"

# commitment = keccak256(channelId || pubKeyA)
COMMITMENT=$(keccak_concat "$CONN_ID_RAW" "$PUB_KEY_A_RAW")

# Chain A: registerChannel → completeChannel
tx "$KEY_A" "$CONTRACT_A" "registerChannel(bytes32,bytes32)" "$CHANNEL_ID" "$COMMITMENT"

# messageHash = keccak256(channelId || address(clprServiceA))
# EthSigner.signMessage applies EIP-191 before signing; cast wallet sign does the same.
MSG_HASH_A=$(keccak_concat "$CONN_ID_RAW" "$CONTRACT_A_RAW")
SIG_A=$(cast wallet sign --private-key "$KEY_A" "$MSG_HASH_A")

tx "$KEY_A" "$CONTRACT_A" \
    "completeChannel(bytes32,bytes,bytes,bytes32,address,bytes)" \
    "$CHANNEL_ID" "$PUB_KEY_A" "$SIG_A" "$SALT" "$QBFT_VERIFIER_A" "$CONFIG_PROOF"
err "  Channel registered on chain A."

# Chain B: same commitment (same pubKey + channelId), different service address in msg hash.
tx "$KEY_A" "$CONTRACT_B" "registerChannel(bytes32,bytes32)" "$CHANNEL_ID" "$COMMITMENT"

MSG_HASH_B=$(keccak_concat "$CONN_ID_RAW" "$CONTRACT_B_RAW")
SIG_B=$(cast wallet sign --private-key "$KEY_A" "$MSG_HASH_B")

tx "$KEY_A" "$CONTRACT_B" \
    "completeChannel(bytes32,bytes,bytes,bytes32,address,bytes)" \
    "$CHANNEL_ID" "$PUB_KEY_A" "$SIG_B" "$SALT" "$QBFT_VERIFIER_B" "$CONFIG_PROOF"
err "  Channel registered on chain B."

# ── [8/8] Register connector (commit-reveal) on both chains ───────────────────
err ""
err "[8/8] Registering connector (commit-reveal)..."

SALT_RAW=$(strip_0x "$SALT")

# connectorId = keccak256(channelId || pubKeyA || salt)
CONNECTOR_ID=$(keccak_concat "$CONN_ID_RAW" "$PUB_KEY_A_RAW" "$SALT_RAW")
CONNECTOR_ID_RAW=$(strip_0x "$CONNECTOR_ID")
err "  Connector ID: $CONNECTOR_ID"

# commitment = keccak256(connectorId || pubKeyA)
CONNECTOR_COMMITMENT=$(keccak_concat "$CONNECTOR_ID_RAW" "$PUB_KEY_A_RAW")

STAKE=100000000000000000  # 0.1 ETH minimum locked stake

# Chain A: registerConnector → completeConnector
tx "$KEY_A" "$CONTRACT_A" "registerConnector(bytes32)" "$CONNECTOR_COMMITMENT"

CONNECTOR_MSG_A=$(keccak_concat "$CONNECTOR_ID_RAW" "$CONTRACT_A_RAW")
CONNECTOR_SIG_A=$(cast wallet sign --private-key "$KEY_A" "$CONNECTOR_MSG_A")

tx "$KEY_A" "$CONTRACT_A" \
    "completeConnector(bytes32,bytes,bytes,bytes32,bytes32,address,address)" \
    "$CONNECTOR_ID" "$PUB_KEY_A" "$CONNECTOR_SIG_A" \
    "$SALT" "$CHANNEL_ID" \
    "$MOCK_CONNECTOR_A" "$ADDR_A" \
    --value "$STAKE"
err "  Connector registered on chain A."

# Chain B: same connectorId and commitment.
tx "$KEY_A" "$CONTRACT_B" "registerConnector(bytes32)" "$CONNECTOR_COMMITMENT"

CONNECTOR_MSG_B=$(keccak_concat "$CONNECTOR_ID_RAW" "$CONTRACT_B_RAW")
CONNECTOR_SIG_B=$(cast wallet sign --private-key "$KEY_A" "$CONNECTOR_MSG_B")

tx "$KEY_A" "$CONTRACT_B" \
    "completeConnector(bytes32,bytes,bytes,bytes32,bytes32,address,address)" \
    "$CONNECTOR_ID" "$PUB_KEY_A" "$CONNECTOR_SIG_B" \
    "$SALT" "$CHANNEL_ID" \
    "$MOCK_CONNECTOR_B" "$ADDR_A" \
    --value "$STAKE"
err "  Connector registered on chain B."

# ── Send initial test messages ─────────────────────────────────────────────────
err ""
err "[+] Sending initial test messages (sender: account 2)..."

cast send "$CONTRACT_A" "sendMessage(bytes32,bytes32,bytes,bytes)" \
    "$CHANNEL_ID" "$CONNECTOR_ID" "$MOCK_APP_B" \
    "0x$(printf 'HELLO-FROM-A' | od -An -tx1 | tr -d ' \n')" \
    --rpc-url "$ANVIL_URL" --private-key "$KEY_SENDER" >/dev/null
err "  Sent HELLO-FROM-A on chain A → targeting mock app on chain B."

cast send "$CONTRACT_B" "sendMessage(bytes32,bytes32,bytes,bytes)" \
    "$CHANNEL_ID" "$CONNECTOR_ID" "$MOCK_APP_A" \
    "0x$(printf 'HELLO-FROM-B' | od -An -tx1 | tr -d ' \n')" \
    --rpc-url "$ANVIL_URL" --private-key "$KEY_SENDER" >/dev/null
err "  Sent HELLO-FROM-B on chain B → targeting mock app on chain A."

# ── Write env file ─────────────────────────────────────────────────────────────
err ""
err "[+] Writing env file to $OUTPUT_FILE..."
cat > "$OUTPUT_FILE" <<EOF
CONTRACT_A=${CONTRACT_A}
CONTRACT_B=${CONTRACT_B}
MOCK_APP_A=${MOCK_APP_A}
MOCK_APP_B=${MOCK_APP_B}
CHANNEL_ID=${CHANNEL_ID}
CONNECTOR_ID=${CONNECTOR_ID}
DEV_ADDRESS_A=${ADDR_A}
DEV_ADDRESS_B=${ADDR_B}
DEV_PRIVATE_KEY_A=${KEY_A}
DEV_PRIVATE_KEY_B=${KEY_B}
CHAIN_ID=${CHAIN_ID}
EOF

err ""
err "Setup complete."
err "  helm install relay-a ... --set evm.contractAddress=${CONTRACT_A}"
err "  helm install relay-b ... --set evm.contractAddress=${CONTRACT_B}"
