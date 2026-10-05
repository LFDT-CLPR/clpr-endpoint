# EVM Integration

How the CLPR protocol (spec §1–§6) is manifested against an EVM-deployed `ClprService` Solidity contract.

Prerequisite: `docs/README.md`, `docs/architecture.md`, and the spec docs.

## Contract surface area used by the relay

The relay calls the following Solidity functions on the configured `ClprService` contract address. Call sites are in `clpr-relay-evm/src/main/java/org/hiero/clpr/relay/evm/AbiCodec.java` (encoders) and `EvmContractStateReader.java` / `AccountTransactionSubmitter.java` (dispatchers). Function signatures are listed verbatim — the function selector (4-byte keccak prefix) is computed from these strings, so any change must match the deployed contract exactly.

### Write methods (transactions)

|      Solidity signature       |                        Purpose                        |                 Caller                 |
|-------------------------------|-------------------------------------------------------|----------------------------------------|
| `submitBundle(bytes32,bytes)` | Land a peer-supplied bundle for a channel. Spec §6.4. | `AccountTransactionSubmitter` (worker) |

The contract takes only `(channelId, proofBytes)`. There is no endpoint signature: the `endpointSignature` argument was removed from both the contract and the relay's `AccountTransactionSubmitter.submit` / `AbiCodec.encodeSubmitBundle` (see `sync-protocol.md`).

Other contract write methods (`registerEndpoint`, `registerChannel`, `completeChannel`, `registerConnector`, `completeConnector`, `updateLedgerConfiguration`, `sendMessage`, etc.) are not invoked by the relay process. They are exercised only by integration tests via `clpr-relay-integration-tests/.../ContractInteractor.java`, which acts as an admin/test driver.

### Read methods (`eth_call`)

|      Solidity signature      |                        Returns                        |                  Caller                   |
|------------------------------|-------------------------------------------------------|-------------------------------------------|
| `getChannel(bytes32)`        | dynamic `Channel` struct (see layout below)           | `EvmContractStateReader.readChannelState` |
| `getMessage(bytes32,uint64)` | `(bytes payload, bytes32 runningHashAfterProcessing)` | `readQueuedMessages`                      |
| `getLedgerConfiguration()`   | `LedgerConfiguration` struct (see layout below)       | `readLedgerConfiguration`                 |
| `isRegistered(address)`      | `bool`                                                | integration tests only                    |

All reads dispatch via `EvmJsonRpcClient.ethCall(contractAddress, callData, blockTag)`. The block tag comes from `CommitmentLevel.toBlockTag()`:
- `LATEST → "latest"` (head, may reorg)
- `SAFE → "safe"`
- `FINALIZED → "finalized"`

The relay reads at commitment level `LATEST` for all channels (it is no longer per-channel config).

## ABI decoding contracts

The codec is hand-rolled (no web3j dependency). Layouts below describe what `EvmContractStateReader` expects the contract to return; this is the **interface contract** between this relay and the `ClprService` Solidity implementation. Changes to the contract storage layout that affect return shape must be mirrored here.

### `Channel` struct returned by `getChannel`

Outer offset pointer at word 0 (always 0x20 in practice; read dynamically). Then a 24-slot head:

| Slot  |        Field        |      Type       |                                            Notes                                            |
|-------|---------------------|-----------------|---------------------------------------------------------------------------------------------|
| 0     | channelId           | bytes32         |                                                                                             |
| 1     | verifier            | address         | left-padded to 32                                                                           |
| 2     | status              | uint8           | enum ordinal; mapped to `ClprChannelStatus`                                                 |
| 3     | nextMessageId       | uint64          |                                                                                             |
| 4     | ackedMessageId      | uint64          |                                                                                             |
| 5     | receivedMessageId   | uint64          |                                                                                             |
| 6     | nextExpectedReplyId | uint64          |                                                                                             |
| 7     | peerConfigTimestamp | uint96          | nanos since epoch                                                                           |
| 8     | lastConfigTimestamp | uint96          |                                                                                             |
| 9     | sentRunningHash     | bytes32         |                                                                                             |
| 10    | receivedRunningHash | bytes32         |                                                                                             |
| 11    | ownershipCommitment | bytes32         |                                                                                             |
| 12    | salt                | bytes32         |                                                                                             |
| 13    | chainId             | string (offset) | tail-encoded                                                                                |
| 14    | peerServiceAddress  | bytes (offset)  | tail-encoded                                                                                |
| 15–19 | Throttles fields    | uint64 inline   | maxMessagesPerBundle, maxMessagePayloadBytes, maxGasPerMessage, maxQueueDepth, maxSyncBytes |
| 20    | trustAnchor         | bytes (offset)  | tail-encoded                                                                                |
| 21    | lastDataMessageId   | uint64          | inline; not modelled by `ClprChannelOps` — skipped                                          |
| 22    | trustAnchorId       | bytes (offset)  | tail-encoded                                                                                |
| 23    | channelContext      | bytes (offset)  | tail-encoded; not modelled by `ClprChannelOps` — skipped                                    |

A short return (< 800 bytes) is treated as "channel not found" (`readChannelState` yields an empty `Optional`). The relay also tolerates contract calls that revert or return empty data — they degrade to "no record" with a warn log; the next sync tick retries.

### `MessageValue` returned by `getMessage`

Outer offset pointer, then a 2-slot struct head: `[offset payload][bytes32 runningHashAfterProcessing]`. The `payload` bytes are protobuf-encoded `ClprMessagePayload` (see spec §1.4) and parsed by PBJ.

### `LedgerConfiguration` returned by `getLedgerConfiguration`

Outer offset pointer, then 10-slot struct head. `decodeLedgerConfiguration` extracts: `protocolVersion` (slot 0), `chainId` (slot 1, string), `serviceAddress` (slot 2, bytes), `peerConfigTimestamp` (slot 3, uint64), all 5 `ClprThrottles` fields (slots 4–8: `maxMessagesPerBundle`, `maxMessagePayloadBytes`, `maxGasPerMessage`, `maxQueueDepth`, `maxSyncBytes`), and `endpoints` (slot 9, tail-encoded dynamic array).

`protocolVersion` is validated against `RelayProtocol.PROTOCOL_VERSION` (currently 1) when each channel is registered (in `ClprChannelHandler.create`). A mismatch fails that channel's registration — the relay logs an ERROR and skips the channel rather than operating against an incompatible contract version — while continuing to serve any other channels whose versions match.

Each endpoint is a tuple `(string ipAddress, uint32 port, bytes tlsCertificate, bytes accountId)`. When `accountId` is empty on chain, the relay synthesizes `ip:port` as the cache key so `PeerSelector` can distinguish peers.

### QBFT-specific wrapper for `getLedgerConfiguration` gRPC

The gRPC `getLedgerConfiguration` response goes beyond the on-chain struct: it also carries a `QbftLedgerConfigurationPayload`. Assembled by `QbftLedgerConfigurationProvider` (`clpr-relay-evm`, implements `core.LedgerConfigurationPayloadProvider`):

- **`genesis_block_header`** — bytes RLP from `eth_getBlockByNumber("0x0", false)`, encoded by `BlockHeaderRlpCodec.encodeRlp` (the same encoder `QbftBundleConstructor` uses, so the wire form matches the bytes Besu signs when computing the QBFT committed-seal hash). The peer's verifier decodes `extra_data` to recover the initial QBFT validator address.
- **`current_block_header`** — same encoding for the block at the requested commitment level (`FINALIZED`). Its `state_root` is the verification root for the proofs below.
- **`ledger_configuration`** — the on-chain `ClprLedgerConfiguration` read at the same commitment level via `ContractStateReader.readLedgerConfiguration`.
- **`clpr_service_account_proof`** — EIP-1186 account proof for the ClprService contract under `current_block_header.state_root`. Populated by `eth_getProof` issued at the same pinned block number as the contract state read.
- **`clpr_service_storage_proofs`** — EIP-1186 storage proofs for the ClprService storage slots that compose the configuration value. **First version proves only the storage slot of `_config.serviceAddress` (slot 23 — `_config` base = 21, field offset 2; see `EvmQbftLedgerConfigurationProvider.CONFIG_SERVICE_ADDRESS_SLOT`)** as a sanity check on the verifier's proof-format handling. A follow-up will extend the slot whitelist to the remaining configuration fields (`protocolVersion`, `chainId`, `timestamp`, all 5 `ClprThrottles` fields, and the `endpoints` array tail — see ABI offsets in §`LedgerConfiguration returned by getLedgerConfiguration` above).

The internal consumers — sync loop, protocolVersion bootstrap check, state-change listener, peer roster reads — still go through the raw `ContractStateReader` and incur no extra `eth_getBlockByNumber` round-trips per cycle.

## Transaction submission

`AccountTransactionSubmitter` builds and submits `submitBundle(bytes32,bytes)` as a client-side-signed EIP-1559 (type-2) raw transaction. An `EthSigner` is constructed per channel from its service's `clprServices[].defaultSigningPrivateKeyHex` (or the channel's per-channel override), and the EVM sender address is derived from that key. Submission goes through `eth_sendRawTransaction`; the node holds no wallet state for the relay.

The RLP envelope follows EIP-1559: `0x02 || rlp([chainId, nonce, maxPriorityFeePerGas, maxFeePerGas, gasLimit, to, value, data, accessList=[], yParity, r, s])`. The signing digest is `keccak256(0x02 || rlp([... without sig fields]))`. ECDSA uses RFC 6979 deterministic `k`, low-S normalisation, and `yParity` masked to the recoveryId's low bit — the high-bit case (`Rx ≥ N`) has probability ~2⁻¹²⁸ with RFC 6979 and is not representable in EIP-1559.

### Gas strategy

`Eip1559GasStrategy.computeFees()` is invoked per submission:

- `baseFee = eth_getBlockByNumber("latest").baseFeePerGas` (treated as `0` on chains that don't report it — Anvil dev mode pre-1559)
- `maxFeePerGas = round(baseFee × gasBufferMultiplier) + gasPriorityFee`
- `maxPriorityFeePerGas = gasPriorityFee` (passed through unchanged)

If `maxFeePerGas` exceeds `maxGasPriceCap` (or long-overflow is detected), the strategy throws `JsonRpcException("gas price cap exceeded ...")`; the `AccountTransactionSubmitter` worker catches it, counts `evm.tx.failures{reason=gas}`, and sends **no** transaction — protecting the operator from runaway spend during a baseFee spike. The gas limit is sized per bundle via `eth_estimateGas` **+50% margin**, capped at `28_000_000` (28M, the block-safe maximum and the fallback if estimation fails). Sizing to actual usage (rather than a flat 28M) keeps the node's **upfront reservation** — `maxFeePerGas × gasLimit`, which the sender's balance must cover for the tx to be mineable — proportional to real cost, so a modestly funded account is not stranded (a flat 28M reserved ~6× the ~5M a typical bundle uses, draining the runway and eventually pooling-but-never-mining the next tx). The transaction is signed at the fixed nonce and driven to a verdict over a bounded number of rounds. Each submit round polls for the receipt up to `50 × 100ms`: a `0x1` receipt counts the submission confirmed (`sync.bundle.submissions` / `evm.tx.submissions`), a non-`0x1` receipt counts a revert (`sync.bundle.reverted` / `evm.tx.reverts`). If no receipt appears within a round, the worker **re-broadcasts the same transaction** and polls again. Rather than bump the fee blindly (with a 28M gas limit, a per-round multiplier would inflate `maxFee × gasLimit` toward the account balance and the node's per-tx fee cap, stranding it), **each unmined round re-reads the live market fee** via `Eip1559GasStrategy.computeFeesCapped()` (an `eth_getBlockByNumber` base-fee read, clamped to `maxGasPriceCap` instead of throwing): if the market now wants more than we last signed, the tx is re-priced up to it and **re-signed at the same nonce**; if it is the same or lower, the current tx is re-sent verbatim. This clears a base-fee rise that occurs *after* the tx is pooled — which re-appears on re-send as "already known", never "underpriced" — by tracking the market instead of over-bumping. The fee is also raised immediately when the node explicitly reports the tx *under-priced* (a `maxGasPriceCap`-clamped +25% bump over our own last fee, which clears the node's ~10% replacement-price rule that a small market delta might miss). Once the fee can no longer be raised (cap reached) or the node reports the fee too high, re-pricing stops and the worker falls back to the last version the node accepted. The loop ends when: the node returns a receipt (success or revert); a definite pre-mempool rejection occurs (intrinsic-gas, block-gas-limit, or fee-too-high with nothing affordable pooled → `evm.tx.failures{reason=send}`); the on-chain nonce is observed to have advanced past ours after a "nonce too low" from a tx we didn't track (the slot is consumed — stop and let the sync loop reconcile); or the round count exceeds the `MAX_SUBMIT_ROUNDS` (= 60) **safety valve** (`evm.tx.failures{reason=abandoned}`), which abandons the request so one stuck bundle cannot wedge the single worker forever — the channel's sync loop re-drives it from fresh state. A transient send error (transport, pool-full) is retried the same way.

### Bundle preview (submit gate)

Before every paid `submitBundle`, the `AccountTransactionSubmitter` worker (in `clpr-relay-evm`) simulates the same `ClprService.submitBundle(bytes32,bytes)` read-only via `eth_call` at send time (with `from` set to the relay's address so the simulated `msg.sender` matches the real transaction). This exercises the contract's full validation chain — endpoint registration, kill-switch, rate limit, channel state, bundle size, the verifier call, running-hash, replay defense, ack monotonicity, reply ordering — without spending gas or persisting state. If the simulation reverts, the worker skips the paid submission (logged at WARN, counted as `sync.bundle.skipped{reason=rejected}`). The preview is always on; it is not gated by configuration. Because the on-chain contract is the single arbiter, the relay does **no** verification of its own — a bundle the chain would accept is submitted, one it would reject is skipped, and a transient failure is simply retried next cycle (nothing is remembered).

### Nonce management

There is **no nonce cache or tracker**. Because the `AccountTransactionSubmitter` worker drives each request to a **definite on-chain verdict** before taking the next — so **at most one transaction per account is ever in flight** (see *Serialization & duplicate suppression* below) — it reads the nonce **once per request** from the account's `latest` (mined) count via `eth_getTransactionCount(from, "latest")` immediately before signing. Because nothing of ours is unconfirmed at that instant, `latest` is exactly the next nonce; every re-send of that request (a fee-bumped replacement) reuses this same nonce, so the nonce is fixed for the life of the request.

`latest` is used deliberately in preference to `pending`: `pending` also counts our own not-yet-mined transaction, so recomputing it mid-flight would yield `nonce + 1` and place a **second** transaction in flight — the exact divergence that let the mempool backlog run away (climbing pending nonce, hundreds of stuck transactions, then `nonce too distant` rejections). With `latest` plus the single-in-flight invariant there is no local counter to drift, no in-flight cap to enforce, and no cross-channel nonce collision to serialise against.

### Serialization & duplicate suppression

Submission for one signing account (EOA) is fully serialised by `AccountTransactionSubmitter` — one instance per `(network, account)`, shared by every channel/service that signs from the same key (obtained from `LocalNetworkAdapter.accountSubmitterFor(signer)`; a channel enqueues onto it through its `ClprChannelHandler.inboundBundleQueue()`, which calls `AccountTransactionSubmitter.submit(serviceAddress, channelId, parsedBundle, peer)`):

- The submitter holds a **bounded FIFO queue per channel** (capacity `LocalNetworkAdapter.PER_CHANNEL_SUBMIT_QUEUE_CAPACITY` = 1024 each) and a **single dedicated worker virtual thread** that drains them **round-robin** — one request per channel per turn. The worker drives one request to a verdict — a mined receipt (success or revert), a definite pre-mempool rejection, a preview skip, an observed on-chain nonce advance, or the `MAX_SUBMIT_ROUNDS` safety valve — re-sending at the same nonce until then, before taking the next. Round-robin is essential: with one shared queue a high-volume channel saturated it and **starved** others (their bundles were always dropped, so they never delivered); per-channel queues guarantee every channel makes progress. The queues hold pending *work* and may exceed one entry; the on-chain **in-flight *transaction* count never does**. This subsumes the old per-*channel* lock and covers channels sharing an EOA in one place.
- **The chain is the duplicate gate; enqueue dedup is only queue hygiene.** The relay's two delivery paths — the pull `ChannelSyncTask` and the push `ClprSyncHandler` — can both construct a bundle for the same channel state, and both re-offer it every poll while it is undelivered. The send-time preview is the authoritative duplicate gate: because the worker is serial and confirm-before-advance, the second of two overlapping bundles is previewed only *after* the first has mined, so it reverts (`ClprReplayDetected` / running-hash mismatch) and is skipped. On top of that, at **enqueue** the submitter suppresses a bundle whose **exact bytes** are already pending in that channel's queue (`evm.tx.queue_deduped`): without it the FIFO fills with identical re-offered copies and a genuinely-new bundle is dropped at enqueue (this was the cause of a real starvation — see below). Only an empty **ACTIVE** initial-state bundle (`next ≤ 1 && acked == 0` with status `ACTIVE`) is dropped without a preview (`sync.bundle.skipped{reason=empty}`; it would revert `EmptyBundle`). A status-only close (`CLOSING`/`DRAINED`/`CLOSED`) rides at those same counters on a never-used channel and is **not** empty — it is always submitted so the lifecycle can advance. There is **no coalescing** within a channel — order is preserved and a valid queued bundle is never displaced. When a *single* channel's queue is full its *new* request is dropped (`evm.tx.queue_dropped`), affecting only that channel.
- The enqueue dedup is **content-exact**, deliberately *not* the counter-keyed fingerprint that was tried and removed. That earlier `(next, acked, status)` fingerprint duplicated the preview for real replays and, keying only on counters, could drop a **PAUSE-recovery** bundle carrying the same counters as the offending one (only its reply content differed) — wedging the channel. Matching on the full bundle bytes cannot make that mistake: a distinct recovery bundle has distinct bytes and is always enqueued; only a byte-identical re-offer of something already queued is suppressed, and the existing entry is kept (a re-offer never displaces it).

## Why polling, not events

The deployed `ClprService` contract does emit Solidity events for some lifecycle changes, but the relay does not subscribe to them. Each channel's `EndpointManifestRefreshTask` (owned by its `ClprChannelHandler`) polls the on-ledger peer endpoint manifest on a timer (one virtual thread per channel) and refreshes the peer cache when its version advances. (Queue/lifecycle state is no longer polled — in the two-phase protocol a peer's own `BundleRequest` states what it needs, and bundles are built ad hoc via `createBundle`.) Reasons the relay polls rather than subscribing to events:

- The relay must support EVM gateways that do not expose reliable WebSocket subscriptions.
- Polling at the configured `commitmentLevel` makes the reorg story trivial — we only act on confirmations.
- State (not events) is the source of truth for the spec — events would be a redundant signal.

If you ever wire event subscriptions, do it as an *optimization* (faster wake-up) layered on top of the existing poll-driven correctness.

## Mapping spec § to code

|         Spec section         |                                                                                                         EVM manifestation                                                                                                         |
|------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| §1.1 ClprLedgerConfiguration | `getLedgerConfiguration()` return decoded in `EvmContractStateReader.decodeLedgerConfiguration`; extracts all 5 throttle fields; `protocolVersion` validated against `RelayProtocol.PROTOCOL_VERSION` per channel at registration |
| §1.2 ClprEndpoint            | `decodeEndpoint`                                                                                                                                                                                                                  |
| §2.1 Channel state machine   | `ClprChannelStatus` enum decoded from struct slot 2; relay enforces status awareness: PAUSED/CLOSING/DRAINED skip outbound syncs; CLOSED rejects inbound bundles — per spec §3.2.3–3.2.5                                          |
| §4 running hash              | enforced on chain; relay only records `sentRunningHash` / `receivedRunningHash` for inclusion in proofs                                                                                                                           |
| §6.4 submitBundle            | `AccountTransactionSubmitter` + `AbiCodec.encodeSubmitBundle`. No endpoint signature arg — removed from contract and relay                                                                                                        |
| §8.3 reorg                   | mitigated via `CommitmentLevel` (use `FINALIZED` for production)                                                                                                                                                                  |
| §8.5 reentrancy              | a contract concern; not the relay's                                                                                                                                                                                               |
