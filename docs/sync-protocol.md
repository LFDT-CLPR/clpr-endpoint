# Peer Sync Protocol

How endpoint-to-endpoint synchronization is implemented over gRPC. Spec §1.5 defines `ClprSyncPayload` (a single `{channel_id, bundle_payload}` message) and leaves the two-phase form unspecified at the wire level; `BundleRequest` / `BundleResponse` are **this implementation's** two-phase extension, pending the spec work. This doc describes the relay's runtime behavior around them.

Prerequisite: `docs/README.md`, `docs/architecture.md`.

## RPCs hosted

The endpoint service is split across two listeners (`ClprGrpcServer`):

- **data plane** (`sync.port`, default 9545) — hosts **`sync`**, a **two-phase bidirectional-streaming** RPC (`stream ClprSyncPayload → stream ClprSyncPayload`). Plaintext by default; **mandatory mTLS** when a leaf certificate is configured, in which case the dialer's client leaf must chain to a CA in the on-chain roster. Peer identity is established at the TLS layer — there is no payload-level endpoint signature.
- **info plane** (`info.port`, default 9546) — **always plaintext**. Hosts **`getLedgerConfiguration`**, which returns chain-verifiable public data.

(The old `discoverEndpoints` RPC has been removed; peer endpoints are read from the on-chain per-Channel endpoint manifest.)

`getLedgerConfiguration(...)` returns this ledger's current `ClprLedgerConfiguration` (protocol version, chain id, service address, throttles, endpoints) read from the on-chain ClprService at `CommitmentLevel.FINALIZED`, together with a QBFT-specific `qbft_payload` wrapper. Used for manual channel setup and by end-to-end tests to obtain the data needed to call `completeChannel` on a peer ledger. The request's optional `service_address` selects which local deployment to serve (empty → the single/primary one; a non-empty unknown address → `NOT_FOUND`; an empty selector with nothing registered yet → `UNIMPLEMENTED`). The `qbft_payload` carries the genesis + current block headers and the EIP-1186 account/storage proofs the peer's QBFT verifier needs to authenticate the configuration against the source ledger's state. Wiring: each channel's `ClprChannelHandler` constructs a `QbftLedgerConfigurationProvider`; `RelayInstance` builds a `GetLedgerConfigurationHandler.Resolver` that maps the request's `service_address` to the matching handler's provider, wrapped in a `GetLedgerConfigurationHandler` that pins `FINALIZED`.

## The two-phase exchange

A bundle is no longer produced on a state-change trigger and cached; it is built **ad hoc** from the peer's request. One `sync` stream carries exactly three messages:

1. **Phase 1 (initiator → responder)** — `ClprSyncPayload{channel_id, bundle_request}`. The `BundleRequest` states what the initiator wants next and what it currently holds:
   - `from_message_id` — the next message id the initiator expects (`receivedMessageId + 1`).
   - `current_status` — the initiator's channel status.
   - `current_trust_anchor_id` — the initiator verifier's current trust anchor, so the responder can decide what validator-set / epoch evidence to include.
   - `current_endpoint_manifest_version` (`uint64`) — the version of the **responder's** endpoint manifest that the initiator currently holds (from the initiator's on-chain `Channel.endpoint_manifest_version`), so the responder re-proves its manifest only when it has advanced.
2. **Phase 1 reply (responder → initiator)** — `ClprSyncPayload{channel_id, bundle_response, bundle_request}`. The responder builds a `bundle_response` answering the initiator's request (via `createBundle`) **and** attaches its own `bundle_request` (from its local chain state) so the initiator can serve it.
3. **Phase 2 (initiator → responder)** — `ClprSyncPayload{channel_id, bundle_response}`. The initiator answers the responder's request with a bundle built by its own `createBundle`. The responder submits it and completes the stream; there is no further reply.

`BundleResponse` carries a single opaque `bundle_payload` (the proof blob, interpreted only by the Channel's verifier contract). An empty payload is a responsive no-op (nothing to convey / nothing to submit).

## `createBundle` — building a bundle for a request

`BundleConstructor.createBundle(channelId, request)` (implemented by `StubBundleConstructor`, `QbftBundleConstructor`, `SeiBundleConstructor`) returns a `BundleWithConstructionMetadata` (the opaque `bundlePayload` plus the local `constructedAtHeight` it was proven against); the sync layer strips the metadata and assembles the wire `BundleResponse` from the payload. It is deterministic for a fixed `(request, local chain height)`, so repeated identical requests can be served from a small `BundleCache` without re-reading the chain. Caching is **not** done by each constructor: production wraps the chain-specific constructor in a `CachedBundleConstructor` decorator (wired in `ClprChannelHandler`) that probes a cheap head read **once** and uses that single probed height to key both the cache lookup and the store — deliberately **not** the delegate's `constructedAtHeight` (an independent chain read that can differ if the head advances mid-build, which would file the entry under a height no later probe reads back — a guaranteed miss). Each `createBundle` call:

1. Reads the local head at the configured `CommitmentLevel` (QBFT proves against the head; Sei backs off `proofLagBlocks` so its CometBFT signed header at H+1/H+2 is committed).
2. Reads `ContractStateReader.readChannelState(channelId, blockTag)`. No record (PENDING / unregistered) → empty payload.
3. **Sender-side trim**: serves messages from `max(1, request.currentReceivedMessageId()+1)` up to `channel.nextMessageId()`.
4. Drives validator-set / epoch catch-up from `request.currentTrustAnchorId()`.
5. Attaches a local **endpoint-manifest proof** when the local manifest is newer than `request.currentEndpointManifestVersion()` — the shared `ManifestProofPolicy.manifestToAttach(stateReader, peerKnownVersion, blockTag)` decision (spec §4.2 Step 1b), so no codec can forget it. The peer-held version comes straight from the request; there is no local per-channel manifest-version cache.

### Progress criteria — when the payload is non-empty

`createBundle` returns an **empty** payload (the "no progress" signal) unless the request surfaces something the peer must act on. On the QBFT path (`QbftBundleConstructor`) a bundle is built when **any** of these holds; otherwise it is suppressed:

- **Pending messages** — one or more messages in the requested range (`request.currentReceivedMessageId()+1 … channel.nextMessageId()`).
- **Ack progress** — `channel.receivedMessageId() > 0`, i.e. there is a received-frontier advance to convey.
- **Trust-anchor catch-up** — the request's `currentTrustAnchorId` is behind our current epoch, so validator-set / epoch headers must ride along (step 4). A pure trust-anchor rotation *does* trigger a bundle here, because the peer states its anchor in the request; there is no local push that could miss it.
- **Lifecycle status** — the channel is `CLOSING`/`DRAINED`, a transition the peer must receive even with no messages, acks, or epoch headers.

None of these holding on an `ACTIVE` channel yields an empty payload. Crucially, a **manifest-only** advance is *not* on this list: the endpoint-manifest proof (step 5) is only *attached to* a bundle that is already being built for one of the reasons above — it is never the sole reason to construct one. `SeiBundleConstructor` applies the same "only send what the peer must act on" principle with its own per-format triggers (see the constructor for exact conditions).

> This supersedes the pre-pull "Bundle Progress Criteria" walkthrough that described a push-based `onStateChanged()` trigger; that path no longer exists.

## Inbound sync flow (responder side)

Path: `ClprSyncService.openSync → ClprSyncHandler.newSession → ClprSyncSession` (one stateful session per stream). `openSync` resolves the authenticated peer once per RPC from the mutual-TLS client leaf (via `PeerEndpointTlsRegistry`) and injects it into the session; `ClprSyncHandler` itself holds only a `ChannelLookup` and the shared metrics counters. Each inbound message resolves its channel by id — `channelLookup.findById(channelId)` returns the per-channel `ClprChannelOps` (that channel's bundle constructor, request constructor, inbound queue, and state reader); an unknown channel replies with an empty payload. PBJ's bidi pipeline drives delivery (it requests unbounded and does not subscribe the session — which nonetheless requests unbounded defensively in `onSubscribe`), so the session reacts in `onNext`:

- **Phase 1** (`handleRequest`): build `bundle_response = ops.bundleConstructor().createBundle(channelId, request.bundleRequest)` (build failures are logged and metered `grpc.sync.errors{reason=build_error}`, not fatal) and attach `bundle_request = ops.bundleRequestConstructor().createBundleRequest()`. Emit one reply.
- **Phase 2** (`handleFollowUp`): hand the peer's `bundle_response` payload to `ops.inboundBundleQueue().offer(payload, peer)`, then complete the stream. That `InboundBundleQueue` implementation (wired in `ClprChannelHandler`) parses the payload with the per-channel `BundlePayloadCodec` (replay-trimmed against the local `receivedMessageId`), gates it on the per-channel `ThrottleEnforcer`, and calls `AccountTransactionSubmitter.submit(...)` — which **enqueues** on the signing account's serial worker and returns; the worker runs a read-only `eth_call` preview at send time and skips (no gas) anything the contract would revert.

A CLOSED (or absent) local channel drops the peer's response. The session is best-effort: a failed submission does not poison the exchange.

## Outbound sync flow (initiator side)

One `ChannelSyncTask` per configured channel, on its own virtual thread, driving that channel's `ClprChannelOps`. Each `executeSyncCycle()`:

1. **Read on-chain state** — `ops.contractStateReader().readChannelState(channelId, commitmentLevel)`. `CLOSED` → self-stop; `PENDING` (or no record) → idle. Any other live status (ACTIVE / PAUSED / CLOSING / DRAINED) keeps syncing.
2. **Pick peer** — `PeerSelector.selectPeer()`; if none (or the selected peer has no service endpoint), skip this cycle.
3. **Phase 1** — send `ClprSyncPayload{channelId, bundleRequest = ops.bundleRequestConstructor().createBundleRequest()}` via `ClprEndpointClient.sync(host, port, phase1, followUpFactory, peerTlsCertificate)` (mutual TLS when the peer publishes a certificate on-chain).
4. **Phase 2 (followUpFactory)** — from the peer's reply, if it carried a `bundle_request`, answer it with `ops.bundleConstructor().createBundle(channelId, peerReply.bundleRequest)`; send that as the phase-2 `bundle_response`, then half-close.
5. **Submit the peer's answer** — the peer's phase-1 reply `bundle_response` answers *our* request; a non-empty payload is handed to `ops.inboundBundleQueue().offer(payload, peer)` (the same parse / replay-trim / throttle / enqueue path as the responder's phase 2). An empty payload is a no-op.
6. **Record peer outcome** (`PeerSelector.recordSuccess` / `recordFailure`) and sleep `syncIntervalMs` (`ChannelSyncTask.DEFAULT_INTERVAL_MS` = 1000ms, uniform per channel).

The relay initiates every cycle (a cheap phase-1 request); whether a *bundle* is delivered is decided by `createBundle`'s suppress logic (an empty payload = no progress).

## Endpoint manifest

Each endpoint has its own `ClprEndpointManifest` (its admitted peer endpoints + service address) recorded on-chain. Two flows keep manifests current:

- **Sending our manifest**: when a peer's request reports (`current_endpoint_manifest_version`) an older version of our manifest than we hold locally, `createBundle` attaches a fresh manifest proof (`ManifestProofPolicy`), in each proof format's own wire shape (QBFT: an EIP-1186 storage proof of the manifest-commitment slot; Sei: the ICS-23 analog). Skipped when the peer is already current or our manifest has no service address yet.
- **Receiving a peer's manifest**: the peer's manifest is verified and stored on our chain. `EndpointManifestRefreshTask` polls the on-ledger peer endpoint manifest and, when its `version` advances (or on first observation), replaces the channel's `PeerEndpointCache` and the mTLS trust registry wholesale (spec §2.4.2). It reads only the manifest — no channel/queue state.

There is **no** `PeerManifestVersionCache`: the version each peer holds of our manifest arrives in its `BundleRequest`, and the version we hold of a peer's manifest is our own `Channel.endpoint_manifest_version` (sent in the request we build via `BundleRequestConstructor.createBundleRequest()`).

## Peer selection

`PeerSelector` tracks `PeerStats` per peer (`accountId`-keyed): `successCount`, consecutive `failureCount`, and an exponential-backoff deadline. Selection each cycle excludes peers currently inside their backoff window, then picks one by **weighted random** among the rest, weight = `successCount + 1.0` (a never-succeeded peer still has weight 1.0, so it is always eligible; peers with more past successes are favoured). If *every* peer is backed off it falls back to uniform-random over the full list rather than skipping the cycle. `recordFailure` applies exponential backoff (1s, 2s, 4s, … capped at 60s); `recordSuccess` increments `successCount`, resets `failureCount`, and clears the backoff. Source: `clpr-relay-sync/.../PeerSelector.java`, `PeerStats.java`. The candidate set comes from `PeerEndpointCache`, seeded at startup from the on-chain per-Channel peer endpoint manifest and replaced wholesale by `EndpointManifestRefreshTask` on a manifest-version advance (above). `PeerSelector` reads the cache live; `PeerStats` for a departed peer is left in place (never selected again).

## Proof handling — current state

- `StubBundleConstructor` builds a placeholder payload (`"CLPRSTUB"`-prefixed `ClprBundleContent`) from local channel state — no real crypto — so the two-phase flow can be exercised end-to-end against the test `StubClprVerifier`; the inbound side parses it with `StubProofCodec`.
- `QbftBundleConstructor` / `SeiBundleConstructor` build real EIP-1186 / ICS-23 proofs against the local `ClprService` storage roots, authenticated on-chain by the Channel's `verifier` contract; the inbound side parses them with `QbftProofCodec` / `SeiProofCodec` (a peer Hiero ledger's proofs parse with `HieroProofCodec`).
- `EthereumPosBundleConstructor` builds an Ethereum post-Merge (proof-of-stake) bundle: a sync-committee light-client attested header + BLS finality (`SyncAggregate` + non-signer proofs) sourced from a beacon (consensus-layer) node via `BeaconApiClient`, plus the EIP-1186 account/storage proofs at the attested execution block — verified by the Hiero-native `EthereumSyncCommitteeProofVerifier`. It is **stateless**: the remote verifier's sync-committee period comes from `request.currentTrustAnchorId()`, and it advances the anchor one period per exchange. The inbound side parses it with `EthereumPosProofCodec` (verbatim, like `HieroProofCodec` — the light-client proof is not trimmable).

The construct side is chosen from the local network's `ProofType` and the parse side from the channel's `peerProofType`; both are exposed per channel through `ClprChannelOps` (`bundleConstructor()` outbound, `inboundBundleQueue()` inbound, the codec resolved in `ClprChannelHandler.codecFor`). `ProofType.Stub` is valid on both sides and used only for integration tests.

The wire format of `bundle_payload` is opaque to `clpr-relay-sync` and `clpr-relay-grpc-server`; the contract layer (`AccountTransactionSubmitter`, `submitBundle(bytes32,bytes)`) treats it as opaque bytes dispatched to the configured verifier.

## Endpoint signing

The relay does **not** sign or verify a per-payload endpoint signature; peer identity is the mandatory-mTLS roster-CA check on the data plane. `ClprSyncPayload` carries only `channel_id`, `bundle_request`, and `bundle_response`. (A signing key is still used — the per-service `clprServices[].defaultSigningPrivateKeyHex` or a per-channel override — but only to sign the EVM `submitBundle` transaction via `EthSigner`; see `evm-integration.md`.)

## Concurrency notes

- `PeerEndpointCache` is thread-safe (concurrent map); `BundleCache` (owned by the `CachedBundleConstructor` decorator) is a synchronized, bounded LRU that evicts the least-recently-used entry on overflow.
- `PeerStats` mutations through `PeerSelector` are synchronized.
- `ChannelSyncTask.running` is volatile; `stop()` flips it and the loop exits at the next iteration boundary.
- Each `ClprSyncSession` is single-use per stream; its phase transitions are `synchronized`.
- Each channel's `EndpointManifestRefreshTask` shares its `PeerEndpointCache` with that channel's `PeerSelector` for endpoint refresh.
