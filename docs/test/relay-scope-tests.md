# CLPR EVM Relay — Spec Conformance Test Cases

Scope: **relay-side** obligations only. Each item is an observable behavior derived from the [CLPR protocol spec](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md).

Out of scope for this catalog:

- ClprService / verifier / on-chain contract behavior, unless a relay
  obligation depends on it.
- Hiero-endpoint behavior.
- Configuration combinations, defaults, and misconfig detection.
- Extreme boundary values (size caps, popcount edges, etc.).
- Chain reorg, retry, and operational-resilience corner cases.

## Section index

1. [Contract state observation](#1-contract-state-observation)
2. [Bundle construction](#2-bundle-construction)
3. [Trust-anchor rotation obligations](#3-trust-anchor-rotation-obligations)
4. [Peer manifest handling](#4-peer-manifest-handling)
5. [Bundle submission](#5-bundle-submission)
6. [Bundle processing (receive-side)](#6-bundle-processing-receive-side)
7. [Channel status handling](#7-channel-status-handling)
8. [Sync protocol (gRPC)](#8-sync-protocol-grpc)
9. [Peer selection & sync scheduling](#9-peer-selection--sync-scheduling)

---

## 1. Contract state observation

Scope: what the relay reads from the local `ClprService` contract and how
it must react to state changes produced on-chain by any submitter.

1. The relay reads the local `Channel` record — `status`, message-id and
   running-hash fields, `trust_anchor_id`, `endpoint_manifest_version` —
   at the start of each sync cycle so bundle construction is grounded in
   the current on-chain state
   ([spec §2.1](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#21-channel)).
2. The relay routes each connection's `submitBundle` transactions and
   view reads to the `ClprService` address it was configured with for
   that connection's ledger; a relay configured with multiple
   `clprServices[]` MUST NOT cross wires between them.
3. The relay observes on-chain advancements of `Channel.trust_anchor_id`
   and `Channel.endpoint_manifest_version` produced by other submitters
   and adjusts its next bundle accordingly (drops the no-longer-needed
   rotation payload; refreshes its manifest cache).
4. The relay observes on-chain advancements of `Channel.status`
   (produced by any submitter, including inbound bundles carrying
   `metadata.status ∈ {CLOSING, DRAINED, CLOSED}`); per-status reactions
   live in section #7
   ([spec §2.1.1](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#211-channel-status-transitions),
   [§4.2 Step 5a](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#42-bundle-verification-algorithm)).
5. When the local CLPR Service has lazily enqueued a `ConfigUpdate`
   Control Message on the outbound queue (triggered by the next
   `submitBundle` or `sendMessage` after
   `current_config.consensus_timestamp` advances past
   `Channel.last_config_timestamp`), the relay includes it in the next
   outbound bundle in queue order, without special-casing it relative to
   Data or Response Messages
   ([spec §1.3](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#13-control-messages),
   [§4.2 Step 5c](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#42-bundle-verification-algorithm),
   [§4.3 Step 1a](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#43-message-enqueue-algorithm)).

---

## 2. Bundle construction

Scope: relay obligations when assembling the `bundle_payload` from its own
ledger's state, to be delivered to a peer via gRPC sync (the peer submits
it to their `ClprService`). Trust-anchor rotation attachment mechanics
are deferred to Section #3.

Every case below must hold for each configured `peerProofTypes` variant —
bundle construction is proof-type-specific in payload shape and
inclusion/state-proof mechanics, and a case verified for one proof type
does not imply the others.

1. The bundle is assembled from a mutually consistent point in the source
   ledger's state — every read used to build it comes from a single
   snapshot the peer's verifier can reconcile against one anchor.
2. The first message included has ID =
   `BundleRequest.current_received_message_id + 1`; if that ID does not
   yet exist in the local queue (≥ local `next_message_id`), the relay
   falls back to `acked_message_id + 1`
   ([spec §1.5](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#15-sync-protocol)).
3. Message IDs in the bundle are contiguous and ascending, with no gaps
   ([spec §4.2 Step 3](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#42-bundle-verification-algorithm)).
4. The bundle honors the **peer's** advertised `max_messages_per_bundle`
   (cached on the local Channel as `peerThrottles`). No source-contract
   check enforces this at enqueue — it is a per-bundle cap, so only the
   relay can honor it at construction. A bundle exceeding the cap is
   rejected wholesale by the destination at
   [§4.2 Step 2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#42-bundle-verification-algorithm)
   (see also the §1.5 `ClprQueueMetadata` comment noting "the bundle
   may be a prefix limited by `max_messages_per_bundle`"). Throttles
   are ledger-wide
   (§1.1) so different peers may advertise different values.
5. On the happy path, no relay-side check on `max_message_payload_bytes`
   is required: the source contract enforces the peer's advertised
   limit at `sendMessage` time
   ([§4.3 Step 4](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#43-message-enqueue-algorithm))
   using `peer_config.throttles`, so no oversized message reaches the
   queue. Edge case: if the peer lowers `max_message_payload_bytes` via
   `ConfigUpdate` after a message is already queued, that message would
   now be rejected by the destination at
   [§4.2 Step 2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#42-bundle-verification-algorithm)
   — and the relay cannot skip it without breaking §4.2 Step 3
   contiguity, so the Channel wedges pending admin redaction or the
   peer restoring the prior limit.
6. The serialized `ClprSyncPayload` honors the **peer's** advertised
   `max_sync_bytes` (from `peerThrottles`); the peer will reject on the
   wire otherwise
   ([spec §1.5](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#15-sync-protocol),
   [§7](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#7-configuration-parameters)).
7. `ClprQueueMetadata.sent_running_hash` covers the last message
   *included in this bundle*, not the tail of the whole queue
   ([spec §4.1](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#41-running-hash-computation),
   [§4.2 Step 4](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#42-bundle-verification-algorithm)).
8. `ClprQueueMetadata.next_message_id` equals
   `last_included_message_id + 1`, not the current on-chain
   `Channel.next_message_id`
   ([spec §4.2 Step 3](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#42-bundle-verification-algorithm)
   consistency).
9. `ClprQueueMetadata.received_message_id` and `.received_running_hash`
   reflect the local Channel's current inbound state so the peer can
   advance their `acked_message_id`
   ([spec §1.5](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#15-sync-protocol)
   `ClprQueueMetadata` field definitions;
   [§4.2 Step 5](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#42-bundle-verification-algorithm)
   for the receiver's use).
10. `ClprQueueMetadata.status` reflects the local Channel's current
    status so the peer can trigger their own
    `CLOSING`/`DRAINED`/`CLOSED` transitions
    ([spec §4.2 Step 5a](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#42-bundle-verification-algorithm)).
11. `ClprQueueMetadata.trust_anchor_id` reflects the local Channel's
    current `trust_anchor_id`
    ([spec §3.2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#32-endpoint-trust-anchor-rotation-obligations)).
12. `ClprQueueMetadata.endpoint_manifest_version` reflects the local
    Channel's current `endpoint_manifest_version` so the peer sees when
    the manifest has advanced
    ([spec §1.5](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#15-sync-protocol)
    field def,
    [§2.1](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#21-channel)
    Channel state).
13. When `BundleRequest.current_endpoint_manifest_version` is less than
    the local `endpoint_manifest_version`, the relay attaches the full
    local `ClprEndpointManifest` bytes plus its inclusion proof so the
    peer's verifier can adopt it
    ([spec §2.4.2 Refresh](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#242-peer-manifest-on-ledger-per-channel),
    [§4.2 Step 1b](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#42-bundle-verification-algorithm)).
14. The relay pre-checks the five Bundle Progress Criteria (new
    messages, trust-anchor advancement, acknowledgement progress,
    channel state transition, endpoint manifest advancement) against the
    peer's cached view and suppresses the bundle when none holds
    ([spec §2.1.2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#212-bundle-progress-criteria)).
15. The bundle is assembled from the current on-chain Channel state and
    the current peer `BundleRequest` — not a stale snapshot cached
    across cycles. Successive cycles reflect state advancements made by
    other submitters or by peer acknowledgment (messages already
    processed are not re-included; control payloads already applied on
    the peer's ledger are not re-attached).

---

## 3. Trust-anchor rotation obligations

Scope: what the relay must do to keep the peer's `Channel.trust_anchor`
(for this relay's source chain) advanced. The relay-as-source constructs
the transition bundles; the peer-as-destination submits them to their
`ClprService`.

Every case below must hold for each configured `peerProofTypes` variant —
transition-bundle shape (authority commitment, state proof, verification
mechanics) is proof-type-specific.

1. Given a peer whose `trust_anchor_id` names any historical authority
   X in the source chain's rotation record, the relay produces a
   transition bundle advancing the peer's anchor from X to its immediate
   successor — not skipping ahead and not restarting the sequence
   ([spec §3.2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#32-endpoint-trust-anchor-rotation-obligations)).
2. Each sync cycle, the relay reads `trust_anchor_id` from the most
   recently received sync payload and identifies which of its source
   chain's signing authorities is currently installed on the peer's
   ledger
   ([spec §3.2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#32-endpoint-trust-anchor-rotation-obligations)).
3. The relay proactively constructs a transition bundle when it detects
   an authority rotation on its own source chain. For **single-authority
   encodings** the transition bundle MUST be delivered before any
   application bundle proven under the new authority. For **window
   encodings** the window-advance MAY be batched into a regular
   application bundle at any point while the current window still covers
   all in-flight proofs
   ([spec §3.2 — Timing depends on trust anchor encoding](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#timing-depends-on-trust-anchor-encoding)).
4. The relay reactively constructs a transition bundle when it observes
   — from a received sync payload or after a restart — that the peer's
   `trust_anchor_id` names an authority under which the relay cannot
   produce proofs
   ([spec §3.2 — Triggering, "Reactive"](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#triggering)).
5. When the peer's `trust_anchor_id` matches the current authority on
   the relay's source chain, the relay constructs an application bundle
   without attaching any rotation payload
   ([spec §3.2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#32-endpoint-trust-anchor-rotation-obligations)).
6. A transition bundle's `bundle_payload` is verifiable by an authority
   currently encoded in the peer's `Channel.trust_anchor`; the relay
   never builds a transition bundle proven under an authority the peer
   has not yet adopted
   ([spec §3.2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#32-endpoint-trust-anchor-rotation-obligations)).
7. A transition bundle embeds a state-proven commitment to the new (or
   additional) authority so the peer's verifier returns non-empty
   `new_trust_anchor` / `new_trust_anchor_id`
   ([spec §3.2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#32-endpoint-trust-anchor-rotation-obligations)).
8. A trust-only transition bundle (zero application messages) is
   delivered without waiting to be batched with application messages
   ([spec §3.2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#32-endpoint-trust-anchor-rotation-obligations),
   [§2.1.2 Criterion 2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#212-bundle-progress-criteria)).
9. The relay maintains, per Channel, a local ordered sequence of the
   `trust_anchor_id` values it has observed on its own source chain —
   which authority succeeded which. This sequence determines the order
   of transition bundles produced under item #4
   ([spec §3.2 — Endpoint local record](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#32-endpoint-trust-anchor-rotation-obligations)).
10. When the peer's `trust_anchor_id` names an authority N rotations
    behind the relay's current source-chain authority, the relay
    produces N consecutive transition bundles (or, if the local verifier
    supports multi-rotation batching, a single bundle that walks the
    chain internally). Each intermediate bundle is verifiable under the
    authority installed by the previous transition
    ([spec §3.2 — Catch-up across multiple missed rotations](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#catch-up-across-multiple-missed-rotations)).
11. When the `trust_anchor_id` from a received sync payload is not in
    the relay's local sequence, the relay reads the peer chain's
    `Channel.trust_anchor` bytes on-demand and caches them by
    identifier before deciding whether to produce a transition bundle
    ([spec §3.2 step 2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#32-endpoint-trust-anchor-rotation-obligations)).

---

## 4. Peer manifest handling

Scope: relay obligations concerning the peer endpoint manifest cached on
the local `Channel.endpoint_manifest`. Detection of on-chain version
advancement is covered in Section #1.

1. For outbound gRPC sync, the relay draws its peer list exclusively
   from the peer manifest read on the local Channel — not from static
   config, DNS, service discovery, or any other out-of-band directory
   ([spec §2.4.2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#242-peer-manifest-on-ledger-per-channel),
   [§5.2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#52-endpoint-discovery)).
2. When `Channel.endpoint_manifest_version` advances on-chain, the
   relay reloads the peer manifest and updates its working set to match
   the new state: newly-added endpoints become eligible for outbound
   sync; removed endpoints are no longer selected for outbound sync;
   endpoints whose address changed use the new address on subsequent
   connections — all without requiring a restart
   ([spec §2.4.2 Refresh](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#242-peer-manifest-on-ledger-per-channel)).
3. A peer manifest with `version ≥ 1` and an empty endpoint list is
   accepted as a valid state — the relay does not treat this as an
   error or refuse to work on the Channel; outbound sync is simply idle
   (no peers to contact) until the manifest is repopulated
   ([spec §2.4.2 Empty manifest](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#242-peer-manifest-on-ledger-per-channel)).
4. A Channel whose peer manifest version is 0 (uninitialized, PENDING)
   is not sync'd against — the relay attempts no peer selection until
   `completeChannel` has populated the manifest to version ≥ 1
   ([spec §2.1](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#21-channel)
   initial values,
   [§2.4.2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#242-peer-manifest-on-ledger-per-channel)
   population at `completeChannel`).

---

## 5. Bundle submission

Scope: relay obligations when calling `submitBundle(bytes32, bytes)` on
the local `ClprService` for a bundle received from a peer via gRPC.
Bundle-content selection is covered in slice #2; state re-read after
submission is covered in section #1. Gas/fee/nonce mechanics are treated
as operational and not enumerated here.

1. **Endpoint hygiene (not spec-mandated).** Before submitting, the
   relay pre-verifies the bundle via `eth_call submitBundle(...)`
   against the configured `ClprService` and skips the on-chain broadcast
   when pre-verify reverts, so no gas is paid on definitively rejected
   bundles.
   [§2.1.2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#212-bundle-progress-criteria)
   defines the required in-endpoint progress pre-check (evaluated
   against the cached destination Channel view).
2. The relay broadcasts the exact `bundle_payload` bytes that
   pre-verified — no mutation of the payload between pre-verify and the
   signed transaction.
3. When an on-chain `submitBundle` transaction reverts (state changed
   between pre-verify and mining — most commonly `NoProgress` at
   §4.2 Step 1a, when all five Bundle Progress Criteria failed on chain
   despite passing the endpoint pre-check), the relay refetches the
   local Channel state and re-drives from that fresh state on the next
   cycle, rather than blindly resubmitting the same transaction
   ([§4.2 Step 1a](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#42-bundle-verification-algorithm),
   [§2.1.2 Service post-check](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#212-bundle-progress-criteria)).
4. The relay serializes `submitBundle` transactions per signing account
   — at most one submission is in flight for a given signing key at any
   time.
5. The relay does not require its signing account to correspond to a
   `registrant_account` in the local endpoint manifest — submission is
   permissionless, and no internal self-check gates the call on
   manifest membership
   ([spec §2.4.1 note](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#241-local-endpoint-manifest),
   [§6.5](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#65-endpoint-management)).

---

## 6. Bundle processing (receive-side)

Scope: throttle limits and defensive checks the relay applies when
RECEIVING a bundle from a peer via gRPC, before handing off to section #5
(Submission). Outbound-side enforcement is in section #2. Peer-manifest
throttle enforcement is in section #4.

1. The relay rejects an inbound `ClprSyncPayload` whose `channel_id`
   length is not exactly 32 bytes; the payload is discarded before any
   further processing
   ([spec §1.5](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#15-sync-protocol)).
2. The relay rejects an inbound `ClprSyncPayload` whose `channel_id`
   does not name a Channel this relay is configured to serve; unknown
   channel_ids are dropped before pre-verify
   ([§1.5](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#15-sync-protocol)
   requires that a stream be scoped to one Channel; discarding payloads
   for unknown Channels is a defensive extension of that scope
   requirement).
3. Within a single sync stream, the relay rejects any subsequent
   `ClprSyncPayload` whose `channel_id` differs from the one first seen
   on that stream — each stream is scoped to a single Channel
   ([spec §1.5](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#15-sync-protocol)).
4. The relay rejects an inbound `ClprSyncPayload` whose serialized size
   exceeds the **local Channel's** own `max_sync_bytes` (the
   destination-side check — the relay's own ledger's throttle applies
   here, not the sender's); the payload is discarded before pre-verify
   ([spec §7](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#7-configuration-parameters)
   — "Endpoints MUST reject messages exceeding this limit";
   [§1.5](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#15-sync-protocol)).
5. When a received `bundle_payload` decodes to more messages than the
   **local Channel's own** `max_messages_per_bundle`, the relay skips
   submission — the on-chain check at
   [§4.2 Step 2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#42-bundle-verification-algorithm)
   would reject anyway, so skipping saves the pre-verify round-trip
   (defensive).
6. When a received `bundle_payload` decodes to any message whose
   serialized payload exceeds the **local Channel's own**
   `max_message_payload_bytes`, the relay skips submission — same
   rationale
   ([§4.2 Step 2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#42-bundle-verification-algorithm),
   defensive).
7. **Endpoint hygiene (not spec-mandated).** Before submitting, the
   relay trims messages whose IDs are ≤ the current local
   `Channel.received_message_id` and re-encodes the retained suffix.
   This handles the inherent race between the peer's bundle
   construction (which uses its cached view of our
   `received_message_id`) and the on-chain submission moment: another
   submitter may advance our `received_message_id` in the interim, and
   the on-chain replay-defense check would otherwise reject the entire
   bundle
   ([§4.2 Step 3](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#42-bundle-verification-algorithm)
   defines the validity rule but does not spell out this endpoint
   trimming obligation).
8. When a received `bundle_payload` decodes to a `ClprControlMessage`
   whose `oneof payload` variant is unset (an unknown control message
   type from a newer protocol version), the relay treats the entire
   bundle as invalid and does not submit it
   ([spec §1.3](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#13-control-messages)
   — "MUST reject the entire bundle").
9. The relay MUST reject inbound `ClprSyncPayload` messages containing
   unrecognized protobuf fields, unknown message types, or malformed
   metadata; no silent field-drop is permitted
   ([spec §8.1](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#81-protocol-strictness)).

---

## 7. Channel status handling

Scope: relay behavior across every `ClprChannelStatus` value — the
`PENDING`/`CLOSED` bookends where sync is idle, the intermediate statuses
where sync continues, the close-notification obligation, and Response
Message passthrough (folded in from the former ORD section).

1. In `PENDING`, the relay performs no sync work — the Channel is not
   yet operational; peer selection and bundle construction are both
   idle for the Channel
   ([spec §2.1.1](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#211-channel-status-transitions)).
2. In `PAUSED`, the relay continues sync cycles — outbound bundles
   still deliver so acks and queued messages flow; inbound bundles are
   still submitted, and a correctly-ordered response bundle triggers
   the on-chain `PAUSED`→`ACTIVE` recovery via the ClprService
   ([spec §2.1.1 PAUSED](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#211-channel-status-transitions),
   [§4.5 recovery](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#45-response-ordering-verification)).
3. In `CLOSING`, the relay continues sync cycles — outbound bundles
   still deliver so the queue drains; inbound bundles are still
   submitted so peer Data Messages get dispatched and generate Response
   Messages
   ([spec §2.1.1 CLOSING](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#211-channel-status-transitions)).
4. In `DRAINED`, the relay continues sync cycles — inbound bundles are
   still submitted so the peer can still drain its own queue, and
   outbound Response Messages generated during `CLOSING` drain out
   ([spec §2.1.1 DRAINED](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#211-channel-status-transitions)).
5. When the local Channel transitions to `CLOSED`, the relay constructs
   and delivers one final close-notification bundle (a normal bundle
   with `ClprQueueMetadata.status = CLOSED` and the final
   `received_message_id`, `next_message_id`, and running-hash values),
   retrying until the remote either accepts it or rejects it because
   the peer's Channel is already `CLOSED`
   ([spec §3.4](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#34-endpoint-close-notification-obligation),
   [§2.1.1 CLOSED terminal](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#211-channel-status-transitions)).
6. In `CLOSED`, once the close-notification obligation (item #5) has
   been satisfied, the relay ceases all sync activity for the Channel —
   no peer selection, no outbound bundles, no inbound submission
   ([spec §2.1.1 CLOSED terminal](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#211-channel-status-transitions)).
7. The relay does not decide status transitions itself — status is
   derived exclusively from on-chain Channel reads. Local ordering
   violations, network failures, or peer misbehavior do not cause the
   relay to change its local view of Channel status independently of
   the on-chain record
   ([spec §2.1.1](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#211-channel-status-transitions)).
8. Within the ID range being included in an outbound bundle, the relay
   preserves the queue order and completeness of Response Messages —
   it does not cherry-pick which responses to include or reorder them
   relative to interleaved Data or Control messages (range-based
   tailoring per the peer's `BundleRequest` is a separate concern,
   covered in section #2 item #2). This outbound obligation is inferred
   from the receive-side ordering rule in
   [spec §4.5](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#45-response-ordering-verification)
   (mismatch pauses the destination Channel) — the spec does not state
   it directly on the sender.
9. When retrying the close-notification bundle (item #5), the relay
   treats a revert-because-`CLOSED` from the remote as **acceptance of
   the terminal state** and ceases sync activity; it does **not**
   conflate this with a `NoProgress` revert — §3.4 states that a
   close-notification carrying no new information is rejected as
   `CLOSED`, not as `NoProgress`
   ([spec §3.4 — Remote already CLOSED](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#34-endpoint-close-notification-obligation)).

---

## 8. Sync protocol (gRPC)

Scope: relay obligations for the `ClprEndpointService.sync` bidirectional
gRPC stream and payload framing. Peer selection is covered in section #9.

1. The relay exposes `ClprEndpointService.sync` as a bidirectional
   streaming RPC
   ([spec §1.5 gRPC Endpoint Service](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#15-sync-protocol)).
2. In each outbound `ClprSyncPayload` where the relay describes its own
   state to the peer, it populates `BundleRequest` with
   `current_received_message_id`, `current_status`,
   `current_trust_anchor_id`, and `current_endpoint_manifest_version`
   read from the local Channel
   ([spec §1.5 BundleRequest](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#15-sync-protocol)).
3. In each outbound `ClprSyncPayload` where the relay answers the
   peer's `BundleRequest`, it populates `BundleResponse.bundle_payload`
   shaped by the peer's requested state; content shape obligations
   live in section #2 and #3
   ([spec §1.5 BundleResponse](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#15-sync-protocol)).
4. A `ClprSyncPayload` with both `bundle_request` and `bundle_response`
   absent closes the stream — the relay honors this signal from a peer
   and stops sending on that stream
   ([spec §1.5](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#15-sync-protocol)).
5. The relay configures its gRPC transport layer's max message size
   (both send and receive directions) to be ≥ the local Channel's
   `max_sync_bytes`, so legitimate payloads up to that limit can flow
   through the transport before reaching application-layer checks
   ([spec §1.5](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#15-sync-protocol)
   — "Implementations MUST configure gRPC max message sizes to
   accommodate `max_sync_bytes`").

---

## 9. Peer selection & sync scheduling

Scope: relay obligations for choosing which peer to sync with and when
to sync. The spec provides only a SHOULD-level directive on peer
selection ([§5.2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#52-endpoint-discovery));
the rest is operator policy. Peer manifest consumption is covered in
section #4.

1. When the peer manifest for a Channel contains one or more endpoints
   and the Channel is in a sync-eligible status (`ACTIVE`, `PAUSED`,
   `CLOSING`, `DRAINED`), the relay initiates sync cycles against those
   peers — no eligible Channel with viable peers sits idle indefinitely
   ([spec §5.2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#52-endpoint-discovery),
   [§2.1.1](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#211-channel-status-transitions)).
2. When multiple peers are present in the peer manifest, the relay
   distributes outbound sync attempts across them across cycles — no
   single peer is monopolized or perpetually starved
   ([spec §5.2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#52-endpoint-discovery)).
3. The relay SHOULD prefer peers that reciprocate (return useful
   bundles) over peers that only consume; peers deemed non-reciprocating
   are deprioritized in subsequent selection cycles
   ([spec §5.2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#52-endpoint-discovery)).
4. A peer that is non-responsive across recent selection attempts
   (failed handshakes, timed-out streams, or persistently empty
   bundles) is deprioritized relative to healthy peers in subsequent
   selection cycles (operator-policy extension of
   [§5.2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#52-endpoint-discovery)'s
   reciprocation SHOULD — spec text only covers persistently empty
   bundles).
5. A previously deprioritized peer that resumes responding
   successfully has its priority restored in subsequent selection
   cycles — deprioritization is not permanent. **Validate against
   actual `PeerSelector` scoring; current implementation may hold a
   grudge.**
6. When every manifest peer is currently failing, the relay does not
   wedge the Channel — it continues attempts with backoff and resumes
   normal cadence as soon as any peer recovers (operator-policy
   extension; not spelled out in
   [§5.2](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#52-endpoint-discovery)).
7. Peer selection incorporates a randomization component so that under
   a stable manifest the same peer pair does not persistently
   monopolize a Channel across cycles
   ([spec §8.4](https://github.com/hiero-hackers/CLPR-spec/blob/main/clpr-service-spec.md#84-endpoint-sybil-resistance)).

## Comparing against the implementation

### Summary of concrete gaps

1. **Section 8 item 1** — two-way sync still on its way.
2. **Section 7 item 5** — no final close-notification bundle; relay stops on CLOSED.
3. **Section 6 item 1** — `channel_id` length is not validated.
4. **Section 2 item 8** — `nextMessageId` uses on-chain frontier, not `last_included + 1`.
5. **Section 2 item 14 / Bundle Progress Criteria** — in the two-phase
   pull model a bundle is built on demand inside
   `BundleConstructor.createBundle(channelId, request)` from the peer's
   `BundleRequest`; there is no push-based `onStateChanged` pre-check any
   more (canonical description: `docs/sync-protocol.md` → "Progress
   criteria — when the payload is non-empty"). Measured against §2.1.2's
   five criteria:
   * **Criterion 2 (trust-anchor advancement)** — now honored on both
     paths, and the move to pull resolved the old gap. The peer states
     its `current_trust_anchor_id` in the request, so when it is behind,
     catch-up evidence is included and the payload is non-empty (QBFT
     epoch headers, `QbftBundleConstructor.java:267`; Sei validator-set
     updates / rotation, `SeiBundleConstructor.java:346-347`). The former
     push path could miss this because its change check excluded the
     trust anchor; a request-driven build cannot.
   * **Criterion 3 (ack progress)** is over-permissive — both
     constructors treat `channel.receivedMessageId() > 0` as sufficient
     ack progress (`QbftBundleConstructor.java:268`;
     `SeiBundleConstructor.java:348`), rather than "advanced since the
     peer's last request". The responder holds no per-peer prior value,
     so any non-zero ack builds a metadata-only bundle.
   * **Criterion 4 (channel-state transition, QBFT path)** — the
     `conveysLifecycle` flag treats only `CLOSING` / `DRAINED` as a
     lifecycle-carrying status, so the `DRAINED → CLOSED` sub-transition
     never triggers construction on its own
     (`QbftBundleConstructor.java:265-269`).
   * **Criterion 4 (channel-state transition, Sei path)** — no
     lifecycle-status term exists in the skip condition, so the Sei
     constructor never fires solely on a channel-state transition
     (`SeiBundleConstructor.java:345-348`).
   * **Criterion 5 (manifest advancement)** — a manifest-only advance
     never causes construction to fire (both paths). The endpoint-manifest
     proof is attached only *after* the skip check, riding along an
     already-triggered bundle — never the sole reason to build one
     (skip check `QbftBundleConstructor.java:266-276`, manifest step
     `:362`; skip check `SeiBundleConstructor.java:345-352`, manifest step
     `:461`).
6. **Section 4** — no startup self-check of the listen address vs the local manifest (it might be difficult as the listener might hide behind a LB).
7. **Section 2 items 4, 5, 6** — outbound bundle construction does not
   honor the peer's advertised throttles. `peerThrottles` is decoded
   (`EvmContractStateReader.java:232`) and stored on the channel state but
   never consumed; outbound message-count / size limits come from the
   local ledger configuration instead — the per-format
   `maxMessagesPerBundle()` passed to each constructor in
   `ClprChannelHandler.java` (QBFT `:211`, Sei `:229`, beacon `:264`).
