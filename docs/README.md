# CLPR EVM Relay — Documentation Index

This is the **always-load** entry point. Read it first; load deeper docs only when the task demands.

## What this project is

A standalone Java 25 process that implements a **CLPR endpoint** for an EVM-compatible chain. It:
- reads on-chain state from a deployed `ClprService` Solidity contract (via JSON-RPC `eth_call`),
- submits `submitBundle(bytes32,bytes)` transactions on behalf of the local ledger,
- exchanges sync payloads with peer endpoints over gRPC.

It is the EVM-side counterpart to a Hiero-native CLPR endpoint. Both implement the same wire protocol; this project does not implement the Hiero side.

## Required prior reading

CLPR background lives in a separate repo. Do not duplicate it here:

- `../clpr-spec/clpr-service.md` — design rationale, glossary, trust model, role definitions.
- `../clpr-spec/clpr-service-spec.md` — normative protocol: protobuf wire formats, on-ledger state model, pseudo-API, security considerations. Platform-neutral.

When this documentation says "see spec §X" without qualification, it means `clpr-service-spec.md`.

## Documentation map (load on demand)

|                         If your task is…                         |              Read               |
|------------------------------------------------------------------|---------------------------------|
| Onboarding / "where does X live?"                                | `architecture.md`               |
| Calling the CLPR contract, ABI, gas, signing, dedup, state reads | `evm-integration.md`            |
| Peer sync, gRPC server/client, proof flow, peer selection        | `sync-protocol.md`              |
| Running the relay, YAML config, integration tests, deployment    | `configuration.md`              |
| Multi-chain support, ProofType, adding a new chain type          | `multi-chain-design-changes.md` |

Each doc is self-contained for its topic and assumes you have read this index plus the two spec docs above.

## Glossary deltas (terms used here, not in spec)

- **Relay** — this process. The spec calls it an "endpoint operator's process"; we use "relay".
- **Submitter** — a `InboundBundleQueue` implementation. Wraps the JSON-RPC call that lands `submitBundle` on chain.
- **Listener** — a poll-based watcher of the on-chain peer endpoint manifest (`EndpointManifestRefreshTask`, one per channel, owned by its `ClprChannelHandler`). The EVM CLPR contract emits no events the relay subscribes to; a manifest-version advance is detected by polling and refreshes the peer cache.
- **Stub proof** — the test-only `ProofType.Stub`: `StubBundleConstructor` builds `"CLPRSTUB" || protobuf(ClprBundleContent)` bundles (no real crypto) that the `StubClprVerifier` contract accepts, and `StubProofCodec` parses them. Used by the integration suite to exercise the two-phase flow end-to-end without a real consensus proof. Real proofs (QBFT, Sei/CometBFT, Ethereum-PoS) are implemented; see `sync-protocol.md`.

## Conventions

- Java package root: `org.hiero.clpr.relay.*` (one sub-package per module).
- Module names mirror package suffixes: `clpr-relay-evm` → `org.hiero.clpr.relay.evm`.
- All file paths in docs are relative to repo root.
- Ledger configuration, channel state, message queue: see spec §2; on EVM these live in contract storage and are read via the contract methods listed in `evm-integration.md`.
