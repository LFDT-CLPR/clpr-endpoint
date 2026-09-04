# CLPR EVM Endpoint — E2E Test Framework

Black-box end-to-end tests for the `clpr-evm-endpoint`. The endpoint runs as a **container**; real
Besu QBFT chains run beside it; the **production** `QBFTVerifier` validates every bundle. Nothing is
stubbed on the path under test.

```bash
RUN_E2E_TESTS=1 ./gradlew :clpr-relay-e2e-tests:test   # 17 of 18 tests, ~1 h 20 min+
```

**One test is `@Disabled`, not missing.** `BacklogDrainE2ETest.drains_a_backlog_larger_than_the_bundle_batch`
tracks an open relay bug (`clpr-hiero#171`): a backlog larger than the bundle batch wedges the channel
for good. It is skipped pending that fix, and now that the suite gates every PR the skip is mandatory
rather than a convenience: a permanently red required check would mask the next real regression
instead of reporting this one. Delete the annotation when the fix lands; its assertions are the
acceptance criteria. Everything else is green.

Requires Docker, JDK 25, and `../clpr-smart-contracts` checked out with `forge build` run.

Budget the wall clock: **1 h 22 min** for the 16 tests that run under `-Dclpr.e2e.profile=ci`, on a
10-core / 25 GB Docker host. The suite is strictly sequential (`maxParallelForks = 1`) and each test
builds its own chains, contracts and endpoints from scratch, so the total tracks the test count
almost linearly — adding a test adds minutes, and a CI timeout has to be set against that number.

Of the 18 `@E2ETest` methods, two do not run in that figure: the `@Disabled` canary above, and
`ManyChannelsE2ETest`, which requires `Capability.HIGH_LOAD` and is therefore reported as disabled
under the `ci` profile. Locally (`local` profile) it runs and adds substantially to the total.

## Running from an IDE

The suite is opt-in via **either** switch:

|         Switch          |                       Where                       |
|-------------------------|---------------------------------------------------|
| `RUN_E2E_TESTS=1`       | environment variable                              |
| `clpr.e2e.enabled=true` | `gradle.properties`, or `-Dclpr.e2e.enabled=true` |

IDEs do not pass your shell's environment to their Gradle runs, so use the property there — put
`clpr.e2e.enabled=true` in `gradle.properties` (project or `~/.gradle/`) once and IDE runs work
without per-configuration fiddling. Setting `RUN_E2E_TESTS=1` in the run configuration's environment
works too.

With neither switch set, every method is disabled by the extension's `ExecutionCondition`. That is
harmless for a full run (`0 tests, BUILD SUCCESSFUL`) but **fails** when combined with a `--tests`
filter, which is what an IDE always sends:

```
No matching tests found in any candidate test task.
    Requested tests: Test pattern org.hiero...UnderfundedConnectorE2ETest in task :clpr-relay-e2e-tests:test
```

The class is disabled, so JUnit produces zero test descriptors, and a filter with zero matches is a
build failure. The message names the filter rather than the missing switch — if you see it, this is
why.

## In CI

Two workflows run this suite; both delegate to the reusable
`.github/workflows/800-call-e2e-tests.yaml`, so there is one definition of how it runs.

|                  Workflow                  |                     When                     |                 What it tests                 |
|--------------------------------------------|----------------------------------------------|-----------------------------------------------|
| `200-flow-pr-checks.yaml` (job `E2E Test`) | every PR, and pushes to `main` / `release/*` | the PR head commit; blocking (required check) |
| `220-cron-e2e-daily.yaml`                  | daily 03:00 UTC, or `workflow_dispatch`      | `main` and the newest `release/*` branch      |

Both run with `-Dclpr.e2e.profile=ci` (see `E2EProfile`), which caps container heaps so ~6 containers
fit on a runner. Both also hold a runner for well over an hour — see the timing note above; the
reusable workflow's `timeout-minutes` defaults to 150 to leave headroom over the measured 1 h 22 min.
On failure the job uploads `build/e2e-output/**` plus the Gradle test reports as the
`e2e-diagnostics-*` artifact — the same tree described under [Diagnostics](#diagnostics).

### Which contracts it tests against

The suite needs a `forge build`-ed `clpr-smart-contracts` checkout, and which ref that is gets
resolved by `scripts/ci/resolve-smart-contracts-ref.sh` rather than pinned in the workflow, so a
release line of this repo is verified against its own contract line:

|    This repo's branch    |           clpr-smart-contracts ref            |
|--------------------------|-----------------------------------------------|
| `main`, feature branches | `main`                                        |
| `release/X.Y`            | `release/X.Y`, if the contracts repo has it   |
| `release/X.Y`            | otherwise its newest `release/*`, else `main` |

The fallback is the newest *release* branch, not the tip — note the consequence, because it is the
case that fires today: this repo's `release/0.6` resolves to contracts `release/0.1`, a much older
minor. Every run prints the resolved ref in the job summary, and `sc-ref` on the reusable workflow
overrides the rule for a repro. The `Integration Test` job uses the same script.

The two repos are checked out as siblings of the workspace because `ContractArtifact` looks for the
Foundry output at `../../clpr-smart-contracts/out`.

## Writing a test

```java
@E2ETest(timeoutSeconds = 900)
void messages_flow_across_a_channel(final E2EEnvironment env) {
    final BesuNetwork besuA = env.besuNetworks().add(BesuNetworkSpec.singleNode());
    final BesuNetwork besuB = env.besuNetworks().add(BesuNetworkSpec.singleNode());

    final EvmEndpoint epA = env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-a"));
    final EvmEndpoint epB = env.endpoints().add(EvmEndpointSpec.defaults().withId("ep-b"));

    final Channel channel = env.channels()
            .create(ChannelSpec.between(besuA, besuB).endpoints(epA, epB));

    env.startAll();   // chains → contracts → channel wiring → endpoints

    final InjectionRun run = env.messages().enqueue(channel.a(), 8, MessageShape.small());
    run.awaitAllConfirmed(Duration.ofMinutes(3));

    env.await().peerReceivedAll(channel.a(), run.messageIds(), Duration.ofMinutes(5));
    env.await().queueDrained(channel.a(), Duration.ofMinutes(5));
}
```

Objects are **declared** first and brought up by `startAll()`. That split is required, not stylistic:
the QBFT genesis has to embed the validator set and prefunded accounts, and the on-chain peer roster
has to embed each endpoint's IP address — none of which is known until the topology is complete.

### Adding a channel while the endpoints run

`startAll()` is a convenience for the common case. A test that needs endpoints running *before* a
channel exists drives the phases itself and then wires the channel for discovery:

```java
env.besuNetworks().startAll();
env.besuNetworks().deployAll();

// Empty channel list: discovery fills it in.
epA.serve(besuA, List.of()).addPeerProofType(besuB.caip2(), "QBFT");
epB.serve(besuB, List.of()).addPeerProofType(besuA.caip2(), "QBFT");
env.endpoints().startAll();

final Channel channel = env.channels().create(ChannelSpec.between(besuA, besuB).endpoints(epA, epB));
env.channels().wireForDiscovery(channel);            // on-chain only, no endpoint reconfiguration
env.await().channelLive(epA, channel.metricLabel(), Duration.ofMinutes(3));
```

The endpoints must be declared with `.withDiscoverChannels(true)`. The relay reads its configuration
once at startup, so a channel wired afterwards is invisible to an endpoint that only has
`predefinedChannels` — it has to poll the service for `ChannelCompleted` events.
`wireForDiscovery` rejects the call outright if a serving endpoint cannot discover, because otherwise
the channel would exist on chain, both relays would look healthy, and nothing would ever be delivered.

`wireAll()` skips channels it has already wired, so it is safe to call again.

`@E2ETest` gives each method its own environment, closed afterwards. Per-method isolation is
mandatory: a channel whose backlog exceeded the relay's bundle batch is wedged permanently, so a
shared environment would let one test poison the rest.

## What it can do

**Chains** — generate Besu QBFT genesis (validator keys, `extraData` RLP, static-nodes, 64 prefunded
accounts), run multi-node networks, `start`/`stop`/`restart`/`kill`/`pause` per node. Chain state
lives in a per-node Docker **volume**, so a restart resumes rather than resetting to genesis; the
genesis file and node key are **copied** into the container. Neither mechanism hands the daemon a
host path, which is what lets the suite run on a Docker-in-Docker runner — a bind mount there
resolves against the daemon's filesystem, not the test JVM's, and Besu exits with code 2 because
its genesis file is missing. Do not reintroduce `withFileSystemBind` in `BesuNode`.

**Contracts** — deploy the full CLPR stack per chain: six logic modules, `ClprService`, real
`QBFTVerifier(1)`, `E2EApplication`, plus a per-channel `QbftE2EVerifier` wrapper and
`MockClprConnector`.

**Channels** — the whole wiring: commit-reveal on both chains, the two RLP config proofs the real
verifier demands (10-field config proof + slot-`0x12` endpoint-manifest proof), connector
registration with stake and prepaid execution budget, peer-roster seeding.

**Endpoints (the SUT)** — containers built from the shipped jars and the production
`docker-entrypoint.sh`; generated `relay.yaml`; the signing key arrives through the real
`__SIGNING_KEY_*__` sentinel substitution. Pinned IPs survive restarts, because
`ClprService` validates seed endpoints as IPv4 literals and stores them verbatim in the peer roster.

**Load** — pipelined sends (locally tracked nonce, no receipt round-trip) spread over four sender
accounts per chain, so a real backlog can form. Message ids come from `MessageQueued` events, and a
backpressure mode holds the in-flight window at a chosen depth.

**Faults** — pause/resume a chain or node, kill or pause an endpoint, partition and rejoin it on the
Docker network (restoring its pinned address), drain an endpoint's signing account, make a connector's
`payForExecution` revert.

**Observation** — Prometheus scrape with label support, log capture that survives container removal
and restarts, and a chain inspector for blocks, receipts, revert lists and per-sender nonce
histograms (duplicate nonce ⇒ two submitters raced one account; gap ⇒ a transaction was lost).

## Test suites

|             Suite             |                                                   What it establishes                                                   |
|-------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| `BesuNetworkE2ETest`          | generated QBFT genesis actually mines; multi-node networks form; chains get distinct ids                                |
| `ContractStackE2ETest`        | the real verifier stack deploys and the service accepts mutating calls                                                  |
| `EndpointContainerE2ETest`    | the container runs the shipped relay; restart keeps IP and key; scrape semantics                                        |
| `SingleChannelSmokeE2ETest`   | a channel comes up across two chains against the real verifier                                                          |
| `BacklogDrainE2ETest`         | sub-batch backlog drains; rolling restarts under load lose nothing; over-batch case is the disabled canary              |
| `UnderfundedConnectorE2ETest` | a starved connector is a business outcome, not a relay failure, and the channel recovers; relay survives a chain outage |
| `ChannelDiscoveryE2ETest`     | a running endpoint picks up a channel wired after it started, and delivers over it                                      |
| `TwoChannelIsolationE2ETest`  | two channels across the same chain pair stay isolated — one channel's traffic never moves the other's watermark         |
| `ManyChannelsE2ETest`         | scale canary: many parallel channels drain without nonce contention or silent drops. `HIGH_LOAD`, so not run in CI      |

Assertions target the hard property, not the convenient one: block production rather than port
reachability, bytecode at the deploy target rather than "an address came back", and a sync cycle
attributed to the channel id rather than a healthy container — a channel whose on-chain protocol
version mismatches is skipped in silence.

## Constraints worth knowing

- **One QBFT validator per chain.** `QBFTVerifier` reverts with `MultiValidatorNotSupported()` on a
  multi-validator epoch header. Use `BesuNetworkSpec.of(n)` — one validator plus full nodes — for a
  genuinely multi-node, still-verifiable network.
- **Non-zero gas price.** The connector-affordability check is guarded by `tx.gasprice > 0`, so
  `minGasPriceWei` defaults to 1; a zero-price chain makes that whole path unreachable.
- **One operator per channel.** The channel id is derived from the operator public key, so both sides
  must use the same one or the second `completeChannel` reverts with `ClprInvalidChannelId`.
- **Sequential runs.** Environments take a fixed `/24` each; the allocator walks forward on overlap,
  but cross-JVM parallelism would need a file lock. `maxParallelForks = 1`.
- **`E2EProfile.CI`** (2×2 nodes, ≤4 endpoints, hard heap caps) is green — 16/16 in 1 h 22 min — but
  so far only on a developer machine. It has still never run on the CI runner itself, where the CPU
  count and Docker memory differ. It withholds `HIGH_LOAD`, which is what keeps `ManyChannelsE2ETest`
  out of the PR pipeline; granting it there would roughly double the job's wall clock.

## Diagnostics

Diagnostics are written **only when a test fails**. A passing test leaves nothing behind, so any
directory under `build/e2e-output/` means something failed there — an empty tree after a green run is
correct, not broken.

On failure, `build/e2e-output/<Class>/<method>/` gets:

|            File             |                        Contents                        |
|-----------------------------|--------------------------------------------------------|
| `failure.txt`               | the assertion, the active profile, the stack trace     |
| `endpoint-<id>.log`         | the relay's full container output                      |
| `endpoint-<id>-metrics.txt` | the final `/metrics` scrape                            |
| `endpoint-<id>-config.yaml` | the generated `relay.yaml` it started with             |
| `besu-<network>-<node>.log` | each Besu node's output                                |
| `chain-<network>.txt`       | head, chain id, validator, and every mined transaction |

Capture happens *inside* the test invocation, via `TestExecutionExceptionHandler`.
`TestWatcher.testFailed` would be too late: JUnit closes the method-scoped store — stopping every
container — before calling it, so a dump taken there finds nothing running and reports
"endpoint is not started" instead of the log and scrape that explain the failure.
