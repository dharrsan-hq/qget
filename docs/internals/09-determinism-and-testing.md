<!-- covers: crates/qget-core/src/sim.rs crates/qget-core/src/engine/tests.rs crates/qget-runtime/src/trace.rs crates/qget-testserver/src/lib.rs crates/qget-testserver/src/main.rs fuzz/fuzz_targets/content_range.rs fuzz/fuzz_targets/extract.rs fuzz/fuzz_targets/journal.rs bench/bench.py .github/workflows/ci.yml -->

# 9. Determinism and testing

Networks are not deterministic, so qget cannot promise identical runs. What it
promises instead (§8.3 of the PRD) is:

1. a **deterministic outcome**: a byte-exact verified file, or a typed error;
2. **reproducible decisions**: given the same events, the engine makes the same
   choices, and a recorded run can be replayed and checked exactly.

The second property is what makes the first one testable at scale.

## What makes the engine deterministic

| Source of nondeterminism | How it is removed |
|---|---|
| The clock | The engine never reads it; every event carries `now` in milliseconds, and `now` is forced to be monotonic inside `handle` |
| Randomness | One SplitMix64 generator (`rng.rs`), seeded from `EngineConfig::seed`. It is used only for backoff jitter. The seed is printed by `--explain` and settable with `--seed` |
| Hash-map iteration order | In-flight requests live in a `BTreeMap` keyed by request number, so iteration order is fixed |
| Floating-point noise in measurements | Rates feed decisions only after quantisation: block sizes are rounded to whole MiB, and utility is bucketed in 5% steps and compared as integers |
| Ties | Every comparison has an explicit tie-break (lowest source index, fewest connections, lowest request id) |

`qget-core` has no access to I/O at all (enforced by
`scripts/check-deps.sh`), so none of this can be bypassed by accident.

Note what a seed alone does **not** do: `--seed 42` on two live runs still
produces different decisions, because the network delivers different events.
To reproduce a live run you need its **trace**.

## Traces and replay

`--trace FILE` records every decision as JSON lines (`trace.rs`):

```text
line 1   {"qget_trace":1, "config":{…EngineConfig…}, "coverage":[[start,end],…]}
line 2+  {"event":{"Tick":{"now":0}}, "commands":[{"Fetch":{"req":1,"source":0,"range":{…}}}, …]}
         {"event":{"Data":{"now":112,"req":1,"offset":0,"len":16384}}, "commands":[]}
         …
```

`qget replay FILE` builds a fresh engine from the header, feeds it every
recorded event, and compares the commands it produces with the recorded ones.
Any difference is reported with the event index, what the trace says and what
the replay produced. A trace from a user's bug report can therefore be stepped
through in a debugger, deterministically, without their network.

## The simulator

`qget-core/src/sim.rs` drives the real engine with a fake network, entirely in
memory:

- simulated time advances in 10 ms steps; a `Tick` is sent every 100 ms;
- each simulated **source** has a per-connection rate, an aggregate rate cap,
  a latency, and seeded probabilities of transient failure part-way through a
  request, of a request being a straggler (running at a fraction of normal
  speed), of throttling above N concurrent requests, and of becoming
  permanently unusable at a given time;
- a simulated streaming consumer reads at a configured rate and reports
  `Consumed`;
- `run_with_crash` stops at a given time and returns the coverage at that
  moment, so a second run can **resume** from it.

All of it comes from one seed, so `(SimConfig, seed)` always produces the same
event log. A simulated hour of downloading takes milliseconds.

### The seed sweep

`seed_sweep_invariants` (`engine/tests.rs`) generates a random scenario from
each seed (1–3 sources, any size up to 300 MiB, random rates, latencies,
failure rates, stragglers, throttling, alignment, streaming windows, and a
crash with resume in a third of runs) and checks after each run:

| Invariant | Meaning |
|---|---|
| The download finishes | Within one simulated hour, despite every injected fault |
| Coverage is `[0, size)` | Every byte arrived |
| Hedged bytes ≤ budget | Duplicates stay within `hedge_budget` |
| In-flight requests ≤ 2 × `max_conns` + 1 | Concurrency never runs away |
| Every `Fetch` is valid | Non-empty, inside the file, aligned to `alignment` |

| Where | Seeds |
|---|---|
| Default `cargo test` | 300 |
| CI on every push and pull request | 1,000 |
| Nightly CI | 100,000 |
| Locally | `QGET_SIM_SEEDS=N cargo test --release -p qget-core seed_sweep` |

A failing seed reproduces exactly from `(seed, commit)`, the same approach as
TigerBeetle's VOPR.

**Current status:** the nightly sweep fails on seed 14668 (the controller
collapses to one connection and the download doesn't finish in the simulated
hour). See [the scheduler's known limitations](04-scheduler.md#known-limitations).
Reproduce with `QGET_SIM_SEEDS=15000`.

Other engine tests cover specific behaviours: concurrency grows when each
connection is capped and stays low when the aggregate is capped; throttling
reduces concurrency; stragglers are hedged; a fatal source falls back to a
mirror; a faster mirror gets more bytes; the streaming window is never
exceeded; resume fetches only missing bytes; the same seed gives the same
commands.

## Layers of testing

| Layer | What it proves | Where |
|---|---|---|
| Property tests | `IntervalSet` matches a bitmap model; the journal and `Content-Range` parsers never panic on arbitrary input and round-trip valid input | `range.rs`, `journal.rs`, `content_range.rs` (proptest) |
| Engine simulation | The scheduling invariants above under random faults | `engine/tests.rs` |
| Fault-injecting HTTP server | The real HTTP stack, runtime and sinks against misbehaving servers | `qget-testserver`, used by `qget/tests/e2e.rs` and `qget-http/src/tests.rs` |
| Crash tests | `kill -9` at random moments, then resume, gives a byte-exact file | `qget-cli/tests/cli.rs` (`sigkill_crash_resume_is_byte_exact`) |
| P2P tests | Loopback transfers including a tampered provider, a lying mirror, a swarm, and resume | `qget/tests/p2p.rs` |
| Extraction tests | Archives made by the system `lz4` and `zstd` tools; hostile archives (traversal, symlink escapes, special files) | `qget-extract/src/tests.rs` |
| Fuzz targets | `Content-Range` parser, journal decoder, extractor (nothing may be written outside the destination) | `fuzz/` (cargo-fuzz, nightly Rust) |
| Distro install tests | The `.deb`, `.rpm` and Arch packages install and run on Debian, Ubuntu, Fedora and Arch, before release and from the live repositories after | `packaging/test/` |
| Benchmarks | Throughput against aria2c, curl, wget and `curl \| lz4 \| tar`, every output checked by SHA-256 | `bench/bench.py`, results in `bench/results/` |

### The fault-injecting server

`qget-testserver` serves one resource with range support and injects faults
**deterministically by request number** (for example "every 7th GET from the
3rd on"):

| Fault | Simulates |
|---|---|
| `ShortBody` | A body that stops half way |
| `WrongContentRange` | A `206` describing a different range |
| `IgnoreRange` | A `200` with the whole file instead of the range |
| `ResetAfter(n)` | A connection reset after n bytes |
| `Status(code, retry_after)` | 404, 429, 503 … with or without `Retry-After` |
| `Stall(ms)` | Headers, then silence |
| `ChangeContent` | The file changes mid-download: new ETag, same size, different bytes |

It can also cap per-connection speed, add latency, limit concurrency, reject
`HEAD`, redirect, omit range support, and serve `Repr-Digest` or a `.sha256`
sidecar. The benchmark harness uses the same server, which is why the current
published numbers are loopback-only.

## CI at a glance

| Job | Runs |
|---|---|
| `lint` | `cargo fmt --check`, clippy with warnings as errors, `scripts/check-deps.sh` |
| `docs` | `scripts/check-internals-docs.sh`: these documents are updated with the code they describe |
| `test` | All tests with and without P2P, then the 1,000-seed sweep |
| `test-macos` | Core, runtime, HTTP and extraction tests; a P2P build |
| `msrv` | `cargo check` on the minimum supported Rust version |
| `supply-chain` | `cargo deny` (licences, advisories, sources) |
| `install-script` | `install.sh` against the newest release, under `sh` and `dash`, on Linux and macOS |
| `nightly` | 100,000-seed sweep and 200 `kill -9` rounds |

## Known gaps

- The fuzz targets exist but have not been run for the 24 hours the PRD's
  release gate asks for.
- Benchmarks are loopback-only, with network conditions emulated by the test
  server; there are no real-network or `tc netem` results yet.
- P2P has not been tested across real NATs.

Next: [Design decisions](10-design-decisions.md).
