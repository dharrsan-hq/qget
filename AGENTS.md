# AGENTS.md

Guidance for AI coding agents (and humans) working on **qget**, a Rust downloader
for big files ("quickly get"): parallel HTTP, stream-extract, peer-to-peer over QUIC.
Read this before changing code. Design background lives in [PRD.md](PRD.md).

qget is **under active development**. Contributions and criticism are welcome, by
[issue](https://github.com/dharrsan-hq/qget/issues), email (dharrsan.work@gmail.com)
or [LinkedIn](https://www.linkedin.com/in/dharrsan-amarnath-75017221a).

## Project layout

```
crates/qget-core        pure logic: scheduler engine, journal format, hashing, simulator (NO I/O)
crates/qget-runtime     tokio driver, crash-safe file sink + journal, stream sink, trace/replay
crates/qget-http        HTTP(S) range source (reqwest + rustls)
crates/qget-extract     streaming tar/lz4/zstd/gz/xz extraction, hardened tar writer
crates/qget-p2p         iroh (QUIC) peer transfer, tickets, SPAKE2 short codes, mailbox
crates/qget             public library: the Download builder; the only crate users depend on
crates/qget-cli         the `qget` binary; uses only the public `qget` API
crates/qget-testserver  fault-injecting HTTP server for tests and benchmarks
packaging/              nfpm (deb/rpm/arch), AUR PKGBUILD, signing public key, distro tests
site/                   landing page for qget.dharrsan.com
bench/                  benchmark harness and results
```

## Hard rules (CI enforces them)

1. **`qget-core` performs no I/O**: no tokio, sockets, files, clocks or ambient randomness.
   The engine is a pure state machine driven by `Event`s with explicit timestamps and a seed.
   `scripts/check-deps.sh` keeps its dependency allow-list.
2. **The default build does not depend on iroh.** P2P is the `p2p` feature.
3. **The CLI uses only the public `qget` API.** Don't reach into internal crates from `qget-cli`.
4. **Never produce a wrong file silently.** Every completed output is byte-exact or a typed error.
   Bytes are validated (Content-Range, byte counts, checksums, BLAKE3 groups) *before* they are written.
5. **Durability ordering**: `pwrite` → `fdatasync` → journal record → `fdatasync(journal)`.
   Renames are followed by an fsync of the parent directory.
6. Library crates never print, never `process::exit`, and keep no global mutable state; use `tracing`.
7. Public error types are `#[non_exhaustive]`, and each maps to one documented exit code.

## Before you finish a change

```sh
cargo fmt --all
cargo clippy --workspace --all-targets --features qget-cli/p2p -- -D warnings
cargo test --workspace --features qget-cli/p2p
scripts/check-deps.sh
```

- Scheduler/engine changes: `QGET_SIM_SEEDS=10000 cargo test --release -p qget-core seed_sweep`
  (nightly CI runs 100,000 seeds; a failing seed reproduces exactly).
- Durability/resume changes: `QGET_CRASH_ROUNDS=50 cargo test -p qget-cli --test cli sigkill`.
- Packaging changes: `packaging/test/distro-smoke.sh` runs in Debian/Ubuntu/Fedora/Arch containers in CI.
- Performance claims need numbers from `bench/bench.py`, including scenarios that don't improve.

## Conventions

- Match the surrounding code: naming, comment density, error style. Comments explain *why*.
- Every bug fix comes with a test that fails without it. Prefer the fault-injecting test server
  (`qget-testserver`) or the deterministic simulator (`qget_core::sim`) over sleeps and real networks.
- Keep dependencies few and well-maintained; `cargo deny check` must stay green.
- User-facing text is plain English: say what happened and what to do next.
- Update `CHANGELOG.md` for user-visible changes; releases need `docs/release-notes/vX.Y.Z.md`.

## Never

- Commit secrets, private keys, signed URLs or credentials. The package-signing private key exists
  only as a GitHub environment secret; only `packaging/qget-signing-key.asc` (public) is in git.
- Weaken verification, TLS checks or archive-extraction safety to make a test pass.
- Add telemetry or network calls the user didn't ask for.
- Push tags or publish releases. Maintainers do that.

## AI-assisted contributions

They're welcome. Please:
- say in the PR description that an AI assistant helped, and roughly with what;
- make sure you understand and can explain every line you submit;
- include the tests and the command output above.
