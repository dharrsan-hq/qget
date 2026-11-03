# Contributing to qget

Thanks for helping. The design is described in [PRD.md](PRD.md); please read
§6 (design principles) and §9 (architecture) before larger changes.

## Layout

```
crates/qget-core        pure logic: engine (scheduler), journal format, hashing — no I/O
crates/qget-runtime     tokio driver, crash-safe file sink, stream sink, trace replay
crates/qget-http        HTTP(S) range source (reqwest + rustls)
crates/qget-extract     streaming tar.lz4/zst/gz/xz extraction
crates/qget-p2p         iroh peer transfer, tickets, SPAKE2 codes, mailbox
crates/qget             public library (Download builder) — the only crate users depend on
crates/qget-cli         the `qget` binary, built only on the public API
crates/qget-testserver  fault-injecting HTTP server for tests and benchmarks
```

Rules (checked by `scripts/check-deps.sh` in CI):

- `qget-core` performs no I/O and depends only on pure-computation crates.
- The default build does not depend on iroh; P2P is the `p2p` feature.
- The CLI uses only the public `qget` API.

## Before sending a PR

```sh
cargo fmt --all
cargo clippy --workspace --all-targets --features qget-cli/p2p -- -D warnings
cargo test --workspace --features qget-cli/p2p
scripts/check-deps.sh
```

Scheduler changes should keep the simulation sweep green
(`QGET_SIM_SEEDS=10000 cargo test --release -p qget-core seed_sweep`) and,
for anything touching durability, the crash test
(`QGET_CRASH_ROUNDS=50 cargo test -p qget-cli --test cli sigkill`).

Correctness changes need a test that fails without them. Performance claims
need numbers from `bench/bench.py`, including scenarios where nothing improves.

## License

By contributing you agree your work is licensed under Apache-2.0.
