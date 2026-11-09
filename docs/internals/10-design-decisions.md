<!-- covers: PRD.md -->

# 10. Design decisions

Each decision below records the problem, what was chosen, what was rejected,
and what the choice costs. The order of priorities behind all of them comes
from the PRD: **correctness, then determinism, then performance, then
features**. "Fastest" only counts if the output is always byte-exact or the
tool fails with an explicit error.

## Architecture

### A pure engine inside an I/O shell

- **Problem.** Download scheduling is full of timing-dependent logic (hedging,
  backoff, concurrency changes) that is nearly impossible to test against a
  real network and impossible to reproduce from a bug report.
- **Chosen.** The scheduler is a pure state machine: events with timestamps
  in, commands out, a seeded RNG, no I/O (the `quinn-proto` pattern). A tokio
  runtime executes its commands.
- **Rejected.** Scheduling logic inside async tasks (the usual design): every
  test would need a real or mocked network, and runs would not replay.
  `turmoil` (simulated network for tokio code) was planned, but a pure engine
  made a much simpler step simulator possible.
- **Cost.** Two layers to keep in sync, and the engine can't take shortcuts
  such as "check the clock now". In return: 100,000-seed simulation runs
  nightly, exact trace replay, and one scheduler for HTTP and P2P.

### Library first, CLI second

- **Problem.** A download engine spreads furthest when other programs embed it
  (libcurl is the example), and a CLI-only design hides API gaps.
- **Chosen.** All behaviour lives in the `qget` library; the CLI may only use
  its public API.
- **Cost.** Every feature needs a public API first. The library is not
  published yet; it stays experimental until 1.0 so early API mistakes can be
  fixed.

### One `Source` trait for every transport

- **Problem.** Mirrors, peers and hybrid downloads would otherwise need
  separate schedulers.
- **Chosen.** Everything that can serve `[start, end)` implements one trait,
  and the engine treats sources interchangeably, ranked by measured speed.
- **Consequence.** Hybrid HTTP + P2P downloads needed no engine changes, only
  a `VerifiedSource` wrapper.

### P2P is an opt-in feature

- **Problem.** Debian and Fedora package every Rust dependency separately.
  iroh's dependency tree is large and would block packaging of the default
  build for years.
- **Chosen.** `p2p` is a Cargo feature, off by default and checked in CI.
  Release binaries include it.

## Correctness

### Validate before writing, and refuse to guess

- **Problem.** Resumable downloaders can corrupt files silently: a server that
  ignores `Range`, a CDN node with a newer version, a resume against a file
  that changed overnight.
- **Chosen.**
  - every range request carries `If-Range` with a **strong** validator;
  - every `206` must match the requested range and size exactly;
  - a `200` to a sub-range request is never stitched in;
  - resume requires proof of identity (validator or checksum); without it,
    partial data is discarded;
  - a changed remote is an error that must be acknowledged (`--restart`), not
    a silent restart.
- **Cost.** Some resumes that would "probably have been fine" start over, and
  some servers that misbehave in harmless ways cost retries.

### An append-only journal with group commit

- **Problem.** Resume state must survive a crash at any instant, without
  `fsync`-ing every chunk.
- **Chosen.** An append-only log of durable ranges, each record framed with a
  length and CRC32C. Data is `fdatasync`ed before the record describing it,
  in groups (256 MiB or 1 s). Compaction uses write-tmp, fsync, rename, fsync
  directory. See [Durability](06-durability-and-resume.md).
- **Rejected.**
  - A bitmap file rewritten in place: a torn write can corrupt it.
  - Periodic snapshots only on pause or error (Surge's approach): a crash
    loses everything since the last snapshot, usually everything.
  - SQLite: correct, but a large dependency for a log of integer pairs.
- **Cost.** Up to one commit group (256 MiB or one second of data) is fetched
  again after a crash.

### Preallocate and check space first

- **Chosen.** `fallocate` the whole file up front and check free space with a
  margin, so "disk full" happens at the start instead of at 90%.

### Extract into staging, install atomically

- **Chosen.** Extract into `DIR.qget-staging`; flush the filesystem once; rename
  into place only after the archive's hash matches. A half-extracted or
  unverified tree is never presented as `DIR`.
- **Cost.** `DIR` and its staging directory must be on the same filesystem.

## Performance

### HTTP/1.1 on separate connections, not HTTP/2 or HTTP/3

- **Problem.** Servers and CDNs limit per-connection throughput, and a single
  TCP connection is limited by its congestion window.
- **Chosen.** N independent HTTP/1.1 keep-alive connections, one range at a
  time each. HTTP/2 is opt-in and then still uses one TCP connection per
  parallel request; HTTP/3 is experimental.
- **Why not HTTP/2 multiplexing.** Many streams over one TCP connection add no
  TCP parallelism, and default HTTP/2 stream windows are 64 KiB.
- **Why not HTTP/3.** On fast links, measured QUIC receive paths cost much more
  CPU than TCP (up to 45% slower than TCP + HTTP/2 in one study).

### Small dynamic blocks instead of one range per connection

- **Problem.** Splitting a file into N equal parts makes the download as slow
  as the slowest connection.
- **Chosen.** Blocks of about one second of transfer (4–64 MiB), handed to
  whichever connection is free, so fast connections take more blocks
  (Rodriguez & Biersack's dynamic parallel access).
- **Cost.** One idle round trip per block boundary; this is why blocks aim for
  a full second of data.

### Measure the right number of connections

- **Problem.** A fixed connection count is too few on throttled servers and
  too many on uncapped ones. Too many is not free: parallel flows take an
  unfair share of shared bottlenecks and can trigger server defences.
- **Chosen.** Probe 4 → 8 → 16 connections for a second each, keep the best by
  a utility that **charges about 2% throughput per connection** (Falcon's
  insight that throughput-only objectives overload networks and storage),
  then hill-climb by ±1 to follow changing conditions. Start at 4, cap at 16
  by default (64 with `--max-connections`), halve on throttling.
- **Rejected.** `sqrt(size)` heuristics (Surge's starting point), fixed
  `-x16` (aria2c's usual tuning), and fitting the Yildirim model from three
  samples (planned in the PRD; comparing three measured utilities proved
  simpler and deterministic).
- **Cost.** About three seconds of probing before reaching 16 connections, and
  a ±1 oscillation around the optimum. The hill climb also has a known failure
  case (see [the scheduler](04-scheduler.md#known-limitations)).

### Split before you duplicate

- **Problem.** The end of a download is dominated by the slowest requests
  (the "tail").
- **Chosen.**
  1. **Steal first**: if a slow or remaining request has ≥ 8 MiB left, split
     it in half and give the tail to another connection. No byte is fetched
     twice.
  2. **Hedge second**: duplicate a remainder too small to split, but only
     after it has had a chance to finish (*The Tail at Scale*: hedged
     requests cut p99.9 latency dramatically for about 2% extra load), and
     within a budget of 5% of the file.
  3. Detect stragglers against the **median** rate, not the mean, so a few
     very fast connections don't make normal ones look slow (a flaw noted in
     Surge's approach).
- **Cost.** Up to 5% duplicated bytes, and stolen or cancelled requests close
  their connection.

### Parallel decoding only where the format allows it

- **Problem.** Decompression is often the real bottleneck of snapshot
  restores, but only some formats can be split.
- **Chosen.** LZ4 with independent blocks and multi-frame zstd decode in
  parallel; single-frame zstd, linked-block LZ4, gzip and xz decode serially,
  and the run says so (`decode: serial …`, and a decode-bound verdict in
  `--explain`). Frames are found by walking headers in the stream, so no extra
  request for an index is needed.
- **Rejected for now.** Parallel gzip (rapidgzip-style), which is complex and
  P2 in the PRD.

### One `syncfs`, not one `fsync` per extracted file

- **Problem.** Snapshots can contain millions of small files; an `fsync` each
  would dominate extraction time.
- **Chosen.** Write everything into the staging directory, flush the whole
  filesystem once at the end, then rename. Durability of the *result* is the
  same, because the rename happens after the flush.

## Peer-to-peer

### QUIC through iroh, not TCP relays or libp2p

- **Problem.** Most machines worth sending data to are behind NAT.
- **Chosen.** iroh 1.x: dial by public key, hole punching inside QUIC, relay
  fallback on HTTPS/443, path migration between relay and direct.
- **Rejected.** TCP via a relay (croc, magic-wormhole's classic transit): the
  relay carries every byte. rust-libp2p: ~70% measured hole-punch success is
  comparable, but the stack is larger and less focused.

### qget's own two-operation protocol

- **Chosen.** `INFO` and `RANGE` over one QUIC stream each, with a capability
  secret.
- **Rejected.** iroh-blobs, which its maintainers do not yet consider
  production quality (and which had a security fix in October 2026).

### BLAKE3 groups of 1 MiB instead of a bao outboard

- **Problem.** Bytes from untrusted peers must be verified before writing,
  per piece, so a bad peer can be identified and dropped.
- **Chosen.** Publish BLAKE3's own chaining value for each 1 MiB subtree
  (32 bytes per MiB). The receiver checks the list against the root, then each
  group on arrival. The root stays the ordinary `b3sum` value.
- **Rejected.** A full bao outboard (~6% overhead at default settings and an
  extra dependency); per-group verification is all swarm and hybrid downloads
  need.

### SPAKE2 confined to delivering the ticket

- **Problem.** Short human-typable codes need a password-authenticated key
  exchange, and the available Rust SPAKE2 crate is not audited.
- **Chosen.** SPAKE2 plus explicit key confirmation only authenticates a short
  exchange that delivers the ticket, encrypted with ChaCha20-Poly1305. All
  data security comes from iroh's QUIC/TLS. Migrating to CPace is planned once
  an audited implementation exists.

### No fountain codes, no tit-for-tat

- **Chosen.** Plain verified ranges with a deterministic scheduler.
- **Why.** Fountain codes (RaptorQ) cost CPU and complicate verification and
  determinism over a transport that is already reliable. BitTorrent's choking
  solves an incentive problem between strangers that qget's users (a team, a
  fleet) don't have, at the price of deliberate nondeterminism.

## Citizenship and safety

- **No telemetry**, and no network calls the user didn't ask for.
- **No listening sockets** in normal downloads; P2P listens only while sending
  or seeding.
- **Honest `User-Agent`** (`qget/<version>`), not a browser impersonation.
- **Polite defaults**: start at 4 connections, cap at 16, always reduce load on
  `429`/`503`, honour `Retry-After`; `--polite` caps at 4 with no duplicate
  requests.
- **`Accept-Encoding: identity`**, so byte ranges always refer to the stored
  bytes.
- **HDD tuning is opt-in** (`--hdd`), because virtual and cloud disks commonly
  report themselves as rotational, which once forced single-connection
  downloads on ordinary VMs.

## Deviations from the PRD

The PRD was written before the code. Where they differ, the code is the
truth, and this table explains why:

| PRD said | Built | Reason |
|---|---|---|
| hyper with a custom connection pool | reqwest 0.13 (on hyper, rustls + aws-lc-rs) | Proxies, redirects, TLS and pooling for free; per-connection control kept by using HTTP/1.1, and separate clients for HTTP/2 |
| turmoil for simulation | A step simulator inside `qget-core` (`sim.rs`) | The engine is pure, so seeded synthetic events cover it without a network shim |
| Fit the Yildirim model from three samples, then a Falcon utility | Best of three measured levels by utility, then ±1 hill climb | Simpler, deterministic, and needs no fitted constants |
| Hedge when projected finish exceeds p95 of recent block times; end-game when blocks < connections | Steal-then-hedge whenever no unclaimed bytes remain; stragglers below 0.3× (0.5× at the stream head) the median rate | One mechanism covers both the tail and stragglers |
| Commit every 256 MiB or 2 s | 256 MiB or 1 s | Smaller re-fetch after a crash at negligible cost |
| `sync_file_range` to smooth writeback | Not built | Group commit already bounds dirty data |
| io_uring backend | Not built | `pwrite` has not been the bottleneck in measurements |
| Custom streaming tar parser | The `tar` crate, sequential, with qget's own safety checks and writer pool | The crate is mature; safety rules are enforced on its output |
| zstd seekable via the tail index (an extra request) | Frame walking on the stream | Works for seekable, pzstd and plain multi-frame without extra requests; single large frames fall back to serial |
| bao-tree for verified ranges | BLAKE3 hazmat chaining values per 1 MiB group | See above |
| iroh-blobs-like protocol | Own two-operation protocol with a capability secret | Smaller surface |
| SPAKE2 authenticates endpoint IDs | SPAKE2 + key confirmation, then the whole ticket encrypted under the PAKE key | The ticket contains a secret that must stay hidden from the mailbox |
| A default public mailbox | None; `--mailbox` / `QGET_MAILBOX` | Depends on hosting decisions (PRD Q8) |
| Mid-stream extraction checkpoints | `--keep-archive` only | Decoder state can't be saved at arbitrary points for most formats |
| gzip parallel decode | Serial | P2 in the PRD |

Next: [How qget compares](11-comparison.md).
