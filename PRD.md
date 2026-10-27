# qget — Product Requirements Document

| | |
|---|---|
| **Product** | qget |
| **Owner** | Dharrsan |
| **Version** | 0.1 (draft) |
| **Date** | 2026-10-28 |
| **Status** | Draft for review |
| **Supersedes** | Sections 3, 4, 6–8 of `PROPOSAL.md` |

> **How to read this document.** Every external claim is backed by a source in §17. Claims we could not verify are marked **[UNVERIFIED]**. Figures that come only from a vendor's own marketing are marked **[vendor claim]**. Performance numbers for qget are **targets**, not results. Nothing has been measured yet.

---

## Table of contents

1. Summary
2. Problem
3. Positioning and competitive analysis
4. Goals, non-goals, success metrics
5. Users and use cases
6. Design principles
7. Functional requirements
8. Non-functional requirements (performance, reliability, determinism, security)
9. Architecture
10. HTTP engine design
11. Stream-extract design
12. P2P design
13. Technology stack
14. Verification: benchmarks and testing
15. Release plan and milestones
16. Risks and open questions
17. References

---

## 1. Summary

qget is a command-line data-transfer engine written in Rust. It does three things in one binary:

1. **Fast, correct HTTP(S) downloads.** It adaptively uses many range connections, avoids stragglers, and resumes safely after crashes. It refuses to produce a corrupted file without telling you.
2. **Stream-extract.** It downloads a huge `.tar.lz4` / `.tar.zst` / `.tar.gz` archive in parallel and extracts it *while downloading*. The archive is never stored. This targets blockchain node snapshots and large datasets.
3. **Verified peer-to-peer transfer.** `qget yeet <file>` / `qget yoink <code>` sends a file directly between machines over QUIC with NAT traversal, and falls back to a relay when needed. The same engine can pull one file from HTTP mirrors and peers at once, with every byte verified against a BLAKE3 root hash.

The order of priorities is **correctness, then determinism, then performance, then features**. "Fastest" only matters if the output is always byte-exact or the tool fails with an explicit error.

---

## 2. Problem

### 2.1 Downloading large files is still unreliable and leaves performance unused

- **The tools people use have stalled or are heavy.** aria2's last release was 1.37.0 in November 2023 [R-aria2]. wget and curl transfer on a single connection by default. Surge is active but built around a TUI and a daemon, and it has correctness gaps (§3.2).
- **Many servers limit throughput per connection.** That includes CDNs, object stores and snapshot hosts, and it means a single connection rarely fills a fast link. AWS explicitly recommends concurrent byte-range GETs on separate connections "to fully use your NIC" [R-S3-patterns].
- **Splitting a file statically causes a slow tail.** The download waits on the slowest connection. Research shows dynamic block assignment plus duplicating the last blocks bounds the finish time to roughly the fastest server's rate [R-Rodriguez].
- **Existing resume implementations can corrupt silently.** They don't validate that the remote file is unchanged (ETag/If-Range), don't check `Content-Range`, and don't follow crash-safe write ordering (§3.2, [R-Pillai]).

### 2.2 Node operators double-handle huge archives

Operators routinely download 30 GB to 1 TB+ snapshots. The documented workflows today are:

- `wget` then `lz4 -d | tar -x`. This is sequential, uses 2× the disk, and takes 2× the time [R-Polkachu].
- `curl … | lz4 -d | tar -x`. This streams, but on **one connection** [R-Polkachu].
- `aria2c -x 5` or `-x 16 -s 16`, then `tar`. This is parallel but needs **2× disk and a second pass** [R-ChainLayer][R-Base].

Databricks built `fastar` and measured ~3× over `wget && tar` and ~2× over `aria2c && tar` [R-Fastar]. reth ships a stream-into-extractor mode limited to its own snapshots [R-Reth]. **No general-purpose CLI combines parallel ranged download, ordered streaming extraction, resume, and integrity verification.**

### 2.3 Peer-to-peer transfer tools force trade-offs

- croc and magic-wormhole are easy to use but have historically used TCP plus relays [R-croc][R-Wormhole].
- sendme (iroh) is fast and verified, but it is a demo-scale tool, and its underlying `iroh-blobs` is self-described as **not yet production quality** [R-iroh-blobs].
- No tool combines HTTP mirrors and peers into one verified download.

---

## 3. Positioning and competitive analysis

### 3.1 Positioning

| | Surge | aria2c | qget |
|---|---|---|---|
| Primary form | Interactive TUI + daemon + browser extension | CLI + RPC daemon | **Single-shot, scriptable CLI + embeddable library** |
| Primary user | Desktop power user | Power user / scripts | **Infra, node operators, CI, servers; also power users** |
| Core promise | "Blazing fast" downloads with a beautiful UI | Many protocols | **Byte-exact, verified, crash-safe, and as fast as the bottleneck allows** |

We do **not** compete with Surge on TUI, browser extension, or desktop download management in v1 (non-goals, §4.2). We compete on **engine quality**: correctness, determinism, throughput, CPU efficiency, stream-extract, and P2P.

### 3.2 Surge technical teardown

Source: SurgeDM/Surge `main` at commit `9de4cfa` (2026-10-27), Go, MIT. All findings come from reading the source and docs [R-Surge]. Items marked *inferred* were derived from code and not reproduced at runtime.

| Area | Surge today | Gap qget exploits |
|---|---|---|
| **Initial connections** | `round(sqrt(sizeMiB))`, capped by `max_connections_per_host` (default 16, max 64). The docs say "16 by default", which is misleading below ~225 MiB | Measurement-driven concurrency (§10.3) |
| **Chunking** | Parallel mode: one large range per worker (`fileSize/numConns`, 4 KiB aligned, min 2 MiB). A 1 GiB file becomes 16 × 64 MiB | Dynamic small blocks with rate-scaled size [R-Rodriguez] |
| **Adaptive concurrency** | AIMD, **off by default**. It only reacts to throttling (429/503/repeated 403) and never probes upward for bandwidth | Online probing and a utility-based controller [R-Yildirim][R-Falcon] |
| **Work stealing** | Only when the queue is empty, one idle worker at a time, halving the largest remainder. The victim stops mid-response without draining, so the connection is likely not reused (*inferred*). Open issue #523: skewed pieces | Rate-aware splits that keep connections reusable |
| **Slow-worker handling** | Cancels a worker below 0.3× the *arithmetic mean* speed, or idle 3 s. The remainder goes to the **back** of the queue and the worker pays a fresh TCP+TLS handshake | Median/percentile-based hedging that doesn't kill the original until the hedge wins [R-TailAtScale] |
| **End-game** | **None** (no duplicate requests for the final bytes) | End-game duplication [R-Rodriguez][R-BEP52] |
| **Mirrors** | Probed with a `bytes=0-0` request. **Size/ETag not cross-checked against the primary**; auth headers dropped; no speed ranking | Identity-checked mirrors, rate-ranked |
| **Streaming mode** | 2 MiB pieces fetched in FIFO order by N workers. **No reorder buffer, no deadline-aware priority**; failed pieces go to the tail | Windowed reorder buffer with head-of-line hedging [R-libtorrent-streaming] |
| **Resume persistence** | Gob files; tmp+fsync+rename but **no directory fsync**; **snapshots only on pause/error/shutdown, no periodic checkpoint**. After a crash, likely restarts from zero (*inferred*) | Append-only CRC'd journal with crash-safe ordering [R-Pillai] |
| **Data durability** | Data file not fsynced before state save | fdatasync before journal commit |
| **Remote-change detection** | **No ETag / If-Range / Last-Modified validation** | Mandatory If-Range with strong validator [R-RFC9110] |
| **Range validation** | **`Content-Range` not checked** against the requested offset | Strict `Content-Range` + byte-count validation |
| **Integrity** | **No checksum verification for downloads** (issue #355 open) | Hashing during download, `Repr-Digest`, `.sha256`, manifests, BLAKE3 |
| **Protocols** | HTTP/1.1 only for downloads (h2 explicitly disabled); no HTTP/3, BitTorrent, Metalink, FTP | HTTP/1.1 + HTTP/2 default, HTTP/3 opt-in, P2P |
| **I/O** | Blocking `WriteAt` from a 512 KiB buffer under a per-task mutex; global bitmap lock; `Truncate` preallocation (sparse on Linux, zero-fill on NTFS → issue #711 "unusable on HDDs") | `fallocate`, positional writes from a bounded pool, optional io_uring |
| **CPU efficiency** | Issue #73: ~14% CPU vs aria2c ~3% on the same 3 GB ISO, *and* slower (84 vs 95 MB/s). 150 ms / 50 ms polling loops | Event-driven core, no polling, CPU budget as a requirement |
| **Extraction / P2P** | None | Core features |
| **Benchmarks** | README: 1 GB, 360 Mbps, Windows; aria2c flags not stated. Issue #66: maintainers call their own results "too inconsistent to trust" | Public, reproducible, netem-shaped matrix (§14) |
| **Security defaults** | API binds `0.0.0.0:1700`; remote TUI uses plain HTTP to private IPs; spoofed Chrome User-Agent by default | No listening sockets by default; honest User-Agent |

**What Surge does well, which we must match or exceed:** careful 429/503/403 handling (Retry-After honoured, coalesced cooldowns, per-host learned caps, throttle budget); global and per-download rate limits; disk-space pre-check; atomic finalisation with cross-filesystem fallback; single-instance lock; refresh of expired signed URLs; wide packaging (AUR, Homebrew, Nix, winget/scoop/choco, AppImage, Docker).

### 3.3 Other comparators

| Tool | Notes |
|---|---|
| **aria2c** | C++, GPL-2.0, ★43k. Max 16 connections per server. Last release Nov 2023 [R-aria2]. Can't stream to stdout in a synchronised way [R-Fastar]. **Primary benchmark baseline**, always run with tuned flags (`-x16 -s16 -k1M --file-allocation=falloc`) |
| **axel** | C, GPL-2.0, ★3.4k, simple static split [R-axel]. Secondary baseline |
| **wget2 / curl** | Single-file single-connection baselines. Also `curl \| lz4 \| tar` for streaming |
| **fastar** (Databricks) | Prior art for parallel download plus parallel tar write [R-Fastar] |
| **reth download** | Prior art for streaming into an extractor, chain-specific [R-Reth] |
| **croc / magic-wormhole / sendme** | P2P baselines [R-croc][R-Wormhole][R-sendme] |

**Licensing:** aria2 and axel are GPL-2.0. We may study their behaviour but must not copy code into qget (MIT/Apache-2.0). Surge is MIT, so its ideas can be used with attribution, but we reimplement rather than copy.

---

## 4. Goals, non-goals, success metrics

### 4.1 Goals

| ID | Goal |
|---|---|
| G1 | **Never silently produce a wrong file.** Every completed download is byte-exact with the source (verified against a hash when one is available), or qget exits with a typed, documented error |
| G2 | **Deterministic behaviour.** The same inputs and seed give the same decisions. Every run can be replayed from its decision trace. Crash at any point, then resume, gives the same final bytes |
| G3 | **Saturate the bottleneck.** Reach ≥95% of the true bottleneck (link, server policy, CPU or disk) and tell the user which one it is |
| G4 | **Beat the field where it matters.** Measurably faster than Surge and tuned aria2c in scenarios with per-connection throttling, high RTT, and stragglers. Never slower in any benchmark scenario (§8.1) |
| G5 | **Stream-extract** of tar archives (lz4, zstd, gzip, xz, none) with bounded memory, finishing within a small margin of the download itself |
| G6 | **Verified P2P transfer and hybrid HTTP+peer downloads** over QUIC with NAT traversal and relay fallback |
| G7 | **Distro-grade engineering**: stable CLI, man page, completions, reproducible builds, signed releases, security policy, no telemetry |

### 4.2 Non-goals (v1)

- TUI dashboard, browser extension, background daemon, download queue manager (Surge's territory; revisit after 1.0)
- BitTorrent protocol compatibility, FTP, SFTP, HLS/DASH, Metalink v3
- Windows as a tier-1 platform for v1. Linux is tier 1, macOS tier 2, Windows best-effort
- Inventing new congestion control or new P2P transport protocols
- Bypassing server rate limits or terms of service. Polite defaults are a requirement (§8.4)

### 4.3 Success metrics

| Metric | Target at 1.0 |
|---|---|
| Silent-corruption incidents (field + fault-injection CI) | **0** |
| Fault-injection CI | ≥10,000 seeded simulation runs per release with 0 invariant violations |
| Throughput vs bottleneck (netem matrix) | ≥95% median |
| Speed vs Surge and tuned aria2c (benchmark matrix) | ≥1.0× in every scenario; ≥1.3× median in throttled/high-RTT/straggler scenarios |
| Stream-extract vs `aria2c && tar` (real snapshot, same host) | ≥1.8× wall-clock (fastar measured ~2× [R-Fastar]) |
| CPU per Gbit/s (plain HTTPS download, Linux x86-64) | ≤0.15 core per Gbit/s, compared against aria2c in the same run |
| P2P direct-connection rate (field telemetry is opt-in only; otherwise lab matrix) | Matches iroh's published characteristics; relay fallback 100% |
| Adoption | AUR at v0.1; Homebrew + Nixpkgs by v0.3; ≥1 official distro repo by 1.0 |

---

## 5. Users and use cases

| Persona | Need | Key features |
|---|---|---|
| **Node operator / SRE** | Restore a 300 GB `.tar.lz4` snapshot onto a fresh server as fast as possible, unattended, without running out of disk | Stream-extract, resume, checksum/manifest verification, `--max-buffer`, exit codes, JSON progress |
| **ML / data engineer** | Pull multi-GB datasets and model weights from S3/GCS/HF onto GPU boxes | Adaptive concurrency, signed-URL handling, high-RTT performance |
| **CI / platform engineer** | Drop-in replacement for `wget`/`curl -O` in scripts and Dockerfiles | wget-compatible flags, deterministic exit codes, no TTY assumptions, static binary |
| **Team moving big files between machines** | Send a 50 GB file to a colleague or another server behind NAT | `qget yeet` / `qget yoink`, resume, verification |
| **Fleet operator** | 20 nodes all need the same snapshot | Hybrid swarm: first node pulls from HTTP, the rest pull from HTTP + each other, verified |
| **Power user** | Fast downloads of ISOs and large files | Default mode, clean progress output |

---

## 6. Design principles

1. **Correctness over speed.** No optimisation may weaken G1. Every write that qget reports as done is verifiable.
2. **Deterministic core, nondeterministic edges.** Scheduling and protocol logic is a pure, sans-I/O state machine driven by events and a seed. Only the I/O shell touches the real network, disk and clock [R-quinn][R-turmoil][R-VOPR].
3. **Measure, don't guess.** Concurrency, chunk size and hedging come from live measurements, not hard-coded constants. Each decision is logged with the inputs that caused it.
4. **Know the bottleneck.** qget always knows and reports what limits it: network, server policy, decode, hash, or disk.
5. **Polite by default, fast by choice.** Defaults respect servers and other users of the network [R-Hacker-SC02][R-BBR-IETF97]. Higher aggression is opt-in and bounded.
6. **Boring, audited dependencies.** Use proven libraries and wrap unaudited crypto in the smallest possible exposure (§12.6).

---

## 7. Functional requirements

Priority: **P0** = required for v0.1, **P1** = required for 1.0, **P2** = after 1.0.

### 7.1 HTTP downloading

| ID | Requirement | Pri |
|---|---|---|
| HTTP-01 | Download a URL to a file using parallel byte-range requests when the server supports ranges; single stream otherwise | P0 |
| HTTP-02 | Metadata probe: HEAD, falling back to `Range: bytes=0-0` GET on 403/405 (for presigned URLs). Capture size, strong ETag, Last-Modified, Accept-Ranges, Content-Disposition, final URL after redirects | P0 |
| HTTP-03 | Every range request carries `If-Range` with the strong ETag, or Last-Modified if no ETag. Weak validators are never used for ranges [R-RFC9110] | P0 |
| HTTP-04 | Validate each 206 response: `Content-Range` start/end/total must equal the request and the known size. A `200` when `206` was expected is a hard error for that range, never stitched. Byte count received must equal the length requested | P0 |
| HTTP-05 | Adaptive concurrency controller (§10.3) with a per-host default cap of 16 and an opt-in hard maximum of 64 | P0 |
| HTTP-06 | Dynamic block scheduling with rate-scaled block size (§10.2) | P0 |
| HTTP-07 | Straggler hedging and end-game duplication (§10.4) | P0 |
| HTTP-08 | Throttle handling: honour `Retry-After`; jittered exponential backoff on 408/429/503/5xx; per-host cooldown shared across the process; multiplicative decrease of the host cap on throttle [R-S3-patterns][R-GCS-rate] | P0 |
| HTTP-09 | Retries: per-range retry budget, then whole-download retry budget; new connection and **fresh DNS lookup** on retry [R-S3-patterns] | P0 |
| HTTP-10 | Idle-read timeout per connection (default 15 s without bytes) plus connect/TLS/header timeouts | P0 |
| HTTP-11 | Multiple mirrors: `qget URL1 URL2 …` or `--mirror`. Each mirror is identity-checked (size + strong ETag, or a known hash) before use; mirrors are ranked live by throughput | P1 |
| HTTP-12 | Expired signed URLs: detect 403 mid-download and support `--refresh-cmd` / `qget refresh` to resume with a new URL after validating identity | P1 |
| HTTP-13 | HTTP/2 when the server negotiates it, with flow-control windows set to ≥ BDP; multiple h2 *connections*, not multiple streams on one connection, for bulk ranges [R-RFC9113] | P1 |
| HTTP-14 | HTTP/3 opt-in (`--http3`). **Not the default for bulk transfer** because of documented receiver-side CPU limits above ~500 Mbps [R-QUIC-WWW24][R-QUIC-IFIP25] | P2 |
| HTTP-15 | Proxy support (env vars, `--proxy`), custom headers, cookies file, basic/bearer auth, `--insecure` (loud warning) | P0 |
| HTTP-16 | Happy Eyeballs v2 dual-stack connection racing (RFC 8305); `--ipv4`/`--ipv6` | P1 |
| HTTP-17 | Rate limit: global and per-download token bucket (`--limit-rate`) | P0 |
| HTTP-18 | Batch input: `-i urls.txt` with bounded parallel downloads | P1 |

### 7.2 Resume and durability

| ID | Requirement | Pri |
|---|---|---|
| DUR-01 | Downloads write to `<name>.qget-part` plus a journal `<name>.qget-journal`; on success, atomic rename to `<name>` followed by fsync of the parent directory [R-Pillai] | P0 |
| DUR-02 | The journal is append-only. Each record holds a length, CRC32C, and payload. A torn or invalid final record is discarded on recovery [R-Pillai] | P0 |
| DUR-03 | Write ordering: `pwrite` data, then `fdatasync(data)` (batched by group commit, default every 256 MiB or 2 s), then append journal record, then `fdatasync(journal)`. A range is recorded as complete only after its data is durable | P0 |
| DUR-04 | Journal compaction: write a snapshot to tmp, fsync it, rename, fsync the directory | P0 |
| DUR-05 | Resume validates identity (size + strong ETag/Last-Modified, or hash) before reusing any bytes. A mismatch is a typed error with a clear message, never a silent splice | P0 |
| DUR-06 | `kill -9` or power loss at any instant, then rerun the same command → the final file is byte-exact. Verified in simulation (§14.3) and with real crash tests | P0 |
| DUR-07 | Preallocate with `fallocate` (Linux) / `F_PREALLOCATE` (macOS) so disk-full fails at start, not at 90% [R-fallocate] | P0 |
| DUR-08 | Disk-space pre-check with a safety margin; for stream-extract, check against the *uncompressed* size when known (tar headers, zstd frame content size, seek table) | P0 |
| DUR-09 | A single-instance lock per destination | P0 |

### 7.3 Integrity

| ID | Requirement | Pri |
|---|---|---|
| INT-01 | `--checksum sha256:<hex>` (also sha512, blake3); hash computed during download, no second pass | P0 |
| INT-02 | Auto-discover `<url>.sha256` / `.sha256sum` / `SHA256SUMS` when `--verify=auto` (default on for stream-extract) | P1 |
| INT-03 | Honour `Repr-Digest` / `Content-Digest` response headers (RFC 9530) when present | P1 |
| INT-04 | Manifest support: per-part checksum manifests, as Arbitrum publishes [R-Arbitrum] | P1 |
| INT-05 | BLAKE3 content ID with bao-style verified ranges for P2P and hybrid downloads (§12.5) | P1 |
| INT-06 | Optional `--verify-after` full re-read and hash of the finished file from disk | P1 |
| INT-07 | When a hash is known, verify before the final rename. A mismatch keeps the part file, writes a diagnostic report, and exits with a distinct code | P0 |

### 7.4 Stream-extract

| ID | Requirement | Pri |
|---|---|---|
| EXT-01 | `qget URL --extract DIR` for `.tar`, `.tar.lz4`, `.tar.zst`, `.tar.gz`, `.tar.xz`; format detected from magic bytes, not the extension | P0 (lz4, zst, plain tar), P1 (gz, xz) |
| EXT-02 | Windowed, ordered reorder buffer with bounded memory `--max-buffer` (default 1 GiB, minimum 64 MiB) | P0 |
| EXT-03 | Parallel decode where the format allows: LZ4 independent blocks, zstd multi-frame/seekable/pzstd, xz multi-block. Serial decode otherwise, with a "decode-bound" warning when the decoder is the bottleneck [R-LZ4-frame][R-zstd-seekable][R-xz] | P0 (lz4 independent, zstd serial), P1 (zstd parallel, xz) |
| EXT-04 | Parallel file writers fed by a single tar header parser; directories created once; metadata (mtime/mode) applied after data; one `syncfs` at the end, not per-file fsync | P0 |
| EXT-05 | Extract into a staging directory `DIR.qget-staging`, renamed atomically into place after the final hash matches. On mismatch, the staging directory is kept for inspection and is never presented as complete | P0 |
| EXT-06 | Path-traversal and link safety: reject absolute paths, `..`, and symlinks or hardlinks escaping the root; `--same-owner` off by default | P0 |
| EXT-07 | Resumable stream-extract at checkpoint boundaries (§11.5); `--keep-archive` mode that also writes the archive for fully resumable operation | P1 |
| EXT-08 | `qget URL -O - \| tool` streaming to stdout in order (plain pipe mode) | P0 |
| EXT-09 | Per-stage throughput metrics: network, reorder wait, decode, hash, write | P0 |

### 7.5 P2P

| ID | Requirement | Pri |
|---|---|---|
| P2P-01 | `qget yeet <path>` prints a short code (≥3 wordlist words) and a `qget://` ticket; `qget yoink <code\|ticket>` receives. Aliases: `send` / `receive` | P1 |
| P2P-02 | Short codes are authenticated with SPAKE2 (symmetric) over a rendezvous mailbox. The PAKE key authenticates **only** the transcript exchanging endpoint IDs and the BLAKE3 root; transport encryption is QUIC/TLS with raw public keys (§12.6) | P1 |
| P2P-03 | Direct connection via QUIC NAT traversal; automatic fallback to relay; seamless migration relay ↔ direct mid-transfer [R-iroh-1.0] | P1 |
| P2P-04 | Every received byte is verified against the BLAKE3 root before it is written (bao ranges) | P1 |
| P2P-05 | Resume of P2P transfers from the verified-chunk bitmap | P1 |
| P2P-06 | Directories (sent as a collection of files) | P1 |
| P2P-07 | Hybrid: `qget blake3:<root> --mirror URL --peer <ticket> …` pulls from HTTP and peers at once, verifying every byte against the root | P2 |
| P2P-08 | Swarm: receivers can re-serve verified chunks to others (`--seed`, off by default; LEDBAT-like low-priority seeding optional) [R-RFC6817] | P2 |
| P2P-09 | Self-hostable relay (`qget relay` or the upstream iroh-relay) and the ability to point to custom relays | P1 |

### 7.6 CLI, UX, observability

| ID | Requirement | Pri |
|---|---|---|
| CLI-01 | wget-compatible subset: `-O`, `-c`, `-q`, `-i`, `-P`, `--header`, `--user-agent`, `--limit-rate`, `--tries`, `--timeout`, `--no-check-certificate` | P0 |
| CLI-02 | Progress on stderr with TTY detection; `--progress=json` streams NDJSON events for automation | P0 |
| CLI-03 | Documented, stable exit codes (e.g. network, HTTP status, integrity mismatch, remote changed, disk full, interrupted-resumable) | P0 |
| CLI-04 | `--explain` prints the bottleneck analysis and key scheduler decisions at the end | P1 |
| CLI-05 | `--trace FILE` writes the full decision trace (seed, inputs, decisions) for replay | P1 |
| CLI-06 | Man page and bash/zsh/fish completions generated from the CLI definition | P0 |
| CLI-07 | Config file (`$XDG_CONFIG_HOME/qget/config.toml`) with the same keys as the flags | P1 |
| CLI-08 | Honest User-Agent `qget/<version>` (overridable) | P0 |
| CLI-09 | `libqget` crate with a stable public API for embedding (e.g. by node clients) | P2 |

---

## 8. Non-functional requirements

### 8.1 Performance

All targets are measured with the benchmark harness (§14). "Bottleneck" means the minimum of link capacity, server-imposed aggregate limit, decode rate, hash rate, and disk write rate, each measured independently on the test host.

| ID | Requirement |
|---|---|
| PERF-01 | **Throughput:** ≥95% of the bottleneck (median of 10 runs) across the netem matrix |
| PERF-02 | **Versus competitors:** ≥1.0× Surge (max connections 16 *and* 64) and tuned aria2c in every matrix cell; ≥1.3× median in cells with per-connection throttling, RTT ≥ 100 ms, or injected stragglers |
| PERF-03 | **Ramp-up:** reach 90% of steady-state throughput within 3 s on RTT ≤ 100 ms (Falcon reports convergence "in as little as 20 seconds" for full optimisation [R-Falcon]; we only need a near-optimal starting point fast and keep refining) |
| PERF-04 | **Tail:** with one injected straggler connection at 10% speed, completion time is within 5% of the no-straggler run |
| PERF-05 | **CPU:** ≤0.15 core per Gbit/s for plain HTTPS download on Linux x86-64 with AES-NI; no busy-polling; idle CPU ≈ 0 while waiting on a throttled server |
| PERF-06 | **Memory:** plain download ≤ 64 MiB RSS + 1 MiB per connection; stream-extract ≤ `--max-buffer` + 128 MiB |
| PERF-07 | **Stream-extract:** wall-clock ≤ 1.1× the plain download time of the same archive when the decoder is not the bottleneck |
| PERF-08 | **Startup:** < 20 ms process start to first request (excluding DNS/TLS) |
| PERF-09 | **Binary:** static Linux binary ≤ 15 MiB with P2P; ≤ 8 MiB HTTP-only build |

**Physical limits we will document rather than hide** [R-LZ4-frame][R-zstd][R-rapidgzip]:

| Format | Single-core decode (compressed input) | Achievable |
|---|---|---|
| lz4 (independent blocks, the CLI default) | ~1.8–2.4 GB/s ≈ 15–19 Gbit (desktop) | 10 Gbit single-core; 25–100 Gbit with parallel blocks, then disk-bound |
| zstd single frame | ~0.5 GB/s ≈ 4 Gbit | Decode-bound above ~4 Gbit |
| zstd multi-frame / seekable / pzstd | Scales with cores | 25+ Gbit |
| gzip (zlib / ISA-L) | ~0.14 / ~0.4 GB/s | Decode-bound above ~1–3 Gbit unless rapidgzip-style decoding is implemented (P2) |
| Whole-file SHA-256 | ~1.5–2 GB/s per core **[UNVERIFIED]** | Hash-bound above ~12–16 Gbit unless per-part hashes are available |

### 8.2 Reliability

| ID | Requirement |
|---|---|
| REL-01 | Zero silent corruption: every completed output is verified (hash if known; otherwise range/size/validator checks) |
| REL-02 | Crash-consistency: resume after a crash at any instant is byte-exact (DUR-06) |
| REL-03 | No unbounded waits: every I/O has a timeout or an idle-deadline; every retry loop has a budget |
| REL-04 | Graceful degradation: no range support → single stream with resume via Range if possible; HTTP/2 or HTTP/3 failure → HTTP/1.1; P2P direct failure → relay |
| REL-05 | Assertions on invariants stay enabled in release builds (TigerBeetle practice) [R-VOPR] |
| REL-06 | Panics never leave a "complete-looking" output: finalisation only happens through the verified rename path |

### 8.3 Determinism

Networks are not deterministic. qget guarantees a **deterministic outcome** (a byte-exact verified file, or a typed error) and a **reproducible decision trace**.

| ID | Requirement |
|---|---|
| DET-01 | Scheduler, concurrency controller, hedging, and P2P session logic are pure functions of `(state, event, seed)` with no direct I/O, clock or RNG access |
| DET-02 | All randomness (jitter, tie-breaks, probe order) comes from one seeded PRNG; the seed is logged and settable via `--seed` |
| DET-03 | Rate estimates enter the scheduler quantised into buckets, so small timing noise does not change decisions |
| DET-04 | Block boundaries are a deterministic function of file size and configuration; hedged/stolen ranges are recorded in the journal so the final layout doesn't depend on which copy won |
| DET-05 | Replaying a `--trace` in the simulator reproduces every decision exactly |
| DET-06 | Explicit state machines (enums with exhaustive transitions) for download, range, connection, and P2P session lifecycles |

### 8.4 Politeness and network citizenship

| ID | Requirement |
|---|---|
| NET-01 | Default per-host cap 16 connections; the controller only goes above its starting point when measured throughput improves. Parallel flows take a larger share at shared bottlenecks [R-Hacker-SC02][R-BBR-IETF97], so `--polite` (cap 4, LEDBAT-style yielding for P2P seeding) is available |
| NET-02 | Throttling signals (429/503/`Retry-After`) always reduce load; they are never retried around by opening more connections |
| NET-03 | GCS-style ramp guidance respected when talking to known object stores (no sudden 10× request-rate spikes) [R-GCS-rate] |
| NET-04 | Optional `--congestion=bbr` (Linux `TCP_CONGESTION`), best-effort, warning on EPERM. Unprivileged processes are limited to `tcp_allowed_congestion_control` [R-tcp7] |

### 8.5 Security

| ID | Requirement |
|---|---|
| SEC-01 | TLS via rustls; certificate verification on by default; `--insecure` prints a warning on every run |
| SEC-02 | No listening sockets in the default HTTP mode. P2P listens only during `yeet`/`seed` |
| SEC-03 | Tar extraction safety (EXT-06); refuse device nodes unless `--allow-special`; cap path length and entry count |
| SEC-04 | P2P: the PAKE exposure is limited to the authentication transcript; single-use codes; mailbox rate-limits guesses; codes ≥3 words |
| SEC-05 | Release signing (minisign or sigstore), SBOM, reproducible builds, `cargo-deny` / `cargo-audit` gates in CI |
| SEC-06 | `SECURITY.md` with a disclosure process and a supported-versions policy |
| SEC-07 | No telemetry. Opt-in anonymous benchmark submission only |
| SEC-08 | Fuzzing (cargo-fuzz) for the HTTP header parsers, Content-Range parser, journal decoder, tar parser and decompressor framing |

---

## 9. Architecture

```
                         ┌────────────────────────────────────────────┐
     qget CLI  ───────►  │                 Controller                 │
   (clap, config)        │  pure state machine: (state, event, seed)  │
                         │  ├─ Concurrency controller  (§10.3)        │
                         │  ├─ Block scheduler / window (§10.2, §11)  │
                         │  ├─ Hedging & end-game      (§10.4)        │
                         │  └─ Decision trace ──────► --trace         │
                         └───────┬────────────────────────▲───────────┘
                     commands    │                        │ events (bytes, errors,
                                 ▼                        │  timers, completions)
   ┌─────────────────────── Sources (I/O shell, tokio) ───┴────────────────────┐
   │  HTTP/1.1 & HTTP/2 (hyper + rustls)   HTTP/3 (opt-in)   P2P peers (iroh)  │
   │  All implement: fetch(range) -> stream of verified-or-validated bytes     │
   └──────────────────────────────────────┬────────────────────────────────────┘
                                          ▼
                    ┌───────────────── Integrity ──────────────────┐
                    │ Content-Range + byte count + If-Range        │
                    │ BLAKE3/bao range verify (P2P, hybrid)        │
                    │ streaming SHA-256/512/BLAKE3 (ordered tap)   │
                    └───────────────┬──────────────────────────────┘
               ┌────────────────────┴─────────────────────┐
               ▼                                          ▼
   ┌───── File sink ──────┐                 ┌──── Stream sink ─────────────────┐
   │ fallocate, pwrite    │                 │ reorder window (bounded)         │
   │ (thread pool or      │                 │  → ordered tap (hash, checkpoint)│
   │  io_uring)           │                 │  → decoder (parallel if format   │
   │ group-commit fsync   │                 │    allows)                       │
   └──────────┬───────────┘                 │  → tar parser → writer pool      │
              ▼                             │  → staging dir → atomic rename   │
   ┌───── Journal ────────┐                 └──────────────────────────────────┘
   │ append-only, CRC32C  │
   │ crash-safe ordering  │
   └──────────────────────┘
```

**Key architectural decisions**

| # | Decision | Rationale |
|---|---|---|
| AD-1 | **Sans-I/O controller**, with a tokio I/O shell around it | Makes determinism (DET-01..05) and simulation testing possible; same pattern as quinn-proto / noq-proto [R-quinn][R-iroh-noq] |
| AD-2 | **One `Source` abstraction** for HTTP, HTTP/3 and P2P | Enables hybrid and multi-mirror downloads with no special cases |
| AD-3 | **Verify, then write.** Bytes never reach the file or the decoder before passing the validation available for that source | G1 |
| AD-4 | **TCP for HTTP bulk by default, QUIC for P2P** | On fast links TCP+HTTP/2 beats QUIC+HTTP/3 by up to 45% because of receiver-side cost [R-QUIC-WWW24]. For P2P, QUIC is needed for NAT traversal and path migration [R-Tailscale-NAT][R-iroh-1.0] |
| AD-5 | **Multi-source aggregation at the application layer**, not via multipath QUIC | The multipath QUIC draft does not specify scheduling across paths [R-MPQUIC]; we need deterministic control |
| AD-6 | **No fountain codes by default** | Over reliable QUIC they add CPU cost and complicate verification and determinism [R-raptorq]; kept as a research option for one-to-many multicast |

---

## 10. HTTP engine design

### 10.1 Connection model

- Use N independent HTTP/1.1 keep-alive connections, each carrying one range request at a time. HTTP/2 multiplexing on a single TCP connection does not add TCP parallelism, and initial h2 windows are only 65,535 bytes [R-RFC9113].
- When the server is HTTP/2-only, use N separate h2 connections with `SETTINGS_INITIAL_WINDOW_SIZE` and connection `WINDOW_UPDATE` ≥ estimated BDP.
- Leave kernel receive-buffer autotuning on: don't set `SO_RCVBUF`, because that disables autotuning [R-tcp7]. Document `tcp_rmem` tuning for >1 Gbit × high-RTT paths: a single flow needs a window ≥ BDP, e.g. 1 Gbit × 100 ms ≈ 12.5 MB [R-BBR-IETF97].
- TLS session resumption shared across connections to a host.

### 10.2 Block scheduling

- Split the file into **blocks**, not one range per worker. Each idle connection takes the next block, so faster connections naturally do more work [R-Rodriguez].
- **Block size is rate-scaled**: aim for each block to take ≥1 s at the observed per-connection rate (so the one-RTT gap between blocks costs ≤ a few percent), clamped to `[4 MiB, 64 MiB]`, starting at 8 MiB (S3/GCS guidance: 8–32 MiB slices are typical [R-GCS-sliced]; the "8–16 MB" S3 figure is **[UNVERIFIED]**).
- The next request is sent immediately when a block's final bytes arrive, to avoid idle time on the connection.
- Block boundaries are deterministic for a given (size, config). Splits from hedging or stealing are journalled (DET-04).

### 10.3 Concurrency controller

The goal is to find the connection count that maximises throughput without adding loss, then hold it there. Research shows returns flatten quickly: a knee around ~10 sockets at MTU 1500 on a 60–70 ms path [R-Hacker-IPDPS02], and "too many streams may have an inverse effect" [R-Yildirim].

1. **Start** at `min(4, cap)` connections.
2. **Probe phase.** Sample throughput at 3 concurrency levels (e.g. 4 → 8 → 16, each held for max(1 s, 4 × RTT)). Fit the Yildirim et al. model, which needs only three samples to predict the optimum [R-Yildirim].
3. **Track phase.** Hill-climb / gradient step on a **utility that penalises connection count and loss/retransmits**, following Falcon: throughput-only objectives "overburden both networks and storage systems" [R-Falcon]. Exact utility constants to be confirmed from the TPDS 2023 paper before implementation **[UNVERIFIED constants]**.
4. **Throttle override.** On 429/503 or Retry-After: multiplicative decrease (halve), host cooldown, then additive increase after healthy progress. This matches Surge's good behaviour but runs by default.
5. All steps are a deterministic schedule driven by quantised measurements and the seed (DET-02/03).

### 10.4 Stragglers, hedging, end-game

- **Detection:** per-connection throughput EWMA compared with the **median**, not the mean (a few very fast connections shouldn't trigger needless restarts, which is a flaw in Surge's approach).
- **Hedge, don't kill.** When a block's projected finish time exceeds the p95 of recent block times, request its **unfinished tail** on another (preferably already-warm) connection. Keep whichever finishes first and cancel the other. This is the "Tail at Scale" hedged request: the paper reports p99.9 dropping from 1,800 ms to 74 ms for 2% extra requests [R-TailAtScale]. AWS gives the same advice for large objects: "retry the slowest 5 percent" [R-S3-patterns].
- **End-game.** When remaining blocks < active connections, duplicate the remaining in-flight blocks across idle connections. Worst-case waste is bounded by (connections − 1) × block size [R-Rodriguez].
- **Budget.** Hedged bytes are capped (default ≤5% of file size) and reported in `--explain`.

### 10.5 Disk I/O

- `fallocate` the whole file at start [R-fallocate]. On HDDs (detected via `/sys/block/*/queue/rotational`), use larger blocks and fewer connections to reduce seeks [R-GCS-sliced].
- Positional writes (`pwrite`) from a small bounded blocking pool by default. **io_uring as an opt-in backend** (no SQPOLL), enabled only after it beats `pwrite` in our benchmarks. Polling modes can collapse under core shortage [R-Didona]; at downloader rates the disk is usually not the bottleneck [R-HaasLeis].
- `sync_file_range(SYNC_FILE_RANGE_WRITE)` only to smooth writeback bursts. It is **not** used for durability [R-sync_file_range].
- No O_DIRECT by default [R-open2].

---

## 11. Stream-extract design

### 11.1 Pipeline

```
 N range connections ──► reorder window [head, head+W) ──► ordered tap ──► decoder ──► tar parser ──► writer pool ──► staging dir
                               ▲                              │  │                                                      │
                               │ backpressure                 │  └─► hash (sha256/blake3/manifest part)                │
                               └──────────────────────────────┘  └─► checkpoint log                     verify ─► atomic rename
```

### 11.2 Reorder window scheduler

- Blocks are allocated in offset order and **never beyond the window**. W bounds memory: default `W ≈ N × C × 2`, capped by `--max-buffer`.
- **Deadline-aware assignment**, adapted from libtorrent's time-critical picker: send the block nearest the head to the connection with the lowest expected queue time, and avoid the slowest ~10% of connections for head blocks [R-libtorrent-streaming].
- **Head-of-line hedging:** if the head block's estimated finish time exceeds 2× the median block time, hedge its remaining range on the fastest idle connection.
- When the window is full and the head is missing, downloads ahead of the head pause. That is the backpressure.

### 11.3 Format probing and decode plans

A HEAD request plus two small Range reads (head and tail of the file) pick one of three decode plans *before* the bulk download starts:

| Plan | Formats | How |
|---|---|---|
| **Indexed parallel** | zstd seekable (seek table in a tail skippable frame, magic `0x8F92EAB1`) [R-zstd-seekable]; xz multi-block (Index at end) [R-xz] | Read the index from the tail; decode frames/blocks on a worker pool; reorder the output |
| **Header-walk parallel** | LZ4 with independent blocks (FLG bit 5 = 1; the `lz4` CLI default) [R-LZ4-frame]; pzstd (skippable headers give frame sizes) [R-pzstd] | Walk block headers in order (cheap), dispatch blocks to the pool |
| **Serial** | zstd single frame; LZ4 linked blocks (`-BD`); gzip | One dedicated decoder thread. Warn if decode-bound. gzip parallel decode (rapidgzip approach) is P2 [R-rapidgzip] |

Whether real-world snapshots (Polkachu, Solana `.tar.zst`, Base) are single-frame or multi-frame is **[UNVERIFIED]** and is the first task of milestone M2.

### 11.4 Tar extraction

- A single-threaded header parser hands each entry's bytes (owned buffers) to a writer pool. The Rust `tar` crate requires sequential reading [R-tar-crate], so we parse in order and write in parallel. Fastar uses the same split [R-Fastar].
- Large entries (e.g. `mdbx.dat`, reth static files) get `fallocate` and large sequential writes, and are disk-bound.
- Directories are cached; metadata is applied after the data; one `syncfs` at the end, then staging → final rename.

### 11.5 Resume for stream-extract

Streaming trades away easy resume; reth's streaming mode is explicitly non-resumable [R-Reth]. qget's approach:

- **Indexed / header-walk formats:** checkpoint `(compressed offset at a frame/block boundary, tar entry index, partial entry offset, hash state)` every K MiB. Resume restarts at the last checkpoint. The hash state is serialisable for BLAKE3; for SHA-256 we store the intermediate state.
- **Serial formats:** checkpoints are only possible at points where the decoder can restart. With no such points, resume re-downloads from the start and skips entries already extracted and verified. `--keep-archive` is offered as the fully resumable option.

### 11.6 Integrity in stream mode

- Hash the **ordered** bytes as they leave the reorder window (whole-file hash), or per-part hashes from a manifest (parallel, resumable) [R-Arbitrum].
- In-format checksums (LZ4 block/content xxHash32, zstd XXH64) are verified when present. They catch corruption, but they are not authentication [R-LZ4-frame][R-RFC8878].
- The staging directory is never renamed into place unless the final hash matches (EXT-05).

---

## 12. P2P design

### 12.1 Requirements recap

Reliability and determinism come first, then throughput, then ease of use. Every byte from a peer is untrusted until verified.

### 12.2 What the research says

- **NAT traversal is not guaranteed.**
  - 82% of NATs supported UDP hole punching and 64% supported TCP in 2005 [R-Ford].
  - A 2025 measurement of 4.4M libp2p attempts found **70% ± 7.1%** hole-punch success, with TCP ≈ QUIC when RTT-synchronised, and 97.6% of successes on the first attempt [R-Trautwein].
  - iroh reports "roughly 9 out of 10" conditions allow direct connections **[vendor claim]** [R-iroh-relays].
  - **Design for 10–30% of sessions being relayed. The relay is a primary path, not an edge case.**
- **Relays must run on HTTPS/443** to get through restrictive networks. Tailscale DERP and iroh-relay both do this [R-Tailscale-NAT][R-iroh-relays].
- **QUIC throughput depends heavily on the implementation and on receive-side offloads.**
  - On 10G, implementations ranged from 90 to 4,900 Mbit/s [R-QUIC-Highway].
  - GSO/GRO raised one stack from 3.7 to 4.94 Gbit/s [R-QUIC-IFIP25].
  - Default UDP buffers should be raised "by at least an order of magnitude" [R-QUIC-Highway].
- **Multipath QUIC** (draft-21, in the RFC Editor queue) gives seamless relay↔direct failover but leaves cross-path scheduling unspecified [R-MPQUIC].

### 12.3 Chosen design

| Layer | Choice | Why |
|---|---|---|
| Connectivity | **iroh 1.x** (1.0 shipped 2026-06-15 with wire/API stability within v1; 1.4.0 on 2026-10-19; MIT/Apache-2.0) [R-iroh-1.0] | Dial-by-public-key, QUIC NAT traversal inside QUIC, relay fallback, address discovery via DNS/pkarr/mDNS |
| QUIC | **noq** (iroh's Quinn fork with multipath, address discovery and NAT traversal) [R-iroh-noq] | Relay and direct are both QUIC paths with per-path congestion state. Path changes underneath a single-path stack caused ~20 s latency settling in the past [R-iroh-fork] |
| Relay | **Self-hosted iroh-relay**, ≥2 regions, HTTPS/443 | Don't depend on rate-limited public relays; full control over capacity |
| Congestion control | **Cubic** default; BBR opt-in after measurement (quinn marks BBR "Experimental") [R-quinn-cc] | Predictability over peak numbers |
| Socket tuning | Large UDP send/receive buffers; GSO/GRO verified at startup and reported | [R-QUIC-Highway][R-QUIC-IFIP25] |
| Transfer protocol | **qget's own range-request protocol over iroh streams, using `bao-tree`** for verified ranges. Modelled on iroh-blobs, which itself is **not** used for production until upstream marks it production-ready | iroh-blobs README: "not yet considered production quality"; security fix 2026-10-08 [R-iroh-blobs] |
| Integrity | **BLAKE3 root + bao outboard**; verify every range before writing | Range verification from untrusted sources; ~6% outboard overhead at default settings [R-bao] |
| Short-code auth | **SPAKE2** (symmetric, RustCrypto `spake2`), migrating to **CPace** once its RFC is published and an audited crate exists | RFC 9382 [R-RFC9382]; CPace is the CFRG-selected PAKE, now "Sent to the RFC Editor" [R-CPace]; `spake2` crate is unaudited, so its exposure is minimised (§12.6) |
| Session durability | **magic-wormhole Dilation patterns**: deterministic leader election, connection generation numbers, acked replay on reconnect [R-Wormhole] | Proven reconnect semantics, deterministic |
| Multi-source scheduling | **Deterministic, seeded rarest-first + end-game**, written as a pure function; no BitTorrent choking/tit-for-tat | Choking adds deliberate non-determinism to solve an incentive problem we don't have [R-BEP52] |
| Fountain codes | Not used by default (RaptorQ decodes ~5–8.7 Gbit/s per core on a high-end desktop [R-raptorq]) | CPU, verification and determinism costs; kept for research |

### 12.4 Flows

**Send / receive (yeet/yoink)**
1. The sender hashes the file (BLAKE3, parallel) and builds the bao outboard.
2. The sender connects to the rendezvous mailbox and receives a channel number. The code is `<channel>-<word>-<word>-<word>`.
3. Both sides run SPAKE2 over the mailbox with the code as the password.
4. They exchange `(EndpointId, BLAKE3 root, size, name)`, authenticated with a MAC under the PAKE key and bound to both EndpointIds.
5. The receiver dials the sender's EndpointId via iroh. Data flows direct or via relay, migrating when hole punching succeeds.
6. The receiver requests ranges, verifies each against the root, writes, and journals.
7. On completion: root match → atomic rename.

**Hybrid (P2)**
`qget blake3:<root> --mirror https://… --peer <ticket>`. HTTP mirrors and peers are all `Source`s. HTTP bytes are verified against the bao tree (fetched from a peer or alongside the mirror) exactly like peer bytes. **Hybrid requires a known BLAKE3 root.** A plain URL with no published root cannot safely be fetched from peers, so qget will not offer that.

### 12.5 Reliability targets for P2P

- Relay fallback must always work when the relay is reachable.
- A transfer survives path changes (Wi-Fi ↔ wired, relay ↔ direct) with no data loss and no duplicated writes.
- Any peer that sends a range failing verification is disconnected and banned for the session, and the range is re-requested elsewhere.

### 12.6 Cryptographic exposure

- The unaudited PAKE code only touches a short transcript.
- All bulk data is protected by iroh's QUIC/TLS with raw public keys.
- Codes are single-use; the mailbox rate-limits guesses; codes contain ≥3 words.

---

## 13. Technology stack

| Concern | Choice | Alternative considered |
|---|---|---|
| Language | Rust (stable, MSRV pinned) | Go: kept only as the prototype reference |
| Async runtime | tokio | monoio/glommio (io_uring-native, but a smaller ecosystem) |
| HTTP | hyper 1.x (+ hyper-util), custom pool | reqwest (too opinionated for per-connection control) |
| TLS | rustls with aws-lc-rs provider | native-tls/OpenSSL |
| DNS | hickory-resolver; Happy Eyeballs v2 | system resolver only |
| HTTP/3 (opt-in) | h3 + quinn, or iroh's noq if feature-compatible | quiche, s2n-quic |
| P2P | iroh 1.x / noq, bao-tree, blake3, spake2 | rust-libp2p (DCUtR ~70% hole-punch, measured [R-Trautwein]) |
| Hashing | blake3 (SIMD, multithreaded), sha2 with SHA-NI, crc32c | ring |
| Decompression | lz4_flex or lz4 bindings; zstd (libzstd bindings, seekable support); zlib-ng / ISA-L via bindings; liblzma for xz | pure-Rust codecs where they match native speed |
| Tar | custom streaming parser (security and parallel hand-off), `tar` crate as reference | `tar` crate only |
| Disk I/O | `pwrite` pool; optional io_uring backend (tokio-uring / io-uring crate) | O_DIRECT (rejected by default) |
| CLI | clap + clap_mangen + clap_complete | — |
| Simulation testing | turmoil (tokio-compatible, seeded faults) [R-turmoil]; property tests with proptest | madsim (needs patched crates) |
| Fuzzing | cargo-fuzz (libFuzzer) | — |
| Supply chain | cargo-deny, cargo-audit, cargo-vet; reproducible builds; minisign/sigstore | — |

---

## 14. Verification

### 14.1 Benchmark harness (public, reproducible)

- **Lab matrix**, using a local nginx/Caddy origin with `tc netem` and per-connection `limit_rate`:
  - RTT: 5, 50, 150, 300 ms
  - Loss: 0, 0.1%, 1%
  - Per-connection cap: none, 5 MB/s, 20 MB/s
  - Link: 1 Gbit, 10 Gbit
  - Stragglers: none, 1 connection at 10%
  - File sizes: 100 MB, 10 GB, 100 GB
- **Real-world:** S3, Cloudflare R2, GCS, Hetzner, and a public snapshot provider (Polkachu-style `.tar.lz4`).
- **Competitors**, all with documented flags:
  - `aria2c -x16 -s16 -k1M --file-allocation=falloc`
  - `surge` (max connections 16 and 64; adaptive on and off)
  - `axel -n 16`
  - `wget2 --max-threads=16`
  - `curl`
  - Stream-extract baselines: `curl | lz4 -d | tar -x`, `aria2c && tar`
- **Method:** 10 runs per cell, randomised order, page cache dropped between runs; report median, p95, CPU-seconds, peak RSS, bytes on the wire (to expose hedging overhead).
- **Publication:** raw CSVs, scripts, hardware and kernel versions. Cells where qget loses or ties are published too.

### 14.2 Correctness testing

- **Fault-injecting HTTP server** (in-process): short bodies, wrong `Content-Range`, 200 instead of 206, mid-stream resets, slowloris trickle, ETag changes mid-download, 429/503 storms, redirect chains, signed-URL expiry.
- **Crash testing:** run downloads, `SIGKILL` at random points (and, in VMs, simulated power loss with dm-flakey/dm-log-writes), resume, and compare hashes. Ordering follows ALICE-style crash vulnerability analysis [R-Pillai].
- **Archive corpus:** lz4 (independent/linked, all block sizes), zstd (single frame, multi-frame, seekable, pzstd, `--long=31`), gzip (single, multi-member), xz (single/multi-block), malicious tars (traversal, symlink escapes, huge headers).

### 14.3 Deterministic simulation testing

- The controller and P2P session logic run under turmoil with simulated network, clock and disk. Seeded faults: latency, loss, partitions, crashes, torn writes [R-turmoil].
- **Invariants checked continuously:** no byte written twice with different content; journal never claims a range whose data is not durable; final output equals source; memory never exceeds the window bound.
- **CI:** seed sweep on every PR (≥1,000 seeds); nightly ≥100,000 seeds; any failure is reproducible from `(seed, commit)`, as with TigerBeetle VOPR [R-VOPR].

### 14.4 Release gates

A release ships only if all of the following hold: all PERF targets met in the lab matrix, zero DST invariant violations in the nightly sweep, fuzzers clean for 24 h, crash-test suite passes, and benchmarks are published.

---

## 15. Release plan and milestones

| Milestone | Scope | Exit criteria |
|---|---|---|
| **M0: Foundations** | Repo, CI, licences (MIT/Apache-2.0), benchmark harness running against the Go prototype + competitors, name reservation | Baseline numbers published internally |
| **M1: Correct HTTP core** | HTTP-01..10, 15, 17; DUR-*; INT-01, 07; CLI-01..03, 06, 08; sans-I/O controller; DST harness | Fault-injection + crash suites green; ≥ parity with tuned aria2c in all lab cells |
| **M2: Fast HTTP** | Concurrency controller, rate-scaled blocks, hedging, end-game, HDD mode | PERF-01..06 met; ≥1.3× Surge/aria2c in throttled/high-RTT/straggler cells |
| **M3: Stream-extract (launch feature)** | EXT-01..06, 08, 09 for tar/lz4/zstd; survey of real snapshots' frame structure | ≥1.8× vs `aria2c && tar` on a real snapshot; **public v0.1 + AUR** |
| **M4: Hardening** | HTTP-11..13, 16, 18; INT-02..04, 06; EXT-07; gzip/xz; `--explain`, `--trace` replay; man page | v0.3; Homebrew + Nixpkgs; distro packaging requests filed |
| **M5: P2P** | P2P-01..06, 09; relay deployment | Lab NAT matrix (full-cone, restricted, symmetric, CGNAT) passes; relay fallback 100%; v0.5 |
| **M6: Hybrid & swarm** | P2P-07, 08; libqget API | v0.8 |
| **M7: 1.0** | CLI and API stability freeze, security review, reproducible builds | 1.0; ≥1 official distro repo |

Milestones are ordered by risk and value, not by calendar. Rough sizing for one developer learning Rust alongside (per the agreed learning plan): M0–M3 ≈ 4–6 months; M4–M5 ≈ 4–6 months.

---

## 16. Risks and open questions

### 16.1 Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Gains over tuned aria2c are small on unthrottled, low-RTT links | Weak headline | Lead with throttled/high-RTT/straggler cells and stream-extract, where gains are structural; publish ties honestly |
| Surge adds integrity checks / HTTP/2 / reorder buffer | Narrower gap | Our moat is the combination: correctness + determinism + stream-extract + P2P + CPU efficiency, all verified by public harnesses |
| Real snapshots are single-frame zstd (decode-bound ~4 Gbit) | Stream-extract can't scale on those | Detect and report honestly; advocate seekable/pzstd output to providers; offer `qget compress` helper (P2) |
| iroh API/behaviour regressions (e.g. 0.96 hole-punch regression after network changes) [R-iroh-blog] | P2P reliability | Pin versions, run our own NAT test matrix in CI, upstream fixes |
| Unaudited PAKE crate | Security | Minimal exposure (§12.6); migrate to CPace + audited crate |
| Aggressive parallelism hurts other users / triggers blocks | Reputation, distro rejection | Polite defaults, NET-* requirements, `--polite` |
| Solo developer learning Rust | Schedule | Sans-I/O core is testable in isolation; Claude used as reviewer/tutor; strict "explain every line" rule |
| Employer overlap (node-operator audience) | Legal/branding | Clarify ownership before public release (open question Q1) |

### 16.2 Open questions

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q1 | Personal or company project? Licence and branding implications | Dharrsan | M0 |
| Q2 | ~~Final name~~ Decided: `qget` | Dharrsan | M0 |
| Q3 | Exact Falcon utility function constants (confirm from TPDS 2023 paper) | Eng | M2 |
| Q4 | Frame/block structure of real Polkachu, Solana and Base snapshots | Eng | M3 start |
| Q5 | GSO/GRO and DPLPMTUD support status in noq; noq vs s2n-quic throughput on our hardware | Eng | M5 start |
| Q6 | Audited Rust CPace implementation available? | Eng | M5 |
| Q7 | SHA-256 throughput on target servers (to confirm the hash-bound threshold) | Eng | M3 |
| Q8 | Relay hosting budget and regions | Dharrsan | M5 |

---

## 17. References

All of these were opened during research on 2026-10-28 unless marked otherwise. "[abstract only]" means only the abstract or landing page was read.

**Competitors and prior art**
- [R-Surge] SurgeDM/Surge, commit `9de4cfa` (2026-10-27): source, `docs/OPTIMIZATIONS.md`, `docs/ARCHITECTURE.md`, `docs/SETTINGS.md`, issues #66, #73, #355, #523, #535, #711. https://github.com/SurgeDM/Surge
- [R-aria2] aria2 repository and releases (latest release-1.37.0, 2023-11-15). https://github.com/aria2/aria2
- [R-axel] axel. https://github.com/axel-download-accelerator/axel
- [R-Fastar] Databricks, "Creating a faster tar extractor" (2022). https://www.databricks.com/blog/2022/01/26/creating-a-faster-tar-extractor.html
- [R-Reth] reth `download` CLI docs. https://reth.rs/cli/reth/download
- [R-croc] schollz/croc. https://github.com/schollz/croc
- [R-Wormhole] magic-wormhole Dilation protocol. https://magic-wormhole.readthedocs.io/en/latest/dilation-protocol.html
- [R-sendme] iroh sendme. https://www.iroh.computer/sendme

**Parallel transfer and scheduling**
- [R-Hacker-IPDPS02] Hacker, Athey, Noble. "The End-to-End Performance Effects of Parallel TCP Sockets on a Lossy Wide-Area Network." IPDPS 2002. https://cpham.perso.univ-pau.fr/TCP/11_220.pdf
- [R-Hacker-SC02] Hacker, Noble, Athey. "The Effects of Systemic Packet Loss on Aggregate TCP Flows." SC 2002. [abstract only]
- [R-Rodriguez] Rodriguez, Biersack. "Dynamic Parallel Access to Replicated Content in the Internet." IEEE/ACM ToN 10(4), 2002. https://www.eurecom.fr/publication/838/download/ce-rodrpa-020801.pdf
- [R-Yildirim] Yildirim, Yin, Kosar. "Prediction of Optimal Parallelism Level in Wide Area Data Transfers." IEEE TPDS 22(12), 2011. https://cse.buffalo.edu/faculty/tkosar/papers/jrnl_tpds_2011-2.pdf
- [R-Falcon] Arifuzzaman, Arslan. "Online Optimization of File Transfers in High-Speed Networks." SC '21 (slides read in full; paper [abstract only]). https://www.sc21.supercomputing.org/proceedings/tech_paper/tech_paper_files/pap476s6.pdf ; Falcon TPDS 2023 [abstract only]
- [R-TailAtScale] Dean, Barroso. "The Tail at Scale." CACM 56(2), 2013. https://www.barroso.org/publications/TheTailAtScale.pdf
- [R-libtorrent-streaming] libtorrent, "Streaming" (time-critical piece picker). https://www.libtorrent.org/streaming.html

**Congestion control, HTTP, QUIC**
- [R-BBR-IETF97] Cardwell et al., BBR slides, IETF 97 ICCRG (2016). https://www.ietf.org/proceedings/97/slides/slides-97-iccrg-bbr-congestion-control-02.pdf ; ACM Queue article [abstract only] https://queue.acm.org/detail.cfm?id=3022184
- [R-tcp7] Linux `tcp(7)`. https://man7.org/linux/man-pages/man7/tcp.7.html
- [R-RFC9110] RFC 9110, HTTP Semantics. https://www.rfc-editor.org/rfc/rfc9110
- [R-RFC9113] RFC 9113, HTTP/2. https://www.rfc-editor.org/rfc/rfc9113
- [R-QUIC-WWW24] Zhang et al. "QUIC is not Quick Enough over Fast Internet." WWW 2024. https://arxiv.org/abs/2310.09423
- [R-QUIC-IFIP25] König, Rust, Zitterbart, Scheuermann. "Examining the Heterogeneous Throughput Performance Landscape of QUIC Implementations." IFIP Networking 2025. https://doc.tm.kit.edu/2025-Examining-the-Heterogeneous-Throughput-Performance-Landscape-of-QUIC-Implementations-Koenig-et-al.pdf
- [R-QUIC-Highway] Jaeger et al. "QUIC on the Highway: Evaluating Performance on High-rate Links." IFIP Networking 2023. https://arxiv.org/abs/2309.16395
- [R-QUIC-Steps] Kempf et al. "QUIC Steps: Evaluating Pacing Strategies in QUIC Implementations." CoNEXT 2025. https://arxiv.org/abs/2505.09222
- [R-Kakhki] Kakhki et al. "Taking a Long Look at QUIC." IMC 2017. https://david.choffnes.com/pubs/long-look-at-quic-imc17.pdf
- [R-MPQUIC] draft-ietf-quic-multipath-21. https://datatracker.ietf.org/doc/draft-ietf-quic-multipath/
- [R-quinn] quinn-rs/quinn. https://github.com/quinn-rs/quinn ; [R-quinn-cc] https://docs.rs/quinn/latest/quinn/congestion/

**Storage and crash consistency**
- [R-Pillai] Pillai et al. "All File Systems Are Not Created Equal." OSDI 2014. https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-pillai.pdf
- [R-Didona] Didona et al. "Understanding Modern Storage APIs." SYSTOR 2022. https://animeshtrivedi.github.io/files/papers/2022-systor-Understanding%20modern%20storage%20APIs:%20a%20systematic%20study%20of%20libaio,%20SPDK,%20and%20io_uring.pdf
- [R-HaasLeis] Haas, Leis. "What Modern NVMe Storage Can Do, And How To Exploit It." PVLDB 16, 2023. https://www.vldb.org/pvldb/vol16/p2090-haas.pdf
- [R-io_uring] Axboe. "Efficient IO with io_uring." 2019. https://kernel.dk/io_uring.pdf
- [R-open2] `open(2)`. https://man7.org/linux/man-pages/man2/open.2.html
- [R-fallocate] `fallocate(2)`. https://man7.org/linux/man-pages/man2/fallocate.2.html
- [R-sync_file_range] `sync_file_range(2)`. https://man7.org/linux/man-pages/man2/sync_file_range.2.html

**Cloud guidance**
- [R-S3-patterns] AWS S3 performance design patterns. https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-design-patterns.html ; guidelines https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-guidelines.html
- [R-GCS-rate] GCS request rate guidelines. https://cloud.google.com/storage/docs/request-rate
- [R-GCS-sliced] GCS sliced object downloads. https://cloud.google.com/storage/docs/sliced-object-downloads

**Compression and archives**
- [R-LZ4-frame] LZ4 frame format v1.6.5. https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md ; lz4 man page https://github.com/lz4/lz4/blob/dev/programs/lz4.1.md
- [R-RFC8878] RFC 8878, Zstandard. https://www.rfc-editor.org/rfc/rfc8878.txt
- [R-zstd] facebook/zstd README (lzbench figures). https://github.com/facebook/zstd
- [R-zstd-seekable] zstd seekable format. https://github.com/facebook/zstd/blob/dev/contrib/seekable_format/zstd_seekable_compression_format.md
- [R-pzstd] pzstd README. https://github.com/facebook/zstd/blob/dev/contrib/pzstd/README.md
- [R-rapidgzip] Knespel, Brunst. "Rapidgzip: Parallel Decompression and Seeking in Gzip Files Using Cache Prefetching." HPDC 2023. https://arxiv.org/abs/2308.08955
- [R-xz] xz(1) and file format. https://tukaani.org/xz/man/xz.1.html ; https://tukaani.org/xz/xz-file-format.txt
- [R-tar-crate] Rust `tar` crate. https://docs.rs/tar/latest/tar/struct.Archive.html

**Snapshot providers**
- [R-Polkachu] https://polkachu.com/tendermint_snapshots/osmosis ; https://polkachu.com/tendermint_snapshots/celestia
- [R-ChainLayer] https://github.com/chainlayer/quicksync-playbooks
- [R-Base] https://docs.base.org/base-chain/node-operators/snapshots
- [R-Arbitrum] https://docs.arbitrum.io/run-arbitrum-node/nitro/nitro-database-snapshots

**P2P**
- [R-Ford] Ford, Srisuresh, Kegel. "Peer-to-Peer Communication Across Network Address Translators." USENIX ATC 2005. https://bford.info/pub/net/p2pnat/
- [R-Trautwein] Trautwein et al. "Challenging Tribal Knowledge – Large Scale Measurement Campaign on Decentralized NAT Traversal." arXiv:2510.27500 (2025). https://arxiv.org/abs/2510.27500
- [R-Tailscale-NAT] Anderson. "How NAT traversal works." Tailscale (2020). https://tailscale.com/blog/how-nat-traversal-works
- [R-iroh-relays] iroh docs: Relays. https://docs.iroh.computer/concepts/relays
- [R-iroh-1.0] "Iroh 1.0 – Dial Keys, not IPs" (2026-06-15); repo https://github.com/n0-computer/iroh ; blog https://www.iroh.computer/blog
- [R-iroh-noq] "noq, noq, who's there?" (2026-03-19); https://github.com/n0-computer/noq
- [R-iroh-fork] "Why We Forked Quinn" (2024-05-20). https://www.iroh.computer/blog
- [R-iroh-blog] "iroh 0.96 – The QUIC Multipaths to 1.0" (2026-01-27) and subsequent release posts. https://www.iroh.computer/blog
- [R-iroh-blobs] n0-computer/iroh-blobs README. https://github.com/n0-computer/iroh-blobs
- [R-bao] oconnor663/bao. https://github.com/oconnor663/bao ; BLAKE3 https://github.com/BLAKE3-team/BLAKE3
- [R-BEP52] BitTorrent BEP 52 (v2). https://www.bittorrent.org/beps/bep_0052.html
- [R-RFC6817] RFC 6817, LEDBAT. https://www.rfc-editor.org/rfc/rfc6817.html
- [R-RFC9382] RFC 9382, SPAKE2. https://www.rfc-editor.org/rfc/rfc9382.html
- [R-CPace] draft-irtf-cfrg-cpace. https://datatracker.ietf.org/doc/draft-irtf-cfrg-cpace/
- [R-raptorq] cberner/raptorq. https://github.com/cberner/raptorq

**Determinism and testing**
- [R-turmoil] tokio-rs/turmoil. https://github.com/tokio-rs/turmoil
- [R-madsim] madsim-rs/madsim. https://github.com/madsim-rs/madsim
- [R-VOPR] TigerBeetle VOPR. https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md
