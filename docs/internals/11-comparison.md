<!-- covers: bench/results/ -->

# 11. How qget compares

This document places qget next to the tools people already use, explains
which differences come from design rather than polish, and states plainly
where qget is currently worse. Claims about other tools are limited to their
documented behaviour, or to the source review recorded in PRD §3 (with the
commit and date it was made at).

## Where qget fits

qget is a single-shot, scriptable downloader and an embeddable library for
**large files on servers**: model weights, datasets, blockchain node
snapshots, disk images. It competes on engine quality (correctness, resume,
throughput, stream-extract, verified P2P), not on desktop features such as a
GUI, browser integration or download queues.

## Downloaders

| | qget | aria2c | curl / wget | axel | Surge |
|---|---|---|---|---|---|
| Parallel range connections | Yes, adaptive (4 → up to 16, max 64) | Yes, fixed (`-x` up to 16 per server) | No (one connection) | Yes, static split | Yes; adaptive mode exists but is off by default |
| Work assignment | Small rate-scaled blocks; split slow requests, then hedge within a 5% budget | Segment-based | n/a | One range per connection | One large range per worker; no end-game duplication |
| Throttling (429/503) | Halve connections, honour `Retry-After` | Retries | Retries with curl `--retry` | | Honours `Retry-After`, learns per-host caps |
| Crash-safe resume | Journal of durable ranges, `fsync` ordering, tested with `kill -9` | Control file (`.aria2`) | By file size (`-c`, `-C -`) | State file | Snapshots on pause or error only, no directory `fsync` |
| Detects a changed remote file on resume | Yes: strong ETag / Last-Modified, `If-Range` on every request | | No; the wget manual warns a changed file gives a garbled result | | No ETag / `If-Range` validation |
| Validates `Content-Range` | Strictly, against the request and the size | | n/a | | Not checked |
| Checksum during download | sha256, sha512, blake3; auto from `Repr-Digest` or `.sha256` sidecars | `--checksum` (after download) | No | No | No |
| Extract while downloading | Yes, parallel decode for LZ4 and multi-frame zstd | No | Single connection via a pipe | No | No |
| Peer-to-peer | Verified, over QUIC (optional feature) | BitTorrent | No | No | No |
| Embeddable library | Yes (Rust; API experimental before 1.0) | libaria2 (C++), RPC | libcurl (C) | No | No |
| Licence | Apache-2.0 | GPL-2.0 | curl: curl licence (MIT-style); wget: GPL-3.0 | GPL-2.0 | MIT |

Surge's column reflects its source at commit `9de4cfa` (2026-10-27), as
reviewed in PRD §3.2. Blank cells mean "not established", not "no".

aria2c also speaks FTP, SFTP, Metalink and BitTorrent, and has an RPC daemon;
qget deliberately does not.

## Snapshot workflows

| Workflow | Connections | Disk needed | Passes |
|---|---|---|---|
| `wget` then `lz4 -d \| tar -x` | 1 | archive + extracted | 2 (sequential) |
| `curl … \| lz4 -d \| tar -x` | 1 | extracted | 1 |
| `aria2c -x16` then `tar` | 16 | archive + extracted | 2 (sequential) |
| fastar (Databricks) | many | extracted | 1, parallel tar writes |
| `reth download` | streaming into the extractor | extracted | 1, reth snapshots only, not resumable |
| `qget URL --extract DIR` | adaptive | extracted | 1, parallel download + parallel decode where possible |
| `qget URL --extract DIR --keep-archive` | adaptive | archive + extracted | 1, resumable |

## Peer-to-peer tools

| | qget yeet / yoink | croc | magic-wormhole | sendme (iroh) | BitTorrent |
|---|---|---|---|---|---|
| Transport | QUIC (iroh): hole punching, relay fallback | TCP, usually via a relay | Transit: direct TCP or relay | QUIC (iroh) | TCP / uTP |
| Short codes | Optional, SPAKE2 via a self-hosted mailbox | Yes (PAKE) | Yes (SPAKE2) | No (tickets) | No (magnet links) |
| Integrity | BLAKE3 root; every 1 MiB group verified before writing | End-to-end encryption under the PAKE key | Encrypted, authenticated transfer | BLAKE3 (bao) | Per-piece hashes |
| Many sources at once | Yes: several peers and HTTP mirrors, one root | No | No | No | Yes (swarm) |
| Resume | Yes (same journal as HTTP) | Yes | | | Yes |
| Built for | Teams and fleets moving big files | Ad-hoc transfers | Ad-hoc transfers | Demo-scale transfers | Public swarms |

qget's distinctive combination is **one verified download fed by HTTP mirrors
and peers at the same time**. That only works because the BLAKE3 root is known
in advance, from a ticket.

## Benchmarks: where qget wins and loses

From `bench/results/2026-11-04-1024m.md` (loopback, network conditions
emulated by the test server, 3 runs, median, every output verified by
SHA-256), plus one real-network test from the README:

| Scenario | qget | aria2c `-x16 -s16 -k1M` | curl / wget | Verdict |
|---|---|---|---|---|
| Extract 387 MiB `.tar.lz4`, 25 MB/s per connection | **4.76 s** | 7.87 s (then `tar`) | 16.38 s (`curl \| lz4 \| tar`) | qget 1.65× faster than aria2c + tar, 3.4× faster than the pipe |
| 1 GiB, 25 MB/s per connection | 4.14 s | **2.73 s** | 42.96 s | aria2c 1.5× faster |
| 1 GiB, 25 MB/s per connection, 40 ms latency | 4.38 s | **2.83 s** | 43.00 s | aria2c 1.5× faster |
| 1 GiB, unthrottled loopback | 1.52 s | 1.21 s | **0.81 s** (wget) | One connection wins when nothing limits it |
| 29 GiB from Cloudflare R2 (real network) | 584 MiB/s | **878 MiB/s** | | aria2c 1.5× faster |

**Stream-extract wins by design**: the overlap of download and decode, and no
second pass over a stored archive, can't be matched by tuning a downloader.

**Plain downloads are currently slower than tuned aria2c.** The likely causes,
not yet confirmed by profiling:

- **Ramp-up**: about three seconds of probing (4 → 8 → 16 connections) before
  running at full width. On a 4-second download that is most of the run.
- **`fdatasync` in the write path**: the sink thread blocks while syncing each
  256 MiB group, and backpressure stalls the network meanwhile.
- **Gaps between blocks**: one round trip of idle time per block, and stolen
  or cancelled requests reconnect from scratch (see
  [the scheduler](04-scheduler.md#known-limitations)).

These numbers are part of the documentation on purpose: a comparison that
hides losing cells is not useful. When new results land in `bench/results/`,
update this table.

Next: back to [the index](README.md).
