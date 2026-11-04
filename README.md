# qget

**Fast, verified, resumable downloads, with archives extracted while they download.**

qget fetches files over many adaptive HTTP range connections, survives
crashes and `kill -9` without corrupting anything, verifies checksums while it
downloads, and can unpack a huge `.tar.lz4` / `.tar.zst` snapshot straight to
disk as it arrives, without ever storing the archive. With the `p2p` build it
also sends files directly between machines over QUIC, verified byte-for-byte.

```sh
# a file, resumable, verified
qget --checksum sha256:9f2c… https://snapshots.example.com/node.tar.lz4

# a 300 GB snapshot, extracted while downloading (no second copy on disk)
qget -x /data/node https://snapshots.example.com/node.tar.lz4

# send a file to another machine (p2p build)
qget yeet ./node.tar.zst          # prints a code / ticket
qget yoink 4217-maple-orbit-canyon
```

> Status: **0.1, pre-release.** The command line may still change before 1.0.

## Why

| | qget | aria2c | curl / wget |
|---|---|---|---|
| Parallel range connections | adaptive (probe + hill-climb, up to 64) | fixed, max 16 | 1 |
| Slow-connection handling | work stealing, hedging, end-game | — | — |
| Crash-safe resume (fsync-ordered journal) | ✓ | control file | partial (`-c`) |
| Detects the file changing mid-download | `If-Range` + validator checks | — | — |
| Checksum while downloading | sha256/512, blake3, `Repr-Digest`, `.sha256` | ✓ (`--checksum`) | — |
| Extract while downloading, in parallel | ✓ (`-x`) | — | single connection via pipe |
| Deterministic, replayable scheduling | `--seed`, `--trace`, `qget replay` | — | — |
| Verified peer-to-peer + hybrid with mirrors | ✓ (`p2p` build) | BitTorrent | — |

## Benchmarks

See [bench/results](bench/results) for the raw numbers and
[bench/bench.py](bench/bench.py) to reproduce them. Every output is verified by
SHA-256; scenarios where qget does not win are reported too.

<!-- BENCH-TABLE -->

## Install

```sh
cargo install --git https://github.com/dharrsan-hq/qget qget-cli            # HTTP + extract
cargo install --git https://github.com/dharrsan-hq/qget qget-cli -F p2p     # + peer-to-peer
```

Prebuilt binaries for Linux (x86_64, arm64) and macOS (Apple Silicon, Intel) are on the
[releases page](https://github.com/dharrsan-hq/qget/releases). An AUR `PKGBUILD` is in [packaging/aur](packaging/aur). Man page and shell
completions: `qget man`, `qget completions bash|zsh|fish`.

## Usage

```
qget [OPTIONS] <URL>...
```

wget-compatible: `-O FILE` (`-` for stdout), `-P DIR`, `-c`, `-q`, `-i FILE`,
`--header`, `-U`, `--limit-rate`, `-t`, `-T`, `--no-check-certificate`.

| Option | Meaning |
|---|---|
| `-x, --extract DIR` | extract `tar`, `tar.lz4`, `tar.zst`, `tar.gz`, `tar.xz` into DIR while downloading |
| `--keep-archive` | with `-x`: also keep the archive, so the download is resumable |
| `--checksum ALGO:HEX` | `sha256:`, `sha512:` or `blake3:`; fails (exit 5) on mismatch |
| `--checksum-url URL` | read the digest from a `sha256sum`-style file |
| `--verify auto\|required\|off` | checksum discovery (`Repr-Digest`, `<url>.sha256` when extracting) |
| `--mirror URL` | another source of the same bytes (needs a checksum or identical ETags) |
| `-n N` / `--max-connections N` | fixed / maximum connections (default: adaptive, max 16) |
| `--polite` | at most 4 connections, no duplicate requests |
| `--refresh-cmd CMD` | command printing a fresh URL when a signed URL expires |
| `--restart` | discard a partial download instead of resuming |
| `--explain` | bottleneck analysis and scheduler statistics |
| `--seed N`, `--trace FILE` | reproducible scheduling; `qget replay FILE` checks it |
| `--progress auto\|bar\|json\|none` | progress on stderr (JSON lines for automation) |

Exit codes: 0 ok · 2 usage · 3 network · 4 HTTP status · 5 checksum mismatch ·
6 remote changed · 7 local I/O / disk full · 8 interrupted (re-run to resume) ·
9 extraction · 10 peer-to-peer · 11 unsupported.

Defaults can be set in `~/.config/qget/config.toml` using the long option names:

```toml
max-connections = 32
progress = "json"
headers = ["Authorization: Bearer …"]
```

### Peer-to-peer (`p2p` build)

```sh
qget yeet ./data.tar                 # prints a qget1… ticket
qget yoink qget1…                    # direct QUIC with hole punching, relay fallback
qget yoink qget1… --mirror https://cdn.example.com/data.tar   # hybrid: peer + HTTP, all verified
qget yoink qget1…A qget1…B           # swarm: fetch from several peers at once
qget yoink qget1… --seed             # keep serving afterwards

qget mailbox --listen 0.0.0.0:7700   # self-hosted rendezvous for short codes
QGET_MAILBOX=mbox.example.com:7700 qget yeet ./dir   # → 4217-maple-orbit-canyon
```

Every 1 MiB is checked against the file's BLAKE3 root (the same value `b3sum`
prints) before it is written, so peers and mirrors never need to be trusted.

## How it works

- A deterministic scheduler in `qget-core` decides which byte ranges to
  request from which source, how many connections to use, and when to split or
  hedge a slow request. It never touches I/O; the runtime feeds it events and
  executes its commands, so any run can be replayed exactly.
- Data is written with `pwrite`, made durable with `fdatasync`, and only then
  recorded in an append-only journal with CRC32C records. A crash at any point
  resumes byte-exactly; the test suite kills qget with `SIGKILL` repeatedly to
  check this.
- Streaming extraction keeps a bounded reorder window (`--max-buffer`),
  decodes LZ4 blocks and zstd frames in parallel when the format allows, and
  writes files from a worker pool into a staging directory that is renamed into
  place only after verification.

The full design, requirements and research references are in [PRD.md](PRD.md).

## Library

```rust
let report = qget::Download::new("https://example.com/snapshot.tar.lz4")
    .extract_to("/data/snapshot")
    .checksum("sha256:…")?
    .run()
    .await?;
```

The library API is experimental until 1.0.

## License

Apache-2.0. See [SECURITY.md](SECURITY.md) for reporting vulnerabilities.
