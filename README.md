# qget

**Quickly get big files, over HTTP or peer-to-peer.** Written in Rust.

> 🚧 **Under active development.** Feedback, criticism and contributions are welcome:
> [open an issue](https://github.com/dharrsan-hq/qget/issues), email **dharrsan.work@gmail.com**,
> or message on [LinkedIn](https://www.linkedin.com/in/dharrsan-amarnath-75017221a).
> Website: **https://qget.dharrsan.com**

qget (*quickly get*) is for the downloads that take hours: AI model weights (GGUF,
safetensors), blockchain node snapshots, datasets and disk images. It fetches over many
adaptive HTTP range connections, survives crashes and `kill -9` without corrupting anything,
verifies checksums while it downloads, and can unpack a `.tar.lz4` / `.tar.zst` as it
arrives, **so you don't need 2× the disk space**. It also sends files directly between machines
over **QUIC** (via iroh, with NAT hole punching and relay fallback), verifying every MiB
against a BLAKE3 root.

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

Loopback with emulated server limits (`bench/results/2026-11-04-1024m.md`) plus one real
Cloudflare R2 download:

| Scenario | qget | aria2c `-x16 -s16` | curl / wget |
|---|---|---|---|
| Download **and extract** 387 MiB `.tar.lz4`, 25 MB/s per connection | **4.8 s** | 7.9 s (then `tar`) | 16.4 s (`curl \| lz4 \| tar`) |
| 1 GiB, 25 MB/s per connection | 4.1 s | **2.7 s** | 43 s |
| 29 GiB from Cloudflare R2 | 584 MiB/s | **878 MiB/s** | — |

Stream-extract wins clearly. For plain downloads aria2c is currently faster: qget's
connection ramp-up and write-path fsyncs are the next things being fixed.

## Install

Signed repositories at **https://qget.dharrsan.com**. Add once; updates arrive with your normal system upgrades.

**Debian / Ubuntu**

```sh
sudo curl --create-dirs -fsSLo /etc/apt/keyrings/qget.asc https://qget.dharrsan.com/key.asc
echo "deb [signed-by=/etc/apt/keyrings/qget.asc] https://qget.dharrsan.com/apt stable main" | sudo tee /etc/apt/sources.list.d/qget.list
sudo apt update && sudo apt install qget
```

**Fedora / RHEL 9+**

```sh
sudo curl -fsSLo /etc/yum.repos.d/qget.repo https://qget.dharrsan.com/rpm/qget.repo
sudo dnf install qget
```

**Arch / CachyOS / Manjaro**

```sh
curl -fsSL https://qget.dharrsan.com/key.asc | sudo pacman-key --add /dev/stdin
sudo pacman-key --lsign-key A03F3628DC0F7B4A2E9AE313AB43FFC72673B3AA
printf '\n[qget]\nServer = https://qget.dharrsan.com/arch/$arch\n' | sudo tee -a /etc/pacman.conf
sudo pacman -Syu qget
```

**macOS, or any Linux without root**: verifies the SHA-256 and installs to `~/.local/bin`:

```sh
curl -fsSL https://qget.dharrsan.com/install.sh | sh
```

**From source**: `cargo install --git https://github.com/dharrsan-hq/qget qget-cli -F p2p`

Packages and repository indexes are signed with the qget key (Ed25519):

```
A03F 3628 DC0F 7B4A 2E9A  E313 AB43 FFC7 2673 B3AA
```

Check that this fingerprint matches what your package manager shows. The same fingerprint is published on
the website and at [keys.openpgp.org](https://keys.openpgp.org/search?q=A03F3628DC0F7B4A2E9AE313AB43FFC72673B3AA).
`.deb`, `.rpm` and `.pkg.tar.zst` files are also attached to every [release](https://github.com/dharrsan-hq/qget/releases).
Every package includes peer-to-peer, the man page and bash/zsh/fish completions, and is install-tested on
Debian 12, Ubuntu 22.04/24.04, Fedora and Arch before each release (Linux packages need glibc 2.34+).

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
| `--hdd` | output is on a spinning disk: larger blocks, at most 8 connections |
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

Peer-to-peer uses QUIC directly (through [iroh](https://www.iroh.computer)), not HTTP/3.
Ordinary URL downloads use many parallel HTTP/1.1 connections (`--http2` optional).

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

## Contributing

Open to contributions and criticism. See [CONTRIBUTING.md](CONTRIBUTING.md) and
[AGENTS.md](AGENTS.md) (rules and commands, also read by AI coding assistants).

## License

Apache-2.0. See [SECURITY.md](SECURITY.md) for reporting vulnerabilities.
