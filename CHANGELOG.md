# Changelog

## Unreleased

- Dropped the macOS Intel (x86_64) build; macOS binaries are Apple Silicon only. `install.sh` explains how to build from source on Intel Macs.
- New [docs/internals](docs/internals/README.md): how qget works, in order of events (protocols, the scheduler state machine, the journal format and crash behaviour, streaming extraction, P2P and its handshake), the reasoning behind each design choice, and how qget compares to other tools. CI checks the documents are updated with the code they describe.

## 0.3.0 — 2026-11-08 (see docs/release-notes/v0.3.0.md)

- Signed package repositories at https://qget.dharrsan.com: apt (Debian/Ubuntu), dnf (Fedora/RHEL 9+), pacman (Arch). Updates through normal system upgrades.
- Packages and repository indexes signed with the qget Ed25519 key A03F3628DC0F7B4A2E9AE313AB43FFC72673B3AA; CI refuses to sign with any other key.
- Website qget.dharrsan.com with install instructions, benchmarks and verification details.
- Every release is installed from the live repositories on Debian, Ubuntu, Fedora and Arch before it is considered done.
- AGENTS.md for contributors and their AI assistants; contact details for feedback.

## 0.2.0 — 2026-11-08 (see docs/release-notes/v0.2.0.md)

- Linux packages on every release: `.deb`, `.rpm` and Arch `.pkg.tar.zst` (x86_64 and arm64), with man page and completions.
- `install.sh`: one-line installer for Linux and macOS with SHA-256 verification, no sudo.
- Packages are installed and used on Debian 12, Ubuntu 22.04/24.04, Fedora and Arch in CI before every release; they depend on `ca-certificates`.
- Linux binaries are built on Ubuntu 22.04 and need glibc 2.34+.

## 0.1.1 — 2026-11-08 (see docs/release-notes/v0.1.1.md)

- One release archive per platform (Linux x86_64/arm64, macOS Apple Silicon/Intel), each including peer-to-peer.
- Clear error when an argument is not an http(s) URL, or when a P2P command or ticket is used with a build that lacks P2P.
- HDD tuning is opt-in (`--hdd`) instead of detected: virtual disks report rotational=1 and were limiting downloads to 8 connections.

## 0.1.0 — 2026-11-04 (see docs/release-notes/v0.1.0.md)

First feature-complete version of the PRD's M0–M6 scope.

- Parallel HTTP(S) range downloads with adaptive concurrency (probe, then
  hill-climbing on a connection-penalised utility), rate-scaled blocks, work
  stealing, straggler hedging and end-game duplication.
- Strict range handling: `If-Range`, exact `Content-Range` and byte-count
  validation, 200-instead-of-206 refused, changed objects detected.
- Crash-safe resume: append-only CRC32C journal, data fsynced before it is
  recorded, torn tails discarded, atomic finalisation with directory fsync.
- Integrity: `--checksum` (sha256/sha512/blake3), `--checksum-url`,
  `Repr-Digest`/`Content-Digest`, `<url>.sha256` discovery, `--verify-after`.
- Stream-extract (`-x DIR`) for tar, tar.lz4, tar.zst, tar.gz, tar.xz with
  parallel LZ4 block and zstd frame decoding, a hardened tar writer and a
  staging directory; `--keep-archive` for resumable extraction.
- Mirrors (`--mirror`), throttling with `Retry-After`, `--refresh-cmd` for
  expired signed URLs, `--limit-rate`, `--polite`, proxies, IPv4/IPv6, HTTP/2.
- Deterministic scheduling: `--seed`, `--trace` and `qget replay`.
- `--explain` bottleneck report, JSON progress, wget-compatible flags,
  config file, man page and completions.
- Peer-to-peer (feature `p2p`): `qget yeet`/`yoink` with tickets or short
  codes (SPAKE2 via `qget mailbox`), BLAKE3-verified 1 MiB groups, directory
  shares, multi-peer swarms, hybrid HTTP + peer downloads, seeding.
