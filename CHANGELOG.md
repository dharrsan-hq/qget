# Changelog

## 0.1.1 — 2026-11-04 (see docs/release-notes/v0.1.1.md)

- Peer-to-peer builds for every platform (`-p2p` archives for Linux x86_64/arm64 and macOS Apple Silicon/Intel).
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
