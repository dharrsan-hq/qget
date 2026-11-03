# Security policy

## Supported versions

Until 1.0, only the latest release receives security fixes.

## Reporting a vulnerability

Please do **not** open a public issue. Use GitHub's private vulnerability
reporting ("Security" tab → "Report a vulnerability") on
https://github.com/dharrsan-hq/qget. You will get an acknowledgement within
7 days. Fixes are released as soon as practical, followed by a public advisory.

## Scope and design notes

Areas that are security-relevant by design:

- **Archive extraction** (`--extract`): entries with absolute paths or `..`,
  symlinks that escape the extraction directory, writes through symlinks
  created by the archive, and device nodes/FIFOs are refused. setuid/setgid
  bits are dropped. Output is staged in `DIR.qget-staging` and only moved into
  place after the download's checksum (if any) matches.
- **Integrity**: with `--checksum`, a discovered `.sha256`, or a
  `Repr-Digest` header, bytes are hashed while downloading and the output is
  not finalised on mismatch. Peer-to-peer transfers verify every 1 MiB group
  against the BLAKE3 root in the ticket before writing it.
- **Peer-to-peer**: transport encryption and peer authentication come from
  iroh (QUIC/TLS with raw public keys). Tickets carry a random capability
  secret. Short codes use SPAKE2 (the `spake2` crate, which has not been
  independently audited) only to authenticate a short exchange that delivers
  the ticket encrypted with ChaCha20-Poly1305; each code allows one guess.
- **TLS**: rustls with the platform verifier; `--no-check-certificate` prints
  a warning on every run.
- qget opens no listening sockets except during `yeet`, `yoink --seed` and
  `mailbox`, and sends no telemetry.
