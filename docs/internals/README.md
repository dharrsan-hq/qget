# How qget works

This section explains qget from the inside: the protocols it speaks, the state
machines that make its decisions, the exact order in which things happen, and
why it was designed this way. It is written for contributors, reviewers, and
anyone who wants to understand the tool well enough to re-implement it.

The documents are meant to be read in order. Each one builds on the vocabulary
of the ones before it.

| # | Document | What you learn |
|---|---|---|
| 1 | [Architecture](01-architecture.md) | The crates, what each one is allowed to do, and the one idea everything rests on: a pure decision engine inside an I/O shell |
| 2 | [The life of a download](02-download-lifecycle.md) | Every step of `qget URL`, in time order, from argument parsing to the final rename |
| 3 | [The HTTP protocol layer](03-http.md) | Probing, validators, `If-Range`, strict `Content-Range` checks, and how every status code is classified |
| 4 | [The scheduler](04-scheduler.md) | The engine state machine: block allocation, the concurrency controller, throttling, work stealing, hedging |
| 5 | [The event loop](05-event-loop.md) | How the runtime executes engine commands: tasks, channels, the sink thread, timeouts, backpressure |
| 6 | [Durability and resume](06-durability-and-resume.md) | The on-disk files, the journal format byte by byte, fsync ordering, and what happens if you crash at any instant |
| 7 | [Streaming and extraction](07-streaming-and-extraction.md) | The reorder window, format detection, parallel LZ4/zstd decoding, the hardened tar writer, staging and install |
| 8 | [Peer-to-peer](08-p2p.md) | iroh/QUIC, tickets, the wire protocol, BLAKE3 group verification, short codes with SPAKE2, swarm and hybrid downloads |
| 9 | [Determinism and testing](09-determinism-and-testing.md) | Seeds, traces and replay, the simulator, the invariants it checks, crash and fault-injection tests |
| 10 | [Design decisions](10-design-decisions.md) | Every major choice, the alternatives that were rejected, and where the code deviates from the PRD |
| 11 | [How qget compares](11-comparison.md) | qget next to aria2c, curl/wget, axel, Surge, fastar, croc, magic-wormhole, sendme and BitTorrent, including where qget loses |

If you only have ten minutes, read 1, 2 and 10.

## Conventions used in these documents

- **Byte ranges are half-open**: `[start, end)` includes `start` and excludes
  `end`. HTTP `Range` headers are inclusive, so `[0, 100)` is sent as
  `bytes=0-99`.
- **MiB** is 2²⁰ bytes, **GiB** is 2³⁰ bytes.
- Constants are quoted with their name in the code, e.g. "4 MiB
  (`min_block`)", so you can search for them.
- Requirement IDs such as `DUR-03` or `HTTP-04` refer to [PRD.md](../../PRD.md).
- Diagrams are [Mermaid](https://mermaid.js.org/); GitHub renders them inline.

## Glossary

| Term | Meaning |
|---|---|
| **Engine** | The scheduler in `qget-core/src/engine.rs`. A pure state machine: it receives `Event`s and returns `Command`s, and never touches the network, disk or clock |
| **Runtime / driver** | The tokio event loop in `qget-runtime/src/driver.rs` that executes engine commands and turns network activity into events |
| **Source** | Anything that can serve bytes `[start, end)` of the resource: an HTTP URL, a mirror, a peer. All implement one trait (`qget-core/src/source.rs`) |
| **Sink** | Where downloaded bytes go: a crash-safe file (`FileSink`) or an in-order stream (`StreamSink`) |
| **Request** (`ReqId`) | One range fetch issued by the engine, served by one source over one connection |
| **Block** | The size of range the engine hands to an idle connection; rate-scaled between 4 and 64 MiB |
| **Coverage** | The set of byte ranges already received (an `IntervalSet`) |
| **Target** | The number of concurrent requests the concurrency controller currently wants |
| **Steal** | Splitting the unfinished tail off a running request and giving it to another connection. No byte is fetched twice |
| **Hedge** | Requesting the same bytes a second time from another connection because the first is too slow. Some bytes are fetched twice; capped by a budget |
| **Validator** | A strong `ETag` or `Last-Modified` value that identifies one version of a remote file |
| **Journal** | The `.qget-journal` file recording which byte ranges are durable on disk |
| **Window** | In streaming mode, the engine only allocates bytes in `[head, head + window)` so memory stays bounded |
| **Group** | In P2P, a 1 MiB slice of the file whose BLAKE3 chaining value is known in advance, so it can be verified on arrival |
| **Ticket** | A `qget1…` string holding everything needed to fetch a P2P share: peer address, BLAKE3 root, size, name and a capability secret |

## Keeping these documents true

These documents describe behaviour, so they go stale silently when the code
changes. Three things keep them honest:

1. **Every document lists the source files it explains** in a `covers:`
   comment at the top. The comment is the map between code and docs.
2. **CI enforces it.** `scripts/check-internals-docs.sh` runs on every push and
   pull request. If a commit range changes a covered file but not the document
   that covers it, the check fails and names the document to review.
3. **When a change genuinely does not affect the documented behaviour** (a
   rename, a refactor, a typo fix), add this trailer to the commit message and
   the check passes:

   ```
   Internals-docs: unchanged
   ```

   Use it honestly. It means "I read the covering document and it is still
   correct", not "I didn't want to look".

When you update a document:

- Describe what the code **does**, not what the PRD planned. Where they differ,
  say so in [Design decisions](10-design-decisions.md#deviations-from-the-prd).
- Update numbers (defaults, limits, constants) together with the code that
  defines them, and keep the constant's name next to the number.
- If you add a new source file, add it to the `covers:` list of the document
  that should explain it. The check warns about source files no document
  covers.
- Keep the reading order intact: a document may refer forward, but it must not
  depend on a later one to make sense.
