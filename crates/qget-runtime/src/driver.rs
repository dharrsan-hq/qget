//! The event loop: engine decisions in, network and disk work out.

use crate::limiter::RateLimiter;
use crate::trace::TraceWriter;
use bytes::Bytes;
use qget_core::engine::{Command, Engine, EngineConfig, Event, FailKind, FatalKind, ReqId, Stats};
use qget_core::{ByteRange, Error, IntervalSet, Result, Sink, SinkReport, Source};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Snapshot passed to the progress callback on every tick.
#[derive(Clone, Debug, Default)]
pub struct Progress {
    pub downloaded: u64,
    pub total: u64,
    /// Bytes per second over the last controller interval.
    pub rate: u64,
    pub connections: u32,
    pub target: u32,
    pub elapsed: Duration,
}

pub type ProgressFn = Arc<dyn Fn(&Progress) + Send + Sync>;

pub struct RunOptions {
    /// Fail a request that receives no bytes for this long (HTTP-10).
    pub idle_timeout: Duration,
    pub tick: Duration,
    pub rate_limit: Option<u64>,
    pub trace: Option<TraceWriter>,
    pub progress: Option<ProgressFn>,
    pub cancel: CancellationToken,
    /// Sink queue depth in chunks (backpressure bound).
    pub sink_queue: usize,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(15),
            tick: Duration::from_millis(100),
            rate_limit: None,
            trace: None,
            progress: None,
            cancel: CancellationToken::new(),
            sink_queue: 256,
        }
    }
}

#[derive(Debug)]
pub struct RunReport {
    pub stats: Stats,
    pub sink: SinkReport,
    pub elapsed: Duration,
    /// Bytes received from the network in this run (including hedges).
    pub bytes_received: u64,
    /// Bytes that were already present when the run started.
    pub resumed_bytes: u64,
    /// Time the sink thread spent writing/hashing/consuming.
    pub sink_busy: Duration,
    /// Time the event loop waited because the sink queue was full.
    pub sink_wait: Duration,
}

enum TaskMsg {
    Data {
        req: ReqId,
        offset: u64,
        bytes: Bytes,
    },
    Done {
        req: ReqId,
    },
    Failed {
        req: ReqId,
        kind: FailKind,
    },
}

enum SinkMsg {
    Write(u64, Bytes),
}

struct Task {
    handle: JoinHandle<()>,
    end: Arc<AtomicU64>,
}

/// Run a download to completion.
pub async fn run(
    cfg: EngineConfig,
    coverage: IntervalSet,
    sources: Vec<Arc<dyn Source>>,
    sink: Box<dyn Sink>,
    mut opts: RunOptions,
) -> Result<RunReport> {
    assert_eq!(
        cfg.sources as usize,
        sources.len(),
        "one source per engine source"
    );
    let size = cfg.size;
    let resumed_bytes = coverage.covered();
    let mut engine = Engine::new(cfg, coverage);
    let start = Instant::now();
    let now_ms = || start.elapsed().as_millis() as u64;
    let limiter = opts.rate_limit.map(|r| Arc::new(RateLimiter::new(r)));

    // The sink runs on its own thread: disk writes, hashing and decoding
    // never block the event loop.
    let (sink_tx, mut sink_rx) = mpsc::channel::<SinkMsg>(opts.sink_queue.max(1));
    let consumed = Arc::new(AtomicU64::new(u64::MAX));
    let sink_busy = Arc::new(AtomicU64::new(0));
    let (consumed2, busy2) = (consumed.clone(), sink_busy.clone());
    let sink_thread = std::thread::Builder::new()
        .name("qget-sink".into())
        .spawn(move || -> (Box<dyn Sink>, Result<()>) {
            let mut sink = sink;
            if let Some(h) = sink.consumed() {
                consumed2.store(h, Ordering::Relaxed);
            }
            while let Some(SinkMsg::Write(off, data)) = sink_rx.blocking_recv() {
                let t = Instant::now();
                if let Err(e) = sink.write(off, data) {
                    return (sink, Err(e));
                }
                busy2.fetch_add(t.elapsed().as_micros() as u64, Ordering::Relaxed);
                if let Some(h) = sink.consumed() {
                    consumed2.store(h, Ordering::Relaxed);
                }
            }
            (sink, Ok(()))
        })
        .map_err(|e| Error::io("spawn sink thread", e))?;

    let (task_tx, mut task_rx) = mpsc::unbounded_channel::<TaskMsg>();
    let mut tasks: HashMap<ReqId, Task> = HashMap::new();
    let mut bytes_received = 0u64;
    let mut sink_wait = Duration::ZERO;
    let mut last_consumed = 0u64;
    let mut outcome: Option<Result<()>> = None;

    let mut ticker = tokio::time::interval(opts.tick);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut pending_events: Vec<Event> = vec![Event::Tick { now: 0 }];

    'outer: loop {
        // Feed queued events to the engine and execute its commands.
        for ev in pending_events.drain(..) {
            let cmds = engine.handle(ev.clone());
            if let Some(t) = opts.trace.as_mut() {
                t.record(&ev, &cmds);
            }
            for cmd in cmds {
                match cmd {
                    Command::Fetch { req, source, range } => {
                        let end = Arc::new(AtomicU64::new(range.end));
                        let handle = tokio::spawn(fetch_task(
                            req,
                            sources[source as usize].clone(),
                            range,
                            end.clone(),
                            opts.idle_timeout,
                            limiter.clone(),
                            task_tx.clone(),
                        ));
                        tasks.insert(req, Task { handle, end });
                    }
                    Command::Shrink { req, end } => {
                        if let Some(t) = tasks.get(&req) {
                            t.end.store(end, Ordering::SeqCst);
                        }
                    }
                    Command::Cancel { req } => {
                        if let Some(t) = tasks.remove(&req) {
                            t.handle.abort();
                        }
                    }
                    Command::Finished => {
                        outcome = Some(Ok(()));
                    }
                    Command::Abort { kind, msg } => {
                        outcome = Some(Err(fatal_to_error(kind, msg, &sources)));
                    }
                }
            }
            if outcome.is_some() {
                break 'outer;
            }
        }

        tokio::select! {
            biased;
            _ = opts.cancel.cancelled() => {
                outcome = Some(Err(Error::Interrupted));
                break 'outer;
            }
            msg = task_rx.recv() => {
                let Some(msg) = msg else { break 'outer };
                let now = now_ms();
                match msg {
                    TaskMsg::Data { req, offset, bytes } => {
                        if !tasks.contains_key(&req) {
                            continue; // cancelled; engine no longer tracks it
                        }
                        let len = bytes.len() as u64;
                        bytes_received += len;
                        let t = Instant::now();
                        if sink_tx.send(SinkMsg::Write(offset, bytes)).await.is_err() {
                            break 'outer; // sink failed; error collected below
                        }
                        sink_wait += t.elapsed();
                        pending_events.push(Event::Data { now: now_ms(), req, offset, len });
                    }
                    TaskMsg::Done { req } => {
                        if tasks.remove(&req).is_some() {
                            pending_events.push(Event::Done { now, req });
                        }
                    }
                    TaskMsg::Failed { req, kind } => {
                        if tasks.remove(&req).is_some() {
                            tracing::debug!(req, ?kind, "request failed");
                            pending_events.push(Event::Failed { now, req, kind });
                        }
                    }
                }
            }
            _ = ticker.tick() => {
                let now = now_ms();
                let c = consumed.load(Ordering::Relaxed);
                if c != u64::MAX && c != last_consumed {
                    last_consumed = c;
                    pending_events.push(Event::Consumed { now, head: c });
                }
                pending_events.push(Event::Tick { now });
                if let Some(p) = &opts.progress {
                    p(&Progress {
                        downloaded: engine.coverage().covered(),
                        total: size,
                        rate: engine.throughput(),
                        connections: tasks.len() as u32,
                        target: engine.target_conns(),
                        elapsed: start.elapsed(),
                    });
                }
            }
        }
    }

    for (_, t) in tasks.drain() {
        t.handle.abort();
    }
    drop(sink_tx);
    let (mut sink, sink_result) =
        tokio::task::spawn_blocking(move || sink_thread.join().expect("sink thread panicked"))
            .await
            .expect("join sink thread");

    if let Some(t) = opts.trace.as_mut() {
        t.flush();
    }
    let finish = match (sink_result, outcome) {
        (Err(e), _) => {
            let _ = sink.abort();
            Err(e)
        }
        (Ok(()), Some(Ok(()))) => tokio::task::spawn_blocking(move || {
            let r = sink.finish();
            if r.is_err() {
                let _ = sink.abort();
            }
            r
        })
        .await
        .expect("finish sink"),
        (Ok(()), Some(Err(e))) => {
            let _ = tokio::task::spawn_blocking(move || sink.abort()).await;
            Err(e)
        }
        (Ok(()), None) => {
            let _ = sink.abort();
            Err(Error::Network("download loop ended unexpectedly".into()))
        }
    };
    let sink_report = finish?;
    if let Some(p) = &opts.progress {
        p(&Progress {
            downloaded: size,
            total: size,
            rate: engine.throughput(),
            connections: 0,
            target: engine.target_conns(),
            elapsed: start.elapsed(),
        });
    }
    Ok(RunReport {
        stats: engine.stats(),
        sink: sink_report,
        elapsed: start.elapsed(),
        bytes_received,
        resumed_bytes,
        sink_busy: Duration::from_micros(sink_busy.load(Ordering::Relaxed)),
        sink_wait,
    })
}

fn fatal_to_error(kind: FatalKind, msg: String, sources: &[Arc<dyn Source>]) -> Error {
    let url = sources.first().map(|s| s.describe()).unwrap_or_default();
    match kind {
        FatalKind::RemoteChanged => Error::RemoteChanged(msg),
        FatalKind::HttpStatus(status) => Error::HttpStatus { status, url },
        FatalKind::Unsupported => Error::Unsupported(msg),
        FatalKind::Integrity => Error::Integrity {
            expected: "verified data".into(),
            actual: msg,
        },
        FatalKind::Other => Error::Network(msg),
    }
}

/// Fetch one range and stream it to the event loop.
async fn fetch_task(
    req: ReqId,
    source: Arc<dyn Source>,
    range: ByteRange,
    end: Arc<AtomicU64>,
    idle: Duration,
    limiter: Option<Arc<RateLimiter>>,
    tx: mpsc::UnboundedSender<TaskMsg>,
) {
    let result = async {
        let mut stream = match tokio::time::timeout(idle, source.open(range)).await {
            Ok(r) => r?,
            Err(_) => return Err(FailKind::Transient("timed out waiting for response".into())),
        };
        let mut pos = range.start;
        loop {
            let limit = end.load(Ordering::SeqCst);
            if pos >= limit {
                return Ok(());
            }
            let chunk = match tokio::time::timeout(idle, stream.next_chunk()).await {
                Err(_) => {
                    return Err(FailKind::Transient(format!(
                        "no data for {} s",
                        idle.as_secs()
                    )));
                }
                Ok(None) => {
                    return if pos >= end.load(Ordering::SeqCst) {
                        Ok(())
                    } else {
                        Err(FailKind::Transient(format!(
                            "body ended early at {pos}, expected {}",
                            range.end
                        )))
                    };
                }
                Ok(Some(c)) => c?,
            };
            if chunk.is_empty() {
                continue;
            }
            // Never deliver beyond the (possibly shrunk) end.
            let limit = end.load(Ordering::SeqCst).max(pos);
            let take = (chunk.len() as u64).min(limit - pos) as usize;
            if (chunk.len() as u64) > range.end - pos {
                return Err(FailKind::Transient(
                    "server sent more bytes than requested".into(),
                ));
            }
            if take == 0 {
                return Ok(());
            }
            if let Some(l) = &limiter {
                l.acquire(take).await;
            }
            let bytes = chunk.slice(..take);
            if tx
                .send(TaskMsg::Data {
                    req,
                    offset: pos,
                    bytes,
                })
                .is_err()
            {
                return Ok(());
            }
            pos += take as u64;
        }
    }
    .await;
    let _ = match result {
        Ok(()) => tx.send(TaskMsg::Done { req }),
        Err(kind) => tx.send(TaskMsg::Failed { req, kind }),
    };
}
