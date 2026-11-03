//! Deterministic simulation of sources driving the [`Engine`] (§14.3).
//!
//! Time advances in fixed steps; every random choice comes from a seeded
//! [`Rng`]. The same `(SimConfig, seed)` always produces the same event log,
//! so any failing seed can be replayed exactly.

use crate::engine::{Command, Engine, EngineConfig, Event, FailKind, FatalKind, ReqId, Stats};
use crate::range::{ByteRange, IntervalSet};
use crate::rng::Rng;
use std::collections::BTreeMap;

const STEP_MS: u64 = 10;
const TICK_MS: u64 = 100;

#[derive(Clone, Debug)]
pub struct SimSource {
    /// Bytes per millisecond each connection gets (the per-connection cap).
    pub per_conn_rate: f64,
    /// Aggregate cap across all connections to this source, bytes/ms.
    pub total_rate: f64,
    pub latency_ms: u64,
    /// Requests beyond this many concurrent ones are answered with 429.
    pub throttle_above: Option<u32>,
    /// Per-request chance (‰) of a transient failure part-way through.
    pub fail_permille: u64,
    /// Per-request chance (‰) of being a straggler running at `straggler_factor`.
    pub straggler_permille: u64,
    pub straggler_factor: f64,
    /// Source becomes permanently unusable at this time.
    pub fatal_at_ms: Option<u64>,
}

impl SimSource {
    pub fn simple(per_conn_rate: f64) -> Self {
        Self {
            per_conn_rate,
            total_rate: f64::INFINITY,
            latency_ms: 20,
            throttle_above: None,
            fail_permille: 0,
            straggler_permille: 0,
            straggler_factor: 1.0,
            fatal_at_ms: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub engine: EngineConfig,
    pub sources: Vec<SimSource>,
    /// Streaming consumer speed in bytes/ms (only used when `engine.window` is set).
    pub consumer_rate: f64,
    pub max_time_ms: u64,
    pub seed: u64,
}

#[derive(Clone, Debug)]
pub struct SimOutcome {
    pub finished: bool,
    pub aborted: Option<(FatalKind, String)>,
    pub elapsed_ms: u64,
    pub coverage: IntervalSet,
    pub stats: Stats,
    /// Every command issued, in order, for determinism checks.
    pub commands: Vec<Command>,
    pub max_inflight: usize,
    pub bytes_transferred: u64,
}

struct SimReq {
    source: usize,
    range: ByteRange,
    end: u64,
    pos: u64,
    starts_at: u64,
    rate_factor: f64,
    fail_at: Option<u64>,
    throttled: bool,
}

/// Run a simulation. `initial` is the coverage already on disk (resume).
pub fn run(cfg: &SimConfig, initial: IntervalSet) -> SimOutcome {
    run_with_crash(cfg, initial, None).0
}

/// Run until `crash_at_ms` (if given) and return the outcome plus the
/// coverage that existed at the crash, to test resume.
pub fn run_with_crash(
    cfg: &SimConfig,
    initial: IntervalSet,
    crash_at_ms: Option<u64>,
) -> (SimOutcome, IntervalSet) {
    let mut engine = Engine::new(cfg.engine.clone(), initial);
    let mut rng = Rng::new(cfg.seed ^ 0xA5A5_5A5A_DEAD_BEEF);
    let mut reqs: BTreeMap<ReqId, SimReq> = BTreeMap::new();
    let mut commands = Vec::new();
    let mut now = 0u64;
    let mut head = 0u64;
    let mut max_inflight = 0;
    let mut bytes_transferred = 0u64;
    let mut aborted = None;
    let mut finished = false;
    let size = cfg.engine.size;

    let apply = |cmds: Vec<Command>,
                 reqs: &mut BTreeMap<ReqId, SimReq>,
                 rng: &mut Rng,
                 now: u64,
                 commands: &mut Vec<Command>|
     -> Option<Result<(), (FatalKind, String)>> {
        let mut result = None;
        for c in cmds {
            commands.push(c.clone());
            match c {
                Command::Fetch { req, source, range } => {
                    assert!(
                        range.start < range.end && range.end <= size,
                        "bad fetch {range:?}"
                    );
                    let s = &cfg.sources[source as usize];
                    let active_on_source = reqs
                        .values()
                        .filter(|r| r.source == source as usize)
                        .count() as u32;
                    let throttled = s.throttle_above.is_some_and(|t| active_on_source >= t);
                    let straggler = rng.below(1000) < s.straggler_permille;
                    let fail_at = (rng.below(1000) < s.fail_permille)
                        .then(|| range.start + rng.below(range.len().max(1)));
                    reqs.insert(
                        req,
                        SimReq {
                            source: source as usize,
                            range,
                            end: range.end,
                            pos: range.start,
                            starts_at: now + s.latency_ms,
                            rate_factor: if straggler { s.straggler_factor } else { 1.0 },
                            fail_at,
                            throttled,
                        },
                    );
                }
                Command::Shrink { req, end } => {
                    if let Some(r) = reqs.get_mut(&req) {
                        r.end = r.end.min(end.max(r.pos));
                    }
                }
                Command::Cancel { req } => {
                    reqs.remove(&req);
                }
                Command::Finished => result = Some(Ok(())),
                Command::Abort { kind, msg } => result = Some(Err((kind, msg))),
            }
        }
        result
    };

    let cmds = engine.handle(Event::Tick { now });
    if let Some(r) = apply(cmds, &mut reqs, &mut rng, now, &mut commands) {
        match r {
            Ok(()) => finished = true,
            Err(e) => aborted = Some(e),
        }
    }

    while !finished && aborted.is_none() && now < cfg.max_time_ms {
        if crash_at_ms.is_some_and(|c| now >= c) {
            break;
        }
        now += STEP_MS;
        max_inflight = max_inflight.max(reqs.len());
        let mut events = Vec::new();

        // Advance every request by one step.
        let mut per_source_budget: Vec<f64> = cfg
            .sources
            .iter()
            .map(|s| s.total_rate * STEP_MS as f64)
            .collect();
        let ids: Vec<ReqId> = reqs.keys().copied().collect();
        for id in ids {
            let r = reqs.get_mut(&id).expect("present");
            if now < r.starts_at {
                continue;
            }
            let s = &cfg.sources[r.source];
            if s.fatal_at_ms.is_some_and(|t| now >= t) {
                events.push(Event::Failed {
                    now,
                    req: id,
                    kind: FailKind::Fatal {
                        kind: FatalKind::HttpStatus(404),
                        msg: "gone".into(),
                    },
                });
                reqs.remove(&id);
                continue;
            }
            if r.throttled {
                events.push(Event::Failed {
                    now,
                    req: id,
                    kind: FailKind::Throttled {
                        retry_after_ms: Some(500),
                    },
                });
                reqs.remove(&id);
                continue;
            }
            let want = (s.per_conn_rate * r.rate_factor * STEP_MS as f64).max(1.0);
            let allowed = want.min(per_source_budget[r.source]).max(0.0);
            per_source_budget[r.source] -= allowed;
            let mut n = (allowed as u64).min(r.end.saturating_sub(r.pos));
            if let Some(f) = r.fail_at {
                if r.pos + n >= f {
                    n = f.saturating_sub(r.pos);
                    if n > 0 {
                        events.push(Event::Data {
                            now,
                            req: id,
                            offset: r.pos,
                            len: n,
                        });
                        bytes_transferred += n;
                    }
                    events.push(Event::Failed {
                        now,
                        req: id,
                        kind: FailKind::Transient("reset".into()),
                    });
                    reqs.remove(&id);
                    continue;
                }
            }
            if n > 0 {
                events.push(Event::Data {
                    now,
                    req: id,
                    offset: r.pos,
                    len: n,
                });
                bytes_transferred += n;
                r.pos += n;
            }
            if r.pos >= r.end {
                events.push(Event::Done { now, req: id });
                reqs.remove(&id);
            }
        }

        if now % TICK_MS == 0 {
            if cfg.engine.window.is_some() {
                let available = engine.coverage().contiguous_end(head);
                let step = (cfg.consumer_rate * TICK_MS as f64) as u64;
                head = available.min(head + step.max(1));
                events.push(Event::Consumed { now, head });
            }
            events.push(Event::Tick { now });
        }

        for ev in events {
            // Data from a request the engine already cancelled is still valid
            // bytes; the runtime writes it, so the engine must see it too.
            let cmds = engine.handle(ev);
            if let Some(r) = apply(cmds, &mut reqs, &mut rng, now, &mut commands) {
                match r {
                    Ok(()) => finished = true,
                    Err(e) => aborted = Some(e),
                }
            }
            if finished || aborted.is_some() {
                break;
            }
        }
    }

    let crash_cov = engine.coverage().clone();
    (
        SimOutcome {
            finished,
            aborted,
            elapsed_ms: now,
            coverage: engine.coverage().clone(),
            stats: engine.stats(),
            commands,
            max_inflight,
            bytes_transferred,
        },
        crash_cov,
    )
}
