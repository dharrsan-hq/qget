//! The download scheduler: a pure state machine (DET-01).
//!
//! The runtime feeds [`Event`]s (with explicit timestamps in milliseconds) and
//! executes the returned [`Command`]s. The engine never reads a clock, touches
//! I/O, or uses ambient randomness, so a recorded event log replays to exactly
//! the same decisions (DET-05).
//!
//! Responsibilities:
//! - dynamic block allocation with rate-scaled block size (§10.2)
//! - concurrency control: a short probe over a few levels, then hill-climbing
//!   on a utility that penalises connection count (§10.3)
//! - throttle handling: multiplicative decrease and per-source cooldowns
//! - work stealing and straggler hedging, plus end-game duplication (§10.4)
//! - a bounded window for in-order (streaming) consumers (§11.2)

use crate::range::{ByteRange, IntervalSet};
use crate::rng::Rng;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type ReqId = u64;
pub type SourceId = u32;

const MIB: u64 = 1 << 20;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Total size of the resource in bytes.
    pub size: u64,
    /// Number of interchangeable sources (mirrors, peers).
    pub sources: u32,
    /// Allocations start on multiples of this (verified groups need 1 MiB).
    pub alignment: u64,
    pub min_block: u64,
    pub max_block: u64,
    pub initial_block: u64,
    pub initial_conns: u32,
    pub max_conns: u32,
    /// Probe and hill-climb the connection count; otherwise stay at `initial_conns`.
    pub adaptive: bool,
    /// Allow duplicate (hedged) requests; work stealing by splitting is always on.
    pub hedge: bool,
    /// Maximum bytes that may be requested twice.
    pub hedge_budget: u64,
    /// Streaming mode: only allocate within `[head, head + window)`.
    pub window: Option<u64>,
    /// Consecutive failures without any progress before giving up.
    pub retry_budget: u32,
    /// Abort when no byte arrives for this long.
    pub stall_abort_ms: u64,
    pub seed: u64,
    /// Length of one controller measurement interval.
    pub probe_ms: u64,
    /// A request must run this long before it can be judged a straggler.
    pub straggler_grace_ms: u64,
}

impl EngineConfig {
    pub fn new(size: u64, sources: u32) -> Self {
        Self {
            size,
            sources: sources.max(1),
            alignment: 1,
            min_block: 4 * MIB,
            max_block: 64 * MIB,
            initial_block: 8 * MIB,
            initial_conns: 4,
            max_conns: 16,
            adaptive: true,
            hedge: true,
            hedge_budget: (size / 20).max(8 * MIB),
            window: None,
            retry_budget: 10,
            stall_abort_ms: 600_000,
            seed: 0,
            probe_ms: 1000,
            straggler_grace_ms: 2000,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FatalKind {
    RemoteChanged,
    HttpStatus(u16),
    Unsupported,
    Integrity,
    Other,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailKind {
    /// Connection reset, timeout, 5xx, short body: retry elsewhere/later.
    Transient(String),
    /// 429/503 or similar: back off this source and reduce concurrency.
    Throttled { retry_after_ms: Option<u64> },
    /// This source can no longer be used.
    Fatal { kind: FatalKind, msg: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    /// Periodic timer; also used to start the engine.
    Tick { now: u64 },
    /// `len` bytes at `offset` for `req` were handed to the sink.
    Data {
        now: u64,
        req: ReqId,
        offset: u64,
        len: u64,
    },
    /// `req` delivered its whole range and ended normally.
    Done { now: u64, req: ReqId },
    Failed {
        now: u64,
        req: ReqId,
        kind: FailKind,
    },
    /// Streaming consumer has taken everything before `head`.
    Consumed { now: u64, head: u64 },
}

impl Event {
    pub fn now(&self) -> u64 {
        match *self {
            Event::Tick { now }
            | Event::Data { now, .. }
            | Event::Done { now, .. }
            | Event::Failed { now, .. }
            | Event::Consumed { now, .. } => now,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Fetch {
        req: ReqId,
        source: SourceId,
        range: ByteRange,
    },
    /// Stop `req` once it reaches `end` (its tail was handed to another request).
    Shrink {
        req: ReqId,
        end: u64,
    },
    Cancel {
        req: ReqId,
    },
    Finished,
    Abort {
        kind: FatalKind,
        msg: String,
    },
}

/// Counters reported by `--explain`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Stats {
    pub requests: u64,
    pub steals: u64,
    pub hedges: u64,
    pub hedged_bytes: u64,
    pub retries: u64,
    pub throttles: u64,
    pub cancels: u64,
    pub max_target: u32,
    pub final_target: u32,
    /// (connections, bytes per second) measured during the probe phase.
    pub probe_results: Vec<(u32, u64)>,
    pub bytes_per_source: Vec<u64>,
    /// Milliseconds spent with every source in cooldown.
    pub throttled_ms: u64,
}

#[derive(Clone, Debug)]
struct Req {
    source: SourceId,
    pos: u64,
    end: u64,
    started: u64,
    sample_at: u64,
    sample_bytes: u64,
    /// EWMA of bytes per millisecond; `None` until the first sample.
    rate: Option<f64>,
    /// A steal or hedge has already been taken from this request.
    hedged: bool,
}

impl Req {
    fn remaining(&self) -> u64 {
        self.end.saturating_sub(self.pos)
    }

    /// Rate estimate that decays when the request stops receiving data.
    fn effective_rate(&self, now: u64) -> f64 {
        let silent = now.saturating_sub(self.sample_at);
        let recent = if silent >= 500 {
            self.sample_bytes as f64 / silent as f64
        } else {
            f64::INFINITY
        };
        match self.rate {
            Some(r) => r.min(recent),
            None if now.saturating_sub(self.started) >= 500 => recent.min(f64::MAX),
            None => f64::INFINITY,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct SourceState {
    enabled: bool,
    cooldown_until: u64,
    backoff_level: u32,
    throttle_level: u32,
    inflight: u32,
    /// EWMA of bytes per millisecond over controller intervals.
    rate: f64,
    interval_bytes: u64,
}

#[derive(Clone, Debug)]
enum Phase {
    Probe {
        levels: Vec<u32>,
        idx: usize,
        results: Vec<(u32, i64)>,
    },
    Track {
        dir: i32,
        last_u: i64,
    },
    Fixed,
}

pub struct Engine {
    cfg: EngineConfig,
    coverage: IntervalSet,
    inflight: BTreeMap<ReqId, Req>,
    next_req: ReqId,
    sources: Vec<SourceState>,
    rng: Rng,
    target: u32,
    phase: Phase,
    interval_start: Option<u64>,
    interval_bytes: u64,
    /// Aggregate bytes/ms over the last full interval.
    agg_rate: f64,
    throttle_episode_until: u64,
    no_increase_until: u64,
    consecutive_failures: u32,
    last_progress: Option<u64>,
    head: u64,
    hedge_used: u64,
    finished: bool,
    last_now: u64,
    stats: Stats,
}

impl Engine {
    /// Create an engine. `coverage` holds bytes already present (resume).
    pub fn new(mut cfg: EngineConfig, coverage: IntervalSet) -> Self {
        cfg.alignment = cfg.alignment.max(1);
        cfg.min_block = round_up(cfg.min_block.max(1), cfg.alignment);
        cfg.max_block = round_up(cfg.max_block.max(cfg.min_block), cfg.alignment);
        cfg.initial_block = cfg.initial_block.clamp(cfg.min_block, cfg.max_block);
        cfg.max_conns = cfg.max_conns.max(1);
        cfg.initial_conns = cfg.initial_conns.clamp(1, cfg.max_conns);
        let phase = if cfg.adaptive && cfg.max_conns > cfg.initial_conns {
            let mut levels = vec![cfg.initial_conns];
            let mut l = cfg.initial_conns;
            while l < cfg.max_conns && levels.len() < 3 {
                l = (l * 2).min(cfg.max_conns);
                levels.push(l);
            }
            Phase::Probe {
                levels,
                idx: 0,
                results: Vec::new(),
            }
        } else {
            Phase::Fixed
        };
        let head = coverage.contiguous_end(0);
        Self {
            target: cfg.initial_conns,
            rng: Rng::new(cfg.seed),
            sources: (0..cfg.sources)
                .map(|_| SourceState {
                    enabled: true,
                    ..Default::default()
                })
                .collect(),
            stats: Stats {
                bytes_per_source: vec![0; cfg.sources as usize],
                max_target: cfg.initial_conns,
                ..Default::default()
            },
            cfg,
            coverage,
            inflight: BTreeMap::new(),
            next_req: 1,
            phase,
            interval_start: None,
            interval_bytes: 0,
            agg_rate: 0.0,
            throttle_episode_until: 0,
            no_increase_until: 0,
            consecutive_failures: 0,
            last_progress: None,
            head,
            hedge_used: 0,
            finished: false,
            last_now: 0,
        }
    }

    pub fn config(&self) -> &EngineConfig {
        &self.cfg
    }

    pub fn coverage(&self) -> &IntervalSet {
        &self.coverage
    }

    pub fn stats(&self) -> Stats {
        let mut s = self.stats.clone();
        s.final_target = self.target;
        s
    }

    pub fn target_conns(&self) -> u32 {
        self.target
    }

    pub fn active(&self) -> usize {
        self.inflight.len()
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Aggregate throughput over the last controller interval, bytes/second.
    pub fn throughput(&self) -> u64 {
        (self.agg_rate * 1000.0) as u64
    }

    pub fn handle(&mut self, ev: Event) -> Vec<Command> {
        let mut out = Vec::new();
        if self.finished {
            return out;
        }
        let now = ev.now().max(self.last_now);
        self.last_now = now;
        if self.last_progress.is_none() {
            self.last_progress = Some(now);
        }
        match ev {
            Event::Tick { .. } => {}
            Event::Data {
                req, offset, len, ..
            } => self.on_data(now, req, offset, len, &mut out),
            Event::Done { req, .. } => {
                if let Some(r) = self.inflight.remove(&req) {
                    self.sources[r.source as usize].inflight -= 1;
                }
            }
            Event::Failed { req, kind, .. } => self.on_failed(now, req, kind, &mut out),
            Event::Consumed { head, .. } => self.head = self.head.max(head),
        }
        if self.finished {
            return out;
        }
        if self.coverage.contains(0, self.cfg.size) {
            self.finish(&mut out);
            return out;
        }
        if let Some(last) = self.last_progress {
            if now.saturating_sub(last) > self.cfg.stall_abort_ms {
                self.abort(
                    FatalKind::Other,
                    format!("no progress for {} s", self.cfg.stall_abort_ms / 1000),
                    &mut out,
                );
                return out;
            }
        }
        self.controller(now);
        self.hedge_stragglers(now, &mut out);
        self.fill(now, &mut out);
        out
    }

    fn on_data(&mut self, now: u64, req: ReqId, offset: u64, len: u64, out: &mut Vec<Command>) {
        if len == 0 {
            return;
        }
        let end = (offset + len).min(self.cfg.size);
        if offset < end {
            self.coverage.insert(offset, end);
        }
        self.interval_bytes += len;
        self.consecutive_failures = 0;
        self.last_progress = Some(now);
        if let Some(r) = self.inflight.get_mut(&req) {
            r.pos = r.pos.max(end);
            r.sample_bytes += len;
            let dt = now.saturating_sub(r.sample_at);
            if dt >= 250 {
                let inst = r.sample_bytes as f64 / dt as f64;
                r.rate = Some(match r.rate {
                    Some(old) => 0.3 * inst + 0.7 * old,
                    None => inst,
                });
                r.sample_at = now;
                r.sample_bytes = 0;
            }
            let s = &mut self.sources[r.source as usize];
            s.backoff_level = 0;
            s.throttle_level = 0;
            s.interval_bytes += len;
            self.stats.bytes_per_source[r.source as usize] += len;
        }
        // Requests whose remaining bytes are now covered elsewhere are redundant.
        let redundant: Vec<ReqId> = self
            .inflight
            .iter()
            .filter(|(_, r)| r.pos < r.end && self.coverage.contains(r.pos, r.end))
            .map(|(&id, _)| id)
            .collect();
        for id in redundant {
            self.cancel(id, out);
        }
    }

    fn on_failed(&mut self, now: u64, req: ReqId, kind: FailKind, out: &mut Vec<Command>) {
        let Some(r) = self.inflight.remove(&req) else {
            return;
        };
        let src = r.source as usize;
        self.sources[src].inflight -= 1;
        match kind {
            FailKind::Transient(_) => {
                self.stats.retries += 1;
                self.consecutive_failures += 1;
                let s = &mut self.sources[src];
                let base = 200u64 << s.backoff_level.min(5);
                s.backoff_level += 1;
                s.cooldown_until = now + self.rng.jitter(base.min(5000), 20);
                if self.consecutive_failures > self.cfg.retry_budget {
                    self.abort(
                        FatalKind::Other,
                        format!(
                            "{} consecutive failures without progress",
                            self.consecutive_failures
                        ),
                        out,
                    );
                }
            }
            FailKind::Throttled { retry_after_ms } => {
                self.stats.throttles += 1;
                let s = &mut self.sources[src];
                let base = (1000u64 << s.throttle_level.min(5)).min(30_000);
                s.throttle_level += 1;
                let wait = self.rng.jitter(base, 20).max(retry_after_ms.unwrap_or(0));
                s.cooldown_until = s.cooldown_until.max(now + wait);
                if now >= self.throttle_episode_until {
                    // One multiplicative decrease per throttle episode.
                    self.target = (self.target / 2).max(1);
                    if !matches!(self.phase, Phase::Fixed) {
                        self.phase = Phase::Track {
                            dir: 1,
                            last_u: i64::MIN,
                        };
                    }
                }
                self.throttle_episode_until = self.throttle_episode_until.max(now + wait);
                self.no_increase_until = now + wait + self.cfg.probe_ms;
            }
            FailKind::Fatal { kind, msg } => {
                self.sources[src].enabled = false;
                if self.sources.iter().all(|s| !s.enabled) {
                    self.abort(kind, msg, out);
                }
            }
        }
    }

    fn cancel(&mut self, id: ReqId, out: &mut Vec<Command>) {
        if let Some(r) = self.inflight.remove(&id) {
            self.sources[r.source as usize].inflight -= 1;
            self.stats.cancels += 1;
            out.push(Command::Cancel { req: id });
        }
    }

    fn finish(&mut self, out: &mut Vec<Command>) {
        let ids: Vec<ReqId> = self.inflight.keys().copied().collect();
        for id in ids {
            self.cancel(id, out);
        }
        self.finished = true;
        out.push(Command::Finished);
    }

    fn abort(&mut self, kind: FatalKind, msg: String, out: &mut Vec<Command>) {
        let ids: Vec<ReqId> = self.inflight.keys().copied().collect();
        for id in ids {
            self.cancel(id, out);
        }
        self.finished = true;
        out.push(Command::Abort { kind, msg });
    }

    /// Concurrency controller, evaluated once per `probe_ms` interval.
    fn controller(&mut self, now: u64) {
        let start = *self.interval_start.get_or_insert(now);
        let elapsed = now.saturating_sub(start);
        if elapsed < self.cfg.probe_ms {
            return;
        }
        let rate = self.interval_bytes as f64 / elapsed as f64;
        self.agg_rate = rate;
        for s in &mut self.sources {
            let r = s.interval_bytes as f64 / elapsed as f64;
            s.rate = if s.rate == 0.0 {
                r
            } else {
                0.5 * r + 0.5 * s.rate
            };
            s.interval_bytes = 0;
        }
        self.interval_start = Some(now);
        self.interval_bytes = 0;
        if self.sources.iter().all(|s| s.cooldown_until > now) {
            self.stats.throttled_ms += elapsed;
        }
        let cap = self.useful_cap();
        let u = utility(rate, self.target);
        match &mut self.phase {
            Phase::Fixed => {}
            Phase::Probe {
                levels,
                idx,
                results,
            } => {
                results.push((self.target, u));
                self.stats
                    .probe_results
                    .push((self.target, (rate * 1000.0) as u64));
                *idx += 1;
                if *idx < levels.len() && levels[*idx] <= cap {
                    self.target = levels[*idx];
                } else {
                    // Highest utility wins; ties go to fewer connections.
                    let best = results
                        .iter()
                        .copied()
                        .max_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(&a.0)))
                        .expect("at least one result");
                    self.target = best.0;
                    self.phase = Phase::Track {
                        dir: 1,
                        last_u: best.1,
                    };
                }
            }
            Phase::Track { dir, last_u } => {
                if now < self.no_increase_until {
                    *last_u = u;
                } else {
                    if u <= *last_u {
                        *dir = -*dir;
                    }
                    *last_u = u;
                    let next =
                        (self.target as i64 + *dir as i64).clamp(1, self.cfg.max_conns as i64);
                    self.target = (next as u32).min(cap.max(1));
                }
            }
        }
        self.stats.max_target = self.stats.max_target.max(self.target);
    }

    /// More connections than remaining blocks cannot help.
    fn useful_cap(&self) -> u32 {
        let remaining = self.cfg.size - self.coverage.covered().min(self.cfg.size);
        let blocks = remaining.div_ceil(self.cfg.min_block).max(1);
        (blocks.min(self.cfg.max_conns as u64)) as u32
    }

    fn block_size(&self) -> u64 {
        let active = self.inflight.len().max(1) as f64;
        let per_conn = self.agg_rate / active; // bytes per ms
        let raw = if per_conn > 0.0 {
            // Aim for roughly one second of transfer per block.
            (per_conn * 1000.0) as u64
        } else {
            self.cfg.initial_block
        };
        let raw = raw.clamp(self.cfg.min_block, self.cfg.max_block);
        // Quantise to 1 MiB (and alignment) so tiny rate noise doesn't matter (DET-03).
        round_up(
            round_up(raw, MIB.min(self.cfg.max_block)),
            self.cfg.alignment,
        )
    }

    fn pick_source(&self, now: u64, avoid: Option<SourceId>) -> Option<SourceId> {
        let mut best: Option<(SourceId, f64)> = None;
        for (i, s) in self.sources.iter().enumerate() {
            if !s.enabled || s.cooldown_until > now {
                continue;
            }
            let i = i as SourceId;
            // Load relative to measured speed; unknown speed counts as fast.
            let speed = if s.rate > 0.0 { s.rate } else { 1e9 };
            let mut score = (s.inflight as f64 + 1.0) / speed;
            if Some(i) == avoid && self.sources.len() > 1 {
                score *= 4.0;
            }
            if best.is_none_or(|(_, b)| score < b) {
                best = Some((i, score));
            }
        }
        best.map(|(i, _)| i)
    }

    fn claimed(&self) -> IntervalSet {
        let mut c = self.coverage.clone();
        for r in self.inflight.values() {
            if r.pos < r.end {
                c.insert(r.pos, r.end);
            }
        }
        c
    }

    fn issue(&mut self, now: u64, source: SourceId, range: ByteRange, out: &mut Vec<Command>) {
        let req = self.next_req;
        self.next_req += 1;
        self.inflight.insert(
            req,
            Req {
                source,
                pos: range.start,
                end: range.end,
                started: now,
                sample_at: now,
                sample_bytes: 0,
                rate: None,
                hedged: false,
            },
        );
        self.sources[source as usize].inflight += 1;
        self.stats.requests += 1;
        out.push(Command::Fetch { req, source, range });
    }

    fn fill(&mut self, now: u64, out: &mut Vec<Command>) {
        let limit = match self.cfg.window {
            Some(w) => self.head.saturating_add(w).min(self.cfg.size),
            None => self.cfg.size,
        };
        let floor = if self.cfg.window.is_some() {
            self.head
        } else {
            0
        };
        while (self.inflight.len() as u32) < self.target {
            let Some(source) = self.pick_source(now, None) else {
                break;
            };
            let claimed = self.claimed();
            if let Some(gap) = claimed.first_gap(floor, limit) {
                let start = round_down(gap.start, self.cfg.alignment);
                let block = self.block_size();
                let end = round_up(start + block, self.cfg.alignment)
                    .min(gap.end.max(start + 1))
                    .min(self.cfg.size);
                // Never leave a sliver smaller than half a block at the end of a gap.
                let end = if gap.end - end < self.cfg.min_block / 2 && gap.end <= limit {
                    gap.end
                } else {
                    end
                };
                self.issue(now, source, ByteRange::new(start, end), out);
                continue;
            }
            // No unclaimed bytes: steal from or hedge the largest remaining request.
            if !self.steal_or_hedge(now, None, out) {
                break;
            }
        }
    }

    /// Split the tail off a request (no waste) or, if it is too small to split,
    /// duplicate its remainder (hedge). Returns whether a request was issued.
    fn steal_or_hedge(&mut self, now: u64, victim: Option<ReqId>, out: &mut Vec<Command>) -> bool {
        let victim = match victim {
            Some(v) => v,
            None => {
                let candidate = self
                    .inflight
                    .iter()
                    .filter(|(_, r)| !r.hedged && r.remaining() > 0)
                    .filter(|(_, r)| {
                        self.cfg
                            .window
                            .is_none_or(|w| r.pos < self.head.saturating_add(w))
                    })
                    .max_by(|a, b| a.1.remaining().cmp(&b.1.remaining()).then(b.0.cmp(a.0)));
                match candidate {
                    Some((&id, _)) => id,
                    None => return false,
                }
            }
        };
        let Some(r) = self.inflight.get(&victim).cloned() else {
            return false;
        };
        let remaining = r.remaining();
        if remaining >= 2 * self.cfg.min_block {
            let mid = round_down(r.pos + remaining / 2, self.cfg.alignment);
            if mid > r.pos && mid < r.end {
                let Some(source) = self.pick_source(now, Some(r.source)) else {
                    return false;
                };
                let vr = self.inflight.get_mut(&victim).expect("victim exists");
                vr.end = mid;
                vr.hedged = true;
                out.push(Command::Shrink {
                    req: victim,
                    end: mid,
                });
                self.stats.steals += 1;
                self.issue(now, source, ByteRange::new(mid, r.end), out);
                return true;
            }
        }
        // Duplicates cost bandwidth, so only hedge a request that has had a
        // fair chance to finish on its own (Tail at Scale: hedge after a delay).
        let start = round_down(r.pos, self.cfg.alignment);
        let dup_len = r.end - start;
        let old_enough = now.saturating_sub(r.started) >= self.cfg.straggler_grace_ms / 2;
        if !self.cfg.hedge || !old_enough || self.hedge_used + dup_len > self.cfg.hedge_budget {
            return false;
        }
        let Some(source) = self.pick_source(now, Some(r.source)) else {
            return false;
        };
        self.inflight
            .get_mut(&victim)
            .expect("victim exists")
            .hedged = true;
        self.hedge_used += dup_len;
        self.stats.hedges += 1;
        self.stats.hedged_bytes += dup_len;
        let dup = self.next_req;
        self.issue(now, source, ByteRange::new(start, r.end), out);
        // Never hedge a hedge.
        self.inflight.get_mut(&dup).expect("just issued").hedged = true;
        true
    }

    /// Hedge requests that run far slower than their peers (Tail at Scale).
    fn hedge_stragglers(&mut self, now: u64, out: &mut Vec<Command>) {
        let grace = self.cfg.straggler_grace_ms;
        let mut rates: Vec<f64> = self
            .inflight
            .values()
            .filter(|r| now.saturating_sub(r.started) >= grace)
            .map(|r| r.effective_rate(now))
            .filter(|r| r.is_finite())
            .collect();
        if rates.len() < 2 {
            return;
        }
        rates.sort_by(|a, b| a.total_cmp(b));
        let median = rates[rates.len() / 2];
        if median <= 0.0 {
            return;
        }
        let head = self.coverage.contiguous_end(self.head);
        let slow: Vec<ReqId> = self
            .inflight
            .iter()
            .filter(|(_, r)| {
                !r.hedged && now.saturating_sub(r.started) >= grace && r.remaining() > 0
            })
            .filter(|(_, r)| {
                let rate = r.effective_rate(now);
                // Requests blocking an in-order consumer are hedged more eagerly.
                let blocking_head = self.cfg.window.is_some() && r.pos <= head && head < r.end;
                let threshold = if blocking_head { 0.5 } else { 0.3 };
                rate < threshold * median
            })
            .map(|(&id, _)| id)
            .collect();
        for id in slow {
            // One extra request above the target is allowed for a hedge: the
            // straggler will shrink or be cancelled shortly.
            if self.inflight.len() as u32 > self.target {
                break;
            }
            self.steal_or_hedge(now, Some(id), out);
        }
    }
}

/// Utility of running `n` connections at `rate` bytes/ms: log-bucketed
/// throughput minus a per-connection penalty (Falcon-style), as an integer so
/// comparisons are exact.
fn utility(rate: f64, n: u32) -> i64 {
    let bucket = if rate <= 0.0 {
        0.0
    } else {
        ((rate + 1.0).ln() / 1.05f64.ln()).floor()
    };
    // ln(1.02)/ln(1.05) ≈ 0.406 buckets per connection.
    (bucket as i64) * 1000 - n as i64 * 406
}

fn round_up(v: u64, a: u64) -> u64 {
    if a <= 1 { v } else { v.div_ceil(a) * a }
}

fn round_down(v: u64, a: u64) -> u64 {
    if a <= 1 { v } else { v / a * a }
}

#[cfg(test)]
mod tests;
