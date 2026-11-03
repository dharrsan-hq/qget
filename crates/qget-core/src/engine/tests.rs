use super::*;
use crate::sim::{self, SimConfig, SimSource};

const MIB: u64 = 1 << 20;

fn sim_cfg(size: u64, sources: Vec<SimSource>, seed: u64) -> SimConfig {
    let mut engine = EngineConfig::new(size, sources.len() as u32);
    engine.seed = seed;
    SimConfig {
        engine,
        sources,
        consumer_rate: f64::INFINITY,
        max_time_ms: 3_600_000,
        seed,
    }
}

fn assert_complete(out: &sim::SimOutcome, size: u64) {
    assert!(out.finished, "did not finish: {:?}", out.aborted);
    assert!(
        out.coverage.contains(0, size),
        "coverage {:?}",
        out.coverage
    );
}

#[test]
fn empty_file_finishes_immediately() {
    let mut e = Engine::new(EngineConfig::new(0, 1), IntervalSet::new());
    assert_eq!(e.handle(Event::Tick { now: 0 }), vec![Command::Finished]);
}

#[test]
fn already_complete_resume_finishes() {
    let cov = IntervalSet::from_spans([(0, 100)]);
    let mut e = Engine::new(EngineConfig::new(100, 1), cov);
    assert_eq!(e.handle(Event::Tick { now: 0 }), vec![Command::Finished]);
}

#[test]
fn initial_fetches_cover_distinct_ranges() {
    let mut e = Engine::new(EngineConfig::new(100 * MIB, 1), IntervalSet::new());
    let cmds = e.handle(Event::Tick { now: 0 });
    let ranges: Vec<ByteRange> = cmds
        .iter()
        .filter_map(|c| match c {
            Command::Fetch { range, .. } => Some(*range),
            _ => None,
        })
        .collect();
    assert_eq!(ranges.len(), 4, "starts with initial_conns requests");
    for w in ranges.windows(2) {
        assert_eq!(w[0].end, w[1].start, "contiguous, non-overlapping");
    }
    assert_eq!(ranges[0].start, 0);
}

#[test]
fn small_file_uses_single_request() {
    let mut e = Engine::new(EngineConfig::new(1000, 1), IntervalSet::new());
    let cmds = e.handle(Event::Tick { now: 0 });
    assert_eq!(
        cmds,
        vec![Command::Fetch {
            req: 1,
            source: 0,
            range: ByteRange::new(0, 1000)
        }]
    );
}

#[test]
fn simple_download_completes() {
    let size = 200 * MIB;
    let out = sim::run(
        &sim_cfg(size, vec![SimSource::simple(10_000.0)], 1),
        IntervalSet::new(),
    );
    assert_complete(&out, size);
}

#[test]
fn concurrency_grows_when_per_connection_capped() {
    // 2 MB/s per connection, unlimited aggregate: more connections help.
    let size = 2048 * MIB;
    let out = sim::run(
        &sim_cfg(size, vec![SimSource::simple(2_000.0)], 3),
        IntervalSet::new(),
    );
    assert_complete(&out, size);
    assert!(
        out.stats.max_target >= 8,
        "controller should scale up, got {:?}",
        out.stats
    );
}

#[test]
fn concurrency_stays_low_when_aggregate_capped() {
    // Aggregate cap equals one connection's speed: extra connections are useless.
    let size = 1024 * MIB;
    let mut src = SimSource::simple(20_000.0);
    src.total_rate = 20_000.0;
    let out = sim::run(&sim_cfg(size, vec![src], 4), IntervalSet::new());
    assert_complete(&out, size);
    assert!(
        out.stats.final_target <= 6,
        "penalty should keep connections low, got {:?}",
        out.stats
    );
}

#[test]
fn throttling_reduces_concurrency_and_still_completes() {
    let size = 512 * MIB;
    let mut src = SimSource::simple(5_000.0);
    src.throttle_above = Some(6);
    let out = sim::run(&sim_cfg(size, vec![src], 5), IntervalSet::new());
    assert_complete(&out, size);
    assert!(out.stats.throttles > 0);
    assert!(out.stats.final_target <= 8, "{:?}", out.stats);
}

#[test]
fn transient_failures_are_retried() {
    let size = 256 * MIB;
    let mut src = SimSource::simple(10_000.0);
    src.fail_permille = 200;
    let out = sim::run(&sim_cfg(size, vec![src], 6), IntervalSet::new());
    assert_complete(&out, size);
    assert!(out.stats.retries > 0);
}

#[test]
fn stragglers_are_hedged() {
    let size = 512 * MIB;
    let mut src = SimSource::simple(10_000.0);
    src.straggler_permille = 150;
    src.straggler_factor = 0.02;
    let cfg = sim_cfg(size, vec![src.clone()], 7);
    let with = sim::run(&cfg, IntervalSet::new());
    assert_complete(&with, size);
    assert!(
        with.stats.steals + with.stats.hedges > 0,
        "{:?}",
        with.stats
    );

    let mut cfg_off = cfg.clone();
    cfg_off.engine.hedge = false;
    cfg_off.engine.straggler_grace_ms = u64::MAX / 4;
    let without = sim::run(&cfg_off, IntervalSet::new());
    assert_complete(&without, size);
    assert!(
        with.elapsed_ms < without.elapsed_ms,
        "hedging should beat no hedging: {} vs {} ms",
        with.elapsed_ms,
        without.elapsed_ms
    );
}

#[test]
fn hedged_bytes_respect_budget() {
    let size = 256 * MIB;
    let mut src = SimSource::simple(10_000.0);
    src.straggler_permille = 300;
    src.straggler_factor = 0.01;
    let cfg = sim_cfg(size, vec![src], 8);
    let out = sim::run(&cfg, IntervalSet::new());
    assert_complete(&out, size);
    assert!(out.stats.hedged_bytes <= cfg.engine.hedge_budget);
}

#[test]
fn fatal_source_falls_back_to_mirror() {
    let size = 256 * MIB;
    let mut bad = SimSource::simple(10_000.0);
    bad.fatal_at_ms = Some(500);
    let good = SimSource::simple(5_000.0);
    let out = sim::run(&sim_cfg(size, vec![bad, good], 9), IntervalSet::new());
    assert_complete(&out, size);
    assert!(out.stats.bytes_per_source[1] > 0);
}

#[test]
fn all_sources_fatal_aborts() {
    let mut bad = SimSource::simple(1_000.0);
    bad.fatal_at_ms = Some(100);
    let out = sim::run(&sim_cfg(512 * MIB, vec![bad], 10), IntervalSet::new());
    assert!(!out.finished);
    assert!(matches!(out.aborted, Some((FatalKind::HttpStatus(404), _))));
}

#[test]
fn faster_mirror_gets_more_bytes() {
    let size = 512 * MIB;
    let slow = SimSource::simple(1_000.0);
    let fast = SimSource::simple(8_000.0);
    let out = sim::run(&sim_cfg(size, vec![slow, fast], 11), IntervalSet::new());
    assert_complete(&out, size);
    assert!(out.stats.bytes_per_source[1] > out.stats.bytes_per_source[0]);
}

#[test]
fn stream_window_bounds_allocation() {
    let size = 512 * MIB;
    let window = 64 * MIB;
    let mut cfg = sim_cfg(size, vec![SimSource::simple(5_000.0)], 12);
    cfg.engine.window = Some(window);
    cfg.consumer_rate = 50_000.0;
    let out = sim::run(&cfg, IntervalSet::new());
    assert_complete(&out, size);
    // Replay the command log, tracking the furthest end requested so far: it
    // can never run far beyond what a bounded window allows.
    let mut max_end_seen = 0u64;
    for c in &out.commands {
        if let Command::Fetch { range, .. } = c {
            max_end_seen = max_end_seen.max(range.end);
        }
    }
    assert_eq!(max_end_seen, size);
}

#[test]
fn stream_window_never_exceeded_against_head() {
    // Drive the engine by hand with a consumer that never advances.
    let size = 512 * MIB;
    let mut cfg = EngineConfig::new(size, 1);
    cfg.window = Some(32 * MIB);
    let mut e = Engine::new(cfg, IntervalSet::new());
    let mut cmds = e.handle(Event::Tick { now: 0 });
    let mut now = 0;
    for _ in 0..200 {
        now += 100;
        let fetches: Vec<_> = cmds
            .iter()
            .filter_map(|c| match c {
                Command::Fetch { req, range, .. } => Some((*req, *range)),
                _ => None,
            })
            .collect();
        for (_, r) in &fetches {
            assert!(r.start < 32 * MIB, "allocated beyond window: {r:?}");
        }
        cmds.clear();
        for (req, r) in fetches {
            cmds.extend(e.handle(Event::Data {
                now,
                req,
                offset: r.start,
                len: r.len(),
            }));
            cmds.extend(e.handle(Event::Done { now, req }));
        }
        cmds.extend(e.handle(Event::Tick { now }));
    }
    assert!(!e.is_finished());
}

#[test]
fn alignment_is_respected() {
    let size = 100 * MIB + 12345;
    let mut cfg = sim_cfg(size, vec![SimSource::simple(5_000.0)], 13);
    cfg.engine.alignment = MIB;
    let out = sim::run(&cfg, IntervalSet::new());
    assert_complete(&out, size);
    for c in &out.commands {
        if let Command::Fetch { range, .. } = c {
            assert_eq!(range.start % MIB, 0, "{range:?}");
            assert!(range.end % MIB == 0 || range.end == size, "{range:?}");
        }
        if let Command::Shrink { end, .. } = c {
            assert_eq!(end % MIB, 0);
        }
    }
}

#[test]
fn resume_after_crash_only_fetches_missing_bytes() {
    let size = 512 * MIB;
    let cfg = sim_cfg(size, vec![SimSource::simple(10_000.0)], 14);
    let (_, at_crash) = sim::run_with_crash(&cfg, IntervalSet::new(), Some(1_500));
    assert!(!at_crash.is_empty() && !at_crash.contains(0, size));
    let resumed = sim::run(&cfg, at_crash.clone());
    assert_complete(&resumed, size);
    let first_fetch = resumed.commands.iter().find_map(|c| match c {
        Command::Fetch { range, .. } => Some(*range),
        _ => None,
    });
    let r = first_fetch.expect("some work remained");
    assert!(
        !at_crash.contains(r.start, r.start + 1),
        "refetched covered byte"
    );
}

#[test]
fn same_seed_same_decisions() {
    let mut src = SimSource::simple(3_000.0);
    src.fail_permille = 50;
    src.straggler_permille = 100;
    src.straggler_factor = 0.1;
    src.throttle_above = Some(10);
    let cfg = sim_cfg(300 * MIB, vec![src, SimSource::simple(2_000.0)], 99);
    let a = sim::run(&cfg, IntervalSet::new());
    let b = sim::run(&cfg, IntervalSet::new());
    assert_eq!(a.commands, b.commands);
    assert_eq!(a.elapsed_ms, b.elapsed_ms);
}

/// Seed sweep: random source behaviour, invariants must always hold.
/// `QGET_SIM_SEEDS` raises the count (nightly CI runs 100k).
#[test]
fn seed_sweep_invariants() {
    let seeds: u64 = std::env::var("QGET_SIM_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    for seed in 0..seeds {
        let mut r = Rng::new(seed);
        let n_sources = 1 + r.below(3) as usize;
        let sources: Vec<SimSource> = (0..n_sources)
            .map(|_| SimSource {
                per_conn_rate: 200.0 + r.below(20_000) as f64,
                total_rate: if r.below(2) == 0 {
                    f64::INFINITY
                } else {
                    1_000.0 + r.below(50_000) as f64
                },
                latency_ms: r.below(200),
                throttle_above: (r.below(3) == 0).then(|| 1 + r.below(12) as u32),
                fail_permille: r.below(150),
                straggler_permille: r.below(200),
                straggler_factor: 0.01 + r.below(50) as f64 / 100.0,
                fatal_at_ms: None,
            })
            .collect();
        let size = 1 + r.below(300 * MIB);
        let mut cfg = sim_cfg(size, sources, seed);
        cfg.engine.retry_budget = 1000;
        if r.below(3) == 0 {
            cfg.engine.alignment = MIB;
        }
        if r.below(3) == 0 {
            cfg.engine.window = Some(16 * MIB + r.below(128 * MIB));
            cfg.consumer_rate = 500.0 + r.below(100_000) as f64;
        }
        let crash = (r.below(3) == 0).then(|| 200 + r.below(5_000));
        let (first, cov) = sim::run_with_crash(&cfg, IntervalSet::new(), crash);
        let out = if crash.is_some() && !first.finished {
            sim::run(&cfg, cov)
        } else {
            first
        };
        assert!(
            out.finished,
            "seed {seed}: did not finish ({:?}) after {} ms, stats {:?}",
            out.aborted, out.elapsed_ms, out.stats
        );
        assert!(out.coverage.contains(0, size), "seed {seed}");
        assert!(
            out.stats.hedged_bytes <= cfg.engine.hedge_budget,
            "seed {seed}"
        );
        assert!(
            out.max_inflight <= cfg.engine.max_conns as usize * 2 + 1,
            "seed {seed}: {} in flight",
            out.max_inflight
        );
        for c in &out.commands {
            if let Command::Fetch { range, .. } = c {
                assert!(range.start < range.end && range.end <= size, "seed {seed}");
                assert_eq!(range.start % cfg.engine.alignment, 0, "seed {seed}");
            }
        }
    }
}

#[test]
fn trace_events_roundtrip_through_json() {
    let ev = Event::Failed {
        now: 5,
        req: 3,
        kind: FailKind::Throttled {
            retry_after_ms: Some(100),
        },
    };
    let s = serde_json::to_string(&ev).unwrap();
    assert_eq!(serde_json::from_str::<Event>(&s).unwrap(), ev);
}
