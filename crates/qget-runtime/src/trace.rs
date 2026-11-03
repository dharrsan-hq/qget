//! Decision traces (CLI-05) and exact replay (DET-05).
//!
//! A trace is JSON lines: a header with the engine config and initial
//! coverage, then one line per event with the commands it produced. Replaying
//! feeds the same events to a fresh engine and checks every command matches.

use qget_core::engine::{Command, Engine, EngineConfig, Event};
use qget_core::range::IntervalSet;
use serde::{Deserialize, Serialize};
use std::io::{BufRead, Write};

#[derive(Serialize, Deserialize)]
pub struct TraceHeader {
    pub qget_trace: u32,
    pub config: EngineConfig,
    pub coverage: Vec<(u64, u64)>,
}

#[derive(Serialize, Deserialize)]
pub struct TraceLine {
    pub event: Event,
    pub commands: Vec<Command>,
}

pub struct TraceWriter {
    out: Box<dyn Write + Send>,
}

impl TraceWriter {
    pub fn new(
        mut out: Box<dyn Write + Send>,
        config: &EngineConfig,
        coverage: &IntervalSet,
    ) -> std::io::Result<Self> {
        let header = TraceHeader {
            qget_trace: 1,
            config: config.clone(),
            coverage: coverage.spans().to_vec(),
        };
        serde_json::to_writer(&mut out, &header)?;
        out.write_all(b"\n")?;
        Ok(Self { out })
    }

    pub fn record(&mut self, event: &Event, commands: &[Command]) {
        let line = TraceLine {
            event: event.clone(),
            commands: commands.to_vec(),
        };
        if serde_json::to_writer(&mut self.out, &line).is_ok() {
            let _ = self.out.write_all(b"\n");
        }
    }

    pub fn flush(&mut self) {
        let _ = self.out.flush();
    }
}

#[derive(Debug)]
pub struct ReplayResult {
    pub events: usize,
    /// First event index whose commands differed, with expected and actual.
    pub mismatch: Option<(usize, Vec<Command>, Vec<Command>)>,
}

/// Replay a trace and compare every decision.
pub fn replay(input: impl BufRead) -> Result<ReplayResult, String> {
    let mut lines = input.lines();
    let header: TraceHeader = match lines.next() {
        Some(Ok(l)) => serde_json::from_str(&l).map_err(|e| format!("bad trace header: {e}"))?,
        _ => return Err("empty trace".into()),
    };
    let mut engine = Engine::new(header.config, IntervalSet::from_spans(header.coverage));
    let mut events = 0;
    for (i, line) in lines.enumerate() {
        let line = line.map_err(|e| e.to_string())?;
        if line.trim().is_empty() {
            continue;
        }
        let rec: TraceLine =
            serde_json::from_str(&line).map_err(|e| format!("bad trace line {}: {e}", i + 2))?;
        let got = engine.handle(rec.event);
        events += 1;
        if got != rec.commands {
            return Ok(ReplayResult {
                events,
                mismatch: Some((i, rec.commands, got)),
            });
        }
    }
    Ok(ReplayResult {
        events,
        mismatch: None,
    })
}
