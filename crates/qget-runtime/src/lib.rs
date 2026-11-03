//! The I/O shell around `qget-core` (AD-1).
//!
//! [`driver::run`] owns the event loop: it asks the engine what to do, runs
//! range requests against [`qget_core::Source`]s, hands bytes to a
//! [`qget_core::Sink`] on a dedicated thread, and reports progress.

pub mod driver;
pub mod file_sink;
pub mod fsutil;
pub mod limiter;
pub mod stream_sink;
pub mod trace;

pub use driver::{Progress, RunOptions, RunReport, run};
pub use file_sink::{FileSink, FileSinkConfig, ResumeDecision};
pub use stream_sink::{StreamSink, WriteConsumer};
