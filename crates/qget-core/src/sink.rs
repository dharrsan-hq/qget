//! Interfaces between the runtime and whatever consumes downloaded bytes.

use crate::{ExpectedHash, Result};
use bytes::Bytes;

/// Where downloaded bytes go. Implementations are driven from a dedicated
/// blocking thread, so `write` may block to apply backpressure.
pub trait Sink: Send {
    /// Accept `data` at absolute `offset`. Overlapping writes always carry
    /// identical bytes (sources are validated), so they may be applied twice.
    fn write(&mut self, offset: u64, data: Bytes) -> Result<()>;

    /// For ordered sinks: how many leading bytes have been handed onwards.
    fn consumed(&self) -> Option<u64> {
        None
    }

    /// All bytes have been written: flush, verify, and finalise.
    fn finish(&mut self) -> Result<SinkReport>;

    /// Stop early, leaving resumable state behind where possible.
    fn abort(&mut self) -> Result<()>;
}

#[derive(Clone, Debug, Default)]
pub struct SinkReport {
    /// Hash computed over the bytes in order, if hashing was enabled.
    pub hash: Option<ExpectedHash>,
    /// Final path(s) produced.
    pub output: Option<std::path::PathBuf>,
    pub consumer: Option<ConsumerReport>,
}

/// Receives the download as an in-order byte stream (stdout, extraction).
pub trait OrderedConsumer: Send {
    /// Consume the next bytes of the stream. May block for backpressure.
    fn consume(&mut self, data: &[u8]) -> Result<()>;

    /// The stream ended and its hash (if any) has been verified.
    fn finish(self: Box<Self>) -> Result<ConsumerReport>;

    /// The stream will not complete; clean up.
    fn abort(self: Box<Self>);
}

#[derive(Clone, Debug, Default)]
pub struct ConsumerReport {
    pub entries: u64,
    pub bytes_out: u64,
    pub output: Option<std::path::PathBuf>,
    /// Human-readable notes (e.g. "decode-bound: single-frame zstd").
    pub notes: Vec<String>,
    /// Busy time of the decoder stage, used for bottleneck analysis.
    pub decode_busy_ms: u64,
}
