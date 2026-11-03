//! I/O-free core of qget.
//!
//! Everything in this crate is deterministic computation: the download
//! [`engine`] is a pure state machine driven by events and a seed, the
//! [`journal`] module only encodes and decodes bytes, and the hashing helpers
//! never touch the filesystem. The I/O shell lives in `qget-runtime`.

pub mod b3tree;
pub mod content_range;
pub mod engine;
pub mod error;
pub mod hash;
pub mod journal;
pub mod range;
pub mod rng;
pub mod sim;
pub mod sink;
pub mod source;

pub use engine::{Command, Engine, EngineConfig, Event, FailKind, ReqId, SourceId};
pub use error::{Error, Result};
pub use hash::{ExpectedHash, HashAlgo, Hasher};
pub use range::{ByteRange, IntervalSet};
pub use sink::{ConsumerReport, OrderedConsumer, Sink, SinkReport};
pub use source::{BoxFuture, ChunkStream, Source};

/// Remote validators used to detect that a resource changed between requests
/// or between runs (DUR-05, HTTP-03).
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Validator {
    /// Strong ETag including quotes, e.g. `"abc"`. Weak validators are never stored.
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

impl Validator {
    pub fn is_empty(&self) -> bool {
        self.etag.is_none() && self.last_modified.is_none()
    }

    /// Value to send in `If-Range`: the strong ETag if known, else Last-Modified.
    pub fn if_range(&self) -> Option<&str> {
        self.etag.as_deref().or(self.last_modified.as_deref())
    }

    /// Whether `other` describes the same representation as `self`.
    ///
    /// Returns `None` when the two cannot be compared because neither side has
    /// a validator in common.
    pub fn same_as(&self, other: &Validator) -> Option<bool> {
        if let (Some(a), Some(b)) = (&self.etag, &other.etag) {
            return Some(a == b);
        }
        if let (Some(a), Some(b)) = (&self.last_modified, &other.last_modified) {
            return Some(a == b);
        }
        None
    }
}
