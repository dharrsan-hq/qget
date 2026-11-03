//! Typed errors shared by every qget crate (LIB-07).
//!
//! Each variant maps to exactly one documented CLI exit code (CLI-03).

use std::io;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("invalid usage: {0}")]
    Usage(String),

    #[error("network error: {0}")]
    Network(String),

    #[error("HTTP {status} from {url}")]
    HttpStatus { status: u16, url: String },

    #[error("integrity check failed: expected {expected}, got {actual}")]
    Integrity { expected: String, actual: String },

    #[error("remote resource changed: {0}")]
    RemoteChanged(String),

    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: io::Error,
    },

    #[error("not enough disk space: need {needed} bytes, {available} available")]
    DiskFull { needed: u64, available: u64 },

    #[error("interrupted; run the same command again to resume")]
    Interrupted,

    #[error("extraction failed: {0}")]
    Extract(String),

    #[error("peer-to-peer transfer failed: {0}")]
    P2p(String),

    #[error("unsupported: {0}")]
    Unsupported(String),
}

impl Error {
    pub fn io(context: impl Into<String>, source: io::Error) -> Self {
        if source.raw_os_error() == Some(28) {
            // ENOSPC
            return Error::DiskFull {
                needed: 0,
                available: 0,
            };
        }
        Error::Io {
            context: context.into(),
            source,
        }
    }

    /// Process exit code for this error, as documented in `qget(1)`.
    pub fn exit_code(&self) -> i32 {
        match self {
            Error::Usage(_) => 2,
            Error::Network(_) => 3,
            Error::HttpStatus { .. } => 4,
            Error::Integrity { .. } => 5,
            Error::RemoteChanged(_) => 6,
            Error::Io { .. } | Error::DiskFull { .. } => 7,
            Error::Interrupted => 8,
            Error::Extract(_) => 9,
            Error::P2p(_) => 10,
            Error::Unsupported(_) => 11,
        }
    }
}

/// Exit codes, also printed in the man page.
pub const EXIT_CODES: &[(i32, &str)] = &[
    (0, "success"),
    (1, "unexpected internal error"),
    (2, "invalid usage"),
    (3, "network error (connection, DNS, TLS, timeout)"),
    (4, "HTTP error status (e.g. 404, 403)"),
    (5, "integrity check failed (checksum mismatch)"),
    (6, "remote resource changed during or between downloads"),
    (7, "local I/O error or disk full"),
    (8, "interrupted; resumable by re-running the same command"),
    (9, "archive extraction failed"),
    (10, "peer-to-peer transfer failed"),
    (11, "unsupported server or input"),
];
