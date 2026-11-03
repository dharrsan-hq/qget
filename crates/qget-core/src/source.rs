//! The interface every byte source implements (AD-2): HTTP mirrors, HTTP/3,
//! and P2P peers all answer "give me bytes `[start, end)`".
//!
//! Only `std` types are used here so that `qget-core` stays runtime-free.

use crate::engine::FailKind;
use crate::range::ByteRange;
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A stream of body chunks for one range request, in order, starting at the
/// range's first byte. The runtime enforces the byte count and idle timeouts.
pub trait ChunkStream: Send {
    /// Next chunk, `None` at end of body.
    fn next_chunk(&mut self) -> BoxFuture<'_, Option<Result<Bytes, FailKind>>>;
}

pub trait Source: Send + Sync + 'static {
    /// Start fetching `range`. Errors are classified for the scheduler.
    fn open(&self, range: ByteRange) -> BoxFuture<'static, Result<Box<dyn ChunkStream>, FailKind>>;

    /// Short human-readable description (host, peer id) for logs and `--explain`.
    fn describe(&self) -> String;
}
