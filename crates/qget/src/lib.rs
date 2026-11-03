//! # qget
//!
//! Fast, verified, resumable downloads, with parallel stream-extraction of
//! archives. This is the public library; the `qget` command-line tool is a
//! thin layer over it (LIB-03).
//!
//! ```no_run
//! # async fn demo() -> qget::Result<()> {
//! let report = qget::Download::new("https://example.com/snapshot.tar.lz4")
//!     .extract_to("/data/snapshot")
//!     .checksum("sha256:9f2c0000000000000000000000000000000000000000000000000000000000ab")?
//!     .max_buffer(1 << 30)
//!     .run()
//!     .await?;
//! println!("{} bytes in {:?}", report.size, report.elapsed);
//! # Ok(()) }
//! ```
//!
//! The API is experimental until 1.0 (LIB-11).

mod download;
mod refresh;

pub use download::{Bottleneck, Download, Output, Report, Verify};
pub use qget_core::engine::Stats;
pub use qget_core::error::EXIT_CODES;
pub use qget_core::{Error, ExpectedHash, HashAlgo, Result};
pub use qget_http::IpFamily;
pub use qget_runtime::Progress;
pub use qget_runtime::file_sink::{ResumeDecision, hash_file};
pub use tokio_util::sync::CancellationToken;

#[cfg(feature = "extract")]
pub use qget_extract::Format as ArchiveFormat;

/// Replay a `--trace` file and check every decision is reproduced (DET-05).
pub fn replay_trace(path: &std::path::Path) -> Result<(usize, Option<String>)> {
    let f =
        std::fs::File::open(path).map_err(|e| Error::io(format!("open {}", path.display()), e))?;
    let r = qget_runtime::trace::replay(std::io::BufReader::new(f)).map_err(Error::Usage)?;
    Ok((
        r.events,
        r.mismatch.map(|(i, want, got)| {
            format!("event {i}: trace recorded {want:?} but replay produced {got:?}")
        }),
    ))
}

#[cfg(feature = "p2p")]
pub mod p2p {
    //! Peer-to-peer transfer (feature `p2p`).
    pub use qget_p2p::*;
}
