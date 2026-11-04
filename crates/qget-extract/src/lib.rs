//! Streaming archive extraction (§11).
//!
//! [`Extractor`] is an [`OrderedConsumer`]: it receives the archive bytes in
//! order and extracts them while the download is still running. The pipeline:
//!
//! ```text
//! bytes ─► format probe ─► decoder (parallel when the format allows) ─► tar
//!       parser ─► writer pool ─► staging dir ─► atomic rename on success
//! ```
//!
//! Decode plans (§11.3): LZ4 frames with independent blocks and multi-frame
//! zstd decode in parallel; single-frame zstd, linked-block LZ4, gzip and xz
//! decode serially and are reported as such.

mod decode;
mod format;
mod platform;
mod tarw;

pub use format::{Format, detect};
pub use tarw::{TarOptions, TarReport, extract_tar};

use qget_core::{ConsumerReport, Error, OrderedConsumer, Result};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct ExtractOptions {
    pub dest: PathBuf,
    /// Decoder and writer threads.
    pub threads: usize,
    /// Skip (instead of refusing) device nodes and FIFOs.
    pub allow_special: bool,
    /// Force a format instead of sniffing magic bytes.
    pub format: Option<Format>,
    /// Extract into an existing non-empty directory (entries are still
    /// written to a staging directory first and moved in afterwards).
    pub into_existing: bool,
}

impl ExtractOptions {
    pub fn new(dest: impl Into<PathBuf>) -> Self {
        Self {
            dest: dest.into(),
            threads: std::thread::available_parallelism()
                .map_or(4, |n| n.get())
                .clamp(2, 32),
            allow_special: false,
            format: None,
            into_existing: false,
        }
    }
}

/// Staging directory used for `dest`.
pub fn staging_path(dest: &Path) -> PathBuf {
    let mut s = dest.as_os_str().to_owned();
    s.push(".qget-staging");
    PathBuf::from(s)
}

/// Check `dest` before any bytes are downloaded, so a bad target fails fast.
pub fn check_destination(dest: &Path, into_existing: bool) -> Result<()> {
    match std::fs::read_dir(dest) {
        Ok(mut it) => {
            if it.next().is_some() && !into_existing {
                return Err(Error::Usage(format!(
                    "{} exists and is not empty (use --extract-into-existing to merge)",
                    dest.display()
                )));
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(Error::io(format!("inspect {}", dest.display()), e)),
    }
}

pub struct Extractor {
    tx: Option<SyncSender<Vec<u8>>>,
    thread: Option<JoinHandle<Result<ConsumerReport>>>,
    staging: PathBuf,
    dest: PathBuf,
    into_existing: bool,
}

impl Extractor {
    pub fn new(opts: ExtractOptions) -> Result<Self> {
        check_destination(&opts.dest, opts.into_existing)?;
        let staging = staging_path(&opts.dest);
        if staging.exists() {
            tracing::warn!("removing stale staging directory {}", staging.display());
            std::fs::remove_dir_all(&staging)
                .map_err(|e| Error::io(format!("remove {}", staging.display()), e))?;
        }
        std::fs::create_dir_all(&staging)
            .map_err(|e| Error::io(format!("create {}", staging.display()), e))?;
        let (tx, rx) = sync_channel::<Vec<u8>>(128);
        let (dest, into_existing) = (opts.dest.clone(), opts.into_existing);
        let staging2 = staging.clone();
        let thread = std::thread::Builder::new()
            .name("qget-extract".into())
            .spawn(move || pipeline(rx, staging2, opts))
            .map_err(|e| Error::io("spawn extract thread", e))?;
        Ok(Self {
            tx: Some(tx),
            thread: Some(thread),
            staging,
            dest,
            into_existing,
        })
    }

    fn join(&mut self) -> Result<ConsumerReport> {
        self.tx.take();
        match self.thread.take() {
            Some(t) => t
                .join()
                .unwrap_or_else(|_| Err(Error::Extract("extraction thread panicked".into()))),
            None => Err(Error::Extract("extractor already finished".into())),
        }
    }
}

impl OrderedConsumer for Extractor {
    fn consume(&mut self, data: &[u8]) -> Result<()> {
        let tx = self
            .tx
            .as_ref()
            .ok_or_else(|| Error::Extract("extractor closed".into()))?;
        if tx.send(data.to_vec()).is_err() {
            // The pipeline stopped early: surface its error.
            return Err(match self.join() {
                Err(e) => e,
                Ok(_) => Error::Extract("archive ended before the download did".into()),
            });
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<ConsumerReport> {
        let mut report = self.join()?;
        // Durability: flush the whole filesystem holding the staging dir once,
        // instead of an fsync per file.
        if let Ok(dir) = std::fs::File::open(&self.staging) {
            platform::sync_filesystem(&dir);
        }
        install(&self.staging, &self.dest, self.into_existing)?;
        report.output = Some(self.dest.clone());
        Ok(report)
    }

    fn abort(mut self: Box<Self>) {
        let _ = self.join();
        tracing::warn!(
            "extraction incomplete; partial output left in {}",
            self.staging.display()
        );
    }
}

/// Move the verified staging tree into place.
fn install(staging: &Path, dest: &Path, into_existing: bool) -> Result<()> {
    let exists = dest.exists();
    if exists {
        let empty = std::fs::read_dir(dest)
            .map(|mut d| d.next().is_none())
            .unwrap_or(false);
        if empty {
            std::fs::remove_dir(dest)
                .map_err(|e| Error::io(format!("remove {}", dest.display()), e))?;
        } else if into_existing {
            for entry in std::fs::read_dir(staging).map_err(|e| Error::io("read staging", e))? {
                let entry = entry.map_err(|e| Error::io("read staging", e))?;
                let target = dest.join(entry.file_name());
                if target.exists() {
                    return Err(Error::Extract(format!(
                        "{} already exists; staged copy kept in {}",
                        target.display(),
                        staging.display()
                    )));
                }
                std::fs::rename(entry.path(), &target)
                    .map_err(|e| Error::io(format!("move into {}", target.display()), e))?;
            }
            std::fs::remove_dir(staging).map_err(|e| Error::io("remove staging", e))?;
            return sync_dir(dest);
        } else {
            return Err(Error::Extract(format!(
                "{} appeared during extraction; staged copy kept in {}",
                dest.display(),
                staging.display()
            )));
        }
    }
    std::fs::rename(staging, dest)
        .map_err(|e| Error::io(format!("rename to {}", dest.display()), e))?;
    sync_dir(
        dest.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )
}

fn sync_dir(dir: &Path) -> Result<()> {
    std::fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| Error::io(format!("fsync {}", dir.display()), e))
}

/// `Read` over the chunks sent to the extractor, timing how long it waits.
pub(crate) struct ChannelReader {
    rx: Receiver<Vec<u8>>,
    cur: Vec<u8>,
    pos: usize,
    pub(crate) waited: Arc<AtomicU64>,
}

impl ChannelReader {
    fn new(rx: Receiver<Vec<u8>>) -> Self {
        Self {
            rx,
            cur: Vec::new(),
            pos: 0,
            waited: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.pos >= self.cur.len() {
            let t = Instant::now();
            match self.rx.recv() {
                Ok(v) => {
                    self.cur = v;
                    self.pos = 0;
                }
                Err(_) => return Ok(0),
            }
            self.waited
                .fetch_add(t.elapsed().as_micros() as u64, Ordering::Relaxed);
        }
        let n = buf.len().min(self.cur.len() - self.pos);
        buf[..n].copy_from_slice(&self.cur[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// Wraps the decoder to measure time spent in it (minus input waits).
struct Timed<R> {
    inner: R,
    spent: Duration,
}

impl<R: Read> Read for Timed<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let t = Instant::now();
        let r = self.inner.read(buf);
        self.spent += t.elapsed();
        r
    }
}

fn pipeline(
    rx: Receiver<Vec<u8>>,
    staging: PathBuf,
    opts: ExtractOptions,
) -> Result<ConsumerReport> {
    let reader = ChannelReader::new(rx);
    let waited = reader.waited.clone();
    let mut probe = format::Probe::new(reader);
    let fmt = match opts.format {
        Some(f) => f,
        None => probe.detect()?,
    };
    let (decoder, plan, workers_busy) = decode::build(fmt, probe, opts.threads)?;
    let mut timed = Timed {
        inner: decoder,
        spent: Duration::ZERO,
    };
    let tar_opts = TarOptions {
        root: staging,
        threads: opts.threads,
        allow_special: opts.allow_special,
        max_entries: 50_000_000,
    };
    let rep = extract_tar(&mut timed, &tar_opts)?;
    // Drain anything after the end-of-archive marker so the download can finish.
    let mut sink = [0u8; 64 * 1024];
    while timed
        .read(&mut sink)
        .map_err(|e| Error::Extract(e.to_string()))?
        > 0
    {}
    let input_wait = Duration::from_micros(waited.load(Ordering::Relaxed));
    let serial_busy = timed.spent.saturating_sub(input_wait);
    let decode_busy = match &workers_busy {
        Some(b) => Duration::from_micros(b.load(Ordering::Relaxed)),
        None => serial_busy,
    };
    let mut notes = vec![format!("format: {}", fmt.name()), format!("decode: {plan}")];
    notes.extend(rep.notes.clone());
    Ok(ConsumerReport {
        entries: rep.entries,
        bytes_out: rep.bytes,
        output: None,
        notes,
        decode_busy_ms: decode_busy.as_millis() as u64,
    })
}

#[cfg(test)]
mod tests;
