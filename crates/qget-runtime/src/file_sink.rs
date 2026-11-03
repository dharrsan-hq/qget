//! Crash-safe file output with an append-only resume journal (§7.2).
//!
//! Write ordering (DUR-03): `pwrite` data → `fdatasync(data)` →
//! append journal record → `fdatasync(journal)`. A range is only ever
//! recorded after its bytes are durable, so a crash at any instant leaves a
//! journal that describes a subset of what is really on disk.

use crate::fsutil;
use bytes::Bytes;
use qget_core::journal::{self, Header, Record};
use qget_core::{
    Error, ExpectedHash, HashAlgo, Hasher, IntervalSet, OrderedConsumer, Result, Sink, SinkReport,
};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub struct FileSinkConfig {
    pub final_path: PathBuf,
    pub header: Header,
    /// Hash to compute while downloading (verification and reporting).
    pub hash_algo: Option<HashAlgo>,
    /// Commit (fsync + journal) after this many bytes...
    pub commit_bytes: u64,
    /// ...or this much time, whichever comes first.
    pub commit_interval: Duration,
    /// Optional ordered consumer fed from disk behind the write frontier
    /// (`--keep-archive` extraction).
    pub tap: Option<Box<dyn OrderedConsumer>>,
    /// Discard any previous partial download.
    pub restart: bool,
}

impl FileSinkConfig {
    pub fn new(final_path: PathBuf, header: Header) -> Self {
        Self {
            final_path,
            header,
            hash_algo: None,
            commit_bytes: 256 << 20,
            commit_interval: Duration::from_secs(2),
            tap: None,
            restart: false,
        }
    }
}

/// What happened to a previous partial download.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResumeDecision {
    Fresh,
    Resumed {
        bytes: u64,
    },
    /// The previous partial download could not be validated and was discarded.
    Restarted {
        reason: String,
    },
}

pub struct FileSink {
    final_path: PathBuf,
    part_path: PathBuf,
    journal_path: PathBuf,
    file: File,
    journal: File,
    header: Header,
    size: u64,
    /// Everything written (durable or not).
    written: IntervalSet,
    /// Written since the last commit.
    pending: IntervalSet,
    pending_bytes: u64,
    last_commit: Instant,
    commit_bytes: u64,
    commit_interval: Duration,
    journal_len: u64,
    hasher: Option<Hasher>,
    tap: Option<Box<dyn OrderedConsumer>>,
    /// Bytes before this offset have been fed to the hasher and tap.
    ordered_upto: u64,
    finished: bool,
    pub busy: Duration,
}

pub fn part_path(final_path: &Path) -> PathBuf {
    with_suffix(final_path, ".qget-part")
}

pub fn journal_path(final_path: &Path) -> PathBuf {
    with_suffix(final_path, ".qget-journal")
}

fn with_suffix(p: &Path, suffix: &str) -> PathBuf {
    let mut s = p.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

impl FileSink {
    /// Open (or resume) the output. Validates any previous partial download
    /// against `cfg.header` (DUR-05).
    pub fn open(cfg: FileSinkConfig) -> Result<(Self, ResumeDecision, IntervalSet)> {
        let final_path = cfg.final_path;
        let part = part_path(&final_path);
        let jpath = journal_path(&final_path);
        let size = cfg.header.size;

        let mut journal = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&jpath)
            .map_err(|e| Error::io(format!("open {}", jpath.display()), e))?;
        if !fsutil::try_lock(&journal) {
            return Err(Error::Usage(format!(
                "another qget is already downloading to {}",
                final_path.display()
            )));
        }

        let previous = std::fs::read(&jpath).map_err(|e| Error::io("read journal", e))?;
        let decoded = journal::decode(&previous);
        let part_exists = part.exists();
        let (decision, coverage) = match decoded.header.as_ref() {
            _ if cfg.restart => (
                if decoded.header.is_some() {
                    ResumeDecision::Restarted {
                        reason: "--restart requested".into(),
                    }
                } else {
                    ResumeDecision::Fresh
                },
                IntervalSet::new(),
            ),
            None => (ResumeDecision::Fresh, IntervalSet::new()),
            Some(_) if !part_exists => (
                ResumeDecision::Restarted {
                    reason: "partial file is missing".into(),
                },
                IntervalSet::new(),
            ),
            Some(prev) => match same_resource(prev, &cfg.header) {
                Some(true) => (
                    ResumeDecision::Resumed {
                        bytes: decoded.coverage.covered(),
                    },
                    decoded.coverage.clone(),
                ),
                Some(false) => {
                    return Err(Error::RemoteChanged(format!(
                        "{} changed since the partial download started; rerun with --restart to discard {}",
                        cfg.header.url,
                        part.display()
                    )));
                }
                None => (
                    ResumeDecision::Restarted {
                        reason: "server provides no ETag/Last-Modified and no checksum was given, so the partial data cannot be validated".into(),
                    },
                    IntervalSet::new(),
                ),
            },
        };

        let resuming = matches!(decision, ResumeDecision::Resumed { .. });
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(!resuming)
            .open(&part)
            .map_err(|e| Error::io(format!("open {}", part.display()), e))?;

        // Disk-space pre-check, then reserve the space (DUR-07, DUR-08).
        let needed = size.saturating_sub(fsutil::allocated(&file));
        if let Some(avail) = fsutil::available_space(&part) {
            if avail < needed.saturating_add(16 << 20) && needed > 0 {
                return Err(Error::DiskFull {
                    needed,
                    available: avail,
                });
            }
        }
        fsutil::preallocate(&file, size)?;
        if file.metadata().map(|m| m.len()).unwrap_or(0) != size {
            file.set_len(size)
                .map_err(|e| Error::io("size output file", e))?;
        }

        // Rewrite the journal from scratch (fresh/restart), or drop a torn tail.
        let journal_len = if resuming {
            if decoded.valid_len != previous.len() as u64 {
                journal
                    .set_len(decoded.valid_len)
                    .map_err(|e| Error::io("truncate torn journal", e))?;
                journal
                    .sync_data()
                    .map_err(|e| Error::io("sync journal", e))?;
            }
            decoded.valid_len
        } else {
            let bytes = journal::encode_file(&cfg.header, &IntervalSet::new());
            journal
                .set_len(0)
                .map_err(|e| Error::io("reset journal", e))?;
            journal
                .write_all_at(&bytes, 0)
                .and_then(|_| journal.sync_data())
                .map_err(|e| Error::io("write journal", e))?;
            file.sync_all().map_err(|e| Error::io("sync output", e))?;
            fsutil::sync_parent(&part)?;
            bytes.len() as u64
        };

        let sink = Self {
            final_path,
            part_path: part,
            journal_path: jpath,
            file,
            journal: {
                use std::io::Seek;
                journal
                    .seek(std::io::SeekFrom::Start(journal_len))
                    .map_err(|e| Error::io("seek journal", e))?;
                journal
            },
            header: cfg.header,
            size,
            written: coverage.clone(),
            pending: IntervalSet::new(),
            pending_bytes: 0,
            last_commit: Instant::now(),
            commit_bytes: cfg.commit_bytes,
            commit_interval: cfg.commit_interval,
            journal_len,
            hasher: cfg.hash_algo.map(Hasher::new),
            tap: cfg.tap,
            ordered_upto: 0,
            finished: false,
            busy: Duration::ZERO,
        };
        Ok((sink, decision, coverage))
    }

    pub fn part_path(&self) -> &Path {
        &self.part_path
    }

    /// Make pending writes durable, then record them (DUR-03).
    fn commit(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.file
            .sync_data()
            .map_err(|e| Error::io("fdatasync output", e))?;
        let rec = journal::encode_record(&Record::Ranges(self.pending.spans().to_vec()));
        self.journal
            .write_all(&rec)
            .and_then(|_| self.journal.sync_data())
            .map_err(|e| Error::io("append journal", e))?;
        self.journal_len += rec.len() as u64;
        self.pending = IntervalSet::new();
        self.pending_bytes = 0;
        self.last_commit = Instant::now();
        if self.journal_len > 4 << 20 {
            self.compact()?;
        }
        Ok(())
    }

    /// Replace the journal with a single snapshot record.
    fn compact(&mut self) -> Result<()> {
        let durable = self.written.clone();
        let bytes = journal::encode_file(&self.header, &durable);
        // Hold the lock on the old inode until the new one is in place.
        fsutil::atomic_write(&self.journal_path, &bytes)?;
        let mut j = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.journal_path)
            .map_err(|e| Error::io("reopen journal", e))?;
        fsutil::try_lock(&j);
        use std::io::Seek;
        j.seek(std::io::SeekFrom::End(0))
            .map_err(|e| Error::io("seek journal", e))?;
        self.journal = j;
        self.journal_len = bytes.len() as u64;
        Ok(())
    }

    /// Feed newly contiguous bytes to the hasher and tap, at most `budget`
    /// bytes per call so a resumed multi-GB prefix doesn't stall writes.
    fn advance_ordered(&mut self, mut budget: u64, direct: Option<(u64, &[u8])>) -> Result<()> {
        if self.hasher.is_none() && self.tap.is_none() {
            return Ok(());
        }
        // Fast path: the write that just happened starts exactly at the frontier.
        if let Some((off, data)) = direct {
            if off == self.ordered_upto {
                self.feed(data)?;
                self.ordered_upto += data.len() as u64;
            }
        }
        let frontier = self.written.contiguous_end(self.ordered_upto);
        let mut buf = Vec::new();
        while self.ordered_upto < frontier && budget > 0 {
            let n = (frontier - self.ordered_upto).min(8 << 20).min(budget) as usize;
            buf.resize(n, 0);
            self.file
                .read_exact_at(&mut buf, self.ordered_upto)
                .map_err(|e| Error::io("read back for hashing", e))?;
            self.feed(&buf)?;
            self.ordered_upto += n as u64;
            budget -= n as u64;
        }
        Ok(())
    }

    fn feed(&mut self, data: &[u8]) -> Result<()> {
        if let Some(h) = self.hasher.as_mut() {
            h.update(data);
        }
        if let Some(t) = self.tap.as_mut() {
            t.consume(data)?;
        }
        Ok(())
    }
}

/// Whether the journal header and the current remote describe the same bytes.
fn same_resource(prev: &Header, now: &Header) -> Option<bool> {
    if prev.size != now.size {
        return Some(false);
    }
    if let (Some(a), Some(b)) = (&prev.expected, &now.expected) {
        if a.algo == b.algo {
            return Some(a == b);
        }
    }
    prev.validator.same_as(&now.validator)
}

impl Sink for FileSink {
    fn write(&mut self, offset: u64, data: Bytes) -> Result<()> {
        let started = Instant::now();
        let end = offset + data.len() as u64;
        if end > self.size {
            return Err(Error::Network(format!(
                "write past end of file ({end} > {})",
                self.size
            )));
        }
        self.file
            .write_all_at(&data, offset)
            .map_err(|e| Error::io(format!("write {}", self.part_path.display()), e))?;
        self.written.insert(offset, end);
        self.pending.insert(offset, end);
        self.pending_bytes += data.len() as u64;
        self.advance_ordered(64 << 20, Some((offset, &data)))?;
        if self.pending_bytes >= self.commit_bytes
            || self.last_commit.elapsed() >= self.commit_interval
        {
            self.commit()?;
        }
        self.busy += started.elapsed();
        Ok(())
    }

    fn consumed(&self) -> Option<u64> {
        // With a tap, the consumer's position bounds nothing: the file holds
        // out-of-order data on disk, not in memory.
        None
    }

    fn finish(&mut self) -> Result<SinkReport> {
        if !self.written.contains(0, self.size) {
            return Err(Error::Network("download ended with missing bytes".into()));
        }
        self.commit()?;
        self.advance_ordered(u64::MAX, None)?;
        let hash = self.hasher.take().map(Hasher::finalize);
        let consumer = match (&hash, &self.header.expected) {
            (Some(actual), Some(expected))
                if actual.algo == expected.algo && actual != expected =>
            {
                if let Some(t) = self.tap.take() {
                    t.abort();
                }
                let report = self.part_path.with_extension("qget-mismatch.txt");
                let _ = std::fs::write(
                    &report,
                    format!(
                        "qget integrity failure\nurl: {}\nexpected: {expected}\nactual:   {actual}\npartial data kept in {}\n",
                        self.header.url,
                        self.part_path.display()
                    ),
                );
                // A rerun must not resume from data known to be wrong.
                let _ = std::fs::remove_file(&self.journal_path);
                return Err(Error::Integrity {
                    expected: expected.to_string(),
                    actual: actual.to_string(),
                });
            }
            _ => match self.tap.take() {
                Some(t) => Some(t.finish()?),
                None => None,
            },
        };
        self.file
            .sync_all()
            .map_err(|e| Error::io("fsync output", e))?;
        std::fs::rename(&self.part_path, &self.final_path)
            .map_err(|e| Error::io(format!("rename to {}", self.final_path.display()), e))?;
        fsutil::sync_parent(&self.final_path)?;
        let _ = std::fs::remove_file(&self.journal_path);
        fsutil::sync_parent(&self.journal_path)?;
        self.finished = true;
        Ok(SinkReport {
            hash,
            output: Some(self.final_path.clone()),
            consumer,
        })
    }

    fn abort(&mut self) -> Result<()> {
        if let Some(t) = self.tap.take() {
            t.abort();
        }
        if !self.finished {
            self.commit()?;
        }
        Ok(())
    }
}

/// Verify a finished file by re-reading it from disk (INT-06).
pub fn hash_file(path: &Path, algo: HashAlgo) -> Result<ExpectedHash> {
    let f = File::open(path).map_err(|e| Error::io(format!("open {}", path.display()), e))?;
    let mut h = Hasher::new(algo);
    let mut buf = vec![0u8; 8 << 20];
    let mut off = 0u64;
    loop {
        let n = f
            .read_at(&mut buf, off)
            .map_err(|e| Error::io(format!("read {}", path.display()), e))?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
        off += n as u64;
    }
    Ok(h.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use qget_core::Validator;

    fn header(size: u64, etag: &str) -> Header {
        Header {
            size,
            validator: Validator {
                etag: Some(etag.into()),
                last_modified: None,
            },
            url: "http://x/f".into(),
            expected: None,
        }
    }

    #[test]
    fn writes_out_of_order_and_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.bin");
        let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let mut cfg = FileSinkConfig::new(path.clone(), header(data.len() as u64, "\"a\""));
        cfg.hash_algo = Some(HashAlgo::Sha256);
        let (mut sink, decision, cov) = FileSink::open(cfg).unwrap();
        assert_eq!(decision, ResumeDecision::Fresh);
        assert!(cov.is_empty());
        sink.write(50_000, Bytes::copy_from_slice(&data[50_000..]))
            .unwrap();
        sink.write(0, Bytes::copy_from_slice(&data[..50_000]))
            .unwrap();
        let rep = sink.finish().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), data);
        let mut h = Hasher::new(HashAlgo::Sha256);
        h.update(&data);
        assert_eq!(rep.hash, Some(h.finalize()));
        assert!(!journal_path(&path).exists());
        assert!(!part_path(&path).exists());
    }

    #[test]
    fn resumes_only_committed_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.bin");
        let size = 1000u64;
        {
            let mut cfg = FileSinkConfig::new(path.clone(), header(size, "\"a\""));
            cfg.commit_bytes = 100;
            let (mut sink, _, _) = FileSink::open(cfg).unwrap();
            sink.write(0, Bytes::from(vec![1u8; 300])).unwrap(); // committed
            let mut cfg2 = FileSinkConfig::new(path.clone(), header(size, "\"a\""));
            cfg2.commit_bytes = 100;
            // Lock is held: a second writer must be refused.
            assert!(matches!(FileSink::open(cfg2), Err(Error::Usage(_))));
            sink.commit_bytes = u64::MAX;
            sink.commit_interval = Duration::MAX;
            sink.write(500, Bytes::from(vec![2u8; 100])).unwrap(); // not committed
            // Simulate a crash: drop without abort().
        }
        let cfg = FileSinkConfig::new(path.clone(), header(size, "\"a\""));
        let (_sink, decision, cov) = FileSink::open(cfg).unwrap();
        assert_eq!(decision, ResumeDecision::Resumed { bytes: 300 });
        assert_eq!(cov.spans(), &[(0, 300)]);
    }

    #[test]
    fn changed_remote_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.bin");
        {
            let (mut sink, _, _) =
                FileSink::open(FileSinkConfig::new(path.clone(), header(100, "\"a\""))).unwrap();
            sink.write(0, Bytes::from(vec![0u8; 10])).unwrap();
            sink.abort().unwrap();
        }
        let err = FileSink::open(FileSinkConfig::new(path.clone(), header(100, "\"b\"")))
            .err()
            .expect("must refuse");
        assert!(matches!(err, Error::RemoteChanged(_)));
        let mut cfg = FileSinkConfig::new(path.clone(), header(100, "\"b\""));
        cfg.restart = true;
        let (_s, decision, cov) = FileSink::open(cfg).unwrap();
        assert!(matches!(decision, ResumeDecision::Restarted { .. }));
        assert!(cov.is_empty());
    }

    #[test]
    fn no_validator_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.bin");
        let mut h = header(100, "x");
        h.validator = Validator::default();
        {
            let (mut sink, _, _) =
                FileSink::open(FileSinkConfig::new(path.clone(), h.clone())).unwrap();
            sink.write(0, Bytes::from(vec![0u8; 10])).unwrap();
            sink.abort().unwrap();
        }
        let (_s, decision, cov) = FileSink::open(FileSinkConfig::new(path.clone(), h)).unwrap();
        assert!(matches!(decision, ResumeDecision::Restarted { .. }));
        assert!(cov.is_empty());
    }

    #[test]
    fn integrity_mismatch_keeps_part_and_reports() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.bin");
        let mut h = header(4, "\"a\"");
        h.expected = ExpectedHash::parse(&"0".repeat(64));
        let mut cfg = FileSinkConfig::new(path.clone(), h);
        cfg.hash_algo = Some(HashAlgo::Sha256);
        let (mut sink, _, _) = FileSink::open(cfg).unwrap();
        sink.write(0, Bytes::from_static(b"abcd")).unwrap();
        assert!(matches!(sink.finish(), Err(Error::Integrity { .. })));
        assert!(!path.exists());
        assert!(part_path(&path).exists());
    }

    #[test]
    fn torn_journal_tail_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.bin");
        {
            let mut cfg = FileSinkConfig::new(path.clone(), header(1000, "\"a\""));
            cfg.commit_bytes = 1;
            let (mut sink, _, _) = FileSink::open(cfg).unwrap();
            sink.write(0, Bytes::from(vec![7u8; 100])).unwrap();
        }
        // Append garbage as if a record write was torn by a crash.
        let jp = journal_path(&path);
        let mut j = OpenOptions::new().append(true).open(&jp).unwrap();
        j.write_all(&[0x10, 0, 0, 0, 1, 2, 3]).unwrap();
        drop(j);
        let mut cfg = FileSinkConfig::new(path.clone(), header(1000, "\"a\""));
        cfg.commit_bytes = 1;
        let (mut sink, decision, _) = FileSink::open(cfg).unwrap();
        assert_eq!(decision, ResumeDecision::Resumed { bytes: 100 });
        sink.write(100, Bytes::from(vec![8u8; 900])).unwrap();
        // The new record must be readable after the truncated tail.
        let d = journal::decode(&std::fs::read(&jp).unwrap());
        assert_eq!(d.coverage.spans(), &[(0, 1000)]);
    }
}
