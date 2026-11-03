//! In-order streaming output: a bounded reorder buffer in front of an
//! [`OrderedConsumer`] (stdout, extraction). Nothing is stored on disk.

use bytes::Bytes;
use qget_core::{Error, ExpectedHash, HashAlgo, Hasher, OrderedConsumer, Result, Sink, SinkReport};
use std::collections::BTreeMap;
use std::io::Write;
use std::time::{Duration, Instant};

pub struct StreamSink {
    size: u64,
    head: u64,
    pending: BTreeMap<u64, Bytes>,
    buffered: u64,
    pub max_buffered: u64,
    hasher: Option<Hasher>,
    expected: Option<ExpectedHash>,
    consumer: Option<Box<dyn OrderedConsumer>>,
    pub busy: Duration,
}

impl StreamSink {
    pub fn new(
        size: u64,
        consumer: Box<dyn OrderedConsumer>,
        hash_algo: Option<HashAlgo>,
        expected: Option<ExpectedHash>,
    ) -> Self {
        let algo = expected.as_ref().map(|e| e.algo).or(hash_algo);
        Self {
            size,
            head: 0,
            pending: BTreeMap::new(),
            buffered: 0,
            max_buffered: 0,
            hasher: algo.map(Hasher::new),
            expected,
            consumer: Some(consumer),
            busy: Duration::ZERO,
        }
    }

    fn drain(&mut self) -> Result<()> {
        while let Some(entry) = self.pending.first_entry() {
            let off = *entry.key();
            if off > self.head {
                break;
            }
            let data = entry.remove();
            self.buffered -= data.len() as u64;
            let end = off + data.len() as u64;
            if end <= self.head {
                continue; // duplicate of bytes already delivered
            }
            let fresh = data.slice((self.head - off) as usize..);
            if let Some(h) = self.hasher.as_mut() {
                h.update(&fresh);
            }
            let consumer = self
                .consumer
                .as_mut()
                .expect("consumer present until finish");
            consumer.consume(&fresh)?;
            self.head = end;
        }
        Ok(())
    }
}

impl Sink for StreamSink {
    fn write(&mut self, offset: u64, data: Bytes) -> Result<()> {
        let started = Instant::now();
        let end = offset + data.len() as u64;
        if end > self.size {
            return Err(Error::Network(format!(
                "data past end ({end} > {})",
                self.size
            )));
        }
        if end > self.head {
            self.buffered += data.len() as u64;
            // Keep the longer chunk if two start at the same offset.
            match self.pending.get(&offset) {
                Some(existing) if existing.len() >= data.len() => {
                    self.buffered -= data.len() as u64;
                }
                Some(existing) => {
                    self.buffered -= existing.len() as u64;
                    self.pending.insert(offset, data);
                }
                None => {
                    self.pending.insert(offset, data);
                }
            }
            self.max_buffered = self.max_buffered.max(self.buffered);
            self.drain()?;
        }
        self.busy += started.elapsed();
        Ok(())
    }

    fn consumed(&self) -> Option<u64> {
        Some(self.head)
    }

    fn finish(&mut self) -> Result<SinkReport> {
        self.drain()?;
        if self.head != self.size {
            return Err(Error::Network(format!(
                "stream ended at {} of {} bytes",
                self.head, self.size
            )));
        }
        let hash = self.hasher.take().map(Hasher::finalize);
        let consumer = self.consumer.take().expect("finish called once");
        if let (Some(actual), Some(expected)) = (&hash, &self.expected)
            && actual != expected
        {
            consumer.abort();
            return Err(Error::Integrity {
                expected: expected.to_string(),
                actual: actual.to_string(),
            });
        }
        let report = consumer.finish()?;
        Ok(SinkReport {
            hash,
            output: report.output.clone(),
            consumer: Some(report),
        })
    }

    fn abort(&mut self) -> Result<()> {
        if let Some(c) = self.consumer.take() {
            c.abort();
        }
        Ok(())
    }
}

/// Ordered consumer writing to any `Write` (e.g. stdout for `-O -`).
pub struct WriteConsumer<W: Write + Send> {
    out: W,
    bytes: u64,
}

impl<W: Write + Send> WriteConsumer<W> {
    pub fn new(out: W) -> Self {
        Self { out, bytes: 0 }
    }
}

impl<W: Write + Send> OrderedConsumer for WriteConsumer<W> {
    fn consume(&mut self, data: &[u8]) -> Result<()> {
        self.out
            .write_all(data)
            .map_err(|e| Error::io("write output stream", e))?;
        self.bytes += data.len() as u64;
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<qget_core::ConsumerReport> {
        self.out.flush().map_err(|e| Error::io("flush output", e))?;
        Ok(qget_core::ConsumerReport {
            bytes_out: self.bytes,
            ..Default::default()
        })
    }

    fn abort(self: Box<Self>) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct Collect(Arc<Mutex<Vec<u8>>>);
    impl OrderedConsumer for Collect {
        fn consume(&mut self, d: &[u8]) -> Result<()> {
            self.0.lock().unwrap().extend_from_slice(d);
            Ok(())
        }
        fn finish(self: Box<Self>) -> Result<qget_core::ConsumerReport> {
            Ok(Default::default())
        }
        fn abort(self: Box<Self>) {}
    }

    #[test]
    fn reorders_and_dedups() {
        let out = Arc::new(Mutex::new(Vec::new()));
        let data: Vec<u8> = (0..100u8).collect();
        let mut s = StreamSink::new(
            100,
            Box::new(Collect(out.clone())),
            Some(HashAlgo::Sha256),
            None,
        );
        s.write(50, Bytes::copy_from_slice(&data[50..])).unwrap();
        assert_eq!(s.consumed(), Some(0));
        s.write(20, Bytes::copy_from_slice(&data[20..60])).unwrap(); // overlaps
        s.write(0, Bytes::copy_from_slice(&data[..30])).unwrap();
        assert_eq!(s.consumed(), Some(100));
        s.write(10, Bytes::copy_from_slice(&data[10..20])).unwrap(); // stale duplicate
        s.finish().unwrap();
        assert_eq!(*out.lock().unwrap(), data);
    }

    #[test]
    fn hash_mismatch_aborts_consumer() {
        let out = Arc::new(Mutex::new(Vec::new()));
        let mut s = StreamSink::new(
            3,
            Box::new(Collect(out)),
            None,
            ExpectedHash::parse(&"1".repeat(64)),
        );
        s.write(0, Bytes::from_static(b"abc")).unwrap();
        assert!(matches!(s.finish(), Err(Error::Integrity { .. })));
    }
}
