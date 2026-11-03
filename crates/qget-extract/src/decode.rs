//! Decoders. Parallel plans split the compressed stream into independent
//! units (LZ4 blocks, zstd frames) on a parser thread, decode them on a
//! worker pool, and reassemble the output in order behind a `Read`.

use crate::format::{Format, Probe};
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use qget_core::{Error, Result};
use std::collections::BTreeMap;
use std::io::{self, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use xxhash_rust::xxh32::{Xxh32, xxh32};

const LZ4_MAGIC: u32 = 0x184D_2204;
const ZSTD_MAGIC: u32 = 0xFD2F_B528;
/// A zstd frame larger than this is decoded serially instead of buffered.
const ZSTD_PARALLEL_FRAME_LIMIT: usize = 64 << 20;

type BoxRead = Box<dyn Read + Send>;

/// Build the decoder for `fmt`. Returns the reader, a description of the
/// plan, and (for parallel plans) the workers' cumulative busy time.
pub fn build<R: Read + Send + 'static>(
    fmt: Format,
    mut probe: Probe<R>,
    threads: usize,
) -> Result<(BoxRead, String, Option<Arc<AtomicU64>>)> {
    let threads = threads.max(1);
    Ok(match fmt {
        Format::Tar => (Box::new(probe), "none (plain tar)".into(), None),
        Format::TarGzip => (
            Box::new(flate2::read::MultiGzDecoder::new(probe)),
            "serial (gzip cannot be split)".into(),
            None,
        ),
        Format::TarXz => (
            Box::new(liblzma::read::XzDecoder::new_multi_decoder(probe)),
            "serial (xz)".into(),
            None,
        ),
        Format::TarLz4 => {
            let head = probe.fill(8).map_err(|e| Error::Extract(e.to_string()))?;
            let independent = first_lz4_frame_independent(head);
            if independent {
                let busy = Arc::new(AtomicU64::new(0));
                let r = OrderedDecoder::spawn(Box::new(probe), threads, busy.clone(), parse_lz4);
                (
                    Box::new(r),
                    format!("parallel LZ4 blocks on {threads} threads"),
                    Some(busy),
                )
            } else {
                (
                    Box::new(lz4_flex::frame::FrameDecoder::new(probe)),
                    "serial (LZ4 linked blocks)".into(),
                    None,
                )
            }
        }
        Format::TarZstd => {
            let busy = Arc::new(AtomicU64::new(0));
            let r = OrderedDecoder::spawn(Box::new(probe), threads, busy.clone(), parse_zstd);
            (
                Box::new(r),
                format!(
                    "parallel zstd frames on {threads} threads (serial for single large frames)"
                ),
                Some(busy),
            )
        }
    })
}

fn first_lz4_frame_independent(head: &[u8]) -> bool {
    let mut off = 0;
    while head.len() >= off + 8 {
        let m = u32::from_le_bytes(head[off..off + 4].try_into().unwrap());
        if m & 0xFFFF_FFF0 == 0x184D_2A50 {
            off += 8 + u32::from_le_bytes(head[off + 4..off + 8].try_into().unwrap()) as usize;
            continue;
        }
        return m == LZ4_MAGIC && head.len() > off + 4 && head[off + 4] & 0x20 != 0;
    }
    false
}

/// Units of work produced by a parser, in stream order.
pub(crate) enum Job {
    Lz4Block {
        data: Vec<u8>,
        uncompressed: bool,
        max: usize,
        checksum: Option<u32>,
    },
    ZstdFrame(Vec<u8>),
    /// Already-decoded output (serial fallback inside a parallel plan).
    Plain(Vec<u8>),
    /// A new LZ4 frame starts; track its content checksum if present.
    FrameStart {
        content_checksum: bool,
    },
    FrameEnd {
        checksum: Option<u32>,
    },
}

enum Out {
    Data(Vec<u8>),
    FrameStart(bool),
    FrameEnd(Option<u32>),
}

enum Msg {
    Block(u64, std::result::Result<Out, String>),
    End(u64, std::result::Result<(), String>),
}

/// The parser side: sends jobs in order via `emit`.
pub(crate) struct Emitter {
    jobs: Sender<(u64, Job)>,
    tickets: Sender<()>,
    seq: u64,
}

impl Emitter {
    fn emit(&mut self, job: Job) -> std::result::Result<(), String> {
        // Bounded tickets cap the number of decoded-but-unconsumed units.
        self.tickets
            .send(())
            .map_err(|_| "consumer stopped".to_string())?;
        self.jobs
            .send((self.seq, job))
            .map_err(|_| "workers stopped".to_string())?;
        self.seq += 1;
        Ok(())
    }
}

type Parser = fn(BoxRead, &mut Emitter) -> std::result::Result<(), String>;

pub(crate) struct OrderedDecoder {
    rx: Receiver<Msg>,
    tickets: Receiver<()>,
    pending: BTreeMap<u64, std::result::Result<Out, String>>,
    next: u64,
    end: Option<u64>,
    end_err: Option<String>,
    cur: Vec<u8>,
    pos: usize,
    frame_hash: Option<Xxh32>,
}

impl OrderedDecoder {
    fn spawn(input: BoxRead, threads: usize, busy: Arc<AtomicU64>, parser: Parser) -> Self {
        let (job_tx, job_rx) = bounded::<(u64, Job)>(threads * 2);
        let (out_tx, out_rx) = unbounded::<Msg>();
        let (ticket_tx, ticket_rx) = bounded::<()>(threads * 4);
        for i in 0..threads {
            let (job_rx, out_tx, busy) = (job_rx.clone(), out_tx.clone(), busy.clone());
            std::thread::Builder::new()
                .name(format!("qget-decode-{i}"))
                .spawn(move || {
                    while let Ok((seq, job)) = job_rx.recv() {
                        let t = Instant::now();
                        let out = run_job(job);
                        busy.fetch_add(t.elapsed().as_micros() as u64, Ordering::Relaxed);
                        if out_tx.send(Msg::Block(seq, out)).is_err() {
                            break;
                        }
                    }
                })
                .expect("spawn decode worker");
        }
        std::thread::Builder::new()
            .name("qget-decode-parse".into())
            .spawn(move || {
                let mut em = Emitter {
                    jobs: job_tx,
                    tickets: ticket_tx,
                    seq: 0,
                };
                let r = parser(input, &mut em);
                let _ = out_tx.send(Msg::End(em.seq, r));
            })
            .expect("spawn decode parser");
        Self {
            rx: out_rx,
            tickets: ticket_rx,
            pending: BTreeMap::new(),
            next: 0,
            end: None,
            end_err: None,
            cur: Vec::new(),
            pos: 0,
            frame_hash: None,
        }
    }

    /// Advance to the next unit of output. Returns false at end of stream.
    fn advance(&mut self) -> io::Result<bool> {
        loop {
            if let Some(out) = self.pending.remove(&self.next) {
                self.next += 1;
                let _ = self.tickets.recv();
                match out.map_err(io::Error::other)? {
                    Out::Data(d) => {
                        if let Some(h) = self.frame_hash.as_mut() {
                            h.update(&d);
                        }
                        self.cur = d;
                        self.pos = 0;
                        return Ok(true);
                    }
                    Out::FrameStart(ck) => {
                        self.frame_hash = ck.then(|| Xxh32::new(0));
                    }
                    Out::FrameEnd(expected) => {
                        if let (Some(h), Some(e)) = (self.frame_hash.take(), expected)
                            && h.digest() != e
                        {
                            return Err(io::Error::other("LZ4 content checksum mismatch"));
                        }
                    }
                }
                continue;
            }
            if let Some(end) = self.end
                && self.next >= end
            {
                return match self.end_err.take() {
                    Some(e) => Err(io::Error::other(e)),
                    None => Ok(false),
                };
            }
            match self.rx.recv() {
                Ok(Msg::Block(seq, out)) => {
                    self.pending.insert(seq, out);
                }
                Ok(Msg::End(n, r)) => {
                    self.end = Some(n);
                    self.end_err = r.err();
                }
                Err(_) => return Err(io::Error::other("decoder threads exited")),
            }
        }
    }
}

impl Read for OrderedDecoder {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.pos >= self.cur.len() {
            if !self.advance()? {
                return Ok(0);
            }
        }
        let n = buf.len().min(self.cur.len() - self.pos);
        buf[..n].copy_from_slice(&self.cur[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

fn run_job(job: Job) -> std::result::Result<Out, String> {
    match job {
        Job::Lz4Block {
            data,
            uncompressed,
            max,
            checksum,
        } => {
            if let Some(c) = checksum
                && xxh32(&data, 0) != c
            {
                return Err("LZ4 block checksum mismatch".into());
            }
            if uncompressed {
                return Ok(Out::Data(data));
            }
            let mut out = vec![0u8; max];
            let n = lz4_flex::block::decompress_into(&data, &mut out)
                .map_err(|e| format!("LZ4 block: {e}"))?;
            out.truncate(n);
            Ok(Out::Data(out))
        }
        Job::ZstdFrame(frame) => {
            let mut d =
                zstd::stream::read::Decoder::with_buffer(&frame[..]).map_err(|e| e.to_string())?;
            d.window_log_max(31).map_err(|e| e.to_string())?;
            let mut out = Vec::new();
            d.read_to_end(&mut out)
                .map_err(|e| format!("zstd frame: {e}"))?;
            Ok(Out::Data(out))
        }
        Job::Plain(d) => Ok(Out::Data(d)),
        Job::FrameStart { content_checksum } => Ok(Out::FrameStart(content_checksum)),
        Job::FrameEnd { checksum } => Ok(Out::FrameEnd(checksum)),
    }
}

fn read_exact_or_eof(r: &mut dyn Read, buf: &mut [u8]) -> std::result::Result<bool, String> {
    let mut got = 0;
    while got < buf.len() {
        match r.read(&mut buf[got..]) {
            Ok(0) if got == 0 => return Ok(false),
            Ok(0) => return Err("truncated archive".into()),
            Ok(n) => got += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(true)
}

fn read_exact(r: &mut dyn Read, buf: &mut [u8]) -> std::result::Result<(), String> {
    if read_exact_or_eof(r, buf)? {
        Ok(())
    } else {
        Err("truncated archive".into())
    }
}

fn read_vec(r: &mut dyn Read, n: usize) -> std::result::Result<Vec<u8>, String> {
    let mut v = vec![0u8; n];
    read_exact(r, &mut v)?;
    Ok(v)
}

fn skip(r: &mut dyn Read, mut n: u64) -> std::result::Result<(), String> {
    let mut buf = [0u8; 8192];
    while n > 0 {
        let k = (n as usize).min(buf.len());
        read_exact(r, &mut buf[..k])?;
        n -= k as u64;
    }
    Ok(())
}

/// LZ4 frame format v1.6 parser (independent blocks).
fn parse_lz4(mut input: BoxRead, em: &mut Emitter) -> std::result::Result<(), String> {
    let r: &mut dyn Read = &mut input;
    loop {
        let mut m = [0u8; 4];
        if !read_exact_or_eof(r, &mut m)? {
            return Ok(());
        }
        let magic = u32::from_le_bytes(m);
        if magic & 0xFFFF_FFF0 == 0x184D_2A50 {
            let mut s = [0u8; 4];
            read_exact(r, &mut s)?;
            skip(r, u32::from_le_bytes(s) as u64)?;
            continue;
        }
        if magic != LZ4_MAGIC {
            return Err(format!("not an LZ4 frame (magic {magic:#010x})"));
        }
        let mut fd = [0u8; 2];
        read_exact(r, &mut fd)?;
        let (flg, bd) = (fd[0], fd[1]);
        if flg >> 6 != 1 {
            return Err("unsupported LZ4 frame version".into());
        }
        if flg & 0x20 == 0 {
            return Err("LZ4 frame with linked blocks inside an independent-block stream".into());
        }
        let block_ck = flg & 0x10 != 0;
        let content_size = flg & 0x08 != 0;
        let content_ck = flg & 0x04 != 0;
        let dict = flg & 0x01 != 0;
        let max = match (bd >> 4) & 7 {
            4 => 64 << 10,
            5 => 256 << 10,
            6 => 1 << 20,
            7 => 4 << 20,
            _ => return Err("invalid LZ4 block maximum size".into()),
        };
        let mut desc = fd.to_vec();
        if content_size {
            desc.extend(read_vec(r, 8)?);
        }
        if dict {
            desc.extend(read_vec(r, 4)?);
        }
        let mut hc = [0u8; 1];
        read_exact(r, &mut hc)?;
        if ((xxh32(&desc, 0) >> 8) & 0xFF) as u8 != hc[0] {
            return Err("LZ4 frame header checksum mismatch".into());
        }
        em.emit(Job::FrameStart {
            content_checksum: content_ck,
        })?;
        loop {
            let mut s = [0u8; 4];
            read_exact(r, &mut s)?;
            let raw = u32::from_le_bytes(s);
            if raw == 0 {
                let checksum = if content_ck {
                    let mut c = [0u8; 4];
                    read_exact(r, &mut c)?;
                    Some(u32::from_le_bytes(c))
                } else {
                    None
                };
                em.emit(Job::FrameEnd { checksum })?;
                break;
            }
            let uncompressed = raw & 0x8000_0000 != 0;
            let len = (raw & 0x7FFF_FFFF) as usize;
            if len > max {
                return Err(format!("LZ4 block of {len} bytes exceeds maximum {max}"));
            }
            let data = read_vec(r, len)?;
            let checksum = if block_ck {
                let mut c = [0u8; 4];
                read_exact(r, &mut c)?;
                Some(u32::from_le_bytes(c))
            } else {
                None
            };
            em.emit(Job::Lz4Block {
                data,
                uncompressed,
                max,
                checksum,
            })?;
        }
    }
}

/// zstd frame splitter (RFC 8878 §3.1). Each frame is independent, so whole
/// frames decode in parallel. A frame too large to buffer switches the rest
/// of the stream to a serial decoder.
fn parse_zstd(mut input: BoxRead, em: &mut Emitter) -> std::result::Result<(), String> {
    let r: &mut dyn Read = &mut input;
    loop {
        let mut m = [0u8; 4];
        if !read_exact_or_eof(r, &mut m)? {
            return Ok(());
        }
        let magic = u32::from_le_bytes(m);
        if magic & 0xFFFF_FFF0 == 0x184D_2A50 {
            let mut s = [0u8; 4];
            read_exact(r, &mut s)?;
            skip(r, u32::from_le_bytes(s) as u64)?;
            continue;
        }
        if magic != ZSTD_MAGIC {
            return Err(format!("not a zstd frame (magic {magic:#010x})"));
        }
        let mut frame = m.to_vec();
        let fhd = read_vec(r, 1)?[0];
        frame.push(fhd);
        let fcs_flag = fhd >> 6;
        let single_segment = fhd & 0x20 != 0;
        let checksum = fhd & 0x04 != 0;
        let dict_len = [0usize, 1, 2, 4][(fhd & 3) as usize];
        let fcs_len = match fcs_flag {
            0 => usize::from(single_segment),
            1 => 2,
            2 => 4,
            _ => 8,
        };
        let header_rest = usize::from(!single_segment) + dict_len + fcs_len;
        frame.extend(read_vec(r, header_rest)?);
        loop {
            let hdr = read_vec(r, 3)?;
            frame.extend_from_slice(&hdr);
            let h = u32::from(hdr[0]) | u32::from(hdr[1]) << 8 | u32::from(hdr[2]) << 16;
            let last = h & 1 != 0;
            let size = (h >> 3) as usize;
            let payload = match (h >> 1) & 3 {
                0 | 2 => size,
                1 => 1,
                _ => return Err("reserved zstd block type".into()),
            };
            frame.extend(read_vec(r, payload)?);
            if frame.len() > ZSTD_PARALLEL_FRAME_LIMIT {
                // Hand the buffered frame plus the rest of the stream to one decoder.
                let rest = io::Cursor::new(std::mem::take(&mut frame)).chain(r);
                let mut d = zstd::stream::read::Decoder::new(rest).map_err(|e| e.to_string())?;
                d.window_log_max(31).map_err(|e| e.to_string())?;
                loop {
                    let mut buf = vec![0u8; 4 << 20];
                    let mut filled = 0;
                    while filled < buf.len() {
                        match d.read(&mut buf[filled..]) {
                            Ok(0) => break,
                            Ok(n) => filled += n,
                            Err(e) => return Err(format!("zstd: {e}")),
                        }
                    }
                    if filled == 0 {
                        return Ok(());
                    }
                    buf.truncate(filled);
                    em.emit(Job::Plain(buf))?;
                }
            }
            if last {
                break;
            }
        }
        if checksum {
            frame.extend(read_vec(r, 4)?);
        }
        em.emit(Job::ZstdFrame(frame))?;
    }
}
