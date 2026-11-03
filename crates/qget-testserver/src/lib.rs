//! A small HTTP/1.1 server for tests and benchmarks.
//!
//! It serves one resource with byte-range support and can inject faults that
//! real servers produce: short bodies, wrong `Content-Range`, ignored ranges,
//! connection resets, stalls, throttling (429/503) and content that changes
//! mid-download. Faults are triggered deterministically by request number.

use base64::Engine as _;
use sha2::Digest;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

#[derive(Clone, Debug)]
pub enum Content {
    Memory(Arc<Vec<u8>>),
    /// Serve a file from disk (benchmarks with multi-GB inputs).
    File {
        path: PathBuf,
        size: u64,
    },
}

impl Content {
    pub fn len(&self) -> u64 {
        match self {
            Content::Memory(v) => v.len() as u64,
            Content::File { size, .. } => *size,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn read(&self, offset: u64, len: usize, version: u64) -> std::io::Result<Vec<u8>> {
        let mut out = match self {
            Content::Memory(v) => v[offset as usize..offset as usize + len].to_vec(),
            Content::File { path, .. } => {
                use std::os::unix::fs::FileExt;
                let f = std::fs::File::open(path)?;
                let mut buf = vec![0; len];
                f.read_exact_at(&mut buf, offset)?;
                buf
            }
        };
        if version > 0 {
            // A changed resource: same size, different bytes.
            for b in &mut out {
                *b ^= 0x5A;
            }
        }
        Ok(out)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fault {
    /// Send only half of the requested body, then close.
    ShortBody,
    /// Answer with a `Content-Range` that does not match the request.
    WrongContentRange,
    /// Ignore `Range` and answer 200 with the full body.
    IgnoreRange,
    /// Close the connection after this many body bytes.
    ResetAfter(u64),
    /// Answer with this status (and optional Retry-After seconds).
    Status(u16, Option<u64>),
    /// Send headers, then stall for this many milliseconds before the body.
    Stall(u64),
    /// From now on serve a different version (new ETag, different bytes).
    ChangeContent,
}

/// Apply `fault` to GET requests number `first`, `first + every`, … (1-based),
/// at most `count` times.
#[derive(Clone, Debug)]
pub struct FaultRule {
    pub fault: Fault,
    pub first: u64,
    pub every: u64,
    pub count: u64,
}

impl FaultRule {
    pub fn once(at: u64, fault: Fault) -> Self {
        Self {
            fault,
            first: at,
            every: 1,
            count: 1,
        }
    }

    pub fn every(first: u64, every: u64, count: u64, fault: Fault) -> Self {
        Self {
            fault,
            first,
            every,
            count,
        }
    }

    fn applies(&self, n: u64) -> bool {
        n >= self.first
            && (n - self.first) % self.every.max(1) == 0
            && (n - self.first) / self.every.max(1) < self.count
    }
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub content: Content,
    /// URL path of the resource, e.g. `/file.bin`.
    pub path: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub support_ranges: bool,
    /// Answer HEAD with 405 (like some presigned URLs).
    pub reject_head: bool,
    /// Per-connection body rate limit in bytes/second.
    pub per_conn_rate: Option<u64>,
    /// Delay before each response.
    pub latency_ms: u64,
    /// Answer 429 when more than this many requests are in flight.
    pub max_concurrent: Option<u32>,
    pub faults: Vec<FaultRule>,
    /// Serve `<path>.sha256` with the content's digest.
    pub serve_sums: bool,
    /// Send `Repr-Digest` on HEAD responses.
    pub repr_digest: bool,
    /// Redirect `/redirect<path>` to `<path>`.
    pub redirect: bool,
}

impl ServerConfig {
    pub fn new(content: Content) -> Self {
        Self {
            content,
            path: "/file.bin".into(),
            etag: Some("\"v1\"".into()),
            last_modified: Some("Wed, 01 Jan 2025 00:00:00 GMT".into()),
            support_ranges: true,
            reject_head: false,
            per_conn_rate: None,
            latency_ms: 0,
            max_concurrent: None,
            faults: Vec::new(),
            serve_sums: false,
            repr_digest: false,
            redirect: false,
        }
    }

    pub fn memory(data: Vec<u8>) -> Self {
        Self::new(Content::Memory(Arc::new(data)))
    }
}

#[derive(Debug, Default)]
pub struct Stats {
    pub gets: AtomicU64,
    pub heads: AtomicU64,
    pub body_bytes: AtomicU64,
    pub in_flight: AtomicU64,
    pub max_in_flight: AtomicU64,
    pub connections: AtomicU64,
    pub throttled: AtomicU64,
}

struct Shared {
    cfg: ServerConfig,
    stats: Arc<Stats>,
    version: AtomicU64,
    digest_hex: Option<String>,
    digest_b64: Option<String>,
}

pub struct TestServer {
    pub addr: SocketAddr,
    pub stats: Arc<Stats>,
    stop: Arc<AtomicBool>,
    shared: Arc<Shared>,
}

impl TestServer {
    pub async fn start(cfg: ServerConfig) -> std::io::Result<Self> {
        Self::bind("127.0.0.1:0", cfg).await
    }

    pub async fn bind(addr: &str, cfg: ServerConfig) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let (digest_hex, digest_b64) = if cfg.serve_sums || cfg.repr_digest {
            let mut h = sha2::Sha256::new();
            let len = cfg.content.len();
            let mut off = 0;
            while off < len {
                let n = (len - off).min(8 << 20) as usize;
                h.update(cfg.content.read(off, n, 0)?);
                off += n as u64;
            }
            let d = h.finalize();
            (
                Some(hex::encode(d)),
                Some(base64::engine::general_purpose::STANDARD.encode(d)),
            )
        } else {
            (None, None)
        };
        let stats = Arc::new(Stats::default());
        let shared = Arc::new(Shared {
            cfg,
            stats: stats.clone(),
            version: AtomicU64::new(0),
            digest_hex,
            digest_b64,
        });
        let stop = Arc::new(AtomicBool::new(false));
        let (s2, stop2) = (shared.clone(), stop.clone());
        let limit = Arc::new(Semaphore::new(4096));
        tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    break;
                };
                if stop2.load(Ordering::Relaxed) {
                    break;
                }
                let _ = sock.set_nodelay(true);
                s2.stats.connections.fetch_add(1, Ordering::Relaxed);
                let s3 = s2.clone();
                let Ok(permit) = limit.clone().acquire_owned().await else {
                    break;
                };
                tokio::spawn(async move {
                    let _ = handle_conn(sock, s3).await;
                    drop(permit);
                });
            }
        });
        Ok(Self {
            addr,
            stats,
            stop,
            shared,
        })
    }

    pub fn url(&self) -> String {
        format!("http://{}{}", self.addr, self.shared.cfg.path)
    }

    pub fn base(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Current served content (after any `ChangeContent`).
    pub fn current_bytes(&self) -> Vec<u8> {
        let v = self.shared.version.load(Ordering::SeqCst);
        let len = self.shared.cfg.content.len();
        self.shared
            .cfg
            .content
            .read(0, len as usize, v)
            .expect("readable")
    }

    pub fn sha256_hex(&self) -> Option<&str> {
        self.shared.digest_hex.as_deref()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

struct Request {
    method: String,
    path: String,
    range: Option<String>,
    if_range: Option<String>,
}

async fn read_request(r: &mut BufReader<tokio::net::tcp::OwnedReadHalf>) -> Option<Request> {
    let mut line = String::new();
    if r.read_line(&mut line).await.ok()? == 0 {
        return None;
    }
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();
    let mut range = None;
    let mut if_range = None;
    loop {
        let mut h = String::new();
        if r.read_line(&mut h).await.ok()? == 0 {
            return None;
        }
        let h = h.trim_end();
        if h.is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            match k.trim().to_ascii_lowercase().as_str() {
                "range" => range = Some(v.trim().to_string()),
                "if-range" => if_range = Some(v.trim().to_string()),
                _ => {}
            }
        }
    }
    Some(Request {
        method,
        path,
        range,
        if_range,
    })
}

fn parse_range(h: &str, size: u64) -> Option<(u64, u64)> {
    let spec = h.strip_prefix("bytes=")?;
    if spec.contains(',') {
        return None;
    }
    let (a, b) = spec.split_once('-')?;
    if a.is_empty() {
        let n: u64 = b.parse().ok()?;
        return Some((size.saturating_sub(n), size));
    }
    let start: u64 = a.parse().ok()?;
    let end = if b.is_empty() {
        size
    } else {
        b.parse::<u64>().ok()?.saturating_add(1).min(size)
    };
    (start < end).then_some((start, end))
}

async fn handle_conn(sock: TcpStream, sh: Arc<Shared>) -> std::io::Result<()> {
    let (rd, mut wr) = sock.into_split();
    let mut rd = BufReader::new(rd);
    while let Some(req) = read_request(&mut rd).await {
        let cfg = &sh.cfg;
        if cfg.latency_ms > 0 {
            tokio::time::sleep(Duration::from_millis(cfg.latency_ms)).await;
        }
        let path = req.path.split('?').next().unwrap_or("").to_string();
        if cfg.redirect && path == format!("/redirect{}", cfg.path) {
            let resp = format!(
                "HTTP/1.1 302 Found\r\nLocation: {}\r\nContent-Length: 0\r\n\r\n",
                cfg.path
            );
            wr.write_all(resp.as_bytes()).await?;
            continue;
        }
        if cfg.serve_sums && path == format!("{}.sha256", cfg.path) {
            let name = cfg.path.rsplit('/').next().unwrap_or("");
            let body = format!("{}  {}\n", sh.digest_hex.as_deref().unwrap_or(""), name);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            wr.write_all(resp.as_bytes()).await?;
            continue;
        }
        if path != cfg.path {
            wr.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
                .await?;
            continue;
        }
        let size = cfg.content.len();
        let (common, _) = common_headers(cfg, sh.version.load(Ordering::SeqCst));

        if req.method == "HEAD" {
            sh.stats.heads.fetch_add(1, Ordering::Relaxed);
            if cfg.reject_head {
                wr.write_all(b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n")
                    .await?;
                continue;
            }
            let mut extra = String::new();
            if cfg.repr_digest {
                if let Some(d) = &sh.digest_b64 {
                    extra = format!("Repr-Digest: sha-256=:{d}:\r\n");
                }
            }
            let resp = format!("HTTP/1.1 200 OK\r\nContent-Length: {size}\r\n{common}{extra}\r\n");
            wr.write_all(resp.as_bytes()).await?;
            continue;
        }

        let n = sh.stats.gets.fetch_add(1, Ordering::SeqCst) + 1;
        let fault = cfg
            .faults
            .iter()
            .find(|f| f.applies(n))
            .map(|f| f.fault.clone());
        if fault == Some(Fault::ChangeContent) {
            sh.version.fetch_add(1, Ordering::SeqCst);
            // Re-run with the new version on the same connection.
        }
        let version = sh.version.load(Ordering::SeqCst);
        let (common, etag_now) = common_headers(cfg, version);

        let inflight = sh.stats.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        sh.stats.max_in_flight.fetch_max(inflight, Ordering::SeqCst);
        let result = async {
            if let Some(max) = cfg.max_concurrent {
                if inflight > max as u64 {
                    sh.stats.throttled.fetch_add(1, Ordering::Relaxed);
                    wr.write_all(
                        b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Length: 0\r\n\r\n",
                    )
                    .await?;
                    return Ok(true);
                }
            }
            if let Some(Fault::Status(code, retry)) = &fault {
                let ra = retry.map(|s| format!("Retry-After: {s}\r\n")).unwrap_or_default();
                let resp = format!("HTTP/1.1 {code} Fault\r\n{ra}Content-Length: 0\r\n\r\n");
                wr.write_all(resp.as_bytes()).await?;
                return Ok(true);
            }

            // If-Range: only honour the range when the validator still matches.
            let validator_ok = match &req.if_range {
                None => true,
                Some(v) => Some(v) == etag_now.as_ref() || (version == 0 && Some(v) == cfg.last_modified.as_ref()),
            };
            let range = if cfg.support_ranges && validator_ok && fault != Some(Fault::IgnoreRange) {
                match &req.range {
                    Some(h) => match parse_range(h, size) {
                        Some(r) => Some(r),
                        None => {
                            let resp = format!(
                                "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{size}\r\nContent-Length: 0\r\n\r\n"
                            );
                            wr.write_all(resp.as_bytes()).await?;
                            return Ok(true);
                        }
                    },
                    None => None,
                }
            } else {
                None
            };
            let (start, end, status) = match range {
                Some((s, e)) => (s, e, "206 Partial Content"),
                None => (0, size, "200 OK"),
            };
            let mut headers = format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\n{common}", end - start);
            if range.is_some() {
                let (cs, ce) = if fault == Some(Fault::WrongContentRange) {
                    (start + 1, end)
                } else {
                    (start, end - 1)
                };
                headers += &format!("Content-Range: bytes {cs}-{ce}/{size}\r\n");
            }
            headers += "\r\n";
            wr.write_all(headers.as_bytes()).await?;
            if let Some(Fault::Stall(ms)) = fault {
                tokio::time::sleep(Duration::from_millis(ms)).await;
            }
            let body_limit = match fault {
                Some(Fault::ShortBody) => (end - start) / 2,
                Some(Fault::ResetAfter(n)) => n.min(end - start),
                _ => end - start,
            };
            let chunk = 64 * 1024u64;
            let mut off = start;
            let mut sent = 0u64;
            let started = tokio::time::Instant::now();
            while sent < body_limit {
                let n = chunk.min(body_limit - sent);
                let content = cfg.content.clone();
                let buf = tokio::task::spawn_blocking(move || content.read(off, n as usize, version))
                    .await
                    .expect("read task")?;
                wr.write_all(&buf).await?;
                sent += n;
                off += n;
                sh.stats.body_bytes.fetch_add(n, Ordering::Relaxed);
                if let Some(rate) = cfg.per_conn_rate {
                    let due = Duration::from_secs_f64(sent as f64 / rate as f64);
                    let el = started.elapsed();
                    if due > el {
                        tokio::time::sleep(due - el).await;
                    }
                }
            }
            Ok::<bool, std::io::Error>(body_limit == end - start)
        }
        .await;
        sh.stats.in_flight.fetch_sub(1, Ordering::SeqCst);
        match result {
            Ok(true) => {}
            // Short body / reset: drop the connection.
            Ok(false) | Err(_) => return Ok(()),
        }
    }
    Ok(())
}

fn common_headers(cfg: &ServerConfig, version: u64) -> (String, Option<String>) {
    let etag = cfg.etag.as_ref().map(|e| {
        if version > 0 {
            format!("\"v{}\"", version + 1)
        } else {
            e.clone()
        }
    });
    let mut h = String::new();
    if let Some(e) = &etag {
        h += &format!("ETag: {e}\r\n");
    }
    if let (Some(lm), 0) = (&cfg.last_modified, version) {
        h += &format!("Last-Modified: {lm}\r\n");
    }
    if cfg.support_ranges {
        h += "Accept-Ranges: bytes\r\n";
    }
    (h, etag)
}

/// Deterministic pseudo-random test data.
pub fn test_data(len: usize, seed: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}
