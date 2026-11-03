//! The `Download` builder: everything the CLI can do, as a library call.

use crate::refresh::command_refresher;
use qget_core::engine::{EngineConfig, Stats};
use qget_core::hash::parse_sums_file;
use qget_core::journal::Header;
use qget_core::{Error, ExpectedHash, HashAlgo, IntervalSet, Result, Sink, Source};
use qget_http::{ClientOptions, HttpClient, HttpSource, IpFamily, RemoteInfo};
use qget_runtime::driver::ProgressFn;
use qget_runtime::file_sink::{FileSink, FileSinkConfig, ResumeDecision, hash_file};
use qget_runtime::stream_sink::{StreamSink, WriteConsumer};
use qget_runtime::trace::TraceWriter;
use qget_runtime::{Progress, RunOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

const MIB: u64 = 1 << 20;

/// Where the downloaded bytes go.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Output {
    /// Current directory, name derived from the URL.
    Auto,
    /// This exact path.
    File(PathBuf),
    /// This directory, name derived from the URL.
    Dir(PathBuf),
    /// In-order stream to stdout (`-O -`).
    Stdout,
    /// Extract the archive into this directory while downloading.
    Extract(PathBuf),
}

/// Checksum discovery policy (INT-02).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Verify {
    /// Use `--checksum`/`Repr-Digest`; probe `<url>.sha256` when extracting.
    #[default]
    Auto,
    /// Also probe `<url>.sha256` for plain downloads, and fail if no checksum is found.
    Required,
    /// Ignore server-provided digests.
    Off,
}

/// What limited the transfer, for `--explain` (G3, CLI-04).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bottleneck {
    Network,
    ServerThrottling,
    Decode,
    Disk,
    Unknown,
}

impl std::fmt::Display for Bottleneck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Bottleneck::Network => "network (link or server per-connection speed)",
            Bottleneck::ServerThrottling => "server throttling (429/503)",
            Bottleneck::Decode => "decompression (single-threaded format)",
            Bottleneck::Disk => "disk or output writer",
            Bottleneck::Unknown => "unknown",
        })
    }
}

#[derive(Debug)]
pub struct Report {
    pub output: Option<PathBuf>,
    pub size: u64,
    pub elapsed: Duration,
    pub bytes_received: u64,
    pub resumed_bytes: u64,
    pub hash: Option<ExpectedHash>,
    /// The bytes were checked against a known checksum.
    pub verified: bool,
    pub resume: Option<ResumeDecision>,
    pub stats: Stats,
    pub seed: u64,
    pub sources: Vec<String>,
    pub bottleneck: Bottleneck,
    pub notes: Vec<String>,
    pub entries: Option<u64>,
}

pub struct Download {
    urls: Vec<String>,
    output: Output,
    checksum: Option<ExpectedHash>,
    checksum_url: Option<String>,
    verify: Verify,
    client: ClientOptions,
    connections: Option<u32>,
    max_connections: u32,
    polite: bool,
    limit_rate: Option<u64>,
    max_buffer: u64,
    seed: Option<u64>,
    trace: Option<PathBuf>,
    keep_archive: bool,
    restart: bool,
    idle_timeout: Duration,
    tries: u32,
    verify_after: bool,
    refresh_cmd: Option<String>,
    content_disposition: bool,
    extract_threads: Option<usize>,
    allow_special: bool,
    into_existing: bool,
    #[cfg(feature = "extract")]
    format: Option<qget_extract::Format>,
    progress: Option<ProgressFn>,
    cancel: CancellationToken,
}

impl Download {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            urls: vec![url.into()],
            output: Output::Auto,
            checksum: None,
            checksum_url: None,
            verify: Verify::Auto,
            client: ClientOptions::default(),
            connections: None,
            max_connections: 16,
            polite: false,
            limit_rate: None,
            max_buffer: 1 << 30,
            seed: None,
            trace: None,
            keep_archive: false,
            restart: false,
            idle_timeout: Duration::from_secs(15),
            tries: 10,
            verify_after: false,
            refresh_cmd: None,
            content_disposition: false,
            extract_threads: None,
            allow_special: false,
            into_existing: false,
            #[cfg(feature = "extract")]
            format: None,
            progress: None,
            cancel: CancellationToken::new(),
        }
    }

    /// Add a mirror serving the same bytes (HTTP-11). Requires a checksum or
    /// identical strong ETags.
    pub fn mirror(mut self, url: impl Into<String>) -> Self {
        self.urls.push(url.into());
        self
    }
    pub fn output(mut self, o: Output) -> Self {
        self.output = o;
        self
    }
    pub fn to_file(self, p: impl Into<PathBuf>) -> Self {
        self.output(Output::File(p.into()))
    }
    pub fn to_dir(self, p: impl Into<PathBuf>) -> Self {
        self.output(Output::Dir(p.into()))
    }
    pub fn extract_to(self, p: impl Into<PathBuf>) -> Self {
        self.output(Output::Extract(p.into()))
    }
    /// Expected checksum such as `sha256:…`, `sha512:…` or `blake3:…`.
    pub fn checksum(mut self, s: &str) -> Result<Self> {
        self.checksum = Some(
            ExpectedHash::parse(s)
                .ok_or_else(|| Error::Usage(format!("invalid checksum {s:?}")))?,
        );
        Ok(self)
    }
    pub fn expected_hash(mut self, h: ExpectedHash) -> Self {
        self.checksum = Some(h);
        self
    }
    /// URL of a `sha256sum`-style file listing the expected digest.
    pub fn checksum_url(mut self, url: impl Into<String>) -> Self {
        self.checksum_url = Some(url.into());
        self
    }
    pub fn verify(mut self, v: Verify) -> Self {
        self.verify = v;
        self
    }
    pub fn verify_after(mut self, v: bool) -> Self {
        self.verify_after = v;
        self
    }
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.client.headers.push((name.into(), value.into()));
        self
    }
    pub fn user_agent(mut self, ua: impl Into<String>) -> Self {
        self.client.user_agent = ua.into();
        self
    }
    pub fn insecure(mut self, v: bool) -> Self {
        self.client.insecure = v;
        self
    }
    pub fn proxy(mut self, p: impl Into<String>) -> Self {
        self.client.proxy = Some(p.into());
        self
    }
    pub fn ip_family(mut self, f: IpFamily) -> Self {
        self.client.ip = f;
        self
    }
    pub fn http2(mut self, v: bool) -> Self {
        self.client.http2 = v;
        self
    }
    pub fn http3(mut self, v: bool) -> Self {
        self.client.http3 = v;
        self
    }
    /// Use exactly `n` connections (disables adaptive concurrency).
    pub fn connections(mut self, n: u32) -> Self {
        self.connections = Some(n.clamp(1, 64));
        self
    }
    /// Upper bound for adaptive concurrency (default 16, max 64).
    pub fn max_connections(mut self, n: u32) -> Self {
        self.max_connections = n.clamp(1, 64);
        self
    }
    /// At most 4 connections and no duplicate (hedged) requests (NET-01).
    pub fn polite(mut self, v: bool) -> Self {
        self.polite = v;
        self
    }
    pub fn limit_rate(mut self, bytes_per_sec: u64) -> Self {
        self.limit_rate = Some(bytes_per_sec);
        self
    }
    /// Memory bound for in-order streaming (EXT-02).
    pub fn max_buffer(mut self, bytes: u64) -> Self {
        self.max_buffer = bytes.max(64 * MIB);
        self
    }
    pub fn seed(mut self, s: u64) -> Self {
        self.seed = Some(s);
        self
    }
    pub fn trace(mut self, p: impl Into<PathBuf>) -> Self {
        self.trace = Some(p.into());
        self
    }
    /// With `extract_to`: also keep the archive on disk, making the
    /// download resumable (EXT-07).
    pub fn keep_archive(mut self, v: bool) -> Self {
        self.keep_archive = v;
        self
    }
    /// Discard any partial download instead of resuming.
    pub fn restart(mut self, v: bool) -> Self {
        self.restart = v;
        self
    }
    pub fn idle_timeout(mut self, d: Duration) -> Self {
        self.idle_timeout = d;
        self
    }
    /// Consecutive failures without progress before giving up.
    pub fn tries(mut self, n: u32) -> Self {
        self.tries = n.max(1);
        self
    }
    pub fn refresh_cmd(mut self, cmd: impl Into<String>) -> Self {
        self.refresh_cmd = Some(cmd.into());
        self
    }
    /// Name the output from `Content-Disposition` (like `wget --content-disposition`).
    pub fn content_disposition(mut self, v: bool) -> Self {
        self.content_disposition = v;
        self
    }
    pub fn extract_threads(mut self, n: usize) -> Self {
        self.extract_threads = Some(n.max(1));
        self
    }
    pub fn allow_special(mut self, v: bool) -> Self {
        self.allow_special = v;
        self
    }
    pub fn extract_into_existing(mut self, v: bool) -> Self {
        self.into_existing = v;
        self
    }
    #[cfg(feature = "extract")]
    pub fn archive_format(mut self, f: qget_extract::Format) -> Self {
        self.format = Some(f);
        self
    }
    pub fn on_progress(mut self, f: impl Fn(&Progress) + Send + Sync + 'static) -> Self {
        self.progress = Some(Arc::new(f));
        self
    }
    /// Cancel the download; partial state stays resumable (LIB-05).
    pub fn cancel_token(mut self, t: CancellationToken) -> Self {
        self.cancel = t;
        self
    }

    pub async fn run(self) -> Result<Report> {
        let started = Instant::now();
        let client = HttpClient::new(&self.client)?;
        // Probe every URL concurrently.
        let probes = futures_join(self.urls.iter().map(|u| {
            let c = client.clone();
            let u = u.clone();
            async move { c.probe(&u).await }
        }))
        .await;
        let mut infos: Vec<RemoteInfo> = Vec::new();
        for (i, p) in probes.into_iter().enumerate() {
            match p {
                Ok(info) => infos.push(info),
                Err(e) if i == 0 => return Err(e),
                Err(e) => tracing::warn!("ignoring mirror {}: {e}", self.urls[i]),
            }
        }
        let primary = infos[0].clone();

        let expected = self.resolve_checksum(&client, &primary).await?;
        if self.verify == Verify::Required && expected.is_none() {
            return Err(Error::Integrity {
                expected: "a published checksum".into(),
                actual: "none found (--verify=required)".into(),
            });
        }

        // Mirrors must provably serve the same bytes (HTTP-11).
        if infos.len() > 1 {
            infos.retain(|m| {
                let ok = m.size == primary.size && m.ranges;
                if !ok {
                    tracing::warn!("ignoring mirror {}: size or range support differs", m.url);
                }
                ok
            });
            let same_etags = infos
                .iter()
                .all(|m| m.validator.etag.is_some() && m.validator.etag == primary.validator.etag);
            if infos.len() > 1 && expected.is_none() && !same_etags {
                return Err(Error::Usage(
                    "mirrors serve objects with different ETags; pass --checksum so their bytes can be verified".into(),
                ));
            }
        }

        let output = self.output.clone();
        let name = self.file_name(&primary);
        let size = match (primary.size, primary.ranges) {
            (Some(s), true) => s,
            _ => {
                return self
                    .run_single_stream(&client, &primary, expected, &name, started)
                    .await;
            }
        };

        let file_path = match &output {
            Output::Auto => Some(PathBuf::from(&name)),
            Output::File(p) => Some(p.clone()),
            Output::Dir(d) => Some(d.join(&name)),
            Output::Stdout => None,
            Output::Extract(dir) if self.keep_archive => Some(
                dir.parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new("."))
                    .join(&name),
            ),
            Output::Extract(_) => None,
        };

        let seed = self.seed.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0)
        });
        let mut cfg = EngineConfig::new(size, infos.len() as u32);
        cfg.seed = seed;
        cfg.retry_budget = self.tries;
        cfg.max_conns = if self.polite {
            self.max_connections.min(4)
        } else {
            self.max_connections
        };
        if let Some(n) = self.connections {
            cfg.max_conns = n;
            cfg.initial_conns = n;
            cfg.adaptive = false;
        }
        cfg.initial_conns = cfg.initial_conns.min(cfg.max_conns);
        cfg.hedge = !self.polite;
        if let Some(p) = &file_path
            && qget_runtime::fsutil::is_rotational(p)
        {
            // HDDs: fewer, larger sequential writes (§10.5).
            cfg.min_block = 16 * MIB;
            cfg.initial_block = 32 * MIB;
            cfg.max_conns = cfg.max_conns.min(8);
            cfg.initial_conns = cfg.initial_conns.min(cfg.max_conns);
        }

        let mut notes = Vec::new();
        let mut resume = None;
        let validator = primary.validator.clone();
        let header = Header {
            size,
            validator: validator.clone(),
            url: primary.url.clone(),
            expected: expected.clone(),
        };

        let (sink, coverage): (Box<dyn Sink>, IntervalSet) = match (&output, file_path.clone()) {
            (Output::Stdout, _) => {
                cfg.window = Some(self.max_buffer);
                let consumer = WriteConsumer::new(std::io::stdout());
                (
                    Box::new(StreamSink::new(
                        size,
                        Box::new(consumer),
                        None,
                        expected.clone(),
                    )),
                    IntervalSet::new(),
                )
            }
            (Output::Extract(dir), None) => {
                cfg.window = Some(self.max_buffer);
                let ex = self.extractor(dir)?;
                (
                    Box::new(StreamSink::new(size, ex, None, expected.clone())),
                    IntervalSet::new(),
                )
            }
            (_, Some(path)) => {
                if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| Error::io(format!("create {}", parent.display()), e))?;
                }
                let mut fc = FileSinkConfig::new(path, header);
                fc.hash_algo = expected.as_ref().map(|e| e.algo);
                fc.restart = self.restart;
                if let Output::Extract(dir) = &output {
                    // Extraction follows the contiguous prefix on disk; the
                    // engine already allocates lowest-offset gaps first.
                    fc.tap = Some(self.extractor(dir)?);
                }
                let (sink, decision, cov) = FileSink::open(fc)?;
                if let ResumeDecision::Restarted { reason } = &decision {
                    notes.push(format!("previous partial download discarded: {reason}"));
                }
                resume = Some(decision);
                (Box::new(sink), cov)
            }
            (_, None) => unreachable!("only stdout and extract have no file path"),
        };

        let sources: Vec<Arc<dyn Source>> = infos
            .iter()
            .map(|info| {
                let mut s = HttpSource::new(client.clone(), info);
                if let Some(cmd) = &self.refresh_cmd {
                    s = s.with_refresher(command_refresher(
                        cmd.clone(),
                        client.clone(),
                        size,
                        info.validator.clone(),
                    ));
                }
                Arc::new(s) as Arc<dyn Source>
            })
            .collect();
        let labels: Vec<String> = sources.iter().map(|s| s.describe()).collect();

        let trace = match &self.trace {
            Some(p) => {
                let f = std::fs::File::create(p)
                    .map_err(|e| Error::io(format!("create {}", p.display()), e))?;
                Some(
                    TraceWriter::new(Box::new(std::io::BufWriter::new(f)), &cfg, &coverage)
                        .map_err(|e| Error::io("write trace", e))?,
                )
            }
            None => None,
        };

        let opts = RunOptions {
            idle_timeout: self.idle_timeout,
            rate_limit: self.limit_rate,
            trace,
            progress: self.progress.clone(),
            cancel: self.cancel.clone(),
            ..Default::default()
        };
        let run = qget_runtime::run(cfg, coverage, sources, sink, opts).await?;

        let mut hash = run.sink.hash.clone();
        let mut verified = expected.is_some();
        let output_path = run.sink.output.clone();
        if self.verify_after
            && let Some(p) = output_path.as_ref().filter(|p| p.is_file())
        {
            let algo = expected.as_ref().map_or(HashAlgo::Sha256, |e| e.algo);
            let h = hash_file(p, algo)?;
            if let Some(e) = &expected
                && &h != e
            {
                return Err(Error::Integrity {
                    expected: e.to_string(),
                    actual: format!("{h} (re-read from disk)"),
                });
            }
            verified = expected.is_some();
            hash = Some(h);
        }
        let consumer = run.sink.consumer.clone();
        if let Some(c) = &consumer {
            notes.extend(c.notes.iter().cloned());
        }
        let elapsed = started.elapsed();
        let bottleneck = classify(&run, consumer.as_ref(), elapsed);
        Ok(Report {
            output: output_path,
            size,
            elapsed,
            bytes_received: run.bytes_received,
            resumed_bytes: run.resumed_bytes,
            hash,
            verified,
            resume,
            stats: run.stats,
            seed,
            sources: labels,
            bottleneck,
            notes,
            entries: consumer.map(|c| c.entries),
        })
    }

    fn file_name(&self, info: &RemoteInfo) -> String {
        if self.content_disposition
            && let Some(n) = &info.filename
        {
            return n.clone();
        }
        name_from_url(&self.urls[0]).unwrap_or_else(|| "index.html".into())
    }

    #[cfg(feature = "extract")]
    fn extractor(&self, dir: &Path) -> Result<Box<dyn qget_core::OrderedConsumer>> {
        let mut o = qget_extract::ExtractOptions::new(dir);
        if let Some(t) = self.extract_threads {
            o.threads = t;
        }
        o.allow_special = self.allow_special;
        o.into_existing = self.into_existing;
        o.format = self.format;
        Ok(Box::new(qget_extract::Extractor::new(o)?))
    }

    #[cfg(not(feature = "extract"))]
    fn extractor(&self, _dir: &Path) -> Result<Box<dyn qget_core::OrderedConsumer>> {
        Err(Error::Unsupported(
            "this build has no extraction support".into(),
        ))
    }

    async fn resolve_checksum(
        &self,
        client: &HttpClient,
        primary: &RemoteInfo,
    ) -> Result<Option<ExpectedHash>> {
        if let Some(c) = &self.checksum {
            return Ok(Some(c.clone()));
        }
        let name = name_from_url(&primary.url).unwrap_or_default();
        if let Some(url) = &self.checksum_url {
            let text = client
                .fetch_text(url, 4 << 20)
                .await?
                .ok_or_else(|| Error::Usage(format!("checksum file {url} not found")))?;
            let algo = if url.contains("512") {
                HashAlgo::Sha512
            } else {
                HashAlgo::Sha256
            };
            return parse_sums_file(&text, &name, algo)
                .map(Some)
                .ok_or_else(|| Error::Usage(format!("no checksum for {name} in {url}")));
        }
        if self.verify == Verify::Off {
            return Ok(None);
        }
        if let Some(d) = &primary.digest {
            return Ok(Some(d.clone()));
        }
        let probe_sidecar =
            self.verify == Verify::Required || matches!(self.output, Output::Extract(_));
        if probe_sidecar {
            let base = primary.url.split('?').next().unwrap_or(&primary.url);
            for (suffix, algo) in [(".sha256", HashAlgo::Sha256), (".sha512", HashAlgo::Sha512)] {
                if let Ok(Some(text)) = client.fetch_text(&format!("{base}{suffix}"), 1 << 16).await
                    && let Some(h) = parse_sums_file(&text, &name, algo)
                {
                    return Ok(Some(h));
                }
            }
        }
        Ok(None)
    }

    /// Servers without range support or with unknown length: one stream,
    /// restarted from scratch on failure (REL-04).
    async fn run_single_stream(
        &self,
        client: &HttpClient,
        info: &RemoteInfo,
        expected: Option<ExpectedHash>,
        name: &str,
        started: Instant,
    ) -> Result<Report> {
        use std::io::Write;
        let algo = expected.as_ref().map(|e| e.algo);
        let mut last_err = None;
        for attempt in 0..self.tries {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(500 << attempt.min(5))).await;
            }
            let mut out: Box<dyn qget_core::OrderedConsumer> = match &self.output {
                Output::Stdout => Box::new(WriteConsumer::new(std::io::stdout())),
                Output::Extract(dir) if !self.keep_archive => self.extractor(dir)?,
                o => {
                    let path = match o {
                        Output::File(p) => p.clone(),
                        Output::Dir(d) => d.join(name),
                        _ => PathBuf::from(name),
                    };
                    Box::new(PartFile::create(path)?)
                }
            };
            let mut hasher = algo.map(qget_core::Hasher::new);
            let mut got = 0u64;
            let result: Result<()> = async {
                let mut stream = client.get_stream(&info.url).await?;
                let mut last_tick = Instant::now();
                loop {
                    if self.cancel.is_cancelled() {
                        return Err(Error::Interrupted);
                    }
                    let chunk = tokio::time::timeout(self.idle_timeout, stream.chunk())
                        .await
                        .map_err(|_| Error::Network("no data received before timeout".into()))??;
                    let Some(c) = chunk else { break };
                    if let Some(h) = hasher.as_mut() {
                        h.update(&c);
                    }
                    out.consume(&c)?;
                    got += c.len() as u64;
                    if let (Some(p), true) = (
                        &self.progress,
                        last_tick.elapsed() > Duration::from_millis(100),
                    ) {
                        last_tick = Instant::now();
                        p(&Progress {
                            downloaded: got,
                            total: info.size.unwrap_or(0),
                            rate: (got as f64 / started.elapsed().as_secs_f64().max(0.001)) as u64,
                            connections: 1,
                            target: 1,
                            elapsed: started.elapsed(),
                        });
                    }
                }
                if let Some(s) = info.size
                    && got != s
                {
                    return Err(Error::Network(format!("body ended at {got} of {s} bytes")));
                }
                Ok(())
            }
            .await;
            match result {
                Ok(()) => {
                    let hash = hasher.map(qget_core::Hasher::finalize);
                    if let (Some(a), Some(e)) = (&hash, &expected)
                        && a != e
                    {
                        out.abort();
                        return Err(Error::Integrity {
                            expected: e.to_string(),
                            actual: a.to_string(),
                        });
                    }
                    let rep = out.finish()?;
                    let _ = std::io::stdout().flush();
                    return Ok(Report {
                        output: rep.output.clone(),
                        size: got,
                        elapsed: started.elapsed(),
                        bytes_received: got,
                        resumed_bytes: 0,
                        hash,
                        verified: expected.is_some(),
                        resume: None,
                        stats: Stats::default(),
                        seed: 0,
                        sources: vec![info.url.clone()],
                        bottleneck: Bottleneck::Network,
                        notes: vec![
                            "server does not support range requests: single connection, not resumable".into(),
                        ],
                        entries: Some(rep.entries).filter(|e| *e > 0),
                    });
                }
                Err(Error::Interrupted) => {
                    out.abort();
                    return Err(Error::Interrupted);
                }
                Err(
                    e @ (Error::HttpStatus { .. }
                    | Error::Extract(_)
                    | Error::Io { .. }
                    | Error::DiskFull { .. }),
                ) => {
                    out.abort();
                    return Err(e);
                }
                Err(e) => {
                    out.abort();
                    if matches!(self.output, Output::Stdout) && got > 0 {
                        // Bytes already went to stdout; restarting would duplicate them.
                        return Err(e);
                    }
                    tracing::warn!("attempt {} failed: {e}", attempt + 1);
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| Error::Network("download failed".into())))
    }
}

/// A `.qget-part` file renamed into place on success (single-stream mode).
struct PartFile {
    path: PathBuf,
    part: PathBuf,
    file: std::io::BufWriter<std::fs::File>,
    bytes: u64,
}

impl PartFile {
    fn create(path: PathBuf) -> Result<Self> {
        let part = qget_runtime::file_sink::part_path(&path);
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).map_err(|e| Error::io("create directory", e))?;
        }
        let f = std::fs::File::create(&part)
            .map_err(|e| Error::io(format!("create {}", part.display()), e))?;
        Ok(Self {
            path,
            part,
            file: std::io::BufWriter::with_capacity(1 << 20, f),
            bytes: 0,
        })
    }
}

impl qget_core::OrderedConsumer for PartFile {
    fn consume(&mut self, data: &[u8]) -> Result<()> {
        use std::io::Write;
        self.file
            .write_all(data)
            .map_err(|e| Error::io("write output", e))?;
        self.bytes += data.len() as u64;
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<qget_core::ConsumerReport> {
        let f = self
            .file
            .into_inner()
            .map_err(|e| Error::io("flush output", e.into_error()))?;
        f.sync_all().map_err(|e| Error::io("fsync output", e))?;
        std::fs::rename(&self.part, &self.path).map_err(|e| Error::io("rename output", e))?;
        qget_runtime::fsutil::sync_parent(&self.path)?;
        Ok(qget_core::ConsumerReport {
            bytes_out: self.bytes,
            output: Some(self.path.clone()),
            ..Default::default()
        })
    }

    fn abort(self: Box<Self>) {
        let _ = std::fs::remove_file(&self.part);
    }
}

fn classify(
    run: &qget_runtime::RunReport,
    consumer: Option<&qget_core::ConsumerReport>,
    elapsed: Duration,
) -> Bottleneck {
    let total = elapsed.as_millis().max(1) as f64;
    if run.stats.throttled_ms as f64 > 0.2 * total || run.stats.throttles > 3 {
        return Bottleneck::ServerThrottling;
    }
    if let Some(c) = consumer {
        let serial = c.notes.iter().any(|n| n.contains("decode: serial"));
        if serial && c.decode_busy_ms as f64 > 0.8 * total {
            return Bottleneck::Decode;
        }
    }
    if run.sink_wait.as_millis() as f64 > 0.3 * total {
        return match consumer {
            Some(_) => Bottleneck::Decode,
            None => Bottleneck::Disk,
        };
    }
    if run.bytes_received == 0 {
        return Bottleneck::Unknown;
    }
    Bottleneck::Network
}

/// Last path segment of a URL, percent-decoded and sanitised.
pub fn name_from_url(url: &str) -> Option<String> {
    let no_query = url.split(['?', '#']).next()?;
    let path = no_query
        .split_once("://")
        .map_or(no_query, |(_, rest)| rest);
    let path = path.split_once('/').map(|(_, p)| p)?;
    let last = path.rsplit('/').next()?;
    qget_http::sanitize_filename(&qget_http::percent_decode(last))
}

/// Await a set of futures concurrently without pulling in `futures`.
async fn futures_join<F, T>(futs: impl Iterator<Item = F>) -> Vec<T>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let handles: Vec<_> = futs.map(tokio::spawn).collect();
    let mut out = Vec::with_capacity(handles.len());
    for h in handles {
        out.push(h.await.expect("probe task"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_from_urls() {
        assert_eq!(
            name_from_url("https://x.com/a/b/snap%20shot.tar.lz4?sig=1").as_deref(),
            Some("snap shot.tar.lz4")
        );
        assert_eq!(name_from_url("https://x.com/"), None);
        assert_eq!(name_from_url("https://x.com"), None);
        assert_eq!(name_from_url("https://x.com/a/..").as_deref(), None);
    }
}
