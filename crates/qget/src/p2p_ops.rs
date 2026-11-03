//! Peer-to-peer send/receive orchestration (feature `p2p`).

use crate::download::{Bottleneck, Output, Report};
use qget_core::b3tree::GROUP_LEN;
use qget_core::engine::EngineConfig;
use qget_core::journal::Header;
use qget_core::{Error, Result, Source, Validator};
use qget_http::{ClientOptions, HttpClient, HttpSource};
use qget_p2p::pake::{self, Code};
use qget_p2p::{NetConfig, PeerSource, Provider, Share, Ticket, VerifiedSource, fetch_info, net};
use qget_runtime::driver::ProgressFn;
use qget_runtime::file_sink::{FileSink, FileSinkConfig};
use qget_runtime::{Progress, RunOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub use qget_p2p::pake::Code as ShortCode;
pub use qget_p2p::{NetConfig as P2pNet, RelayChoice, Ticket as P2pTicket};

/// A running share. Dropping it stops serving.
pub struct Sending {
    pub ticket: Ticket,
    pub code: Option<Code>,
    provider: Option<Provider>,
    exchange: Option<JoinHandle<Result<()>>>,
    _tmp: Option<tempfile::TempDir>,
}

impl Sending {
    /// Bytes of content served so far.
    pub fn served(&self) -> u64 {
        self.provider.as_ref().map_or(0, |p| p.served())
    }

    /// Wait for the short-code exchange (if any) to deliver the ticket.
    pub async fn code_claimed(&mut self) -> Result<()> {
        match self.exchange.take() {
            Some(h) => h.await.map_err(|e| Error::P2p(e.to_string()))?,
            None => Ok(()),
        }
    }

    pub async fn shutdown(mut self) {
        if let Some(p) = self.provider.take() {
            p.shutdown().await;
        }
    }
}

/// Share a file or directory. With a mailbox, also allocates a short code.
pub async fn send(path: &Path, net: &NetConfig, mailbox: Option<&str>) -> Result<Sending> {
    let meta = std::fs::metadata(path).map_err(|e| Error::io(format!("{}", path.display()), e))?;
    let base = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("share")
        .to_string();
    let (file, name, dir, tmp) = if meta.is_dir() {
        let tmp = tempfile::tempdir().map_err(|e| Error::io("temp dir", e))?;
        let tar_path = tmp.path().join(format!("{base}.tar"));
        let (src, dst) = (path.to_path_buf(), tar_path.clone());
        let base2 = base.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let f = std::fs::File::create(&dst).map_err(|e| Error::io("create tar", e))?;
            let mut b = tar::Builder::new(std::io::BufWriter::new(f));
            b.follow_symlinks(false);
            b.append_dir_all(&base2, &src)
                .and_then(|_| {
                    b.into_inner()
                        .and_then(|w| w.into_inner().map_err(|e| e.into_error()))
                })
                .map_err(|e| Error::io("build tar", e))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::P2p(e.to_string()))??;
        (tar_path, base, true, Some(tmp))
    } else {
        (path.to_path_buf(), base, false, None)
    };
    let share = tokio::task::spawn_blocking(move || Share::prepare(&file, name, dir))
        .await
        .map_err(|e| Error::P2p(e.to_string()))??;
    let endpoint = net::bind(net, true).await?;
    let provider = Provider::start(endpoint.clone(), share);
    let ticket = provider.ticket(net::public_addr(&endpoint, net).await);
    let (code, exchange) = match mailbox {
        Some(m) => {
            let (code, fut) = pake::offer(m, ticket.clone()).await?;
            (Some(code), Some(tokio::spawn(fut)))
        }
        None => (None, None),
    };
    Ok(Sending {
        ticket,
        code,
        provider: Some(provider),
        exchange,
        _tmp: tmp,
    })
}

/// Resolve a short code into a ticket via the mailbox.
pub async fn resolve_code(mailbox: &str, code: &str) -> Result<Ticket> {
    let c =
        Code::parse(code).ok_or_else(|| Error::Usage(format!("{code:?} is not a valid code")))?;
    pake::accept(mailbox, &c).await
}

pub struct Receive {
    tickets: Vec<Ticket>,
    output: Output,
    mirrors: Vec<String>,
    net: NetConfig,
    progress: Option<ProgressFn>,
    cancel: CancellationToken,
    restart: bool,
    max_connections: u32,
    keep_tar: bool,
}

impl Receive {
    pub fn new(ticket: Ticket) -> Self {
        Self {
            tickets: vec![ticket],
            output: Output::Auto,
            mirrors: Vec::new(),
            net: NetConfig::default(),
            progress: None,
            cancel: CancellationToken::new(),
            restart: false,
            max_connections: 16,
            keep_tar: false,
        }
    }
    /// Another peer sharing the same content (swarm).
    pub fn peer(mut self, t: Ticket) -> Self {
        self.tickets.push(t);
        self
    }
    /// An HTTP mirror of the same bytes, verified group by group (hybrid).
    pub fn mirror(mut self, url: impl Into<String>) -> Self {
        self.mirrors.push(url.into());
        self
    }
    pub fn output(mut self, o: Output) -> Self {
        self.output = o;
        self
    }
    pub fn net(mut self, n: NetConfig) -> Self {
        self.net = n;
        self
    }
    pub fn on_progress(mut self, f: impl Fn(&Progress) + Send + Sync + 'static) -> Self {
        self.progress = Some(Arc::new(f));
        self
    }
    pub fn cancel_token(mut self, t: CancellationToken) -> Self {
        self.cancel = t;
        self
    }
    pub fn restart(mut self, v: bool) -> Self {
        self.restart = v;
        self
    }
    pub fn max_connections(mut self, n: u32) -> Self {
        self.max_connections = n.clamp(1, 64);
        self
    }
    /// For directory shares: keep the transferred tar (needed for `--seed`).
    pub fn keep_tar(mut self, v: bool) -> Self {
        self.keep_tar = v;
        self
    }

    pub async fn run(self) -> Result<Report> {
        let started = Instant::now();
        let first = self.tickets[0].clone();
        if self
            .tickets
            .iter()
            .any(|t| t.root != first.root || t.size != first.size)
        {
            return Err(Error::Usage(
                "all peers must share the same content (BLAKE3 root)".into(),
            ));
        }
        let endpoint = net::bind(&self.net, false).await?;
        let (info, conn) = fetch_info(&endpoint, &first).await?;
        let outboard = Arc::new(info.outboard);
        let mut sources: Vec<Arc<dyn Source>> = vec![Arc::new(PeerSource::new(
            endpoint.clone(),
            &first,
            outboard.clone(),
            Some(conn),
        )?)];
        for t in &self.tickets[1..] {
            sources.push(Arc::new(PeerSource::new(
                endpoint.clone(),
                t,
                outboard.clone(),
                None,
            )?));
        }
        if !self.mirrors.is_empty() {
            let client = HttpClient::new(&ClientOptions::default())?;
            for m in &self.mirrors {
                match client.probe(m).await {
                    Ok(info) if info.size == Some(first.size) && info.ranges => {
                        let http: Arc<dyn Source> =
                            Arc::new(HttpSource::new(client.clone(), &info));
                        sources.push(Arc::new(VerifiedSource::new(http, outboard.clone())));
                    }
                    Ok(_) => {
                        tracing::warn!("ignoring mirror {m}: size differs or no range support")
                    }
                    Err(e) => tracing::warn!("ignoring mirror {m}: {e}"),
                }
            }
        }
        let name = qget_http::sanitize_filename(&first.name).unwrap_or_else(|| "download".into());
        let (file_path, extract_dir) = if first.dir {
            let dest = match &self.output {
                Output::File(p) | Output::Extract(p) => p.clone(),
                Output::Dir(d) => d.join(&name),
                _ => PathBuf::from(&name),
            };
            let mut tar = dest.as_os_str().to_owned();
            tar.push(".qget-recv.tar");
            (PathBuf::from(tar), Some(dest))
        } else {
            let p = match &self.output {
                Output::File(p) => p.clone(),
                Output::Dir(d) => d.join(&name),
                Output::Auto => PathBuf::from(&name),
                Output::Stdout | Output::Extract(_) => {
                    return Err(Error::Unsupported("P2P receive writes to a file".into()));
                }
            };
            (p, None)
        };
        if let Some(parent) = file_path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).map_err(|e| Error::io("create directory", e))?;
        }
        let header = Header {
            size: first.size,
            validator: Validator {
                etag: Some(format!("\"b3:{}\"", first.root)),
                last_modified: None,
            },
            url: format!("qget://{}", first.root),
            expected: Some(first.expected()),
        };
        let mut fc = FileSinkConfig::new(file_path.clone(), header);
        fc.hash_algo = Some(qget_core::HashAlgo::Blake3);
        fc.restart = self.restart;
        #[cfg(feature = "extract")]
        if let Some(dest) = &extract_dir {
            let mut o = qget_extract::ExtractOptions::new(dest.clone());
            o.format = Some(qget_extract::Format::Tar);
            fc.tap = Some(Box::new(qget_extract::Extractor::new(o)?));
        }
        #[cfg(not(feature = "extract"))]
        if extract_dir.is_some() {
            return Err(Error::Unsupported(
                "receiving directories needs the extract feature".into(),
            ));
        }
        let (sink, decision, coverage) = FileSink::open(fc)?;
        let mut cfg = EngineConfig::new(first.size, sources.len() as u32);
        cfg.alignment = GROUP_LEN;
        cfg.min_block = 4 * GROUP_LEN;
        cfg.max_conns = self.max_connections;
        let labels: Vec<String> = sources.iter().map(|s| s.describe()).collect();
        let opts = RunOptions {
            progress: self.progress.clone(),
            cancel: self.cancel.clone(),
            ..Default::default()
        };
        let run = qget_runtime::run(cfg, coverage, sources, Box::new(sink), opts).await;
        endpoint.close().await;
        let run = run?;
        let mut notes = vec![format!(
            "verified every 1 MiB group against BLAKE3 root {}",
            first.root
        )];
        let mut output = run.sink.output.clone();
        if let Some(dest) = extract_dir {
            if !self.keep_tar {
                let _ = std::fs::remove_file(&file_path);
            }
            notes.push("directory share extracted".into());
            output = Some(dest);
        }
        Ok(Report {
            output,
            size: first.size,
            elapsed: started.elapsed(),
            bytes_received: run.bytes_received,
            resumed_bytes: run.resumed_bytes,
            hash: run.sink.hash.clone(),
            verified: true,
            resume: Some(decision),
            stats: run.stats,
            seed: 0,
            sources: labels,
            bottleneck: Bottleneck::Network,
            notes,
            entries: run.sink.consumer.map(|c| c.entries),
        })
    }
}

/// Serve an already-downloaded file to others (P2P-08 seeding).
pub async fn seed(path: &Path, name: &str, net: &NetConfig) -> Result<Sending> {
    let (p, n) = (path.to_path_buf(), name.to_string());
    let share = tokio::task::spawn_blocking(move || Share::prepare(&p, n, false))
        .await
        .map_err(|e| Error::P2p(e.to_string()))??;
    let endpoint = net::bind(net, true).await?;
    let provider = Provider::start(endpoint.clone(), share);
    let ticket = provider.ticket(net::public_addr(&endpoint, net).await);
    Ok(Sending {
        ticket,
        code: None,
        provider: Some(provider),
        exchange: None,
        _tmp: None,
    })
}

/// Run a mailbox server (for short codes) on `listen`.
pub async fn run_mailbox(listen: &str) -> Result<()> {
    let l = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|e| Error::io(format!("bind {listen}"), e))?;
    qget_p2p::mailbox::serve(l)
        .await
        .map_err(|e| Error::io("mailbox", e))
}
