//! HTTP(S) source for qget.
//!
//! - [`HttpClient::probe`] discovers size, range support, validators,
//!   filename and any `Repr-Digest` (HTTP-02, INT-03).
//! - [`HttpSource`] serves byte ranges with `If-Range`, strict
//!   `Content-Range` validation and error classification (HTTP-03/04/08).

use base64::Engine as _;
use bytes::Bytes;
use qget_core::content_range::{self, Parsed};
use qget_core::engine::{FailKind, FatalKind};
use qget_core::{
    BoxFuture, ByteRange, ChunkStream, Error, ExpectedHash, HashAlgo, Result, Source, Validator,
};
use reqwest::header::{self, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Response, StatusCode};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IpFamily {
    #[default]
    Any,
    V4,
    V6,
}

#[derive(Clone, Debug)]
pub struct ClientOptions {
    pub user_agent: String,
    pub headers: Vec<(String, String)>,
    pub insecure: bool,
    pub proxy: Option<String>,
    pub ip: IpFamily,
    pub connect_timeout: Duration,
    /// Allow HTTP/2. Each parallel connection then gets its own client so
    /// ranges still travel over separate TCP connections (§10.1).
    pub http2: bool,
    /// HTTP/3 with prior knowledge (experimental build feature `http3`).
    pub http3: bool,
    /// Number of independent connection pools (only matters with HTTP/2).
    pub pools: usize,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            user_agent: format!("qget/{}", env!("CARGO_PKG_VERSION")),
            headers: Vec::new(),
            insecure: false,
            proxy: None,
            ip: IpFamily::Any,
            connect_timeout: Duration::from_secs(10),
            http2: false,
            http3: false,
            pools: 16,
        }
    }
}

/// What a probe learned about a URL.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RemoteInfo {
    /// URL after redirects; range requests go here (signed redirect targets).
    pub url: String,
    pub size: Option<u64>,
    pub ranges: bool,
    pub validator: Validator,
    pub filename: Option<String>,
    pub digest: Option<ExpectedHash>,
}

#[derive(Clone)]
pub struct HttpClient {
    clients: Arc<Vec<Client>>,
    next: Arc<AtomicUsize>,
}

impl HttpClient {
    pub fn new(opts: &ClientOptions) -> Result<Self> {
        let n = if opts.http2 { opts.pools.max(1) } else { 1 };
        let clients = (0..n)
            .map(|_| build_client(opts))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            clients: Arc::new(clients),
            next: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn client(&self) -> &Client {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        &self.clients[i]
    }

    /// Discover size, range support and validators (HTTP-02).
    pub async fn probe(&self, url: &str) -> Result<RemoteInfo> {
        let mut info = RemoteInfo {
            url: url.to_string(),
            ..Default::default()
        };
        // HEAD for metadata; failures (e.g. 403/405 on GET-only presigned
        // URLs) are fine because the ranged GET below is authoritative.
        let mut head_size = None;
        if let Ok(resp) = self.client().head(url).send().await
            && resp.status().is_success()
        {
            info.url = resp.url().to_string();
            head_size = header_u64(resp.headers(), header::CONTENT_LENGTH);
            info.validator = validator_from(resp.headers());
            info.filename = filename_from(resp.headers());
            info.digest = digest_from(resp.headers());
        }
        let resp = self
            .client()
            .get(&info.url)
            .header(header::RANGE, "bytes=0-0")
            .send()
            .await
            .map_err(net_error)?;
        info.url = resp.url().to_string();
        let status = resp.status();
        let headers = resp.headers().clone();
        if info.validator.is_empty() {
            info.validator = validator_from(&headers);
        }
        if info.filename.is_none() {
            info.filename = filename_from(&headers);
        }
        if info.digest.is_none() {
            info.digest = digest_from(&headers);
        }
        match status {
            StatusCode::PARTIAL_CONTENT => {
                let cr = headers
                    .get(header::CONTENT_RANGE)
                    .and_then(|v| v.to_str().ok())
                    .and_then(content_range::parse);
                match cr {
                    Some(Parsed::Range(r)) if r.first == 0 && r.last == 0 => {
                        info.size = r.complete.or(head_size);
                        info.ranges = info.size.is_some();
                    }
                    _ => {
                        return Err(Error::Unsupported(format!(
                            "{url}: invalid Content-Range on probe"
                        )));
                    }
                }
            }
            StatusCode::OK => {
                info.size = header_u64(&headers, header::CONTENT_LENGTH).or(head_size);
                info.ranges = false;
            }
            StatusCode::RANGE_NOT_SATISFIABLE => {
                // Empty resources answer 416 to bytes=0-0.
                let complete = headers
                    .get(header::CONTENT_RANGE)
                    .and_then(|v| v.to_str().ok())
                    .and_then(content_range::parse);
                if let Some(Parsed::Unsatisfied(u)) = complete {
                    info.size = Some(u.complete);
                } else if head_size == Some(0) {
                    info.size = Some(0);
                } else {
                    return Err(Error::HttpStatus {
                        status: 416,
                        url: url.to_string(),
                    });
                }
            }
            s => {
                return Err(Error::HttpStatus {
                    status: s.as_u16(),
                    url: url.to_string(),
                });
            }
        }
        drop(resp);
        Ok(info)
    }

    /// GET a small text resource (checksum files). `Ok(None)` on 404/410.
    pub async fn fetch_text(&self, url: &str, max_len: usize) -> Result<Option<String>> {
        let resp = self.client().get(url).send().await.map_err(net_error)?;
        match resp.status() {
            s if s.is_success() => {}
            StatusCode::NOT_FOUND | StatusCode::GONE | StatusCode::FORBIDDEN => return Ok(None),
            s => {
                return Err(Error::HttpStatus {
                    status: s.as_u16(),
                    url: url.to_string(),
                });
            }
        }
        if resp.content_length().is_some_and(|l| l as usize > max_len) {
            return Ok(None);
        }
        let bytes = resp.bytes().await.map_err(net_error)?;
        if bytes.len() > max_len {
            return Ok(None);
        }
        Ok(Some(String::from_utf8_lossy(&bytes).into_owned()))
    }

    /// GET the whole body as a stream (servers without range support or
    /// unknown length).
    pub async fn get_stream(&self, url: &str) -> Result<HttpStream> {
        let resp = self.client().get(url).send().await.map_err(net_error)?;
        if !resp.status().is_success() {
            return Err(Error::HttpStatus {
                status: resp.status().as_u16(),
                url: url.to_string(),
            });
        }
        Ok(HttpStream { resp })
    }
}

fn build_client(opts: &ClientOptions) -> Result<Client> {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::ACCEPT_ENCODING,
        HeaderValue::from_static("identity"),
    );
    for (k, v) in &opts.headers {
        let name = HeaderName::from_bytes(k.as_bytes())
            .map_err(|_| Error::Usage(format!("invalid header name {k:?}")))?;
        let value = HeaderValue::from_str(v)
            .map_err(|_| Error::Usage(format!("invalid header value for {k}")))?;
        headers.insert(name, value);
    }
    let mut b = Client::builder()
        .user_agent(opts.user_agent.clone())
        .default_headers(headers)
        .connect_timeout(opts.connect_timeout)
        .tcp_nodelay(true)
        .pool_max_idle_per_host(64)
        .pool_idle_timeout(Duration::from_secs(60))
        .redirect(reqwest::redirect::Policy::limited(10));
    if opts.insecure {
        b = b.danger_accept_invalid_certs(true);
    }
    if let Some(p) = &opts.proxy {
        b = b.proxy(reqwest::Proxy::all(p).map_err(|e| Error::Usage(format!("proxy: {e}")))?);
    }
    match opts.ip {
        IpFamily::Any => {}
        IpFamily::V4 => b = b.local_address(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
        IpFamily::V6 => b = b.local_address(IpAddr::V6(Ipv6Addr::UNSPECIFIED)),
    }
    #[cfg(feature = "http3")]
    if opts.http3 {
        b = b.http3_prior_knowledge();
    }
    #[cfg(not(feature = "http3"))]
    if opts.http3 {
        return Err(Error::Unsupported(
            "this build of qget has no HTTP/3 support (build feature `http3`)".into(),
        ));
    }
    if opts.http2 {
        b = b.http2_adaptive_window(true);
    } else if !opts.http3 {
        b = b.http1_only();
    }
    b.build()
        .map_err(|e| Error::Usage(format!("HTTP client: {e}")))
}

fn net_error(e: reqwest::Error) -> Error {
    Error::Network(error_chain(&e))
}

fn error_chain(e: &dyn std::error::Error) -> String {
    let mut s = e.to_string();
    let mut cur = e.source();
    while let Some(c) = cur {
        let cs = c.to_string();
        if !s.contains(&cs) {
            s.push_str(": ");
            s.push_str(&cs);
        }
        cur = c.source();
    }
    s
}

fn header_u64(h: &HeaderMap, name: HeaderName) -> Option<u64> {
    h.get(name)?.to_str().ok()?.trim().parse().ok()
}

/// Strong ETag and Last-Modified (weak ETags are never used for ranges).
pub fn validator_from(h: &HeaderMap) -> Validator {
    let etag = h
        .get(header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|e| !e.starts_with("W/") && e.starts_with('"'))
        .map(str::to_string);
    let last_modified = h
        .get(header::LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    Validator {
        etag,
        last_modified,
    }
}

/// Filename from `Content-Disposition`, sanitised to a single path component.
pub fn filename_from(h: &HeaderMap) -> Option<String> {
    let v = h.get(header::CONTENT_DISPOSITION)?.to_str().ok()?;
    let mut plain = None;
    for part in v.split(';').map(str::trim) {
        if let Some(rest) = part.strip_prefix("filename*=") {
            // RFC 5987: charset'lang'pct-encoded
            if let Some(enc) = rest.splitn(3, '\'').nth(2) {
                return sanitize_filename(&percent_decode(enc));
            }
        } else if let Some(rest) = part.strip_prefix("filename=") {
            plain = Some(rest.trim_matches('"').to_string());
        }
    }
    plain.and_then(|p| sanitize_filename(&p))
}

/// Keep only the final path component; reject empty, `.` and `..`.
pub fn sanitize_filename(name: &str) -> Option<String> {
    let base = name.rsplit(['/', '\\']).next()?.trim();
    let base: String = base.chars().filter(|c| !c.is_control()).collect();
    if base.is_empty() || base == "." || base == ".." {
        None
    } else {
        Some(base)
    }
}

pub fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    let hex = |b: u8| (b as char).to_digit(16).map(|d| d as u8);
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(h), Some(l)) = (hex(bytes[i + 1]), hex(bytes[i + 2]))
        {
            out.push(h << 4 | l);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `Repr-Digest` / `Content-Digest` (RFC 9530), preferring SHA-512.
pub fn digest_from(h: &HeaderMap) -> Option<ExpectedHash> {
    let mut best: Option<ExpectedHash> = None;
    for name in ["repr-digest", "content-digest"] {
        let Some(v) = h.get(name).and_then(|v| v.to_str().ok()) else {
            continue;
        };
        for item in v.split(',') {
            let Some((alg, val)) = item.trim().split_once('=') else {
                continue;
            };
            let algo = match alg.trim().to_ascii_lowercase().as_str() {
                "sha-256" => HashAlgo::Sha256,
                "sha-512" => HashAlgo::Sha512,
                _ => continue,
            };
            let b64 = val.trim().trim_matches(':');
            let Ok(raw) = base64::engine::general_purpose::STANDARD.decode(b64) else {
                continue;
            };
            let cand = ExpectedHash::from_bytes(algo, &raw);
            if ExpectedHash::parse(&cand.to_string()).is_none() {
                continue;
            }
            if best.as_ref().is_none_or(|b| b.algo == HashAlgo::Sha256) {
                best = Some(cand);
            }
        }
        if best.is_some() {
            break;
        }
    }
    best
}

/// Parse `Retry-After` seconds (HTTP-dates fall back to 5 s).
fn retry_after(h: &HeaderMap) -> Option<u64> {
    let v = h.get(header::RETRY_AFTER)?.to_str().ok()?.trim();
    Some(v.parse::<u64>().map(|s| s.min(300) * 1000).unwrap_or(5000))
}

/// Async function that returns a fresh URL for an expired signed URL.
pub type Refresher =
    Arc<dyn Fn() -> BoxFuture<'static, std::result::Result<String, String>> + Send + Sync>;

/// One HTTP(S) URL serving the resource.
pub struct HttpSource {
    client: HttpClient,
    url: Arc<RwLock<String>>,
    validator: Validator,
    size: u64,
    refresher: Option<Refresher>,
    refreshes: Arc<AtomicUsize>,
}

impl HttpSource {
    pub fn new(client: HttpClient, info: &RemoteInfo) -> Self {
        Self {
            client,
            url: Arc::new(RwLock::new(info.url.clone())),
            validator: info.validator.clone(),
            size: info.size.unwrap_or(0),
            refresher: None,
            refreshes: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Call `refresher` on 403 to obtain a new URL (HTTP-12).
    pub fn with_refresher(mut self, refresher: Refresher) -> Self {
        self.refresher = Some(refresher);
        self
    }
}

pub struct HttpStream {
    resp: Response,
}

impl HttpStream {
    pub async fn chunk(&mut self) -> Result<Option<Bytes>> {
        self.resp.chunk().await.map_err(net_error)
    }
}

impl ChunkStream for HttpStream {
    fn next_chunk(&mut self) -> BoxFuture<'_, Option<std::result::Result<Bytes, FailKind>>> {
        Box::pin(async move {
            match self.resp.chunk().await {
                Ok(Some(b)) => Some(Ok(b)),
                Ok(None) => None,
                Err(e) => Some(Err(FailKind::Transient(error_chain(&e)))),
            }
        })
    }
}

impl Source for HttpSource {
    fn open(
        &self,
        range: ByteRange,
    ) -> BoxFuture<'static, std::result::Result<Box<dyn ChunkStream>, FailKind>> {
        let client = self.client.client().clone();
        let url = self.url.read().expect("url lock").clone();
        let validator = self.validator.clone();
        let size = self.size;
        let refresher = self.refresher.clone();
        let refreshes = self.refreshes.clone();
        let url_slot = self.url.clone();
        Box::pin(async move {
            let mut req = client.get(&url).header(header::RANGE, range.http_header());
            if let Some(v) = validator.if_range() {
                req = req.header(header::IF_RANGE, v);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| FailKind::Transient(error_chain(&e)))?;
            let status = resp.status();
            let h = resp.headers();
            match status {
                StatusCode::PARTIAL_CONTENT => {
                    let cr = h
                        .get(header::CONTENT_RANGE)
                        .and_then(|v| v.to_str().ok())
                        .and_then(content_range::parse);
                    match cr {
                        Some(Parsed::Range(r)) if r.matches(range.start, range.end, size) => {}
                        Some(Parsed::Range(r)) if r.complete.is_some_and(|c| c != size) => {
                            return Err(FailKind::Fatal {
                                kind: FatalKind::RemoteChanged,
                                msg: format!("size changed to {:?}", r.complete),
                            });
                        }
                        other => {
                            return Err(FailKind::Transient(format!(
                                "Content-Range {other:?} does not match requested {}",
                                range.http_header()
                            )));
                        }
                    }
                    if let Some(len) = header_u64(h, header::CONTENT_LENGTH)
                        && len != range.len()
                    {
                        return Err(FailKind::Transient(format!(
                            "Content-Length {len} does not match range length {}",
                            range.len()
                        )));
                    }
                    // A changed strong ETag on a 206 means the object changed
                    // (servers that ignore If-Range).
                    let now = validator_from(h);
                    if validator.same_as(&now) == Some(false) {
                        return Err(FailKind::Fatal {
                            kind: FatalKind::RemoteChanged,
                            msg: "validator changed during download".into(),
                        });
                    }
                    Ok(Box::new(HttpStream { resp }) as Box<dyn ChunkStream>)
                }
                StatusCode::OK => {
                    // If-Range failed (object changed) or the server ignored Range.
                    let now = validator_from(h);
                    if validator.same_as(&now) == Some(false) {
                        return Err(FailKind::Fatal {
                            kind: FatalKind::RemoteChanged,
                            msg: "server returned a different version (If-Range failed)".into(),
                        });
                    }
                    if range.start == 0 && range.end == size {
                        return Ok(Box::new(HttpStream { resp }) as Box<dyn ChunkStream>);
                    }
                    Err(FailKind::Transient(
                        "server ignored the Range header (200 instead of 206)".into(),
                    ))
                }
                StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE => {
                    Err(FailKind::Throttled {
                        retry_after_ms: retry_after(h),
                    })
                }
                StatusCode::RANGE_NOT_SATISFIABLE => Err(FailKind::Fatal {
                    kind: FatalKind::RemoteChanged,
                    msg: "range not satisfiable (resource shrank?)".into(),
                }),
                StatusCode::FORBIDDEN
                    if refresher.is_some() && refreshes.load(Ordering::SeqCst) < 5 =>
                {
                    refreshes.fetch_add(1, Ordering::SeqCst);
                    let f = refresher.expect("checked");
                    match f().await {
                        Ok(new_url) => {
                            *url_slot.write().expect("url lock") = new_url;
                            Err(FailKind::Transient("URL refreshed after 403".into()))
                        }
                        Err(e) => Err(FailKind::Fatal {
                            kind: FatalKind::HttpStatus(403),
                            msg: format!("URL refresh failed: {e}"),
                        }),
                    }
                }
                s if s.is_server_error() || s == StatusCode::REQUEST_TIMEOUT => {
                    Err(FailKind::Transient(format!("HTTP {}", s.as_u16())))
                }
                s => Err(FailKind::Fatal {
                    kind: FatalKind::HttpStatus(s.as_u16()),
                    msg: format!("HTTP {}", s.as_u16()),
                }),
            }
        })
    }

    fn describe(&self) -> String {
        let url = self.url.read().expect("url lock").clone();
        match reqwest::Url::parse(&url) {
            Ok(u) => format!("{}{}", u.host_str().unwrap_or("?"), u.path()),
            Err(_) => url,
        }
    }
}

#[cfg(test)]
mod tests;
