//! Standalone test server for benchmarks: serves one file with optional
//! per-connection rate limits and latency.

use clap::Parser;
use qget_testserver::{Content, ServerConfig, TestServer};
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "Serve a file over HTTP/1.1 with range support, for qget benchmarks")]
struct Args {
    /// File to serve.
    file: PathBuf,
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: String,
    /// Per-connection rate limit, bytes per second.
    #[arg(long)]
    rate_per_conn: Option<u64>,
    /// Delay before each response, in milliseconds.
    #[arg(long, default_value_t = 0)]
    latency_ms: u64,
    /// Answer 429 above this many concurrent requests.
    #[arg(long)]
    max_concurrent: Option<u32>,
    /// Also serve `<path>.sha256`.
    #[arg(long)]
    sums: bool,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let a = Args::parse();
    let size = std::fs::metadata(&a.file)?.len();
    let name = a
        .file
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("file.bin")
        .to_string();
    let mut cfg = ServerConfig::new(Content::File {
        path: a.file.clone(),
        size,
    });
    cfg.path = format!("/{name}");
    cfg.per_conn_rate = a.rate_per_conn;
    cfg.latency_ms = a.latency_ms;
    cfg.max_concurrent = a.max_concurrent;
    cfg.serve_sums = a.sums;
    let srv = TestServer::bind(&a.listen, cfg).await?;
    println!("{}", srv.url());
    tokio::signal::ctrl_c().await?;
    Ok(())
}
