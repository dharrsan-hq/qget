//! Command-line definition. Flag names follow wget where they overlap (CLI-01).

use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "qget",
    version,
    about = "Fast, verified, resumable downloads with parallel stream-extraction",
    long_about = "qget downloads files over many adaptive HTTP range connections, resumes safely after \
crashes, verifies checksums while downloading, and can extract .tar.lz4/.tar.zst/.tar.gz/.tar.xz \
archives while they download without storing the archive.",
    args_conflicts_with_subcommands = true,
    subcommand_negates_reqs = true,
    after_help = "Exit codes: 0 ok, 2 usage, 3 network, 4 HTTP status, 5 checksum mismatch, \
6 remote changed, 7 local I/O or disk full, 8 interrupted (re-run to resume), 9 extraction, \
10 peer-to-peer, 11 unsupported."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Cmd>,

    #[command(flatten)]
    pub get: GetArgs,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Replay a --trace file and check every scheduling decision is reproduced.
    Replay { trace: PathBuf },
    /// Print shell completions.
    Completions { shell: clap_complete::Shell },
    /// Print the man page (roff).
    Man,
    /// Print a file's checksum in qget's `algo:hex` form.
    Hash {
        file: PathBuf,
        #[arg(long, value_enum, default_value_t = AlgoArg::Blake3)]
        algo: AlgoArg,
    },
    /// Send a file or directory to a peer (prints a code / ticket).
    #[cfg(feature = "p2p")]
    #[command(visible_alias = "send")]
    Yeet(crate::p2p::YeetArgs),
    /// Receive from a peer using a code or ticket.
    #[cfg(feature = "p2p")]
    #[command(visible_alias = "receive")]
    Yoink(crate::p2p::YoinkArgs),
    /// Run a rendezvous mailbox server for short codes.
    #[cfg(feature = "p2p")]
    Mailbox(crate::p2p::MailboxArgs),
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
pub enum AlgoArg {
    Sha256,
    Sha512,
    Blake3,
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProgressArg {
    Auto,
    Bar,
    Json,
    None,
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VerifyArg {
    Auto,
    Required,
    Off,
}

#[derive(Args, Debug, Default)]
pub struct GetArgs {
    /// URL(s) to download. Several URLs are downloaded one after another.
    #[arg(required = true, value_name = "URL")]
    pub urls: Vec<String>,

    /// Write to FILE ('-' for stdout).
    #[arg(short = 'O', long = "output-document", value_name = "FILE")]
    pub output: Option<String>,

    /// Save files under DIR.
    #[arg(short = 'P', long = "directory-prefix", value_name = "DIR")]
    pub dir: Option<PathBuf>,

    /// Resume a partial download (always on; accepted for wget compatibility).
    #[arg(short = 'c', long = "continue")]
    pub continue_: bool,

    /// Discard any partial download and start over.
    #[arg(long)]
    pub restart: bool,

    /// Read URLs from FILE ('-' for stdin), one per line.
    #[arg(short = 'i', long = "input-file", value_name = "FILE")]
    pub input_file: Option<String>,

    /// Extract the archive into DIR while downloading (archive is not stored).
    #[arg(short = 'x', long, value_name = "DIR")]
    pub extract: Option<PathBuf>,

    /// With --extract: also keep the archive, making the download resumable.
    #[arg(long, requires = "extract")]
    pub keep_archive: bool,

    /// With --extract: merge into an existing non-empty directory.
    #[arg(long, requires = "extract")]
    pub extract_into_existing: bool,

    /// With --extract: skip device nodes and FIFOs instead of failing.
    #[arg(long, requires = "extract")]
    pub allow_special: bool,

    /// Archive format, instead of detecting it from magic bytes.
    #[arg(long, value_name = "tar|lz4|zst|gz|xz", requires = "extract")]
    pub format: Option<String>,

    /// Memory for in-order streaming (stdout, --extract), e.g. 512M, 2G.
    #[arg(long, value_name = "SIZE")]
    pub max_buffer: Option<String>,

    /// Expected checksum: sha256:HEX, sha512:HEX or blake3:HEX.
    #[arg(long, value_name = "ALGO:HEX")]
    pub checksum: Option<String>,

    /// URL of a sha256sum-style file listing the expected checksum.
    #[arg(long, value_name = "URL")]
    pub checksum_url: Option<String>,

    /// Checksum discovery: auto (Repr-Digest, .sha256 when extracting), required, off.
    #[arg(long, value_enum)]
    pub verify: Option<VerifyArg>,

    /// Re-read the finished file from disk and hash it again.
    #[arg(long)]
    pub verify_after: bool,

    /// Additional URL serving identical bytes (repeatable).
    #[arg(long = "mirror", value_name = "URL")]
    pub mirrors: Vec<String>,

    /// Use exactly N connections (disables adaptive concurrency).
    #[arg(short = 'n', long, value_name = "N")]
    pub connections: Option<u32>,

    /// Upper bound for adaptive concurrency (default 16, max 64).
    #[arg(long, value_name = "N")]
    pub max_connections: Option<u32>,

    /// At most 4 connections and no duplicate requests.
    #[arg(long)]
    pub polite: bool,

    /// Output is on a spinning disk: larger blocks, at most 8 connections.
    #[arg(long)]
    pub hdd: bool,

    /// Limit the total download rate, e.g. 500k, 10M.
    #[arg(long, value_name = "RATE")]
    pub limit_rate: Option<String>,

    /// Add a request header (repeatable), e.g. 'Authorization: Bearer X'.
    #[arg(long = "header", value_name = "NAME: VALUE")]
    pub headers: Vec<String>,

    /// User-Agent header.
    #[arg(short = 'U', long, value_name = "AGENT")]
    pub user_agent: Option<String>,

    /// Proxy URL (http://, https://, socks5://). Environment proxies are used by default.
    #[arg(long, value_name = "URL")]
    pub proxy: Option<String>,

    /// Do not verify TLS certificates (insecure).
    #[arg(long)]
    pub no_check_certificate: bool,

    /// Connect over IPv4 only.
    #[arg(short = '4', long = "inet4-only", conflicts_with = "inet6_only")]
    pub inet4_only: bool,

    /// Connect over IPv6 only.
    #[arg(short = '6', long = "inet6-only")]
    pub inet6_only: bool,

    /// Allow HTTP/2 (each connection still gets its own TCP stream).
    #[arg(long)]
    pub http2: bool,

    /// HTTP/3 (experimental; requires a build with the http3 feature).
    #[arg(long)]
    pub http3: bool,

    /// Consecutive failures without progress before giving up.
    #[arg(short = 't', long, value_name = "N")]
    pub tries: Option<u32>,

    /// Seconds without data before a connection is retried.
    #[arg(short = 'T', long, value_name = "SECS")]
    pub timeout: Option<u64>,

    /// Command printing a fresh URL when a signed URL expires (403).
    #[arg(long, value_name = "CMD")]
    pub refresh_cmd: Option<String>,

    /// Name the file from the Content-Disposition header.
    #[arg(long)]
    pub content_disposition: bool,

    /// Progress output on stderr.
    #[arg(long, value_enum)]
    pub progress: Option<ProgressArg>,

    /// No progress or summary output.
    #[arg(short, long)]
    pub quiet: bool,

    /// More log output (-v, -vv).
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Print a bottleneck analysis and scheduler statistics at the end.
    #[arg(long)]
    pub explain: bool,

    /// Seed for every randomised scheduling decision.
    #[arg(long, value_name = "N")]
    pub seed: Option<u64>,

    /// Write the full decision trace (JSON lines) to FILE.
    #[arg(long, value_name = "FILE")]
    pub trace: Option<PathBuf>,

    /// Config file (default: $XDG_CONFIG_HOME/qget/config.toml).
    #[arg(long, value_name = "FILE")]
    pub config: Option<PathBuf>,
}

/// Parse sizes like `512`, `64k`, `10M`, `2G`, `1.5G` (binary units).
pub fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let (num, mult) = match s.char_indices().find(|(_, c)| c.is_ascii_alphabetic()) {
        Some((i, _)) => {
            let unit = s[i..]
                .trim_end_matches(['B', 'b', 'i'])
                .to_ascii_lowercase();
            let m: u64 = match unit.as_str() {
                "" => 1,
                "k" => 1 << 10,
                "m" => 1 << 20,
                "g" => 1 << 30,
                "t" => 1 << 40,
                _ => return Err(format!("unknown size unit in {s:?}")),
            };
            (&s[..i], m)
        }
        None => (s, 1),
    };
    let v: f64 = num
        .trim()
        .parse()
        .map_err(|_| format!("invalid size {s:?}"))?;
    if !v.is_finite() || v < 0.0 {
        return Err(format!("invalid size {s:?}"));
    }
    Ok((v * mult as f64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_is_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn sizes() {
        assert_eq!(parse_size("512").unwrap(), 512);
        assert_eq!(parse_size("64k").unwrap(), 65536);
        assert_eq!(parse_size("10M").unwrap(), 10 << 20);
        assert_eq!(parse_size("1.5G").unwrap(), 3 << 29);
        assert_eq!(parse_size("2GiB").unwrap(), 2 << 30);
        assert!(parse_size("5x").is_err());
        assert!(parse_size("-1").is_err());
    }

    #[test]
    fn parses_wget_style_invocation() {
        let c = Cli::try_parse_from([
            "qget",
            "-c",
            "-q",
            "-O",
            "out.bin",
            "--header",
            "A: b",
            "https://x/y",
        ])
        .unwrap();
        assert_eq!(c.get.urls, vec!["https://x/y"]);
        assert_eq!(c.get.output.as_deref(), Some("out.bin"));
        assert!(c.get.quiet && c.get.continue_);
    }

    #[test]
    fn subcommands_parse() {
        let c = Cli::try_parse_from(["qget", "hash", "f", "--algo", "sha256"]).unwrap();
        assert!(matches!(c.command, Some(Cmd::Hash { .. })));
        let c = Cli::try_parse_from(["qget", "replay", "t.jsonl"]).unwrap();
        assert!(matches!(c.command, Some(Cmd::Replay { .. })));
    }
}
