//! `qget` command-line tool: a thin layer over the `qget` library (LIB-03).

mod cli;
mod config;
#[cfg(feature = "p2p")]
mod p2p;

use clap::{CommandFactory, Parser};
use cli::{AlgoArg, Cli, Cmd, GetArgs, ProgressArg, VerifyArg, parse_size};
use indicatif::{ProgressBar, ProgressStyle};
use qget::{Download, Error, Output, Progress, Report, Verify};
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

fn main() -> ExitCode {
    let cli = Cli::parse();
    let verbose = cli.get.verbose;
    let filter = std::env::var("QGET_LOG").unwrap_or_else(|_| {
        match verbose {
            0 => "warn",
            1 => "info",
            _ => "debug",
        }
        .into()
    });
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .with_writer(std::io::stderr)
        .with_target(false)
        .without_time()
        .init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let code = rt.block_on(async {
        match cli.command {
            Some(cmd) => run_command(cmd).await,
            None => run_downloads(cli.get).await,
        }
    });
    ExitCode::from(code as u8)
}

async fn run_command(cmd: Cmd) -> i32 {
    match cmd {
        Cmd::Replay { trace } => match qget::replay_trace(&trace) {
            Ok((n, None)) => {
                println!("replayed {n} events: every decision reproduced");
                0
            }
            Ok((n, Some(m))) => {
                eprintln!("qget: replay diverged after {n} events: {m}");
                1
            }
            Err(e) => report_error(&e),
        },
        Cmd::Completions { shell } => {
            clap_complete::generate(shell, &mut Cli::command(), "qget", &mut std::io::stdout());
            0
        }
        Cmd::Man => {
            let mut out = Vec::new();
            if clap_mangen::Man::new(Cli::command())
                .render(&mut out)
                .is_err()
            {
                return 1;
            }
            let mut extra = String::from(".SH \"EXIT STATUS\"\n");
            for (code, desc) in qget::EXIT_CODES {
                extra.push_str(&format!(".TP\n\\fB{code}\\fR\n{desc}\n"));
            }
            extra.push_str(
                ".SH FILES\n.TP\n\\fI$XDG_CONFIG_HOME/qget/config.toml\\fR\nDefaults for long options (kebab-case keys).\n\
.SH ENVIRONMENT\n.TP\n\\fBQGET_LOG\\fR\nLog filter (e.g. debug).\n.TP\n\\fBHTTP_PROXY\\fR, \\fBHTTPS_PROXY\\fR, \\fBNO_PROXY\\fR\nProxy settings.\n",
            );
            out.extend_from_slice(extra.as_bytes());
            let _ = std::io::stdout().write_all(&out);
            0
        }
        Cmd::Hash { file, algo } => {
            let algo = match algo {
                AlgoArg::Sha256 => qget::HashAlgo::Sha256,
                AlgoArg::Sha512 => qget::HashAlgo::Sha512,
                AlgoArg::Blake3 => qget::HashAlgo::Blake3,
            };
            match tokio::task::spawn_blocking(move || qget::hash_file(&file, algo)).await {
                Ok(Ok(h)) => {
                    println!("{h}");
                    0
                }
                Ok(Err(e)) => report_error(&e),
                Err(_) => 1,
            }
        }
        #[cfg(feature = "p2p")]
        Cmd::Yeet(a) => p2p::yeet(a).await,
        #[cfg(feature = "p2p")]
        Cmd::Yoink(a) => p2p::yoink(a).await,
        #[cfg(feature = "p2p")]
        Cmd::Mailbox(a) => p2p::mailbox(a).await,
    }
}

pub fn report_error(e: &Error) -> i32 {
    eprintln!("qget: {e}");
    e.exit_code()
}

struct Settings {
    progress: ProgressArg,
    quiet: bool,
}

async fn run_downloads(mut a: GetArgs) -> i32 {
    let cfg = match config::load(a.config.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("qget: config: {e}");
            return 2;
        }
    };
    if let Some(list) = &a.input_file {
        let text = if list == "-" {
            std::io::read_to_string(std::io::stdin())
        } else {
            std::fs::read_to_string(list)
        };
        match text {
            Ok(t) => a.urls.extend(
                t.lines()
                    .map(str::trim)
                    .filter(|l| !l.is_empty() && !l.starts_with('#'))
                    .map(String::from),
            ),
            Err(e) => {
                eprintln!("qget: {list}: {e}");
                return 2;
            }
        }
    }
    if a.urls.len() > 1 && (a.output.is_some() || a.extract.is_some()) {
        eprintln!("qget: -O/--extract need a single URL (use --mirror for alternative sources)");
        return 2;
    }
    let progress = a.progress.or(cfg.progress).unwrap_or(ProgressArg::Auto);
    let settings = Settings {
        progress: match progress {
            _ if a.quiet => ProgressArg::None,
            ProgressArg::Auto if std::io::stderr().is_terminal() => ProgressArg::Bar,
            ProgressArg::Auto => ProgressArg::None,
            p => p,
        },
        quiet: a.quiet,
    };

    let token = qget::CancellationToken::new();
    let t2 = token.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            t2.cancel();
            if tokio::signal::ctrl_c().await.is_ok() {
                std::process::exit(130);
            }
        }
    });

    let urls = std::mem::take(&mut a.urls);
    let mut worst = 0;
    for url in urls {
        let dl = match build(&a, &cfg, &url, token.clone()) {
            Ok(d) => d,
            Err(e) => return report_error(&e),
        };
        let code = run_one(dl, &url, &a, &settings).await;
        if code == 8 {
            return code;
        }
        worst = worst.max(code);
    }
    worst
}

fn build(
    a: &GetArgs,
    cfg: &config::Config,
    url: &str,
    token: qget::CancellationToken,
) -> Result<Download, Error> {
    let usage = |e: String| Error::Usage(e);
    let mut d = Download::new(url).cancel_token(token);
    for m in &a.mirrors {
        d = d.mirror(m.clone());
    }
    d = match (&a.output, &a.extract) {
        (_, Some(dir)) => d.extract_to(dir.clone()),
        (Some(o), None) if o == "-" => d.output(Output::Stdout),
        (Some(o), None) => d.to_file(match &a.dir {
            Some(dir) => dir.join(o),
            None => PathBuf::from(o),
        }),
        (None, None) => match &a.dir {
            Some(dir) => d.to_dir(dir.clone()),
            None => d,
        },
    };
    if let Some(c) = &a.checksum {
        d = d.checksum(c)?;
    }
    if let Some(u) = &a.checksum_url {
        d = d.checksum_url(u.clone());
    }
    d = d.verify(match a.verify.or(cfg.verify).unwrap_or(VerifyArg::Auto) {
        VerifyArg::Auto => Verify::Auto,
        VerifyArg::Required => Verify::Required,
        VerifyArg::Off => Verify::Off,
    });
    d = d.verify_after(a.verify_after).restart(a.restart);
    if let Some(n) = a.connections {
        d = d.connections(n);
    }
    if let Some(n) = a.max_connections.or(cfg.max_connections) {
        d = d.max_connections(n);
    }
    d = d.polite(a.polite || cfg.polite.unwrap_or(false)).hdd(a.hdd);
    if let Some(r) = a.limit_rate.as_ref().or(cfg.limit_rate.as_ref()) {
        d = d.limit_rate(parse_size(r).map_err(usage)?);
    }
    if let Some(b) = a.max_buffer.as_ref().or(cfg.max_buffer.as_ref()) {
        d = d.max_buffer(parse_size(b).map_err(usage)?);
    }
    for h in cfg.headers.iter().chain(&a.headers) {
        let (k, v) = h
            .split_once(':')
            .ok_or_else(|| Error::Usage(format!("header {h:?} must look like 'Name: value'")))?;
        d = d.header(k.trim(), v.trim());
    }
    if let Some(ua) = a.user_agent.as_ref().or(cfg.user_agent.as_ref()) {
        d = d.user_agent(ua.clone());
    }
    if let Some(p) = a.proxy.as_ref().or(cfg.proxy.as_ref()) {
        d = d.proxy(p.clone());
    }
    if a.no_check_certificate {
        eprintln!(
            "qget: WARNING: TLS certificate verification is disabled (--no-check-certificate)"
        );
        d = d.insecure(true);
    }
    if a.inet4_only {
        d = d.ip_family(qget::IpFamily::V4);
    } else if a.inet6_only {
        d = d.ip_family(qget::IpFamily::V6);
    }
    d = d
        .http2(a.http2 || cfg.http2.unwrap_or(false))
        .http3(a.http3);
    if let Some(t) = a.tries.or(cfg.tries) {
        d = d.tries(t);
    }
    if let Some(t) = a.timeout.or(cfg.timeout) {
        d = d.idle_timeout(std::time::Duration::from_secs(t.max(1)));
    }
    if let Some(c) = &a.refresh_cmd {
        d = d.refresh_cmd(c.clone());
    }
    d = d.content_disposition(a.content_disposition);
    if let Some(s) = a.seed {
        d = d.seed(s);
    }
    if let Some(t) = &a.trace {
        d = d.trace(t.clone());
    }
    if a.extract.is_some() {
        d = d
            .keep_archive(a.keep_archive)
            .extract_into_existing(a.extract_into_existing)
            .allow_special(a.allow_special);
        #[cfg(feature = "extract")]
        if let Some(f) = &a.format {
            let fmt = qget::ArchiveFormat::from_name(f)
                .ok_or_else(|| Error::Usage(format!("unknown archive format {f:?}")))?;
            d = d.archive_format(fmt);
        }
    }
    Ok(d)
}

async fn run_one(mut dl: Download, url: &str, a: &GetArgs, s: &Settings) -> i32 {
    let bar = match s.progress {
        ProgressArg::Bar => {
            let pb = ProgressBar::new(0);
            pb.set_style(
                ProgressStyle::with_template(
                    "{spinner:.cyan} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} {msg}",
                )
                .expect("valid template")
                .progress_chars("=> "),
            );
            pb.set_message(short_url(url));
            let pb = Arc::new(pb);
            let pb2 = pb.clone();
            dl = dl.on_progress(move |p: &Progress| {
                pb2.set_length(p.total);
                pb2.set_position(p.downloaded);
                pb2.set_message(format!("{}/s  {} conns", human(p.rate), p.connections));
            });
            Some(pb)
        }
        ProgressArg::Json => {
            dl = dl.on_progress(|p: &Progress| {
                let line = serde_json::json!({
                    "event": "progress",
                    "downloaded": p.downloaded,
                    "total": p.total,
                    "rate": p.rate,
                    "connections": p.connections,
                    "elapsed_ms": p.elapsed.as_millis() as u64,
                });
                eprintln!("{line}");
            });
            None
        }
        _ => None,
    };
    let result = dl.run().await;
    if let Some(pb) = &bar {
        pb.finish_and_clear();
    }
    match result {
        Ok(r) => {
            if s.progress == ProgressArg::Json {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "event": "done",
                        "output": r.output,
                        "size": r.size,
                        "elapsed_ms": r.elapsed.as_millis() as u64,
                        "hash": r.hash.as_ref().map(|h| h.to_string()),
                        "verified": r.verified,
                        "bottleneck": r.bottleneck.to_string(),
                    })
                );
            } else if !s.quiet {
                summary(&r);
            }
            if a.explain {
                explain(&r);
            }
            0
        }
        Err(e) => {
            if s.progress == ProgressArg::Json {
                eprintln!(
                    "{}",
                    serde_json::json!({"event": "error", "code": e.exit_code(), "message": e.to_string()})
                );
            }
            report_error(&e)
        }
    }
}

fn summary(r: &Report) {
    let secs = r.elapsed.as_secs_f64().max(0.001);
    let rate = (r.bytes_received as f64 / secs) as u64;
    let target = r
        .output
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "stdout".into());
    let mut line = format!(
        "qget: {} → {} in {:.1}s ({}/s)",
        human(r.size),
        target,
        secs,
        human(rate)
    );
    if r.resumed_bytes > 0 {
        line.push_str(&format!(", resumed {}", human(r.resumed_bytes)));
    }
    if let Some(h) = &r.hash {
        line.push_str(if r.verified { ", verified " } else { ", " });
        line.push_str(&h.to_string());
    }
    if let Some(n) = r.entries {
        line.push_str(&format!(", {n} entries"));
    }
    eprintln!("{line}");
    for n in &r.notes {
        eprintln!("qget: note: {n}");
    }
}

fn explain(r: &Report) {
    let s = &r.stats;
    eprintln!("── qget explain ─────────────────────────────");
    eprintln!("bottleneck:      {}", r.bottleneck);
    eprintln!(
        "throughput:      {}/s",
        human((r.bytes_received as f64 / r.elapsed.as_secs_f64().max(0.001)) as u64)
    );
    eprintln!(
        "connections:     final {}, peak {}; probe {:?}",
        s.final_target,
        s.max_target,
        s.probe_results
            .iter()
            .map(|(n, bps)| format!("{n}→{}/s", human(*bps)))
            .collect::<Vec<_>>()
    );
    eprintln!(
        "requests:        {} ({} retries, {} cancelled)",
        s.requests, s.retries, s.cancels
    );
    eprintln!(
        "stragglers:      {} split, {} hedged ({} duplicated)",
        s.steals,
        s.hedges,
        human(s.hedged_bytes)
    );
    eprintln!(
        "throttling:      {} responses, {} ms with all sources cooling down",
        s.throttles, s.throttled_ms
    );
    for (i, src) in r.sources.iter().enumerate() {
        let b = s.bytes_per_source.get(i).copied().unwrap_or(0);
        eprintln!("source {i}:        {src} ({})", human(b));
    }
    eprintln!(
        "bytes:           {} received, {} resumed, {} overhead",
        human(r.bytes_received),
        human(r.resumed_bytes),
        human(
            r.bytes_received
                .saturating_sub(r.size.saturating_sub(r.resumed_bytes))
        )
    );
    eprintln!(
        "seed:            {} (reproduce with --seed {})",
        r.seed, r.seed
    );
}

fn short_url(u: &str) -> String {
    u.rsplit('/').next().unwrap_or(u).chars().take(40).collect()
}

fn human(b: u64) -> String {
    const U: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < U.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{b} B")
    } else {
        format!("{v:.1} {}", U[i])
    }
}
