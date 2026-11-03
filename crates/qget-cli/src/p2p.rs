//! Peer-to-peer subcommands (feature `p2p`).

use crate::report_error;
use clap::Args;
use qget::Output;
use qget::p2p::{self, P2pNet, P2pTicket, Receive, RelayChoice};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Args, Debug)]
pub struct NetArgs {
    /// Use these relay server(s) instead of the public n0 relays (repeatable).
    #[arg(long = "relay", value_name = "URL")]
    relays: Vec<String>,
    /// Direct connections only (same LAN or reachable addresses), no relay.
    #[arg(long, conflicts_with = "relays")]
    no_relay: bool,
    /// Local address to bind, e.g. 0.0.0.0:7000.
    #[arg(long, value_name = "ADDR")]
    bind: Option<std::net::SocketAddr>,
}

impl NetArgs {
    fn config(&self) -> P2pNet {
        let mut n = P2pNet::default();
        if self.no_relay {
            n.relay = RelayChoice::Disabled;
        } else if !self.relays.is_empty() {
            n.relay = RelayChoice::Custom(self.relays.clone());
        }
        n.bind = self.bind;
        n
    }
}

#[derive(Args, Debug)]
pub struct YeetArgs {
    /// File or directory to send.
    path: PathBuf,
    /// Mailbox for short codes (host:port). Without it only a ticket is printed.
    #[arg(long, env = "QGET_MAILBOX", value_name = "HOST:PORT")]
    mailbox: Option<String>,
    /// Exit after one full copy has been served.
    #[arg(long)]
    once: bool,
    #[command(flatten)]
    net: NetArgs,
}

#[derive(Args, Debug)]
pub struct YoinkArgs {
    /// Short code (4217-maple-orbit-canyon) or qget1… ticket. Several tickets
    /// for the same content download from all peers at once.
    #[arg(required = true, value_name = "CODE|TICKET")]
    from: Vec<String>,
    /// Write to FILE (or, for directory shares, extract into DIR).
    #[arg(short = 'O', long, value_name = "PATH")]
    output: Option<PathBuf>,
    /// Save under DIR.
    #[arg(short = 'P', long = "directory-prefix", value_name = "DIR")]
    dir: Option<PathBuf>,
    /// Also fetch from this HTTP(S) mirror, verifying every byte (repeatable).
    #[arg(long = "mirror", value_name = "URL")]
    mirrors: Vec<String>,
    #[arg(long, env = "QGET_MAILBOX", value_name = "HOST:PORT")]
    mailbox: Option<String>,
    /// After receiving, keep serving the file to others.
    #[arg(long)]
    seed: bool,
    /// Discard any partial download.
    #[arg(long)]
    restart: bool,
    #[arg(short, long)]
    quiet: bool,
    #[command(flatten)]
    net: NetArgs,
}

#[derive(Args, Debug)]
pub struct MailboxArgs {
    /// Address to listen on.
    #[arg(long, default_value = "0.0.0.0:7700")]
    listen: String,
}

pub async fn yeet(a: YeetArgs) -> i32 {
    let net = a.net.config();
    let mut s = match p2p::send(&a.path, &net, a.mailbox.as_deref()).await {
        Ok(s) => s,
        Err(e) => return report_error(&e),
    };
    let kind = if s.ticket.dir { "directory" } else { "file" };
    eprintln!(
        "qget: sharing {kind} {} ({} bytes)",
        s.ticket.name, s.ticket.size
    );
    eprintln!("qget: blake3:{}", s.ticket.root);
    if let Some(code) = &s.code {
        eprintln!("qget: on the other machine run:\n\n    qget yoink {code}\n");
    }
    eprintln!(
        "qget: ticket (works without a mailbox):\n\n    qget yoink {}\n",
        s.ticket.encode()
    );
    eprintln!(
        "qget: serving until Ctrl+C{}",
        if a.once { " or one full copy" } else { "" }
    );
    let size = s.ticket.size;
    let has_code = s.code.is_some();
    let mut claimed = !has_code;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            r = s.code_claimed(), if !claimed => {
                claimed = true;
                if let Err(e) = r {
                    eprintln!("qget: code exchange failed: {e}");
                    if !a.once { continue }
                    s.shutdown().await;
                    return e.exit_code();
                }
                eprintln!("qget: code accepted by receiver");
            }
            _ = tokio::time::sleep(Duration::from_millis(500)) => {
                if a.once && s.served() >= size.max(1) {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    break;
                }
            }
        }
    }
    eprintln!("qget: served {} bytes", s.served());
    s.shutdown().await;
    0
}

pub async fn yoink(a: YoinkArgs) -> i32 {
    let mut tickets = Vec::new();
    for f in &a.from {
        let t = if f.starts_with("qget1") {
            P2pTicket::decode(f)
        } else {
            match &a.mailbox {
                Some(m) => p2p::resolve_code(m, f).await,
                None => Err(qget::Error::Usage(
                    "short codes need --mailbox or QGET_MAILBOX (or pass the qget1… ticket)".into(),
                )),
            }
        };
        match t {
            Ok(t) => tickets.push(t),
            Err(e) => return report_error(&e),
        }
    }
    let mut r = Receive::new(tickets[0].clone())
        .net(a.net.config())
        .restart(a.restart)
        .keep_tar(a.seed);
    for t in &tickets[1..] {
        r = r.peer(t.clone());
    }
    for m in &a.mirrors {
        r = r.mirror(m.clone());
    }
    r = r.output(match (&a.output, &a.dir) {
        (Some(o), Some(d)) => Output::File(d.join(o)),
        (Some(o), None) => Output::File(o.clone()),
        (None, Some(d)) => Output::Dir(d.clone()),
        (None, None) => Output::Auto,
    });
    let token = qget::CancellationToken::new();
    let t2 = token.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            t2.cancel();
        }
    });
    r = r.cancel_token(token);
    if !a.quiet {
        let pb = indicatif::ProgressBar::new(tickets[0].size);
        pb.set_style(
            indicatif::ProgressStyle::with_template("{spinner:.cyan} [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} {binary_bytes_per_sec}")
                .expect("template")
                .progress_chars("=> "),
        );
        r = r.on_progress(move |p| pb.set_position(p.downloaded));
    }
    match r.run().await {
        Ok(rep) => {
            let out = rep.output.clone().unwrap_or_default();
            if !a.quiet {
                eprintln!(
                    "qget: received {} → {} in {:.1}s, verified blake3:{}",
                    rep.size,
                    out.display(),
                    rep.elapsed.as_secs_f64(),
                    tickets[0].root
                );
            }
            if a.seed {
                let file = if tickets[0].dir {
                    let mut p = out.as_os_str().to_owned();
                    p.push(".qget-recv.tar");
                    PathBuf::from(p)
                } else {
                    out
                };
                match p2p::seed(&file, &tickets[0].name, &a.net.config()).await {
                    Ok(s) => {
                        eprintln!(
                            "qget: seeding; others can join with:\n\n    qget yoink {} {}\n",
                            tickets[0].encode(),
                            s.ticket.encode()
                        );
                        let _ = tokio::signal::ctrl_c().await;
                        s.shutdown().await;
                    }
                    Err(e) => return report_error(&e),
                }
            }
            0
        }
        Err(e) => report_error(&e),
    }
}

pub async fn mailbox(a: MailboxArgs) -> i32 {
    eprintln!("qget: mailbox listening on {}", a.listen);
    match p2p::run_mailbox(&a.listen).await {
        Ok(()) => 0,
        Err(e) => report_error(&e),
    }
}
