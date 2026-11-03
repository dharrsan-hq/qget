//! A tiny rendezvous server for short codes (`qget mailbox`).
//!
//! Protocol (one TCP connection per party):
//! - sender:   `ALLOC\n`      → `OK <channel>\n` … later `PAIRED\n`
//! - receiver: `CLAIM <n>\n`  → `OK\n` (or `ERR …\n`)
//!
//! Once paired the server only relays bytes between the two parties; it
//! learns nothing useful because the exchange is authenticated with SPAKE2
//! and the ticket is encrypted under the PAKE key. A channel can be claimed
//! once, so each code allows a single password guess.

use crate::p2p_err;
use qget_core::Result;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

/// How long an allocated channel waits for its receiver.
pub const CHANNEL_TTL: Duration = Duration::from_secs(30 * 60);

type Waiting = Arc<Mutex<HashMap<u32, oneshot::Sender<TcpStream>>>>;

/// Run the mailbox until the listener fails.
pub async fn serve(listener: TcpListener) -> std::io::Result<()> {
    let waiting: Waiting = Arc::new(Mutex::new(HashMap::new()));
    loop {
        let (sock, peer) = listener.accept().await?;
        let w = waiting.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(sock, w).await {
                tracing::debug!("mailbox client {peer}: {e}");
            }
        });
    }
}

async fn handle(sock: TcpStream, waiting: Waiting) -> std::io::Result<()> {
    let mut reader = BufReader::new(sock);
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(30), reader.read_line(&mut line))
        .await
        .map_err(|_| std::io::Error::other("timeout"))??;
    if line.len() > 64 {
        return Ok(());
    }
    let mut sock = reader.into_inner();
    let mut parts = line.split_whitespace();
    match (parts.next(), parts.next()) {
        (Some("ALLOC"), None) => {
            let (tx, rx) = oneshot::channel();
            let channel = {
                let mut w = waiting.lock().expect("mailbox lock");
                let mut n;
                loop {
                    n = 1000
                        + crate::random_bytes::<4>()
                            .iter()
                            .fold(0u32, |a, b| a * 256 + *b as u32)
                            % 9000;
                    if !w.contains_key(&n) {
                        break;
                    }
                }
                w.insert(n, tx);
                n
            };
            sock.write_all(format!("OK {channel}\n").as_bytes()).await?;
            let other = tokio::time::timeout(CHANNEL_TTL, rx).await;
            waiting.lock().expect("mailbox lock").remove(&channel);
            let Ok(Ok(mut other)) = other else {
                return Ok(());
            };
            sock.write_all(b"PAIRED\n").await?;
            other.write_all(b"OK\n").await?;
            let _ = tokio::time::timeout(
                Duration::from_secs(120),
                tokio::io::copy_bidirectional(&mut sock, &mut other),
            )
            .await;
            Ok(())
        }
        (Some("CLAIM"), Some(n)) => {
            let tx = n
                .parse::<u32>()
                .ok()
                .and_then(|n| waiting.lock().expect("mailbox lock").remove(&n));
            match tx {
                Some(tx) => {
                    let _ = tx.send(sock);
                }
                None => {
                    sock.write_all(b"ERR no such code (expired or already used)\n")
                        .await?;
                }
            }
            Ok(())
        }
        _ => {
            sock.write_all(b"ERR bad request\n").await?;
            Ok(())
        }
    }
}

/// Client side: allocate a channel. Returns the channel and a future-ready
/// stream that becomes usable after `wait_paired`.
pub async fn allocate(mailbox: &str) -> Result<(u32, BufReader<TcpStream>)> {
    let sock = TcpStream::connect(mailbox)
        .await
        .map_err(|e| p2p_err(format!("mailbox {mailbox}: {e}")))?;
    let mut r = BufReader::new(sock);
    r.get_mut().write_all(b"ALLOC\n").await.map_err(p2p_err)?;
    let mut line = String::new();
    r.read_line(&mut line).await.map_err(p2p_err)?;
    let n = line
        .trim()
        .strip_prefix("OK ")
        .and_then(|n| n.parse().ok())
        .ok_or_else(|| p2p_err(format!("mailbox: {}", line.trim())))?;
    Ok((n, r))
}

pub async fn wait_paired(r: &mut BufReader<TcpStream>) -> Result<()> {
    let mut line = String::new();
    r.read_line(&mut line).await.map_err(p2p_err)?;
    if line.trim() == "PAIRED" {
        Ok(())
    } else {
        Err(p2p_err(format!("mailbox: {}", line.trim())))
    }
}

pub async fn claim(mailbox: &str, channel: u32) -> Result<BufReader<TcpStream>> {
    let sock = TcpStream::connect(mailbox)
        .await
        .map_err(|e| p2p_err(format!("mailbox {mailbox}: {e}")))?;
    let mut r = BufReader::new(sock);
    r.get_mut()
        .write_all(format!("CLAIM {channel}\n").as_bytes())
        .await
        .map_err(p2p_err)?;
    let mut line = String::new();
    r.read_line(&mut line).await.map_err(p2p_err)?;
    if line.trim() == "OK" {
        Ok(r)
    } else {
        Err(p2p_err(format!("mailbox: {}", line.trim())))
    }
}
