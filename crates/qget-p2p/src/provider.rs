//! Serving a file to ticket holders (P2P-01).

use crate::proto::{self, Info};
use crate::{Ticket, p2p_err, random_bytes};
use iroh::Endpoint;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use qget_core::b3tree::{Outboard, OutboardBuilder};
use qget_core::{Error, Result};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::JoinHandle;

/// A file prepared for sharing: its BLAKE3 outboard and capability secret.
pub struct Share {
    pub path: PathBuf,
    pub name: String,
    pub size: u64,
    pub outboard: Outboard,
    pub secret: [u8; 16],
    pub dir: bool,
}

impl Share {
    /// Hash `path` (blocking; call from `spawn_blocking` for large files).
    pub fn prepare(path: &Path, name: String, dir: bool) -> Result<Self> {
        let f = std::fs::File::open(path)
            .map_err(|e| Error::io(format!("open {}", path.display()), e))?;
        let size = f.metadata().map_err(|e| Error::io("stat", e))?.len();
        let mut b = OutboardBuilder::new(size);
        let mut buf = vec![0u8; 8 << 20];
        let mut off = 0u64;
        while off < size {
            let n = f.read_at(&mut buf, off).map_err(|e| Error::io("read", e))?;
            if n == 0 {
                return Err(Error::Io {
                    context: format!("{} shrank while hashing", path.display()),
                    source: std::io::ErrorKind::UnexpectedEof.into(),
                });
            }
            b.update(&buf[..n]);
            off += n as u64;
        }
        Ok(Self {
            path: path.to_path_buf(),
            name,
            size,
            outboard: b.finish(),
            secret: random_bytes(),
            dir,
        })
    }
}

pub struct Provider {
    endpoint: Endpoint,
    share: Arc<Share>,
    served: Arc<AtomicU64>,
    task: JoinHandle<()>,
}

impl Provider {
    pub fn start(endpoint: Endpoint, share: Share) -> Self {
        let share = Arc::new(share);
        let served = Arc::new(AtomicU64::new(0));
        let (ep, sh, sv) = (endpoint.clone(), share.clone(), served.clone());
        let task = tokio::spawn(async move {
            while let Some(incoming) = ep.accept().await {
                let (sh, sv) = (sh.clone(), sv.clone());
                tokio::spawn(async move {
                    let Ok(conn) = incoming.await else { return };
                    serve_connection(conn, sh, sv).await;
                });
            }
        });
        Self {
            endpoint,
            share,
            served,
            task,
        }
    }

    pub fn ticket(&self, addr: iroh::EndpointAddr) -> Ticket {
        Ticket {
            addr,
            root: self.share.outboard.root.to_hex().to_string(),
            size: self.share.size,
            name: self.share.name.clone(),
            secret: hex::encode(self.share.secret),
            dir: self.share.dir,
        }
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Bytes of file content sent so far.
    pub fn served(&self) -> u64 {
        self.served.load(Ordering::Relaxed)
    }

    pub async fn shutdown(self) {
        self.task.abort();
        self.endpoint.close().await;
    }
}

async fn serve_connection(conn: Connection, share: Arc<Share>, served: Arc<AtomicU64>) {
    while let Ok((send, recv)) = conn.accept_bi().await {
        let (sh, sv) = (share.clone(), served.clone());
        tokio::spawn(async move {
            if let Err(e) = serve_stream(send, recv, sh, sv).await {
                tracing::debug!("peer stream ended: {e}");
            }
        });
    }
}

async fn serve_stream(
    mut send: SendStream,
    mut recv: RecvStream,
    share: Arc<Share>,
    served: Arc<AtomicU64>,
) -> Result<()> {
    let mut head = [0u8; 17];
    recv.read_exact(&mut head).await.map_err(p2p_err)?;
    // Constant-time comparison of the capability secret.
    let ok = head[1..17]
        .iter()
        .zip(share.secret.iter())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0;
    if !ok {
        let _ = send.write_all(&[proto::STATUS_DENIED]).await;
        let _ = send.finish();
        return Err(p2p_err("request with wrong secret"));
    }
    match head[0] {
        proto::OP_INFO => {
            let info = Info {
                size: share.size,
                root: *share.outboard.root.as_bytes(),
                name: share.name.clone(),
                dir: share.dir,
                cvs: share.outboard.cvs_bytes(),
            };
            send.write_all(&info.encode()).await.map_err(p2p_err)?;
        }
        proto::OP_RANGE => {
            let mut r = [0u8; 16];
            recv.read_exact(&mut r).await.map_err(p2p_err)?;
            let start = u64::from_le_bytes(r[..8].try_into().expect("8 bytes"));
            let end = u64::from_le_bytes(r[8..].try_into().expect("8 bytes"));
            if start >= end || end > share.size {
                send.write_all(&[proto::STATUS_BAD_RANGE])
                    .await
                    .map_err(p2p_err)?;
                let _ = send.finish();
                return Ok(());
            }
            send.write_all(&[proto::STATUS_OK]).await.map_err(p2p_err)?;
            let file = Arc::new(
                std::fs::File::open(&share.path).map_err(|e| Error::io("open shared file", e))?,
            );
            let mut off = start;
            while off < end {
                let n = (end - off).min(1 << 20) as usize;
                let f = file.clone();
                let buf = tokio::task::spawn_blocking(move || {
                    let mut b = vec![0u8; n];
                    f.read_exact_at(&mut b, off).map(|_| b)
                })
                .await
                .map_err(p2p_err)?
                .map_err(|e| Error::io("read shared file", e))?;
                send.write_all(&buf).await.map_err(p2p_err)?;
                served.fetch_add(n as u64, Ordering::Relaxed);
                off += n as u64;
            }
        }
        _ => {
            send.write_all(&[proto::STATUS_ERROR])
                .await
                .map_err(p2p_err)?;
        }
    }
    send.finish().map_err(p2p_err)?;
    // Let the receiver read everything before the stream is dropped.
    let _ = send.stopped().await;
    Ok(())
}
