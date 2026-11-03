//! Fetching from peers, and verifying any source against the BLAKE3 tree.

use crate::proto::{self, MAX_CVS, MAX_NAME};
use crate::{ALPN, Ticket, p2p_err};
use bytes::{Bytes, BytesMut};
use iroh::endpoint::{Connection, RecvStream};
use iroh::{Endpoint, EndpointAddr};
use qget_core::b3tree::{GROUP_LEN, Outboard, group_range};
use qget_core::engine::{FailKind, FatalKind};
use qget_core::{BoxFuture, ByteRange, ChunkStream, Error, Result, Source};
use std::sync::Arc;
use tokio::sync::Mutex;

/// What a peer reports about its share, verified against the ticket's root.
pub struct PeerInfo {
    pub outboard: Outboard,
    pub name: String,
    pub dir: bool,
}

/// Connect to the ticket's peer and fetch the verified outboard.
pub async fn fetch_info(endpoint: &Endpoint, ticket: &Ticket) -> Result<(PeerInfo, Connection)> {
    let root = ticket.root_hash()?;
    let secret = ticket.secret_bytes()?;
    let conn = endpoint
        .connect(ticket.addr.clone(), ALPN)
        .await
        .map_err(|e| p2p_err(format!("could not reach peer: {e}")))?;
    let (mut send, mut recv) = conn.open_bi().await.map_err(p2p_err)?;
    send.write_all(&proto::info_request(&secret))
        .await
        .map_err(p2p_err)?;
    send.finish().map_err(p2p_err)?;
    let mut status = [0u8; 1];
    recv.read_exact(&mut status).await.map_err(p2p_err)?;
    if status[0] != proto::STATUS_OK {
        return Err(Error::P2p(format!(
            "peer refused the request (status {})",
            status[0]
        )));
    }
    let mut fixed = [0u8; 8 + 32 + 2];
    recv.read_exact(&mut fixed).await.map_err(p2p_err)?;
    let size = u64::from_le_bytes(fixed[..8].try_into().expect("8"));
    let peer_root: [u8; 32] = fixed[8..40].try_into().expect("32");
    let name_len = u16::from_le_bytes(fixed[40..42].try_into().expect("2")) as usize;
    if name_len > MAX_NAME {
        return Err(p2p_err("peer sent an oversized name"));
    }
    let mut name = vec![0u8; name_len];
    recv.read_exact(&mut name).await.map_err(p2p_err)?;
    let mut tail = [0u8; 5];
    recv.read_exact(&mut tail).await.map_err(p2p_err)?;
    let dir = tail[0] != 0;
    let n = u32::from_le_bytes(tail[1..5].try_into().expect("4"));
    if n > MAX_CVS {
        return Err(p2p_err("peer sent too many chaining values"));
    }
    let mut cvs = vec![0u8; n as usize * 32];
    recv.read_exact(&mut cvs).await.map_err(p2p_err)?;
    let cvs = Outboard::cvs_from_bytes(&cvs).ok_or_else(|| p2p_err("bad chaining values"))?;
    // The ticket is the trust anchor: everything must match its root and size.
    if peer_root != *root.as_bytes()
        || size != ticket.size
        || !Outboard::verify_cvs(&root, size, &cvs)
    {
        return Err(Error::Integrity {
            expected: ticket.expected().to_string(),
            actual: "peer advertised a different file".into(),
        });
    }
    Ok((
        PeerInfo {
            outboard: Outboard { size, root, cvs },
            name: String::from_utf8_lossy(&name).into_owned(),
            dir,
        },
        conn,
    ))
}

/// A peer serving the file, used as an engine source (requests are aligned
/// to `GROUP_LEN` by the engine).
pub struct PeerSource {
    endpoint: Endpoint,
    addr: EndpointAddr,
    secret: [u8; 16],
    outboard: Arc<Outboard>,
    conn: Arc<Mutex<Option<Connection>>>,
    label: String,
}

impl PeerSource {
    pub fn new(
        endpoint: Endpoint,
        ticket: &Ticket,
        outboard: Arc<Outboard>,
        conn: Option<Connection>,
    ) -> Result<Self> {
        Ok(Self {
            label: format!("peer {}", ticket.addr.id.fmt_short()),
            endpoint,
            addr: ticket.addr.clone(),
            secret: ticket.secret_bytes()?,
            outboard,
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

struct PeerStream {
    recv: RecvStream,
}

impl ChunkStream for PeerStream {
    fn next_chunk(&mut self) -> BoxFuture<'_, Option<std::result::Result<Bytes, FailKind>>> {
        Box::pin(async move {
            match self.recv.read_chunk(1 << 20).await {
                Ok(Some(c)) => Some(Ok(c)),
                Ok(None) => None,
                Err(e) => Some(Err(FailKind::Transient(e.to_string()))),
            }
        })
    }
}

impl Source for PeerSource {
    fn open(
        &self,
        range: ByteRange,
    ) -> BoxFuture<'static, std::result::Result<Box<dyn ChunkStream>, FailKind>> {
        let (ep, addr, secret, slot, outboard) = (
            self.endpoint.clone(),
            self.addr.clone(),
            self.secret,
            self.conn.clone(),
            self.outboard.clone(),
        );
        Box::pin(async move {
            let conn = {
                let mut guard = slot.lock().await;
                match guard.as_ref().filter(|c| c.close_reason().is_none()) {
                    Some(c) => c.clone(),
                    None => {
                        let c = ep
                            .connect(addr, ALPN)
                            .await
                            .map_err(|e| FailKind::Transient(format!("connect to peer: {e}")))?;
                        *guard = Some(c.clone());
                        c
                    }
                }
            };
            let (mut send, mut recv) = conn
                .open_bi()
                .await
                .map_err(|e| FailKind::Transient(e.to_string()))?;
            send.write_all(&proto::range_request(&secret, range.start, range.end))
                .await
                .map_err(|e| FailKind::Transient(e.to_string()))?;
            let _ = send.finish();
            let mut status = [0u8; 1];
            recv.read_exact(&mut status)
                .await
                .map_err(|e| FailKind::Transient(e.to_string()))?;
            match status[0] {
                proto::STATUS_OK => {}
                proto::STATUS_DENIED => {
                    return Err(FailKind::Fatal {
                        kind: FatalKind::Other,
                        msg: "peer denied access".into(),
                    });
                }
                s => return Err(FailKind::Transient(format!("peer error status {s}"))),
            }
            Ok(Box::new(VerifyingStream::new(
                Box::new(PeerStream { recv }),
                outboard,
                range,
            )?) as Box<dyn ChunkStream>)
        })
    }

    fn describe(&self) -> String {
        self.label.clone()
    }
}

/// Wraps any source so each group is verified before it is released
/// (hybrid HTTP + P2P downloads, P2P-07).
pub struct VerifiedSource {
    inner: Arc<dyn Source>,
    outboard: Arc<Outboard>,
}

impl VerifiedSource {
    pub fn new(inner: Arc<dyn Source>, outboard: Arc<Outboard>) -> Self {
        Self { inner, outboard }
    }
}

impl Source for VerifiedSource {
    fn open(
        &self,
        range: ByteRange,
    ) -> BoxFuture<'static, std::result::Result<Box<dyn ChunkStream>, FailKind>> {
        let fut = self.inner.open(range);
        let outboard = self.outboard.clone();
        Box::pin(async move {
            let inner = fut.await?;
            Ok(Box::new(VerifyingStream::new(inner, outboard, range)?) as Box<dyn ChunkStream>)
        })
    }

    fn describe(&self) -> String {
        format!("{} (verified)", self.inner.describe())
    }
}

/// Buffers one group at a time and releases it only if its chaining value
/// matches. A mismatch disables the source (FatalKind::Integrity).
pub struct VerifyingStream {
    inner: Box<dyn ChunkStream>,
    outboard: Arc<Outboard>,
    pos: u64,
    end: u64,
    buf: BytesMut,
}

impl VerifyingStream {
    fn new(
        inner: Box<dyn ChunkStream>,
        outboard: Arc<Outboard>,
        range: ByteRange,
    ) -> std::result::Result<Self, FailKind> {
        if !range.start.is_multiple_of(GROUP_LEN)
            || (!range.end.is_multiple_of(GROUP_LEN) && range.end != outboard.size)
        {
            return Err(FailKind::Fatal {
                kind: FatalKind::Other,
                msg: format!("unaligned verified range {range:?}"),
            });
        }
        Ok(Self {
            inner,
            outboard,
            pos: range.start,
            end: range.end,
            buf: BytesMut::new(),
        })
    }
}

impl ChunkStream for VerifyingStream {
    fn next_chunk(&mut self) -> BoxFuture<'_, Option<std::result::Result<Bytes, FailKind>>> {
        Box::pin(async move {
            if self.pos >= self.end {
                return None;
            }
            let index = self.pos / GROUP_LEN;
            let (gs, ge) = group_range(index, self.outboard.size);
            let want = (ge - gs) as usize;
            while self.buf.len() < want {
                match self.inner.next_chunk().await {
                    Some(Ok(c)) => self.buf.extend_from_slice(&c),
                    Some(Err(e)) => return Some(Err(e)),
                    None => return None, // runtime reports the short body
                }
            }
            let group = self.buf.split_to(want).freeze();
            if !self.outboard.verify_group(index, &group) {
                return Some(Err(FailKind::Fatal {
                    kind: FatalKind::Integrity,
                    msg: format!("group {index} failed BLAKE3 verification"),
                }));
            }
            self.pos = ge;
            Some(Ok(group))
        })
    }
}
