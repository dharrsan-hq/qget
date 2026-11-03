//! Short codes: SPAKE2 over the mailbox, then the ticket encrypted under the
//! derived key (P2P-01, P2P-02, SEC-04).
//!
//! Code format: `<channel>-<word>-<word>-<word>`. The channel only locates the
//! mailbox slot; the three words (24 bits) are the PAKE password. The PAKE is
//! bound to the channel through its identity string, and each channel can be
//! claimed once, so an attacker gets one online guess per code.

use crate::words::WORDS;
use crate::{Ticket, mailbox, p2p_err, random_bytes};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Nonce};
use hmac::{Hmac, Mac};
use qget_core::{Error, Result};
use sha2::Sha256;
use spake2::{Ed25519Group, Identity, Password, Spake2};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAX_MSG: u32 = 64 * 1024;

/// A parsed short code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Code {
    pub channel: u32,
    pub words: [String; 3],
}

impl Code {
    pub fn generate(channel: u32) -> Self {
        let r: [u8; 3] = random_bytes();
        Self {
            channel,
            words: r.map(|b| WORDS[b as usize].to_string()),
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        let mut it = s.trim().split('-');
        let channel = it.next()?.parse().ok()?;
        let words: Vec<String> = it.map(|w| w.to_ascii_lowercase()).collect();
        if words.len() != 3 || !words.iter().all(|w| WORDS.contains(&w.as_str())) {
            return None;
        }
        Some(Self {
            channel,
            words: [words[0].clone(), words[1].clone(), words[2].clone()],
        })
    }

    fn password(&self) -> Password {
        Password::new(self.words.join("-"))
    }

    fn identity(&self) -> Identity {
        Identity::new(format!("qget/1 mailbox channel {}", self.channel).as_bytes())
    }
}

impl std::fmt::Display for Code {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.channel, self.words.join("-"))
    }
}

async fn send_msg<W: AsyncWrite + Unpin>(w: &mut W, m: &[u8]) -> Result<()> {
    w.write_all(&(m.len() as u32).to_le_bytes())
        .await
        .map_err(p2p_err)?;
    w.write_all(m).await.map_err(p2p_err)?;
    w.flush().await.map_err(p2p_err)
}

async fn recv_msg<R: AsyncRead + Unpin>(r: &mut R) -> Result<Vec<u8>> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len)
        .await
        .map_err(|_| p2p_err("peer disconnected during code exchange"))?;
    let len = u32::from_le_bytes(len);
    if len > MAX_MSG {
        return Err(p2p_err("oversized code-exchange message"));
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf).await.map_err(p2p_err)?;
    Ok(buf)
}

fn subkey(key: &[u8], label: &str) -> [u8; 32] {
    let mut m = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("any key length");
    m.update(label.as_bytes());
    m.finalize().into_bytes().into()
}

fn wrong_code() -> Error {
    Error::P2p(
        "code did not match (typo, or someone else tried it); ask the sender for a new code".into(),
    )
}

/// Run the PAKE and key confirmation on an established pipe.
async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut S,
    code: &Code,
    sender: bool,
) -> Result<[u8; 32]> {
    let (state, msg) = Spake2::<Ed25519Group>::start_symmetric(&code.password(), &code.identity());
    send_msg(s, &msg).await?;
    let peer = recv_msg(s).await?;
    let key = state.finish(&peer).map_err(|_| wrong_code())?;
    let (mine, theirs) = if sender {
        ("qget confirm sender", "qget confirm receiver")
    } else {
        ("qget confirm receiver", "qget confirm sender")
    };
    send_msg(s, &subkey(&key, mine)).await?;
    let got = recv_msg(s).await?;
    let mut m = <Hmac<Sha256> as Mac>::new_from_slice(&key).expect("any key length");
    m.update(theirs.as_bytes());
    m.verify_slice(&got).map_err(|_| wrong_code())?;
    Ok(subkey(&key, "qget ticket key"))
}

/// Sender: allocate a code. Returns it immediately along with a future that
/// completes the exchange once a receiver claims it.
pub async fn offer(
    mailbox_addr: &str,
    ticket: Ticket,
) -> Result<(Code, qget_core::BoxFuture<'static, Result<()>>)> {
    let (channel, mut stream) = mailbox::allocate(mailbox_addr).await?;
    let code = Code::generate(channel);
    let c2 = code.clone();
    let fut = Box::pin(async move {
        mailbox::wait_paired(&mut stream).await?;
        let key = exchange(&mut stream, &c2, true).await?;
        let cipher = ChaCha20Poly1305::new(&key.into());
        let nonce: [u8; 12] = random_bytes();
        let ct = cipher
            .encrypt(Nonce::from_slice(&nonce), ticket.encode().as_bytes())
            .map_err(|_| p2p_err("encrypt ticket"))?;
        let mut msg = nonce.to_vec();
        msg.extend(ct);
        send_msg(&mut stream, &msg).await?;
        Ok(())
    });
    Ok((code, fut))
}

/// Receiver: claim a code and obtain the sender's ticket.
pub async fn accept(mailbox_addr: &str, code: &Code) -> Result<Ticket> {
    let mut stream = mailbox::claim(mailbox_addr, code.channel).await?;
    let key = exchange(&mut stream, code, false).await?;
    let msg = recv_msg(&mut stream).await?;
    if msg.len() < 12 {
        return Err(p2p_err("short ticket message"));
    }
    let cipher = ChaCha20Poly1305::new(&key.into());
    let pt = cipher
        .decrypt(Nonce::from_slice(&msg[..12]), &msg[12..])
        .map_err(|_| p2p_err("ticket failed authentication"))?;
    Ticket::decode(&String::from_utf8_lossy(&pt))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_roundtrip() {
        let c = Code::generate(4217);
        let s = c.to_string();
        assert_eq!(Code::parse(&s), Some(c));
        assert_eq!(Code::parse("12-a-b"), None);
        assert_eq!(Code::parse("12-notaword-maple-orbit"), None);
        assert_eq!(Code::parse("x-maple-orbit-canyon"), None);
        assert!(Code::parse("12-Maple-orbit-canyon").is_some());
    }
}
