//! `qget1…` tickets: everything a receiver needs, in one copy-pasteable string.

use base64::Engine as _;
use iroh::EndpointAddr;
use qget_core::{Error, ExpectedHash, HashAlgo, Result};
use serde::{Deserialize, Serialize};

const PREFIX: &str = "qget1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ticket {
    pub addr: EndpointAddr,
    /// BLAKE3 root of the shared bytes (identical to `b3sum` output).
    pub root: String,
    pub size: u64,
    pub name: String,
    /// Capability secret proving the holder was given this ticket.
    pub secret: String,
    /// The share is a tar of a directory, to be extracted on receipt.
    #[serde(default)]
    pub dir: bool,
}

impl Ticket {
    pub fn encode(&self) -> String {
        let json = serde_json::to_vec(self).expect("serialisable");
        format!(
            "{PREFIX}{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
        )
    }

    pub fn decode(s: &str) -> Result<Self> {
        let body = s
            .trim()
            .strip_prefix(PREFIX)
            .ok_or_else(|| Error::Usage("not a qget ticket (expected qget1…)".into()))?;
        let json = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(body)
            .map_err(|_| Error::Usage("corrupt ticket".into()))?;
        let t: Ticket = serde_json::from_slice(&json)
            .map_err(|e| Error::Usage(format!("corrupt ticket: {e}")))?;
        t.root_hash()?;
        t.secret_bytes()?;
        Ok(t)
    }

    pub fn root_hash(&self) -> Result<blake3::Hash> {
        blake3::Hash::from_hex(&self.root)
            .map_err(|_| Error::Usage("ticket has an invalid root hash".into()))
    }

    pub fn secret_bytes(&self) -> Result<[u8; 16]> {
        let v = hex::decode(&self.secret)
            .map_err(|_| Error::Usage("ticket has an invalid secret".into()))?;
        v.try_into()
            .map_err(|_| Error::Usage("ticket has an invalid secret".into()))
    }

    pub fn expected(&self) -> ExpectedHash {
        ExpectedHash {
            algo: HashAlgo::Blake3,
            hex: self.root.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let key = iroh::SecretKey::generate();
        let t = Ticket {
            addr: EndpointAddr::new(key.public()).with_ip_addr("127.0.0.1:4433".parse().unwrap()),
            root: blake3::hash(b"x").to_hex().to_string(),
            size: 1,
            name: "x".into(),
            secret: hex::encode([7u8; 16]),
            dir: false,
        };
        let s = t.encode();
        assert!(s.starts_with("qget1"));
        assert_eq!(Ticket::decode(&s).unwrap(), t);
        assert!(Ticket::decode("qget1!!!").is_err());
        assert!(Ticket::decode("http://x").is_err());
    }
}
