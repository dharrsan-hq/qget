//! Verified peer-to-peer transfer over iroh (§12).
//!
//! - [`net`]: endpoint setup (n0 relays + hole punching, custom relays, or local-only)
//! - [`ticket`]: self-contained `qget1…` tickets (address, BLAKE3 root, capability secret)
//! - [`provider`]: serves one file to ticket holders
//! - [`source`]: a [`qget_core::Source`] that fetches ranges from a peer and
//!   verifies every 1 MiB group against the BLAKE3 tree before releasing it;
//!   [`source::VerifiedSource`] applies the same check to HTTP mirrors (hybrid)
//! - [`pake`] + [`mailbox`]: short codes like `4217-maple-orbit-canyon` via a
//!   rendezvous mailbox, authenticated with SPAKE2
//!
//! Security model: transport encryption and peer authentication come from
//! iroh's QUIC/TLS with raw public keys. Ticket holders prove access with a
//! random capability secret. SPAKE2 (unaudited crate) is confined to
//! authenticating a short exchange that delivers the ticket, encrypted under
//! the PAKE key (§12.6).

pub mod mailbox;
pub mod net;
pub mod pake;
pub mod proto;
pub mod provider;
pub mod source;
pub mod ticket;
mod words;

pub use net::{NetConfig, RelayChoice};
pub use provider::{Provider, Share};
pub use source::{PeerSource, VerifiedSource, fetch_info};
pub use ticket::Ticket;

/// ALPN of the qget peer protocol.
pub const ALPN: &[u8] = b"qget/1";

pub(crate) fn random_bytes<const N: usize>() -> [u8; N] {
    let mut b = [0u8; N];
    getrandom::fill(&mut b).expect("OS randomness");
    b
}

pub(crate) fn p2p_err(e: impl std::fmt::Display) -> qget_core::Error {
    qget_core::Error::P2p(e.to_string())
}
