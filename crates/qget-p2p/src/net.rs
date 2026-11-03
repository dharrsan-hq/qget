//! iroh endpoint setup.

use crate::{ALPN, p2p_err};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, RelayMap, RelayMode, RelayUrl, TransportAddr};
use qget_core::Result;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum RelayChoice {
    /// number 0's public relays and DNS address lookup.
    #[default]
    Default,
    /// Self-hosted relay(s) (P2P-09).
    Custom(Vec<String>),
    /// Direct connections only, no relay, no address lookup (LANs and tests).
    Disabled,
}

#[derive(Clone, Debug, Default)]
pub struct NetConfig {
    pub relay: RelayChoice,
    /// Bind to this address instead of all interfaces.
    pub bind: Option<SocketAddr>,
}

impl NetConfig {
    /// Loopback-only configuration, used by tests.
    pub fn local() -> Self {
        Self {
            relay: RelayChoice::Disabled,
            bind: Some("127.0.0.1:0".parse().expect("valid")),
        }
    }
}

/// Bind an endpoint. `serve` makes it accept qget connections.
pub async fn bind(cfg: &NetConfig, serve: bool) -> Result<Endpoint> {
    let mut b = match &cfg.relay {
        RelayChoice::Default => Endpoint::builder(presets::N0),
        RelayChoice::Custom(urls) => {
            let urls = urls
                .iter()
                .map(|u| RelayUrl::from_str(u).map_err(|e| p2p_err(format!("relay URL {u}: {e}"))))
                .collect::<Result<Vec<_>>>()?;
            Endpoint::builder(presets::N0).relay_mode(RelayMode::Custom(RelayMap::from_iter(urls)))
        }
        RelayChoice::Disabled => {
            Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled)
        }
    };
    if let Some(addr) = cfg.bind {
        b = b.bind_addr(addr).map_err(p2p_err)?;
    }
    if serve {
        b = b.alpns(vec![ALPN.to_vec()]);
    }
    b.bind().await.map_err(p2p_err)
}

/// The address to publish in a ticket. With relays, waits briefly until the
/// endpoint has a home relay so peers behind NAT can reach it.
pub async fn public_addr(ep: &Endpoint, cfg: &NetConfig) -> EndpointAddr {
    if cfg.relay == RelayChoice::Disabled {
        let addrs = ep.bound_sockets().into_iter().map(TransportAddr::Ip);
        return EndpointAddr::from_parts(ep.id(), addrs);
    }
    let _ = tokio::time::timeout(Duration::from_secs(10), ep.online()).await;
    ep.addr()
}
