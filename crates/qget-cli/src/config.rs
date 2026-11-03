//! Optional config file (CLI-07). Keys mirror the long flag names; command
//! line flags always win.

use crate::cli::{ProgressArg, VerifyArg};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct Config {
    pub max_connections: Option<u32>,
    pub polite: Option<bool>,
    pub user_agent: Option<String>,
    pub limit_rate: Option<String>,
    pub max_buffer: Option<String>,
    pub progress: Option<ProgressArg>,
    pub verify: Option<VerifyArg>,
    pub timeout: Option<u64>,
    pub tries: Option<u32>,
    pub proxy: Option<String>,
    #[serde(default)]
    pub headers: Vec<String>,
    pub http2: Option<bool>,
}

pub fn default_path() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))?;
    Some(base.join("qget/config.toml"))
}

/// Load `explicit` (must exist) or the default path (may be absent).
pub fn load(explicit: Option<&Path>) -> Result<Config, String> {
    let (path, required) = match explicit {
        Some(p) => (p.to_path_buf(), true),
        None => match default_path() {
            Some(p) => (p, false),
            None => return Ok(Config::default()),
        },
    };
    match std::fs::read_to_string(&path) {
        Ok(text) => toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display())),
        Err(e) if !required && e.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_config() {
        let c: Config = toml::from_str(
            "max-connections = 8\npolite = true\nprogress = \"json\"\nheaders = [\"X-A: b\"]\n",
        )
        .unwrap();
        assert_eq!(c.max_connections, Some(8));
        assert_eq!(c.progress, Some(ProgressArg::Json));
        assert!(toml::from_str::<Config>("unknown = 1").is_err());
    }
}
