//! Expected checksums and incremental hashers (INT-01).

use serde::{Deserialize, Serialize};
use sha2::Digest;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HashAlgo {
    Sha256,
    Sha512,
    Blake3,
}

impl HashAlgo {
    pub fn name(self) -> &'static str {
        match self {
            HashAlgo::Sha256 => "sha256",
            HashAlgo::Sha512 => "sha512",
            HashAlgo::Blake3 => "blake3",
        }
    }

    fn hex_len(self) -> usize {
        match self {
            HashAlgo::Sha256 | HashAlgo::Blake3 => 64,
            HashAlgo::Sha512 => 128,
        }
    }
}

/// A checksum the downloaded bytes must match, e.g. `sha256:9f2c…`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedHash {
    pub algo: HashAlgo,
    /// Lowercase hex digest.
    pub hex: String,
}

impl ExpectedHash {
    /// Parse `algo:hex`. A bare 64-character hex string is treated as SHA-256
    /// and a bare 128-character one as SHA-512.
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        let (algo, hex) = match s.split_once(':') {
            Some((a, h)) => {
                let algo = match a.to_ascii_lowercase().replace('-', "").as_str() {
                    "sha256" => HashAlgo::Sha256,
                    "sha512" => HashAlgo::Sha512,
                    "blake3" | "b3" => HashAlgo::Blake3,
                    _ => return None,
                };
                (algo, h)
            }
            None => match s.len() {
                64 => (HashAlgo::Sha256, s),
                128 => (HashAlgo::Sha512, s),
                _ => return None,
            },
        };
        let hex = hex.to_ascii_lowercase();
        if hex.len() != algo.hex_len() || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        Some(Self { algo, hex })
    }

    pub fn from_bytes(algo: HashAlgo, digest: &[u8]) -> Self {
        Self {
            algo,
            hex: hex::encode(digest),
        }
    }
}

impl std::fmt::Display for ExpectedHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.algo.name(), self.hex)
    }
}

/// Incremental hasher over one of the supported algorithms.
#[derive(Clone)]
pub enum Hasher {
    Sha256(sha2::Sha256),
    Sha512(sha2::Sha512),
    Blake3(Box<blake3::Hasher>),
}

impl Hasher {
    pub fn new(algo: HashAlgo) -> Self {
        match algo {
            HashAlgo::Sha256 => Hasher::Sha256(sha2::Sha256::new()),
            HashAlgo::Sha512 => Hasher::Sha512(sha2::Sha512::new()),
            HashAlgo::Blake3 => Hasher::Blake3(Box::new(blake3::Hasher::new())),
        }
    }

    pub fn algo(&self) -> HashAlgo {
        match self {
            Hasher::Sha256(_) => HashAlgo::Sha256,
            Hasher::Sha512(_) => HashAlgo::Sha512,
            Hasher::Blake3(_) => HashAlgo::Blake3,
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        match self {
            Hasher::Sha256(h) => h.update(data),
            Hasher::Sha512(h) => h.update(data),
            Hasher::Blake3(h) => {
                // Large buffers benefit from blake3's SIMD tree hashing.
                h.update(data);
            }
        }
    }

    pub fn finalize(self) -> ExpectedHash {
        let algo = self.algo();
        let bytes: Vec<u8> = match self {
            Hasher::Sha256(h) => h.finalize().to_vec(),
            Hasher::Sha512(h) => h.finalize().to_vec(),
            Hasher::Blake3(h) => h.finalize().as_bytes().to_vec(),
        };
        ExpectedHash::from_bytes(algo, &bytes)
    }
}

/// Parse the first digest for `file_name` from a `sha256sum`-style listing
/// (`<hex>  <name>` or `<hex> *<name>` per line). If the listing has a single
/// line, its digest is used regardless of the name.
pub fn parse_sums_file(text: &str, file_name: &str, algo_hint: HashAlgo) -> Option<ExpectedHash> {
    let lines: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect();
    for line in &lines {
        let mut parts = line.split_whitespace();
        let (Some(digest), Some(name)) = (parts.next(), parts.next()) else {
            continue;
        };
        let name = name.trim_start_matches('*');
        let base = name.rsplit('/').next().unwrap_or(name);
        if base == file_name {
            return ExpectedHash::parse(&format!("{}:{}", algo_hint.name(), digest))
                .or_else(|| ExpectedHash::parse(digest));
        }
    }
    if lines.len() == 1 {
        let digest = lines[0].split_whitespace().next()?;
        return ExpectedHash::parse(&format!("{}:{}", algo_hint.name(), digest))
            .or_else(|| ExpectedHash::parse(digest));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_expected_hashes() {
        let h = ExpectedHash::parse(
            "SHA256:ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
        )
        .unwrap();
        assert_eq!(h.algo, HashAlgo::Sha256);
        assert!(h.hex.chars().all(|c| !c.is_ascii_uppercase()));
        assert!(ExpectedHash::parse("sha256:abc").is_none());
        assert!(ExpectedHash::parse("md5:d41d8cd98f00b204e9800998ecf8427e").is_none());
        assert_eq!(
            ExpectedHash::parse(&"a".repeat(128)).unwrap().algo,
            HashAlgo::Sha512
        );
    }

    #[test]
    fn hashers_match_known_vectors() {
        let mut h = Hasher::new(HashAlgo::Sha256);
        h.update(b"abc");
        assert_eq!(
            h.finalize().hex,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let mut h = Hasher::new(HashAlgo::Blake3);
        h.update(b"abc");
        assert_eq!(h.finalize().hex, blake3::hash(b"abc").to_hex().to_string());
    }

    #[test]
    fn parses_sums_files() {
        let text = "\
# comment
1111111111111111111111111111111111111111111111111111111111111111  other.tar
2222222222222222222222222222222222222222222222222222222222222222 *snap.tar.lz4
";
        assert_eq!(
            parse_sums_file(text, "snap.tar.lz4", HashAlgo::Sha256)
                .unwrap()
                .hex,
            "2".repeat(64)
        );
        assert!(parse_sums_file(text, "missing", HashAlgo::Sha256).is_none());
        let single = format!("{}\n", "3".repeat(64));
        assert_eq!(
            parse_sums_file(&single, "anything", HashAlgo::Sha256)
                .unwrap()
                .hex,
            "3".repeat(64)
        );
    }
}
