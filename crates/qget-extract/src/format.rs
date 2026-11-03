//! Archive format detection from magic bytes (EXT-01).

use qget_core::{Error, Result};
use std::io::Read;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Tar,
    TarLz4,
    TarZstd,
    TarGzip,
    TarXz,
}

impl Format {
    pub fn name(self) -> &'static str {
        match self {
            Format::Tar => "tar",
            Format::TarLz4 => "tar.lz4",
            Format::TarZstd => "tar.zst",
            Format::TarGzip => "tar.gz",
            Format::TarXz => "tar.xz",
        }
    }

    pub fn from_name(s: &str) -> Option<Self> {
        Some(match s.trim_start_matches('.') {
            "tar" => Format::Tar,
            "lz4" | "tar.lz4" => Format::TarLz4,
            "zst" | "zstd" | "tar.zst" | "tar.zstd" => Format::TarZstd,
            "gz" | "gzip" | "tgz" | "tar.gz" => Format::TarGzip,
            "xz" | "txz" | "tar.xz" => Format::TarXz,
            _ => return None,
        })
    }
}

const LZ4_MAGIC: u32 = 0x184D_2204;
const ZSTD_MAGIC: u32 = 0xFD2F_B528;

fn is_skippable(m: u32) -> bool {
    m & 0xFFFF_FFF0 == 0x184D_2A50
}

/// Identify the format of an archive from its first bytes.
pub fn detect(head: &[u8]) -> Option<Format> {
    let mut off = 0usize;
    // Skippable frames (pzstd, seekable index) may precede the first real frame.
    while head.len() >= off + 8 {
        let m = u32::from_le_bytes(head[off..off + 4].try_into().ok()?);
        if !is_skippable(m) {
            break;
        }
        let size = u32::from_le_bytes(head[off + 4..off + 8].try_into().ok()?) as usize;
        off += 8 + size;
    }
    if head.len() >= off + 4 {
        match u32::from_le_bytes(head[off..off + 4].try_into().ok()?) {
            LZ4_MAGIC => return Some(Format::TarLz4),
            ZSTD_MAGIC => return Some(Format::TarZstd),
            _ => {}
        }
    }
    if head.starts_with(&[0x1F, 0x8B]) {
        return Some(Format::TarGzip);
    }
    if head.starts_with(&[0xFD, b'7', b'z', b'X', b'Z', 0x00]) {
        return Some(Format::TarXz);
    }
    if head.len() >= 512 && (&head[257..262] == b"ustar" || tar_checksum_ok(&head[..512])) {
        return Some(Format::Tar);
    }
    None
}

fn tar_checksum_ok(h: &[u8]) -> bool {
    let field = std::str::from_utf8(&h[148..156]).unwrap_or("");
    let Ok(expected) = u32::from_str_radix(field.trim_matches(|c: char| c == '\0' || c == ' '), 8)
    else {
        return false;
    };
    let sum: u32 = h
        .iter()
        .enumerate()
        .map(|(i, &b)| {
            if (148..156).contains(&i) {
                b' ' as u32
            } else {
                b as u32
            }
        })
        .sum();
    sum == expected && h.iter().any(|&b| b != 0)
}

/// Buffers the first bytes of a stream so they can be inspected and then
/// replayed to the decoder.
pub struct Probe<R> {
    inner: R,
    pub(crate) head: Vec<u8>,
    pos: usize,
}

impl<R: Read> Probe<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            head: Vec::new(),
            pos: 0,
        }
    }

    /// Read up to `n` bytes into the probe buffer (fewer at EOF).
    pub fn fill(&mut self, n: usize) -> std::io::Result<&[u8]> {
        while self.head.len() < n {
            let mut buf = vec![0u8; n - self.head.len()];
            let got = self.inner.read(&mut buf)?;
            if got == 0 {
                break;
            }
            self.head.extend_from_slice(&buf[..got]);
        }
        Ok(&self.head)
    }

    pub fn detect(&mut self) -> Result<Format> {
        let head = self
            .fill(64 * 1024)
            .map_err(|e| Error::Extract(e.to_string()))?
            .to_vec();
        detect(&head).ok_or_else(|| {
            Error::Unsupported(
                "unrecognised archive format (expected tar, tar.lz4, tar.zst, tar.gz or tar.xz)"
                    .into(),
            )
        })
    }
}

impl<R: Read> Read for Probe<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos < self.head.len() {
            let n = buf.len().min(self.head.len() - self.pos);
            buf[..n].copy_from_slice(&self.head[self.pos..self.pos + n]);
            self.pos += n;
            return Ok(n);
        }
        self.inner.read(buf)
    }
}
