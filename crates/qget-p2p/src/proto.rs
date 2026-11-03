//! Wire protocol, one request per QUIC bidirectional stream.
//!
//! Request:  `[op: u8][secret: 16]` then op-specific fields.
//! - `OP_INFO` → `[status][size: u64][root: 32][name_len: u16][name][dir: u8][n: u32][cvs: 32·n]`
//! - `OP_RANGE [start: u64][end: u64]` → `[status]` then the raw bytes.
//!
//! All integers are little-endian.

pub const OP_INFO: u8 = 1;
pub const OP_RANGE: u8 = 2;

pub const STATUS_OK: u8 = 0;
pub const STATUS_DENIED: u8 = 1;
pub const STATUS_BAD_RANGE: u8 = 2;
pub const STATUS_ERROR: u8 = 3;

/// Largest name accepted in an info response.
pub const MAX_NAME: usize = 1024;
/// Upper bound on group CVs (1 MiB groups → 64 TiB files).
pub const MAX_CVS: u32 = 64 << 20;

pub fn range_request(secret: &[u8; 16], start: u64, end: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(33);
    v.push(OP_RANGE);
    v.extend_from_slice(secret);
    v.extend_from_slice(&start.to_le_bytes());
    v.extend_from_slice(&end.to_le_bytes());
    v
}

pub fn info_request(secret: &[u8; 16]) -> Vec<u8> {
    let mut v = Vec::with_capacity(17);
    v.push(OP_INFO);
    v.extend_from_slice(secret);
    v
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Info {
    pub size: u64,
    pub root: [u8; 32],
    pub name: String,
    pub dir: bool,
    pub cvs: Vec<u8>,
}

impl Info {
    pub fn encode(&self) -> Vec<u8> {
        let mut v = vec![STATUS_OK];
        v.extend_from_slice(&self.size.to_le_bytes());
        v.extend_from_slice(&self.root);
        let name = &self.name.as_bytes()[..self.name.len().min(MAX_NAME)];
        v.extend_from_slice(&(name.len() as u16).to_le_bytes());
        v.extend_from_slice(name);
        v.push(u8::from(self.dir));
        v.extend_from_slice(&((self.cvs.len() / 32) as u32).to_le_bytes());
        v.extend_from_slice(&self.cvs);
        v
    }
}
