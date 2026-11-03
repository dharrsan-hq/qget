//! Verified chunk groups on top of the standard BLAKE3 tree (INT-05).
//!
//! A file is split into fixed-size groups (`GROUP_LEN`, a power-of-two number
//! of BLAKE3 chunks). The sender publishes the chaining value (CV) of every
//! group. A receiver checks that the CV list merges to the file's ordinary
//! BLAKE3 root hash (the same value `b3sum` prints), then verifies each group
//! it receives, from any peer or mirror, against its CV before writing it.

use blake3::hazmat::{
    ChainingValue, HasherExt, Mode, merge_subtrees_non_root, merge_subtrees_root,
};

/// Size of a verified group: 1 MiB (1024 BLAKE3 chunks).
pub const GROUP_LEN: u64 = 1 << 20;

/// Number of groups for a file of `size` bytes.
pub fn group_count(size: u64) -> u64 {
    size.div_ceil(GROUP_LEN).max(1)
}

/// Byte range of group `index` in a file of `size` bytes.
pub fn group_range(index: u64, size: u64) -> (u64, u64) {
    let start = index * GROUP_LEN;
    (start, (start + GROUP_LEN).min(size))
}

/// Chaining value of one group. `data` must be the full group (only the last
/// group of a file may be shorter). Not meaningful for single-group files,
/// whose identity is the root hash itself.
pub fn group_cv(index: u64, data: &[u8]) -> ChainingValue {
    blake3::Hasher::new()
        .set_input_offset(index * GROUP_LEN)
        .update(data)
        .finalize_non_root()
}

/// Merge group CVs to the root hash. Requires at least two CVs.
pub fn root_from_cvs(cvs: &[ChainingValue]) -> Option<blake3::Hash> {
    if cvs.len() < 2 {
        return None;
    }
    let mut layer: Vec<ChainingValue> = cvs.to_vec();
    while layer.len() > 2 {
        let n = layer.len();
        let mut next = Vec::with_capacity(n.div_ceil(2));
        for pair in layer.chunks(2) {
            match pair {
                [l, r] => next.push(merge_subtrees_non_root(l, r, Mode::Hash)),
                [odd] => next.push(*odd),
                _ => unreachable!(),
            }
        }
        layer = next;
    }
    Some(merge_subtrees_root(&layer[0], &layer[1], Mode::Hash))
}

/// The verification outboard for a file: its root hash and per-group CVs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outboard {
    pub size: u64,
    pub root: blake3::Hash,
    /// Empty for single-group files (verify against `root` directly).
    pub cvs: Vec<ChainingValue>,
}

impl Outboard {
    /// Build from a full in-memory buffer (tests and small files).
    pub fn from_bytes(data: &[u8]) -> Self {
        let size = data.len() as u64;
        if group_count(size) == 1 {
            return Self {
                size,
                root: blake3::hash(data),
                cvs: Vec::new(),
            };
        }
        let cvs: Vec<_> = data
            .chunks(GROUP_LEN as usize)
            .enumerate()
            .map(|(i, g)| group_cv(i as u64, g))
            .collect();
        let root = root_from_cvs(&cvs).expect("at least two groups");
        Self { size, root, cvs }
    }

    /// Check that a received CV list is consistent with `root` and `size`.
    pub fn verify_cvs(root: &blake3::Hash, size: u64, cvs: &[ChainingValue]) -> bool {
        let groups = group_count(size);
        if groups == 1 {
            return cvs.is_empty();
        }
        cvs.len() as u64 == groups && root_from_cvs(cvs).as_ref() == Some(root)
    }

    /// Verify one group's bytes.
    pub fn verify_group(&self, index: u64, data: &[u8]) -> bool {
        let (s, e) = group_range(index, self.size);
        if data.len() as u64 != e - s {
            return false;
        }
        if self.cvs.is_empty() {
            return index == 0 && blake3::hash(data) == self.root;
        }
        match self.cvs.get(index as usize) {
            Some(cv) => group_cv(index, data) == *cv,
            None => false,
        }
    }

    /// Serialise the CV list as raw 32-byte values.
    pub fn cvs_bytes(&self) -> Vec<u8> {
        self.cvs.iter().flat_map(|cv| cv.iter().copied()).collect()
    }

    pub fn cvs_from_bytes(bytes: &[u8]) -> Option<Vec<ChainingValue>> {
        if !bytes.len().is_multiple_of(32) {
            return None;
        }
        Some(bytes.as_chunks::<32>().0.to_vec())
    }
}

/// Incrementally build an [`Outboard`] while reading a file sequentially.
pub struct OutboardBuilder {
    size: u64,
    index: u64,
    buf: Vec<u8>,
    full: blake3::Hasher,
    cvs: Vec<ChainingValue>,
}

impl OutboardBuilder {
    pub fn new(size: u64) -> Self {
        Self {
            size,
            index: 0,
            buf: Vec::with_capacity(GROUP_LEN as usize),
            full: blake3::Hasher::new(),
            cvs: Vec::new(),
        }
    }

    pub fn update(&mut self, mut data: &[u8]) {
        self.full.update(data);
        if group_count(self.size) == 1 {
            return;
        }
        while !data.is_empty() {
            let want = GROUP_LEN as usize - self.buf.len();
            let take = want.min(data.len());
            self.buf.extend_from_slice(&data[..take]);
            data = &data[take..];
            if self.buf.len() == GROUP_LEN as usize {
                self.cvs.push(group_cv(self.index, &self.buf));
                self.index += 1;
                self.buf.clear();
            }
        }
    }

    pub fn finish(mut self) -> Outboard {
        let root = self.full.finalize();
        if group_count(self.size) > 1 && !self.buf.is_empty() {
            self.cvs.push(group_cv(self.index, &self.buf));
        }
        Outboard {
            size: self.size,
            root,
            cvs: self.cvs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 31 % 251) as u8).collect()
    }

    #[test]
    fn root_matches_plain_blake3() {
        for len in [
            0,
            1,
            1024,
            GROUP_LEN as usize,
            GROUP_LEN as usize + 1,
            3 * GROUP_LEN as usize,
            5 * GROUP_LEN as usize - 7,
        ] {
            let d = data(len);
            let ob = Outboard::from_bytes(&d);
            assert_eq!(ob.root, blake3::hash(&d), "len {len}");
            assert!(Outboard::verify_cvs(&ob.root, ob.size, &ob.cvs));
            for i in 0..group_count(ob.size) {
                let (s, e) = group_range(i, ob.size);
                assert!(ob.verify_group(i, &d[s as usize..e as usize]));
            }
        }
    }

    #[test]
    fn builder_matches_from_bytes() {
        let d = data(3 * GROUP_LEN as usize + 12345);
        let mut b = OutboardBuilder::new(d.len() as u64);
        for piece in d.chunks(77_777) {
            b.update(piece);
        }
        assert_eq!(b.finish(), Outboard::from_bytes(&d));
    }

    #[test]
    fn detects_tampering() {
        let mut d = data(2 * GROUP_LEN as usize + 5);
        let ob = Outboard::from_bytes(&d);
        d[GROUP_LEN as usize + 3] ^= 1;
        let (s, e) = group_range(1, ob.size);
        assert!(!ob.verify_group(1, &d[s as usize..e as usize]));
        let mut bad = ob.cvs.clone();
        bad[0][0] ^= 1;
        assert!(!Outboard::verify_cvs(&ob.root, ob.size, &bad));
        // Wrong group index (data moved) also fails.
        let (s0, e0) = group_range(0, ob.size);
        assert!(!ob.verify_group(1, &d[s0 as usize..e0 as usize]));
    }
}
