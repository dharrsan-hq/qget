//! Half-open byte ranges and a merged interval set.

use serde::{Deserialize, Serialize};

/// A half-open byte range `[start, end)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

impl ByteRange {
    pub fn new(start: u64, end: u64) -> Self {
        debug_assert!(start <= end, "invalid range {start}..{end}");
        Self { start, end }
    }

    pub fn len(&self) -> u64 {
        self.end - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    /// The inclusive `Range` header value, e.g. `bytes=0-99`.
    pub fn http_header(&self) -> String {
        format!("bytes={}-{}", self.start, self.end - 1)
    }
}

/// A set of non-overlapping, non-adjacent half-open intervals kept sorted.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntervalSet {
    spans: Vec<(u64, u64)>,
}

impl IntervalSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_spans(spans: impl IntoIterator<Item = (u64, u64)>) -> Self {
        let mut set = Self::new();
        for (s, e) in spans {
            set.insert(s, e);
        }
        set
    }

    pub fn spans(&self) -> &[(u64, u64)] {
        &self.spans
    }

    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    /// Total number of bytes covered.
    pub fn covered(&self) -> u64 {
        self.spans.iter().map(|(s, e)| e - s).sum()
    }

    /// Insert `[start, end)`, merging with overlapping or adjacent spans.
    pub fn insert(&mut self, start: u64, end: u64) {
        if start >= end {
            return;
        }
        // First span whose end is >= start (could merge with us).
        let i = self.spans.partition_point(|&(_, e)| e < start);
        let mut new_start = start;
        let mut new_end = end;
        let mut j = i;
        while j < self.spans.len() && self.spans[j].0 <= new_end {
            new_start = new_start.min(self.spans[j].0);
            new_end = new_end.max(self.spans[j].1);
            j += 1;
        }
        self.spans
            .splice(i..j, std::iter::once((new_start, new_end)));
    }

    /// Whether every byte of `[start, end)` is covered.
    pub fn contains(&self, start: u64, end: u64) -> bool {
        if start >= end {
            return true;
        }
        let i = self.spans.partition_point(|&(_, e)| e <= start);
        matches!(self.spans.get(i), Some(&(s, e)) if s <= start && e >= end)
    }

    /// End of the contiguous covered prefix starting at `from`
    /// (returns `from` if `from` itself is not covered).
    pub fn contiguous_end(&self, from: u64) -> u64 {
        let i = self.spans.partition_point(|&(_, e)| e <= from);
        match self.spans.get(i) {
            Some(&(s, e)) if s <= from => e,
            _ => from,
        }
    }

    /// First uncovered sub-range of `[from, limit)`, if any.
    pub fn first_gap(&self, from: u64, limit: u64) -> Option<ByteRange> {
        let mut cursor = from;
        let i = self.spans.partition_point(|&(_, e)| e <= from);
        for &(s, e) in &self.spans[i..] {
            if cursor >= limit {
                return None;
            }
            if s > cursor {
                return Some(ByteRange::new(cursor, s.min(limit)));
            }
            cursor = cursor.max(e);
        }
        (cursor < limit).then(|| ByteRange::new(cursor, limit))
    }

    /// Union of two sets.
    pub fn union(&self, other: &IntervalSet) -> IntervalSet {
        let mut out = self.clone();
        for &(s, e) in &other.spans {
            out.insert(s, e);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn insert_merges_overlapping_and_adjacent() {
        let mut s = IntervalSet::new();
        s.insert(10, 20);
        s.insert(30, 40);
        s.insert(20, 30);
        assert_eq!(s.spans(), &[(10, 40)]);
        s.insert(0, 5);
        s.insert(3, 12);
        assert_eq!(s.spans(), &[(0, 40)]);
    }

    #[test]
    fn gaps_and_contains() {
        let s = IntervalSet::from_spans([(0, 10), (20, 30)]);
        assert!(s.contains(0, 10));
        assert!(!s.contains(5, 15));
        assert_eq!(s.first_gap(0, 100), Some(ByteRange::new(10, 20)));
        assert_eq!(s.first_gap(25, 100), Some(ByteRange::new(30, 100)));
        assert_eq!(s.first_gap(0, 10), None);
        assert_eq!(s.first_gap(0, 15), Some(ByteRange::new(10, 15)));
        assert_eq!(s.contiguous_end(0), 10);
        assert_eq!(s.contiguous_end(10), 10);
        assert_eq!(s.contiguous_end(22), 30);
    }

    proptest! {
        #[test]
        fn matches_bitmap_model(ops in prop::collection::vec((0u64..200, 0u64..40), 0..60)) {
            let mut set = IntervalSet::new();
            let mut model = vec![false; 260];
            for (s, l) in ops {
                set.insert(s, s + l);
                for b in &mut model[s as usize..(s + l) as usize] { *b = true; }
            }
            // spans sorted, disjoint, non-adjacent
            for w in set.spans().windows(2) {
                prop_assert!(w[0].1 < w[1].0);
            }
            prop_assert_eq!(set.covered(), model.iter().filter(|b| **b).count() as u64);
            for i in 0..260u64 {
                prop_assert_eq!(set.contains(i, i + 1), model[i as usize]);
            }
            if let Some(g) = set.first_gap(0, 260) {
                prop_assert!(!model[g.start as usize]);
                prop_assert!(model[..g.start as usize].iter().all(|b| *b));
            }
        }
    }
}
