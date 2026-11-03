//! Seeded PRNG used for every randomised decision (DET-02).

use serde::{Deserialize, Serialize};

/// SplitMix64: tiny, fast, and fully reproducible from its seed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform value in `[0, n)`; `n` must be non-zero.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    /// Scale `value` by a uniform factor in `[1 - frac, 1 + frac]` (jitter).
    pub fn jitter(&mut self, value: u64, frac_percent: u64) -> u64 {
        let span = value * frac_percent / 100;
        if span == 0 {
            return value;
        }
        value - span + self.below(2 * span + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_for_same_seed() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
        assert_ne!(Rng::new(1).next_u64(), Rng::new(2).next_u64());
    }

    #[test]
    fn jitter_stays_in_bounds() {
        let mut r = Rng::new(7);
        for _ in 0..1000 {
            let v = r.jitter(1000, 20);
            assert!((800..=1200).contains(&v));
        }
    }
}
