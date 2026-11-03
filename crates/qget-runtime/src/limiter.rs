//! Global token-bucket rate limiter (HTTP-17).

use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct RateLimiter {
    rate: f64,
    burst: f64,
    state: Mutex<(f64, Instant)>,
}

impl RateLimiter {
    /// `bytes_per_sec` sustained rate with a one-second burst.
    pub fn new(bytes_per_sec: u64) -> Self {
        let rate = bytes_per_sec.max(1) as f64;
        Self {
            rate,
            burst: rate,
            state: Mutex::new((rate, Instant::now())),
        }
    }

    /// Wait until `n` bytes may be consumed.
    pub async fn acquire(&self, n: usize) {
        let wait = {
            let mut st = self.state.lock().expect("limiter lock");
            let now = Instant::now();
            let refill = now.duration_since(st.1).as_secs_f64() * self.rate;
            st.0 = (st.0 + refill).min(self.burst);
            st.1 = now;
            st.0 -= n as f64;
            if st.0 >= 0.0 {
                Duration::ZERO
            } else {
                Duration::from_secs_f64(-st.0 / self.rate)
            }
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn limits_throughput() {
        let l = RateLimiter::new(1_000_000);
        let start = Instant::now();
        // 1 MB burst is free, the next 500 KB take ~0.5 s.
        for _ in 0..15 {
            l.acquire(100_000).await;
        }
        let el = start.elapsed();
        assert!(el >= Duration::from_millis(400), "{el:?}");
        assert!(el < Duration::from_millis(1500), "{el:?}");
    }
}
