use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

const WINDOW_SECS: u64 = 60;

pub struct RateLimiter {
    requests: RwLock<HashMap<String, Vec<Instant>>>,
    max_requests: u32,
    window: Duration,
}

impl RateLimiter {
    #[must_use]
    pub fn new(max_requests_per_minute: u32) -> Self {
        Self {
            requests: RwLock::new(HashMap::new()),
            max_requests: max_requests_per_minute,
            window: Duration::from_secs(WINDOW_SECS),
        }
    }

    pub fn check_and_record(&self, key: &str) -> bool {
        let now = Instant::now();
        let Some(cutoff) = now.checked_sub(self.window) else {
            return true;
        };

        let Ok(mut requests) = self.requests.write() else {
            return true;
        };

        let timestamps = requests.entry(key.to_string()).or_default();
        timestamps.retain(|&t| t > cutoff);

        if timestamps.len() >= self.max_requests as usize {
            return false;
        }

        timestamps.push(now);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allows_under_limit() {
        let limiter = RateLimiter::new(5);
        for _ in 0..5 {
            assert!(limiter.check_and_record("test"));
        }
    }

    #[test]
    fn test_blocks_over_limit() {
        let limiter = RateLimiter::new(3);
        assert!(limiter.check_and_record("test"));
        assert!(limiter.check_and_record("test"));
        assert!(limiter.check_and_record("test"));
        assert!(!limiter.check_and_record("test"));
    }

    #[test]
    fn test_separate_keys() {
        let limiter = RateLimiter::new(2);
        assert!(limiter.check_and_record("user1"));
        assert!(limiter.check_and_record("user1"));
        assert!(!limiter.check_and_record("user1"));
        assert!(limiter.check_and_record("user2"));
        assert!(limiter.check_and_record("user2"));
    }
}
