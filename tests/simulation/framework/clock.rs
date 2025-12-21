use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct VirtualClock {
    nanos: Arc<AtomicU64>,
}

impl VirtualClock {
    #[must_use]
    pub fn new() -> Self {
        Self {
            nanos: Arc::new(AtomicU64::new(0)),
        }
    }

    #[must_use]
    pub fn now(&self) -> u64 {
        self.nanos.load(Ordering::SeqCst)
    }

    #[must_use]
    #[allow(dead_code)]
    pub fn now_duration(&self) -> Duration {
        Duration::from_nanos(self.now())
    }

    pub fn advance(&self, duration: Duration) {
        self.nanos.fetch_add(duration.as_nanos() as u64, Ordering::SeqCst);
    }

    pub fn advance_ms(&self, ms: u64) {
        self.advance(Duration::from_millis(ms));
    }

    pub fn set(&self, nanos: u64) {
        self.nanos.store(nanos, Ordering::SeqCst);
    }
}

impl Default for VirtualClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_at_zero() {
        let clock = VirtualClock::new();
        assert_eq!(clock.now(), 0);
    }

    #[test]
    fn advance_increments_time() {
        let clock = VirtualClock::new();
        clock.advance(Duration::from_secs(1));
        assert_eq!(clock.now(), 1_000_000_000);
    }

    #[test]
    fn advance_ms_works() {
        let clock = VirtualClock::new();
        clock.advance_ms(100);
        assert_eq!(clock.now(), 100_000_000);
    }

    #[test]
    fn clones_share_state() {
        let clock1 = VirtualClock::new();
        let clock2 = clock1.clone();
        clock1.advance_ms(50);
        assert_eq!(clock2.now(), 50_000_000);
    }

    #[test]
    fn set_overrides_time() {
        let clock = VirtualClock::new();
        clock.advance_ms(100);
        clock.set(0);
        assert_eq!(clock.now(), 0);
    }
}
