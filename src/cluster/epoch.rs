use bebytes::BeBytes;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, BeBytes)]
pub struct Epoch {
    value: u64,
}

impl Epoch {
    pub const ZERO: Self = Self { value: 0 };

    #[must_use]
    pub const fn get(self) -> u64 {
        self.value
    }

    #[must_use]
    pub const fn next(self) -> Self {
        Self {
            value: self.value.saturating_add(1),
        }
    }

    #[must_use]
    pub fn is_stale(self, current: Self) -> bool {
        self < current
    }
}

impl Default for Epoch {
    fn default() -> Self {
        Self::ZERO
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "e{}", self.value)
    }
}

impl From<Epoch> for u64 {
    fn from(epoch: Epoch) -> Self {
        epoch.value
    }
}

impl From<u64> for Epoch {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_ordering() {
        let e1 = Epoch::new(1);
        let e2 = Epoch::new(2);
        assert!(e1 < e2);
        assert!(e2 > e1);
    }

    #[test]
    fn epoch_next() {
        let e1 = Epoch::new(5);
        let e2 = e1.next();
        assert_eq!(e2.get(), 6);
    }

    #[test]
    fn epoch_stale_check() {
        let old = Epoch::new(1);
        let current = Epoch::new(5);
        assert!(old.is_stale(current));
        assert!(!current.is_stale(old));
    }

    #[test]
    fn epoch_saturating_increment() {
        let max = Epoch::new(u64::MAX);
        assert_eq!(max.next().get(), u64::MAX);
    }

    #[test]
    fn epoch_bebytes_roundtrip() {
        let epoch = Epoch::new(12345);
        let bytes = epoch.to_be_bytes();
        let (decoded, _) = Epoch::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(epoch, decoded);
    }
}
