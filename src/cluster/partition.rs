use std::fmt;

pub const NUM_PARTITIONS: u16 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionId(u16);

impl PartitionId {
    pub const ZERO: Self = Self(0);

    #[allow(clippy::must_use_candidate)]
    pub fn new(id: u16) -> Option<Self> {
        if id < NUM_PARTITIONS {
            Some(Self(id))
        } else {
            None
        }
    }

    #[must_use]
    pub fn from_key(key: &str) -> Self {
        let hash = crc32fast::hash(key.as_bytes());
        Self(u16::try_from(hash % u32::from(NUM_PARTITIONS)).unwrap_or(0))
    }

    #[must_use]
    pub fn from_entity_id(entity: &str, id: &str) -> Self {
        let key = format!("{entity}/{id}");
        Self::from_key(&key)
    }

    #[must_use]
    pub fn get(self) -> u16 {
        self.0
    }

    pub fn all() -> impl Iterator<Item = Self> {
        (0..NUM_PARTITIONS).map(Self)
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "p{}", self.0)
    }
}

impl From<PartitionId> for u16 {
    fn from(partition: PartitionId) -> Self {
        partition.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_from_key_deterministic() {
        let p1 = PartitionId::from_key("users/123");
        let p2 = PartitionId::from_key("users/123");
        assert_eq!(p1, p2);
    }

    #[test]
    fn partition_from_entity_id_deterministic() {
        let p1 = PartitionId::from_entity_id("users", "123");
        let p2 = PartitionId::from_entity_id("users", "123");
        assert_eq!(p1, p2);
    }

    #[test]
    fn partition_within_bounds() {
        for i in 0..1000 {
            let key = format!("test/{i}");
            let partition = PartitionId::from_key(&key);
            assert!(partition.get() < NUM_PARTITIONS);
        }
    }

    #[test]
    fn partition_new_validates_bounds() {
        assert!(PartitionId::new(0).is_some());
        assert!(PartitionId::new(63).is_some());
        assert!(PartitionId::new(64).is_none());
        assert!(PartitionId::new(100).is_none());
    }

    #[test]
    fn partition_all_iterates_correctly() {
        let partitions: Vec<_> = PartitionId::all().collect();
        assert_eq!(partitions.len(), NUM_PARTITIONS as usize);
        assert_eq!(partitions[0].get(), 0);
        assert_eq!(partitions[63].get(), 63);
    }
}
