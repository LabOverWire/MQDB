// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use bebytes::BeBytes;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, BeBytes)]
pub struct NodeId {
    id: u16,
}

impl NodeId {
    pub const INVALID: Self = Self { id: 0 };

    #[must_use]
    pub fn validated(id: u16) -> Option<Self> {
        if id == 0 { None } else { Some(Self::new(id)) }
    }

    #[must_use]
    pub const fn get(self) -> u16 {
        self.id
    }

    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.id != 0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n{}", self.id)
    }
}

impl From<NodeId> for u16 {
    fn from(node: NodeId) -> Self {
        node.id
    }
}

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

pub const NUM_PARTITIONS: u16 = 256;

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
    fn node_id_zero_is_invalid() {
        assert!(NodeId::validated(0).is_none());
        assert!(!NodeId::INVALID.is_valid());
    }

    #[test]
    fn node_id_non_zero_is_valid() {
        assert!(NodeId::validated(1).is_some());
        assert!(NodeId::validated(1).unwrap().is_valid());
        assert!(NodeId::validated(65535).is_some());
    }

    #[test]
    fn node_id_ordering() {
        let n1 = NodeId::validated(1).unwrap();
        let n2 = NodeId::validated(2).unwrap();
        assert!(n1 < n2);
    }

    #[test]
    fn node_id_bebytes_roundtrip() {
        let node = NodeId::validated(42).unwrap();
        let bytes = node.to_be_bytes();
        let (decoded, _) = NodeId::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(node, decoded);
    }

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
        assert!(PartitionId::new(NUM_PARTITIONS - 1).is_some());
        assert!(PartitionId::new(NUM_PARTITIONS).is_none());
        assert!(PartitionId::new(NUM_PARTITIONS + 100).is_none());
    }

    #[test]
    fn partition_all_iterates_correctly() {
        let partitions: Vec<_> = PartitionId::all().collect();
        assert_eq!(partitions.len(), NUM_PARTITIONS as usize);
        assert_eq!(partitions[0].get(), 0);
        assert_eq!(
            partitions[NUM_PARTITIONS as usize - 1].get(),
            NUM_PARTITIONS - 1
        );
    }
}
