// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::cluster::{NUM_PARTITIONS, PartitionId};

/// # Panics
/// Never panics: result of `% NUM_PARTITIONS` is always valid.
#[must_use]
pub fn data_partition(entity: &str, id: &str) -> PartitionId {
    let key = format!("{entity}/{id}");
    let hash = crc32fast::hash(key.as_bytes());
    #[allow(clippy::cast_possible_truncation)]
    PartitionId::new((hash % u32::from(NUM_PARTITIONS)) as u16).unwrap()
}

/// # Panics
/// Never panics: result of `% NUM_PARTITIONS` is always valid.
#[must_use]
pub fn index_partition(entity: &str, field: &str, value: &[u8]) -> PartitionId {
    let prefix = format!("idx:{entity}:{field}:");
    let mut key_bytes = prefix.into_bytes();
    key_bytes.extend_from_slice(value);
    let hash = crc32fast::hash(&key_bytes);
    #[allow(clippy::cast_possible_truncation)]
    PartitionId::new((hash % u32::from(NUM_PARTITIONS)) as u16).unwrap()
}

/// # Panics
/// Never panics: result of `% NUM_PARTITIONS` is always valid.
#[must_use]
pub fn unique_partition(entity: &str, field: &str, value: &[u8]) -> PartitionId {
    let prefix = format!("unique:{entity}:{field}:");
    let mut key_bytes = prefix.into_bytes();
    key_bytes.extend_from_slice(value);
    let hash = crc32fast::hash(&key_bytes);
    #[allow(clippy::cast_possible_truncation)]
    PartitionId::new((hash % u32::from(NUM_PARTITIONS)) as u16).unwrap()
}

/// # Panics
/// Never panics: result of `% NUM_PARTITIONS` is always valid.
#[must_use]
pub fn schema_partition(entity: &str) -> PartitionId {
    let key = format!("schema:{entity}");
    let hash = crc32fast::hash(key.as_bytes());
    #[allow(clippy::cast_possible_truncation)]
    PartitionId::new((hash % u32::from(NUM_PARTITIONS)) as u16).unwrap()
}

#[must_use]
pub fn generate_id_for_partition(
    node_id: u16,
    entity: &str,
    partition: PartitionId,
    data: &[u8],
) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    entity.hash(&mut hasher);
    data.hash(&mut hasher);
    node_id.hash(&mut hasher);
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos())
        .hash(&mut hasher);

    let base_id = hasher.finish();

    for suffix in 0..1000_u16 {
        let id = format!("{base_id:016x}-{suffix:04x}");
        if data_partition(entity, &id) == partition {
            return id;
        }
    }

    format!("{base_id:016x}-p{}", partition.get())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_partition_is_deterministic() {
        let p1 = data_partition("users", "123");
        let p2 = data_partition("users", "123");
        assert_eq!(p1, p2);
    }

    #[test]
    fn data_partition_varies_with_id() {
        let p1 = data_partition("users", "123");
        let p2 = data_partition("users", "456");
        assert_ne!(p1, p2);
    }

    #[test]
    fn data_partition_varies_with_entity() {
        let p1 = data_partition("users", "123");
        let p2 = data_partition("orders", "123");
        assert_ne!(p1, p2);
    }

    #[test]
    fn index_partition_is_deterministic() {
        let p1 = index_partition("users", "email", b"alice@example.com");
        let p2 = index_partition("users", "email", b"alice@example.com");
        assert_eq!(p1, p2);
    }

    #[test]
    fn unique_partition_is_deterministic() {
        let p1 = unique_partition("users", "email", b"alice@example.com");
        let p2 = unique_partition("users", "email", b"alice@example.com");
        assert_eq!(p1, p2);
    }

    #[test]
    fn all_partitions_in_valid_range() {
        for i in 0..1000 {
            let entity = format!("entity{i}");
            let id = format!("id{i}");
            let p = data_partition(&entity, &id);
            assert!(p.get() < NUM_PARTITIONS);
        }
    }
}
