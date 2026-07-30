// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::{NUM_PARTITIONS, PartitionId};

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

/// Generate a server-side id that maps to `partition`.
///
/// The base is a UUID v7 (48-bit millisecond timestamp prefix + random), so ids
/// are lexicographically time-ordered — prefix scans over `data/{entity}/` return
/// records in insertion order. The suffix loop preserves partition targeting: it
/// appends a 16-bit suffix until `data_partition(entity, id)` matches `partition`.
#[must_use]
pub fn generate_id_for_partition(entity: &str, partition: PartitionId) -> String {
    let base = uuid::Uuid::now_v7();

    for suffix in 0..1000_u16 {
        let id = format!("{base}-{suffix:04x}");
        if data_partition(entity, &id) == partition {
            return id;
        }
    }

    format!("{base}-p{}", partition.get())
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

    #[test]
    fn generate_id_uses_time_ordered_uuid_v7_base() {
        let partition = data_partition("users", "seed");
        let id = generate_id_for_partition("users", partition);
        let base = id.rsplitn(2, '-').last().unwrap();
        let uuid = uuid::Uuid::parse_str(base).expect("id base must be a valid uuid");
        assert_eq!(
            uuid.get_version_num(),
            7,
            "id base must be a time-ordered uuid v7"
        );
    }

    #[test]
    fn generate_id_is_unique_across_calls() {
        let partition = data_partition("users", "seed");
        let a = generate_id_for_partition("users", partition);
        let b = generate_id_for_partition("users", partition);
        assert_ne!(a, b, "generated ids must be unique");
    }
}
