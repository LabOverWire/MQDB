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
