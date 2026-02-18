// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::error::Result;
use crate::storage::StorageBackend;

use super::protocol::{Operation, ReplicationWrite};
use super::{NUM_PARTITIONS, PartitionId};

use std::sync::Arc;

pub struct PartitionStorage {
    backend: Arc<dyn StorageBackend>,
}

impl PartitionStorage {
    #[must_use]
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self { backend }
    }

    /// Persists a single replication write to storage.
    ///
    /// # Errors
    /// Returns an error if the storage backend fails to write.
    pub fn write(&self, write: &ReplicationWrite) -> Result<()> {
        let key = Self::make_key(write.partition, &write.entity, &write.id);
        match write.operation {
            Operation::Insert | Operation::Update => {
                self.backend.insert(&key, &write.data)?;
            }
            Operation::Delete => {
                self.backend.remove(&key)?;
            }
        }
        Ok(())
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn write_with_outbox(
        &self,
        write: &ReplicationWrite,
        outbox_key: Vec<u8>,
        outbox_value: Vec<u8>,
    ) -> Result<()> {
        let key = Self::make_key(write.partition, &write.entity, &write.id);
        let mut batch = self.backend.batch();
        match write.operation {
            Operation::Insert | Operation::Update => {
                batch.insert(key, write.data.clone());
            }
            Operation::Delete => {
                batch.remove(key);
            }
        }
        batch.insert(outbox_key, outbox_value);
        batch.commit()
    }

    /// Persists multiple replication writes atomically as a batch.
    ///
    /// # Errors
    /// Returns an error if the storage backend fails to commit the batch.
    pub fn write_batch(&self, writes: &[ReplicationWrite]) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        let mut batch = self.backend.batch();
        for write in writes {
            let key = Self::make_key(write.partition, &write.entity, &write.id);
            match write.operation {
                Operation::Insert | Operation::Update => {
                    batch.insert(key, write.data.clone());
                }
                Operation::Delete => {
                    batch.remove(key);
                }
            }
        }
        batch.commit()?;
        self.backend.flush()
    }

    /// Scans all partitions for entries of a specific entity type.
    ///
    /// # Errors
    /// Returns an error if the storage backend fails to scan.
    pub fn scan_entity(&self, entity: &str) -> Result<Vec<(PartitionId, String, Vec<u8>)>> {
        let mut results = Vec::new();
        for p in 0..NUM_PARTITIONS {
            let prefix = format!("{p:02x}/{entity}/");
            let entries = self.backend.prefix_scan(prefix.as_bytes())?;
            for (key, value) in entries {
                let key_str = String::from_utf8_lossy(&key);
                if let Some(id) = key_str.strip_prefix(&prefix)
                    && let Some(partition) = PartitionId::new(p)
                {
                    results.push((partition, id.to_string(), value));
                }
            }
        }
        Ok(results)
    }

    /// Scans all entries for a specific partition.
    ///
    /// # Errors
    /// Returns an error if the storage backend fails to scan.
    pub fn scan_partition(&self, partition: PartitionId) -> Result<Vec<(String, String, Vec<u8>)>> {
        let prefix = format!("{:02x}/", partition.get());
        let entries = self.backend.prefix_scan(prefix.as_bytes())?;

        let mut results = Vec::new();
        for (key, value) in entries {
            let key_str = String::from_utf8_lossy(&key);
            if let Some(rest) = key_str.strip_prefix(&prefix)
                && let Some((entity, id)) = rest.split_once('/')
            {
                results.push((entity.to_string(), id.to_string(), value));
            }
        }
        Ok(results)
    }

    /// Flushes pending writes to durable storage.
    ///
    /// # Errors
    /// Returns an error if the storage backend fails to flush.
    pub fn flush(&self) -> Result<()> {
        self.backend.flush()
    }

    #[must_use]
    pub fn make_key(partition: PartitionId, entity: &str, id: &str) -> Vec<u8> {
        format!("{:02x}/{entity}/{id}", partition.get()).into_bytes()
    }
}

impl std::fmt::Debug for PartitionStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionStorage").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::Epoch;
    use crate::storage::MemoryBackend;

    fn storage() -> PartitionStorage {
        PartitionStorage::new(Arc::new(MemoryBackend::new()))
    }

    fn partition(n: u16) -> PartitionId {
        PartitionId::new(n).unwrap()
    }

    #[test]
    fn write_and_scan_entity() {
        let storage = storage();

        let write = ReplicationWrite::new(
            partition(0),
            Operation::Insert,
            Epoch::ZERO,
            1,
            "_sessions".to_string(),
            "client1".to_string(),
            vec![1, 2, 3],
        );

        storage.write(&write).unwrap();

        let entries = storage.scan_entity("_sessions").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, partition(0));
        assert_eq!(entries[0].1, "client1");
        assert_eq!(entries[0].2, vec![1, 2, 3]);
    }

    #[test]
    fn write_multiple_partitions() {
        let storage = storage();

        for p in [0u8, 15, 63] {
            let write = ReplicationWrite::new(
                partition(u16::from(p)),
                Operation::Insert,
                Epoch::ZERO,
                1,
                "_sessions".to_string(),
                format!("client{p}"),
                vec![p],
            );
            storage.write(&write).unwrap();
        }

        let entries = storage.scan_entity("_sessions").unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn delete_removes_entry() {
        let storage = storage();

        let write = ReplicationWrite::new(
            partition(0),
            Operation::Insert,
            Epoch::ZERO,
            1,
            "_sessions".to_string(),
            "client1".to_string(),
            vec![1, 2, 3],
        );
        storage.write(&write).unwrap();

        let delete = ReplicationWrite::new(
            partition(0),
            Operation::Delete,
            Epoch::ZERO,
            2,
            "_sessions".to_string(),
            "client1".to_string(),
            vec![],
        );
        storage.write(&delete).unwrap();

        let entries = storage.scan_entity("_sessions").unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn update_overwrites_entry() {
        let storage = storage();

        let write = ReplicationWrite::new(
            partition(0),
            Operation::Insert,
            Epoch::ZERO,
            1,
            "_sessions".to_string(),
            "client1".to_string(),
            vec![1, 2, 3],
        );
        storage.write(&write).unwrap();

        let update = ReplicationWrite::new(
            partition(0),
            Operation::Update,
            Epoch::ZERO,
            2,
            "_sessions".to_string(),
            "client1".to_string(),
            vec![4, 5, 6],
        );
        storage.write(&update).unwrap();

        let entries = storage.scan_entity("_sessions").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].2, vec![4, 5, 6]);
    }

    #[test]
    fn batch_write_all_partitions() {
        let storage = storage();

        let writes: Vec<_> = (0..NUM_PARTITIONS)
            .map(|p| {
                ReplicationWrite::new(
                    partition(p),
                    Operation::Insert,
                    Epoch::ZERO,
                    1,
                    "_topic_index".to_string(),
                    "test/topic".to_string(),
                    vec![1, 2, 3],
                )
            })
            .collect();

        storage.write_batch(&writes).unwrap();

        let entries = storage.scan_entity("_topic_index").unwrap();
        assert_eq!(entries.len(), NUM_PARTITIONS as usize);
    }

    #[test]
    fn scan_partition() {
        let storage = storage();

        let writes = vec![
            ReplicationWrite::new(
                partition(5),
                Operation::Insert,
                Epoch::ZERO,
                1,
                "_sessions".to_string(),
                "client1".to_string(),
                vec![1],
            ),
            ReplicationWrite::new(
                partition(5),
                Operation::Insert,
                Epoch::ZERO,
                2,
                "_qos2".to_string(),
                "client1:42".to_string(),
                vec![2],
            ),
            ReplicationWrite::new(
                partition(10),
                Operation::Insert,
                Epoch::ZERO,
                3,
                "_sessions".to_string(),
                "client2".to_string(),
                vec![3],
            ),
        ];

        for write in &writes {
            storage.write(write).unwrap();
        }

        let p5_entries = storage.scan_partition(partition(5)).unwrap();
        assert_eq!(p5_entries.len(), 2);

        let p10_entries = storage.scan_partition(partition(10)).unwrap();
        assert_eq!(p10_entries.len(), 1);
    }

    #[test]
    fn make_key_format() {
        assert_eq!(
            PartitionStorage::make_key(partition(0), "_sessions", "client1"),
            b"00/_sessions/client1".to_vec()
        );
        assert_eq!(
            PartitionStorage::make_key(partition(15), "_qos2", "c:42"),
            b"0f/_qos2/c:42".to_vec()
        );
        assert_eq!(
            PartitionStorage::make_key(partition(63), "_db_data", "users/abc"),
            b"3f/_db_data/users/abc".to_vec()
        );
    }

    #[test]
    fn empty_batch_succeeds() {
        let storage = storage();
        storage.write_batch(&[]).unwrap();
    }

    #[test]
    fn write_with_outbox_persists_both_atomically() {
        let storage = storage();

        let write = ReplicationWrite::new(
            partition(0),
            Operation::Insert,
            Epoch::ZERO,
            1,
            "_db_data".to_string(),
            "users/u1".to_string(),
            vec![10, 20],
        );

        let outbox_key = b"_outbox/op-001".to_vec();
        let outbox_value = b"change-event-payload".to_vec();

        storage
            .write_with_outbox(&write, outbox_key.clone(), outbox_value.clone())
            .unwrap();

        let entries = storage.scan_entity("_db_data").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].2, vec![10, 20]);

        let outbox_entries = storage.backend.prefix_scan(b"_outbox/").unwrap();
        assert_eq!(outbox_entries.len(), 1);
        assert_eq!(outbox_entries[0].1, b"change-event-payload");
    }
}
