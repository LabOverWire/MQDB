use super::{NodeId, PartitionId, session_partition};
use bebytes::BeBytes;
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct ConsumerOffset {
    pub version: u8,
    pub consumer_id_len: u8,
    #[FromField(consumer_id_len)]
    pub consumer_id: Vec<u8>,
    pub partition: u16,
    pub sequence: u64,
    pub timestamp: u64,
}

impl ConsumerOffset {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(
        consumer_id: &str,
        partition: PartitionId,
        sequence: u64,
        timestamp: u64,
    ) -> Self {
        let consumer_bytes = consumer_id.as_bytes().to_vec();
        Self {
            version: 1,
            consumer_id_len: consumer_bytes.len() as u8,
            consumer_id: consumer_bytes,
            partition: partition.get(),
            sequence,
            timestamp,
        }
    }

    #[must_use]
    pub fn consumer_id_str(&self) -> &str {
        std::str::from_utf8(&self.consumer_id).unwrap_or("")
    }

    #[must_use]
    pub fn partition_id(&self) -> Option<PartitionId> {
        PartitionId::new(self.partition)
    }
}

#[must_use]
pub fn offset_key(consumer_id: &str, partition: PartitionId) -> String {
    let consumer_partition = session_partition(consumer_id);
    format!(
        "_offset/p{}/{}/p{}",
        consumer_partition.get(),
        consumer_id,
        partition.get()
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetStoreError {
    NotFound,
    InvalidPartition,
    SerializationError,
}

impl std::fmt::Display for OffsetStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "offset not found"),
            Self::InvalidPartition => write!(f, "invalid partition"),
            Self::SerializationError => write!(f, "serialization error"),
        }
    }
}

impl std::error::Error for OffsetStoreError {}

type OffsetKey = (String, u16);

pub struct OffsetStore {
    node_id: NodeId,
    offsets: RwLock<HashMap<OffsetKey, ConsumerOffset>>,
}

impl OffsetStore {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            offsets: RwLock::new(HashMap::new()),
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn commit(&self, consumer_id: &str, partition: PartitionId, sequence: u64, timestamp: u64) {
        let key = (consumer_id.to_string(), partition.get());
        let offset = ConsumerOffset::create(consumer_id, partition, sequence, timestamp);
        self.offsets.write().unwrap().insert(key, offset);
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get(&self, consumer_id: &str, partition: PartitionId) -> Option<ConsumerOffset> {
        let key = (consumer_id.to_string(), partition.get());
        self.offsets.read().unwrap().get(&key).cloned()
    }

    #[must_use]
    pub fn get_sequence(&self, consumer_id: &str, partition: PartitionId) -> Option<u64> {
        self.get(consumer_id, partition).map(|o| o.sequence)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get_all_for_consumer(&self, consumer_id: &str) -> Vec<ConsumerOffset> {
        self.offsets
            .read()
            .unwrap()
            .iter()
            .filter(|((cid, _), _)| cid == consumer_id)
            .map(|(_, offset)| offset.clone())
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn delete(&self, consumer_id: &str, partition: PartitionId) -> Option<ConsumerOffset> {
        let key = (consumer_id.to_string(), partition.get());
        self.offsets.write().unwrap().remove(&key)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn delete_all_for_consumer(&self, consumer_id: &str) -> usize {
        let mut offsets = self.offsets.write().unwrap();
        let before = offsets.len();
        offsets.retain(|(cid, _), _| cid != consumer_id);
        before - offsets.len()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn offsets_on_partition(&self, partition: PartitionId) -> Vec<ConsumerOffset> {
        self.offsets
            .read()
            .unwrap()
            .iter()
            .filter(|((cid, _), _)| session_partition(cid) == partition)
            .map(|(_, offset)| offset.clone())
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn count(&self) -> usize {
        self.offsets.read().unwrap().len()
    }

    #[must_use]
    pub fn serialize(offset: &ConsumerOffset) -> Vec<u8> {
        offset.to_be_bytes()
    }

    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<ConsumerOffset> {
        ConsumerOffset::try_from_be_bytes(bytes)
            .ok()
            .map(|(o, _)| o)
    }
}

impl std::fmt::Debug for OffsetStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OffsetStore")
            .field("node_id", &self.node_id)
            .field("count", &self.count())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    fn partition(n: u16) -> PartitionId {
        PartitionId::new(n).unwrap()
    }

    #[test]
    fn consumer_offset_bebytes_roundtrip() {
        let offset = ConsumerOffset::create("consumer1", partition(10), 12345, 1000);
        let bytes = OffsetStore::serialize(&offset);
        let parsed = OffsetStore::deserialize(&bytes).unwrap();

        assert_eq!(offset.consumer_id, parsed.consumer_id);
        assert_eq!(offset.partition, parsed.partition);
        assert_eq!(offset.sequence, parsed.sequence);
        assert_eq!(offset.timestamp, parsed.timestamp);
    }

    #[test]
    fn offset_key_format() {
        let key = offset_key("consumer1", partition(10));
        assert!(key.starts_with("_offset/p"));
        assert!(key.contains("/consumer1/p10"));
    }

    #[test]
    fn commit_and_get() {
        let store = OffsetStore::new(node(1));

        store.commit("consumer1", partition(10), 100, 1000);

        let offset = store.get("consumer1", partition(10)).unwrap();
        assert_eq!(offset.sequence, 100);
        assert_eq!(offset.timestamp, 1000);
    }

    #[test]
    fn commit_updates_existing() {
        let store = OffsetStore::new(node(1));

        store.commit("consumer1", partition(10), 100, 1000);
        store.commit("consumer1", partition(10), 200, 2000);

        let offset = store.get("consumer1", partition(10)).unwrap();
        assert_eq!(offset.sequence, 200);
        assert_eq!(offset.timestamp, 2000);
    }

    #[test]
    fn multiple_partitions() {
        let store = OffsetStore::new(node(1));

        store.commit("consumer1", partition(0), 100, 1000);
        store.commit("consumer1", partition(1), 200, 1000);
        store.commit("consumer1", partition(2), 300, 1000);

        let offsets = store.get_all_for_consumer("consumer1");
        assert_eq!(offsets.len(), 3);

        assert_eq!(store.get_sequence("consumer1", partition(0)), Some(100));
        assert_eq!(store.get_sequence("consumer1", partition(1)), Some(200));
        assert_eq!(store.get_sequence("consumer1", partition(2)), Some(300));
    }

    #[test]
    fn delete_consumer() {
        let store = OffsetStore::new(node(1));

        store.commit("consumer1", partition(0), 100, 1000);
        store.commit("consumer1", partition(1), 200, 1000);
        store.commit("consumer2", partition(0), 50, 1000);

        assert_eq!(store.count(), 3);

        let deleted = store.delete_all_for_consumer("consumer1");
        assert_eq!(deleted, 2);
        assert_eq!(store.count(), 1);
        assert!(store.get("consumer1", partition(0)).is_none());
        assert!(store.get("consumer2", partition(0)).is_some());
    }
}
