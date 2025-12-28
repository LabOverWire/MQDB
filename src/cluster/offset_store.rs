use super::protocol::Operation;
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

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn commit_with_data(
        &self,
        consumer_id: &str,
        partition: PartitionId,
        sequence: u64,
        timestamp: u64,
    ) -> (ConsumerOffset, Vec<u8>) {
        let key = (consumer_id.to_string(), partition.get());
        let offset = ConsumerOffset::create(consumer_id, partition, sequence, timestamp);
        let data = Self::serialize(&offset);
        self.offsets.write().unwrap().insert(key, offset.clone());
        (offset, data)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn delete_with_data(
        &self,
        consumer_id: &str,
        partition: PartitionId,
    ) -> Option<(ConsumerOffset, Vec<u8>)> {
        let key = (consumer_id.to_string(), partition.get());
        let offset = self.offsets.write().unwrap().remove(&key)?;
        let data = Self::serialize(&offset);
        Some((offset, data))
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

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns an error if ID parsing or deserialization fails.
    pub fn apply_replicated(
        &self,
        operation: Operation,
        id: &str,
        data: &[u8],
    ) -> Result<(), OffsetStoreError> {
        let parts: Vec<&str> = id.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(OffsetStoreError::InvalidPartition);
        }
        let consumer_id = parts[0];
        let partition_num: u16 = parts[1]
            .parse()
            .map_err(|_| OffsetStoreError::InvalidPartition)?;

        match operation {
            Operation::Insert | Operation::Update => {
                let offset = Self::deserialize(data).ok_or(OffsetStoreError::SerializationError)?;
                let key = (consumer_id.to_string(), partition_num);
                let mut offsets = self.offsets.write().unwrap();
                offsets.insert(key, offset);
                Ok(())
            }
            Operation::Delete => {
                let key = (consumer_id.to_string(), partition_num);
                let mut offsets = self.offsets.write().unwrap();
                offsets.remove(&key);
                Ok(())
            }
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn export_for_partition(&self, partition: PartitionId) -> Vec<u8> {
        let offsets = self.offsets.read().unwrap();
        let partition_offsets: Vec<_> = offsets
            .iter()
            .filter(|((cid, _), _)| session_partition(cid) == partition)
            .collect();

        let mut buf = Vec::new();
        buf.extend_from_slice(&(partition_offsets.len() as u32).to_be_bytes());

        for ((consumer_id, part_num), offset) in partition_offsets {
            let id = format!("{consumer_id}:{part_num}");
            let id_bytes = id.as_bytes();
            buf.extend_from_slice(&(id_bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(id_bytes);

            let data = offset.to_be_bytes();
            buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
            buf.extend_from_slice(&data);
        }

        buf
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `SerializationError` if UTF-8 parsing fails.
    pub fn import_offsets(&self, data: &[u8]) -> Result<usize, OffsetStoreError> {
        if data.len() < 4 {
            return Ok(0);
        }

        let count = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut offset = 4;
        let mut imported = 0;

        for _ in 0..count {
            if offset + 2 > data.len() {
                break;
            }
            let id_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2;

            if offset + id_len > data.len() {
                break;
            }
            let id = std::str::from_utf8(&data[offset..offset + id_len])
                .map_err(|_| OffsetStoreError::SerializationError)?;
            offset += id_len;

            if offset + 4 > data.len() {
                break;
            }
            let data_len = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + data_len > data.len() {
                break;
            }
            let offset_data = &data[offset..offset + data_len];
            offset += data_len;

            let parts: Vec<&str> = id.splitn(2, ':').collect();
            if parts.len() != 2 {
                continue;
            }
            let consumer_id = parts[0];
            let partition_num: u16 = match parts[1].parse() {
                Ok(p) => p,
                Err(_) => continue,
            };

            if let Some(consumer_offset) = Self::deserialize(offset_data) {
                let key = (consumer_id.to_string(), partition_num);
                self.offsets.write().unwrap().insert(key, consumer_offset);
                imported += 1;
            }
        }

        Ok(imported)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn clear_partition(&self, partition: PartitionId) -> usize {
        let mut offsets = self.offsets.write().unwrap();
        let before = offsets.len();
        offsets.retain(|(cid, _), _| session_partition(cid) != partition);
        before - offsets.len()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn query(
        &self,
        _filter: Option<&str>,
        limit: u32,
        cursor: Option<&[u8]>,
    ) -> (Vec<ConsumerOffset>, bool, Option<Vec<u8>>) {
        let offsets = self.offsets.read().unwrap();
        let start_key = cursor.and_then(|c| std::str::from_utf8(c).ok());

        let mut results: Vec<_> = offsets
            .iter()
            .filter(|((cid, topic), _)| {
                let key = format!("{cid}:{topic}");
                start_key.is_none_or(|sk| key.as_str() > sk)
            })
            .take(limit as usize + 1)
            .collect();

        results.sort_by_key(|((cid, topic), _)| format!("{cid}:{topic}"));
        let has_more = results.len() > limit as usize;
        if has_more {
            results.pop();
        }

        let next_cursor = if has_more {
            results
                .last()
                .map(|((cid, topic), _)| format!("{cid}:{topic}").into_bytes())
        } else {
            None
        };

        let data = results.into_iter().map(|(_, v)| v.clone()).collect();
        (data, has_more, next_cursor)
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
