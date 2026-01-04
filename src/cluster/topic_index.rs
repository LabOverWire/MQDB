use super::protocol::Operation;
use super::{NUM_PARTITIONS, NodeId, PartitionId};
use bebytes::BeBytes;
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct SubscriberLocation {
    pub client_id_len: u8,
    #[FromField(client_id_len)]
    pub client_id: Vec<u8>,
    pub client_partition: u16,
    pub qos: u8,
}

impl SubscriberLocation {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(client_id: &str, client_partition: PartitionId, qos: u8) -> Self {
        let client_bytes = client_id.as_bytes().to_vec();
        Self {
            client_id_len: client_bytes.len() as u8,
            client_id: client_bytes,
            client_partition: client_partition.get(),
            qos,
        }
    }

    #[must_use]
    pub fn client_id_str(&self) -> &str {
        std::str::from_utf8(&self.client_id).unwrap_or("")
    }

    #[must_use]
    pub fn partition(&self) -> Option<PartitionId> {
        PartitionId::new(self.client_partition)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct TopicIndexEntry {
    pub version: u8,
    pub topic_len: u16,
    #[FromField(topic_len)]
    pub topic: Vec<u8>,
    pub last_seq: u64,
    pub subscriber_count: u16,
    #[FromField(subscriber_count)]
    pub subscribers: Vec<SubscriberLocation>,
}

impl TopicIndexEntry {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(topic: &str) -> Self {
        let topic_bytes = topic.as_bytes().to_vec();
        Self {
            version: 1,
            topic_len: topic_bytes.len() as u16,
            topic: topic_bytes,
            last_seq: 0,
            subscriber_count: 0,
            subscribers: Vec::new(),
        }
    }

    #[must_use]
    pub fn topic_str(&self) -> &str {
        std::str::from_utf8(&self.topic).unwrap_or("")
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn add_subscriber(&mut self, client_id: &str, client_partition: PartitionId, qos: u8) {
        if self
            .subscribers
            .iter()
            .any(|s| s.client_id_str() == client_id)
        {
            return;
        }
        self.subscribers
            .push(SubscriberLocation::create(client_id, client_partition, qos));
        self.subscriber_count = self.subscribers.len() as u16;
    }

    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn remove_subscriber(&mut self, client_id: &str) -> bool {
        let len_before = self.subscribers.len();
        self.subscribers.retain(|s| s.client_id_str() != client_id);
        self.subscriber_count = self.subscribers.len() as u16;
        self.subscribers.len() != len_before
    }

    pub fn next_seq(&mut self) -> u64 {
        self.last_seq += 1;
        self.last_seq
    }
}

/// # Panics
/// Panics if the partition number is out of range.
#[allow(clippy::cast_possible_truncation)]
#[must_use]
pub fn topic_partition(topic: &str) -> PartitionId {
    let hash = crc32fast::hash(topic.as_bytes());
    let partition_num = (hash % u32::from(NUM_PARTITIONS)) as u16;
    PartitionId::new(partition_num).unwrap()
}

#[must_use]
pub fn topic_index_key(topic: &str) -> String {
    let partition = topic_partition(topic);
    format!("_topic_index/p{}/topics/{}", partition.get(), topic)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicIndexError {
    NotFound,
    AlreadySubscribed,
    SerializationError,
}

impl std::fmt::Display for TopicIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "topic not found"),
            Self::AlreadySubscribed => write!(f, "already subscribed"),
            Self::SerializationError => write!(f, "serialization error"),
        }
    }
}

impl std::error::Error for TopicIndexError {}

pub struct TopicIndex {
    node_id: NodeId,
    entries: RwLock<HashMap<String, TopicIndexEntry>>,
}

impl TopicIndex {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// This function currently always succeeds.
    pub fn subscribe(
        &self,
        topic: &str,
        client_id: &str,
        client_partition: PartitionId,
        qos: u8,
    ) -> Result<(), TopicIndexError> {
        tracing::debug!(topic, client_id, "subscribe: local subscribe");
        let mut entries = self.entries.write().unwrap();
        let entry = entries
            .entry(topic.to_string())
            .or_insert_with(|| TopicIndexEntry::create(topic));
        entry.add_subscriber(client_id, client_partition, qos);
        tracing::debug!(
            topic,
            count = entry.subscribers.len(),
            "subscribe: after add"
        );
        Ok(())
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if the topic does not exist.
    pub fn unsubscribe(&self, topic: &str, client_id: &str) -> Result<(), TopicIndexError> {
        tracing::debug!(topic, client_id, "unsubscribe called");
        let mut entries = self.entries.write().unwrap();
        if let Some(entry) = entries.get_mut(topic) {
            let _ = entry.remove_subscriber(client_id);
            if entry.subscribers.is_empty() {
                tracing::debug!(topic, "unsubscribe: removing empty entry");
                entries.remove(topic);
            }
            Ok(())
        } else {
            Err(TopicIndexError::NotFound)
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get_subscribers(&self, topic: &str) -> Vec<SubscriberLocation> {
        let entries = self.entries.read().unwrap();
        let result = entries
            .get(topic)
            .map(|e| e.subscribers.clone())
            .unwrap_or_default();
        tracing::debug!(
            topic,
            entry_exists = entries.contains_key(topic),
            subscriber_count = result.len(),
            total_topics = entries.len(),
            "get_subscribers lookup"
        );
        result
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get_entry(&self, topic: &str) -> Option<TopicIndexEntry> {
        self.entries.read().unwrap().get(topic).cloned()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn next_seq(&self, topic: &str) -> Option<u64> {
        let mut entries = self.entries.write().unwrap();
        entries.get_mut(topic).map(TopicIndexEntry::next_seq)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn topics_on_partition(&self, partition: PartitionId) -> Vec<String> {
        self.entries
            .read()
            .unwrap()
            .keys()
            .filter(|t| topic_partition(t) == partition)
            .cloned()
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn topic_count(&self) -> usize {
        self.entries.read().unwrap().len()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get_client_topics(&self, client_id: &str) -> Vec<(String, u8)> {
        self.entries
            .read()
            .unwrap()
            .iter()
            .filter_map(|(topic, entry)| {
                entry
                    .subscribers
                    .iter()
                    .find(|s| s.client_id_str() == client_id)
                    .map(|s| (topic.clone(), s.qos))
            })
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn subscribe_with_data(
        &self,
        topic: &str,
        client_id: &str,
        client_partition: PartitionId,
        qos: u8,
    ) -> (TopicIndexEntry, Vec<u8>) {
        let mut entries = self.entries.write().unwrap();
        let entry = entries
            .entry(topic.to_string())
            .or_insert_with(|| TopicIndexEntry::create(topic));
        entry.add_subscriber(client_id, client_partition, qos);
        let data = Self::serialize(entry);
        (entry.clone(), data)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if the topic does not exist.
    pub fn unsubscribe_with_data(
        &self,
        topic: &str,
        client_id: &str,
    ) -> Result<(TopicIndexEntry, Vec<u8>), TopicIndexError> {
        let mut entries = self.entries.write().unwrap();
        if let Some(entry) = entries.get_mut(topic) {
            let _ = entry.remove_subscriber(client_id);
            let data = Self::serialize(entry);
            let result = entry.clone();
            if entry.subscribers.is_empty() {
                entries.remove(topic);
            }
            Ok((result, data))
        } else {
            Err(TopicIndexError::NotFound)
        }
    }

    #[must_use]
    pub fn serialize(entry: &TopicIndexEntry) -> Vec<u8> {
        entry.to_be_bytes()
    }

    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<TopicIndexEntry> {
        TopicIndexEntry::try_from_be_bytes(bytes)
            .ok()
            .map(|(e, _)| e)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `SerializationError` if deserialization fails.
    pub fn apply_replicated(
        &self,
        operation: Operation,
        id: &str,
        data: &[u8],
    ) -> Result<(), TopicIndexError> {
        match operation {
            Operation::Insert | Operation::Update => {
                let incoming =
                    Self::deserialize(data).ok_or(TopicIndexError::SerializationError)?;
                tracing::debug!(
                    id,
                    incoming_subscribers = incoming.subscribers.len(),
                    "apply_replicated: deserialized entry"
                );
                let mut entries = self.entries.write().unwrap();
                let existed_before = entries.contains_key(id);
                let before_count = entries.get(id).map_or(0, |e| e.subscribers.len());
                if incoming.subscribers.is_empty() {
                    entries.remove(id);
                    tracing::debug!(
                        id,
                        existed_before,
                        before_count,
                        "apply_replicated: entry removed (no subscribers)"
                    );
                } else {
                    entries.insert(id.to_string(), incoming);
                    tracing::debug!(
                        id,
                        existed_before,
                        before_count,
                        final_count = entries.get(id).map_or(0, |e| e.subscribers.len()),
                        "apply_replicated: entry replaced"
                    );
                }
                Ok(())
            }
            Operation::Delete => {
                let mut entries = self.entries.write().unwrap();
                entries.remove(id);
                Ok(())
            }
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn export_for_partition(&self, partition: PartitionId) -> Vec<u8> {
        let entries = self.entries.read().unwrap();
        let partition_entries: Vec<_> = entries
            .iter()
            .filter(|(topic, _)| topic_partition(topic) == partition)
            .collect();

        let mut buf = Vec::new();
        buf.extend_from_slice(&(partition_entries.len() as u32).to_be_bytes());

        for (topic, entry) in partition_entries {
            let id_bytes = topic.as_bytes();
            buf.extend_from_slice(&(id_bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(id_bytes);

            let data = entry.to_be_bytes();
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
    pub fn import_entries(&self, data: &[u8]) -> Result<usize, TopicIndexError> {
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
            let topic = std::str::from_utf8(&data[offset..offset + id_len])
                .map_err(|_| TopicIndexError::SerializationError)?;
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
            let entry_data = &data[offset..offset + data_len];
            offset += data_len;

            if let Some(entry) = Self::deserialize(entry_data) {
                tracing::debug!(
                    topic,
                    subscribers = entry.subscribers.len(),
                    "import_entries: replacing entry"
                );
                self.entries
                    .write()
                    .unwrap()
                    .insert(topic.to_string(), entry);
                imported += 1;
            }
        }

        Ok(imported)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn clear_partition(&self, partition: PartitionId) -> usize {
        tracing::debug!(partition = partition.get(), "clear_partition called");
        let mut entries = self.entries.write().unwrap();
        let before = entries.len();
        entries.retain(|topic, _| topic_partition(topic) != partition);
        let removed = before - entries.len();
        if removed > 0 {
            tracing::debug!(
                partition = partition.get(),
                removed,
                "clear_partition: removed entries"
            );
        }
        removed
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
    ) -> (Vec<TopicIndexEntry>, bool, Option<Vec<u8>>) {
        let entries = self.entries.read().unwrap();
        let start_key = cursor.and_then(|c| std::str::from_utf8(c).ok());

        let mut results: Vec<_> = entries
            .iter()
            .filter(|(key, _)| start_key.is_none_or(|sk| key.as_str() > sk))
            .take(limit as usize + 1)
            .collect();

        results.sort_by_key(|(k, _)| k.as_str());
        let has_more = results.len() > limit as usize;
        if has_more {
            results.pop();
        }

        let next_cursor = if has_more {
            results.last().map(|(k, _)| k.as_bytes().to_vec())
        } else {
            None
        };

        let data = results.into_iter().map(|(_, v)| v.clone()).collect();
        (data, has_more, next_cursor)
    }
}

impl std::fmt::Debug for TopicIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicIndex")
            .field("node_id", &self.node_id)
            .field("topic_count", &self.topic_count())
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
    fn topic_partition_deterministic() {
        let p1 = topic_partition("sensors/temp");
        let p2 = topic_partition("sensors/temp");
        assert_eq!(p1, p2);
    }

    #[test]
    fn topic_index_key_format() {
        let key = topic_index_key("sensors/temp");
        assert!(key.starts_with("_topic_index/p"));
        assert!(key.ends_with("/topics/sensors/temp"));
    }

    #[test]
    fn subscriber_location_bebytes_roundtrip() {
        let sub = SubscriberLocation::create("client1", partition(5), 2);
        let bytes = sub.to_be_bytes();
        let (parsed, _) = SubscriberLocation::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(sub.client_id, parsed.client_id);
        assert_eq!(sub.client_partition, parsed.client_partition);
        assert_eq!(sub.qos, parsed.qos);
    }

    #[test]
    fn topic_index_entry_bebytes_roundtrip() {
        let mut entry = TopicIndexEntry::create("sensors/temp");
        entry.add_subscriber("client1", partition(12), 1);
        entry.add_subscriber("client2", partition(45), 2);
        entry.next_seq();
        entry.next_seq();

        let bytes = TopicIndex::serialize(&entry);
        let parsed = TopicIndex::deserialize(&bytes).unwrap();

        assert_eq!(entry.topic, parsed.topic);
        assert_eq!(entry.last_seq, parsed.last_seq);
        assert_eq!(entry.subscribers.len(), parsed.subscribers.len());
    }

    #[test]
    fn topic_index_subscribe_unsubscribe() {
        let index = TopicIndex::new(node(1));

        index
            .subscribe("topic/a", "client1", partition(10), 1)
            .unwrap();
        index
            .subscribe("topic/a", "client2", partition(20), 2)
            .unwrap();

        let subs = index.get_subscribers("topic/a");
        assert_eq!(subs.len(), 2);

        index.unsubscribe("topic/a", "client1").unwrap();
        let subs = index.get_subscribers("topic/a");
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].client_id_str(), "client2");

        index.unsubscribe("topic/a", "client2").unwrap();
        assert!(index.get_entry("topic/a").is_none());
    }

    #[test]
    fn topic_index_sequence() {
        let index = TopicIndex::new(node(1));
        index
            .subscribe("topic/a", "client1", partition(10), 1)
            .unwrap();

        assert_eq!(index.next_seq("topic/a"), Some(1));
        assert_eq!(index.next_seq("topic/a"), Some(2));
        assert_eq!(index.next_seq("topic/a"), Some(3));

        assert_eq!(index.next_seq("nonexistent"), None);
    }

    #[test]
    fn duplicate_subscribe_ignored() {
        let index = TopicIndex::new(node(1));
        index
            .subscribe("topic/a", "client1", partition(10), 1)
            .unwrap();
        index
            .subscribe("topic/a", "client1", partition(10), 1)
            .unwrap();

        let subs = index.get_subscribers("topic/a");
        assert_eq!(subs.len(), 1);
    }
}
