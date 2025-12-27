use super::protocol::Operation;
use super::{NodeId, PartitionId, topic_partition};
use bebytes::BeBytes;
use std::collections::HashMap;
use std::sync::RwLock;

pub fn topic_matches_pattern(topic: &str, pattern: &str) -> bool {
    let topic_parts: Vec<&str> = topic.split('/').collect();
    let pattern_parts: Vec<&str> = pattern.split('/').collect();

    let mut t_idx = 0;
    let mut p_idx = 0;

    while p_idx < pattern_parts.len() {
        let p = pattern_parts[p_idx];

        if p == "#" {
            return true;
        }

        if t_idx >= topic_parts.len() {
            return false;
        }

        if p == "+" || p == topic_parts[t_idx] {
            t_idx += 1;
            p_idx += 1;
        } else {
            return false;
        }
    }

    t_idx == topic_parts.len() && p_idx == pattern_parts.len()
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct RetainedMessage {
    pub version: u8,
    pub topic_len: u16,
    #[FromField(topic_len)]
    pub topic: Vec<u8>,
    pub qos: u8,
    pub timestamp_ms: u64,
    pub payload_len: u32,
    #[FromField(payload_len)]
    pub payload: Vec<u8>,
}

impl RetainedMessage {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(topic: &str, qos: u8, payload: &[u8], timestamp_ms: u64) -> Self {
        let topic_bytes = topic.as_bytes().to_vec();
        Self {
            version: 1,
            topic_len: topic_bytes.len() as u16,
            topic: topic_bytes,
            qos,
            timestamp_ms,
            payload_len: payload.len() as u32,
            payload: payload.to_vec(),
        }
    }

    #[must_use]
    pub fn topic_str(&self) -> &str {
        std::str::from_utf8(&self.topic).unwrap_or("")
    }

    #[must_use]
    pub fn is_empty_payload(&self) -> bool {
        self.payload.is_empty()
    }
}

#[must_use]
pub fn retained_message_key(topic: &str) -> String {
    let partition = topic_partition(topic);
    format!("_mqtt_retained/p{}/topics/{}", partition.get(), topic)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainedStoreError {
    NotFound,
    SerializationError,
}

impl std::fmt::Display for RetainedStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "retained message not found"),
            Self::SerializationError => write!(f, "serialization error"),
        }
    }
}

impl std::error::Error for RetainedStoreError {}

pub struct RetainedStore {
    node_id: NodeId,
    messages: RwLock<HashMap<String, RetainedMessage>>,
}

impl RetainedStore {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            messages: RwLock::new(HashMap::new()),
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn set(&self, topic: &str, qos: u8, payload: &[u8], timestamp_ms: u64) {
        let mut messages = self.messages.write().unwrap();
        if payload.is_empty() {
            messages.remove(topic);
        } else {
            let msg = RetainedMessage::create(topic, qos, payload, timestamp_ms);
            messages.insert(topic.to_string(), msg);
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get(&self, topic: &str) -> Option<RetainedMessage> {
        self.messages.read().unwrap().get(topic).cloned()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn remove(&self, topic: &str) -> Option<RetainedMessage> {
        self.messages.write().unwrap().remove(topic)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn topics_on_partition(&self, partition: PartitionId) -> Vec<String> {
        self.messages
            .read()
            .unwrap()
            .keys()
            .filter(|t| topic_partition(t) == partition)
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn get_matching_exact(&self, topic: &str) -> Option<RetainedMessage> {
        self.get(topic)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn message_count(&self) -> usize {
        self.messages.read().unwrap().len()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn all_topics(&self) -> Vec<String> {
        self.messages.read().unwrap().keys().cloned().collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn set_with_data(
        &self,
        topic: &str,
        qos: u8,
        payload: &[u8],
        timestamp_ms: u64,
    ) -> (Option<RetainedMessage>, Vec<u8>) {
        let mut messages = self.messages.write().unwrap();
        if payload.is_empty() {
            let removed = messages.remove(topic);
            let data = removed.as_ref().map(Self::serialize).unwrap_or_default();
            (removed, data)
        } else {
            let msg = RetainedMessage::create(topic, qos, payload, timestamp_ms);
            let data = Self::serialize(&msg);
            messages.insert(topic.to_string(), msg.clone());
            (Some(msg), data)
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn remove_with_data(&self, topic: &str) -> Option<(RetainedMessage, Vec<u8>)> {
        let msg = self.messages.write().unwrap().remove(topic)?;
        let data = Self::serialize(&msg);
        Some((msg, data))
    }

    #[must_use]
    pub fn serialize(msg: &RetainedMessage) -> Vec<u8> {
        msg.to_be_bytes()
    }

    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<RetainedMessage> {
        RetainedMessage::try_from_be_bytes(bytes)
            .ok()
            .map(|(m, _)| m)
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
    ) -> Result<(), RetainedStoreError> {
        match operation {
            Operation::Insert | Operation::Update => {
                let msg = Self::deserialize(data).ok_or(RetainedStoreError::SerializationError)?;
                let mut messages = self.messages.write().unwrap();
                messages.insert(id.to_string(), msg);
                Ok(())
            }
            Operation::Delete => {
                let mut messages = self.messages.write().unwrap();
                messages.remove(id);
                Ok(())
            }
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn export_for_partition(&self, partition: PartitionId) -> Vec<u8> {
        let messages = self.messages.read().unwrap();
        let partition_messages: Vec<_> = messages
            .iter()
            .filter(|(topic, _)| topic_partition(topic) == partition)
            .collect();

        let mut buf = Vec::new();
        buf.extend_from_slice(&(partition_messages.len() as u32).to_be_bytes());

        for (topic, msg) in partition_messages {
            let id_bytes = topic.as_bytes();
            buf.extend_from_slice(&(id_bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(id_bytes);

            let data = msg.to_be_bytes();
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
    pub fn import_retained(&self, data: &[u8]) -> Result<usize, RetainedStoreError> {
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
                .map_err(|_| RetainedStoreError::SerializationError)?;
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
            let msg_data = &data[offset..offset + data_len];
            offset += data_len;

            if let Some(msg) = Self::deserialize(msg_data) {
                self.messages.write().unwrap().insert(topic.to_string(), msg);
                imported += 1;
            }
        }

        Ok(imported)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn clear_partition(&self, partition: PartitionId) -> usize {
        let mut messages = self.messages.write().unwrap();
        let before = messages.len();
        messages.retain(|topic, _| topic_partition(topic) != partition);
        before - messages.len()
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
    ) -> (Vec<RetainedMessage>, bool, Option<Vec<u8>>) {
        let messages = self.messages.read().unwrap();
        let start_key = cursor.and_then(|c| std::str::from_utf8(c).ok());

        let mut results: Vec<_> = messages
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

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn query_matching_pattern(&self, pattern: &str) -> Vec<RetainedMessage> {
        let messages = self.messages.read().unwrap();
        messages
            .iter()
            .filter(|(topic, _)| topic_matches_pattern(topic, pattern))
            .map(|(_, msg)| msg.clone())
            .collect()
    }

    #[must_use]
    pub fn get_exact(&self, topic: &str) -> Option<RetainedMessage> {
        self.get(topic)
    }
}

impl std::fmt::Debug for RetainedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetainedStore")
            .field("node_id", &self.node_id)
            .field("message_count", &self.message_count())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    #[test]
    fn retained_message_bebytes_roundtrip() {
        let msg = RetainedMessage::create("sensors/temp", 1, b"25.5", 1000);
        let bytes = RetainedStore::serialize(&msg);
        let parsed = RetainedStore::deserialize(&bytes).unwrap();

        assert_eq!(msg.topic, parsed.topic);
        assert_eq!(msg.qos, parsed.qos);
        assert_eq!(msg.payload, parsed.payload);
        assert_eq!(msg.timestamp_ms, parsed.timestamp_ms);
    }

    #[test]
    fn retained_message_key_format() {
        let key = retained_message_key("sensors/temp");
        assert!(key.starts_with("_mqtt_retained/p"));
        assert!(key.ends_with("/topics/sensors/temp"));
    }

    #[test]
    fn retained_store_set_get() {
        let store = RetainedStore::new(node(1));

        store.set("sensors/temp", 1, b"25.5", 1000);
        let msg = store.get("sensors/temp").unwrap();

        assert_eq!(msg.topic_str(), "sensors/temp");
        assert_eq!(msg.payload, b"25.5");
    }

    #[test]
    fn retained_store_empty_payload_deletes() {
        let store = RetainedStore::new(node(1));

        store.set("sensors/temp", 1, b"25.5", 1000);
        assert!(store.get("sensors/temp").is_some());

        store.set("sensors/temp", 1, b"", 2000);
        assert!(store.get("sensors/temp").is_none());
    }

    #[test]
    fn retained_store_update_replaces() {
        let store = RetainedStore::new(node(1));

        store.set("sensors/temp", 1, b"25.5", 1000);
        store.set("sensors/temp", 2, b"30.0", 2000);

        let msg = store.get("sensors/temp").unwrap();
        assert_eq!(msg.payload, b"30.0");
        assert_eq!(msg.qos, 2);
        assert_eq!(msg.timestamp_ms, 2000);
    }

    #[test]
    fn retained_store_multiple_topics() {
        let store = RetainedStore::new(node(1));

        store.set("sensors/temp", 1, b"25.5", 1000);
        store.set("sensors/humidity", 1, b"65%", 1000);
        store.set("actuators/fan", 0, b"on", 1000);

        assert_eq!(store.message_count(), 3);

        let topics = store.all_topics();
        assert_eq!(topics.len(), 3);
    }

    #[test]
    fn topic_matches_pattern_exact() {
        assert!(topic_matches_pattern("sensors/temp", "sensors/temp"));
        assert!(!topic_matches_pattern("sensors/temp", "sensors/humidity"));
        assert!(!topic_matches_pattern("sensors/temp", "sensors"));
    }

    #[test]
    fn topic_matches_pattern_single_level_wildcard() {
        assert!(topic_matches_pattern("sensors/building1/temp", "sensors/+/temp"));
        assert!(topic_matches_pattern("sensors/building2/temp", "sensors/+/temp"));
        assert!(!topic_matches_pattern("sensors/temp", "sensors/+/temp"));
        assert!(!topic_matches_pattern("sensors/a/b/temp", "sensors/+/temp"));
    }

    #[test]
    fn topic_matches_pattern_multi_level_wildcard() {
        assert!(topic_matches_pattern("sensors", "sensors/#"));
        assert!(topic_matches_pattern("sensors/temp", "sensors/#"));
        assert!(topic_matches_pattern("sensors/building1/temp", "sensors/#"));
        assert!(topic_matches_pattern("sensors/a/b/c/d", "sensors/#"));
        assert!(!topic_matches_pattern("actuators/fan", "sensors/#"));
    }

    #[test]
    fn topic_matches_pattern_combined() {
        assert!(topic_matches_pattern("home/living/temp", "home/+/temp"));
        assert!(topic_matches_pattern("home/kitchen/temp", "home/+/temp"));
        assert!(topic_matches_pattern("home/living/sensors/temp", "home/+/sensors/#"));
        assert!(topic_matches_pattern("home/living/sensors/a/b", "home/+/sensors/#"));
    }

    #[test]
    fn query_matching_pattern_returns_matches() {
        let store = RetainedStore::new(node(1));
        store.set("sensors/building1/temp", 1, b"25.0", 1000);
        store.set("sensors/building2/temp", 1, b"26.0", 1000);
        store.set("sensors/building1/humidity", 1, b"65%", 1000);
        store.set("actuators/fan", 0, b"on", 1000);

        let matches = store.query_matching_pattern("sensors/+/temp");
        assert_eq!(matches.len(), 2);

        let matches = store.query_matching_pattern("sensors/#");
        assert_eq!(matches.len(), 3);

        let matches = store.query_matching_pattern("actuators/+");
        assert_eq!(matches.len(), 1);
    }
}
