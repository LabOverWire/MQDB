use super::protocol::Operation;
use super::{NodeId, PartitionId, session_partition};
use bebytes::BeBytes;
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct MqttTopicEntry {
    pub topic_len: u16,
    #[FromField(topic_len)]
    pub topic: Vec<u8>,
    pub qos: u8,
    pub is_wildcard: u8,
}

impl MqttTopicEntry {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(topic: &str, qos: u8, is_wildcard: bool) -> Self {
        let topic_bytes = topic.as_bytes().to_vec();
        Self {
            topic_len: topic_bytes.len() as u16,
            topic: topic_bytes,
            qos,
            is_wildcard: u8::from(is_wildcard),
        }
    }

    #[must_use]
    pub fn topic_str(&self) -> &str {
        std::str::from_utf8(&self.topic).unwrap_or("")
    }

    #[must_use]
    pub fn is_wildcard(&self) -> bool {
        self.is_wildcard != 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct MqttSubscriptionSnapshot {
    pub version: u8,
    pub client_id_len: u8,
    #[FromField(client_id_len)]
    pub client_id: Vec<u8>,
    pub topic_count: u16,
    #[FromField(topic_count)]
    pub topics: Vec<MqttTopicEntry>,
    pub snapshot_version: u64,
}

impl MqttSubscriptionSnapshot {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(client_id: &str) -> Self {
        let client_bytes = client_id.as_bytes().to_vec();
        Self {
            version: 1,
            client_id_len: client_bytes.len() as u8,
            client_id: client_bytes,
            topic_count: 0,
            topics: Vec::new(),
            snapshot_version: 0,
        }
    }

    #[must_use]
    pub fn client_id_str(&self) -> &str {
        std::str::from_utf8(&self.client_id).unwrap_or("")
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn add_subscription(&mut self, topic: &str, qos: u8) {
        let is_wildcard = topic.contains('+') || topic.contains('#');
        if self.topics.iter().any(|t| t.topic_str() == topic) {
            return;
        }
        self.topics
            .push(MqttTopicEntry::create(topic, qos, is_wildcard));
        self.topic_count = self.topics.len() as u16;
        self.snapshot_version += 1;
    }

    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn remove_subscription(&mut self, topic: &str) -> bool {
        let len_before = self.topics.len();
        self.topics.retain(|t| t.topic_str() != topic);
        self.topic_count = self.topics.len() as u16;
        let removed = self.topics.len() < len_before;
        if removed {
            self.snapshot_version += 1;
        }
        removed
    }

    #[must_use]
    pub fn has_subscription(&self, topic: &str) -> bool {
        self.topics.iter().any(|t| t.topic_str() == topic)
    }

    #[must_use]
    pub fn wildcard_subscriptions(&self) -> Vec<&MqttTopicEntry> {
        self.topics.iter().filter(|t| t.is_wildcard()).collect()
    }

    #[must_use]
    pub fn exact_subscriptions(&self) -> Vec<&MqttTopicEntry> {
        self.topics.iter().filter(|t| !t.is_wildcard()).collect()
    }
}

#[must_use]
pub fn mqtt_subscription_key(client_id: &str) -> String {
    let partition = session_partition(client_id);
    format!(
        "_mqtt_subs/p{}/clients/{}/topics",
        partition.get(),
        client_id
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionCacheError {
    NotFound,
    AlreadyExists,
    SerializationError,
}

impl std::fmt::Display for SubscriptionCacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "subscription not found"),
            Self::AlreadyExists => write!(f, "subscription already exists"),
            Self::SerializationError => write!(f, "serialization error"),
        }
    }
}

impl std::error::Error for SubscriptionCacheError {}

pub struct SubscriptionCache {
    node_id: NodeId,
    snapshots: RwLock<HashMap<String, MqttSubscriptionSnapshot>>,
}

impl SubscriptionCache {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            snapshots: RwLock::new(HashMap::new()),
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// This function currently always succeeds.
    pub fn add_subscription(
        &self,
        client_id: &str,
        topic: &str,
        qos: u8,
    ) -> Result<(), SubscriptionCacheError> {
        let mut snapshots = self.snapshots.write().unwrap();
        let snapshot = snapshots
            .entry(client_id.to_string())
            .or_insert_with(|| MqttSubscriptionSnapshot::create(client_id));
        snapshot.add_subscription(topic, qos);
        Ok(())
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if the client has no subscriptions.
    pub fn remove_subscription(
        &self,
        client_id: &str,
        topic: &str,
    ) -> Result<(), SubscriptionCacheError> {
        let mut snapshots = self.snapshots.write().unwrap();
        if let Some(snapshot) = snapshots.get_mut(client_id) {
            let _ = snapshot.remove_subscription(topic);
            if snapshot.topics.is_empty() {
                snapshots.remove(client_id);
            }
            Ok(())
        } else {
            Err(SubscriptionCacheError::NotFound)
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get_snapshot(&self, client_id: &str) -> Option<MqttSubscriptionSnapshot> {
        self.snapshots.read().unwrap().get(client_id).cloned()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get_subscriptions(&self, client_id: &str) -> Vec<MqttTopicEntry> {
        self.snapshots
            .read()
            .unwrap()
            .get(client_id)
            .map(|s| s.topics.clone())
            .unwrap_or_default()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn clear_client(&self, client_id: &str) -> Option<MqttSubscriptionSnapshot> {
        self.snapshots.write().unwrap().remove(client_id)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn client_count(&self) -> usize {
        self.snapshots.read().unwrap().len()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn clients_on_partition(&self, partition: PartitionId) -> Vec<String> {
        self.snapshots
            .read()
            .unwrap()
            .keys()
            .filter(|c| session_partition(c) == partition)
            .cloned()
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn add_subscription_with_data(
        &self,
        client_id: &str,
        topic: &str,
        qos: u8,
    ) -> (MqttSubscriptionSnapshot, Vec<u8>) {
        let mut snapshots = self.snapshots.write().unwrap();
        let snapshot = snapshots
            .entry(client_id.to_string())
            .or_insert_with(|| MqttSubscriptionSnapshot::create(client_id));
        snapshot.add_subscription(topic, qos);
        let data = Self::serialize(snapshot);
        (snapshot.clone(), data)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if the client has no subscriptions.
    pub fn remove_subscription_with_data(
        &self,
        client_id: &str,
        topic: &str,
    ) -> Result<(MqttSubscriptionSnapshot, Vec<u8>), SubscriptionCacheError> {
        let mut snapshots = self.snapshots.write().unwrap();
        if let Some(snapshot) = snapshots.get_mut(client_id) {
            let _ = snapshot.remove_subscription(topic);
            let data = Self::serialize(snapshot);
            let result = snapshot.clone();
            if snapshot.topics.is_empty() {
                snapshots.remove(client_id);
            }
            Ok((result, data))
        } else {
            Err(SubscriptionCacheError::NotFound)
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn clear_client_with_data(
        &self,
        client_id: &str,
    ) -> Option<(MqttSubscriptionSnapshot, Vec<u8>)> {
        let snapshot = self.snapshots.write().unwrap().remove(client_id)?;
        let data = Self::serialize(&snapshot);
        Some((snapshot, data))
    }

    #[must_use]
    pub fn serialize(snapshot: &MqttSubscriptionSnapshot) -> Vec<u8> {
        snapshot.to_be_bytes()
    }

    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<MqttSubscriptionSnapshot> {
        MqttSubscriptionSnapshot::try_from_be_bytes(bytes)
            .ok()
            .map(|(s, _)| s)
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
    ) -> Result<(), SubscriptionCacheError> {
        match operation {
            Operation::Insert | Operation::Update => {
                let snapshot =
                    Self::deserialize(data).ok_or(SubscriptionCacheError::SerializationError)?;
                let mut snapshots = self.snapshots.write().unwrap();
                snapshots.insert(id.to_string(), snapshot);
                Ok(())
            }
            Operation::Delete => {
                let mut snapshots = self.snapshots.write().unwrap();
                snapshots.remove(id);
                Ok(())
            }
        }
    }
}

impl std::fmt::Debug for SubscriptionCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionCache")
            .field("node_id", &self.node_id)
            .field("client_count", &self.client_count())
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
    fn mqtt_topic_entry_bebytes_roundtrip() {
        let entry = MqttTopicEntry::create("sensors/+/temp", 2, true);
        let bytes = entry.to_be_bytes();
        let (parsed, _) = MqttTopicEntry::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(entry.topic, parsed.topic);
        assert_eq!(entry.qos, parsed.qos);
        assert_eq!(entry.is_wildcard, parsed.is_wildcard);
    }

    #[test]
    fn mqtt_subscription_snapshot_bebytes_roundtrip() {
        let mut snapshot = MqttSubscriptionSnapshot::create("client1");
        snapshot.add_subscription("topic/a", 1);
        snapshot.add_subscription("sensors/+/temp", 2);

        let bytes = SubscriptionCache::serialize(&snapshot);
        let parsed = SubscriptionCache::deserialize(&bytes).unwrap();

        assert_eq!(snapshot.client_id, parsed.client_id);
        assert_eq!(snapshot.topics.len(), parsed.topics.len());
        assert_eq!(snapshot.snapshot_version, parsed.snapshot_version);
    }

    #[test]
    fn subscription_key_format() {
        let key = mqtt_subscription_key("client1");
        assert!(key.starts_with("_mqtt_subs/p"));
        assert!(key.ends_with("/clients/client1/topics"));
    }

    #[test]
    fn subscription_cache_add_remove() {
        let cache = SubscriptionCache::new(node(1));

        cache.add_subscription("client1", "topic/a", 1).unwrap();
        cache.add_subscription("client1", "topic/b", 2).unwrap();

        let subs = cache.get_subscriptions("client1");
        assert_eq!(subs.len(), 2);

        cache.remove_subscription("client1", "topic/a").unwrap();
        let subs = cache.get_subscriptions("client1");
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].topic_str(), "topic/b");
    }

    #[test]
    fn wildcard_detection() {
        let cache = SubscriptionCache::new(node(1));

        cache.add_subscription("client1", "exact/topic", 1).unwrap();
        cache
            .add_subscription("client1", "sensors/+/temp", 1)
            .unwrap();
        cache.add_subscription("client1", "events/#", 1).unwrap();

        let snapshot = cache.get_snapshot("client1").unwrap();
        let wildcards = snapshot.wildcard_subscriptions();
        let exact = snapshot.exact_subscriptions();

        assert_eq!(wildcards.len(), 2);
        assert_eq!(exact.len(), 1);
    }

    #[test]
    fn clear_client_removes_all() {
        let cache = SubscriptionCache::new(node(1));

        cache.add_subscription("client1", "topic/a", 1).unwrap();
        cache.add_subscription("client1", "topic/b", 2).unwrap();

        let removed = cache.clear_client("client1");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().topics.len(), 2);
        assert!(cache.get_snapshot("client1").is_none());
    }

    #[test]
    fn snapshot_version_increments() {
        let cache = SubscriptionCache::new(node(1));

        cache.add_subscription("client1", "topic/a", 1).unwrap();
        let v1 = cache.get_snapshot("client1").unwrap().snapshot_version;

        cache.add_subscription("client1", "topic/b", 1).unwrap();
        let v2 = cache.get_snapshot("client1").unwrap().snapshot_version;

        cache.remove_subscription("client1", "topic/a").unwrap();
        let v3 = cache.get_snapshot("client1").unwrap().snapshot_version;

        assert!(v2 > v1);
        assert!(v3 > v2);
    }
}
