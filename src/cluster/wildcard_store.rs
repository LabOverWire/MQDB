use super::protocol::Operation;
use super::store_utils;
use super::{
    NodeId, PartitionId, SubscriberLocation, SubscriptionType, TopicTrie, WildcardSubscriber,
    is_wildcard_pattern, validate_pattern,
};
use bebytes::BeBytes;
use std::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct WildcardEntry {
    pub version: u8,
    pub pattern_len: u16,
    #[FromField(pattern_len)]
    pub pattern: Vec<u8>,
    pub client_id_len: u8,
    #[FromField(client_id_len)]
    pub client_id: Vec<u8>,
    pub client_partition: u16,
    pub qos: u8,
    pub subscription_type: u8,
}

impl WildcardEntry {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(
        pattern: &str,
        client_id: &str,
        client_partition: PartitionId,
        qos: u8,
        subscription_type: SubscriptionType,
    ) -> Self {
        let pattern_bytes = pattern.as_bytes().to_vec();
        let client_bytes = client_id.as_bytes().to_vec();
        Self {
            version: 1,
            pattern_len: pattern_bytes.len() as u16,
            pattern: pattern_bytes,
            client_id_len: client_bytes.len() as u8,
            client_id: client_bytes,
            client_partition: client_partition.get(),
            qos,
            subscription_type: match subscription_type {
                SubscriptionType::Mqtt => 0,
                SubscriptionType::Db => 1,
            },
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn from_subscriber(pattern: &str, sub: &WildcardSubscriber) -> Self {
        let pattern_bytes = pattern.as_bytes().to_vec();
        let client_bytes = sub.client_id.as_bytes().to_vec();
        Self {
            version: 1,
            pattern_len: pattern_bytes.len() as u16,
            pattern: pattern_bytes,
            client_id_len: client_bytes.len() as u8,
            client_id: client_bytes,
            client_partition: sub.client_partition.get(),
            qos: sub.qos,
            subscription_type: match sub.subscription_type {
                SubscriptionType::Mqtt => 0,
                SubscriptionType::Db => 1,
            },
        }
    }

    #[must_use]
    pub fn pattern_str(&self) -> &str {
        store_utils::bytes_to_str(&self.pattern)
    }

    #[must_use]
    pub fn client_id_str(&self) -> &str {
        store_utils::bytes_to_str(&self.client_id)
    }

    #[must_use]
    pub fn partition(&self) -> Option<PartitionId> {
        PartitionId::new(self.client_partition)
    }

    #[must_use]
    pub fn sub_type(&self) -> SubscriptionType {
        match self.subscription_type {
            0 => SubscriptionType::Mqtt,
            _ => SubscriptionType::Db,
        }
    }
}

#[must_use]
pub fn wildcard_key(pattern: &str, client_id: &str) -> String {
    let hash = crc32fast::hash(pattern.as_bytes());
    format!("_wildcards/{hash:08x}/{client_id}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WildcardStoreError {
    InvalidPattern,
    NotWildcard,
    NotFound,
    SerializationError,
}

impl std::fmt::Display for WildcardStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPattern => write!(f, "invalid wildcard pattern"),
            Self::NotWildcard => write!(f, "pattern is not a wildcard"),
            Self::NotFound => write!(f, "wildcard subscription not found"),
            Self::SerializationError => write!(f, "serialization error"),
        }
    }
}

impl std::error::Error for WildcardStoreError {}

pub struct WildcardStore {
    node_id: NodeId,
    trie: RwLock<TopicTrie>,
}

impl WildcardStore {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            trie: RwLock::new(TopicTrie::new()),
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `InvalidPattern` or `NotWildcard` for invalid patterns.
    pub fn subscribe(
        &self,
        pattern: &str,
        client_id: &str,
        qos: u8,
        subscription_type: SubscriptionType,
    ) -> Result<(), WildcardStoreError> {
        if !validate_pattern(pattern) {
            return Err(WildcardStoreError::InvalidPattern);
        }
        if !is_wildcard_pattern(pattern) {
            return Err(WildcardStoreError::NotWildcard);
        }

        let subscriber = match subscription_type {
            SubscriptionType::Mqtt => WildcardSubscriber::mqtt(client_id, qos),
            SubscriptionType::Db => WildcardSubscriber::db(client_id, qos),
        };

        self.trie.write().unwrap().insert(pattern, subscriber);
        Ok(())
    }

    /// # Errors
    /// Returns `InvalidPattern` or `NotWildcard` for invalid patterns.
    pub fn subscribe_mqtt(
        &self,
        pattern: &str,
        client_id: &str,
        qos: u8,
    ) -> Result<(), WildcardStoreError> {
        self.subscribe(pattern, client_id, qos, SubscriptionType::Mqtt)
    }

    /// # Errors
    /// Returns `InvalidPattern` or `NotWildcard` for invalid patterns.
    pub fn subscribe_db(
        &self,
        pattern: &str,
        client_id: &str,
        qos: u8,
    ) -> Result<(), WildcardStoreError> {
        self.subscribe(pattern, client_id, qos, SubscriptionType::Db)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `InvalidPattern` or `NotFound` for invalid/missing patterns.
    pub fn unsubscribe(&self, pattern: &str, client_id: &str) -> Result<(), WildcardStoreError> {
        if !validate_pattern(pattern) {
            return Err(WildcardStoreError::InvalidPattern);
        }

        let removed = self.trie.write().unwrap().remove(pattern, client_id);
        if removed {
            Ok(())
        } else {
            Err(WildcardStoreError::NotFound)
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn match_topic(&self, topic: &str) -> Vec<SubscriberLocation> {
        let trie = self.trie.read().unwrap();
        trie.match_topic(topic)
            .into_iter()
            .map(|sub| SubscriberLocation::create(&sub.client_id, sub.client_partition, sub.qos))
            .collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn pattern_count(&self) -> usize {
        self.trie.read().unwrap().pattern_count()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.trie.read().unwrap().is_empty()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get_client_patterns(&self, client_id: &str) -> Vec<(String, u8)> {
        self.trie
            .read()
            .unwrap()
            .all_subscriptions()
            .into_iter()
            .filter(|(_, sub)| sub.client_id == client_id)
            .map(|(pattern, sub)| (pattern, sub.qos))
            .collect()
    }

    #[must_use]
    pub fn serialize_entry(entry: &WildcardEntry) -> Vec<u8> {
        store_utils::serialize(entry)
    }

    #[must_use]
    pub fn deserialize_entry(bytes: &[u8]) -> Option<WildcardEntry> {
        store_utils::deserialize(bytes)
    }

    #[must_use]
    pub fn create_entry(
        &self,
        pattern: &str,
        client_id: &str,
        client_partition: PartitionId,
        qos: u8,
        subscription_type: SubscriptionType,
    ) -> WildcardEntry {
        WildcardEntry::create(pattern, client_id, client_partition, qos, subscription_type)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `InvalidPattern` or `NotWildcard` for invalid patterns.
    pub fn subscribe_with_data(
        &self,
        pattern: &str,
        client_id: &str,
        client_partition: PartitionId,
        qos: u8,
        subscription_type: SubscriptionType,
    ) -> Result<(WildcardEntry, Vec<u8>), WildcardStoreError> {
        if !validate_pattern(pattern) {
            return Err(WildcardStoreError::InvalidPattern);
        }
        if !is_wildcard_pattern(pattern) {
            return Err(WildcardStoreError::NotWildcard);
        }

        let subscriber = match subscription_type {
            SubscriptionType::Mqtt => WildcardSubscriber::mqtt(client_id, qos),
            SubscriptionType::Db => WildcardSubscriber::db(client_id, qos),
        };

        self.trie.write().unwrap().insert(pattern, subscriber);

        let entry =
            WildcardEntry::create(pattern, client_id, client_partition, qos, subscription_type);
        let data = Self::serialize_entry(&entry);
        Ok((entry, data))
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `InvalidPattern` or `NotFound` for invalid/missing patterns.
    pub fn unsubscribe_with_data(
        &self,
        pattern: &str,
        client_id: &str,
    ) -> Result<Vec<u8>, WildcardStoreError> {
        if !validate_pattern(pattern) {
            return Err(WildcardStoreError::InvalidPattern);
        }

        let removed = self.trie.write().unwrap().remove(pattern, client_id);
        if removed {
            Ok(Vec::new())
        } else {
            Err(WildcardStoreError::NotFound)
        }
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
    ) -> Result<(), WildcardStoreError> {
        let parts: Vec<&str> = id.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(WildcardStoreError::InvalidPattern);
        }
        let pattern = parts[0];
        let client_id = parts[1];

        match operation {
            Operation::Insert | Operation::Update => {
                let entry =
                    Self::deserialize_entry(data).ok_or(WildcardStoreError::SerializationError)?;
                let subscriber = WildcardSubscriber {
                    client_id: client_id.to_string(),
                    client_partition: entry.partition().unwrap_or(PartitionId::ZERO),
                    qos: entry.qos,
                    subscription_type: entry.sub_type(),
                };
                self.trie.write().unwrap().insert(pattern, subscriber);
                Ok(())
            }
            Operation::Delete => {
                self.trie.write().unwrap().remove(pattern, client_id);
                Ok(())
            }
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn export_for_partition(&self, partition: PartitionId) -> Vec<u8> {
        let trie = self.trie.read().unwrap();
        let partition_entries: Vec<_> = trie
            .all_subscriptions()
            .into_iter()
            .filter(|(_, sub)| sub.client_partition == partition)
            .collect();

        let mut buf = Vec::new();
        buf.extend_from_slice(&(partition_entries.len() as u32).to_be_bytes());

        for (pattern, sub) in partition_entries {
            let entry = WildcardEntry::create(
                &pattern,
                &sub.client_id,
                sub.client_partition,
                sub.qos,
                sub.subscription_type,
            );

            let id = format!("{}:{}", pattern, sub.client_id);
            let id_bytes = id.as_bytes();
            buf.extend_from_slice(&(id_bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(id_bytes);

            let data = Self::serialize_entry(&entry);
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
    pub fn import_wildcards(&self, data: &[u8]) -> Result<usize, WildcardStoreError> {
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
                .map_err(|_| WildcardStoreError::SerializationError)?;
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

            let parts: Vec<&str> = id.splitn(2, ':').collect();
            if parts.len() != 2 {
                continue;
            }
            let pattern = parts[0];
            let client_id = parts[1];

            if let Some(entry) = Self::deserialize_entry(entry_data) {
                let subscriber = WildcardSubscriber {
                    client_id: client_id.to_string(),
                    client_partition: entry.partition().unwrap_or(PartitionId::ZERO),
                    qos: entry.qos,
                    subscription_type: entry.sub_type(),
                };
                self.trie.write().unwrap().insert(pattern, subscriber);
                imported += 1;
            }
        }

        Ok(imported)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn clear_partition(&self, partition: PartitionId) -> usize {
        self.trie.write().unwrap().clear_for_partition(partition)
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
    ) -> (Vec<WildcardEntry>, bool, Option<Vec<u8>>) {
        let trie = self.trie.read().unwrap();
        let start_key = cursor.and_then(|c| std::str::from_utf8(c).ok());

        let mut results: Vec<_> = trie
            .all_subscriptions()
            .into_iter()
            .filter(|(pattern, _)| start_key.is_none_or(|sk| pattern.as_str() > sk))
            .take(limit as usize + 1)
            .collect();

        results.sort_by_key(|(k, _)| k.clone());
        let has_more = results.len() > limit as usize;
        if has_more {
            results.pop();
        }

        let next_cursor = if has_more {
            results.last().map(|(k, _)| k.as_bytes().to_vec())
        } else {
            None
        };

        let data = results
            .into_iter()
            .map(|(pattern, sub)| WildcardEntry::from_subscriber(&pattern, sub))
            .collect();
        (data, has_more, next_cursor)
    }
}

impl std::fmt::Debug for WildcardStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WildcardStore")
            .field("node_id", &self.node_id)
            .field("pattern_count", &self.pattern_count())
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
    fn wildcard_entry_bebytes_roundtrip() {
        let entry = WildcardEntry::create(
            "sensors/+/temp",
            "client1",
            partition(10),
            1,
            SubscriptionType::Mqtt,
        );
        let bytes = WildcardStore::serialize_entry(&entry);
        let parsed = WildcardStore::deserialize_entry(&bytes).unwrap();

        assert_eq!(entry.pattern, parsed.pattern);
        assert_eq!(entry.client_id, parsed.client_id);
        assert_eq!(entry.qos, parsed.qos);
    }

    #[test]
    fn wildcard_key_format() {
        let key = wildcard_key("sensors/+/temp", "client1");
        assert!(key.starts_with("_wildcards/"));
        assert!(key.ends_with("/client1"));
    }

    #[test]
    fn subscribe_and_match() {
        let store = WildcardStore::new(node(1));

        store
            .subscribe_mqtt("sensors/+/temp", "client1", 1)
            .unwrap();
        store.subscribe_mqtt("sensors/#", "client2", 2).unwrap();

        let matches = store.match_topic("sensors/building1/temp");
        assert_eq!(matches.len(), 2);

        let client_ids: Vec<&str> = matches
            .iter()
            .map(SubscriberLocation::client_id_str)
            .collect();
        assert!(client_ids.contains(&"client1"));
        assert!(client_ids.contains(&"client2"));
    }

    #[test]
    fn unsubscribe_removes_match() {
        let store = WildcardStore::new(node(1));

        store
            .subscribe_mqtt("sensors/+/temp", "client1", 1)
            .unwrap();
        store
            .subscribe_mqtt("sensors/+/temp", "client2", 1)
            .unwrap();

        assert_eq!(store.pattern_count(), 2);

        store.unsubscribe("sensors/+/temp", "client1").unwrap();

        assert_eq!(store.pattern_count(), 1);

        let matches = store.match_topic("sensors/building1/temp");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].client_id_str(), "client2");
    }

    #[test]
    fn invalid_pattern_rejected() {
        let store = WildcardStore::new(node(1));

        let result = store.subscribe_mqtt("sensors/#/temp", "client1", 1);
        assert_eq!(result, Err(WildcardStoreError::InvalidPattern));

        let result = store.subscribe_mqtt("", "client1", 1);
        assert_eq!(result, Err(WildcardStoreError::InvalidPattern));
    }

    #[test]
    fn non_wildcard_pattern_rejected() {
        let store = WildcardStore::new(node(1));

        let result = store.subscribe_mqtt("sensors/temp", "client1", 1);
        assert_eq!(result, Err(WildcardStoreError::NotWildcard));
    }

    #[test]
    fn db_subscription_type() {
        let store = WildcardStore::new(node(1));

        store
            .subscribe_db("users/+/profile", "db_client", 2)
            .unwrap();

        let matches = store.match_topic("users/123/profile");
        assert_eq!(matches.len(), 1);
    }
}
