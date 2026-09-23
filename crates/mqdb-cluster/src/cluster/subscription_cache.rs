// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::protocol::Operation;
use super::{NodeId, PartitionId, TopicIndex, WildcardStore, is_response_topic, session_partition};
use bebytes::BeBytes;
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

pub const SUBSCRIPTION_RECONCILIATION_INTERVAL_MS: u64 = 300_000;

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
    last_reconciliation: RwLock<u64>,
}

#[derive(Debug, Default)]
pub struct ReconciliationResult {
    pub clients_checked: usize,
    pub index_entries_restored: usize,
}

impl SubscriptionCache {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            snapshots: RwLock::new(HashMap::new()),
            last_reconciliation: RwLock::new(0),
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn len(&self) -> usize {
        self.snapshots.read().unwrap().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn needs_reconciliation(&self, now: u64) -> bool {
        let last = *self.last_reconciliation.read().unwrap();
        now.saturating_sub(last) >= SUBSCRIPTION_RECONCILIATION_INTERVAL_MS
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn mark_reconciliation(&self, now: u64) {
        *self.last_reconciliation.write().unwrap() = now;
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn reconcile(
        &self,
        topic_index: &TopicIndex,
        wildcard_store: &WildcardStore,
        holds_partition: impl Fn(PartitionId) -> bool,
    ) -> ReconciliationResult {
        let mut result = ReconciliationResult::default();

        for snapshot in self.all_snapshots() {
            let client_id = snapshot.client_id_str();
            if !holds_partition(session_partition(client_id)) {
                continue;
            }
            result.clients_checked += 1;

            let indexed_exact: HashSet<String> = topic_index
                .get_client_topics(client_id)
                .into_iter()
                .map(|(topic, _)| topic)
                .collect();
            for entry in snapshot.exact_subscriptions() {
                if !indexed_exact.contains(entry.topic_str())
                    && !is_response_topic(entry.topic_str())
                    && topic_index
                        .subscribe(
                            entry.topic_str(),
                            client_id,
                            session_partition(client_id),
                            entry.qos,
                        )
                        .is_ok()
                {
                    result.index_entries_restored += 1;
                }
            }

            let indexed_wildcards: HashSet<String> = wildcard_store
                .get_client_patterns(client_id)
                .into_iter()
                .map(|(pattern, _)| pattern)
                .collect();
            for entry in snapshot.wildcard_subscriptions() {
                if !indexed_wildcards.contains(entry.topic_str())
                    && wildcard_store
                        .subscribe_mqtt(entry.topic_str(), client_id, entry.qos)
                        .is_ok()
                {
                    result.index_entries_restored += 1;
                }
            }
        }

        result
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn all_client_ids(&self) -> Vec<String> {
        self.snapshots.read().unwrap().keys().cloned().collect()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn all_snapshots(&self) -> Vec<MqttSubscriptionSnapshot> {
        self.snapshots.read().unwrap().values().cloned().collect()
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

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn export_for_partition(&self, partition: PartitionId) -> Vec<u8> {
        let snapshots = self.snapshots.read().unwrap();
        let partition_snapshots: Vec<_> = snapshots
            .iter()
            .filter(|(cid, _)| session_partition(cid) == partition)
            .collect();

        let mut buf = Vec::new();
        buf.extend_from_slice(&(partition_snapshots.len() as u32).to_be_bytes());

        for (client_id, snapshot) in partition_snapshots {
            let id_bytes = client_id.as_bytes();
            buf.extend_from_slice(&(id_bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(id_bytes);

            let data = snapshot.to_be_bytes();
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
    pub fn import_subscriptions(&self, data: &[u8]) -> Result<usize, SubscriptionCacheError> {
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
                .map_err(|_| SubscriptionCacheError::SerializationError)?;
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
            let snapshot_data = &data[offset..offset + data_len];
            offset += data_len;

            if let Some(snapshot) = Self::deserialize(snapshot_data) {
                self.snapshots
                    .write()
                    .unwrap()
                    .insert(id.to_string(), snapshot);
                imported += 1;
            }
        }

        Ok(imported)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn clear_partition(&self, partition: PartitionId) -> usize {
        let mut snapshots = self.snapshots.write().unwrap();
        let before = snapshots.len();
        snapshots.retain(|cid, _| session_partition(cid) != partition);
        before - snapshots.len()
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
    ) -> (Vec<MqttSubscriptionSnapshot>, bool, Option<Vec<u8>>) {
        let snapshots = self.snapshots.read().unwrap();
        let start_key = cursor.and_then(|c| std::str::from_utf8(c).ok());

        let mut results: Vec<_> = snapshots
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

    fn partition() -> PartitionId {
        PartitionId::new(5).unwrap()
    }

    #[test]
    fn reconcile_restores_missing_index_entries_from_cache() {
        let cache = SubscriptionCache::new(node(1));
        let topic_index = TopicIndex::new(node(1));
        let wildcard_store = WildcardStore::new(node(1));

        cache.add_subscription("client1", "topic/a", 1).unwrap();
        cache.add_subscription("client1", "topic/b", 2).unwrap();
        cache
            .add_subscription("client1", "sensors/+/temp", 1)
            .unwrap();
        topic_index
            .subscribe("topic/a", "client1", partition(), 1)
            .unwrap();

        let result = cache.reconcile(&topic_index, &wildcard_store, |_| true);

        assert_eq!(result.clients_checked, 1);
        assert_eq!(result.index_entries_restored, 2);
        let mut indexed = topic_index.get_client_topics("client1");
        indexed.sort();
        assert_eq!(
            indexed,
            vec![("topic/a".to_string(), 1), ("topic/b".to_string(), 2)]
        );
        assert_eq!(
            topic_index.get_subscribers("topic/b")[0].partition(),
            Some(session_partition("client1"))
        );
        assert_eq!(
            wildcard_store.get_client_patterns("client1"),
            vec![("sensors/+/temp".to_string(), 1)]
        );
        assert_eq!(cache.get_subscriptions("client1").len(), 3);
    }

    #[test]
    fn reconcile_does_not_resurrect_unsubscribed_topics_from_index() {
        let cache = SubscriptionCache::new(node(1));
        let topic_index = TopicIndex::new(node(1));
        let wildcard_store = WildcardStore::new(node(1));

        cache.add_subscription("client1", "topic/kept", 1).unwrap();
        topic_index
            .subscribe("topic/kept", "client1", partition(), 1)
            .unwrap();
        topic_index
            .subscribe("topic/dropped", "client1", partition(), 1)
            .unwrap();
        wildcard_store
            .subscribe_mqtt("dropped/+/pattern", "client1", 1)
            .unwrap();

        let result = cache.reconcile(&topic_index, &wildcard_store, |_| true);

        assert_eq!(result.index_entries_restored, 0);
        let topics: Vec<String> = cache
            .get_subscriptions("client1")
            .iter()
            .map(|t| t.topic_str().to_string())
            .collect();
        assert_eq!(topics, vec!["topic/kept".to_string()]);
    }

    #[test]
    fn reconcile_skips_response_topics() {
        let cache = SubscriptionCache::new(node(1));
        let topic_index = TopicIndex::new(node(1));
        let wildcard_store = WildcardStore::new(node(1));

        cache
            .add_subscription("client1", "resp/client1", 0)
            .unwrap();
        cache
            .add_subscription("client1", "app/resp/client1", 0)
            .unwrap();

        let result = cache.reconcile(&topic_index, &wildcard_store, |_| true);

        assert_eq!(result.index_entries_restored, 0);
        assert!(topic_index.get_client_topics("client1").is_empty());
        assert_eq!(cache.get_subscriptions("client1").len(), 2);
    }

    #[test]
    fn reconcile_ignores_records_for_partitions_not_held() {
        let cache = SubscriptionCache::new(node(1));
        let topic_index = TopicIndex::new(node(1));
        let wildcard_store = WildcardStore::new(node(1));

        cache.add_subscription("held-client", "topic/a", 1).unwrap();
        cache
            .add_subscription("stale-client", "topic/a", 1)
            .unwrap();
        cache
            .add_subscription("stale-client", "stale/+/pattern", 1)
            .unwrap();
        let held = session_partition("held-client");
        assert_ne!(held, session_partition("stale-client"));

        let result = cache.reconcile(&topic_index, &wildcard_store, |partition| partition == held);

        assert_eq!(result.clients_checked, 1);
        assert_eq!(result.index_entries_restored, 1);
        let subscribers: Vec<String> = topic_index
            .get_subscribers("topic/a")
            .iter()
            .map(|s| s.client_id_str().to_string())
            .collect();
        assert_eq!(subscribers, vec!["held-client".to_string()]);
        assert!(
            wildcard_store
                .get_client_patterns("stale-client")
                .is_empty()
        );
        assert_eq!(cache.get_subscriptions("stale-client").len(), 2);
    }

    #[test]
    fn reconcile_is_idempotent() {
        let cache = SubscriptionCache::new(node(1));
        let topic_index = TopicIndex::new(node(1));
        let wildcard_store = WildcardStore::new(node(1));

        cache.add_subscription("client1", "topic/a", 1).unwrap();
        cache.add_subscription("client1", "alerts/#", 1).unwrap();

        assert_eq!(
            cache
                .reconcile(&topic_index, &wildcard_store, |_| true)
                .index_entries_restored,
            2
        );
        assert_eq!(
            cache
                .reconcile(&topic_index, &wildcard_store, |_| true)
                .index_entries_restored,
            0
        );
        assert_eq!(topic_index.get_subscribers("topic/a").len(), 1);
    }

    #[test]
    fn replicated_subscription_survives_reconcile_when_broadcast_was_missed() {
        let replica = SubscriptionCache::new(node(2));
        let topic_index = TopicIndex::new(node(2));
        let wildcard_store = WildcardStore::new(node(2));

        let mut snapshot = MqttSubscriptionSnapshot::create("client1");
        snapshot.add_subscription("sensors/a", 1);
        snapshot.add_subscription("alerts/+", 1);
        replica
            .apply_replicated(
                Operation::Insert,
                "client1",
                &SubscriptionCache::serialize(&snapshot),
            )
            .unwrap();

        let result = replica.reconcile(&topic_index, &wildcard_store, |_| true);

        assert_eq!(result.index_entries_restored, 2);
        assert_eq!(
            topic_index.get_client_topics("client1"),
            vec![("sensors/a".to_string(), 1)]
        );
        assert_eq!(
            wildcard_store.get_client_patterns("client1"),
            vec![("alerts/+".to_string(), 1)]
        );

        let promoted = SubscriptionCache::new(node(3));
        promoted
            .import_subscriptions(&replica.export_for_partition(session_partition("client1")))
            .unwrap();

        let mut topics: Vec<String> = promoted
            .get_subscriptions("client1")
            .iter()
            .map(|t| t.topic_str().to_string())
            .collect();
        topics.sort();
        assert_eq!(
            topics,
            vec!["alerts/+".to_string(), "sensors/a".to_string()],
            "replicated subscriptions were deleted by reconcile because the index broadcast was missed"
        );
    }

    #[test]
    fn needs_reconciliation_respects_interval() {
        let cache = SubscriptionCache::new(node(1));

        assert!(cache.needs_reconciliation(SUBSCRIPTION_RECONCILIATION_INTERVAL_MS));

        cache.mark_reconciliation(100_000);

        assert!(!cache.needs_reconciliation(200_000));
        assert!(!cache.needs_reconciliation(399_999));
        assert!(cache.needs_reconciliation(400_001));
    }
}
