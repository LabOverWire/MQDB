// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::protocol::Operation;
use super::{NUM_PARTITIONS, NodeId, PartitionId};
use bebytes::BeBytes;
use mqtt5::parse_shared_subscription;
use std::collections::{BTreeSet, HashMap};
use std::ops::Deref;
use std::sync::{PoisonError, RwLock};

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct SubscriberLocation {
    pub client_id_len: u8,
    #[FromField(client_id_len)]
    pub client_id: Vec<u8>,
    pub client_partition: u16,
    pub qos: u8,
}

impl SubscriberLocation {
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
        super::store_utils::bytes_to_str(&self.client_id)
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
        super::store_utils::bytes_to_str(&self.topic)
    }

    pub fn add_subscriber(&mut self, client_id: &str, client_partition: PartitionId, qos: u8) {
        if self
            .subscribers
            .iter()
            .any(|s| s.client_id_str() == client_id)
        {
            return;
        }
        let Ok(subscriber_count) = u16::try_from(self.subscribers.len() + 1) else {
            tracing::warn!(
                topic = self.topic_str(),
                client_id,
                "topic index entry is full, subscriber not recorded"
            );
            return;
        };
        self.subscribers
            .push(SubscriberLocation::create(client_id, client_partition, qos));
        self.subscriber_count = subscriber_count;
    }

    #[must_use]
    pub fn remove_subscriber(&mut self, client_id: &str) -> bool {
        let len_before = self.subscribers.len();
        self.subscribers.retain(|s| s.client_id_str() != client_id);
        self.subscriber_count = u16::try_from(self.subscribers.len()).unwrap_or(u16::MAX);
        self.subscribers.len() != len_before
    }

    pub fn next_seq(&mut self) -> u64 {
        self.last_seq += 1;
        self.last_seq
    }
}

#[must_use]
pub fn topic_partition(topic: &str) -> PartitionId {
    let hash = crc32fast::hash(topic.as_bytes());
    u16::try_from(hash % u32::from(NUM_PARTITIONS))
        .ok()
        .and_then(PartitionId::new)
        .unwrap_or(PartitionId::ZERO)
}

#[must_use]
pub fn is_response_topic(topic: &str) -> bool {
    topic.starts_with("resp/") || topic.contains("/resp/")
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

#[derive(Default)]
struct IndexedEntries {
    by_topic: HashMap<String, TopicIndexEntry>,
    shared_by_filter: HashMap<String, BTreeSet<String>>,
}

impl Deref for IndexedEntries {
    type Target = HashMap<String, TopicIndexEntry>;

    fn deref(&self) -> &Self::Target {
        &self.by_topic
    }
}

impl IndexedEntries {
    fn insert(&mut self, topic: String, entry: TopicIndexEntry) {
        if let (filter, Some(_)) = parse_shared_subscription(&topic) {
            self.shared_by_filter
                .entry(filter.to_string())
                .or_default()
                .insert(topic.clone());
        }
        self.by_topic.insert(topic, entry);
    }

    fn remove(&mut self, topic: &str) {
        if self.by_topic.remove(topic).is_some()
            && let (filter, Some(_)) = parse_shared_subscription(topic)
            && let Some(topics) = self.shared_by_filter.get_mut(filter)
        {
            topics.remove(topic);
            if topics.is_empty() {
                self.shared_by_filter.remove(filter);
            }
        }
    }

    fn get_mut(&mut self, topic: &str) -> Option<&mut TopicIndexEntry> {
        self.by_topic.get_mut(topic)
    }

    fn get_or_create(&mut self, topic: &str) -> &mut TopicIndexEntry {
        if let (filter, Some(_)) = parse_shared_subscription(topic) {
            self.shared_by_filter
                .entry(filter.to_string())
                .or_default()
                .insert(topic.to_string());
        }
        self.by_topic
            .entry(topic.to_string())
            .or_insert_with(|| TopicIndexEntry::create(topic))
    }

    fn retain(&mut self, keep: impl Fn(&str) -> bool) {
        let dropped: Vec<String> = self
            .by_topic
            .keys()
            .filter(|topic| !keep(topic))
            .cloned()
            .collect();
        for topic in &dropped {
            self.remove(topic);
        }
    }
}

pub struct TopicIndex {
    node_id: NodeId,
    entries: RwLock<IndexedEntries>,
}

impl TopicIndex {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            entries: RwLock::new(IndexedEntries::default()),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

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
        let mut entries = self.entries.write().unwrap_or_else(PoisonError::into_inner);
        let entry = entries.get_or_create(topic);
        entry.add_subscriber(client_id, client_partition, qos);
        tracing::debug!(
            topic,
            count = entry.subscribers.len(),
            "subscribe: after add"
        );
        Ok(())
    }

    /// # Errors
    /// Returns `NotFound` if the topic does not exist.
    pub fn unsubscribe(&self, topic: &str, client_id: &str) -> Result<(), TopicIndexError> {
        tracing::debug!(topic, client_id, "unsubscribe called");
        let mut entries = self.entries.write().unwrap_or_else(PoisonError::into_inner);
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

    #[must_use]
    pub fn get_subscribers(&self, topic: &str) -> Vec<SubscriberLocation> {
        let entries = self.entries.read().unwrap_or_else(PoisonError::into_inner);
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

    #[must_use]
    pub fn shared_subscribers(&self, topic: &str) -> Vec<(String, String, SubscriberLocation)> {
        let entries = self.entries.read().unwrap_or_else(PoisonError::into_inner);
        let Some(shared_topics) = entries.shared_by_filter.get(topic) else {
            return Vec::new();
        };
        shared_topics
            .iter()
            .filter_map(|shared_topic| {
                let group = parse_shared_subscription(shared_topic).1?;
                let entry = entries.get(shared_topic)?;
                Some((group, entry))
            })
            .flat_map(|(group, entry)| {
                entry.subscribers.iter().map(move |subscriber| {
                    (group.to_string(), topic.to_string(), subscriber.clone())
                })
            })
            .collect()
    }

    #[must_use]
    pub fn get_entry(&self, topic: &str) -> Option<TopicIndexEntry> {
        self.entries
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(topic)
            .cloned()
    }

    #[must_use]
    pub fn next_seq(&self, topic: &str) -> Option<u64> {
        let mut entries = self.entries.write().unwrap_or_else(PoisonError::into_inner);
        entries.get_mut(topic).map(TopicIndexEntry::next_seq)
    }

    #[must_use]
    pub fn topics_on_partition(&self, partition: PartitionId) -> Vec<String> {
        self.entries
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .filter(|t| topic_partition(t) == partition)
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn topic_count(&self) -> usize {
        self.entries
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    #[must_use]
    pub fn get_client_topics(&self, client_id: &str) -> Vec<(String, u8)> {
        self.entries
            .read()
            .unwrap_or_else(PoisonError::into_inner)
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

    pub fn subscribe_with_data(
        &self,
        topic: &str,
        client_id: &str,
        client_partition: PartitionId,
        qos: u8,
    ) -> (TopicIndexEntry, Vec<u8>) {
        let mut entries = self.entries.write().unwrap_or_else(PoisonError::into_inner);
        let entry = entries.get_or_create(topic);
        entry.add_subscriber(client_id, client_partition, qos);
        let data = Self::serialize(entry);
        (entry.clone(), data)
    }

    /// # Errors
    /// Returns `NotFound` if the topic does not exist.
    pub fn unsubscribe_with_data(
        &self,
        topic: &str,
        client_id: &str,
    ) -> Result<(TopicIndexEntry, Vec<u8>), TopicIndexError> {
        let mut entries = self.entries.write().unwrap_or_else(PoisonError::into_inner);
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
        super::store_utils::serialize(entry)
    }

    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<TopicIndexEntry> {
        super::store_utils::deserialize(bytes)
    }

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
                let mut entries = self.entries.write().unwrap_or_else(PoisonError::into_inner);
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
                let mut entries = self.entries.write().unwrap_or_else(PoisonError::into_inner);
                entries.remove(id);
                Ok(())
            }
        }
    }

    #[must_use]
    pub fn export_for_partition(&self, partition: PartitionId) -> Vec<u8> {
        let entries = self.entries.read().unwrap_or_else(PoisonError::into_inner);
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
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(topic.to_string(), entry);
                imported += 1;
            }
        }

        Ok(imported)
    }

    pub fn clear_partition(&self, partition: PartitionId) -> usize {
        tracing::debug!(partition = partition.get(), "clear_partition called");
        let mut entries = self.entries.write().unwrap_or_else(PoisonError::into_inner);
        let before = entries.len();
        entries.retain(|topic| topic_partition(topic) != partition);
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

    #[must_use]
    pub fn query(
        &self,
        _filter: Option<&str>,
        limit: u32,
        cursor: Option<&[u8]>,
    ) -> (Vec<TopicIndexEntry>, bool, Option<Vec<u8>>) {
        let entries = self.entries.read().unwrap_or_else(PoisonError::into_inner);
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
    fn shared_subscribers_are_found_by_their_plain_topic_and_kept_apart_from_it() {
        let index = TopicIndex::new(node(1));
        index.subscribe("sh/x", "plain", partition(1), 1).unwrap();
        index
            .subscribe("$share/g/sh/x", "member-g", partition(2), 1)
            .unwrap();
        index
            .subscribe("$share/h/sh/x", "member-h", partition(3), 1)
            .unwrap();
        index
            .subscribe("$share/g/sh/y", "member-y", partition(4), 1)
            .unwrap();

        let mut shared: Vec<(String, String)> = index
            .shared_subscribers("sh/x")
            .into_iter()
            .map(|(group, filter, sub)| {
                assert_eq!(filter, "sh/x");
                (group, sub.client_id_str().to_string())
            })
            .collect();
        shared.sort();

        assert_eq!(
            shared,
            vec![
                ("g".to_string(), "member-g".to_string()),
                ("h".to_string(), "member-h".to_string()),
            ]
        );
        let plain: Vec<_> = index
            .get_subscribers("sh/x")
            .iter()
            .map(|sub| sub.client_id_str().to_string())
            .collect();
        assert_eq!(plain, vec!["plain".to_string()]);

        index.unsubscribe("$share/g/sh/x", "member-g").unwrap();
        index
            .apply_replicated(Operation::Delete, "$share/h/sh/x", &[])
            .unwrap();
        assert!(index.shared_subscribers("sh/x").is_empty());
        assert_eq!(index.shared_subscribers("sh/y").len(), 1);
        assert_eq!(index.clear_partition(topic_partition("$share/g/sh/y")), 1);
        assert!(index.shared_subscribers("sh/y").is_empty());
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

    #[test]
    fn entry_refuses_a_subscriber_its_count_cannot_hold() {
        let subscribers: Vec<SubscriberLocation> = (0..u16::MAX)
            .map(|i| SubscriberLocation::create(&format!("c{i}"), partition(1), 0))
            .collect();
        let mut entry = TopicIndexEntry {
            subscriber_count: u16::MAX,
            subscribers,
            ..TopicIndexEntry::create("topic/a")
        };

        entry.add_subscriber("one-too-many", partition(1), 0);

        assert_eq!(entry.subscribers.len(), usize::from(u16::MAX));
        assert_eq!(entry.subscriber_count, u16::MAX);
        let parsed = TopicIndex::deserialize(&TopicIndex::serialize(&entry)).unwrap();
        assert_eq!(parsed.subscribers.len(), usize::from(u16::MAX));
    }
}
