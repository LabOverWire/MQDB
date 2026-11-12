// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::protocol::{ListingDigest, Operation};
use super::{NUM_PARTITIONS, NodeId, PartitionId};
use bebytes::BeBytes;
use mqtt5::parse_shared_subscription;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Deref;
use std::sync::{PoisonError, RwLock};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriberLocation {
    pub client_id: Vec<u8>,
    pub client_partition: u16,
    pub qos: u8,
}

impl SubscriberLocation {
    #[must_use]
    pub fn create(client_id: &str, client_partition: PartitionId, qos: u8) -> Self {
        Self {
            client_id: client_id.as_bytes().to_vec(),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicIndexEntry {
    pub topic: Vec<u8>,
    pub last_seq: u64,
    pub subscribers: Vec<SubscriberLocation>,
}

const NARROW_ENTRY_VERSION: u8 = 1;
const WIDE_ENTRY_VERSION: u8 = 2;
const NARROW_READER_GUARD: u16 = u16::MAX;

#[derive(Debug, Clone, BeBytes)]
struct NarrowSubscriber {
    client_id_len: u8,
    #[FromField(client_id_len)]
    client_id: Vec<u8>,
    client_partition: u16,
    qos: u8,
}

#[derive(Debug, Clone, BeBytes)]
struct NarrowEntry {
    version: u8,
    topic_len: u16,
    #[FromField(topic_len)]
    topic: Vec<u8>,
    last_seq: u64,
    subscriber_count: u16,
    #[FromField(subscriber_count)]
    subscribers: Vec<NarrowSubscriber>,
}

#[derive(Debug, Clone, BeBytes)]
struct WideSubscriber {
    client_id_len: u16,
    #[FromField(client_id_len)]
    client_id: Vec<u8>,
    client_partition: u16,
    qos: u8,
}

#[derive(Debug, Clone, BeBytes)]
struct WideEntry {
    version: u8,
    narrow_reader_guard: u16,
    topic_len: u16,
    #[FromField(topic_len)]
    topic: Vec<u8>,
    last_seq: u64,
    subscriber_count: u32,
    #[FromField(subscriber_count)]
    subscribers: Vec<WideSubscriber>,
}

fn decode_exact<T: BeBytes>(bytes: &[u8]) -> Option<T> {
    let (value, consumed) = T::try_from_be_bytes(bytes).ok()?;
    (consumed == bytes.len()).then_some(value)
}

impl NarrowEntry {
    fn is_complete(&self) -> bool {
        self.subscribers.len() == usize::from(self.subscriber_count)
    }
}

impl WideEntry {
    fn is_complete(&self) -> bool {
        self.narrow_reader_guard == NARROW_READER_GUARD
            && u32::try_from(self.subscribers.len()).ok() == Some(self.subscriber_count)
    }
}

impl TopicIndexEntry {
    #[must_use]
    pub fn create(topic: &str) -> Self {
        Self {
            topic: topic.as_bytes().to_vec(),
            last_seq: 0,
            subscribers: Vec::new(),
        }
    }

    fn narrow(&self) -> Option<NarrowEntry> {
        let subscribers = self
            .subscribers
            .iter()
            .map(|sub| {
                Some(NarrowSubscriber {
                    client_id_len: u8::try_from(sub.client_id.len()).ok()?,
                    client_id: sub.client_id.clone(),
                    client_partition: sub.client_partition,
                    qos: sub.qos,
                })
            })
            .collect::<Option<Vec<_>>>()?;
        Some(NarrowEntry {
            version: NARROW_ENTRY_VERSION,
            topic_len: u16::try_from(self.topic.len()).ok()?,
            topic: self.topic.clone(),
            last_seq: self.last_seq,
            subscriber_count: u16::try_from(subscribers.len()).ok()?,
            subscribers,
        })
    }

    fn wide(&self) -> Option<WideEntry> {
        let subscribers = self
            .subscribers
            .iter()
            .map(|sub| {
                Some(WideSubscriber {
                    client_id_len: u16::try_from(sub.client_id.len()).ok()?,
                    client_id: sub.client_id.clone(),
                    client_partition: sub.client_partition,
                    qos: sub.qos,
                })
            })
            .collect::<Option<Vec<_>>>()?;
        Some(WideEntry {
            version: WIDE_ENTRY_VERSION,
            narrow_reader_guard: NARROW_READER_GUARD,
            topic_len: u16::try_from(self.topic.len()).ok()?,
            topic: self.topic.clone(),
            last_seq: self.last_seq,
            subscriber_count: u32::try_from(subscribers.len()).ok()?,
            subscribers,
        })
    }

    #[must_use]
    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        match self.narrow() {
            Some(narrow) => Some(narrow.to_be_bytes()),
            None => self.wide().map(|wide| wide.to_be_bytes()),
        }
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        match *bytes.first()? {
            NARROW_ENTRY_VERSION => {
                let entry = decode_exact::<NarrowEntry>(bytes).filter(NarrowEntry::is_complete)?;
                Some(Self {
                    topic: entry.topic,
                    last_seq: entry.last_seq,
                    subscribers: entry
                        .subscribers
                        .into_iter()
                        .map(|sub| SubscriberLocation {
                            client_id: sub.client_id,
                            client_partition: sub.client_partition,
                            qos: sub.qos,
                        })
                        .collect(),
                })
            }
            WIDE_ENTRY_VERSION => {
                let entry = decode_exact::<WideEntry>(bytes).filter(WideEntry::is_complete)?;
                Some(Self {
                    topic: entry.topic,
                    last_seq: entry.last_seq,
                    subscribers: entry
                        .subscribers
                        .into_iter()
                        .map(|sub| SubscriberLocation {
                            client_id: sub.client_id,
                            client_partition: sub.client_partition,
                            qos: sub.qos,
                        })
                        .collect(),
                })
            }
            _ => None,
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
        self.subscribers
            .push(SubscriberLocation::create(client_id, client_partition, qos));
    }

    #[must_use]
    pub fn remove_subscriber(&mut self, client_id: &str) -> bool {
        let len_before = self.subscribers.len();
        self.subscribers.retain(|s| s.client_id_str() != client_id);
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
    topics_by_client: HashMap<String, HashSet<String>>,
    digest_by_client: HashMap<String, u64>,
}

fn digest_term(topic: &str, client_id: &str, qos: u8) -> u64 {
    if is_response_topic(topic) {
        0
    } else {
        ListingDigest::topic_term(client_id, topic, qos)
    }
}

impl Deref for IndexedEntries {
    type Target = HashMap<String, TopicIndexEntry>;

    fn deref(&self) -> &Self::Target {
        &self.by_topic
    }
}

impl IndexedEntries {
    fn link(&mut self, topic: &str, client_id: &str, qos: u8) {
        let linked = self
            .topics_by_client
            .entry(client_id.to_string())
            .or_default()
            .insert(topic.to_string());
        if linked {
            let digest = self
                .digest_by_client
                .entry(client_id.to_string())
                .or_default();
            *digest = digest.wrapping_add(digest_term(topic, client_id, qos));
        }
    }

    fn unlink(&mut self, topic: &str, client_id: &str, qos: u8) {
        let Some(topics) = self.topics_by_client.get_mut(client_id) else {
            return;
        };
        if !topics.remove(topic) {
            return;
        }
        if topics.is_empty() {
            self.topics_by_client.remove(client_id);
            self.digest_by_client.remove(client_id);
        } else if let Some(digest) = self.digest_by_client.get_mut(client_id) {
            *digest = digest.wrapping_sub(digest_term(topic, client_id, qos));
        }
    }

    fn track_shared(&mut self, topic: &str) {
        if let (filter, Some(_)) = parse_shared_subscription(topic) {
            self.shared_by_filter
                .entry(filter.to_string())
                .or_default()
                .insert(topic.to_string());
        }
    }

    fn insert(&mut self, topic: String, entry: TopicIndexEntry) {
        self.remove(&topic);
        self.track_shared(&topic);
        for subscriber in &entry.subscribers {
            self.link(&topic, subscriber.client_id_str(), subscriber.qos);
        }
        self.by_topic.insert(topic, entry);
    }

    fn remove(&mut self, topic: &str) {
        let Some(entry) = self.by_topic.remove(topic) else {
            return;
        };
        for subscriber in &entry.subscribers {
            self.unlink(topic, subscriber.client_id_str(), subscriber.qos);
        }
        if let (filter, Some(_)) = parse_shared_subscription(topic)
            && let Some(topics) = self.shared_by_filter.get_mut(filter)
        {
            topics.remove(topic);
            if topics.is_empty() {
                self.shared_by_filter.remove(filter);
            }
        }
    }

    fn add_subscriber(
        &mut self,
        topic: &str,
        client_id: &str,
        client_partition: PartitionId,
        qos: u8,
    ) -> usize {
        self.track_shared(topic);
        let entry = self
            .by_topic
            .entry(topic.to_string())
            .or_insert_with(|| TopicIndexEntry::create(topic));
        let present = entry
            .subscribers
            .iter()
            .any(|s| s.client_id_str() == client_id);
        if !present {
            entry.add_subscriber(client_id, client_partition, qos);
        }
        let subscribers = entry.subscribers.len();
        if !present {
            self.link(topic, client_id, qos);
        }
        subscribers
    }

    fn remove_subscriber(&mut self, topic: &str, client_id: &str) -> Option<usize> {
        let entry = self.by_topic.get_mut(topic)?;
        let removed_qos = entry
            .subscribers
            .iter()
            .find(|s| s.client_id_str() == client_id)
            .map(|s| s.qos);
        let _ = entry.remove_subscriber(client_id);
        let remaining = entry.subscribers.len();
        if let Some(qos) = removed_qos {
            self.unlink(topic, client_id, qos);
        }
        if remaining == 0 {
            self.remove(topic);
        }
        Some(remaining)
    }

    fn next_seq(&mut self, topic: &str) -> Option<u64> {
        self.by_topic.get_mut(topic).map(TopicIndexEntry::next_seq)
    }

    fn client_topics<'a>(&'a self, client_id: &'a str) -> impl Iterator<Item = (&'a String, u8)> {
        self.topics_by_client
            .get(client_id)
            .into_iter()
            .flatten()
            .filter_map(move |topic| {
                self.by_topic.get(topic).and_then(|entry| {
                    entry
                        .subscribers
                        .iter()
                        .find(|s| s.client_id_str() == client_id)
                        .map(|s| (topic, s.qos))
                })
            })
    }

    fn client_subscriptions(&self, client_id: &str) -> Vec<(String, u8)> {
        self.client_topics(client_id)
            .map(|(topic, qos)| (topic.clone(), qos))
            .collect()
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
        let count = self
            .entries
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .add_subscriber(topic, client_id, client_partition, qos);
        tracing::debug!(topic, count, "subscribe: after add");
        Ok(())
    }

    /// # Errors
    /// Returns `NotFound` if the topic does not exist.
    pub fn unsubscribe(&self, topic: &str, client_id: &str) -> Result<(), TopicIndexError> {
        tracing::debug!(topic, client_id, "unsubscribe called");
        match self
            .entries
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove_subscriber(topic, client_id)
        {
            Some(remaining) => {
                tracing::debug!(topic, remaining, "unsubscribe: removed");
                Ok(())
            }
            None => Err(TopicIndexError::NotFound),
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
        self.entries
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .next_seq(topic)
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
            .client_subscriptions(client_id)
    }

    #[must_use]
    pub fn digest_of(&self, clients: &HashSet<String>) -> u64 {
        let entries = self.entries.read().unwrap_or_else(PoisonError::into_inner);
        clients.iter().fold(0_u64, |sum, client_id| {
            sum.wrapping_add(
                entries
                    .digest_by_client
                    .get(client_id)
                    .copied()
                    .unwrap_or_default(),
            )
        })
    }

    #[must_use]
    pub fn subscriptions_of(
        &self,
        clients: &HashSet<String>,
    ) -> HashMap<String, Vec<(String, u8)>> {
        let entries = self.entries.read().unwrap_or_else(PoisonError::into_inner);
        clients
            .iter()
            .map(|client_id| (client_id.clone(), entries.client_subscriptions(client_id)))
            .filter(|(_, topics)| !topics.is_empty())
            .collect()
    }

    #[must_use]
    pub fn serialize(entry: &TopicIndexEntry) -> Option<Vec<u8>> {
        entry.to_bytes()
    }

    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<TopicIndexEntry> {
        TopicIndexEntry::from_bytes(bytes)
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
        let framed: Vec<Vec<u8>> = entries
            .iter()
            .filter(|(topic, _)| topic_partition(topic) == partition)
            .filter_map(|(topic, entry)| {
                let frame = Self::frame(topic, entry);
                if frame.is_none() {
                    tracing::warn!(topic, "topic index entry too large to export");
                }
                frame
            })
            .collect();

        let count = u32::try_from(framed.len()).unwrap_or(u32::MAX);
        let mut buf = count.to_be_bytes().to_vec();
        for frame in framed
            .into_iter()
            .take(usize::try_from(count).unwrap_or(usize::MAX))
        {
            buf.extend_from_slice(&frame);
        }
        buf
    }

    fn frame(topic: &str, entry: &TopicIndexEntry) -> Option<Vec<u8>> {
        let id_bytes = topic.as_bytes();
        let data = Self::serialize(entry)?;
        let mut frame = Vec::with_capacity(6 + id_bytes.len() + data.len());
        frame.extend_from_slice(&u16::try_from(id_bytes.len()).ok()?.to_be_bytes());
        frame.extend_from_slice(id_bytes);
        frame.extend_from_slice(&u32::try_from(data.len()).ok()?.to_be_bytes());
        frame.extend_from_slice(&data);
        Some(frame)
    }

    /// # Errors
    /// Returns `SerializationError` if UTF-8 parsing fails.
    pub fn import_entries(
        &self,
        data: &[u8],
        local_clients: &HashSet<String>,
    ) -> Result<usize, TopicIndexError> {
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

            if let Some(mut entry) = Self::deserialize(entry_data) {
                let mut entries = self.entries.write().unwrap_or_else(PoisonError::into_inner);
                let kept: Vec<SubscriberLocation> = entries
                    .get(topic)
                    .map(|existing| {
                        existing
                            .subscribers
                            .iter()
                            .filter(|local| local_clients.contains(local.client_id_str()))
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                entry
                    .subscribers
                    .retain(|incoming| !local_clients.contains(incoming.client_id_str()));
                tracing::debug!(
                    topic,
                    subscribers = entry.subscribers.len(),
                    kept_local = kept.len(),
                    "import_entries: replacing entry"
                );
                entry.subscribers.extend(kept);
                if entry.subscribers.is_empty() {
                    entries.remove(topic);
                } else {
                    entries.insert(topic.to_string(), entry);
                }
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
        limit: u32,
        cursor: Option<&[u8]>,
    ) -> (Vec<TopicIndexEntry>, bool, Option<Vec<u8>>) {
        let entries = self.entries.read().unwrap_or_else(PoisonError::into_inner);
        let start_key = cursor.and_then(|c| std::str::from_utf8(c).ok());

        let mut results: Vec<_> = entries
            .iter()
            .filter(|(key, _)| start_key.is_none_or(|sk| key.as_str() > sk))
            .collect();
        results.sort_unstable_by_key(|(k, _)| k.as_str());
        results.truncate(limit as usize + 1);
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
    fn query_pages_cover_every_topic_exactly_once() {
        let expected: Vec<String> = (0..10).map(|n| format!("t/{n}")).collect();
        for _ in 0..20 {
            let index = TopicIndex::new(node(1));
            for topic in &expected {
                index.subscribe(topic, "c", partition(1), 0).unwrap();
            }
            let mut seen = Vec::new();
            let mut cursor: Option<Vec<u8>> = None;
            loop {
                let (page, has_more, next) = index.query(2, cursor.as_deref());
                assert!(page.len() <= 2);
                if has_more {
                    assert_eq!(page.len(), 2);
                }
                seen.extend(page.iter().map(|e| e.topic_str().to_string()));
                if !has_more {
                    break;
                }
                cursor = next;
            }
            assert_eq!(seen, expected);
        }
    }

    fn scanned_client_topics(
        index: &TopicIndex,
        topics: &[&str],
        client_id: &str,
    ) -> Vec<(String, u8)> {
        let mut found: Vec<(String, u8)> = topics
            .iter()
            .flat_map(|topic| {
                index
                    .get_subscribers(topic)
                    .into_iter()
                    .filter(|s| s.client_id_str() == client_id)
                    .map(|s| (topic.to_string(), s.qos))
            })
            .collect();
        found.sort();
        found
    }

    #[test]
    fn the_per_client_lookup_matches_a_full_scan_after_every_kind_of_change() {
        let topics = ["a/1", "a/2", "b/1", "$share/g/a/1", "resp/c1/x"];
        let clients = ["c1", "c2", "c3"];
        let index = TopicIndex::new(node(1));
        index.subscribe("a/1", "c1", partition(1), 1).unwrap();
        index.subscribe("resp/c1/x", "c1", partition(1), 1).unwrap();
        index.subscribe("a/1", "c1", partition(1), 2).unwrap();
        index.subscribe("a/2", "c1", partition(1), 0).unwrap();
        index.subscribe("a/1", "c2", partition(2), 2).unwrap();
        index
            .subscribe("$share/g/a/1", "c3", partition(3), 1)
            .unwrap();
        index.unsubscribe("a/2", "c1").unwrap();

        let mut replaced = TopicIndexEntry::create("a/1");
        replaced.add_subscriber("c3", partition(3), 1);
        let data = TopicIndex::serialize(&replaced).unwrap();
        index
            .apply_replicated(Operation::Insert, "a/1", &data)
            .unwrap();

        let other = TopicIndex::new(node(2));
        other.subscribe("b/1", "c2", partition(2), 1).unwrap();
        index
            .import_entries(
                &other.export_for_partition(topic_partition("b/1")),
                &HashSet::new(),
            )
            .unwrap();
        index.clear_partition(topic_partition("$share/g/a/1"));

        for client in clients {
            let mut indexed = index.get_client_topics(client);
            indexed.sort();
            let scanned = scanned_client_topics(&index, &topics, client);
            let scanned_digest = scanned.iter().fold(0_u64, |sum, (topic, qos)| {
                sum.wrapping_add(digest_term(topic, client, *qos))
            });
            assert_eq!(indexed, scanned, "{client}");
            assert_eq!(
                index.digest_of(&HashSet::from([client.to_string()])),
                scanned_digest,
                "{client}"
            );
        }
    }

    #[test]
    fn topic_index_entry_roundtrip() {
        let mut entry = TopicIndexEntry::create("sensors/temp");
        entry.add_subscriber("client1", partition(12), 1);
        entry.add_subscriber("client2", partition(45), 2);
        entry.next_seq();
        entry.next_seq();

        let bytes = TopicIndex::serialize(&entry).unwrap();

        assert_eq!(bytes[0], NARROW_ENTRY_VERSION);
        assert_eq!(TopicIndex::deserialize(&bytes), Some(entry));
    }

    #[test]
    fn a_client_id_longer_than_255_bytes_survives_serialization() {
        let id = "x".repeat(300);
        let mut entry = TopicIndexEntry::create("a/b");
        entry.add_subscriber("short", partition(2), 0);
        entry.add_subscriber(&id, partition(1), 1);

        let bytes = TopicIndex::serialize(&entry).unwrap();

        assert_eq!(bytes[0], WIDE_ENTRY_VERSION);
        assert_eq!(TopicIndex::deserialize(&bytes), Some(entry));
    }

    #[test]
    fn a_topic_with_more_than_u16_max_subscribers_survives_serialization() {
        let mut entry = TopicIndexEntry::create("a/b");
        for n in 0..=u32::from(u16::MAX) {
            entry.subscribers.push(SubscriberLocation::create(
                &format!("c{n}"),
                partition(1),
                0,
            ));
        }

        let decoded = TopicIndex::deserialize(&TopicIndex::serialize(&entry).unwrap()).unwrap();

        assert_eq!(decoded.subscribers.len(), entry.subscribers.len());
    }

    #[derive(Debug, Clone, BeBytes)]
    struct PreviousSubscriberLocation {
        client_id_len: u8,
        #[FromField(client_id_len)]
        client_id: Vec<u8>,
        client_partition: u16,
        qos: u8,
    }

    #[derive(Debug, Clone, BeBytes)]
    struct PreviousTopicIndexEntry {
        version: u8,
        topic_len: u16,
        #[FromField(topic_len)]
        topic: Vec<u8>,
        last_seq: u64,
        subscriber_count: u16,
        #[FromField(subscriber_count)]
        subscribers: Vec<PreviousSubscriberLocation>,
    }

    #[test]
    fn a_node_running_the_previous_decoder_rejects_a_wide_entry() {
        let mut entry = TopicIndexEntry::create("a/b");
        entry.add_subscriber("short1", partition(1), 1);
        entry.add_subscriber("short2", partition(2), 1);
        entry.add_subscriber(&"x".repeat(300), partition(3), 1);
        let bytes = TopicIndex::serialize(&entry).unwrap();

        assert_eq!(bytes[0], WIDE_ENTRY_VERSION);
        assert!(PreviousTopicIndexEntry::try_from_be_bytes(&bytes).is_err());
    }

    #[test]
    fn a_node_running_the_previous_decoder_still_reads_a_narrow_entry() {
        let mut entry = TopicIndexEntry::create("a/b");
        entry.add_subscriber("short1", partition(1), 1);
        let bytes = TopicIndex::serialize(&entry).unwrap();

        let (previous, _) = PreviousTopicIndexEntry::try_from_be_bytes(&bytes).unwrap();

        assert_eq!(previous.version, NARROW_ENTRY_VERSION);
        assert_eq!(previous.topic_len, 3);
        assert_eq!(previous.topic, b"a/b");
        assert_eq!(previous.last_seq, 0);
        assert_eq!(previous.subscriber_count, 1);
        assert_eq!(previous.subscribers[0].client_id_len, 6);
        assert_eq!(previous.subscribers[0].client_id, b"short1");
        assert_eq!(previous.subscribers[0].client_partition, 1);
        assert_eq!(previous.subscribers[0].qos, 1);
    }

    #[test]
    fn an_exported_partition_with_a_long_client_id_imports_intact() {
        let src = TopicIndex::new(node(1));
        let id = "y".repeat(300);
        src.subscribe("a/b", &id, partition(3), 1).unwrap();
        let dst = TopicIndex::new(node(2));

        assert_eq!(
            dst.import_entries(
                &src.export_for_partition(topic_partition("a/b")),
                &HashSet::new()
            ),
            Ok(1)
        );
        assert_eq!(dst.get_client_topics(&id), vec![("a/b".to_string(), 1)]);
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
    fn a_wide_entry_declaring_more_subscribers_than_it_carries_is_refused() {
        let mut bytes = vec![WIDE_ENTRY_VERSION, 0xff, 0xff, 0, 1, b'a'];
        bytes.extend_from_slice(&0u64.to_be_bytes());
        bytes.extend_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(TopicIndexEntry::from_bytes(&bytes), None);
    }

    fn entry_with(client_ids: &[&str]) -> TopicIndexEntry {
        let mut entry = TopicIndexEntry::create("a");
        for client_id in client_ids {
            entry
                .subscribers
                .push(SubscriberLocation::create(client_id, partition(1), 1));
        }
        entry
    }

    #[test]
    fn truncated_entries_are_refused() {
        let narrow = entry_with(&["c1", "c2"]).to_bytes().unwrap();
        assert_eq!(narrow[0], NARROW_ENTRY_VERSION);
        let wide_client = "w".repeat(300);
        let wide = entry_with(&["c1", "c2", &wide_client]).to_bytes().unwrap();
        assert_eq!(wide[0], WIDE_ENTRY_VERSION);

        assert_eq!(
            TopicIndexEntry::from_bytes(&narrow[..narrow.len() - 6]),
            None
        );
        assert_eq!(
            TopicIndexEntry::from_bytes(&narrow[..narrow.len() - 1]),
            None
        );
        assert_eq!(TopicIndexEntry::from_bytes(&wide[..wide.len() - 306]), None);
        assert_eq!(TopicIndexEntry::from_bytes(&wide[..wide.len() - 1]), None);
    }

    #[test]
    fn a_wide_entry_without_the_narrow_reader_guard_is_refused() {
        let mut bytes = entry_with(&[&"w".repeat(300)]).to_bytes().unwrap();
        assert_eq!(bytes[0], WIDE_ENTRY_VERSION);
        bytes[1] = 0;
        assert_eq!(TopicIndexEntry::from_bytes(&bytes), None);
    }

    #[test]
    fn entries_with_trailing_bytes_are_refused() {
        for entry in [entry_with(&["c1"]), entry_with(&[&"w".repeat(300)])] {
            let mut bytes = entry.to_bytes().unwrap();
            assert_eq!(TopicIndexEntry::from_bytes(&bytes), Some(entry));
            bytes.push(0);
            assert_eq!(TopicIndexEntry::from_bytes(&bytes), None);
        }
    }

    #[test]
    fn an_import_keeps_local_clients_it_lacks_and_replaces_the_rest() {
        let index = TopicIndex::new(node(1));
        index.subscribe("a/b", "local", partition(1), 1).unwrap();
        index.subscribe("a/b", "remote", partition(2), 1).unwrap();
        let source = TopicIndex::new(node(2));
        source.subscribe("a/b", "other", partition(3), 0).unwrap();

        index
            .import_entries(
                &source.export_for_partition(topic_partition("a/b")),
                &HashSet::from(["local".to_string()]),
            )
            .unwrap();

        let mut clients: Vec<String> = index
            .get_subscribers("a/b")
            .iter()
            .map(|s| s.client_id_str().to_string())
            .collect();
        clients.sort();
        assert_eq!(clients, ["local", "other"]);
        assert_eq!(index.get_client_topics("local"), [("a/b".to_string(), 1)]);
        assert!(index.get_client_topics("remote").is_empty());
    }

    #[test]
    fn an_import_does_not_restore_a_topic_a_local_client_left() {
        let index = TopicIndex::new(node(1));
        index.subscribe("a/b", "local", partition(1), 1).unwrap();
        let source = TopicIndex::new(node(2));
        source.subscribe("a/b", "local", partition(1), 1).unwrap();
        index.unsubscribe("a/b", "local").unwrap();

        index
            .import_entries(
                &source.export_for_partition(topic_partition("a/b")),
                &HashSet::from(["local".to_string()]),
            )
            .unwrap();

        assert!(index.get_client_topics("local").is_empty());
        assert!(index.get_entry("a/b").is_none());
    }

    #[test]
    fn an_import_keeps_a_local_clients_qos_over_the_snapshots() {
        let index = TopicIndex::new(node(1));
        index.subscribe("a/b", "local", partition(1), 2).unwrap();
        let source = TopicIndex::new(node(2));
        source.subscribe("a/b", "local", partition(1), 0).unwrap();

        index
            .import_entries(
                &source.export_for_partition(topic_partition("a/b")),
                &HashSet::from(["local".to_string()]),
            )
            .unwrap();

        assert_eq!(index.get_client_topics("local"), [("a/b".to_string(), 2)]);
    }
}
