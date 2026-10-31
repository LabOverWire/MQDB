// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::protocol::Operation;
use super::store_utils;
use super::{
    NodeId, PartitionId, SubscriberLocation, SubscriptionType, TopicTrie, WildcardSubscriber,
    is_wildcard_pattern, validate_pattern,
};
use bebytes::BeBytes;
use std::sync::{PoisonError, RwLock};

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

fn snapshot_frame(pattern: &str, client_id: &str, data: &[u8]) -> Vec<u8> {
    let id = format!("{pattern}:{client_id}");
    let (id_len, id_bytes) = match u16::try_from(id.len()) {
        Ok(len) => (len, id.as_bytes()),
        Err(_) => {
            tracing::warn!(
                pattern_len = pattern.len(),
                client_id,
                "wildcard id too long for the snapshot id field, sent without it"
            );
            (0, &[][..])
        }
    };
    let data_len = u32::try_from(data.len()).unwrap_or(u32::MAX);
    let mut frame = Vec::with_capacity(6 + id_bytes.len() + data.len());
    frame.extend_from_slice(&id_len.to_be_bytes());
    frame.extend_from_slice(id_bytes);
    frame.extend_from_slice(&data_len.to_be_bytes());
    frame.extend_from_slice(data);
    frame
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

        self.trie
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(pattern, subscriber);
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

    /// # Errors
    /// Returns `InvalidPattern` or `NotFound` for invalid/missing patterns.
    pub fn unsubscribe(&self, pattern: &str, client_id: &str) -> Result<(), WildcardStoreError> {
        if !validate_pattern(pattern) {
            return Err(WildcardStoreError::InvalidPattern);
        }

        let removed = self
            .trie
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(pattern, client_id);
        if removed {
            Ok(())
        } else {
            Err(WildcardStoreError::NotFound)
        }
    }

    #[must_use]
    pub fn match_topic(&self, topic: &str) -> Vec<SubscriberLocation> {
        let trie = self.trie.read().unwrap_or_else(PoisonError::into_inner);
        trie.match_topic(topic)
            .into_iter()
            .map(|sub| SubscriberLocation::create(&sub.client_id, sub.client_partition, sub.qos))
            .collect()
    }

    #[must_use]
    pub fn match_shared(&self, topic: &str) -> Vec<(String, String, SubscriberLocation)> {
        let trie = self.trie.read().unwrap_or_else(PoisonError::into_inner);
        trie.match_shared(topic)
            .into_iter()
            .map(|(group, filter, sub)| {
                (
                    group.to_string(),
                    filter.to_string(),
                    SubscriberLocation::create(&sub.client_id, sub.client_partition, sub.qos),
                )
            })
            .collect()
    }

    #[must_use]
    pub fn pattern_count(&self) -> usize {
        self.trie
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .pattern_count()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.trie
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    }

    #[must_use]
    pub fn get_client_patterns(&self, client_id: &str) -> Vec<(String, u8)> {
        self.trie
            .read()
            .unwrap_or_else(PoisonError::into_inner)
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

        self.trie
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(pattern, subscriber);

        let entry =
            WildcardEntry::create(pattern, client_id, client_partition, qos, subscription_type);
        let data = Self::serialize_entry(&entry);
        Ok((entry, data))
    }

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

        let removed = self
            .trie
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(pattern, client_id);
        if removed {
            Ok(Vec::new())
        } else {
            Err(WildcardStoreError::NotFound)
        }
    }

    /// # Errors
    /// Returns an error if ID parsing or deserialization fails.
    pub fn apply_replicated(
        &self,
        operation: Operation,
        id: &str,
        data: &[u8],
    ) -> Result<(), WildcardStoreError> {
        match operation {
            Operation::Insert | Operation::Update => {
                let entry =
                    Self::deserialize_entry(data).ok_or(WildcardStoreError::SerializationError)?;
                self.insert_entry(&entry);
                Ok(())
            }
            Operation::Delete => {
                let (pattern, client_id) = id
                    .split_once(':')
                    .ok_or(WildcardStoreError::InvalidPattern)?;
                self.trie
                    .write()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(pattern, client_id);
                Ok(())
            }
        }
    }

    fn insert_entry(&self, entry: &WildcardEntry) {
        let subscriber = WildcardSubscriber {
            client_id: entry.client_id_str().to_string(),
            client_partition: entry.partition().unwrap_or(PartitionId::ZERO),
            qos: entry.qos,
            subscription_type: entry.sub_type(),
        };
        self.trie
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(entry.pattern_str(), subscriber);
    }

    #[must_use]
    pub fn export_for_partition(&self, partition: PartitionId) -> Vec<u8> {
        let trie = self.trie.read().unwrap_or_else(PoisonError::into_inner);
        let partition_entries: Vec<_> = trie
            .all_subscriptions()
            .into_iter()
            .filter(|(_, sub)| sub.client_partition == partition)
            .collect();

        let count = u32::try_from(partition_entries.len()).unwrap_or(u32::MAX);
        let mut buf = count.to_be_bytes().to_vec();

        for (pattern, sub) in partition_entries
            .into_iter()
            .take(usize::try_from(count).unwrap_or(usize::MAX))
        {
            let entry = WildcardEntry::create(
                &pattern,
                &sub.client_id,
                sub.client_partition,
                sub.qos,
                sub.subscription_type,
            );
            let data = Self::serialize_entry(&entry);
            buf.extend_from_slice(&snapshot_frame(&pattern, &sub.client_id, &data));
        }

        buf
    }

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

            if let Some(entry) = Self::deserialize_entry(entry_data) {
                self.insert_entry(&entry);
                imported += 1;
            }
        }

        Ok(imported)
    }

    pub fn clear_partition(&self, partition: PartitionId) -> usize {
        self.trie
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .clear_for_partition(partition)
    }

    #[must_use]
    pub fn query(
        &self,
        _filter: Option<&str>,
        limit: u32,
        cursor: Option<&[u8]>,
    ) -> (Vec<WildcardEntry>, bool, Option<Vec<u8>>) {
        let trie = self.trie.read().unwrap_or_else(PoisonError::into_inner);
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
    use crate::cluster::session_partition;

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
    fn shared_wildcard_subscribers_match_by_their_filter_with_their_group() {
        let store = WildcardStore::new(node(1));
        store.subscribe_mqtt("sh/+", "plain", 1).unwrap();
        store
            .subscribe_mqtt("$share/g/sh/+", "member-g", 1)
            .unwrap();
        store.subscribe_mqtt("$share/h/#", "member-h", 1).unwrap();
        store
            .subscribe_mqtt("$share/k/other/+", "member-k", 1)
            .unwrap();

        let mut shared: Vec<(String, String, String)> = store
            .match_shared("sh/x")
            .into_iter()
            .map(|(group, filter, sub)| (group, filter, sub.client_id_str().to_string()))
            .collect();
        shared.sort();

        assert_eq!(
            shared,
            vec![
                ("g".to_string(), "sh/+".to_string(), "member-g".to_string()),
                ("h".to_string(), "#".to_string(), "member-h".to_string()),
            ]
        );
        let plain: Vec<_> = store
            .match_topic("sh/x")
            .iter()
            .map(|sub| sub.client_id_str().to_string())
            .collect();
        assert_eq!(plain, vec!["plain".to_string()]);

        store.unsubscribe("$share/g/sh/+", "member-g").unwrap();
        let remaining: Vec<String> = store
            .match_shared("sh/x")
            .into_iter()
            .map(|(group, _, _)| group)
            .collect();
        assert_eq!(remaining, vec!["h".to_string()]);
    }

    #[test]
    fn patterns_with_a_leading_empty_level_keep_it() {
        let store = WildcardStore::new(node(1));
        store.subscribe_mqtt("/x/+", "plain", 1).unwrap();
        store
            .subscribe_mqtt("$share/g//x/+", "member-g", 1)
            .unwrap();

        let shared: Vec<(String, String)> = store
            .match_shared("/x/y")
            .into_iter()
            .map(|(group, filter, _)| (group, filter))
            .collect();

        assert_eq!(shared, vec![("g".to_string(), "/x/+".to_string())]);
        assert!(store.match_shared("x/y").is_empty());
        assert_eq!(
            store.get_client_patterns("plain"),
            vec![("/x/+".to_string(), 1)]
        );
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

    fn snapshot_ids(snapshot: &[u8]) -> Vec<String> {
        let count = u32::from_be_bytes([snapshot[0], snapshot[1], snapshot[2], snapshot[3]]);
        let mut offset = 4;
        let mut ids = Vec::new();
        for _ in 0..count {
            let id_len = usize::from(u16::from_be_bytes([snapshot[offset], snapshot[offset + 1]]));
            offset += 2;
            ids.push(String::from_utf8(snapshot[offset..offset + id_len].to_vec()).unwrap());
            offset += id_len;
            let data_len = u32::from_be_bytes([
                snapshot[offset],
                snapshot[offset + 1],
                snapshot[offset + 2],
                snapshot[offset + 3],
            ]);
            offset += 4 + usize::try_from(data_len).unwrap();
        }
        assert_eq!(offset, snapshot.len());
        ids
    }

    fn long_filter() -> String {
        format!("{}/#", "a".repeat(65_450))
    }

    #[test]
    fn snapshot_keeps_a_filter_whose_id_does_not_fit_and_the_entries_after_it() {
        let src = WildcardStore::new(node(1));
        let client = "c".repeat(100);
        src.subscribe_mqtt(&long_filter(), &client, 1).unwrap();
        src.subscribe_mqtt("short/+", &client, 0).unwrap();
        src.subscribe_mqtt("other/#", &client, 2).unwrap();

        let snapshot = src.export_for_partition(session_partition(&client));
        let dst = WildcardStore::new(node(2));

        assert_eq!(dst.import_wildcards(&snapshot), Ok(3));
        let long_topic = format!("{}/x", "a".repeat(65_450));
        assert_eq!(dst.match_topic(&long_topic).len(), 1);
        assert_eq!(dst.match_topic("short/x").len(), 1);
        assert_eq!(dst.match_topic("other/x/y").len(), 1);
        let mut patterns = dst.get_client_patterns(&client);
        patterns.sort();
        let mut expected = src.get_client_patterns(&client);
        expected.sort();
        assert_eq!(patterns, expected);
    }

    #[test]
    fn snapshot_framing_stays_readable_by_id_based_importers() {
        let src = WildcardStore::new(node(1));
        let client = "c".repeat(100);
        src.subscribe_mqtt(&long_filter(), &client, 1).unwrap();
        src.subscribe_mqtt("short/+", &client, 0).unwrap();

        let snapshot = src.export_for_partition(session_partition(&client));
        let mut ids = snapshot_ids(&snapshot);
        ids.sort();

        assert_eq!(ids, vec![String::new(), format!("short/+:{client}")]);
    }

    #[test]
    fn snapshot_keeps_a_filter_containing_a_colon() {
        let src = WildcardStore::new(node(1));
        src.subscribe_mqtt("urn:dev/+", "client1", 1).unwrap();

        let snapshot = src.export_for_partition(session_partition("client1"));
        let dst = WildcardStore::new(node(2));

        assert_eq!(dst.import_wildcards(&snapshot), Ok(1));
        assert_eq!(
            dst.get_client_patterns("client1"),
            vec![("urn:dev/+".to_string(), 1)]
        );
        let matches = dst.match_topic("urn:dev/x");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].client_id_str(), "client1");
    }

    #[test]
    fn replicated_insert_keeps_a_filter_containing_a_colon() {
        let store = WildcardStore::new(node(1));
        let entry = WildcardEntry::create(
            "urn:dev/+",
            "client1",
            session_partition("client1"),
            1,
            SubscriptionType::Mqtt,
        );

        store
            .apply_replicated(
                Operation::Insert,
                "urn:dev/+:client1",
                &WildcardStore::serialize_entry(&entry),
            )
            .unwrap();

        assert_eq!(
            store.get_client_patterns("client1"),
            vec![("urn:dev/+".to_string(), 1)]
        );
    }
}
