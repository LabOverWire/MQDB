// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

const RESERVED_SCOPE_LEVEL: &str = "events";
const ENCODED_RESERVED_SCOPE_LEVEL: &str = "%65vents";

#[must_use]
pub fn scope_topic_level(scope_value: &str) -> String {
    if scope_value == RESERVED_SCOPE_LEVEL {
        return ENCODED_RESERVED_SCOPE_LEVEL.to_string();
    }
    let mut level = String::with_capacity(scope_value.len());
    for character in scope_value.chars() {
        match character {
            '%' => level.push_str("%25"),
            '/' => level.push_str("%2F"),
            '+' => level.push_str("%2B"),
            '#' => level.push_str("%23"),
            '\0' => level.push_str("%00"),
            other => level.push(other),
        }
    }
    level
}

#[must_use]
pub fn scope_value_from_level(level: &str) -> String {
    let mut value = String::with_capacity(level.len());
    let mut rest = level;
    while let Some((head, tail)) = rest.split_once('%') {
        value.push_str(head);
        let escaped = tail
            .get(..2)
            .and_then(|hex| u8::from_str_radix(hex, 16).ok())
            .filter(u8::is_ascii);
        rest = if let Some(byte) = escaped {
            value.push(char::from(byte));
            &tail[2..]
        } else {
            value.push('%');
            tail
        };
    }
    value.push_str(rest);
    value
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operation {
    Create,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub sequence: u64,
    pub entity: String,
    pub id: String,
    pub operation: Operation,
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<(String, String)>,
    #[serde(skip)]
    pub recipients: Option<Vec<String>>,
}

impl ChangeEvent {
    pub fn new(
        entity: String,
        id: String,
        operation: Operation,
        data: Option<serde_json::Value>,
    ) -> Self {
        Self {
            sequence: SEQUENCE.fetch_add(1, Ordering::SeqCst),
            entity,
            id,
            operation,
            data,
            operation_id: None,
            sender: None,
            client_id: None,
            scope: None,
            recipients: None,
        }
    }

    #[must_use]
    pub fn with_operation_id(mut self, operation_id: String) -> Self {
        self.operation_id = Some(operation_id);
        self
    }

    #[must_use]
    pub fn with_sender(mut self, sender: Option<String>) -> Self {
        self.sender = sender;
        self
    }

    #[must_use]
    pub fn with_client_id(mut self, client_id: Option<String>) -> Self {
        self.client_id = client_id;
        self
    }

    #[must_use]
    pub fn with_scope(mut self, scope: Option<(String, String)>) -> Self {
        self.scope = scope;
        self
    }

    #[must_use]
    pub fn create(entity: String, id: String, data: serde_json::Value) -> Self {
        Self::new(entity, id, Operation::Create, Some(data))
    }

    #[must_use]
    pub fn update(entity: String, id: String, data: serde_json::Value) -> Self {
        Self::new(entity, id, Operation::Update, Some(data))
    }

    #[must_use]
    pub fn delete(entity: String, id: String, data: serde_json::Value) -> Self {
        Self::new(entity, id, Operation::Delete, Some(data))
    }

    #[must_use]
    pub fn event_topic(&self, num_partitions: u8) -> String {
        let event_type = match self.operation {
            Operation::Create => "created",
            Operation::Update => "updated",
            Operation::Delete => "deleted",
        };

        if let Some((ref scope_entity, ref scope_value)) = self.scope {
            let level = scope_topic_level(scope_value);
            if *scope_entity == self.entity {
                return format!("$DB/{scope_entity}/{level}/events/{event_type}");
            }
            return format!(
                "$DB/{scope_entity}/{level}/{}/events/{event_type}",
                self.entity
            );
        }

        if num_partitions > 0 {
            let partition = self.partition(num_partitions);
            format!("$DB/{}/events/p{partition}/{}", self.entity, self.id)
        } else {
            format!("$DB/{}/events/{}", self.entity, self.id)
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn partition(&self, num_partitions: u8) -> u8 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        if num_partitions == 0 {
            return 0;
        }

        let key = format!("{}:{}", self.entity, self.id);
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() % u64::from(num_partitions)) as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scoped_create(value: &str) -> ChangeEvent {
        ChangeEvent::create("items".into(), "1".into(), serde_json::json!({}))
            .with_scope(Some(("lists".into(), value.into())))
    }

    #[test]
    fn a_scope_value_takes_exactly_one_topic_level() {
        for value in [
            "L1", "a/b", "/", "a//b", "a+b", "#", "50%", "%2F", "events", "\0", "é/ü", "",
        ] {
            let topic = scoped_create(value).event_topic(0);
            let levels: Vec<&str> = topic.split('/').collect();
            assert_eq!(levels.len(), 6, "{topic}");
            assert_eq!(levels[2], scope_topic_level(value), "{topic}");
            assert!(!levels[2].contains(['+', '#', '\0']), "{topic}");
            assert_ne!(levels[2], "events", "{topic}");
            assert_eq!(scope_value_from_level(levels[2]), value, "{topic}");
        }
    }

    #[test]
    fn distinct_scope_values_never_share_a_topic_level() {
        let values = [
            "a/b", "a%2Fb", "a%252Fb", "events", "%65vents", "a", "a+b", "a%2Bb",
        ];
        for (position, left) in values.iter().enumerate() {
            for right in &values[position + 1..] {
                assert_ne!(
                    scope_topic_level(left),
                    scope_topic_level(right),
                    "{left} {right}"
                );
            }
        }
    }

    #[test]
    fn plain_scope_values_keep_their_topic() {
        assert_eq!(
            scoped_create("L1").event_topic(0),
            "$DB/lists/L1/items/events/created"
        );
    }

    #[test]
    fn test_partition_zero_partitions() {
        let event = ChangeEvent::create("users".into(), "1".into(), serde_json::json!({}));
        assert_eq!(event.partition(0), 0);
    }

    #[test]
    fn test_partition_determinism() {
        let event = ChangeEvent::create("orders".into(), "123".into(), serde_json::json!({}));
        let p1 = event.partition(8);
        let p2 = event.partition(8);
        assert_eq!(p1, p2);
    }

    #[test]
    fn test_partition_distribution() {
        let mut counts = [0u32; 8];
        for i in 0..100 {
            let event = ChangeEvent::create("orders".into(), i.to_string(), serde_json::json!({}));
            let partition = event.partition(8);
            counts[partition as usize] += 1;
        }
        for count in counts {
            assert!(count > 0, "partition should have at least one event");
        }
    }

    #[test]
    fn test_partition_same_entity_same_id() {
        let e1 = ChangeEvent::create("orders".into(), "42".into(), serde_json::json!({"a": 1}));
        let e2 = ChangeEvent::update("orders".into(), "42".into(), serde_json::json!({"b": 2}));
        let e3 = ChangeEvent::delete("orders".into(), "42".into(), serde_json::json!({"a": 1}));

        assert_eq!(e1.partition(8), e2.partition(8));
        assert_eq!(e2.partition(8), e3.partition(8));
    }
}
