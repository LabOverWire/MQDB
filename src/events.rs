use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

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
}

impl ChangeEvent {
    pub fn new(entity: String, id: String, operation: Operation, data: Option<serde_json::Value>) -> Self {
        Self {
            sequence: SEQUENCE.fetch_add(1, Ordering::SeqCst),
            entity,
            id,
            operation,
            data,
            operation_id: None,
        }
    }

    #[must_use]
    pub fn with_operation_id(mut self, operation_id: String) -> Self {
        self.operation_id = Some(operation_id);
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
    pub fn delete(entity: String, id: String) -> Self {
        Self::new(entity, id, Operation::Delete, None)
    }

    #[must_use]
    pub fn partition(&self, num_partitions: u8) -> u8 {
        if num_partitions == 0 {
            return 0;
        }
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let key = format!("{}:{}", self.entity, self.id);
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() % num_partitions as u64) as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let e3 = ChangeEvent::delete("orders".into(), "42".into());

        assert_eq!(e1.partition(8), e2.partition(8));
        assert_eq!(e2.partition(8), e3.partition(8));
    }
}
