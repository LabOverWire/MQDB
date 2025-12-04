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

    pub fn with_operation_id(mut self, operation_id: String) -> Self {
        self.operation_id = Some(operation_id);
        self
    }

    pub fn create(entity: String, id: String, data: serde_json::Value) -> Self {
        Self::new(entity, id, Operation::Create, Some(data))
    }

    pub fn update(entity: String, id: String, data: serde_json::Value) -> Self {
        Self::new(entity, id, Operation::Update, Some(data))
    }

    pub fn delete(entity: String, id: String) -> Self {
        Self::new(entity, id, Operation::Delete, None)
    }
}
