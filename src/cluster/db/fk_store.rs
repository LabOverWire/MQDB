// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::partition::data_partition;
use crate::cluster::protocol::Operation;
use crate::cluster::{NodeId, PartitionId};
use bebytes::BeBytes;
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct FkValidationRequest {
    pub version: u8,
    pub entity_len: u16,
    #[FromField(entity_len)]
    pub entity: Vec<u8>,
    pub id_len: u16,
    #[FromField(id_len)]
    pub id: Vec<u8>,
    pub request_id_len: u16,
    #[FromField(request_id_len)]
    pub request_id: Vec<u8>,
    pub timeout_ms: u32,
    pub created_at: u64,
}

impl FkValidationRequest {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(entity: &str, id: &str, request_id: &str, timeout_ms: u32, now: u64) -> Self {
        let entity_bytes = entity.as_bytes().to_vec();
        let id_bytes = id.as_bytes().to_vec();
        let request_id_bytes = request_id.as_bytes().to_vec();

        Self {
            version: 1,
            entity_len: entity_bytes.len() as u16,
            entity: entity_bytes,
            id_len: id_bytes.len() as u16,
            id: id_bytes,
            request_id_len: request_id_bytes.len() as u16,
            request_id: request_id_bytes,
            timeout_ms,
            created_at: now,
        }
    }

    #[must_use]
    pub fn entity_str(&self) -> &str {
        std::str::from_utf8(&self.entity).unwrap_or("")
    }

    #[must_use]
    pub fn id_str(&self) -> &str {
        std::str::from_utf8(&self.id).unwrap_or("")
    }

    #[must_use]
    pub fn request_id_str(&self) -> &str {
        std::str::from_utf8(&self.request_id).unwrap_or("")
    }

    #[must_use]
    pub fn target_partition(&self) -> PartitionId {
        data_partition(self.entity_str(), self.id_str())
    }

    #[must_use]
    pub fn is_expired(&self, now: u64) -> bool {
        now > self.created_at + u64::from(self.timeout_ms)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FkValidationResult {
    Valid = 0,
    Invalid = 1,
    Timeout = 2,
    Unknown = 3,
}

impl From<u8> for FkValidationResult {
    fn from(v: u8) -> Self {
        match v {
            0 => Self::Valid,
            1 => Self::Invalid,
            2 => Self::Timeout,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct FkValidationResponse {
    pub version: u8,
    pub request_id_len: u16,
    #[FromField(request_id_len)]
    pub request_id: Vec<u8>,
    pub result: u8,
}

impl FkValidationResponse {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(request_id: &str, result: FkValidationResult) -> Self {
        let request_id_bytes = request_id.as_bytes().to_vec();

        Self {
            version: 1,
            request_id_len: request_id_bytes.len() as u16,
            request_id: request_id_bytes,
            result: result as u8,
        }
    }

    #[must_use]
    pub fn request_id_str(&self) -> &str {
        std::str::from_utf8(&self.request_id).unwrap_or("")
    }

    #[must_use]
    pub fn result(&self) -> FkValidationResult {
        FkValidationResult::from(self.result)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FkStoreError {
    NotFound,
    AlreadyExists,
    SerializationError,
}

impl std::fmt::Display for FkStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "FK validation request not found"),
            Self::AlreadyExists => write!(f, "FK validation request already exists"),
            Self::SerializationError => write!(f, "serialization error"),
        }
    }
}

impl std::error::Error for FkStoreError {}

pub struct FkValidationStore {
    node_id: NodeId,
    pending_requests: RwLock<HashMap<String, FkValidationRequest>>,
}

impl FkValidationStore {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            pending_requests: RwLock::new(HashMap::new()),
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `AlreadyExists` if a request with the same ID already exists.
    pub fn add_request(&self, request: FkValidationRequest) -> Result<(), FkStoreError> {
        let mut requests = self.pending_requests.write().unwrap();
        let key = request.request_id_str().to_string();

        if requests.contains_key(&key) {
            return Err(FkStoreError::AlreadyExists);
        }

        requests.insert(key, request);
        Ok(())
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `NotFound` if the request does not exist.
    pub fn complete_request(&self, request_id: &str) -> Result<FkValidationRequest, FkStoreError> {
        self.pending_requests
            .write()
            .unwrap()
            .remove(request_id)
            .ok_or(FkStoreError::NotFound)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get_request(&self, request_id: &str) -> Option<FkValidationRequest> {
        self.pending_requests
            .read()
            .unwrap()
            .get(request_id)
            .cloned()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn cleanup_expired(&self, now: u64) -> usize {
        let mut requests = self.pending_requests.write().unwrap();
        let before = requests.len();
        requests.retain(|_, r| !r.is_expired(now));
        before - requests.len()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn count(&self) -> usize {
        self.pending_requests.read().unwrap().len()
    }

    #[must_use]
    pub fn serialize_request(request: &FkValidationRequest) -> Vec<u8> {
        request.to_be_bytes()
    }

    #[must_use]
    pub fn deserialize_request(bytes: &[u8]) -> Option<FkValidationRequest> {
        FkValidationRequest::try_from_be_bytes(bytes)
            .ok()
            .map(|(r, _)| r)
    }

    #[must_use]
    pub fn serialize_response(response: &FkValidationResponse) -> Vec<u8> {
        response.to_be_bytes()
    }

    #[must_use]
    pub fn deserialize_response(bytes: &[u8]) -> Option<FkValidationResponse> {
        FkValidationResponse::try_from_be_bytes(bytes)
            .ok()
            .map(|(r, _)| r)
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
    ) -> Result<(), FkStoreError> {
        match operation {
            Operation::Delete => {
                self.pending_requests.write().unwrap().remove(id);
                Ok(())
            }
            Operation::Insert | Operation::Update => {
                let request =
                    Self::deserialize_request(data).ok_or(FkStoreError::SerializationError)?;
                self.pending_requests
                    .write()
                    .unwrap()
                    .insert(id.to_string(), request);
                Ok(())
            }
        }
    }
}

impl std::fmt::Debug for FkValidationStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FkValidationStore")
            .field("node_id", &self.node_id)
            .field("pending_count", &self.count())
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
    fn fk_validation_request_bebytes_roundtrip() {
        let request = FkValidationRequest::create("users", "user123", "req-abc", 2000, 1000);
        let bytes = FkValidationStore::serialize_request(&request);
        let parsed = FkValidationStore::deserialize_request(&bytes).unwrap();

        assert_eq!(request.entity, parsed.entity);
        assert_eq!(request.id, parsed.id);
        assert_eq!(request.request_id, parsed.request_id);
        assert_eq!(request.timeout_ms, parsed.timeout_ms);
    }

    #[test]
    fn fk_validation_response_bebytes_roundtrip() {
        let response = FkValidationResponse::create("req-abc", FkValidationResult::Valid);
        let bytes = FkValidationStore::serialize_response(&response);
        let parsed = FkValidationStore::deserialize_response(&bytes).unwrap();

        assert_eq!(response.request_id, parsed.request_id);
        assert_eq!(response.result, parsed.result);
    }

    #[test]
    fn add_and_complete_request() {
        let store = FkValidationStore::new(node(1));
        let request = FkValidationRequest::create("users", "user123", "req-abc", 2000, 1000);

        store.add_request(request.clone()).unwrap();

        assert!(store.get_request("req-abc").is_some());

        let completed = store.complete_request("req-abc").unwrap();
        assert_eq!(completed.id_str(), "user123");

        assert!(store.get_request("req-abc").is_none());
    }

    #[test]
    fn add_duplicate_fails() {
        let store = FkValidationStore::new(node(1));
        let request = FkValidationRequest::create("users", "user123", "req-abc", 2000, 1000);

        store.add_request(request.clone()).unwrap();
        let result = store.add_request(request);

        assert_eq!(result, Err(FkStoreError::AlreadyExists));
    }

    #[test]
    fn request_is_expired() {
        let request = FkValidationRequest::create("users", "user123", "req-abc", 2000, 1000);

        assert!(!request.is_expired(2000));
        assert!(!request.is_expired(3000));
        assert!(request.is_expired(3001));
    }

    #[test]
    fn cleanup_expired_removes_old_requests() {
        let store = FkValidationStore::new(node(1));

        store
            .add_request(FkValidationRequest::create(
                "users", "user1", "req-1", 1000, 1000,
            ))
            .unwrap();

        store
            .add_request(FkValidationRequest::create(
                "users", "user2", "req-2", 10000, 1000,
            ))
            .unwrap();

        let removed = store.cleanup_expired(3000);
        assert_eq!(removed, 1);
        assert_eq!(store.count(), 1);
        assert!(store.get_request("req-2").is_some());
    }

    #[test]
    fn validation_result_roundtrip() {
        assert_eq!(FkValidationResult::from(0), FkValidationResult::Valid);
        assert_eq!(FkValidationResult::from(1), FkValidationResult::Invalid);
        assert_eq!(FkValidationResult::from(2), FkValidationResult::Timeout);
        assert_eq!(FkValidationResult::from(255), FkValidationResult::Unknown);
    }

    #[test]
    fn apply_replicated_insert() {
        let store = FkValidationStore::new(node(1));
        let request = FkValidationRequest::create("users", "user456", "req-test", 2000, 1000);
        let data = FkValidationStore::serialize_request(&request);

        store
            .apply_replicated(Operation::Insert, "req-test", &data)
            .unwrap();

        assert_eq!(store.count(), 1);
    }
}
