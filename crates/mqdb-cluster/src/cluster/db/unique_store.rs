// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::partition::unique_partition;
use crate::cluster::protocol::Operation;
use crate::cluster::{NodeId, PartitionId};
use bebytes::BeBytes;
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct UniqueReservation {
    pub version: u8,
    pub entity_len: u16,
    #[FromField(entity_len)]
    pub entity: Vec<u8>,
    pub field_len: u16,
    #[FromField(field_len)]
    pub field: Vec<u8>,
    pub value_len: u32,
    #[FromField(value_len)]
    pub value: Vec<u8>,
    pub record_id_len: u16,
    #[FromField(record_id_len)]
    pub record_id: Vec<u8>,
    pub request_id_len: u16,
    #[FromField(request_id_len)]
    pub request_id: Vec<u8>,
    pub data_partition: u16,
    pub expires_at: u64,
    pub committed: u8,
}

impl UniqueReservation {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(
        entity: &str,
        field: &str,
        value: &[u8],
        record_id: &str,
        request_id: &str,
        data_partition: PartitionId,
        expires_at: u64,
    ) -> Self {
        let entity_bytes = entity.as_bytes().to_vec();
        let field_bytes = field.as_bytes().to_vec();
        let record_id_bytes = record_id.as_bytes().to_vec();
        let request_id_bytes = request_id.as_bytes().to_vec();

        Self {
            version: 1,
            entity_len: entity_bytes.len() as u16,
            entity: entity_bytes,
            field_len: field_bytes.len() as u16,
            field: field_bytes,
            value_len: value.len() as u32,
            value: value.to_vec(),
            record_id_len: record_id_bytes.len() as u16,
            record_id: record_id_bytes,
            request_id_len: request_id_bytes.len() as u16,
            request_id: request_id_bytes,
            data_partition: data_partition.get(),
            expires_at,
            committed: 0,
        }
    }

    #[must_use]
    pub fn entity_str(&self) -> &str {
        std::str::from_utf8(&self.entity).unwrap_or("")
    }

    #[must_use]
    pub fn field_str(&self) -> &str {
        std::str::from_utf8(&self.field).unwrap_or("")
    }

    #[must_use]
    pub fn record_id_str(&self) -> &str {
        std::str::from_utf8(&self.record_id).unwrap_or("")
    }

    #[must_use]
    pub fn request_id_str(&self) -> &str {
        std::str::from_utf8(&self.request_id).unwrap_or("")
    }

    #[must_use]
    pub fn data_partition(&self) -> PartitionId {
        PartitionId::new(self.data_partition).unwrap_or(PartitionId::ZERO)
    }

    #[must_use]
    pub fn unique_partition(&self) -> PartitionId {
        unique_partition(self.entity_str(), self.field_str(), &self.value)
    }

    #[must_use]
    pub fn is_committed(&self) -> bool {
        self.committed != 0
    }

    #[must_use]
    pub fn is_expired(&self, now: u64) -> bool {
        !self.is_committed() && now > self.expires_at
    }

    #[must_use]
    pub fn with_committed(mut self) -> Self {
        self.committed = 1;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniqueStoreError {
    AlreadyReserved,
    AlreadyCommitted,
    NotFound,
    WrongRequestId,
    SerializationError,
}

impl std::fmt::Display for UniqueStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyReserved => write!(f, "value already reserved by another request"),
            Self::AlreadyCommitted => write!(f, "value already committed"),
            Self::NotFound => write!(f, "reservation not found"),
            Self::WrongRequestId => write!(f, "request id does not match"),
            Self::SerializationError => write!(f, "serialization error"),
        }
    }
}

impl std::error::Error for UniqueStoreError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReserveResult {
    Reserved,
    AlreadyReservedBySameRequest,
    Conflict,
}

pub struct UniqueReserveParams<'a> {
    pub entity: &'a str,
    pub field: &'a str,
    pub value: &'a [u8],
    pub record_id: &'a str,
    pub request_id: &'a str,
    pub data_partition: PartitionId,
    pub ttl_ms: u64,
}

pub struct UniqueStore {
    node_id: NodeId,
    reservations: RwLock<HashMap<String, UniqueReservation>>,
}

impl UniqueStore {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            reservations: RwLock::new(HashMap::new()),
        }
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn reserve(&self, params: &UniqueReserveParams<'_>, now: u64) -> ReserveResult {
        let key = Self::unique_key(params.entity, params.field, params.value);
        let mut reservations = self.reservations.write().unwrap();

        if let Some(existing) = reservations.get(&key) {
            if existing.is_committed() {
                return ReserveResult::Conflict;
            }

            if existing.request_id_str() == params.request_id {
                return ReserveResult::AlreadyReservedBySameRequest;
            }

            if !existing.is_expired(now) {
                return ReserveResult::Conflict;
            }
        }

        let reservation = UniqueReservation::create(
            params.entity,
            params.field,
            params.value,
            params.record_id,
            params.request_id,
            params.data_partition,
            now + params.ttl_ms,
        );
        reservations.insert(key, reservation);
        ReserveResult::Reserved
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns error if reservation not found, wrong `request_id`, or already committed.
    pub fn commit(
        &self,
        entity: &str,
        field: &str,
        value: &[u8],
        request_id: &str,
    ) -> Result<UniqueReservation, UniqueStoreError> {
        let key = Self::unique_key(entity, field, value);
        let mut reservations = self.reservations.write().unwrap();

        let existing = reservations.get(&key).ok_or(UniqueStoreError::NotFound)?;

        if existing.is_committed() {
            return Err(UniqueStoreError::AlreadyCommitted);
        }

        if existing.request_id_str() != request_id {
            return Err(UniqueStoreError::WrongRequestId);
        }

        let committed = existing.clone().with_committed();
        reservations.insert(key, committed.clone());
        Ok(committed)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn release(&self, entity: &str, field: &str, value: &[u8], request_id: &str) -> bool {
        let key = Self::unique_key(entity, field, value);
        let mut reservations = self.reservations.write().unwrap();

        if let Some(existing) = reservations.get(&key) {
            if !existing.is_committed() && existing.request_id_str() == request_id {
                reservations.remove(&key);
                return true;
            }
            if existing.is_committed() && existing.record_id_str() == request_id {
                reservations.remove(&key);
                return true;
            }
        }
        false
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn check(&self, entity: &str, field: &str, value: &[u8], now: u64) -> bool {
        let key = Self::unique_key(entity, field, value);
        let reservations = self.reservations.read().unwrap();

        if let Some(existing) = reservations.get(&key)
            && (existing.is_committed() || !existing.is_expired(now))
        {
            return false;
        }
        true
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get(&self, entity: &str, field: &str, value: &[u8]) -> Option<UniqueReservation> {
        let key = Self::unique_key(entity, field, value);
        self.reservations.read().unwrap().get(&key).cloned()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get_by_request_id(&self, request_id: &str) -> Option<UniqueReservation> {
        self.reservations
            .read()
            .unwrap()
            .values()
            .find(|r| r.request_id_str() == request_id)
            .cloned()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn cleanup_expired(&self, now: u64) -> usize {
        let mut reservations = self.reservations.write().unwrap();
        let before = reservations.len();

        reservations.retain(|_, r| r.is_committed() || !r.is_expired(now));

        before - reservations.len()
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn count(&self) -> usize {
        self.reservations.read().unwrap().len()
    }

    #[must_use]
    pub fn serialize(reservation: &UniqueReservation) -> Vec<u8> {
        reservation.to_be_bytes()
    }

    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<UniqueReservation> {
        UniqueReservation::try_from_be_bytes(bytes)
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
    ) -> Result<(), UniqueStoreError> {
        match operation {
            Operation::Insert | Operation::Update => {
                let reservation =
                    Self::deserialize(data).ok_or(UniqueStoreError::SerializationError)?;
                self.reservations
                    .write()
                    .unwrap()
                    .insert(id.to_string(), reservation);
                Ok(())
            }
            Operation::Delete => {
                self.reservations.write().unwrap().remove(id);
                Ok(())
            }
        }
    }

    fn unique_key(entity: &str, field: &str, value: &[u8]) -> String {
        fn encode_hex(bytes: &[u8]) -> String {
            use std::fmt::Write;
            bytes
                .iter()
                .fold(String::with_capacity(bytes.len() * 2), |mut acc, b| {
                    let _ = write!(acc, "{b:02x}");
                    acc
                })
        }
        format!("_unique/{entity}/{field}/{}", encode_hex(value))
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn export_for_partition(&self, partition: PartitionId) -> Vec<u8> {
        let reservations = self.reservations.read().unwrap();
        let matching: Vec<_> = reservations
            .iter()
            .filter(|(_, r)| r.unique_partition() == partition)
            .collect();

        let mut buf = Vec::new();
        buf.extend_from_slice(&(matching.len() as u32).to_be_bytes());

        for (key, reservation) in matching {
            let key_bytes = key.as_bytes();
            buf.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(key_bytes);

            let data = Self::serialize(reservation);
            buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
            buf.extend_from_slice(&data);
        }

        buf
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    ///
    /// # Errors
    /// Returns `SerializationError` if a key fails UTF-8 parsing or a
    /// reservation fails to deserialize.
    pub fn import_reservations(&self, data: &[u8]) -> Result<usize, UniqueStoreError> {
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
            let key_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2;

            if offset + key_len > data.len() {
                break;
            }
            let key = std::str::from_utf8(&data[offset..offset + key_len])
                .map_err(|_| UniqueStoreError::SerializationError)?;
            offset += key_len;

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
            let res_bytes = &data[offset..offset + data_len];
            offset += data_len;

            let reservation =
                Self::deserialize(res_bytes).ok_or(UniqueStoreError::SerializationError)?;
            self.reservations
                .write()
                .unwrap()
                .insert(key.to_string(), reservation);
            imported += 1;
        }

        Ok(imported)
    }

    /// # Panics
    /// Panics if the internal lock is poisoned.
    pub fn clear_partition(&self, partition: PartitionId) -> usize {
        let mut reservations = self.reservations.write().unwrap();
        let before = reservations.len();
        reservations.retain(|_, r| r.unique_partition() != partition);
        before - reservations.len()
    }
}

impl std::fmt::Debug for UniqueStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UniqueStore")
            .field("node_id", &self.node_id)
            .field("reservation_count", &self.count())
            .finish_non_exhaustive()
    }
}

#[must_use]
pub fn unique_key(reservation: &UniqueReservation) -> String {
    fn encode_hex(bytes: &[u8]) -> String {
        use std::fmt::Write;
        bytes
            .iter()
            .fold(String::with_capacity(bytes.len() * 2), |mut acc, b| {
                let _ = write!(acc, "{b:02x}");
                acc
            })
    }
    format!(
        "_unique/{}/{}/{}",
        reservation.entity_str(),
        reservation.field_str(),
        encode_hex(&reservation.value)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    fn partition(id: u16) -> PartitionId {
        PartitionId::new(id).unwrap()
    }

    fn params<'a>(
        entity: &'a str,
        field: &'a str,
        value: &'a [u8],
        record_id: &'a str,
        request_id: &'a str,
        data_partition: PartitionId,
        ttl_ms: u64,
    ) -> UniqueReserveParams<'a> {
        UniqueReserveParams {
            entity,
            field,
            value,
            record_id,
            request_id,
            data_partition,
            ttl_ms,
        }
    }

    #[test]
    fn unique_reservation_bebytes_roundtrip() {
        let reservation = UniqueReservation::create(
            "users",
            "email",
            b"alice@example.com",
            "user123",
            "req-abc",
            partition(42),
            1_702_041_600_000,
        );
        let bytes = UniqueStore::serialize(&reservation);
        let parsed = UniqueStore::deserialize(&bytes).unwrap();

        assert_eq!(reservation.entity, parsed.entity);
        assert_eq!(reservation.field, parsed.field);
        assert_eq!(reservation.value, parsed.value);
        assert_eq!(reservation.record_id, parsed.record_id);
        assert_eq!(reservation.request_id, parsed.request_id);
    }

    #[test]
    fn reserve_new_value_succeeds() {
        let store = UniqueStore::new(node(1));

        let result = store.reserve(
            &params(
                "users",
                "email",
                b"alice@example.com",
                "user123",
                "req-abc",
                partition(42),
                5000,
            ),
            1000,
        );

        assert_eq!(result, ReserveResult::Reserved);
    }

    #[test]
    fn reserve_same_request_id_is_idempotent() {
        let store = UniqueStore::new(node(1));

        store.reserve(
            &params(
                "users",
                "email",
                b"alice@example.com",
                "user123",
                "req-abc",
                partition(42),
                5000,
            ),
            1000,
        );

        let result = store.reserve(
            &params(
                "users",
                "email",
                b"alice@example.com",
                "user456",
                "req-abc",
                partition(42),
                5000,
            ),
            1000,
        );

        assert_eq!(result, ReserveResult::AlreadyReservedBySameRequest);
    }

    #[test]
    fn reserve_different_request_id_conflicts() {
        let store = UniqueStore::new(node(1));

        store.reserve(
            &params(
                "users",
                "email",
                b"alice@example.com",
                "user123",
                "req-abc",
                partition(42),
                5000,
            ),
            1000,
        );

        let result = store.reserve(
            &params(
                "users",
                "email",
                b"alice@example.com",
                "user456",
                "req-xyz",
                partition(42),
                5000,
            ),
            1000,
        );

        assert_eq!(result, ReserveResult::Conflict);
    }

    #[test]
    fn reserve_after_commit_conflicts() {
        let store = UniqueStore::new(node(1));

        store.reserve(
            &params(
                "users",
                "email",
                b"alice@example.com",
                "user123",
                "req-abc",
                partition(42),
                5000,
            ),
            1000,
        );
        store
            .commit("users", "email", b"alice@example.com", "req-abc")
            .unwrap();

        let result = store.reserve(
            &params(
                "users",
                "email",
                b"alice@example.com",
                "user456",
                "req-xyz",
                partition(42),
                5000,
            ),
            2000,
        );

        assert_eq!(result, ReserveResult::Conflict);
    }

    #[test]
    fn reserve_after_expiry_succeeds() {
        let store = UniqueStore::new(node(1));

        store.reserve(
            &params(
                "users",
                "email",
                b"alice@example.com",
                "user123",
                "req-abc",
                partition(42),
                5000,
            ),
            1000,
        );

        let result = store.reserve(
            &params(
                "users",
                "email",
                b"alice@example.com",
                "user456",
                "req-xyz",
                partition(42),
                5000,
            ),
            7000,
        );

        assert_eq!(result, ReserveResult::Reserved);
    }

    #[test]
    fn commit_succeeds() {
        let store = UniqueStore::new(node(1));

        store.reserve(
            &params(
                "users",
                "email",
                b"alice@example.com",
                "user123",
                "req-abc",
                partition(42),
                5000,
            ),
            1000,
        );

        let committed = store
            .commit("users", "email", b"alice@example.com", "req-abc")
            .unwrap();

        assert!(committed.is_committed());
        assert_eq!(committed.record_id_str(), "user123");
    }

    #[test]
    fn commit_wrong_request_id_fails() {
        let store = UniqueStore::new(node(1));

        store.reserve(
            &params(
                "users",
                "email",
                b"alice@example.com",
                "user123",
                "req-abc",
                partition(42),
                5000,
            ),
            1000,
        );

        let result = store.commit("users", "email", b"alice@example.com", "req-wrong");

        assert_eq!(result, Err(UniqueStoreError::WrongRequestId));
    }

    #[test]
    fn release_removes_reservation() {
        let store = UniqueStore::new(node(1));

        store.reserve(
            &params(
                "users",
                "email",
                b"alice@example.com",
                "user123",
                "req-abc",
                partition(42),
                5000,
            ),
            1000,
        );

        let released = store.release("users", "email", b"alice@example.com", "req-abc");
        assert!(released);

        let available = store.check("users", "email", b"alice@example.com", 1000);
        assert!(available);
    }

    #[test]
    fn release_committed_does_nothing() {
        let store = UniqueStore::new(node(1));

        store.reserve(
            &params(
                "users",
                "email",
                b"alice@example.com",
                "user123",
                "req-abc",
                partition(42),
                5000,
            ),
            1000,
        );
        store
            .commit("users", "email", b"alice@example.com", "req-abc")
            .unwrap();

        let released = store.release("users", "email", b"alice@example.com", "req-abc");
        assert!(!released);
    }

    #[test]
    fn check_returns_false_for_reserved() {
        let store = UniqueStore::new(node(1));

        store.reserve(
            &params(
                "users",
                "email",
                b"alice@example.com",
                "user123",
                "req-abc",
                partition(42),
                5000,
            ),
            1000,
        );

        let available = store.check("users", "email", b"alice@example.com", 1000);
        assert!(!available);
    }

    #[test]
    fn check_returns_true_for_expired() {
        let store = UniqueStore::new(node(1));

        store.reserve(
            &params(
                "users",
                "email",
                b"alice@example.com",
                "user123",
                "req-abc",
                partition(42),
                5000,
            ),
            1000,
        );

        let available = store.check("users", "email", b"alice@example.com", 7000);
        assert!(available);
    }

    #[test]
    fn cleanup_expired_removes_old_reservations() {
        let store = UniqueStore::new(node(1));

        store.reserve(
            &params(
                "users",
                "email",
                b"alice@example.com",
                "user1",
                "req-1",
                partition(42),
                1000,
            ),
            1000,
        );

        store.reserve(
            &params(
                "users",
                "email",
                b"bob@example.com",
                "user2",
                "req-2",
                partition(42),
                10000,
            ),
            1000,
        );

        let removed = store.cleanup_expired(3000);
        assert_eq!(removed, 1);
        assert_eq!(store.count(), 1);
    }

    #[test]
    fn release_committed_by_record_id() {
        let store = UniqueStore::new(node(1));

        store.reserve(
            &params(
                "users",
                "email",
                b"alice@example.com",
                "user123",
                "req-abc",
                partition(42),
                5000,
            ),
            1000,
        );
        store
            .commit("users", "email", b"alice@example.com", "req-abc")
            .unwrap();

        let released = store.release("users", "email", b"alice@example.com", "req-abc");
        assert!(!released, "should not release committed by request_id");

        let released = store.release("users", "email", b"alice@example.com", "user123");
        assert!(released, "should release committed by record_id");

        let available = store.check("users", "email", b"alice@example.com", 1000);
        assert!(available, "value should be available after release");
    }

    #[test]
    fn apply_replicated_insert() {
        let store = UniqueStore::new(node(1));
        let reservation = UniqueReservation::create(
            "users",
            "email",
            b"test@example.com",
            "user456",
            "req-test",
            partition(5),
            1_702_041_600_000,
        );
        let data = UniqueStore::serialize(&reservation);

        store
            .apply_replicated(Operation::Insert, "_unique/users/email/abc", &data)
            .unwrap();

        assert_eq!(store.count(), 1);
    }

    #[test]
    fn export_import_roundtrip_preserves_partition() {
        let src = UniqueStore::new(node(1));
        let dst = UniqueStore::new(node(2));

        let mut reservations = Vec::new();
        for i in 0_u16..16 {
            let value = format!("user{i}@example.com");
            src.reserve(
                &UniqueReserveParams {
                    entity: "users",
                    field: "email",
                    value: value.as_bytes(),
                    record_id: &format!("u{i}"),
                    request_id: &format!("req-{i}"),
                    data_partition: partition(i),
                    ttl_ms: 60_000,
                },
                1_000,
            );
            reservations.push(value);
        }

        let target = src
            .get("users", "email", reservations[0].as_bytes())
            .unwrap()
            .unique_partition();
        let target_count = reservations
            .iter()
            .filter(|v| {
                src.get("users", "email", v.as_bytes())
                    .is_some_and(|r| r.unique_partition() == target)
            })
            .count();
        assert!(target_count >= 1);

        let payload = src.export_for_partition(target);
        let imported = dst.import_reservations(&payload).unwrap();
        assert_eq!(imported, target_count);

        for value in &reservations {
            let src_res = src.get("users", "email", value.as_bytes()).unwrap();
            let dst_res = dst.get("users", "email", value.as_bytes());
            if src_res.unique_partition() == target {
                let copy = dst_res.expect("imported reservation");
                assert_eq!(copy.record_id_str(), src_res.record_id_str());
            } else {
                assert!(
                    dst_res.is_none(),
                    "reservation from partition {} leaked into snapshot",
                    src_res.unique_partition().get()
                );
            }
        }
    }

    #[test]
    fn clear_partition_removes_only_target() {
        let store = UniqueStore::new(node(1));
        let mut values = Vec::new();
        for i in 0_u16..8 {
            let value = format!("v{i}");
            store.reserve(
                &UniqueReserveParams {
                    entity: "users",
                    field: "email",
                    value: value.as_bytes(),
                    record_id: &format!("u{i}"),
                    request_id: &format!("req-{i}"),
                    data_partition: partition(i),
                    ttl_ms: 60_000,
                },
                1_000,
            );
            values.push(value);
        }

        let target = store
            .get("users", "email", values[0].as_bytes())
            .unwrap()
            .unique_partition();
        let removed = store.clear_partition(target);
        assert!(removed >= 1);

        for value in &values {
            let res = store.get("users", "email", value.as_bytes());
            if let Some(r) = res {
                assert_ne!(r.unique_partition(), target);
            }
        }
    }
}
