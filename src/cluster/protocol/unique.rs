// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::cluster::PartitionId;
use bebytes::BeBytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UniqueReserveStatus {
    Reserved = 0,
    AlreadyReserved = 1,
    Conflict = 2,
    Error = 3,
}

impl UniqueReserveStatus {
    #[must_use]
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Reserved,
            1 => Self::AlreadyReserved,
            2 => Self::Conflict,
            _ => Self::Error,
        }
    }

    #[must_use]
    pub fn is_ok(self) -> bool {
        matches!(self, Self::Reserved | Self::AlreadyReserved)
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct UniqueReserveRequest {
    pub version: u8,
    pub request_id: u64,
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
    pub idempotency_key_len: u16,
    #[FromField(idempotency_key_len)]
    pub idempotency_key: Vec<u8>,
    pub data_partition: u16,
    pub ttl_ms: u64,
}

impl UniqueReserveRequest {
    pub const VERSION: u8 = 1;

    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::too_many_arguments)]
    pub fn create(
        request_id: u64,
        entity: &str,
        field: &str,
        value: &[u8],
        record_id: &str,
        idempotency_key: &str,
        data_partition: PartitionId,
        ttl_ms: u64,
    ) -> Self {
        Self {
            version: Self::VERSION,
            request_id,
            entity_len: entity.len() as u16,
            entity: entity.as_bytes().to_vec(),
            field_len: field.len() as u16,
            field: field.as_bytes().to_vec(),
            value_len: value.len() as u32,
            value: value.to_vec(),
            record_id_len: record_id.len() as u16,
            record_id: record_id.as_bytes().to_vec(),
            idempotency_key_len: idempotency_key.len() as u16,
            idempotency_key: idempotency_key.as_bytes().to_vec(),
            data_partition: data_partition.get(),
            ttl_ms,
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
    pub fn idempotency_key_str(&self) -> &str {
        std::str::from_utf8(&self.idempotency_key).unwrap_or("")
    }

    #[must_use]
    pub fn data_partition(&self) -> Option<PartitionId> {
        PartitionId::new(self.data_partition)
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct UniqueReserveResponse {
    pub version: u8,
    pub request_id: u64,
    pub result: u8,
}

impl UniqueReserveResponse {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn create(request_id: u64, result: UniqueReserveStatus) -> Self {
        Self {
            version: Self::VERSION,
            request_id,
            result: result as u8,
        }
    }

    #[must_use]
    pub fn status(&self) -> UniqueReserveStatus {
        UniqueReserveStatus::from_u8(self.result)
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct UniqueCommitRequest {
    pub version: u8,
    pub request_id: u64,
    pub entity_len: u16,
    #[FromField(entity_len)]
    pub entity: Vec<u8>,
    pub field_len: u16,
    #[FromField(field_len)]
    pub field: Vec<u8>,
    pub value_len: u32,
    #[FromField(value_len)]
    pub value: Vec<u8>,
    pub idempotency_key_len: u16,
    #[FromField(idempotency_key_len)]
    pub idempotency_key: Vec<u8>,
}

impl UniqueCommitRequest {
    pub const VERSION: u8 = 1;

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn create(
        request_id: u64,
        entity: &str,
        field: &str,
        value: &[u8],
        idempotency_key: &str,
    ) -> Self {
        Self {
            version: Self::VERSION,
            request_id,
            entity_len: entity.len() as u16,
            entity: entity.as_bytes().to_vec(),
            field_len: field.len() as u16,
            field: field.as_bytes().to_vec(),
            value_len: value.len() as u32,
            value: value.to_vec(),
            idempotency_key_len: idempotency_key.len() as u16,
            idempotency_key: idempotency_key.as_bytes().to_vec(),
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
    pub fn idempotency_key_str(&self) -> &str {
        std::str::from_utf8(&self.idempotency_key).unwrap_or("")
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct UniqueCommitResponse {
    pub version: u8,
    pub request_id: u64,
    pub success: u8,
}

impl UniqueCommitResponse {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn create(request_id: u64, success: bool) -> Self {
        Self {
            version: Self::VERSION,
            request_id,
            success: u8::from(success),
        }
    }

    #[must_use]
    pub fn is_success(&self) -> bool {
        self.success != 0
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct UniqueReleaseRequest {
    pub version: u8,
    pub request_id: u64,
    pub entity_len: u16,
    #[FromField(entity_len)]
    pub entity: Vec<u8>,
    pub field_len: u16,
    #[FromField(field_len)]
    pub field: Vec<u8>,
    pub value_len: u32,
    #[FromField(value_len)]
    pub value: Vec<u8>,
    pub idempotency_key_len: u16,
    #[FromField(idempotency_key_len)]
    pub idempotency_key: Vec<u8>,
}

impl UniqueReleaseRequest {
    pub const VERSION: u8 = 1;

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn create(
        request_id: u64,
        entity: &str,
        field: &str,
        value: &[u8],
        idempotency_key: &str,
    ) -> Self {
        Self {
            version: Self::VERSION,
            request_id,
            entity_len: entity.len() as u16,
            entity: entity.as_bytes().to_vec(),
            field_len: field.len() as u16,
            field: field.as_bytes().to_vec(),
            value_len: value.len() as u32,
            value: value.to_vec(),
            idempotency_key_len: idempotency_key.len() as u16,
            idempotency_key: idempotency_key.as_bytes().to_vec(),
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
    pub fn idempotency_key_str(&self) -> &str {
        std::str::from_utf8(&self.idempotency_key).unwrap_or("")
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct UniqueReleaseResponse {
    pub version: u8,
    pub request_id: u64,
    pub success: u8,
}

impl UniqueReleaseResponse {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn create(request_id: u64, success: bool) -> Self {
        Self {
            version: Self::VERSION,
            request_id,
            success: u8::from(success),
        }
    }

    #[must_use]
    pub fn is_success(&self) -> bool {
        self.success != 0
    }
}
