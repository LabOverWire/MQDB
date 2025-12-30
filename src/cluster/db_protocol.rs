use bebytes::BeBytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DbStatus {
    Ok = 0,
    NotFound = 1,
    AlreadyExists = 2,
    InvalidPartition = 3,
    InvalidRequest = 4,
    Error = 5,
}

impl From<u8> for DbStatus {
    fn from(v: u8) -> Self {
        match v {
            0 => Self::Ok,
            1 => Self::NotFound,
            2 => Self::AlreadyExists,
            3 => Self::InvalidPartition,
            4 => Self::InvalidRequest,
            _ => Self::Error,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct DbWriteRequest {
    pub version: u8,
    pub timestamp_ms: u64,
    pub data_len: u32,
    #[FromField(data_len)]
    pub data: Vec<u8>,
}

impl DbWriteRequest {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(data: &[u8], timestamp_ms: u64) -> Self {
        Self {
            version: 1,
            timestamp_ms,
            data_len: data.len() as u32,
            data: data.to_vec(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct DbReadRequest {
    pub version: u8,
}

impl DbReadRequest {
    #[must_use]
    pub fn create() -> Self {
        Self { version: 1 }
    }
}

impl Default for DbReadRequest {
    fn default() -> Self {
        Self::create()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct DbResponse {
    pub version: u8,
    pub status: u8,
    pub data_len: u32,
    #[FromField(data_len)]
    pub data: Vec<u8>,
}

impl DbResponse {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn ok(data: &[u8]) -> Self {
        Self {
            version: 1,
            status: DbStatus::Ok as u8,
            data_len: data.len() as u32,
            data: data.to_vec(),
        }
    }

    #[must_use]
    pub fn error(status: DbStatus) -> Self {
        Self {
            version: 1,
            status: status as u8,
            data_len: 0,
            data: Vec::new(),
        }
    }

    #[must_use]
    pub fn status(&self) -> DbStatus {
        DbStatus::from(self.status)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct IndexUpdateRequest {
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
    pub data_partition: u16,
    pub record_id_len: u16,
    #[FromField(record_id_len)]
    pub record_id: Vec<u8>,
}

impl IndexUpdateRequest {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(
        entity: &str,
        field: &str,
        value: &[u8],
        data_partition: u16,
        record_id: &str,
    ) -> Self {
        let entity_bytes = entity.as_bytes().to_vec();
        let field_bytes = field.as_bytes().to_vec();
        let record_id_bytes = record_id.as_bytes().to_vec();

        Self {
            version: 1,
            entity_len: entity_bytes.len() as u16,
            entity: entity_bytes,
            field_len: field_bytes.len() as u16,
            field: field_bytes,
            value_len: value.len() as u32,
            value: value.to_vec(),
            data_partition,
            record_id_len: record_id_bytes.len() as u16,
            record_id: record_id_bytes,
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
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct UniqueReserveRequest {
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
    pub ttl_ms: u32,
    pub now_ms: u64,
}

impl UniqueReserveRequest {
    #[allow(clippy::cast_possible_truncation, clippy::too_many_arguments)]
    #[must_use]
    pub fn create(
        entity: &str,
        field: &str,
        value: &[u8],
        record_id: &str,
        request_id: &str,
        data_partition: u16,
        ttl_ms: u32,
        now_ms: u64,
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
            data_partition,
            ttl_ms,
            now_ms,
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
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct UniqueCommitRequest {
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
    pub request_id_len: u16,
    #[FromField(request_id_len)]
    pub request_id: Vec<u8>,
}

impl UniqueCommitRequest {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(entity: &str, field: &str, value: &[u8], request_id: &str) -> Self {
        let entity_bytes = entity.as_bytes().to_vec();
        let field_bytes = field.as_bytes().to_vec();
        let request_id_bytes = request_id.as_bytes().to_vec();

        Self {
            version: 1,
            entity_len: entity_bytes.len() as u16,
            entity: entity_bytes,
            field_len: field_bytes.len() as u16,
            field: field_bytes,
            value_len: value.len() as u32,
            value: value.to_vec(),
            request_id_len: request_id_bytes.len() as u16,
            request_id: request_id_bytes,
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
    pub fn request_id_str(&self) -> &str {
        std::str::from_utf8(&self.request_id).unwrap_or("")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct UniqueReleaseRequest {
    pub version: u8,
    pub request_id_len: u16,
    #[FromField(request_id_len)]
    pub request_id: Vec<u8>,
}

impl UniqueReleaseRequest {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(request_id: &str) -> Self {
        let request_id_bytes = request_id.as_bytes().to_vec();

        Self {
            version: 1,
            request_id_len: request_id_bytes.len() as u16,
            request_id: request_id_bytes,
        }
    }

    #[must_use]
    pub fn request_id_str(&self) -> &str {
        std::str::from_utf8(&self.request_id).unwrap_or("")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UniqueReserveStatus {
    Reserved = 0,
    Conflict = 1,
    AlreadyReservedBySameRequest = 2,
    Error = 3,
}

impl From<u8> for UniqueReserveStatus {
    fn from(v: u8) -> Self {
        match v {
            0 => Self::Reserved,
            1 => Self::Conflict,
            2 => Self::AlreadyReservedBySameRequest,
            _ => Self::Error,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct UniqueReserveResponse {
    pub version: u8,
    pub status: u8,
}

impl UniqueReserveResponse {
    #[must_use]
    pub fn create(status: UniqueReserveStatus) -> Self {
        Self {
            version: 1,
            status: status as u8,
        }
    }

    #[must_use]
    pub fn status(&self) -> UniqueReserveStatus {
        UniqueReserveStatus::from(self.status)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct FkValidateRequest {
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
}

impl FkValidateRequest {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(entity: &str, id: &str, request_id: &str, timeout_ms: u32) -> Self {
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FkValidateStatus {
    Valid = 0,
    Invalid = 1,
    Timeout = 2,
    Error = 3,
}

impl From<u8> for FkValidateStatus {
    fn from(v: u8) -> Self {
        match v {
            0 => Self::Valid,
            1 => Self::Invalid,
            2 => Self::Timeout,
            _ => Self::Error,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct FkValidateResponse {
    pub version: u8,
    pub status: u8,
    pub request_id_len: u16,
    #[FromField(request_id_len)]
    pub request_id: Vec<u8>,
}

impl FkValidateResponse {
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn create(status: FkValidateStatus, request_id: &str) -> Self {
        let request_id_bytes = request_id.as_bytes().to_vec();

        Self {
            version: 1,
            status: status as u8,
            request_id_len: request_id_bytes.len() as u16,
            request_id: request_id_bytes,
        }
    }

    #[must_use]
    pub fn status(&self) -> FkValidateStatus {
        FkValidateStatus::from(self.status)
    }

    #[must_use]
    pub fn request_id_str(&self) -> &str {
        std::str::from_utf8(&self.request_id).unwrap_or("")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn db_write_request_roundtrip() {
        let request = DbWriteRequest::create(b"test data", 1_234_567_890);
        let bytes = request.to_be_bytes();
        let (parsed, _) = DbWriteRequest::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(request, parsed);
    }

    #[test]
    fn db_response_ok_roundtrip() {
        let response = DbResponse::ok(b"response data");
        let bytes = response.to_be_bytes();
        let (parsed, _) = DbResponse::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(response, parsed);
        assert_eq!(parsed.status(), DbStatus::Ok);
    }

    #[test]
    fn db_response_error_roundtrip() {
        let response = DbResponse::error(DbStatus::NotFound);
        let bytes = response.to_be_bytes();
        let (parsed, _) = DbResponse::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(response, parsed);
        assert_eq!(parsed.status(), DbStatus::NotFound);
    }

    #[test]
    fn index_update_request_roundtrip() {
        let request = IndexUpdateRequest::create("users", "email", b"test@example.com", 17, "user-123");
        let bytes = request.to_be_bytes();
        let (parsed, _) = IndexUpdateRequest::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(request, parsed);
        assert_eq!(parsed.entity_str(), "users");
        assert_eq!(parsed.field_str(), "email");
        assert_eq!(parsed.record_id_str(), "user-123");
    }

    #[test]
    fn unique_reserve_request_roundtrip() {
        let request = UniqueReserveRequest::create(
            "users",
            "email",
            b"alice@example.com",
            "user-456",
            "req-789",
            10,
            5000,
            1_234_567_890,
        );
        let bytes = request.to_be_bytes();
        let (parsed, _) = UniqueReserveRequest::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(request, parsed);
    }

    #[test]
    fn fk_validate_request_roundtrip() {
        let request = FkValidateRequest::create("users", "user-123", "req-abc", 2000);
        let bytes = request.to_be_bytes();
        let (parsed, _) = FkValidateRequest::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(request, parsed);
        assert_eq!(parsed.entity_str(), "users");
        assert_eq!(parsed.id_str(), "user-123");
    }

    #[test]
    fn fk_validate_response_roundtrip() {
        let response = FkValidateResponse::create(FkValidateStatus::Valid, "req-abc");
        let bytes = response.to_be_bytes();
        let (parsed, _) = FkValidateResponse::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(response, parsed);
        assert_eq!(parsed.status(), FkValidateStatus::Valid);
    }
}
