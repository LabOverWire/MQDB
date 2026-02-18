// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use bebytes::BeBytes;

#[derive(Debug, Clone, BeBytes)]
pub struct FkCheckRequest {
    pub version: u8,
    pub request_id: u64,
    pub entity_len: u16,
    #[FromField(entity_len)]
    pub entity: Vec<u8>,
    pub id_len: u16,
    #[FromField(id_len)]
    pub id: Vec<u8>,
}

impl FkCheckRequest {
    pub const VERSION: u8 = 1;

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn create(request_id: u64, entity: &str, id: &str) -> Self {
        Self {
            version: Self::VERSION,
            request_id,
            entity_len: entity.len() as u16,
            entity: entity.as_bytes().to_vec(),
            id_len: id.len() as u16,
            id: id.as_bytes().to_vec(),
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
}

#[derive(Debug, Clone, BeBytes)]
pub struct FkCheckResponse {
    pub version: u8,
    pub request_id: u64,
    pub exists: u8,
}

impl FkCheckResponse {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn create(request_id: u64, exists: bool) -> Self {
        Self {
            version: Self::VERSION,
            request_id,
            exists: u8::from(exists),
        }
    }

    #[must_use]
    pub fn exists(&self) -> bool {
        self.exists != 0
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct FkReverseLookupRequest {
    pub version: u8,
    pub request_id: u64,
    pub source_entity_len: u16,
    #[FromField(source_entity_len)]
    pub source_entity: Vec<u8>,
    pub source_field_len: u16,
    #[FromField(source_field_len)]
    pub source_field: Vec<u8>,
    pub target_id_len: u16,
    #[FromField(target_id_len)]
    pub target_id: Vec<u8>,
}

impl FkReverseLookupRequest {
    pub const VERSION: u8 = 1;

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn create(
        request_id: u64,
        source_entity: &str,
        source_field: &str,
        target_id: &str,
    ) -> Self {
        Self {
            version: Self::VERSION,
            request_id,
            source_entity_len: source_entity.len() as u16,
            source_entity: source_entity.as_bytes().to_vec(),
            source_field_len: source_field.len() as u16,
            source_field: source_field.as_bytes().to_vec(),
            target_id_len: target_id.len() as u16,
            target_id: target_id.as_bytes().to_vec(),
        }
    }

    #[must_use]
    pub fn source_entity_str(&self) -> &str {
        std::str::from_utf8(&self.source_entity).unwrap_or("")
    }

    #[must_use]
    pub fn source_field_str(&self) -> &str {
        std::str::from_utf8(&self.source_field).unwrap_or("")
    }

    #[must_use]
    pub fn target_id_str(&self) -> &str {
        std::str::from_utf8(&self.target_id).unwrap_or("")
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct FkReverseLookupResponse {
    pub version: u8,
    pub request_id: u64,
    pub count: u32,
    #[FromField(count)]
    pub ids_data: Vec<u8>,
}

impl FkReverseLookupResponse {
    pub const VERSION: u8 = 1;

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn create(request_id: u64, ids: &[String]) -> Self {
        let mut ids_data = Vec::new();
        for id in ids {
            let bytes = id.as_bytes();
            ids_data.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
            ids_data.extend_from_slice(bytes);
        }
        Self {
            version: Self::VERSION,
            request_id,
            count: ids_data.len() as u32,
            ids_data,
        }
    }

    #[must_use]
    pub fn referencing_ids(&self) -> Vec<String> {
        let mut result = Vec::new();
        let mut pos = 0;
        while pos + 2 <= self.ids_data.len() {
            let len = u16::from_be_bytes([self.ids_data[pos], self.ids_data[pos + 1]]) as usize;
            pos += 2;
            if pos + len > self.ids_data.len() {
                break;
            }
            if let Ok(s) = std::str::from_utf8(&self.ids_data[pos..pos + len]) {
                result.push(s.to_string());
            }
            pos += len;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fk_check_request_roundtrip() {
        let req = FkCheckRequest::create(42, "users", "user-123");
        let bytes = req.clone().to_be_bytes();
        let (decoded, _) = FkCheckRequest::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(decoded.request_id, 42);
        assert_eq!(decoded.entity_str(), "users");
        assert_eq!(decoded.id_str(), "user-123");
    }

    #[test]
    fn fk_check_response_roundtrip() {
        let resp = FkCheckResponse::create(42, true);
        let bytes = resp.clone().to_be_bytes();
        let (decoded, _) = FkCheckResponse::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(decoded.request_id, 42);
        assert!(decoded.exists());

        let resp = FkCheckResponse::create(43, false);
        let bytes = resp.clone().to_be_bytes();
        let (decoded, _) = FkCheckResponse::try_from_be_bytes(&bytes).unwrap();
        assert!(!decoded.exists());
    }

    #[test]
    fn fk_reverse_lookup_request_roundtrip() {
        let req = FkReverseLookupRequest::create(99, "posts", "author_id", "user-456");
        let bytes = req.clone().to_be_bytes();
        let (decoded, _) = FkReverseLookupRequest::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(decoded.request_id, 99);
        assert_eq!(decoded.source_entity_str(), "posts");
        assert_eq!(decoded.source_field_str(), "author_id");
        assert_eq!(decoded.target_id_str(), "user-456");
    }

    #[test]
    fn fk_reverse_lookup_response_roundtrip() {
        let ids = vec![
            "post-1".to_string(),
            "post-2".to_string(),
            "post-3".to_string(),
        ];
        let resp = FkReverseLookupResponse::create(99, &ids);
        let bytes = resp.clone().to_be_bytes();
        let (decoded, _) = FkReverseLookupResponse::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(decoded.request_id, 99);
        assert_eq!(decoded.referencing_ids(), ids);
    }

    #[test]
    fn fk_reverse_lookup_response_empty() {
        let resp = FkReverseLookupResponse::create(1, &[]);
        let bytes = resp.clone().to_be_bytes();
        let (decoded, _) = FkReverseLookupResponse::try_from_be_bytes(&bytes).unwrap();
        assert!(decoded.referencing_ids().is_empty());
    }
}
