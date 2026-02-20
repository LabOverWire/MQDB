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
    pub fn create(request_id: u64, entity: &str, id: &str) -> Self {
        assert!(
            u16::try_from(entity.len()).is_ok(),
            "entity name exceeds u16::MAX bytes"
        );
        assert!(u16::try_from(id.len()).is_ok(), "id exceeds u16::MAX bytes");
        #[allow(clippy::cast_possible_truncation)]
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
        if let Ok(s) = std::str::from_utf8(&self.entity) {
            s
        } else {
            tracing::warn!("invalid UTF-8 in FkCheckRequest entity field");
            ""
        }
    }

    #[must_use]
    pub fn id_str(&self) -> &str {
        if let Ok(s) = std::str::from_utf8(&self.id) {
            s
        } else {
            tracing::warn!("invalid UTF-8 in FkCheckRequest id field");
            ""
        }
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
    pub target_entity_len: u16,
    #[FromField(target_entity_len)]
    pub target_entity: Vec<u8>,
}

impl FkReverseLookupRequest {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn create(
        request_id: u64,
        source_entity: &str,
        source_field: &str,
        target_id: &str,
        target_entity: &str,
    ) -> Self {
        assert!(
            u16::try_from(source_entity.len()).is_ok(),
            "source_entity exceeds u16::MAX bytes"
        );
        assert!(
            u16::try_from(source_field.len()).is_ok(),
            "source_field exceeds u16::MAX bytes"
        );
        assert!(
            u16::try_from(target_id.len()).is_ok(),
            "target_id exceeds u16::MAX bytes"
        );
        assert!(
            u16::try_from(target_entity.len()).is_ok(),
            "target_entity exceeds u16::MAX bytes"
        );
        #[allow(clippy::cast_possible_truncation)]
        Self {
            version: Self::VERSION,
            request_id,
            source_entity_len: source_entity.len() as u16,
            source_entity: source_entity.as_bytes().to_vec(),
            source_field_len: source_field.len() as u16,
            source_field: source_field.as_bytes().to_vec(),
            target_id_len: target_id.len() as u16,
            target_id: target_id.as_bytes().to_vec(),
            target_entity_len: target_entity.len() as u16,
            target_entity: target_entity.as_bytes().to_vec(),
        }
    }

    #[must_use]
    pub fn source_entity_str(&self) -> &str {
        if let Ok(s) = std::str::from_utf8(&self.source_entity) {
            s
        } else {
            tracing::warn!("invalid UTF-8 in FkReverseLookupRequest source_entity field");
            ""
        }
    }

    #[must_use]
    pub fn source_field_str(&self) -> &str {
        if let Ok(s) = std::str::from_utf8(&self.source_field) {
            s
        } else {
            tracing::warn!("invalid UTF-8 in FkReverseLookupRequest source_field field");
            ""
        }
    }

    #[must_use]
    pub fn target_id_str(&self) -> &str {
        if let Ok(s) = std::str::from_utf8(&self.target_id) {
            s
        } else {
            tracing::warn!("invalid UTF-8 in FkReverseLookupRequest target_id field");
            ""
        }
    }

    #[must_use]
    pub fn target_entity_str(&self) -> &str {
        if let Ok(s) = std::str::from_utf8(&self.target_entity) {
            s
        } else {
            tracing::warn!("invalid UTF-8 in FkReverseLookupRequest target_entity field");
            ""
        }
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct FkReverseLookupResponse {
    pub version: u8,
    pub request_id: u64,
    pub ids_data_len: u32,
    #[FromField(ids_data_len)]
    pub ids_data: Vec<u8>,
}

impl FkReverseLookupResponse {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn create(request_id: u64, ids: &[String]) -> Self {
        let mut ids_data = Vec::new();
        for id in ids {
            let bytes = id.as_bytes();
            assert!(
                u16::try_from(bytes.len()).is_ok(),
                "individual id exceeds u16::MAX bytes"
            );
            #[allow(clippy::cast_possible_truncation)]
            ids_data.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
            ids_data.extend_from_slice(bytes);
        }
        assert!(
            u32::try_from(ids_data.len()).is_ok(),
            "ids_data exceeds u32::MAX bytes"
        );
        #[allow(clippy::cast_possible_truncation)]
        Self {
            version: Self::VERSION,
            request_id,
            ids_data_len: ids_data.len() as u32,
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
            } else {
                tracing::warn!(
                    byte_offset = pos,
                    len,
                    "non-UTF-8 ID in FK reverse lookup response, skipping"
                );
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
        let req = FkReverseLookupRequest::create(99, "posts", "author_id", "user-456", "users");
        let bytes = req.clone().to_be_bytes();
        let (decoded, _) = FkReverseLookupRequest::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(decoded.request_id, 99);
        assert_eq!(decoded.source_entity_str(), "posts");
        assert_eq!(decoded.source_field_str(), "author_id");
        assert_eq!(decoded.target_id_str(), "user-456");
        assert_eq!(decoded.target_entity_str(), "users");
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

    #[test]
    fn ids_data_len_is_byte_length_not_id_count() {
        let ids = vec!["ab".to_string(), "cdef".to_string()];
        let resp = FkReverseLookupResponse::create(1, &ids);
        let expected_byte_len: u32 = (2 + 2 + 2 + 4) as u32;
        assert_eq!(resp.ids_data_len, expected_byte_len);
        assert_eq!(resp.referencing_ids().len(), 2);

        let bytes = resp.to_be_bytes();
        let (decoded, _) = FkReverseLookupResponse::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(decoded.ids_data_len, expected_byte_len);
        assert_eq!(decoded.referencing_ids(), ids);
    }

    #[test]
    fn fk_check_request_version_mismatch_rejected() {
        use crate::cluster::transport::ClusterMessage;

        let mut req = FkCheckRequest::create(42, "users", "u1");
        req.version = 99;
        let bytes = req.to_be_bytes();
        let result = ClusterMessage::decode_from_wire(90, &bytes);
        assert!(result.is_none());
    }
}
