// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::types::JsonDbOp;
use super::wire::{WireReader, push_length_prefixed};

#[derive(Debug, Clone)]
pub struct JsonDbRequest {
    pub request_id: u64,
    pub op: JsonDbOp,
    pub entity: String,
    pub id: Option<String>,
    pub payload: Vec<u8>,
    pub response_topic: String,
    pub correlation_data: Option<Vec<u8>>,
    pub sender: Option<String>,
    pub client_id: Option<String>,
}

impl JsonDbRequest {
    pub const VERSION: u8 = 2;
    pub const WIDE_VERSION: u8 = 3;

    #[must_use]
    pub fn fits_wire(&self) -> bool {
        let short_fields = [
            self.entity.len(),
            self.id.as_deref().map_or(0, str::len),
            self.response_topic.len(),
            self.correlation_data.as_deref().map_or(0, <[u8]>::len),
            self.sender.as_deref().map_or(0, str::len),
            self.client_id.as_deref().map_or(0, str::len),
        ];
        short_fields.iter().all(|len| u16::try_from(*len).is_ok())
            && u32::try_from(self.payload.len()).is_ok()
    }

    #[must_use]
    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        let entity = self.entity.as_bytes();
        let id = self.id.as_deref().map_or(&[][..], str::as_bytes);
        let response_topic = self.response_topic.as_bytes();
        let correlation = self.correlation_data.as_deref().unwrap_or_default();
        let sender = self.sender.as_deref().map_or(&[][..], str::as_bytes);
        let client_id = self.client_id.as_deref().map_or(&[][..], str::as_bytes);
        let narrow = [entity, id, sender]
            .iter()
            .all(|field| u8::try_from(field.len()).is_ok());

        let mut buf = Vec::with_capacity(
            26 + entity.len()
                + id.len()
                + self.payload.len()
                + response_topic.len()
                + correlation.len()
                + sender.len()
                + client_id.len(),
        );
        buf.push(if narrow {
            Self::VERSION
        } else {
            Self::WIDE_VERSION
        });
        buf.extend_from_slice(&self.request_id.to_be_bytes());
        buf.push(self.op as u8);
        push_length_prefixed(&mut buf, entity, narrow)?;
        push_length_prefixed(&mut buf, id, narrow)?;
        buf.extend_from_slice(&u32::try_from(self.payload.len()).ok()?.to_be_bytes());
        buf.extend_from_slice(&self.payload);
        push_length_prefixed(&mut buf, response_topic, false)?;
        push_length_prefixed(&mut buf, correlation, false)?;
        push_length_prefixed(&mut buf, sender, narrow)?;
        push_length_prefixed(&mut buf, client_id, false)?;
        Some(buf)
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let mut reader = WireReader::new(bytes);
        let version = reader.u8()?;
        if !(1..=Self::WIDE_VERSION).contains(&version) {
            return None;
        }
        let wide = version == Self::WIDE_VERSION;

        let request_id = reader.u64()?;
        let op = JsonDbOp::from_u8(reader.u8()?)?;
        let entity_len = reader.length(wide)?;
        let entity = reader.text(entity_len)?;
        let id_len = reader.length(wide)?;
        let id = Some(reader.text(id_len)?).filter(|id| !id.is_empty());
        let payload_len = usize::try_from(reader.u32()?).ok()?;
        let payload = reader.take(payload_len)?.to_vec();
        let response_topic_len = reader.length(true)?;
        let response_topic = reader.text(response_topic_len)?;
        let correlation_len = reader.length(true)?;
        let correlation_data =
            Some(reader.take(correlation_len)?.to_vec()).filter(|c| !c.is_empty());

        let sender = if version >= 2 && !reader.is_exhausted() {
            let sender_len = reader.length(wide)?;
            Some(reader.text(sender_len)?).filter(|s| !s.is_empty())
        } else {
            None
        };
        let client_id = if reader.is_exhausted() {
            None
        } else {
            let client_id_len = reader.length(true)?;
            Some(reader.text(client_id_len)?).filter(|c| !c.is_empty())
        };

        Some(Self {
            request_id,
            op,
            entity,
            id,
            payload,
            response_topic,
            correlation_data,
            sender,
            client_id,
        })
    }
}

#[derive(Debug, Clone)]
pub struct JsonDbResponse {
    pub request_id: u64,
    pub payload: Vec<u8>,
    pub response_topic: String,
    pub correlation_data: Option<Vec<u8>>,
}

impl JsonDbResponse {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn new(
        request_id: u64,
        payload: Vec<u8>,
        response_topic: String,
        correlation_data: Option<Vec<u8>>,
    ) -> Self {
        Self {
            request_id,
            payload,
            response_topic,
            correlation_data,
        }
    }

    #[must_use]
    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        let response_topic = self.response_topic.as_bytes();
        let correlation = self.correlation_data.as_deref().unwrap_or_default();

        let mut buf =
            Vec::with_capacity(17 + self.payload.len() + response_topic.len() + correlation.len());

        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.request_id.to_be_bytes());
        buf.extend_from_slice(&u32::try_from(self.payload.len()).ok()?.to_be_bytes());
        buf.extend_from_slice(&self.payload);
        push_length_prefixed(&mut buf, response_topic, false)?;
        push_length_prefixed(&mut buf, correlation, false)?;
        Some(buf)
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 15 {
            return None;
        }

        let version = bytes[0];
        if version != Self::VERSION {
            return None;
        }

        let request_id = u64::from_be_bytes([
            bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
        ]);

        let payload_len = u32::from_be_bytes([bytes[9], bytes[10], bytes[11], bytes[12]]) as usize;
        let mut offset = 13;

        if bytes.len() < offset + payload_len + 4 {
            return None;
        }

        let payload = bytes[offset..offset + payload_len].to_vec();
        offset += payload_len;

        let response_topic_len = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        offset += 2;

        if bytes.len() < offset + response_topic_len + 2 {
            return None;
        }

        let response_topic =
            String::from_utf8(bytes[offset..offset + response_topic_len].to_vec()).ok()?;
        offset += response_topic_len;

        let corr_len = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        offset += 2;

        let correlation_data = if corr_len > 0 {
            if bytes.len() < offset + corr_len {
                return None;
            }
            Some(bytes[offset..offset + corr_len].to_vec())
        } else {
            None
        };

        Some(Self {
            request_id,
            payload,
            response_topic,
            correlation_data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrip_with_sender() {
        let req = JsonDbRequest {
            request_id: 42,
            op: JsonDbOp::Update,
            entity: "users".to_string(),
            id: Some("abc".to_string()),
            payload: b"payload".to_vec(),
            response_topic: "resp/topic".to_string(),
            correlation_data: Some(b"corr".to_vec()),
            sender: Some("alice".to_string()),
            client_id: None,
        };
        let bytes = req.to_bytes().unwrap();
        let decoded = JsonDbRequest::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.request_id, 42);
        assert_eq!(decoded.entity, "users");
        assert_eq!(decoded.id.as_deref(), Some("abc"));
        assert_eq!(decoded.payload, b"payload");
        assert_eq!(decoded.response_topic, "resp/topic");
        assert_eq!(decoded.correlation_data.as_deref(), Some(b"corr".as_ref()));
        assert_eq!(decoded.sender.as_deref(), Some("alice"));
    }

    #[test]
    fn request_carries_client_id_after_sender_and_decodes_without_it() {
        let req = JsonDbRequest {
            request_id: 7,
            op: JsonDbOp::Create,
            entity: "msg".to_string(),
            id: None,
            payload: b"{}".to_vec(),
            response_topic: "resp".to_string(),
            correlation_data: None,
            sender: Some("alice".to_string()),
            client_id: Some("editor-1".to_string()),
        };
        let bytes = req.to_bytes().unwrap();
        let decoded = JsonDbRequest::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.sender.as_deref(), Some("alice"));
        assert_eq!(decoded.client_id.as_deref(), Some("editor-1"));

        let from_older_node = &bytes[..bytes.len() - 2 - "editor-1".len()];
        let decoded = JsonDbRequest::from_bytes(from_older_node).unwrap();
        assert_eq!(decoded.sender.as_deref(), Some("alice"));
        assert!(decoded.client_id.is_none());
    }

    #[test]
    fn request_with_long_sender_or_id_round_trips_every_field() {
        let request_with = |id_len: usize, sender_len: usize| JsonDbRequest {
            request_id: 9,
            op: JsonDbOp::Delete,
            entity: "doc".to_string(),
            id: Some("i".repeat(id_len)),
            payload: b"{}".to_vec(),
            response_topic: "resp".to_string(),
            correlation_data: Some(b"corr".to_vec()),
            sender: Some("s".repeat(sender_len)),
            client_id: Some("editor-1".to_string()),
        };
        for (id_len, sender_len, version) in [
            (3, 255, JsonDbRequest::VERSION),
            (3, 256, JsonDbRequest::WIDE_VERSION),
            (3, 300, JsonDbRequest::WIDE_VERSION),
            (512, 5, JsonDbRequest::WIDE_VERSION),
        ] {
            let bytes = request_with(id_len, sender_len).to_bytes().unwrap();
            assert_eq!(bytes[0], version);
            let decoded = JsonDbRequest::from_bytes(&bytes).unwrap();
            assert_eq!(decoded.id.map(|id| id.len()), Some(id_len));
            assert_eq!(decoded.sender.map(|s| s.len()), Some(sender_len));
            assert_eq!(decoded.client_id.as_deref(), Some("editor-1"));
            assert_eq!(decoded.payload, b"{}");
            assert_eq!(decoded.correlation_data.as_deref(), Some(b"corr".as_ref()));
        }
    }

    #[test]
    fn request_too_long_for_the_wire_is_not_encoded() {
        let request = JsonDbRequest {
            request_id: 1,
            op: JsonDbOp::Create,
            entity: "doc".to_string(),
            id: Some("i".repeat(usize::from(u16::MAX) + 1)),
            payload: b"{}".to_vec(),
            response_topic: "resp".to_string(),
            correlation_data: None,
            sender: None,
            client_id: None,
        };
        assert!(!request.fits_wire());
        assert!(request.to_bytes().is_none());
    }

    #[test]
    fn request_roundtrip_without_sender() {
        let req = JsonDbRequest {
            request_id: 1,
            op: JsonDbOp::Read,
            entity: "items".to_string(),
            id: Some("id1".to_string()),
            payload: vec![],
            response_topic: "resp".to_string(),
            correlation_data: None,
            sender: None,
            client_id: None,
        };
        let bytes = req.to_bytes().unwrap();
        let decoded = JsonDbRequest::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.entity, "items");
        assert!(decoded.sender.is_none());
    }
}
