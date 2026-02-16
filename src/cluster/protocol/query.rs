// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::types::QueryStatus;
use crate::cluster::PartitionId;

#[derive(Debug, Clone)]
pub struct QueryRequest {
    pub query_id: u64,
    pub timeout_ms: u32,
    pub entity: String,
    pub filter: Option<String>,
    pub limit: u32,
    pub cursor: Option<Vec<u8>>,
}

impl QueryRequest {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn new(
        query_id: u64,
        timeout_ms: u32,
        entity: String,
        filter: Option<String>,
        limit: u32,
        cursor: Option<Vec<u8>>,
    ) -> Self {
        Self {
            query_id,
            timeout_ms,
            entity,
            filter,
            limit,
            cursor,
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let entity_bytes = self.entity.as_bytes();
        let filter_bytes = self.filter.as_ref().map(String::as_bytes);
        let filter_len = filter_bytes.map_or(0, <[u8]>::len);
        let cursor_len = self.cursor.as_ref().map_or(0, Vec::len);

        let mut buf = Vec::with_capacity(21 + entity_bytes.len() + filter_len + cursor_len);

        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.query_id.to_be_bytes());
        buf.extend_from_slice(&self.timeout_ms.to_be_bytes());
        buf.push(entity_bytes.len() as u8);
        buf.extend_from_slice(entity_bytes);
        buf.extend_from_slice(&(filter_len as u16).to_be_bytes());
        if let Some(fb) = filter_bytes {
            buf.extend_from_slice(fb);
        }
        buf.extend_from_slice(&self.limit.to_be_bytes());
        buf.extend_from_slice(&(cursor_len as u16).to_be_bytes());
        if let Some(c) = &self.cursor {
            buf.extend_from_slice(c);
        }

        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 21 {
            return None;
        }

        let version = bytes[0];
        if version != Self::VERSION {
            return None;
        }

        let query_id = u64::from_be_bytes([
            bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
        ]);
        let timeout_ms = u32::from_be_bytes([bytes[9], bytes[10], bytes[11], bytes[12]]);
        let entity_len = bytes[13] as usize;

        let mut offset = 14;
        if bytes.len() < offset + entity_len + 8 {
            return None;
        }

        let entity = String::from_utf8(bytes[offset..offset + entity_len].to_vec()).ok()?;
        offset += entity_len;

        let filter_len = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        offset += 2;

        let filter = if filter_len > 0 {
            if bytes.len() < offset + filter_len {
                return None;
            }
            Some(String::from_utf8(bytes[offset..offset + filter_len].to_vec()).ok()?)
        } else {
            None
        };
        offset += filter_len;

        if bytes.len() < offset + 6 {
            return None;
        }

        let limit = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        let cursor_len = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        offset += 2;

        let cursor = if cursor_len > 0 {
            if bytes.len() < offset + cursor_len {
                return None;
            }
            Some(bytes[offset..offset + cursor_len].to_vec())
        } else {
            None
        };

        Some(Self {
            query_id,
            timeout_ms,
            entity,
            filter,
            limit,
            cursor,
        })
    }
}

#[derive(Debug, Clone)]
pub struct QueryResponse {
    pub query_id: u64,
    pub partition: PartitionId,
    pub status: QueryStatus,
    pub results: Vec<u8>,
    pub has_more: bool,
    pub cursor: Option<Vec<u8>>,
}

impl QueryResponse {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn ok(
        query_id: u64,
        partition: PartitionId,
        results: Vec<u8>,
        has_more: bool,
        cursor: Option<Vec<u8>>,
    ) -> Self {
        Self {
            query_id,
            partition,
            status: QueryStatus::Ok,
            results,
            has_more,
            cursor,
        }
    }

    #[must_use]
    pub fn error(query_id: u64, partition: PartitionId, status: QueryStatus) -> Self {
        Self {
            query_id,
            partition,
            status,
            results: Vec::new(),
            has_more: false,
            cursor: None,
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let cursor_len = self.cursor.as_ref().map_or(0, Vec::len);
        let mut buf = Vec::with_capacity(18 + self.results.len() + cursor_len);

        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.query_id.to_be_bytes());
        buf.extend_from_slice(&self.partition.get().to_be_bytes());
        buf.push(self.status as u8);
        buf.extend_from_slice(&(self.results.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.results);
        buf.push(u8::from(self.has_more));
        buf.extend_from_slice(&(cursor_len as u16).to_be_bytes());
        if let Some(c) = &self.cursor {
            buf.extend_from_slice(c);
        }

        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 18 {
            return None;
        }

        let version = bytes[0];
        if version != Self::VERSION {
            return None;
        }

        let query_id = u64::from_be_bytes([
            bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
        ]);
        let partition = u16::from_be_bytes([bytes[9], bytes[10]]);
        let status = QueryStatus::from_u8(bytes[11])?;
        let results_len = u32::from_be_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]) as usize;

        let mut offset = 16;
        if bytes.len() < offset + results_len + 3 {
            return None;
        }

        let results = bytes[offset..offset + results_len].to_vec();
        offset += results_len;

        let has_more = bytes[offset] != 0;
        offset += 1;

        let cursor_len = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        offset += 2;

        let cursor = if cursor_len > 0 {
            if bytes.len() < offset + cursor_len {
                return None;
            }
            Some(bytes[offset..offset + cursor_len].to_vec())
        } else {
            None
        };

        Some(Self {
            query_id,
            partition: PartitionId::new(partition)?,
            status,
            results,
            has_more,
            cursor,
        })
    }
}

#[derive(Debug, Clone)]
pub struct BatchReadRequest {
    pub request_id: u64,
    pub partition: PartitionId,
    pub entity: String,
    pub ids: Vec<String>,
}

impl BatchReadRequest {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn new(request_id: u64, partition: PartitionId, entity: String, ids: Vec<String>) -> Self {
        Self {
            request_id,
            partition,
            entity,
            ids,
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let entity_bytes = self.entity.as_bytes();
        let ids_size: usize = self.ids.iter().map(|id| 1 + id.len()).sum();

        let mut buf = Vec::with_capacity(14 + entity_bytes.len() + ids_size);

        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.request_id.to_be_bytes());
        buf.extend_from_slice(&self.partition.get().to_be_bytes());
        buf.push(entity_bytes.len() as u8);
        buf.extend_from_slice(entity_bytes);
        buf.extend_from_slice(&(self.ids.len() as u16).to_be_bytes());
        for id in &self.ids {
            let id_bytes = id.as_bytes();
            buf.push(id_bytes.len() as u8);
            buf.extend_from_slice(id_bytes);
        }

        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 14 {
            return None;
        }

        let version = bytes[0];
        if version != Self::VERSION {
            return None;
        }

        let request_id = u64::from_be_bytes([
            bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
        ]);
        let partition = u16::from_be_bytes([bytes[9], bytes[10]]);
        let entity_len = bytes[11] as usize;

        let mut offset = 12;
        if bytes.len() < offset + entity_len + 2 {
            return None;
        }

        let entity = String::from_utf8(bytes[offset..offset + entity_len].to_vec()).ok()?;
        offset += entity_len;

        let id_count = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        offset += 2;

        let mut ids = Vec::with_capacity(id_count);
        for _ in 0..id_count {
            if offset >= bytes.len() {
                return None;
            }
            let id_len = bytes[offset] as usize;
            offset += 1;

            if bytes.len() < offset + id_len {
                return None;
            }
            let id = String::from_utf8(bytes[offset..offset + id_len].to_vec()).ok()?;
            ids.push(id);
            offset += id_len;
        }

        Some(Self {
            request_id,
            partition: PartitionId::new(partition)?,
            entity,
            ids,
        })
    }
}

#[derive(Debug, Clone)]
pub struct BatchReadResponse {
    pub request_id: u64,
    pub partition: PartitionId,
    pub results: Vec<(String, Option<Vec<u8>>)>,
}

impl BatchReadResponse {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn new(
        request_id: u64,
        partition: PartitionId,
        results: Vec<(String, Option<Vec<u8>>)>,
    ) -> Self {
        Self {
            request_id,
            partition,
            results,
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let results_size: usize = self
            .results
            .iter()
            .map(|(id, data)| 1 + id.len() + 4 + data.as_ref().map_or(0, Vec::len))
            .sum();

        let mut buf = Vec::with_capacity(13 + results_size);

        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.request_id.to_be_bytes());
        buf.extend_from_slice(&self.partition.get().to_be_bytes());
        buf.extend_from_slice(&(self.results.len() as u16).to_be_bytes());

        for (id, data) in &self.results {
            let id_bytes = id.as_bytes();
            buf.push(id_bytes.len() as u8);
            buf.extend_from_slice(id_bytes);

            match data {
                Some(d) => {
                    buf.extend_from_slice(&(d.len() as u32).to_be_bytes());
                    buf.extend_from_slice(d);
                }
                None => {
                    buf.extend_from_slice(&0xFFFF_FFFFu32.to_be_bytes());
                }
            }
        }

        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 13 {
            return None;
        }

        let version = bytes[0];
        if version != Self::VERSION {
            return None;
        }

        let request_id = u64::from_be_bytes([
            bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
        ]);
        let partition = u16::from_be_bytes([bytes[9], bytes[10]]);
        let result_count = u16::from_be_bytes([bytes[11], bytes[12]]) as usize;

        let mut offset = 13;
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            if offset >= bytes.len() {
                return None;
            }
            let id_len = bytes[offset] as usize;
            offset += 1;

            if bytes.len() < offset + id_len + 4 {
                return None;
            }
            let id = String::from_utf8(bytes[offset..offset + id_len].to_vec()).ok()?;
            offset += id_len;

            let data_len = u32::from_be_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]);
            offset += 4;

            let data = if data_len == 0xFFFF_FFFF {
                None
            } else {
                let len = data_len as usize;
                if bytes.len() < offset + len {
                    return None;
                }
                let d = bytes[offset..offset + len].to_vec();
                offset += len;
                Some(d)
            };

            results.push((id, data));
        }

        Some(Self {
            request_id,
            partition: PartitionId::new(partition)?,
            results,
        })
    }
}
