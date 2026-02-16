// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::cluster::NodeId;

#[derive(Debug, Clone)]
pub struct ForwardTarget {
    pub client_id: String,
    pub qos: u8,
}

impl ForwardTarget {
    #[must_use]
    pub fn new(client_id: String, qos: u8) -> Self {
        Self { client_id, qos }
    }
}

#[derive(Debug, Clone)]
pub struct ForwardedPublish {
    pub origin_node: NodeId,
    pub topic: String,
    pub qos: u8,
    pub retain: bool,
    pub payload: Vec<u8>,
    pub targets: Vec<ForwardTarget>,
    pub timestamp_ms: u64,
}

impl ForwardedPublish {
    pub const VERSION: u8 = 2;

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(
        origin_node: NodeId,
        topic: String,
        qos: u8,
        retain: bool,
        payload: Vec<u8>,
        targets: Vec<ForwardTarget>,
    ) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);

        Self {
            origin_node,
            topic,
            qos,
            retain,
            payload,
            targets,
            timestamp_ms,
        }
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let topic_bytes = self.topic.as_bytes();

        let targets_size: usize = self.targets.iter().map(|t| 2 + t.client_id.len()).sum();

        let mut buf =
            Vec::with_capacity(20 + topic_bytes.len() + self.payload.len() + targets_size);

        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.origin_node.get().to_be_bytes());
        buf.extend_from_slice(&self.timestamp_ms.to_be_bytes());
        buf.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(topic_bytes);
        buf.push(self.qos);
        buf.push(u8::from(self.retain));
        buf.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf.push(self.targets.len() as u8);

        for target in &self.targets {
            let client_bytes = target.client_id.as_bytes();
            buf.push(client_bytes.len() as u8);
            buf.extend_from_slice(client_bytes);
            buf.push(target.qos);
        }

        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 20 {
            return None;
        }

        let version = bytes[0];
        if version != Self::VERSION {
            return None;
        }

        let origin_node = u16::from_be_bytes([bytes[1], bytes[2]]);
        let timestamp_ms = u64::from_be_bytes([
            bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9], bytes[10],
        ]);
        let topic_len = u16::from_be_bytes([bytes[11], bytes[12]]) as usize;

        let mut offset = 13;
        if bytes.len() < offset + topic_len + 7 {
            return None;
        }

        let topic = String::from_utf8(bytes[offset..offset + topic_len].to_vec()).ok()?;
        offset += topic_len;

        let qos = bytes[offset];
        offset += 1;

        let retain = bytes[offset] != 0;
        offset += 1;

        if bytes.len() < offset + 4 {
            return None;
        }
        let payload_len = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4;

        if bytes.len() < offset + payload_len + 1 {
            return None;
        }
        let payload = bytes[offset..offset + payload_len].to_vec();
        offset += payload_len;

        let target_count = bytes[offset] as usize;
        offset += 1;

        let mut targets = Vec::with_capacity(target_count);
        for _ in 0..target_count {
            if offset >= bytes.len() {
                return None;
            }
            let client_id_len = bytes[offset] as usize;
            offset += 1;

            if bytes.len() < offset + client_id_len + 1 {
                return None;
            }
            let client_id =
                String::from_utf8(bytes[offset..offset + client_id_len].to_vec()).ok()?;
            offset += client_id_len;

            let target_qos = bytes[offset];
            offset += 1;

            targets.push(ForwardTarget {
                client_id,
                qos: target_qos,
            });
        }

        Some(Self {
            origin_node: NodeId::validated(origin_node)?,
            topic,
            qos,
            retain,
            payload,
            targets,
            timestamp_ms,
        })
    }
}
