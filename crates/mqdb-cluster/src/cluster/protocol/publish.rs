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
    pub timestamp_us: u64,
    pub user_properties: Vec<(String, String)>,
    pub order_key: Option<String>,
    pub share_groups: Option<Vec<String>>,
}

impl ForwardedPublish {
    pub const VERSION: u8 = 2;

    #[must_use]
    pub fn new(
        origin_node: NodeId,
        topic: String,
        qos: u8,
        retain: bool,
        payload: Vec<u8>,
        targets: Vec<ForwardTarget>,
    ) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() as u64);

        Self {
            origin_node,
            topic,
            qos,
            retain,
            payload,
            targets,
            timestamp_us,
            user_properties: Vec::new(),
            order_key: None,
            share_groups: None,
        }
    }

    #[must_use]
    pub fn with_user_properties(mut self, user_properties: Vec<(String, String)>) -> Self {
        self.user_properties = user_properties;
        self
    }

    #[must_use]
    pub fn with_order_key(mut self, order_key: Option<String>) -> Self {
        self.order_key = order_key;
        self
    }

    #[must_use]
    pub fn with_share_groups(mut self, share_groups: Vec<String>) -> Self {
        self.share_groups = Some(share_groups);
        self
    }

    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let topic_bytes = self.topic.as_bytes();

        let targets_size: usize = self.targets.iter().map(|t| 2 + t.client_id.len()).sum();

        let mut buf =
            Vec::with_capacity(20 + topic_bytes.len() + self.payload.len() + targets_size);

        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.origin_node.get().to_be_bytes());
        buf.extend_from_slice(&self.timestamp_us.to_be_bytes());
        buf.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(topic_bytes);
        buf.push(self.qos);
        buf.push(u8::from(self.retain));
        buf.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.payload);
        let encodable_targets = self
            .targets
            .iter()
            .filter(|t| t.client_id.len() <= usize::from(u8::MAX))
            .take(usize::from(u8::MAX));
        buf.push(encodable_targets.clone().count() as u8);

        for target in encodable_targets {
            let client_bytes = target.client_id.as_bytes();
            buf.push(client_bytes.len() as u8);
            buf.extend_from_slice(client_bytes);
            buf.push(target.qos);
        }

        let order_key = self
            .order_key
            .as_deref()
            .filter(|key| key.len() <= usize::from(u16::MAX))
            .unwrap_or("");
        put_str16(&mut buf, order_key);

        let encodable_properties = self
            .user_properties
            .iter()
            .filter(|(key, value)| {
                key.len() <= usize::from(u16::MAX) && value.len() <= usize::from(u16::MAX)
            })
            .take(usize::from(u16::MAX));
        buf.extend_from_slice(&(encodable_properties.clone().count() as u16).to_be_bytes());
        for (key, value) in encodable_properties {
            put_str16(&mut buf, key);
            put_str16(&mut buf, value);
        }

        if let Some(groups) = &self.share_groups {
            let encodable_groups = groups
                .iter()
                .filter(|group| group.len() <= usize::from(u16::MAX))
                .take(usize::from(u16::MAX));
            let group_count = u16::try_from(encodable_groups.clone().count()).unwrap_or(u16::MAX);
            buf.extend_from_slice(&group_count.to_be_bytes());
            for group in encodable_groups {
                put_str16(&mut buf, group);
            }
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
        let timestamp_us = u64::from_be_bytes([
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

        let (order_key, user_properties, offset) = if offset == bytes.len() {
            (None, Vec::new(), offset)
        } else {
            let (order_key, mut offset) = read_str16(bytes, offset)?;
            if bytes.len() < offset + 2 {
                return None;
            }
            let property_count =
                usize::from(u16::from_be_bytes([bytes[offset], bytes[offset + 1]]));
            offset += 2;
            let mut user_properties = Vec::with_capacity(property_count);
            for _ in 0..property_count {
                let (key, next) = read_str16(bytes, offset)?;
                let (value, next) = read_str16(bytes, next)?;
                offset = next;
                user_properties.push((key, value));
            }
            (
                (!order_key.is_empty()).then_some(order_key),
                user_properties,
                offset,
            )
        };

        let share_groups = if offset == bytes.len() {
            None
        } else {
            if bytes.len() < offset + 2 {
                return None;
            }
            let group_count = usize::from(u16::from_be_bytes([bytes[offset], bytes[offset + 1]]));
            let mut offset = offset + 2;
            let mut groups = Vec::with_capacity(group_count);
            for _ in 0..group_count {
                let (group, next) = read_str16(bytes, offset)?;
                offset = next;
                groups.push(group);
            }
            Some(groups)
        };

        Some(Self {
            origin_node: NodeId::validated(origin_node)?,
            topic,
            qos,
            retain,
            payload,
            targets,
            timestamp_us,
            user_properties,
            order_key,
            share_groups,
        })
    }
}

fn put_str16(buf: &mut Vec<u8>, value: &str) {
    let len = u16::try_from(value.len()).unwrap_or(0);
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&value.as_bytes()[..usize::from(len)]);
}

fn read_str16(bytes: &[u8], offset: usize) -> Option<(String, usize)> {
    if bytes.len() < offset + 2 {
        return None;
    }
    let len = usize::from(u16::from_be_bytes([bytes[offset], bytes[offset + 1]]));
    let start = offset + 2;
    if bytes.len() < start + len {
        return None;
    }
    let value = String::from_utf8(bytes[start..start + len].to_vec()).ok()?;
    Some((value, start + len))
}
