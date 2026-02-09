// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::cluster::{NUM_PARTITIONS, NodeId, PartitionId};

const BITMAP_WORDS: usize = (NUM_PARTITIONS as usize).div_ceil(64);

#[derive(Debug, Clone)]
pub struct Heartbeat {
    version: u8,
    node_id: u16,
    timestamp_ms: u64,
    primary_bitmap: [u64; BITMAP_WORDS],
    replica_bitmap: [u64; BITMAP_WORDS],
}

impl Heartbeat {
    pub const VERSION: u8 = 2;

    const FIXED_HEADER_SIZE: usize = 1 + 2 + 8;
    const TOTAL_SIZE: usize = Self::FIXED_HEADER_SIZE + BITMAP_WORDS * 8 * 2;

    #[must_use]
    pub fn create(node_id: NodeId, timestamp_ms: u64) -> Self {
        Self {
            version: Self::VERSION,
            node_id: node_id.get(),
            timestamp_ms,
            primary_bitmap: [0; BITMAP_WORDS],
            replica_bitmap: [0; BITMAP_WORDS],
        }
    }

    pub fn set_primary(&mut self, partition: PartitionId) {
        let idx = partition.get() as usize / 64;
        let bit = partition.get() as usize % 64;
        self.primary_bitmap[idx] |= 1u64 << bit;
    }

    pub fn set_replica(&mut self, partition: PartitionId) {
        let idx = partition.get() as usize / 64;
        let bit = partition.get() as usize % 64;
        self.replica_bitmap[idx] |= 1u64 << bit;
    }

    #[must_use]
    pub fn is_primary(&self, partition: PartitionId) -> bool {
        let idx = partition.get() as usize / 64;
        let bit = partition.get() as usize % 64;
        (self.primary_bitmap[idx] >> bit) & 1 == 1
    }

    #[must_use]
    pub fn is_replica(&self, partition: PartitionId) -> bool {
        let idx = partition.get() as usize / 64;
        let bit = partition.get() as usize % 64;
        (self.replica_bitmap[idx] >> bit) & 1 == 1
    }

    #[must_use]
    pub fn node_id(&self) -> u16 {
        self.node_id
    }

    #[must_use]
    pub fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }

    #[must_use]
    pub fn to_be_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::TOTAL_SIZE);
        buf.push(self.version);
        buf.extend_from_slice(&self.node_id.to_be_bytes());
        buf.extend_from_slice(&self.timestamp_ms.to_be_bytes());
        for word in &self.primary_bitmap {
            buf.extend_from_slice(&word.to_be_bytes());
        }
        for word in &self.replica_bitmap {
            buf.extend_from_slice(&word.to_be_bytes());
        }
        buf
    }

    #[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
    pub fn try_from_be_bytes(data: &[u8]) -> std::result::Result<(Self, usize), String> {
        if data.len() < Self::TOTAL_SIZE {
            return Err(format!(
                "heartbeat too short: {} < {}",
                data.len(),
                Self::TOTAL_SIZE
            ));
        }

        let version = data[0];
        let node_id = u16::from_be_bytes([data[1], data[2]]);
        let timestamp_ms = u64::from_be_bytes(data[3..11].try_into().unwrap());

        let mut primary_bitmap = [0u64; BITMAP_WORDS];
        let mut replica_bitmap = [0u64; BITMAP_WORDS];

        let bitmap_start = Self::FIXED_HEADER_SIZE;
        for (i, word) in primary_bitmap.iter_mut().enumerate() {
            let offset = bitmap_start + i * 8;
            *word = u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
        }

        let replica_start = bitmap_start + BITMAP_WORDS * 8;
        for (i, word) in replica_bitmap.iter_mut().enumerate() {
            let offset = replica_start + i * 8;
            *word = u64::from_be_bytes(data[offset..offset + 8].try_into().unwrap());
        }

        Ok((
            Self {
                version,
                node_id,
                timestamp_ms,
                primary_bitmap,
                replica_bitmap,
            },
            Self::TOTAL_SIZE,
        ))
    }
}
