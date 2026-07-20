// Copyright 2025-2026 LabOverWire. All rights reserved.
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
    voter_term: u64,
    voter_seq: u64,
    voters: Vec<u16>,
}

impl Heartbeat {
    pub const VERSION: u8 = 3;

    const FIXED_HEADER_SIZE: usize = 1 + 2 + 8;
    const BITMAP_END: usize = Self::FIXED_HEADER_SIZE + BITMAP_WORDS * 8 * 2;
    /// Size of a version-2 heartbeat (no voter trailer) — the minimum a version-3 reader accepts.
    const V2_SIZE: usize = Self::BITMAP_END;

    #[must_use]
    pub fn create(node_id: NodeId, timestamp_ms: u64) -> Self {
        Self {
            version: Self::VERSION,
            node_id: node_id.get(),
            timestamp_ms,
            primary_bitmap: [0; BITMAP_WORDS],
            replica_bitmap: [0; BITMAP_WORDS],
            voter_term: 0,
            voter_seq: 0,
            voters: Vec::new(),
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

    /// Stamp the leader-authoritative unique-voter set onto this heartbeat. Receivers adopt it iff
    /// its `(term, seq)` is greater than the one they currently hold — only the raft leader ever
    /// raises `(term, seq)`, so the newest leader's set wins.
    pub fn set_voters(&mut self, term: u64, seq: u64, voters: &[NodeId]) {
        self.voter_term = term;
        self.voter_seq = seq;
        self.voters = voters.iter().map(|n| n.get()).collect();
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

    /// Whether this heartbeat carries unique-voter information (version 3+). A version-2 heartbeat
    /// from a not-yet-upgraded node carries none and must not be adopted as an empty voter set.
    #[must_use]
    pub fn carries_voters(&self) -> bool {
        self.version >= 3
    }

    #[must_use]
    pub fn voter_term(&self) -> u64 {
        self.voter_term
    }

    #[must_use]
    pub fn voter_seq(&self) -> u64 {
        self.voter_seq
    }

    #[must_use]
    pub fn voters(&self) -> Vec<NodeId> {
        self.voters
            .iter()
            .filter_map(|&n| NodeId::validated(n))
            .collect()
    }

    #[must_use]
    pub fn to_be_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::V2_SIZE + 18 + self.voters.len() * 2);
        buf.push(self.version);
        buf.extend_from_slice(&self.node_id.to_be_bytes());
        buf.extend_from_slice(&self.timestamp_ms.to_be_bytes());
        for word in &self.primary_bitmap {
            buf.extend_from_slice(&word.to_be_bytes());
        }
        for word in &self.replica_bitmap {
            buf.extend_from_slice(&word.to_be_bytes());
        }
        buf.extend_from_slice(&self.voter_term.to_be_bytes());
        buf.extend_from_slice(&self.voter_seq.to_be_bytes());
        #[allow(clippy::cast_possible_truncation)]
        let count = self.voters.len() as u16;
        buf.extend_from_slice(&count.to_be_bytes());
        for &node in &self.voters {
            buf.extend_from_slice(&node.to_be_bytes());
        }
        buf
    }

    #[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
    pub fn try_from_be_bytes(data: &[u8]) -> std::result::Result<(Self, usize), String> {
        if data.len() < Self::V2_SIZE {
            return Err(format!(
                "heartbeat too short: {} < {}",
                data.len(),
                Self::V2_SIZE
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

        let mut voter_term = 0u64;
        let mut voter_seq = 0u64;
        let mut voters = Vec::new();
        let mut consumed = Self::V2_SIZE;

        if version >= 3 && data.len() >= Self::V2_SIZE + 18 {
            voter_term =
                u64::from_be_bytes(data[Self::V2_SIZE..Self::V2_SIZE + 8].try_into().unwrap());
            voter_seq = u64::from_be_bytes(
                data[Self::V2_SIZE + 8..Self::V2_SIZE + 16]
                    .try_into()
                    .unwrap(),
            );
            let count = u16::from_be_bytes(
                data[Self::V2_SIZE + 16..Self::V2_SIZE + 18]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let list_start = Self::V2_SIZE + 18;
            if data.len() >= list_start + count * 2 {
                for i in 0..count {
                    let off = list_start + i * 2;
                    voters.push(u16::from_be_bytes([data[off], data[off + 1]]));
                }
                consumed = list_start + count * 2;
            }
        }

        Ok((
            Self {
                version,
                node_id,
                timestamp_ms,
                primary_bitmap,
                replica_bitmap,
                voter_term,
                voter_seq,
                voters,
            },
            consumed,
        ))
    }
}
