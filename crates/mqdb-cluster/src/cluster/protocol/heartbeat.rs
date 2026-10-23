// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::wire::WireReader;
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
    pub const VERSION: u8 = 4;

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
    pub fn delivers_acked_events(&self) -> bool {
        self.version >= 4
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
        let voter_count = u16::try_from(self.voters.len()).unwrap_or(u16::MAX);
        let mut buf = Vec::with_capacity(Self::V2_SIZE + 18 + usize::from(voter_count) * 2);
        buf.push(self.version);
        buf.extend_from_slice(&self.node_id.to_be_bytes());
        buf.extend_from_slice(&self.timestamp_ms.to_be_bytes());
        for word in self.primary_bitmap.iter().chain(&self.replica_bitmap) {
            buf.extend_from_slice(&word.to_be_bytes());
        }
        buf.extend_from_slice(&self.voter_term.to_be_bytes());
        buf.extend_from_slice(&self.voter_seq.to_be_bytes());
        buf.extend_from_slice(&voter_count.to_be_bytes());
        for &node in self.voters.iter().take(usize::from(voter_count)) {
            buf.extend_from_slice(&node.to_be_bytes());
        }
        buf
    }

    /// # Errors
    /// Returns an error if `data` is shorter than a version-2 heartbeat.
    pub fn try_from_be_bytes(data: &[u8]) -> std::result::Result<(Self, usize), String> {
        let too_short = || format!("heartbeat too short: {} < {}", data.len(), Self::V2_SIZE);
        let mut reader = WireReader::new(data);
        let version = reader.u8().ok_or_else(too_short)?;
        let node_id = reader.u16().ok_or_else(too_short)?;
        let timestamp_ms = reader.u64().ok_or_else(too_short)?;
        let mut read_bitmap = || {
            let mut bitmap = [0u64; BITMAP_WORDS];
            for word in &mut bitmap {
                *word = reader.u64()?;
            }
            Some(bitmap)
        };
        let primary_bitmap = read_bitmap().ok_or_else(too_short)?;
        let replica_bitmap = read_bitmap().ok_or_else(too_short)?;

        let mut voter_term = 0u64;
        let mut voter_seq = 0u64;
        let mut voters = Vec::new();
        let mut consumed = Self::V2_SIZE;

        if version >= 3
            && let (Some(term), Some(seq), Some(count)) = (reader.u64(), reader.u64(), reader.u16())
        {
            voter_term = term;
            voter_seq = seq;
            if let Some(list) = (0..count).map(|_| reader.u16()).collect::<Option<Vec<_>>>() {
                voters = list;
                consumed = Self::V2_SIZE + 18 + usize::from(count) * 2;
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
