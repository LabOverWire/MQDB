// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{NodeId, PartitionId};
use bebytes::BeBytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SnapshotStatus {
    Ok = 0,
    Failed = 1,
    NoData = 2,
}

impl SnapshotStatus {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Ok),
            1 => Some(Self::Failed),
            2 => Some(Self::NoData),
            _ => None,
        }
    }

    #[must_use]
    pub fn is_ok(self) -> bool {
        self == Self::Ok
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct SnapshotRequest {
    version: u8,
    partition: u16,
    requester_id: u16,
}

impl SnapshotRequest {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn create(partition: PartitionId, requester: NodeId) -> Self {
        Self {
            version: Self::VERSION,
            partition: partition.get(),
            requester_id: requester.get(),
        }
    }

    #[must_use]
    pub fn partition(&self) -> Option<PartitionId> {
        PartitionId::new(self.partition)
    }

    #[must_use]
    pub fn requester_id(&self) -> Option<NodeId> {
        NodeId::validated(self.requester_id)
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotChunk {
    pub partition: PartitionId,
    pub chunk_index: u32,
    pub total_chunks: u32,
    pub sequence_at_snapshot: u64,
    pub data: Vec<u8>,
}

impl SnapshotChunk {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn new(
        partition: PartitionId,
        chunk_index: u32,
        total_chunks: u32,
        sequence_at_snapshot: u64,
        data: Vec<u8>,
    ) -> Self {
        Self {
            partition,
            chunk_index,
            total_chunks,
            sequence_at_snapshot,
            data,
        }
    }

    #[must_use]
    pub fn is_last(&self) -> bool {
        self.chunk_index + 1 >= self.total_chunks
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(19 + self.data.len());
        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.partition.get().to_be_bytes());
        buf.extend_from_slice(&self.chunk_index.to_be_bytes());
        buf.extend_from_slice(&self.total_chunks.to_be_bytes());
        buf.extend_from_slice(&self.sequence_at_snapshot.to_be_bytes());
        buf.extend_from_slice(&(self.data.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.data);
        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 23 {
            return None;
        }

        let version = bytes[0];
        if version != Self::VERSION {
            return None;
        }

        let partition = u16::from_be_bytes([bytes[1], bytes[2]]);
        let chunk_index = u32::from_be_bytes([bytes[3], bytes[4], bytes[5], bytes[6]]);
        let total_chunks = u32::from_be_bytes([bytes[7], bytes[8], bytes[9], bytes[10]]);
        let sequence_at_snapshot = u64::from_be_bytes([
            bytes[11], bytes[12], bytes[13], bytes[14], bytes[15], bytes[16], bytes[17], bytes[18],
        ]);
        let data_len = u32::from_be_bytes([bytes[19], bytes[20], bytes[21], bytes[22]]) as usize;

        if bytes.len() < 23 + data_len {
            return None;
        }

        let data = bytes[23..23 + data_len].to_vec();

        Some(Self {
            partition: PartitionId::new(partition)?,
            chunk_index,
            total_chunks,
            sequence_at_snapshot,
            data,
        })
    }
}

#[derive(Debug, Clone, BeBytes)]
pub struct SnapshotComplete {
    version: u8,
    partition: u16,
    status: u8,
    final_sequence: u64,
}

impl SnapshotComplete {
    pub const VERSION: u8 = 1;

    #[must_use]
    pub fn ok(partition: PartitionId, final_sequence: u64) -> Self {
        Self {
            version: Self::VERSION,
            partition: partition.get(),
            status: SnapshotStatus::Ok as u8,
            final_sequence,
        }
    }

    #[must_use]
    pub fn failed(partition: PartitionId) -> Self {
        Self {
            version: Self::VERSION,
            partition: partition.get(),
            status: SnapshotStatus::Failed as u8,
            final_sequence: 0,
        }
    }

    #[must_use]
    pub fn no_data(partition: PartitionId) -> Self {
        Self {
            version: Self::VERSION,
            partition: partition.get(),
            status: SnapshotStatus::NoData as u8,
            final_sequence: 0,
        }
    }

    #[must_use]
    pub fn partition(&self) -> Option<PartitionId> {
        PartitionId::new(self.partition)
    }

    #[must_use]
    pub fn status(&self) -> Option<SnapshotStatus> {
        SnapshotStatus::from_u8(self.status)
    }

    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.status == SnapshotStatus::Ok as u8
    }

    #[must_use]
    pub fn final_sequence(&self) -> u64 {
        self.final_sequence
    }
}

#[derive(Debug)]
pub struct SnapshotBuilder {
    partition: PartitionId,
    expected_chunks: u32,
    received_chunks: Vec<Option<Vec<u8>>>,
    sequence_at_snapshot: u64,
    chunks_received: u32,
}

impl SnapshotBuilder {
    #[must_use]
    pub fn new(partition: PartitionId, total_chunks: u32, sequence_at_snapshot: u64) -> Self {
        Self {
            partition,
            expected_chunks: total_chunks,
            received_chunks: vec![None; total_chunks as usize],
            sequence_at_snapshot,
            chunks_received: 0,
        }
    }

    pub fn add_chunk(&mut self, chunk: &SnapshotChunk) -> bool {
        if chunk.partition != self.partition {
            return false;
        }
        if chunk.chunk_index >= self.expected_chunks {
            return false;
        }
        if chunk.total_chunks != self.expected_chunks {
            return false;
        }

        let idx = chunk.chunk_index as usize;
        if self.received_chunks[idx].is_none() {
            self.received_chunks[idx] = Some(chunk.data.clone());
            self.chunks_received += 1;
        }
        true
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.chunks_received == self.expected_chunks
    }

    #[must_use]
    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    #[must_use]
    pub fn sequence_at_snapshot(&self) -> u64 {
        self.sequence_at_snapshot
    }

    #[must_use]
    pub fn assemble(self) -> Option<Vec<u8>> {
        if !self.is_complete() {
            return None;
        }

        let total_len: usize = self
            .received_chunks
            .iter()
            .filter_map(|c| c.as_ref().map(Vec::len))
            .sum();

        let mut data = Vec::with_capacity(total_len);
        for chunk in self.received_chunks {
            data.extend(chunk?);
        }
        Some(data)
    }
}

#[derive(Debug)]
pub struct SnapshotSender {
    partition: PartitionId,
    data: Vec<u8>,
    chunk_size: usize,
    current_chunk: u32,
    total_chunks: u32,
    sequence_at_snapshot: u64,
}

impl SnapshotSender {
    pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(partition: PartitionId, data: Vec<u8>, sequence_at_snapshot: u64) -> Self {
        let chunk_size = Self::DEFAULT_CHUNK_SIZE;
        let total_chunks = if data.is_empty() {
            1
        } else {
            data.len().div_ceil(chunk_size) as u32
        };

        Self {
            partition,
            data,
            chunk_size,
            current_chunk: 0,
            total_chunks,
            sequence_at_snapshot,
        }
    }

    #[must_use]
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        #[allow(clippy::cast_possible_truncation)]
        {
            self.total_chunks = if self.data.is_empty() {
                1
            } else {
                self.data.len().div_ceil(chunk_size) as u32
            };
        }
        self
    }

    #[must_use]
    pub fn next_chunk(&mut self) -> Option<SnapshotChunk> {
        if self.current_chunk >= self.total_chunks {
            return None;
        }

        let start = (self.current_chunk as usize) * self.chunk_size;
        let end = (start + self.chunk_size).min(self.data.len());
        let chunk_data = if start < self.data.len() {
            self.data[start..end].to_vec()
        } else {
            Vec::new()
        };

        let chunk = SnapshotChunk::new(
            self.partition,
            self.current_chunk,
            self.total_chunks,
            self.sequence_at_snapshot,
            chunk_data,
        );

        self.current_chunk += 1;
        Some(chunk)
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.current_chunk >= self.total_chunks
    }

    #[must_use]
    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    #[must_use]
    pub fn total_chunks(&self) -> u32 {
        self.total_chunks
    }

    #[must_use]
    pub fn chunks_sent(&self) -> u32 {
        self.current_chunk
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition(id: u16) -> PartitionId {
        PartitionId::new(id).unwrap()
    }

    fn node(id: u16) -> NodeId {
        NodeId::validated(id).unwrap()
    }

    #[test]
    fn snapshot_request_roundtrip() {
        let req = SnapshotRequest::create(partition(10), node(5));
        let bytes = req.to_be_bytes();
        let (decoded, _) = SnapshotRequest::try_from_be_bytes(&bytes).unwrap();

        assert_eq!(decoded.partition(), Some(partition(10)));
        assert_eq!(decoded.requester_id(), Some(node(5)));
    }

    #[test]
    fn snapshot_chunk_roundtrip() {
        let chunk = SnapshotChunk::new(partition(7), 2, 5, 12345, b"test data".to_vec());

        let bytes = chunk.to_bytes();
        let decoded = SnapshotChunk::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.partition, partition(7));
        assert_eq!(decoded.chunk_index, 2);
        assert_eq!(decoded.total_chunks, 5);
        assert_eq!(decoded.sequence_at_snapshot, 12345);
        assert_eq!(decoded.data, b"test data");
        assert!(!decoded.is_last());
    }

    #[test]
    fn snapshot_chunk_is_last() {
        let last = SnapshotChunk::new(partition(0), 4, 5, 100, vec![]);
        assert!(last.is_last());

        let not_last = SnapshotChunk::new(partition(0), 3, 5, 100, vec![]);
        assert!(!not_last.is_last());
    }

    #[test]
    fn snapshot_complete_roundtrip() {
        let ok = SnapshotComplete::ok(partition(3), 9999);
        let bytes = ok.to_be_bytes();
        let (decoded, _) = SnapshotComplete::try_from_be_bytes(&bytes).unwrap();

        assert_eq!(decoded.partition(), Some(partition(3)));
        assert!(decoded.is_ok());
        assert_eq!(decoded.status(), Some(SnapshotStatus::Ok));
        assert_eq!(decoded.final_sequence(), 9999);

        let failed = SnapshotComplete::failed(partition(5));
        let bytes = failed.to_be_bytes();
        let (decoded, _) = SnapshotComplete::try_from_be_bytes(&bytes).unwrap();
        assert!(!decoded.is_ok());
        assert_eq!(decoded.status(), Some(SnapshotStatus::Failed));
    }

    #[test]
    fn snapshot_builder_assembly() {
        let mut builder = SnapshotBuilder::new(partition(1), 3, 500);

        let chunk0 = SnapshotChunk::new(partition(1), 0, 3, 500, b"AAA".to_vec());
        let chunk1 = SnapshotChunk::new(partition(1), 1, 3, 500, b"BBB".to_vec());
        let chunk2 = SnapshotChunk::new(partition(1), 2, 3, 500, b"CCC".to_vec());

        assert!(!builder.is_complete());
        assert!(builder.add_chunk(&chunk0));
        assert!(!builder.is_complete());
        assert!(builder.add_chunk(&chunk2));
        assert!(!builder.is_complete());
        assert!(builder.add_chunk(&chunk1));
        assert!(builder.is_complete());

        let data = builder.assemble().unwrap();
        assert_eq!(data, b"AAABBBCCC");
    }

    #[test]
    fn snapshot_builder_rejects_wrong_partition() {
        let mut builder = SnapshotBuilder::new(partition(1), 2, 100);
        let wrong_chunk = SnapshotChunk::new(partition(2), 0, 2, 100, b"data".to_vec());

        assert!(!builder.add_chunk(&wrong_chunk));
    }

    #[test]
    fn snapshot_sender_generates_chunks() {
        let data = b"Hello, World!".to_vec();
        let mut sender = SnapshotSender::new(partition(5), data.clone(), 1000).with_chunk_size(5);

        assert_eq!(sender.total_chunks(), 3);
        assert!(!sender.is_complete());

        let c0 = sender.next_chunk().unwrap();
        assert_eq!(c0.chunk_index, 0);
        assert_eq!(c0.data, b"Hello");
        assert_eq!(c0.total_chunks, 3);

        let c1 = sender.next_chunk().unwrap();
        assert_eq!(c1.chunk_index, 1);
        assert_eq!(c1.data, b", Wor");

        let c2 = sender.next_chunk().unwrap();
        assert_eq!(c2.chunk_index, 2);
        assert_eq!(c2.data, b"ld!");
        assert!(c2.is_last());

        assert!(sender.is_complete());
        assert!(sender.next_chunk().is_none());
    }

    #[test]
    fn snapshot_sender_empty_data() {
        let mut sender = SnapshotSender::new(partition(0), vec![], 0);
        assert_eq!(sender.total_chunks(), 1);

        let chunk = sender.next_chunk().unwrap();
        assert!(chunk.data.is_empty());
        assert!(chunk.is_last());
        assert!(sender.is_complete());
    }

    #[test]
    fn snapshot_status_from_u8() {
        assert_eq!(SnapshotStatus::from_u8(0), Some(SnapshotStatus::Ok));
        assert_eq!(SnapshotStatus::from_u8(1), Some(SnapshotStatus::Failed));
        assert_eq!(SnapshotStatus::from_u8(2), Some(SnapshotStatus::NoData));
        assert_eq!(SnapshotStatus::from_u8(255), None);
    }
}
