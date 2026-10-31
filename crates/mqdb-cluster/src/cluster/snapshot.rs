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
    pub transfer_id: Option<u64>,
}

impl SnapshotChunk {
    pub const VERSION: u8 = 1;
    const HEADER_LEN: usize = 23;
    const TRANSFER_ID_LEN: usize = 8;

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
            transfer_id: None,
        }
    }

    #[must_use]
    pub fn with_transfer_id(mut self, transfer_id: u64) -> Self {
        self.transfer_id = Some(transfer_id);
        self
    }

    #[must_use]
    pub fn is_last(&self) -> bool {
        self.chunk_index + 1 >= self.total_chunks
    }

    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let data_len = u32::try_from(self.data.len()).unwrap_or(u32::MAX);
        let mut buf =
            Vec::with_capacity(Self::HEADER_LEN + self.data.len() + Self::TRANSFER_ID_LEN);
        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.partition.get().to_be_bytes());
        buf.extend_from_slice(&self.chunk_index.to_be_bytes());
        buf.extend_from_slice(&self.total_chunks.to_be_bytes());
        buf.extend_from_slice(&self.sequence_at_snapshot.to_be_bytes());
        buf.extend_from_slice(&data_len.to_be_bytes());
        buf.extend_from_slice(&self.data);
        if let Some(transfer_id) = self.transfer_id {
            buf.extend_from_slice(&transfer_id.to_be_bytes());
        }
        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let header = bytes.get(..Self::HEADER_LEN)?;
        if header[0] != Self::VERSION {
            return None;
        }

        let partition = u16::from_be_bytes([header[1], header[2]]);
        let chunk_index = u32::from_be_bytes([header[3], header[4], header[5], header[6]]);
        let total_chunks = u32::from_be_bytes([header[7], header[8], header[9], header[10]]);
        let sequence_at_snapshot = u64::from_be_bytes([
            header[11], header[12], header[13], header[14], header[15], header[16], header[17],
            header[18],
        ]);
        let data_len = usize::try_from(u32::from_be_bytes([
            header[19], header[20], header[21], header[22],
        ]))
        .ok()?;

        let data_end = Self::HEADER_LEN.checked_add(data_len)?;
        let data = bytes.get(Self::HEADER_LEN..data_end)?.to_vec();
        let transfer_id = bytes
            .get(data_end..data_end + Self::TRANSFER_ID_LEN)
            .and_then(|trailer| <[u8; 8]>::try_from(trailer).ok())
            .map(u64::from_be_bytes);

        Some(Self {
            partition: PartitionId::new(partition)?,
            chunk_index,
            total_chunks,
            sequence_at_snapshot,
            data,
            transfer_id,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkOutcome {
    Accepted,
    Stale,
    Gap,
}

#[derive(Debug)]
pub struct SnapshotBuilder {
    partition: PartitionId,
    transfer_id: Option<u64>,
    total_chunks: u32,
    next_index: u32,
    sequence_at_snapshot: u64,
    data: Vec<u8>,
}

impl SnapshotBuilder {
    #[must_use]
    pub fn start(chunk: &SnapshotChunk) -> Option<Self> {
        if chunk.chunk_index != 0 || chunk.total_chunks == 0 {
            return None;
        }
        Some(Self {
            partition: chunk.partition,
            transfer_id: chunk.transfer_id,
            total_chunks: chunk.total_chunks,
            next_index: 1,
            sequence_at_snapshot: chunk.sequence_at_snapshot,
            data: chunk.data.clone(),
        })
    }

    pub fn offer(&mut self, chunk: &SnapshotChunk) -> ChunkOutcome {
        if chunk.partition != self.partition || chunk.transfer_id != self.transfer_id {
            return ChunkOutcome::Stale;
        }
        if chunk.total_chunks != self.total_chunks || chunk.chunk_index > self.next_index {
            return ChunkOutcome::Gap;
        }
        if chunk.chunk_index < self.next_index {
            return ChunkOutcome::Stale;
        }
        self.data.extend_from_slice(&chunk.data);
        self.next_index += 1;
        ChunkOutcome::Accepted
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.next_index >= self.total_chunks
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
        self.is_complete().then_some(self.data)
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
    transfer_id: Option<u64>,
}

impl SnapshotSender {
    pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

    #[must_use]
    pub fn new(partition: PartitionId, data: Vec<u8>, sequence_at_snapshot: u64) -> Self {
        let chunk_size = Self::DEFAULT_CHUNK_SIZE;
        Self {
            partition,
            total_chunks: Self::chunk_count(data.len(), chunk_size),
            data,
            chunk_size,
            current_chunk: 0,
            sequence_at_snapshot,
            transfer_id: None,
        }
    }

    fn chunk_count(len: usize, chunk_size: usize) -> u32 {
        if len == 0 {
            1
        } else {
            u32::try_from(len.div_ceil(chunk_size)).unwrap_or(u32::MAX)
        }
    }

    #[must_use]
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size.max(1);
        self.total_chunks = Self::chunk_count(self.data.len(), self.chunk_size);
        self
    }

    #[must_use]
    pub fn with_transfer_id(mut self, transfer_id: u64) -> Self {
        self.transfer_id = Some(transfer_id);
        self
    }

    #[must_use]
    pub fn current_chunk(&self) -> Option<SnapshotChunk> {
        if self.current_chunk >= self.total_chunks {
            return None;
        }

        let start = (self.current_chunk as usize).saturating_mul(self.chunk_size);
        let end = start.saturating_add(self.chunk_size).min(self.data.len());
        let chunk_data = self
            .data
            .get(start..end)
            .map_or_else(Vec::new, <[u8]>::to_vec);

        let chunk = SnapshotChunk::new(
            self.partition,
            self.current_chunk,
            self.total_chunks,
            self.sequence_at_snapshot,
            chunk_data,
        );
        Some(match self.transfer_id {
            Some(transfer_id) => chunk.with_transfer_id(transfer_id),
            None => chunk,
        })
    }

    pub fn advance(&mut self) {
        if self.current_chunk < self.total_chunks {
            self.current_chunk += 1;
        }
    }

    #[must_use]
    pub fn next_chunk(&mut self) -> Option<SnapshotChunk> {
        let chunk = self.current_chunk()?;
        self.advance();
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

    fn chunk(index: u32, total: u32, transfer_id: u64, data: &[u8]) -> SnapshotChunk {
        SnapshotChunk::new(partition(1), index, total, 500, data.to_vec())
            .with_transfer_id(transfer_id)
    }

    #[test]
    fn snapshot_builder_assembly() {
        let mut builder = SnapshotBuilder::start(&chunk(0, 3, 9, b"AAA")).unwrap();

        assert!(!builder.is_complete());
        assert_eq!(
            builder.offer(&chunk(1, 3, 9, b"BBB")),
            ChunkOutcome::Accepted
        );
        assert!(!builder.is_complete());
        assert_eq!(
            builder.offer(&chunk(2, 3, 9, b"CCC")),
            ChunkOutcome::Accepted
        );
        assert!(builder.is_complete());

        let data = builder.assemble().unwrap();
        assert_eq!(data, b"AAABBBCCC");
    }

    #[test]
    fn snapshot_builder_starts_only_at_chunk_zero() {
        assert!(SnapshotBuilder::start(&chunk(1, 3, 9, b"BBB")).is_none());
        assert!(SnapshotBuilder::start(&chunk(0, 0, 9, b"")).is_none());
    }

    #[test]
    fn snapshot_builder_reports_a_skipped_chunk_as_a_gap() {
        let mut builder = SnapshotBuilder::start(&chunk(0, 3, 9, b"AAA")).unwrap();
        assert_eq!(builder.offer(&chunk(2, 3, 9, b"CCC")), ChunkOutcome::Gap);
        assert!(!builder.is_complete());
    }

    #[test]
    fn snapshot_builder_ignores_duplicates_and_other_transfers() {
        let mut builder = SnapshotBuilder::start(&chunk(0, 3, 9, b"AAA")).unwrap();
        assert_eq!(builder.offer(&chunk(0, 3, 9, b"AAA")), ChunkOutcome::Stale);
        assert_eq!(builder.offer(&chunk(1, 3, 7, b"XXX")), ChunkOutcome::Stale);
        assert_eq!(
            builder.offer(&chunk(1, 3, 9, b"BBB")),
            ChunkOutcome::Accepted
        );
        assert_eq!(
            builder.offer(&chunk(2, 3, 9, b"CCC")),
            ChunkOutcome::Accepted
        );
        assert_eq!(builder.assemble().unwrap(), b"AAABBBCCC");
    }

    #[test]
    fn snapshot_builder_reports_a_changed_total_as_a_gap() {
        let mut builder = SnapshotBuilder::start(&chunk(0, 3, 9, b"AAA")).unwrap();
        assert_eq!(builder.offer(&chunk(1, 4, 9, b"BBB")), ChunkOutcome::Gap);
    }

    #[test]
    fn snapshot_builder_rejects_wrong_partition() {
        let mut builder = SnapshotBuilder::start(&chunk(0, 2, 9, b"data")).unwrap();
        let wrong_chunk =
            SnapshotChunk::new(partition(2), 1, 2, 500, b"data".to_vec()).with_transfer_id(9);

        assert_eq!(builder.offer(&wrong_chunk), ChunkOutcome::Stale);
    }

    #[test]
    fn snapshot_chunk_carries_its_transfer_id_after_the_data() {
        let tagged =
            SnapshotChunk::new(partition(7), 1, 4, 3, b"abc".to_vec()).with_transfer_id(42);
        let bytes = tagged.to_bytes();
        let decoded = SnapshotChunk::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.transfer_id, Some(42));
        assert_eq!(decoded.data, b"abc");

        let legacy_len = bytes.len() - 8;
        let legacy = SnapshotChunk::from_bytes(&bytes[..legacy_len]).unwrap();
        assert_eq!(legacy.transfer_id, None);
        assert_eq!(legacy.data, b"abc");
        assert_eq!(legacy.chunk_index, 1);
        assert_eq!(legacy.total_chunks, 4);
    }

    #[test]
    fn snapshot_chunk_rejects_truncated_data() {
        let bytes = SnapshotChunk::new(partition(7), 0, 1, 3, b"abcdef".to_vec()).to_bytes();
        assert!(SnapshotChunk::from_bytes(&bytes[..bytes.len() - 1]).is_none());
        assert!(SnapshotChunk::from_bytes(&bytes[..10]).is_none());
    }

    #[test]
    fn snapshot_sender_repeats_the_current_chunk_until_advanced() {
        let mut sender = SnapshotSender::new(partition(5), b"Hello, World!".to_vec(), 1)
            .with_chunk_size(5)
            .with_transfer_id(77);

        let first = sender.current_chunk().unwrap();
        let again = sender.current_chunk().unwrap();
        assert_eq!(first.chunk_index, 0);
        assert_eq!(again.chunk_index, 0);
        assert_eq!(first.transfer_id, Some(77));

        sender.advance();
        assert_eq!(sender.current_chunk().unwrap().chunk_index, 1);
        sender.advance();
        sender.advance();
        assert!(sender.is_complete());
        assert!(sender.current_chunk().is_none());
        sender.advance();
        assert_eq!(sender.chunks_sent(), 3);
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
