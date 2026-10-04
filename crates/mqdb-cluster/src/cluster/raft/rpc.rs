// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::state::{LogEntry, PartitionUpdate};
use crate::cluster::{Epoch, NUM_PARTITIONS, NodeId, PartitionId, PartitionMap};
use bebytes::BeBytes;
use std::time::{SystemTime, UNIX_EPOCH};

#[allow(clippy::cast_possible_truncation)]
fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, BeBytes)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: u16,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub timestamp_ms: u64,
}

impl RequestVoteRequest {
    #[must_use]
    pub fn create(term: u64, candidate_id: u16, last_log_index: u64, last_log_term: u64) -> Self {
        Self::new(
            term,
            candidate_id,
            last_log_index,
            last_log_term,
            current_timestamp_ms(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, BeBytes)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: u8,
    pub timestamp_ms: u64,
}

impl RequestVoteResponse {
    #[must_use]
    pub fn granted(term: u64) -> Self {
        Self::new(term, 1, current_timestamp_ms())
    }

    #[must_use]
    pub fn rejected(term: u64) -> Self {
        Self::new(term, 0, current_timestamp_ms())
    }

    #[must_use]
    pub fn is_granted(&self) -> bool {
        self.vote_granted != 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
pub struct AppendEntriesHeader {
    pub term: u64,
    pub leader_id: u16,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub leader_commit: u64,
    pub entry_count: u32,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: u16,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

impl AppendEntriesRequest {
    #[must_use]
    pub fn create(
        term: u64,
        leader_id: u16,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> Self {
        Self {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        }
    }

    #[must_use]
    pub fn heartbeat(
        term: u64,
        leader_id: u16,
        prev_log_index: u64,
        prev_log_term: u64,
        leader_commit: u64,
    ) -> Self {
        Self::create(
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            Vec::new(),
            leader_commit,
        )
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let header = AppendEntriesHeader::new(
            self.term,
            self.leader_id,
            self.prev_log_index,
            self.prev_log_term,
            self.leader_commit,
            self.entries.len() as u32,
            current_timestamp_ms(),
        );
        let mut buf = header.to_be_bytes();

        for entry in &self.entries {
            let entry_bytes = entry.to_be_bytes();
            buf.extend_from_slice(&(entry_bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(&entry_bytes);
        }

        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let (header, consumed) = AppendEntriesHeader::try_from_be_bytes(bytes).ok()?;

        let mut entries = Vec::with_capacity(header.entry_count as usize);
        let mut offset = consumed;

        for _ in 0..header.entry_count {
            if offset + 4 > bytes.len() {
                return None;
            }
            let entry_len = u32::from_be_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + entry_len > bytes.len() {
                return None;
            }
            let (entry, _) =
                LogEntry::try_from_be_bytes(&bytes[offset..offset + entry_len]).ok()?;
            entries.push(entry);
            offset += entry_len;
        }

        Some(Self {
            term: header.term,
            leader_id: header.leader_id,
            prev_log_index: header.prev_log_index,
            prev_log_term: header.prev_log_term,
            entries,
            leader_commit: header.leader_commit,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, BeBytes)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: u8,
    pub match_index: u64,
    pub timestamp_ms: u64,
}

impl AppendEntriesResponse {
    #[must_use]
    pub fn success(term: u64, match_index: u64) -> Self {
        Self::new(term, 1, match_index, current_timestamp_ms())
    }

    #[must_use]
    pub fn failure(term: u64) -> Self {
        Self::new(term, 0, 0, current_timestamp_ms())
    }

    #[must_use]
    pub fn is_success(&self) -> bool {
        self.success != 0
    }
}

const SNAPSHOT_PARTITIONS: usize = NUM_PARTITIONS as usize;
const PARTITION_UPDATE_LEN: usize = 11;

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
struct SnapshotHeader {
    last_index: u64,
    last_term: u64,
    member_count: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftSnapshot {
    pub last_index: u64,
    pub last_term: u64,
    pub members: Vec<NodeId>,
    pub partitions: Box<[PartitionUpdate; SNAPSHOT_PARTITIONS]>,
}

impl RaftSnapshot {
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn capture(
        last_index: u64,
        last_term: u64,
        map: &PartitionMap,
        members: &[NodeId],
    ) -> Self {
        let mut partitions = Box::new([PartitionUpdate::new(0, 0, 0, 0, 0); SNAPSHOT_PARTITIONS]);
        for (slot, partition) in partitions.iter_mut().zip(PartitionId::all()) {
            let assignment = map.get(partition);
            *slot = PartitionUpdate::new(
                partition.get() as u8,
                assignment.primary.map_or(0, NodeId::get),
                assignment.replicas.first().map_or(0, |n| n.get()),
                assignment.replicas.get(1).map_or(0, |n| n.get()),
                assignment.epoch.get() as u32,
            );
        }
        Self {
            last_index,
            last_term,
            members: members.to_vec(),
            partitions,
        }
    }

    #[must_use]
    pub fn partition_map(&self) -> PartitionMap {
        let mut map = PartitionMap::new();
        for update in self.partitions.iter() {
            let Some(partition) = PartitionId::new(u16::from(update.partition)) else {
                continue;
            };
            let Some(primary) = NodeId::validated(update.primary) else {
                continue;
            };
            let replicas = [update.replica1, update.replica2]
                .into_iter()
                .filter_map(NodeId::validated)
                .collect();
            map.set(
                partition,
                crate::cluster::PartitionAssignment::new(
                    primary,
                    replicas,
                    Epoch::new(u64::from(update.epoch)),
                ),
            );
        }
        map
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let header =
            SnapshotHeader::new(self.last_index, self.last_term, self.members.len() as u16);
        let mut buf = header.to_be_bytes();
        for member in &self.members {
            buf.extend_from_slice(&member.get().to_be_bytes());
        }
        for update in self.partitions.iter() {
            buf.extend_from_slice(&update.to_be_bytes());
        }
        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let (header, consumed) = SnapshotHeader::try_from_be_bytes(bytes).ok()?;
        let members_end = consumed + usize::from(header.member_count) * 2;
        let partitions_end = members_end + SNAPSHOT_PARTITIONS * PARTITION_UPDATE_LEN;
        if bytes.len() != partitions_end {
            return None;
        }
        let members = bytes[consumed..members_end]
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| NodeId::validated(u16::from_be_bytes(*pair)))
            .collect::<Option<Vec<_>>>()?;
        let mut partitions = Box::new([PartitionUpdate::new(0, 0, 0, 0, 0); SNAPSHOT_PARTITIONS]);
        for (slot, chunk) in partitions
            .iter_mut()
            .zip(bytes[members_end..].as_chunks::<PARTITION_UPDATE_LEN>().0)
        {
            *slot = PartitionUpdate::try_from_be_bytes(chunk).ok()?.0;
        }
        Some(Self {
            last_index: header.last_index,
            last_term: header.last_term,
            members,
            partitions,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BeBytes)]
struct InstallSnapshotHeader {
    term: u64,
    leader_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallSnapshotRequest {
    pub term: u64,
    pub leader_id: u16,
    pub snapshot: RaftSnapshot,
}

impl InstallSnapshotRequest {
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = InstallSnapshotHeader::new(self.term, self.leader_id).to_be_bytes();
        buf.extend_from_slice(&self.snapshot.to_bytes());
        buf
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let (header, consumed) = InstallSnapshotHeader::try_from_be_bytes(bytes).ok()?;
        Some(Self {
            term: header.term,
            leader_id: header.leader_id,
            snapshot: RaftSnapshot::from_bytes(&bytes[consumed..])?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::raft::RaftCommand;

    #[test]
    fn request_vote_roundtrip() {
        let req = RequestVoteRequest::create(5, 2, 10, 3);
        let bytes = req.to_be_bytes();
        let (parsed, _) = RequestVoteRequest::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(req, parsed);
    }

    #[test]
    fn request_vote_response_roundtrip() {
        let resp = RequestVoteResponse::granted(5);
        let bytes = resp.to_be_bytes();
        let (parsed, _) = RequestVoteResponse::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(resp, parsed);
        assert!(parsed.is_granted());
    }

    #[test]
    fn append_entries_empty_roundtrip() {
        let req = AppendEntriesRequest::heartbeat(5, 1, 10, 3, 8);
        let bytes = req.to_bytes();
        let parsed = AppendEntriesRequest::from_bytes(&bytes).unwrap();
        assert_eq!(req, parsed);
    }

    #[test]
    fn append_entries_with_entries_roundtrip() {
        let entries = vec![
            LogEntry::create(11, 5, RaftCommand::Noop),
            LogEntry::create(12, 5, RaftCommand::AddNode { node_id: 4 }),
        ];
        let req = AppendEntriesRequest::create(5, 1, 10, 3, entries, 8);
        let bytes = req.to_bytes();
        let parsed = AppendEntriesRequest::from_bytes(&bytes).unwrap();
        assert_eq!(req.term, parsed.term);
        assert_eq!(req.entries.len(), parsed.entries.len());
    }

    #[test]
    fn append_entries_response_roundtrip() {
        let resp = AppendEntriesResponse::success(5, 12);
        let bytes = resp.to_be_bytes();
        let (parsed, _) = AppendEntriesResponse::try_from_be_bytes(&bytes).unwrap();
        assert_eq!(resp, parsed);
        assert!(parsed.is_success());
    }

    #[test]
    fn install_snapshot_roundtrip() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();
        let mut map = PartitionMap::new();
        map.set(
            PartitionId::new(7).unwrap(),
            crate::cluster::PartitionAssignment::new(node2, vec![node1], Epoch::new(9)),
        );
        let request = InstallSnapshotRequest {
            term: 3,
            leader_id: 1,
            snapshot: RaftSnapshot::capture(1272, 3, &map, &[node1, node2]),
        };
        let bytes = request.to_bytes();
        let parsed = InstallSnapshotRequest::from_bytes(&bytes).unwrap();
        assert_eq!(parsed, request);
        assert_eq!(
            parsed
                .snapshot
                .partition_map()
                .get(PartitionId::new(7).unwrap()),
            map.get(PartitionId::new(7).unwrap())
        );
        assert!(InstallSnapshotRequest::from_bytes(&bytes[..bytes.len() - 1]).is_none());
    }
}
