use super::protocol::{AckStatus, ReplicationAck, ReplicationWrite};
use super::{Epoch, NodeId, PartitionId};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaRole {
    None,
    Primary,
    Replica,
}

#[derive(Debug)]
pub struct ReplicaState {
    partition: PartitionId,
    node_id: NodeId,
    role: ReplicaRole,
    epoch: Epoch,
    sequence: u64,
    pending_writes: BTreeMap<u64, ReplicationWrite>,
    max_pending_gap: u64,
    last_catchup_request_ms: u64,
}

pub const CATCHUP_REQUEST_INTERVAL_MS: u64 = 5000;

impl ReplicaState {
    #[must_use]
    pub fn new(partition: PartitionId, node_id: NodeId) -> Self {
        Self {
            partition,
            node_id,
            role: ReplicaRole::None,
            epoch: Epoch::ZERO,
            sequence: 0,
            pending_writes: BTreeMap::new(),
            max_pending_gap: 1000,
            last_catchup_request_ms: 0,
        }
    }

    #[must_use]
    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    #[must_use]
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub fn role(&self) -> ReplicaRole {
        self.role
    }

    #[must_use]
    pub fn next_expected_sequence(&self) -> u64 {
        self.sequence + 1
    }

    pub fn become_primary(&mut self, epoch: Epoch) {
        self.role = ReplicaRole::Primary;
        self.epoch = epoch;
        self.pending_writes.clear();
    }

    pub fn become_replica(&mut self, epoch: Epoch, sequence: u64) {
        self.role = ReplicaRole::Replica;
        self.epoch = epoch;
        self.sequence = sequence;
        self.pending_writes.clear();
    }

    pub fn step_down(&mut self) {
        self.role = ReplicaRole::None;
        self.pending_writes.clear();
    }

    pub fn handle_write(&mut self, write: &ReplicationWrite) -> ReplicationAck {
        if self.role == ReplicaRole::None {
            return ReplicationAck::not_replica(self.partition, self.node_id);
        }

        if write.epoch < self.epoch {
            return ReplicationAck::stale_epoch(self.partition, self.epoch, self.node_id);
        }

        if write.epoch > self.epoch {
            self.epoch = write.epoch;
            self.sequence = 0;
            self.pending_writes.clear();
        }

        let expected = self.next_expected_sequence();

        if write.sequence < expected {
            return ReplicationAck::ok(self.partition, self.epoch, write.sequence, self.node_id);
        }

        if write.sequence == expected {
            self.sequence = write.sequence;
            self.apply_pending();
            return ReplicationAck::ok(self.partition, self.epoch, write.sequence, self.node_id);
        }

        let gap = write.sequence - expected;
        if gap > self.max_pending_gap {
            return ReplicationAck::sequence_gap(
                self.partition,
                self.epoch,
                expected,
                self.node_id,
            );
        }

        self.pending_writes.insert(write.sequence, write.clone());
        ReplicationAck::sequence_gap(self.partition, self.epoch, expected, self.node_id)
    }

    fn apply_pending(&mut self) {
        while let Some((&seq, _)) = self.pending_writes.first_key_value() {
            if seq == self.sequence + 1 {
                self.pending_writes.pop_first();
                self.sequence = seq;
            } else {
                break;
            }
        }
    }

    pub fn advance_sequence(&mut self) -> u64 {
        self.sequence += 1;
        self.sequence
    }

    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending_writes.len()
    }

    #[must_use]
    pub fn has_gap(&self) -> bool {
        !self.pending_writes.is_empty()
    }

    #[must_use]
    pub fn gap_range(&self) -> Option<(u64, u64)> {
        if self.pending_writes.is_empty() {
            return None;
        }

        let first_pending = *self.pending_writes.first_key_value()?.0;
        Some((self.sequence + 1, first_pending - 1))
    }

    #[must_use]
    pub fn needs_catchup_request(&self, now: u64) -> bool {
        if self.role != ReplicaRole::Replica {
            return false;
        }
        if !self.has_gap() {
            return false;
        }
        now.saturating_sub(self.last_catchup_request_ms) >= CATCHUP_REQUEST_INTERVAL_MS
    }

    pub fn mark_catchup_requested(&mut self, now: u64) {
        self.last_catchup_request_ms = now;
    }
}

#[derive(Debug)]
pub struct ReplicationError {
    pub status: AckStatus,
    pub epoch: Epoch,
    pub expected_sequence: Option<u64>,
}

impl ReplicationError {
    #[must_use]
    pub fn stale_epoch(current: Epoch) -> Self {
        Self {
            status: AckStatus::StaleEpoch,
            epoch: current,
            expected_sequence: None,
        }
    }

    #[must_use]
    pub fn not_replica() -> Self {
        Self {
            status: AckStatus::NotReplica,
            epoch: Epoch::ZERO,
            expected_sequence: None,
        }
    }

    #[must_use]
    pub fn sequence_gap(epoch: Epoch, expected: u64) -> Self {
        Self {
            status: AckStatus::SequenceGap,
            epoch,
            expected_sequence: Some(expected),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::protocol::Operation;
    use super::*;

    fn partition() -> PartitionId {
        PartitionId::new(0).unwrap()
    }

    fn node() -> NodeId {
        NodeId::validated(1).unwrap()
    }

    fn write(epoch: u64, seq: u64) -> ReplicationWrite {
        ReplicationWrite::new(
            partition(),
            Operation::Insert,
            Epoch::new(epoch),
            seq,
            "test".to_string(),
            "1".to_string(),
            vec![],
        )
    }

    #[test]
    fn reject_when_not_replica() {
        let mut state = ReplicaState::new(partition(), node());
        let w = write(1, 1);
        let ack = state.handle_write(&w);
        assert_eq!(ack.status(), Some(AckStatus::NotReplica));
    }

    #[test]
    fn accept_in_order_writes() {
        let mut state = ReplicaState::new(partition(), node());
        state.become_replica(Epoch::new(1), 0);

        let ack1 = state.handle_write(&write(1, 1));
        assert!(ack1.is_ok());
        assert_eq!(state.sequence(), 1);

        let ack2 = state.handle_write(&write(1, 2));
        assert!(ack2.is_ok());
        assert_eq!(state.sequence(), 2);
    }

    #[test]
    fn reject_stale_epoch() {
        let mut state = ReplicaState::new(partition(), node());
        state.become_replica(Epoch::new(5), 10);

        let ack = state.handle_write(&write(3, 11));
        assert_eq!(ack.status(), Some(AckStatus::StaleEpoch));
        assert_eq!(ack.epoch(), Epoch::new(5));
    }

    #[test]
    fn accept_newer_epoch_resets_sequence() {
        let mut state = ReplicaState::new(partition(), node());
        state.become_replica(Epoch::new(1), 100);

        let ack = state.handle_write(&write(2, 1));
        assert!(ack.is_ok());
        assert_eq!(state.epoch(), Epoch::new(2));
        assert_eq!(state.sequence(), 1);
    }

    #[test]
    fn buffer_out_of_order_writes() {
        let mut state = ReplicaState::new(partition(), node());
        state.become_replica(Epoch::new(1), 0);

        let ack3 = state.handle_write(&write(1, 3));
        assert_eq!(ack3.status(), Some(AckStatus::SequenceGap));
        assert_eq!(state.pending_count(), 1);
        assert_eq!(state.sequence(), 0);

        let ack2 = state.handle_write(&write(1, 2));
        assert_eq!(ack2.status(), Some(AckStatus::SequenceGap));
        assert_eq!(state.pending_count(), 2);

        let ack1 = state.handle_write(&write(1, 1));
        assert!(ack1.is_ok());
        assert_eq!(state.sequence(), 3);
        assert_eq!(state.pending_count(), 0);
    }

    #[test]
    fn idempotent_duplicate_writes() {
        let mut state = ReplicaState::new(partition(), node());
        state.become_replica(Epoch::new(1), 0);

        state.handle_write(&write(1, 1));
        state.handle_write(&write(1, 2));

        let ack = state.handle_write(&write(1, 1));
        assert!(ack.is_ok());
        assert_eq!(state.sequence(), 2);
    }

    #[test]
    fn gap_range_detection() {
        let mut state = ReplicaState::new(partition(), node());
        state.become_replica(Epoch::new(1), 0);

        state.handle_write(&write(1, 1));
        assert!(state.gap_range().is_none());

        state.handle_write(&write(1, 5));
        let gap = state.gap_range().unwrap();
        assert_eq!(gap, (2, 4));
    }

    #[test]
    fn primary_can_advance_sequence() {
        let mut state = ReplicaState::new(partition(), node());
        state.become_primary(Epoch::new(1));

        assert_eq!(state.advance_sequence(), 1);
        assert_eq!(state.advance_sequence(), 2);
        assert_eq!(state.sequence(), 2);
    }

    #[test]
    fn step_down_clears_state() {
        let mut state = ReplicaState::new(partition(), node());
        state.become_replica(Epoch::new(1), 0);
        state.handle_write(&write(1, 5));

        state.step_down();

        assert_eq!(state.role(), ReplicaRole::None);
        assert_eq!(state.pending_count(), 0);
    }

    #[test]
    fn needs_catchup_only_when_replica_with_gap() {
        let mut state = ReplicaState::new(partition(), node());

        assert!(!state.needs_catchup_request(10_000));

        state.become_primary(Epoch::new(1));
        state.handle_write(&write(1, 5));
        assert!(!state.needs_catchup_request(10_000));

        state.step_down();
        state.become_replica(Epoch::new(2), 0);
        assert!(!state.needs_catchup_request(10_000));

        state.handle_write(&write(2, 5));
        assert!(state.needs_catchup_request(10_000));
    }

    #[test]
    fn catchup_request_respects_interval() {
        let mut state = ReplicaState::new(partition(), node());
        state.become_replica(Epoch::new(1), 0);
        state.handle_write(&write(1, 5));

        assert!(state.needs_catchup_request(10_000));

        state.mark_catchup_requested(10_000);
        assert!(!state.needs_catchup_request(10_000));
        assert!(!state.needs_catchup_request(14_000));

        assert!(state.needs_catchup_request(15_001));
    }
}
