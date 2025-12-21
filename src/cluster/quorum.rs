use super::protocol::{AckStatus, ReplicationAck};
use super::{Epoch, NodeId};
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuorumResult {
    Pending,
    Success,
    Failed,
}

#[derive(Debug)]
pub struct QuorumTracker {
    sequence: u64,
    epoch: Epoch,
    required: usize,
    expected_nodes: HashSet<u16>,
    acked_nodes: HashSet<u16>,
    failed_nodes: HashSet<u16>,
    stale_epoch_seen: bool,
    highest_epoch_seen: Epoch,
}

impl QuorumTracker {
    #[must_use]
    pub fn new(sequence: u64, epoch: Epoch, replica_nodes: &[NodeId], required_acks: usize) -> Self {
        let expected_nodes: HashSet<u16> = replica_nodes.iter().map(|n| n.get()).collect();
        Self {
            sequence,
            epoch,
            required: required_acks,
            expected_nodes,
            acked_nodes: HashSet::new(),
            failed_nodes: HashSet::new(),
            stale_epoch_seen: false,
            highest_epoch_seen: epoch,
        }
    }

    pub fn record_ack(&mut self, ack: &ReplicationAck) -> QuorumResult {
        let node_id = ack.node_id();
        if !self.expected_nodes.contains(&node_id) {
            return self.current_result();
        }

        match ack.status() {
            Some(AckStatus::Ok) => {
                if ack.sequence() != self.sequence {
                    return self.current_result();
                }
                if ack.epoch() == self.epoch {
                    self.acked_nodes.insert(node_id);
                } else if ack.epoch() > self.epoch {
                    self.stale_epoch_seen = true;
                    if ack.epoch() > self.highest_epoch_seen {
                        self.highest_epoch_seen = ack.epoch();
                    }
                    self.failed_nodes.insert(node_id);
                }
            }
            Some(AckStatus::StaleEpoch) => {
                self.stale_epoch_seen = true;
                if ack.epoch() > self.highest_epoch_seen {
                    self.highest_epoch_seen = ack.epoch();
                }
                self.failed_nodes.insert(node_id);
            }
            Some(AckStatus::NotReplica) | Some(AckStatus::SequenceGap) => {
                self.failed_nodes.insert(node_id);
            }
            None => {}
        }

        self.current_result()
    }

    pub fn record_timeout(&mut self, node: NodeId) {
        if self.expected_nodes.contains(&node.get()) {
            self.failed_nodes.insert(node.get());
        }
    }

    #[must_use]
    pub fn current_result(&self) -> QuorumResult {
        if self.acked_nodes.len() >= self.required {
            return QuorumResult::Success;
        }

        if self.stale_epoch_seen {
            return QuorumResult::Failed;
        }

        let remaining = self.expected_nodes.len() - self.acked_nodes.len() - self.failed_nodes.len();
        let possible = self.acked_nodes.len() + remaining;

        if possible < self.required {
            return QuorumResult::Failed;
        }

        QuorumResult::Pending
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        matches!(self.current_result(), QuorumResult::Success | QuorumResult::Failed)
    }

    #[must_use]
    pub fn ack_count(&self) -> usize {
        self.acked_nodes.len()
    }

    #[must_use]
    pub fn failed_count(&self) -> usize {
        self.failed_nodes.len()
    }

    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.expected_nodes.len() - self.acked_nodes.len() - self.failed_nodes.len()
    }

    #[must_use]
    pub fn saw_stale_epoch(&self) -> bool {
        self.stale_epoch_seen
    }

    #[must_use]
    pub fn highest_epoch(&self) -> Epoch {
        self.highest_epoch_seen
    }

    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

#[derive(Debug)]
pub struct PendingWrites {
    trackers: Vec<QuorumTracker>,
    max_pending: usize,
}

impl PendingWrites {
    #[must_use]
    pub fn new(max_pending: usize) -> Self {
        Self {
            trackers: Vec::new(),
            max_pending,
        }
    }

    pub fn add(&mut self, tracker: QuorumTracker) -> bool {
        if self.trackers.len() >= self.max_pending {
            return false;
        }
        self.trackers.push(tracker);
        true
    }

    pub fn record_ack(&mut self, ack: &ReplicationAck) {
        for tracker in &mut self.trackers {
            if tracker.sequence() == ack.sequence() {
                tracker.record_ack(ack);
                break;
            }
        }
    }

    pub fn drain_completed(&mut self) -> Vec<(u64, QuorumResult)> {
        let mut completed = Vec::new();
        let mut i = 0;
        while i < self.trackers.len() {
            if self.trackers[i].is_complete() {
                let tracker = self.trackers.swap_remove(i);
                completed.push((tracker.sequence(), tracker.current_result()));
            } else {
                i += 1;
            }
        }
        completed
    }

    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.trackers.len()
    }

    #[must_use]
    pub fn is_full(&self) -> bool {
        self.trackers.len() >= self.max_pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::PartitionId;

    fn nodes(ids: &[u16]) -> Vec<NodeId> {
        ids.iter().map(|&id| NodeId::validated(id).unwrap()).collect()
    }

    fn ok_ack(partition: u16, epoch: u64, seq: u64, node: u16) -> ReplicationAck {
        ReplicationAck::ok(
            PartitionId::new(partition).unwrap(),
            Epoch::new(epoch),
            seq,
            NodeId::validated(node).unwrap(),
        )
    }

    fn stale_ack(partition: u16, epoch: u64, node: u16) -> ReplicationAck {
        ReplicationAck::stale_epoch(
            PartitionId::new(partition).unwrap(),
            Epoch::new(epoch),
            NodeId::validated(node).unwrap(),
        )
    }

    #[test]
    fn quorum_reached_with_majority() {
        let replicas = nodes(&[2, 3, 4]);
        let mut tracker = QuorumTracker::new(1, Epoch::new(1), &replicas, 2);

        assert_eq!(tracker.current_result(), QuorumResult::Pending);

        tracker.record_ack(&ok_ack(0, 1, 1, 2));
        assert_eq!(tracker.current_result(), QuorumResult::Pending);
        assert_eq!(tracker.ack_count(), 1);

        tracker.record_ack(&ok_ack(0, 1, 1, 3));
        assert_eq!(tracker.current_result(), QuorumResult::Success);
        assert_eq!(tracker.ack_count(), 2);
    }

    #[test]
    fn quorum_fails_when_impossible() {
        let replicas = nodes(&[2, 3, 4]);
        let mut tracker = QuorumTracker::new(1, Epoch::new(1), &replicas, 2);

        tracker.record_timeout(NodeId::validated(2).unwrap());
        assert_eq!(tracker.current_result(), QuorumResult::Pending);

        tracker.record_timeout(NodeId::validated(3).unwrap());
        assert_eq!(tracker.current_result(), QuorumResult::Failed);
    }

    #[test]
    fn stale_epoch_fails_immediately() {
        let replicas = nodes(&[2, 3]);
        let mut tracker = QuorumTracker::new(1, Epoch::new(1), &replicas, 1);

        tracker.record_ack(&stale_ack(0, 5, 2));
        assert_eq!(tracker.current_result(), QuorumResult::Failed);
        assert!(tracker.saw_stale_epoch());
        assert_eq!(tracker.highest_epoch(), Epoch::new(5));
    }

    #[test]
    fn ignores_unexpected_nodes() {
        let replicas = nodes(&[2, 3]);
        let mut tracker = QuorumTracker::new(1, Epoch::new(1), &replicas, 2);

        tracker.record_ack(&ok_ack(0, 1, 1, 99));
        assert_eq!(tracker.ack_count(), 0);
    }

    #[test]
    fn ignores_wrong_sequence() {
        let replicas = nodes(&[2]);
        let mut tracker = QuorumTracker::new(5, Epoch::new(1), &replicas, 1);

        tracker.record_ack(&ok_ack(0, 1, 3, 2));
        assert_eq!(tracker.ack_count(), 0);

        tracker.record_ack(&ok_ack(0, 1, 5, 2));
        assert_eq!(tracker.ack_count(), 1);
    }

    #[test]
    fn pending_writes_drain() {
        let replicas = nodes(&[2, 3]);
        let mut pending = PendingWrites::new(10);

        pending.add(QuorumTracker::new(1, Epoch::new(1), &replicas, 1));
        pending.add(QuorumTracker::new(2, Epoch::new(1), &replicas, 1));
        pending.add(QuorumTracker::new(3, Epoch::new(1), &replicas, 1));

        assert_eq!(pending.pending_count(), 3);

        pending.record_ack(&ok_ack(0, 1, 1, 2));
        pending.record_ack(&ok_ack(0, 1, 3, 3));

        let completed = pending.drain_completed();
        assert_eq!(completed.len(), 2);
        assert_eq!(pending.pending_count(), 1);
    }
}
