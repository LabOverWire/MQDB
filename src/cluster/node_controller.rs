use super::heartbeat::{HeartbeatManager, NodeStatus};
use super::protocol::{ReplicationAck, ReplicationWrite};
use super::quorum::{PendingWrites, QuorumResult, QuorumTracker};
use super::replication::{ReplicaRole, ReplicaState};
use super::transport::{ClusterMessage, ClusterTransport, InboundMessage, TransportConfig};
use super::{Epoch, NodeId, PartitionId, PartitionMap};
use std::collections::HashMap;

#[derive(Debug)]
pub struct NodeController<T: ClusterTransport> {
    node_id: NodeId,
    transport: T,
    heartbeat: HeartbeatManager,
    replicas: HashMap<u16, ReplicaState>,
    pending: PendingWrites,
    partition_map: PartitionMap,
    current_time: u64,
}

impl<T: ClusterTransport> NodeController<T> {
    pub fn new(node_id: NodeId, transport: T, config: TransportConfig) -> Self {
        Self {
            node_id,
            heartbeat: HeartbeatManager::new(node_id, config),
            transport,
            replicas: HashMap::new(),
            pending: PendingWrites::new(1000),
            partition_map: PartitionMap::default(),
            current_time: 0,
        }
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn register_peer(&mut self, peer: NodeId) {
        self.heartbeat.register_node(peer);
    }

    pub fn become_primary(&mut self, partition: PartitionId, epoch: Epoch) {
        let state = self
            .replicas
            .entry(partition.get())
            .or_insert_with(|| ReplicaState::new(partition, self.node_id));
        state.become_primary(epoch);
    }

    pub fn become_replica(&mut self, partition: PartitionId, epoch: Epoch, sequence: u64) {
        let state = self
            .replicas
            .entry(partition.get())
            .or_insert_with(|| ReplicaState::new(partition, self.node_id));
        state.become_replica(epoch, sequence);
    }

    pub fn step_down(&mut self, partition: PartitionId) {
        if let Some(state) = self.replicas.get_mut(&partition.get()) {
            state.step_down();
        }
    }

    #[must_use]
    pub fn role(&self, partition: PartitionId) -> ReplicaRole {
        self.replicas
            .get(&partition.get())
            .map_or(ReplicaRole::None, super::replication::ReplicaState::role)
    }

    pub fn update_partition_map(&mut self, map: PartitionMap) {
        self.partition_map = map.clone();
        self.heartbeat.update_partition_map(map);
    }

    pub fn tick(&mut self, now: u64) {
        self.current_time = now;

        if self.heartbeat.should_send(now) {
            let hb = self.heartbeat.create_heartbeat(now);
            let _ = self.transport.broadcast(hb);
        }

        let dead = self.heartbeat.check_timeouts(now);
        for node in dead {
            self.handle_node_death(node);
        }
    }

    fn handle_node_death(&mut self, dead_node: NodeId) {
        tracing::warn!(?dead_node, "node death detected");

        for partition in super::PartitionId::all() {
            let assignment = self.partition_map.get(partition);

            if assignment.primary == Some(dead_node)
                && assignment.replicas.contains(&self.node_id)
            {
                tracing::info!(
                    ?partition,
                    "primary died, this node is replica - awaiting failover"
                );
            }

            if assignment.replicas.contains(&dead_node)
                && assignment.primary == Some(self.node_id)
            {
                tracing::info!(
                    ?partition,
                    "replica died, this node is primary - replication factor reduced"
                );
            }
        }
    }

    pub fn process_messages(&mut self) {
        while let Some(msg) = self.transport.recv() {
            self.handle_message(msg);
        }
    }

    fn handle_message(&mut self, msg: InboundMessage) {
        match msg.message {
            ClusterMessage::Heartbeat(hb) => {
                self.heartbeat
                    .receive_heartbeat(msg.from, &hb, msg.received_at);
            }
            ClusterMessage::Write(ref write) => {
                self.handle_write(msg.from, write);
            }
            ClusterMessage::Ack(ack) => {
                self.handle_ack(ack);
            }
            ClusterMessage::DeathNotice { node_id } => {
                self.heartbeat.handle_death_notice(node_id);
                self.handle_node_death(node_id);
            }
            ClusterMessage::RequestVote(_)
            | ClusterMessage::RequestVoteResponse(_)
            | ClusterMessage::AppendEntries(_)
            | ClusterMessage::AppendEntriesResponse(_) => {}
        }
    }

    fn handle_write(&mut self, from: NodeId, write: &ReplicationWrite) {
        let partition = write.partition;

        let ack = if let Some(state) = self.replicas.get_mut(&partition.get()) {
            state.handle_write(write)
        } else {
            ReplicationAck::not_replica(partition, self.node_id)
        };

        let _ = self.transport.send(from, ClusterMessage::Ack(ack));
    }

    fn handle_ack(&mut self, ack: ReplicationAck) {
        self.pending.record_ack(&ack);

        for (seq, result) in self.pending.drain_completed() {
            self.on_write_complete(ack.partition(), seq, result);
        }
    }

    fn on_write_complete(&mut self, partition: PartitionId, seq: u64, result: QuorumResult) {
        match result {
            QuorumResult::Success => {
                tracing::debug!(?partition, seq, "quorum write succeeded");
            }
            QuorumResult::Failed => {
                tracing::warn!(?partition, seq, "quorum write failed");
                if let Some(state) = self.replicas.get(&partition.get()) {
                    tracing::debug!(
                        ?partition,
                        epoch = ?state.epoch(),
                        "write failed for partition"
                    );
                }
            }
            QuorumResult::Pending => {
                tracing::trace!(?partition, seq, "quorum write still pending");
            }
        }
    }

    /// # Errors
    /// Returns `NotPrimary` if this node is not the primary for the partition.
    /// Returns `TooManyPending` if the pending writes limit is reached.
    pub fn replicate_write(
        &mut self,
        write: ReplicationWrite,
        replicas: &[NodeId],
        required_acks: usize,
    ) -> Result<u64, ReplicationError> {
        let partition = write.partition;

        let state = self
            .replicas
            .get_mut(&partition.get())
            .ok_or(ReplicationError::NotPrimary)?;

        if state.role() != ReplicaRole::Primary {
            return Err(ReplicationError::NotPrimary);
        }

        let sequence = state.advance_sequence();

        let write_msg = ReplicationWrite::new(
            partition,
            write.operation,
            state.epoch(),
            sequence,
            write.entity,
            write.id,
            write.data,
        );

        let tracker = QuorumTracker::new(sequence, state.epoch(), replicas, required_acks);

        if !self.pending.add(tracker) {
            return Err(ReplicationError::TooManyPending);
        }

        for &replica in replicas {
            let _ = self
                .transport
                .send(replica, ClusterMessage::Write(write_msg.clone()));
        }

        Ok(sequence)
    }

    #[must_use]
    pub fn node_status(&self, node: NodeId) -> NodeStatus {
        self.heartbeat.node_status(node)
    }

    #[must_use]
    pub fn alive_peers(&self) -> Vec<NodeId> {
        self.heartbeat.alive_nodes()
    }

    #[must_use]
    pub fn sequence(&self, partition: PartitionId) -> Option<u64> {
        self.replicas
            .get(&partition.get())
            .map(ReplicaState::sequence)
    }

    #[must_use]
    pub fn epoch(&self, partition: PartitionId) -> Option<Epoch> {
        self.replicas.get(&partition.get()).map(ReplicaState::epoch)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationError {
    NotPrimary,
    TooManyPending,
    QuorumFailed,
    Timeout,
}

impl std::fmt::Display for ReplicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotPrimary => write!(f, "not primary for partition"),
            Self::TooManyPending => write!(f, "too many pending writes"),
            Self::QuorumFailed => write!(f, "quorum failed"),
            Self::Timeout => write!(f, "replication timeout"),
        }
    }
}

impl std::error::Error for ReplicationError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::protocol::{Heartbeat, Operation};
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    struct MockTransport {
        node_id: NodeId,
        inbox: Arc<Mutex<VecDeque<InboundMessage>>>,
        outbox: Arc<Mutex<Vec<(NodeId, ClusterMessage)>>>,
    }

    impl MockTransport {
        fn new(node_id: NodeId) -> Self {
            Self {
                node_id,
                inbox: Arc::new(Mutex::new(VecDeque::new())),
                outbox: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn inject_message(&self, from: NodeId, message: ClusterMessage) {
            self.inbox.lock().unwrap().push_back(InboundMessage {
                from,
                message,
                received_at: 0,
            });
        }

        fn sent_messages(&self) -> Vec<(NodeId, ClusterMessage)> {
            self.outbox.lock().unwrap().clone()
        }
    }

    impl ClusterTransport for MockTransport {
        fn local_node(&self) -> NodeId {
            self.node_id
        }

        fn send(
            &self,
            to: NodeId,
            message: ClusterMessage,
        ) -> Result<(), super::super::transport::TransportError> {
            self.outbox.lock().unwrap().push((to, message));
            Ok(())
        }

        fn broadcast(
            &self,
            message: ClusterMessage,
        ) -> Result<(), super::super::transport::TransportError> {
            self.outbox
                .lock()
                .unwrap()
                .push((NodeId::validated(0).unwrap_or(self.node_id), message));
            Ok(())
        }

        fn send_to_partition_primary(
            &self,
            _partition: PartitionId,
            _message: ClusterMessage,
        ) -> Result<(), super::super::transport::TransportError> {
            Ok(())
        }

        fn recv(&self) -> Option<InboundMessage> {
            self.inbox.lock().unwrap().pop_front()
        }

        fn try_recv_timeout(&self, _timeout_ms: u64) -> Option<InboundMessage> {
            self.inbox.lock().unwrap().pop_front()
        }
    }

    #[test]
    fn become_primary_and_replicate() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let partition = PartitionId::new(0).unwrap();
        ctrl.become_primary(partition, Epoch::new(1));

        assert_eq!(ctrl.role(partition), ReplicaRole::Primary);
        assert_eq!(ctrl.sequence(partition), Some(0));
    }

    #[test]
    fn replicate_write_sends_to_replicas() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();
        let node3 = NodeId::validated(3).unwrap();

        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let partition = PartitionId::new(0).unwrap();
        ctrl.become_primary(partition, Epoch::new(1));

        let write = ReplicationWrite::new(
            partition,
            Operation::Insert,
            Epoch::new(1),
            0,
            "test".to_string(),
            "1".to_string(),
            vec![1, 2, 3],
        );

        let seq = ctrl.replicate_write(write, &[node2, node3], 1).unwrap();
        assert_eq!(seq, 1);

        let sent = ctrl.transport.sent_messages();
        assert_eq!(sent.len(), 2);
    }

    #[test]
    fn receive_heartbeat_marks_alive() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        ctrl.register_peer(node2);
        assert_eq!(ctrl.node_status(node2), NodeStatus::Unknown);

        let hb = Heartbeat::create(node2, 1000);
        ctrl.transport
            .inject_message(node2, ClusterMessage::Heartbeat(hb));
        ctrl.process_messages();

        assert_eq!(ctrl.node_status(node2), NodeStatus::Alive);
    }

    #[test]
    fn handle_write_as_replica() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let partition = PartitionId::new(0).unwrap();
        ctrl.become_replica(partition, Epoch::new(1), 0);

        let write = ReplicationWrite::new(
            partition,
            Operation::Insert,
            Epoch::new(1),
            1,
            "test".to_string(),
            "1".to_string(),
            vec![],
        );

        ctrl.transport
            .inject_message(node2, ClusterMessage::Write(write));
        ctrl.process_messages();

        assert_eq!(ctrl.sequence(partition), Some(1));

        let sent = ctrl.transport.sent_messages();
        assert_eq!(sent.len(), 1);
        match &sent[0].1 {
            ClusterMessage::Ack(ack) => assert!(ack.is_ok()),
            _ => panic!("expected ack"),
        }
    }
}
