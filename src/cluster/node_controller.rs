use super::heartbeat::{HeartbeatManager, NodeStatus};
use super::protocol::{CatchupResponse, ForwardedPublish, ReplicationAck, ReplicationWrite};
use super::quorum::{PendingWrites, QuorumResult, QuorumTracker};
use super::replication::{ReplicaRole, ReplicaState};
use super::store_manager::StoreManager;
use super::transport::{ClusterMessage, ClusterTransport, InboundMessage, TransportConfig};
use super::write_log::PartitionWriteLog;
use super::{Epoch, NodeId, PartitionId, PartitionMap, session_partition};
use std::collections::{HashMap, HashSet, VecDeque};

const FORWARD_DEDUP_CAPACITY: usize = 1000;

#[derive(Debug)]
pub struct NodeController<T: ClusterTransport> {
    node_id: NodeId,
    transport: T,
    heartbeat: HeartbeatManager,
    replicas: HashMap<u16, ReplicaState>,
    pending: PendingWrites,
    partition_map: PartitionMap,
    current_time: u64,
    stores: StoreManager,
    write_log: PartitionWriteLog,
    forward_dedup: HashSet<u64>,
    forward_dedup_order: VecDeque<u64>,
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
            stores: StoreManager::new(node_id),
            write_log: PartitionWriteLog::new(),
            forward_dedup: HashSet::with_capacity(FORWARD_DEDUP_CAPACITY),
            forward_dedup_order: VecDeque::with_capacity(FORWARD_DEDUP_CAPACITY),
        }
    }

    #[must_use]
    pub fn stores(&self) -> &StoreManager {
        &self.stores
    }

    #[must_use]
    pub fn write_log(&self) -> &PartitionWriteLog {
        &self.write_log
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn transport(&self) -> &T {
        &self.transport
    }

    #[must_use]
    pub fn is_local_partition(&self, partition: PartitionId) -> bool {
        self.replicas
            .get(&partition.get())
            .is_some_and(|s| s.role() == ReplicaRole::Primary)
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

            if assignment.primary == Some(dead_node) && assignment.replicas.contains(&self.node_id)
            {
                tracing::info!(
                    ?partition,
                    "primary died, this node is replica - awaiting failover"
                );
            }

            if assignment.replicas.contains(&dead_node) && assignment.primary == Some(self.node_id)
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
            ClusterMessage::WriteRequest(write) => {
                self.handle_write_request(msg.from, write);
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
            ClusterMessage::CatchupRequest(req) => {
                self.handle_catchup_request(
                    msg.from,
                    req.partition(),
                    req.from_sequence(),
                    req.to_sequence(),
                );
            }
            ClusterMessage::CatchupResponse(resp) => {
                self.handle_catchup_response(resp);
            }
            ClusterMessage::ForwardedPublish(ref fwd) => {
                self.handle_forwarded_publish(msg.from, fwd);
            }
        }
    }

    fn handle_write(&mut self, from: NodeId, write: &ReplicationWrite) {
        let partition = write.partition;

        tracing::debug!(
            ?partition,
            from = from.get(),
            sequence = write.sequence,
            entity = ?write.entity,
            "received replication write"
        );

        let ack = if let Some(state) = self.replicas.get_mut(&partition.get()) {
            let ack = state.handle_write(write);
            tracing::debug!(
                ?partition,
                sequence = write.sequence,
                ack_ok = ack.is_ok(),
                ack_status = ?ack.status(),
                "handle_write result"
            );
            if ack.is_ok() {
                if let Err(e) = self.stores.apply_write(write) {
                    tracing::error!(?partition, ?e, "failed to apply write to stores");
                } else {
                    tracing::debug!(
                        ?partition,
                        sequence = write.sequence,
                        entity = ?write.entity,
                        id = ?write.id,
                        "applied write to stores"
                    );
                }
                self.write_log
                    .append(partition, write.sequence, write.clone());
            }
            ack
        } else {
            tracing::warn!(
                ?partition,
                from = from.get(),
                "not a replica for partition, rejecting write"
            );
            ReplicationAck::not_replica(partition, self.node_id)
        };

        let _ = self.transport.send(from, ClusterMessage::Ack(ack));
    }

    fn handle_write_request(&mut self, from: NodeId, write: ReplicationWrite) {
        let partition = write.partition;

        tracing::debug!(
            ?partition,
            from = from.get(),
            entity = ?write.entity,
            "received write request"
        );

        if !self.is_local_partition(partition) {
            tracing::warn!(
                ?partition,
                from = from.get(),
                "received write request but not primary"
            );
            return;
        }

        if let Err(e) = self.stores.apply_write(&write) {
            tracing::error!(?partition, ?e, "failed to apply write request to stores");
            return;
        }

        tracing::debug!(
            ?partition,
            entity = ?write.entity,
            id = ?write.id,
            "applied write request to stores"
        );

        let replicas: Vec<NodeId> = self.partition_map.replicas(partition).to_vec();
        if let Err(e) = self.replicate_write_async(write, &replicas) {
            tracing::warn!(?partition, ?e, "failed to replicate write request");
        }
    }

    fn handle_ack(&mut self, ack: ReplicationAck) {
        self.pending.record_ack(&ack);

        if ack.status() == Some(super::protocol::AckStatus::SequenceGap) {
            self.handle_sequence_gap_ack(&ack);
        }

        for (seq, result) in self.pending.drain_completed() {
            self.on_write_complete(ack.partition(), seq, result);
        }
    }

    fn handle_sequence_gap_ack(&self, ack: &ReplicationAck) {
        let partition = ack.partition();
        let from_seq = ack.sequence();
        let replica_id = ack.node_id();

        let Some(state) = self.replicas.get(&partition.get()) else {
            return;
        };

        if state.role() != ReplicaRole::Primary {
            return;
        }

        let current_seq = state.sequence();
        if from_seq >= current_seq {
            return;
        }

        tracing::debug!(
            ?partition,
            from_seq,
            current_seq,
            ?replica_id,
            "replica has sequence gap, sending catchup"
        );

        let writes = self.write_log.get_range(partition, from_seq, current_seq);
        if writes.is_empty() {
            tracing::warn!(
                ?partition,
                from_seq,
                current_seq,
                "cannot serve catchup - data evicted from log"
            );
            return;
        }

        let Some(replica_node) = NodeId::validated(replica_id) else {
            return;
        };

        let resp = CatchupResponse::create(partition, self.node_id, writes);
        let _ = self
            .transport
            .send(replica_node, ClusterMessage::CatchupResponse(resp));
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

    pub fn stores_mut(&mut self) -> &mut StoreManager {
        &mut self.stores
    }

    #[must_use]
    pub fn partition_map(&self) -> &PartitionMap {
        &self.partition_map
    }

    /// Creates a session and replicates it to replicas (fire-and-forget).
    ///
    /// This method sends replication writes but does NOT wait for quorum.
    /// Use `create_session_quorum` for synchronous quorum waiting.
    ///
    /// # Errors
    /// Returns `NotPrimary` if this node is not the primary for the partition.
    /// Returns `TooManyPending` if the pending writes limit is reached.
    pub fn create_session_replicated(
        &mut self,
        client_id: &str,
    ) -> Result<super::SessionData, ReplicationError> {
        let partition = session_partition(client_id);

        let state = self
            .replicas
            .get(&partition.get())
            .ok_or(ReplicationError::NotPrimary)?;

        if state.role() != ReplicaRole::Primary {
            return Err(ReplicationError::NotPrimary);
        }

        let (session, write) = self
            .stores
            .create_session_replicated(client_id)
            .map_err(|_| ReplicationError::QuorumFailed)?;

        let replicas: Vec<NodeId> = self.partition_map.replicas(partition).to_vec();
        let required_acks = replicas.len();

        if !replicas.is_empty() {
            self.replicate_write(write, &replicas, required_acks)?;
        }

        Ok(session)
    }

    /// Creates a session and waits for quorum replication before returning.
    ///
    /// Returns the session and a receiver that resolves when quorum is reached.
    /// The caller must ensure `process_messages()` is being called (e.g., in a
    /// separate task) for the quorum to complete.
    ///
    /// # Errors
    /// Returns `NotPrimary` if this node is not the primary for the partition.
    /// Returns `TooManyPending` if the pending writes limit is reached.
    #[cfg(feature = "native")]
    pub fn create_session_quorum(
        &mut self,
        client_id: &str,
    ) -> Result<
        (
            super::SessionData,
            tokio::sync::oneshot::Receiver<QuorumResult>,
        ),
        ReplicationError,
    > {
        let partition = session_partition(client_id);

        let state = self
            .replicas
            .get_mut(&partition.get())
            .ok_or(ReplicationError::NotPrimary)?;

        if state.role() != ReplicaRole::Primary {
            return Err(ReplicationError::NotPrimary);
        }

        let sequence = state.advance_sequence();
        let epoch = state.epoch();

        let (session, write_data) = self
            .stores
            .create_session_replicated(client_id)
            .map_err(|_| ReplicationError::QuorumFailed)?;

        let replicas: Vec<NodeId> = self.partition_map.replicas(partition).to_vec();
        let required_acks = replicas.len();

        if replicas.is_empty() {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = tx.send(QuorumResult::Success);
            return Ok((session, rx));
        }

        let write_msg = ReplicationWrite::new(
            partition,
            write_data.operation,
            epoch,
            sequence,
            write_data.entity,
            write_data.id,
            write_data.data,
        );

        let (tracker, rx) =
            QuorumTracker::with_completion(sequence, epoch, &replicas, required_acks);

        if !self.pending.add(tracker) {
            return Err(ReplicationError::TooManyPending);
        }

        self.write_log
            .append(partition, sequence, write_msg.clone());

        for &replica in &replicas {
            let _ = self
                .transport
                .send(replica, ClusterMessage::Write(write_msg.clone()));
        }

        Ok((session, rx))
    }

    /// # Errors
    /// Returns `NotPrimary` if this node is not the primary for the partition.
    pub fn replicate_write_async(
        &mut self,
        write: ReplicationWrite,
        replicas: &[NodeId],
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

        for &replica in replicas {
            let _ = self
                .transport
                .send(replica, ClusterMessage::Write(write_msg.clone()));
        }

        Ok(sequence)
    }

    pub fn write_or_forward(&mut self, write: ReplicationWrite) {
        let partition = write.partition;

        if self.is_local_partition(partition) {
            tracing::debug!(
                ?partition,
                entity = ?write.entity,
                "write_or_forward: local partition, replicating"
            );
            let replicas: Vec<NodeId> = self.partition_map.replicas(partition).to_vec();
            let _ = self.replicate_write_async(write, &replicas);
        } else if let Some(primary) = self.partition_map.primary(partition) {
            tracing::debug!(
                ?partition,
                primary = primary.get(),
                entity = ?write.entity,
                "write_or_forward: forwarding to primary"
            );
            let _ = self
                .transport
                .send(primary, ClusterMessage::WriteRequest(write));
        } else {
            tracing::warn!(
                ?partition,
                entity = ?write.entity,
                "write_or_forward: no primary found for partition"
            );
        }
    }

    /// # Errors
    /// Returns `WildcardStoreError` if the wildcard subscription fails.
    pub fn subscribe_wildcard_broadcast(
        &mut self,
        pattern: &str,
        client_id: &str,
        client_partition: PartitionId,
        qos: u8,
    ) -> Result<Vec<ReplicationWrite>, super::WildcardStoreError> {
        let (entry, data) = self.stores.wildcards.subscribe_with_data(
            pattern,
            client_id,
            client_partition,
            qos,
            super::SubscriptionType::Mqtt,
        )?;

        let writes: Vec<ReplicationWrite> = PartitionId::all()
            .map(|p| {
                ReplicationWrite::new(
                    p,
                    super::protocol::Operation::Insert,
                    Epoch::ZERO,
                    0,
                    super::entity::WILDCARDS.to_string(),
                    format!("{}:{}", entry.pattern_str(), entry.client_id_str()),
                    data.clone(),
                )
            })
            .collect();

        Ok(writes)
    }

    pub fn broadcast_wildcard_writes(&mut self, writes: Vec<ReplicationWrite>) {
        for write in writes {
            let partition = write.partition;
            let replicas: Vec<NodeId> = self.partition_map.replicas(partition).to_vec();

            for &replica in &replicas {
                let _ = self
                    .transport
                    .send(replica, ClusterMessage::Write(write.clone()));
            }
        }
    }

    fn handle_catchup_request(
        &self,
        from: NodeId,
        partition: PartitionId,
        from_seq: u64,
        to_seq: u64,
    ) {
        tracing::debug!(?partition, from_seq, to_seq, "processing catchup request");

        let writes = if self.write_log.can_catchup(partition, from_seq) {
            self.write_log.get_range(partition, from_seq, to_seq)
        } else {
            tracing::warn!(
                ?partition,
                from_seq,
                "cannot serve catchup - data evicted from log"
            );
            Vec::new()
        };

        let resp = CatchupResponse::create(partition, self.node_id, writes);
        let _ = self
            .transport
            .send(from, ClusterMessage::CatchupResponse(resp));
    }

    fn handle_catchup_response(&mut self, resp: CatchupResponse) {
        let partition = resp.partition;
        tracing::debug!(
            ?partition,
            write_count = resp.writes.len(),
            "received catchup response"
        );

        if resp.writes.is_empty() {
            return;
        }

        let mut writes: Vec<_> = resp.writes;
        writes.sort_by_key(|w| w.sequence);

        for write in &writes {
            if let Some(state) = self.replicas.get_mut(&partition.get()) {
                let ack = state.handle_write(write);
                if ack.is_ok() {
                    if let Err(e) = self.stores.apply_write(write) {
                        tracing::error!(?partition, ?e, "failed to apply catchup write to stores");
                    }
                    self.write_log
                        .append(partition, write.sequence, write.clone());
                } else {
                    tracing::warn!(
                        ?partition,
                        seq = write.sequence,
                        "catchup write rejected by replica state"
                    );
                }
            }
        }
    }

    fn handle_forwarded_publish(&mut self, from: NodeId, fwd: &ForwardedPublish) {
        let fingerprint = Self::forward_fingerprint(fwd);

        if self.forward_dedup.contains(&fingerprint) {
            tracing::trace!(
                ?from,
                topic = %fwd.topic,
                "deduplicated forwarded publish"
            );
            return;
        }

        if self.forward_dedup.len() >= FORWARD_DEDUP_CAPACITY
            && let Some(old) = self.forward_dedup_order.pop_front()
        {
            self.forward_dedup.remove(&old);
        }
        self.forward_dedup.insert(fingerprint);
        self.forward_dedup_order.push_back(fingerprint);

        tracing::debug!(
            local_node = ?self.node_id,
            origin = ?fwd.origin_node,
            topic = %fwd.topic,
            qos = fwd.qos,
            retain = fwd.retain,
            payload_len = fwd.payload.len(),
            target_count = fwd.targets.len(),
            ?from,
            "received forwarded publish"
        );

        self.transport.queue_local_publish(
            fwd.topic.clone(),
            fwd.payload.clone(),
            fwd.qos,
        );
    }

    fn forward_fingerprint(fwd: &ForwardedPublish) -> u64 {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        fwd.origin_node.hash(&mut hasher);
        fwd.topic.hash(&mut hasher);
        fwd.payload.hash(&mut hasher);
        hasher.finish()
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

    #[test]
    fn create_session_quorum_requires_primary() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let result = ctrl.create_session_quorum("client1");
        assert!(matches!(result, Err(ReplicationError::NotPrimary)));
    }

    #[test]
    fn create_session_quorum_succeeds_as_primary() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let partition = session_partition("client1");
        ctrl.become_primary(partition, Epoch::new(1));

        let (session, _rx) = ctrl.create_session_quorum("client1").unwrap();
        assert_eq!(session.client_id_str(), "client1");
        assert!(ctrl.stores().sessions.get("client1").is_some());
    }

    #[test]
    fn replicate_write_async_sends_without_tracking() {
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

        let seq = ctrl.replicate_write_async(write, &[node2, node3]).unwrap();
        assert_eq!(seq, 1);

        let sent = ctrl.transport.sent_messages();
        assert_eq!(sent.len(), 2);
    }

    #[test]
    fn subscribe_wildcard_broadcast_creates_64_writes() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let partition = PartitionId::new(0).unwrap();
        let writes = ctrl
            .subscribe_wildcard_broadcast("sensors/+/temp", "client1", partition, 1)
            .unwrap();

        assert_eq!(writes.len(), 64);

        let mut partitions_covered: Vec<u16> = writes.iter().map(|w| w.partition.get()).collect();
        partitions_covered.sort_unstable();
        let expected: Vec<u16> = (0..64).collect();
        assert_eq!(partitions_covered, expected);
    }

    #[test]
    fn broadcast_wildcard_writes_sends_to_replicas() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let partition = PartitionId::new(0).unwrap();
        let mut map = PartitionMap::default();
        map.set(
            partition,
            crate::cluster::PartitionAssignment {
                primary: Some(node1),
                replicas: vec![node2],
                epoch: Epoch::new(1),
            },
        );
        ctrl.update_partition_map(map);

        let writes = vec![ReplicationWrite::new(
            partition,
            Operation::Insert,
            Epoch::new(1),
            1,
            "test".to_string(),
            "1".to_string(),
            vec![],
        )];

        ctrl.broadcast_wildcard_writes(writes);

        let sent = ctrl.transport.sent_messages();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, node2);
    }

    #[test]
    fn create_session_quorum_signals_completion_on_acks() {
        use crate::cluster::protocol::ReplicationAck;
        use crate::cluster::transport::InboundMessage;

        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let partition = session_partition("quorum-test-client");
        ctrl.become_primary(partition, Epoch::new(1));

        let mut map = PartitionMap::default();
        map.set(
            partition,
            crate::cluster::PartitionAssignment {
                primary: Some(node1),
                replicas: vec![node2],
                epoch: Epoch::new(1),
            },
        );
        ctrl.update_partition_map(map);

        let (session, mut rx) = ctrl.create_session_quorum("quorum-test-client").unwrap();
        assert_eq!(session.client_id_str(), "quorum-test-client");

        assert!(rx.try_recv().is_err());

        let ack = ReplicationAck::ok(partition, Epoch::new(1), 1, node2);
        ctrl.handle_message(InboundMessage {
            from: node2,
            message: ClusterMessage::Ack(ack),
            received_at: 0,
        });

        let result = rx.try_recv().unwrap();
        assert_eq!(result, QuorumResult::Success);
    }
}
