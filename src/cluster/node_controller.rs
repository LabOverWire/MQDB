use super::entity;
use super::heartbeat::{HeartbeatManager, NodeStatus};
use super::idempotency_store::{IdempotencyCheck, IdempotencyError};
use super::migration::{MigrationManager, MigrationPhase};
use super::offset_store::ConsumerOffset;
use super::protocol::{
    BatchReadRequest, BatchReadResponse, CatchupRequest, CatchupResponse, ForwardedPublish,
    JsonDbOp, JsonDbRequest, JsonDbResponse, Operation, QueryRequest, QueryResponse, QueryStatus,
    ReplicationAck, ReplicationWrite, UniqueCommitRequest, UniqueCommitResponse,
    UniqueReleaseRequest, UniqueReleaseResponse, UniqueReserveRequest, UniqueReserveResponse,
    UniqueReserveStatus,
};
use super::query_coordinator::QueryCoordinator;
use super::quorum::{PendingWrites, QuorumResult, QuorumTracker};
use super::raft::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use super::replication::{ReplicaRole, ReplicaState};
use super::retained_store::RetainedMessage;
use super::snapshot::{
    SnapshotBuilder, SnapshotChunk, SnapshotComplete, SnapshotRequest, SnapshotSender,
};
use super::store_manager::StoreManager;
use super::transport::{ClusterMessage, ClusterTransport, InboundMessage, TransportConfig};
use super::write_log::PartitionWriteLog;
use super::{Epoch, NodeId, PartitionId, PartitionMap, session_partition};
use crate::storage::StorageBackend;
use bebytes::BeBytes;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::oneshot;

const FORWARD_DEDUP_CAPACITY: usize = 1000;
const UNIQUE_REQUEST_TIMEOUT_SECS: u64 = 5;

pub struct PendingScatterRequest {
    pub expected_count: usize,
    pub received: Vec<serde_json::Value>,
    pub client_response_topic: String,
    pub created_at_ms: u64,
    pub filters: Vec<crate::Filter>,
    pub sorts: Vec<crate::SortOrder>,
}

#[derive(Debug)]
pub enum RaftMessage {
    RequestVote {
        from: NodeId,
        request: RequestVoteRequest,
    },
    RequestVoteResponse {
        from: NodeId,
        response: RequestVoteResponse,
    },
    AppendEntries {
        from: NodeId,
        request: AppendEntriesRequest,
    },
    AppendEntriesResponse {
        from: NodeId,
        response: AppendEntriesResponse,
    },
}

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
    raft_messages: VecDeque<RaftMessage>,
    partition_updates: VecDeque<super::raft::PartitionUpdate>,
    dead_nodes: VecDeque<NodeId>,
    draining_nodes: VecDeque<NodeId>,
    migration_manager: MigrationManager,
    pending_snapshots: HashMap<PartitionId, SnapshotBuilder>,
    outgoing_snapshots: HashMap<(NodeId, PartitionId), SnapshotSender>,
    draining: bool,
    query_coordinator: QueryCoordinator,
    synced_retained_topics: Option<Arc<tokio::sync::RwLock<HashSet<String>>>>,
    pending_retained_queries: HashMap<u64, oneshot::Sender<Vec<RetainedMessage>>>,
    pending_scatter_requests: HashMap<u64, PendingScatterRequest>,
    pending_unique_requests: HashMap<u64, oneshot::Sender<UniqueReserveStatus>>,
    pending_unique_commit_requests: HashMap<u64, oneshot::Sender<bool>>,
    pending_unique_release_requests: HashMap<u64, oneshot::Sender<bool>>,
    next_unique_request_id: u64,
}

impl<T: ClusterTransport> std::fmt::Debug for NodeController<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeController")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl<T: ClusterTransport> NodeController<T> {
    #[must_use]
    pub fn new(node_id: NodeId, transport: T, config: TransportConfig) -> Self {
        Self::new_with_storage(node_id, transport, config, None)
    }

    #[must_use]
    pub fn new_with_storage(
        node_id: NodeId,
        transport: T,
        config: TransportConfig,
        storage: Option<Arc<dyn StorageBackend>>,
    ) -> Self {
        Self {
            node_id,
            heartbeat: HeartbeatManager::new(node_id, config),
            transport,
            replicas: HashMap::new(),
            pending: PendingWrites::new(1000),
            partition_map: PartitionMap::default(),
            current_time: 0,
            stores: StoreManager::new_with_storage(node_id, storage),
            write_log: PartitionWriteLog::new(),
            forward_dedup: HashSet::with_capacity(FORWARD_DEDUP_CAPACITY),
            forward_dedup_order: VecDeque::with_capacity(FORWARD_DEDUP_CAPACITY),
            raft_messages: VecDeque::new(),
            partition_updates: VecDeque::new(),
            dead_nodes: VecDeque::new(),
            draining_nodes: VecDeque::new(),
            migration_manager: MigrationManager::new(node_id),
            pending_snapshots: HashMap::new(),
            outgoing_snapshots: HashMap::new(),
            draining: false,
            query_coordinator: QueryCoordinator::new(node_id),
            synced_retained_topics: None,
            pending_retained_queries: HashMap::new(),
            pending_scatter_requests: HashMap::new(),
            pending_unique_requests: HashMap::new(),
            pending_unique_commit_requests: HashMap::new(),
            pending_unique_release_requests: HashMap::new(),
            next_unique_request_id: 1,
        }
    }

    pub fn set_synced_retained_topics(
        &mut self,
        topics: Arc<tokio::sync::RwLock<HashSet<String>>>,
    ) {
        self.synced_retained_topics = Some(topics);
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
        if self.partition_map.primary(partition) == Some(self.node_id) {
            return true;
        }
        self.replicas
            .get(&partition.get())
            .is_some_and(|s| s.role() == ReplicaRole::Primary)
    }

    #[must_use]
    pub fn can_serve_reads(&self, partition: PartitionId) -> bool {
        if self.is_local_partition(partition) {
            return true;
        }
        self.partition_map.replicas(partition).contains(&self.node_id)
    }

    /// Pick a local partition for creating new entities (round-robin).
    ///
    /// # Panics
    /// Panics if no valid partition can be created (should never happen with 64 partitions).
    #[must_use]
    pub fn pick_partition_for_create(&self) -> PartitionId {
        use std::sync::atomic::{AtomicU16, Ordering};
        static COUNTER: AtomicU16 = AtomicU16::new(0);

        for _ in 0..super::NUM_PARTITIONS {
            let idx = COUNTER.fetch_add(1, Ordering::Relaxed) % super::NUM_PARTITIONS;
            if let Some(partition) = PartitionId::new(idx)
                && self.is_local_partition(partition)
            {
                return partition;
            }
        }
        PartitionId::new(0).expect("partition 0 should always be valid")
    }

    pub fn register_peer(&mut self, peer: NodeId) {
        self.heartbeat.register_node(peer);
    }

    pub fn alive_nodes(&self) -> Vec<NodeId> {
        self.heartbeat.alive_nodes()
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

    pub async fn tick(&mut self, now: u64) {
        self.current_time = now;

        if self.heartbeat.should_send(now) {
            let hb = self.heartbeat.create_heartbeat(now);
            let _ = self.transport.broadcast(hb).await;
        }

        let dead = self.heartbeat.check_timeouts(now);
        for node in dead {
            self.handle_node_death(node);
        }

        self.initiate_catchup_requests(now).await;
        self.cleanup_stale_scatter_requests(now).await;
    }

    async fn cleanup_stale_scatter_requests(&mut self, now: u64) {
        const SCATTER_TIMEOUT_MS: u64 = 10_000;
        let stale_threshold = now.saturating_sub(SCATTER_TIMEOUT_MS);

        let stale_ids: Vec<u64> = self
            .pending_scatter_requests
            .iter()
            .filter(|(_, req)| req.created_at_ms < stale_threshold)
            .map(|(id, _)| *id)
            .collect();

        for id in stale_ids {
            if let Some(pending) = self.pending_scatter_requests.remove(&id) {
                tracing::warn!(
                    request_id = id,
                    expected = pending.expected_count,
                    received = pending.received.len(),
                    "scatter request timed out, sending partial results"
                );

                let mut seen_ids = std::collections::HashSet::new();
                let deduped: Vec<serde_json::Value> = pending
                    .received
                    .into_iter()
                    .filter(|item| {
                        if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                            seen_ids.insert(id.to_string())
                        } else {
                            true
                        }
                    })
                    .collect();

                let filtered: Vec<serde_json::Value> = deduped
                    .into_iter()
                    .filter(|item| {
                        if let Some(data) = item.get("data") {
                            Self::matches_filters(data, &pending.filters)
                        } else {
                            false
                        }
                    })
                    .collect();

                let result = serde_json::json!({
                    "status": "ok",
                    "data": filtered,
                    "partial": true
                });
                let payload = serde_json::to_vec(&result).unwrap_or_default();

                self.transport
                    .queue_local_publish(pending.client_response_topic, payload, 0)
                    .await;
            }
        }
    }

    async fn initiate_catchup_requests(&mut self, now: u64) {
        for (&partition_id, state) in &mut self.replicas {
            if !state.needs_catchup_request(now) {
                continue;
            }

            let Some((from_seq, to_seq)) = state.gap_range() else {
                continue;
            };

            let Some(partition) = PartitionId::new(partition_id) else {
                continue;
            };

            let assignment = self.partition_map.get(partition);
            let Some(primary) = assignment.primary else {
                continue;
            };

            tracing::debug!(
                ?partition,
                from_seq,
                to_seq,
                ?primary,
                "initiating catchup request as replica"
            );

            let req = CatchupRequest::create(partition, from_seq, to_seq, self.node_id);
            let _ = self
                .transport
                .send(primary, ClusterMessage::CatchupRequest(req))
                .await;

            state.mark_catchup_requested(now);
        }
    }

    fn handle_node_death(&mut self, dead_node: NodeId) {
        tracing::warn!(?dead_node, "node death detected");
        self.dead_nodes.push_back(dead_node);

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

    pub async fn process_messages(&mut self) {
        while let Some(msg) = self.transport.recv() {
            self.handle_message(msg).await;
        }
    }

    pub fn drain_raft_messages(&mut self) -> impl Iterator<Item = RaftMessage> + '_ {
        self.raft_messages.drain(..)
    }

    pub fn drain_partition_updates(
        &mut self,
    ) -> impl Iterator<Item = super::raft::PartitionUpdate> + '_ {
        self.partition_updates.drain(..)
    }

    pub fn drain_dead_nodes(&mut self) -> impl Iterator<Item = NodeId> + '_ {
        self.dead_nodes.drain(..)
    }

    pub fn drain_draining_nodes(&mut self) -> impl Iterator<Item = NodeId> + '_ {
        self.draining_nodes.drain(..)
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_message(&mut self, msg: InboundMessage) {
        match msg.message {
            ClusterMessage::Heartbeat(hb) => {
                self.heartbeat
                    .receive_heartbeat(msg.from, &hb, msg.received_at);
            }
            ClusterMessage::Write(ref write) => {
                self.handle_write(msg.from, write).await;
            }
            ClusterMessage::WriteRequest(write) => {
                self.handle_write_request(msg.from, write).await;
            }
            ClusterMessage::Ack(ack) => {
                self.handle_ack(ack).await;
            }
            ClusterMessage::DeathNotice { node_id } => {
                self.heartbeat.handle_death_notice(node_id);
                self.handle_node_death(node_id);
            }
            ClusterMessage::DrainNotification { node_id } => {
                tracing::info!(?node_id, "received drain notification");
                self.draining_nodes.push_back(node_id);
            }
            ClusterMessage::RequestVote(req) => {
                self.raft_messages.push_back(RaftMessage::RequestVote {
                    from: msg.from,
                    request: req,
                });
            }
            ClusterMessage::RequestVoteResponse(resp) => {
                self.raft_messages
                    .push_back(RaftMessage::RequestVoteResponse {
                        from: msg.from,
                        response: resp,
                    });
            }
            ClusterMessage::AppendEntries(req) => {
                self.raft_messages.push_back(RaftMessage::AppendEntries {
                    from: msg.from,
                    request: req,
                });
            }
            ClusterMessage::AppendEntriesResponse(resp) => {
                self.raft_messages
                    .push_back(RaftMessage::AppendEntriesResponse {
                        from: msg.from,
                        response: resp,
                    });
            }
            ClusterMessage::CatchupRequest(req) => {
                self.handle_catchup_request(
                    msg.from,
                    req.partition(),
                    req.from_sequence(),
                    req.to_sequence(),
                )
                .await;
            }
            ClusterMessage::CatchupResponse(resp) => {
                self.handle_catchup_response(resp).await;
            }
            ClusterMessage::ForwardedPublish(ref fwd) => {
                self.handle_forwarded_publish(msg.from, fwd).await;
            }
            ClusterMessage::SnapshotRequest(ref req) => {
                self.handle_snapshot_request(msg.from, req).await;
            }
            ClusterMessage::SnapshotChunk(ref chunk) => {
                self.handle_snapshot_chunk(msg.from, chunk).await;
            }
            ClusterMessage::SnapshotComplete(ref complete) => {
                self.handle_snapshot_complete(msg.from, complete);
            }
            ClusterMessage::QueryRequest {
                partition,
                ref request,
            } => {
                let response = self.handle_query_request(partition, request);
                let response_msg = ClusterMessage::QueryResponse(response);
                let _ = self.transport.send(msg.from, response_msg).await;
            }
            ClusterMessage::QueryResponse(ref response) => {
                if self
                    .pending_retained_queries
                    .contains_key(&response.query_id)
                {
                    let results = Self::parse_retained_query_response(response);
                    self.complete_retained_query(response.query_id, results);
                } else if let Some(result) =
                    self.query_coordinator.receive_response(response.clone())
                {
                    tracing::debug!(
                        query_id = result.query_id,
                        partial = result.partial,
                        "query completed"
                    );
                }
            }
            ClusterMessage::BatchReadRequest(ref request) => {
                let response = self.handle_batch_read_request(request);
                let response_msg = ClusterMessage::BatchReadResponse(response);
                let _ = self.transport.send(msg.from, response_msg).await;
            }
            ClusterMessage::BatchReadResponse(_) => {}
            ClusterMessage::WildcardBroadcast(ref broadcast) => {
                self.handle_wildcard_broadcast(msg.from, broadcast);
            }
            ClusterMessage::TopicSubscriptionBroadcast(ref broadcast) => {
                self.handle_topic_subscription_broadcast(msg.from, broadcast);
            }
            ClusterMessage::PartitionUpdate(ref update) => {
                if let (Some(partition), Some(primary)) = (
                    PartitionId::new(u16::from(update.partition)),
                    NodeId::validated(update.primary),
                ) {
                    let epoch = Epoch::new(u64::from(update.epoch));

                    if primary == self.node_id {
                        self.become_primary(partition, epoch);
                    } else if NodeId::validated(update.replica1) == Some(self.node_id)
                        || NodeId::validated(update.replica2) == Some(self.node_id)
                    {
                        self.become_replica(partition, epoch, 0);
                    } else {
                        self.step_down(partition);
                    }
                }

                self.partition_map.apply_update(update);
                self.heartbeat.partition_map_mut().apply_update(update);
                self.partition_updates.push_back(*update);
                tracing::info!(
                    partition = update.partition,
                    primary = update.primary,
                    "received partition update from cluster"
                );
            }
            ClusterMessage::JsonDbRequest {
                partition,
                ref request,
            } => {
                self.handle_json_db_request(msg.from, partition, request)
                    .await;
            }
            ClusterMessage::JsonDbResponse(ref response) => {
                self.handle_json_db_response(response).await;
            }
            ClusterMessage::UniqueReserveRequest(ref req) => {
                self.handle_unique_reserve_request(msg.from, req).await;
            }
            ClusterMessage::UniqueReserveResponse(ref resp) => {
                self.handle_unique_reserve_response(resp);
            }
            ClusterMessage::UniqueCommitRequest(ref req) => {
                self.handle_unique_commit_request(msg.from, req).await;
            }
            ClusterMessage::UniqueCommitResponse(ref resp) => {
                self.handle_unique_commit_response(resp);
            }
            ClusterMessage::UniqueReleaseRequest(ref req) => {
                self.handle_unique_release_request(msg.from, req).await;
            }
            ClusterMessage::UniqueReleaseResponse(ref resp) => {
                self.handle_unique_release_response(resp);
            }
        }
    }

    fn handle_wildcard_broadcast(
        &mut self,
        from: NodeId,
        broadcast: &super::protocol::WildcardBroadcast,
    ) {
        let pattern = broadcast.pattern_str();
        let client_id = broadcast.client_id_str();

        match broadcast.operation() {
            Some(super::protocol::WildcardOp::Subscribe) => {
                let subscription_type = if broadcast.subscription_type() == 1 {
                    super::topic_trie::SubscriptionType::Db
                } else {
                    super::topic_trie::SubscriptionType::Mqtt
                };
                let result = self.stores.wildcards.subscribe(
                    pattern,
                    client_id,
                    broadcast.qos(),
                    subscription_type,
                );
                match result {
                    Ok(()) => {
                        tracing::debug!(
                            pattern,
                            client_id,
                            from = from.get(),
                            "applied wildcard subscription from broadcast"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            pattern,
                            client_id,
                            ?e,
                            "failed to apply wildcard subscription from broadcast"
                        );
                    }
                }
            }
            Some(super::protocol::WildcardOp::Unsubscribe) => {
                let _ = self.stores.wildcards.unsubscribe(pattern, client_id);
                tracing::debug!(
                    pattern,
                    client_id,
                    from = from.get(),
                    "applied wildcard unsubscription from broadcast"
                );
            }
            None => {
                tracing::warn!("unknown wildcard broadcast operation");
            }
        }
    }

    fn handle_topic_subscription_broadcast(
        &mut self,
        from: NodeId,
        broadcast: &super::protocol::TopicSubscriptionBroadcast,
    ) {
        let topic = broadcast.topic_str();
        let client_id = broadcast.client_id_str();

        match broadcast.operation() {
            Some(super::protocol::WildcardOp::Subscribe) => {
                if let Some(client_partition) = broadcast.client_partition() {
                    let result = self.stores.topics.subscribe(
                        topic,
                        client_id,
                        client_partition,
                        broadcast.qos(),
                    );
                    match result {
                        Ok(()) => {
                            tracing::debug!(
                                topic,
                                client_id,
                                from = from.get(),
                                "applied topic subscription from broadcast"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                topic,
                                client_id,
                                ?e,
                                "failed to apply topic subscription from broadcast"
                            );
                        }
                    }
                }
            }
            Some(super::protocol::WildcardOp::Unsubscribe) => {
                let _ = self.stores.topics.unsubscribe(topic, client_id);
                tracing::debug!(
                    topic,
                    client_id,
                    from = from.get(),
                    "applied topic unsubscription from broadcast"
                );
            }
            None => {
                tracing::warn!("unknown topic subscription broadcast operation");
            }
        }
    }

    async fn handle_write(&mut self, from: NodeId, write: &ReplicationWrite) {
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
                    self.sync_retained_to_broker(write).await;
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

        let _ = self.transport.send(from, ClusterMessage::Ack(ack)).await;
    }

    async fn handle_write_request(&mut self, from: NodeId, write: ReplicationWrite) {
        let partition = write.partition;
        let is_broadcast = write.entity == entity::TOPIC_INDEX
            || write.entity == entity::WILDCARDS
            || write.entity == entity::CLIENT_LOCATIONS
            || write.entity == entity::DB_SCHEMA
            || write.entity == entity::DB_CONSTRAINT;

        tracing::debug!(
            ?partition,
            from = from.get(),
            entity = ?write.entity,
            is_broadcast,
            "received write request"
        );

        if is_broadcast && let Err(e) = self.stores.apply_write(&write) {
            tracing::error!(?partition, ?e, "failed to apply broadcast write to stores");
        }

        if !self.is_local_partition(partition) {
            if !is_broadcast {
                tracing::warn!(
                    ?partition,
                    from = from.get(),
                    "received write request but not primary"
                );
            }
            return;
        }

        if !is_broadcast && let Err(e) = self.stores.apply_write(&write) {
            tracing::error!(?partition, ?e, "failed to apply write request to stores");
            return;
        }

        tracing::debug!(
            ?partition,
            entity = ?write.entity,
            id = ?write.id,
            "applied write request to stores"
        );
        self.sync_retained_to_broker(&write).await;

        let replicas: Vec<NodeId> = self.partition_map.replicas(partition).to_vec();
        if let Err(e) = self.replicate_write_async(write, &replicas).await {
            tracing::warn!(?partition, ?e, "failed to replicate write request");
        }
    }

    async fn handle_ack(&mut self, ack: ReplicationAck) {
        self.pending.record_ack(&ack);

        if ack.status() == Some(super::protocol::AckStatus::SequenceGap) {
            self.handle_sequence_gap_ack(&ack).await;
        }

        for (seq, result) in self.pending.drain_completed() {
            self.on_write_complete(ack.partition(), seq, result);
        }
    }

    async fn handle_sequence_gap_ack(&self, ack: &ReplicationAck) {
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
            .send(replica_node, ClusterMessage::CatchupResponse(resp))
            .await;
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
    pub async fn replicate_write(
        &mut self,
        write: ReplicationWrite,
        replicas: &[NodeId],
        required_acks: usize,
    ) -> Result<u64, ReplicationError> {
        let partition = write.partition;

        let (sequence, epoch) = {
            let state = self
                .replicas
                .get_mut(&partition.get())
                .ok_or(ReplicationError::NotPrimary)?;

            if state.role() != ReplicaRole::Primary {
                return Err(ReplicationError::NotPrimary);
            }

            (state.advance_sequence(), state.epoch())
        };

        let write_msg = ReplicationWrite::new(
            partition,
            write.operation,
            epoch,
            sequence,
            write.entity,
            write.id,
            write.data,
        );

        self.write_log
            .append(partition, sequence, write_msg.clone());
        let _ = self.stores.apply_write(&write_msg);

        let tracker = QuorumTracker::new(sequence, epoch, replicas, required_acks);

        if !self.pending.add(tracker) {
            return Err(ReplicationError::TooManyPending);
        }

        for &replica in replicas {
            let _ = self
                .transport
                .send(replica, ClusterMessage::Write(write_msg.clone()))
                .await;
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
    pub fn has_alive_peers(&self) -> bool {
        self.heartbeat.has_alive_peers()
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
    pub async fn create_session_replicated(
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
            self.replicate_write(write, &replicas, required_acks)
                .await?;
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
    pub async fn create_session_quorum(
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
                .send(replica, ClusterMessage::Write(write_msg.clone()))
                .await;
        }

        Ok((session, rx))
    }

    /// # Errors
    /// Returns `NotPrimary` if this node is not the primary for the partition.
    pub async fn replicate_write_async(
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

        self.write_log
            .append(partition, sequence, write_msg.clone());
        let _ = self.stores.apply_write(&write_msg);

        for &replica in replicas {
            let _ = self
                .transport
                .send(replica, ClusterMessage::Write(write_msg.clone()))
                .await;
        }

        Ok(sequence)
    }

    pub async fn write_or_forward(&mut self, write: ReplicationWrite) {
        let partition = write.partition;

        let is_broadcast_entity = write.entity == entity::TOPIC_INDEX
            || write.entity == entity::WILDCARDS
            || write.entity == entity::CLIENT_LOCATIONS
            || write.entity == entity::DB_SCHEMA
            || write.entity == entity::DB_CONSTRAINT;
        if is_broadcast_entity {
            let _ = self.stores.apply_write(&write);
            let alive = self.heartbeat.alive_nodes();
            let targets: Vec<_> = alive.iter().filter(|&n| *n != self.node_id).collect();
            tracing::debug!(
                entity = write.entity,
                id = write.id,
                alive_count = alive.len(),
                target_count = targets.len(),
                targets = ?targets.iter().map(|n| n.get()).collect::<Vec<_>>(),
                "write_or_forward: broadcasting to alive nodes"
            );
            for node in targets {
                let _ = self
                    .transport
                    .send(*node, ClusterMessage::WriteRequest(write.clone()))
                    .await;
            }
            return;
        }

        if self.is_local_partition(partition) {
            tracing::debug!(
                ?partition,
                entity = ?write.entity,
                "write_or_forward: local partition, replicating"
            );
            let replicas: Vec<NodeId> = self.partition_map.replicas(partition).to_vec();
            let _ = self.replicate_write_async(write, &replicas).await;
        } else if let Some(primary) = self
            .partition_map
            .primary(partition)
            .or_else(|| self.heartbeat.partition_map().primary(partition))
        {
            tracing::debug!(
                ?partition,
                primary = primary.get(),
                entity = ?write.entity,
                "write_or_forward: forwarding to primary"
            );
            let _ = self
                .transport
                .send(primary, ClusterMessage::WriteRequest(write))
                .await;
        } else {
            tracing::warn!(
                ?partition,
                entity = ?write.entity,
                "write_or_forward: no primary found for partition"
            );
        }
    }

    /// # Errors
    /// Returns `DbDataStoreError::AlreadyExists` if the entity already exists.
    pub async fn db_create(
        &mut self,
        entity_type: &str,
        id: &str,
        data: &[u8],
        timestamp_ms: u64,
    ) -> Result<super::db::DbEntity, super::db::DbDataStoreError> {
        let (db_entity, write) =
            self.stores
                .db_create_replicated(entity_type, id, data, timestamp_ms)?;
        self.write_or_forward(write).await;
        Ok(db_entity)
    }

    /// # Errors
    /// Returns `DbDataStoreError::NotFound` if the entity does not exist.
    pub async fn db_update(
        &mut self,
        entity_type: &str,
        id: &str,
        data: &[u8],
        timestamp_ms: u64,
    ) -> Result<super::db::DbEntity, super::db::DbDataStoreError> {
        let (db_entity, write) =
            self.stores
                .db_update_replicated(entity_type, id, data, timestamp_ms)?;
        self.write_or_forward(write).await;
        Ok(db_entity)
    }

    pub async fn db_upsert(
        &mut self,
        entity_type: &str,
        id: &str,
        data: &[u8],
        timestamp_ms: u64,
    ) -> super::db::DbEntity {
        let (db_entity, write) =
            self.stores
                .db_upsert_replicated(entity_type, id, data, timestamp_ms);
        self.write_or_forward(write).await;
        db_entity
    }

    /// # Errors
    /// Returns `DbDataStoreError::NotFound` if the entity does not exist.
    pub async fn db_delete(
        &mut self,
        entity_type: &str,
        id: &str,
    ) -> Result<super::db::DbEntity, super::db::DbDataStoreError> {
        let (db_entity, write) = self.stores.db_delete_replicated(entity_type, id)?;
        self.write_or_forward(write).await;
        Ok(db_entity)
    }

    #[must_use]
    pub fn db_get(&self, entity_type: &str, id: &str) -> Option<super::db::DbEntity> {
        self.stores.db_get(entity_type, id)
    }

    #[must_use]
    pub fn db_list(&self, entity_type: &str) -> Vec<super::db::DbEntity> {
        self.stores.db_list(entity_type)
    }

    /// # Errors
    /// Returns `SchemaStoreError::AlreadyExists` if the schema already exists.
    ///
    /// # Panics
    /// Panics if partition 0 is invalid (should never happen as 0-63 are valid).
    pub async fn schema_register(
        &mut self,
        entity: &str,
        schema_data: &[u8],
    ) -> Result<super::db::ClusterSchema, super::db::SchemaStoreError> {
        let schema = self.stores.db_schema.register(entity, schema_data)?;
        let serialized = super::db::SchemaStore::serialize(&schema);
        let write = ReplicationWrite::new(
            PartitionId::new(0).unwrap(),
            super::protocol::Operation::Insert,
            Epoch::ZERO,
            0,
            entity::DB_SCHEMA.to_string(),
            entity.to_string(),
            serialized,
        );
        self.write_or_forward(write).await;
        Ok(schema)
    }

    /// # Errors
    /// Returns `SchemaStoreError::NotFound` if the schema does not exist.
    ///
    /// # Panics
    /// Panics if partition 0 is invalid (should never happen as 0-63 are valid).
    pub async fn schema_update(
        &mut self,
        entity: &str,
        schema_data: &[u8],
    ) -> Result<super::db::ClusterSchema, super::db::SchemaStoreError> {
        let schema = self.stores.db_schema.update(entity, schema_data)?;
        let serialized = super::db::SchemaStore::serialize(&schema);
        let write = ReplicationWrite::new(
            PartitionId::new(0).unwrap(),
            super::protocol::Operation::Update,
            Epoch::ZERO,
            0,
            entity::DB_SCHEMA.to_string(),
            entity.to_string(),
            serialized,
        );
        self.write_or_forward(write).await;
        Ok(schema)
    }

    #[must_use]
    pub fn schema_get(&self, entity: &str) -> Option<super::db::ClusterSchema> {
        self.stores.schema_get(entity)
    }

    #[must_use]
    pub fn schema_list(&self) -> Vec<super::db::ClusterSchema> {
        self.stores.schema_list()
    }

    #[must_use]
    pub fn schema_is_valid_for_write(&self, entity: &str) -> bool {
        self.stores.schema_is_valid_for_write(entity)
    }

    /// # Errors
    /// Returns `ConstraintStoreError::AlreadyExists` if the constraint already exists.
    pub async fn constraint_add(
        &mut self,
        constraint: &super::db::ClusterConstraint,
    ) -> Result<(), super::db::ConstraintStoreError> {
        let write = self.stores.constraint_add_replicated(constraint)?;
        self.write_or_forward(write).await;
        Ok(())
    }

    /// # Errors
    /// Returns `ConstraintStoreError::NotFound` if the constraint does not exist.
    pub async fn constraint_remove(
        &mut self,
        entity: &str,
        name: &str,
    ) -> Result<(), super::db::ConstraintStoreError> {
        let write = self.stores.constraint_remove_replicated(entity, name)?;
        self.write_or_forward(write).await;
        Ok(())
    }

    #[must_use]
    pub fn constraint_list(&self, entity: &str) -> Vec<super::db::ClusterConstraint> {
        self.stores.constraint_list(entity)
    }

    #[must_use]
    pub fn constraint_list_all(&self) -> Vec<super::db::ClusterConstraint> {
        self.stores.constraint_list_all()
    }

    #[must_use]
    pub fn constraint_get_unique_fields(&self, entity: &str) -> Vec<String> {
        self.stores.constraint_get_unique_fields(entity)
    }

    /// # Errors
    /// Returns the conflicting field name if a unique constraint violation is detected.
    pub async fn check_unique_constraints(
        &mut self,
        entity: &str,
        id: &str,
        data: &serde_json::Value,
        data_partition: PartitionId,
        request_id: &str,
        now_ms: u64,
    ) -> Result<(), String> {
        let unique_fields = self.stores.constraint_get_unique_fields(entity);
        let mut local_reserved: Vec<(String, Vec<u8>)> = Vec::new();
        let mut remote_reserved: Vec<(String, Vec<u8>, NodeId)> = Vec::new();

        for field in &unique_fields {
            let value = match data.get(field) {
                Some(v) => serde_json::to_vec(v).unwrap_or_default(),
                None => continue,
            };

            let unique_part = super::db::unique_partition(entity, field, &value);
            let primary = self.partition_map.primary(unique_part);

            let is_conflict = if primary == Some(self.node_id) {
                let (result, write) = self.stores.unique_reserve_replicated(
                    entity,
                    field,
                    &value,
                    id,
                    request_id,
                    data_partition,
                    30_000,
                    now_ms,
                );

                match result {
                    super::db::ReserveResult::Reserved => {
                        if let Some(w) = write {
                            self.write_or_forward(w).await;
                        }
                        local_reserved.push((field.clone(), value));
                        false
                    }
                    super::db::ReserveResult::AlreadyReservedBySameRequest => {
                        local_reserved.push((field.clone(), value));
                        false
                    }
                    super::db::ReserveResult::Conflict => true,
                }
            } else if let Some(target_node) = primary {
                let result = self
                    .send_unique_reserve_request(
                        target_node,
                        entity,
                        field,
                        &value,
                        id,
                        request_id,
                        data_partition,
                        30_000,
                    )
                    .await;

                match result {
                    Ok(UniqueReserveStatus::Reserved | UniqueReserveStatus::AlreadyReserved) => {
                        remote_reserved.push((field.clone(), value, target_node));
                        false
                    }
                    Ok(UniqueReserveStatus::Conflict | UniqueReserveStatus::Error) | Err(_) => true,
                }
            } else {
                true
            };

            if is_conflict {
                for (f, v) in &local_reserved {
                    if let Some(w) = self
                        .stores
                        .unique_release_replicated(entity, f, v, request_id)
                    {
                        self.write_or_forward(w).await;
                    }
                }
                for (f, v, target) in &remote_reserved {
                    let _ = self
                        .send_unique_release_request(*target, entity, f, v, request_id)
                        .await;
                }
                return Err(field.clone());
            }
        }
        Ok(())
    }

    pub async fn commit_unique_constraints(
        &mut self,
        entity: &str,
        _id: &str,
        data: &serde_json::Value,
        _data_partition: PartitionId,
        request_id: &str,
        _now_ms: u64,
    ) {
        let unique_fields = self.stores.constraint_get_unique_fields(entity);

        for field in &unique_fields {
            let value = match data.get(field) {
                Some(v) => serde_json::to_vec(v).unwrap_or_default(),
                None => continue,
            };

            let unique_part = super::db::unique_partition(entity, field, &value);
            let primary = self.partition_map.primary(unique_part);

            if primary == Some(self.node_id) {
                if let Ok((_, w)) = self
                    .stores
                    .unique_commit_replicated(entity, field, &value, request_id)
                {
                    self.write_or_forward(w).await;
                }
            } else if let Some(target_node) = primary {
                let _ = self
                    .send_unique_commit_request(target_node, entity, field, &value, request_id)
                    .await;
            }
        }
    }

    pub async fn release_unique_constraints(
        &mut self,
        entity: &str,
        _id: &str,
        data: &serde_json::Value,
        _data_partition: PartitionId,
        request_id: &str,
        _now_ms: u64,
    ) {
        let unique_fields = self.stores.constraint_get_unique_fields(entity);

        for field in &unique_fields {
            let value = match data.get(field) {
                Some(v) => serde_json::to_vec(v).unwrap_or_default(),
                None => continue,
            };

            let unique_part = super::db::unique_partition(entity, field, &value);
            let primary = self.partition_map.primary(unique_part);

            if primary == Some(self.node_id) {
                if let Some(w) = self
                    .stores
                    .unique_release_replicated(entity, field, &value, request_id)
                {
                    self.write_or_forward(w).await;
                }
            } else if let Some(target_node) = primary {
                let _ = self
                    .send_unique_release_request(target_node, entity, field, &value, request_id)
                    .await;
            }
        }
    }

    pub async fn commit_offset(
        &mut self,
        consumer_id: &str,
        partition: PartitionId,
        sequence: u64,
        timestamp: u64,
    ) -> ConsumerOffset {
        let (offset, write) =
            self.stores
                .commit_offset_replicated(consumer_id, partition, sequence, timestamp);
        self.write_or_forward(write).await;
        offset
    }

    #[must_use]
    pub fn get_offset(&self, consumer_id: &str, partition: PartitionId) -> Option<u64> {
        self.stores.offsets.get_sequence(consumer_id, partition)
    }

    /// # Errors
    /// Returns `IdempotencyError::AlreadyProcessing` if a write is already in progress.
    /// Returns `IdempotencyError::PartitionFull` if partition has too many records.
    pub fn check_idempotency(
        &self,
        idempotency_key: &str,
        partition: PartitionId,
        entity: &str,
        id: &str,
        timestamp: u64,
    ) -> Result<IdempotencyCheck, IdempotencyError> {
        let epoch = self
            .replicas
            .get(&partition.get())
            .map_or(Epoch::ZERO, super::replication::ReplicaState::epoch);
        self.stores
            .check_idempotency(idempotency_key, partition, epoch, entity, id, timestamp)
    }

    pub async fn commit_idempotency(
        &mut self,
        partition: PartitionId,
        idempotency_key: &str,
        response: &[u8],
    ) {
        let write = self
            .stores
            .commit_idempotency(partition, idempotency_key, response);
        self.write_or_forward(write).await;
    }

    pub fn rollback_idempotency(&self, partition: PartitionId, idempotency_key: &str) {
        self.stores.rollback_idempotency(partition, idempotency_key);
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

    pub async fn broadcast_wildcard_writes(&mut self, writes: Vec<ReplicationWrite>) {
        for write in writes {
            let partition = write.partition;
            let replicas: Vec<NodeId> = self.partition_map.replicas(partition).to_vec();

            for &replica in &replicas {
                let _ = self
                    .transport
                    .send(replica, ClusterMessage::Write(write.clone()))
                    .await;
            }
        }
    }

    async fn handle_catchup_request(
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
            .send(from, ClusterMessage::CatchupResponse(resp))
            .await;
    }

    async fn handle_catchup_response(&mut self, resp: CatchupResponse) {
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
                    } else {
                        self.sync_retained_to_broker(write).await;
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

    async fn handle_forwarded_publish(&mut self, from: NodeId, fwd: &ForwardedPublish) {
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

        self.transport
            .queue_local_publish(fwd.topic.clone(), fwd.payload.clone(), fwd.qos)
            .await;
    }

    fn forward_fingerprint(fwd: &ForwardedPublish) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        fwd.origin_node.hash(&mut hasher);
        fwd.timestamp_ms.hash(&mut hasher);
        fwd.topic.hash(&mut hasher);
        fwd.payload.hash(&mut hasher);
        hasher.finish()
    }

    async fn handle_snapshot_request(&mut self, from: NodeId, req: &SnapshotRequest) {
        let Some(partition) = req.partition() else {
            tracing::warn!(?from, "received snapshot request with invalid partition");
            return;
        };

        tracing::info!(
            ?partition,
            requester = from.get(),
            "received snapshot request, starting snapshot send"
        );

        let data = self.stores.export_partition(partition);
        let sequence = self.sequence(partition).unwrap_or(0);
        let sender = SnapshotSender::new(partition, data, sequence);

        self.outgoing_snapshots.insert((from, partition), sender);
        self.send_next_snapshot_chunk(from, partition).await;
    }

    async fn send_next_snapshot_chunk(&mut self, to: NodeId, partition: PartitionId) {
        let Some(sender) = self.outgoing_snapshots.get_mut(&(to, partition)) else {
            return;
        };

        if let Some(chunk) = sender.next_chunk() {
            let is_last = chunk.is_last();
            let _ = self
                .transport
                .send(to, ClusterMessage::SnapshotChunk(chunk))
                .await;

            if is_last {
                self.outgoing_snapshots.remove(&(to, partition));
            }
        }
    }

    async fn handle_snapshot_chunk(&mut self, from: NodeId, chunk: &SnapshotChunk) {
        let partition = chunk.partition;

        tracing::debug!(
            ?partition,
            from = from.get(),
            chunk_index = chunk.chunk_index,
            total_chunks = chunk.total_chunks,
            "received snapshot chunk"
        );

        let builder = self.pending_snapshots.entry(partition).or_insert_with(|| {
            SnapshotBuilder::new(partition, chunk.total_chunks, chunk.sequence_at_snapshot)
        });

        if !builder.add_chunk(chunk) {
            tracing::warn!(?partition, "failed to add snapshot chunk");
            let complete = SnapshotComplete::failed(partition);
            let _ = self
                .transport
                .send(from, ClusterMessage::SnapshotComplete(complete))
                .await;
            self.pending_snapshots.remove(&partition);
            return;
        }

        if builder.is_complete() {
            let sequence = builder.sequence_at_snapshot();
            let Some(data) = self
                .pending_snapshots
                .remove(&partition)
                .and_then(SnapshotBuilder::assemble)
            else {
                tracing::error!(?partition, "failed to assemble snapshot");
                let complete = SnapshotComplete::failed(partition);
                let _ = self
                    .transport
                    .send(from, ClusterMessage::SnapshotComplete(complete))
                    .await;
                return;
            };

            match self.stores.import_partition(&data) {
                Ok(count) => {
                    tracing::info!(
                        ?partition,
                        imported = count,
                        sequence,
                        "snapshot import complete"
                    );

                    if let Err(e) = self
                        .migration_manager
                        .mark_snapshot_complete(partition, sequence)
                    {
                        tracing::debug!(?partition, ?e, "no active migration for snapshot");
                    }

                    let complete = SnapshotComplete::ok(partition, sequence);
                    let _ = self
                        .transport
                        .send(from, ClusterMessage::SnapshotComplete(complete))
                        .await;
                }
                Err(e) => {
                    tracing::error!(?partition, ?e, "failed to import snapshot");
                    let complete = SnapshotComplete::failed(partition);
                    let _ = self
                        .transport
                        .send(from, ClusterMessage::SnapshotComplete(complete))
                        .await;
                }
            }
        }
    }

    fn handle_snapshot_complete(&mut self, from: NodeId, complete: &SnapshotComplete) {
        let Some(partition) = complete.partition() else {
            tracing::warn!(?from, "received snapshot complete with invalid partition");
            return;
        };

        let status = complete.status();
        let final_sequence = complete.final_sequence();

        tracing::info!(
            ?partition,
            from = from.get(),
            ?status,
            final_sequence,
            "received snapshot complete"
        );

        if complete.is_ok() {
            if self.migration_manager.is_sending(partition)
                && let Err(e) = self
                    .migration_manager
                    .advance_phase(partition, MigrationPhase::Overlapping)
            {
                tracing::debug!(?partition, ?e, "could not advance migration phase");
            }
        } else {
            tracing::warn!(?partition, ?status, "snapshot transfer failed");
        }
    }

    pub async fn request_snapshot(&mut self, partition: PartitionId, from: NodeId) {
        tracing::info!(?partition, source = from.get(), "requesting snapshot");

        let request = SnapshotRequest::create(partition, self.node_id);
        let _ = self
            .transport
            .send(from, ClusterMessage::SnapshotRequest(request))
            .await;
    }

    pub async fn start_partition_migration(
        &mut self,
        partition: PartitionId,
        old_primary: NodeId,
        new_primary: NodeId,
        epoch: Epoch,
    ) {
        self.migration_manager.start_migration(
            partition,
            old_primary,
            new_primary,
            epoch,
            self.current_time,
        );

        if new_primary == self.node_id {
            self.request_snapshot(partition, old_primary).await;
        }
    }

    #[must_use]
    pub fn is_migrating(&self, partition: PartitionId) -> bool {
        self.migration_manager.is_migrating(partition)
    }

    #[must_use]
    pub fn migration_phase(&self, partition: PartitionId) -> Option<MigrationPhase> {
        self.migration_manager.get_phase(partition)
    }

    pub fn complete_migration(&mut self, partition: PartitionId) {
        if let Some(state) = self.migration_manager.complete_migration(partition) {
            tracing::info!(
                ?partition,
                old_primary = state.old_primary.get(),
                new_primary = state.new_primary.get(),
                "migration completed"
            );
        }
    }

    #[must_use]
    pub fn migration_manager(&self) -> &MigrationManager {
        &self.migration_manager
    }

    pub async fn set_draining(&mut self, draining: bool) {
        self.draining = draining;
        if draining {
            tracing::info!(node_id = self.node_id.get(), "node entering draining mode");
            let msg = ClusterMessage::DrainNotification {
                node_id: self.node_id,
            };
            let _ = self.transport.broadcast(msg).await;
        } else {
            tracing::info!(node_id = self.node_id.get(), "node exiting draining mode");
        }
    }

    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.draining
    }

    #[must_use]
    pub fn pending_writes_empty(&self) -> bool {
        self.pending.is_empty()
    }

    #[must_use]
    pub fn pending_write_count(&self) -> usize {
        self.pending.len()
    }

    #[must_use]
    pub fn can_shutdown_safely(&self) -> bool {
        self.draining && self.pending.is_empty() && self.outgoing_snapshots.is_empty()
    }

    #[must_use]
    pub fn active_migrations_count(&self) -> usize {
        self.migration_manager.migration_count()
    }

    #[must_use]
    pub fn query_coordinator(&self) -> &QueryCoordinator {
        &self.query_coordinator
    }

    pub fn query_coordinator_mut(&mut self) -> &mut QueryCoordinator {
        &mut self.query_coordinator
    }

    pub fn handle_query_request(
        &self,
        partition: PartitionId,
        request: &QueryRequest,
    ) -> QueryResponse {
        if self.partition_map.primary(partition) != Some(self.node_id) {
            return QueryResponse::error(request.query_id, partition, QueryStatus::NotPrimary);
        }

        let results = self.stores.query_entity(
            &request.entity,
            request.filter.as_deref(),
            request.limit,
            request.cursor.as_deref(),
        );

        match results {
            Ok((data, has_more, cursor)) => {
                QueryResponse::ok(request.query_id, partition, data, has_more, cursor)
            }
            Err(_) => QueryResponse::error(request.query_id, partition, QueryStatus::Error),
        }
    }

    pub fn handle_batch_read_request(&self, request: &BatchReadRequest) -> BatchReadResponse {
        let results = request
            .ids
            .iter()
            .map(|id| {
                let data = self.stores.get_entity(&request.entity, id);
                (id.clone(), data)
            })
            .collect();

        BatchReadResponse::new(request.request_id, request.partition, results)
    }

    async fn handle_json_db_request(
        &mut self,
        from: NodeId,
        partition: PartitionId,
        request: &JsonDbRequest,
    ) {
        let response_payload = match request.op {
            JsonDbOp::Read => {
                let id = request.id.as_deref().unwrap_or("");
                self.handle_json_read_local(&request.entity, id)
            }
            JsonDbOp::Update => {
                let id = request.id.as_deref().unwrap_or("");
                self.handle_json_update_local(&request.entity, id, &request.payload)
                    .await
            }
            JsonDbOp::Delete => {
                let id = request.id.as_deref().unwrap_or("");
                self.handle_json_delete_local(&request.entity, id).await
            }
            JsonDbOp::Create => {
                self.handle_json_create_local(partition, &request.entity, &request.payload)
                    .await
            }
            JsonDbOp::List => self.handle_json_list_local(&request.entity, &request.payload),
        };

        let response = JsonDbResponse::new(
            request.request_id,
            response_payload,
            request.response_topic.clone(),
            request.correlation_data.clone(),
        );

        let _ = self
            .transport
            .send(from, ClusterMessage::JsonDbResponse(response))
            .await;
    }

    async fn handle_json_db_response(&mut self, response: &JsonDbResponse) {
        let scatter_prefix = format!("_mqdb/scatter/{}/", self.node_id.get());

        tracing::debug!(
            response_topic = %response.response_topic,
            scatter_prefix = %scatter_prefix,
            pending_count = self.pending_scatter_requests.len(),
            "received JsonDbResponse"
        );

        if let Some(request_id_str) = response.response_topic.strip_prefix(&scatter_prefix)
            && let Ok(request_id) = request_id_str.parse::<u64>()
        {
            tracing::debug!(request_id, "matched scatter response");
            let items: Vec<serde_json::Value> =
                serde_json::from_slice::<serde_json::Value>(&response.payload)
                    .ok()
                    .and_then(|parsed| parsed.get("data").and_then(|d| d.as_array()).cloned())
                    .unwrap_or_default();
            self.handle_scatter_list_response(request_id, items).await;
            return;
        }

        self.transport
            .queue_local_publish(response.response_topic.clone(), response.payload.clone(), 0)
            .await;
    }

    fn handle_json_read_local(&self, entity: &str, id: &str) -> Vec<u8> {
        match self.stores.db_get(entity, id) {
            Some(db_entity) => {
                let data_json: serde_json::Value =
                    serde_json::from_slice(&db_entity.data).unwrap_or(serde_json::Value::Null);
                let result = serde_json::json!({
                    "status": "ok",
                    "id": db_entity.id_str(),
                    "entity": entity,
                    "data": data_json
                });
                serde_json::to_vec(&result).unwrap_or_default()
            }
            None => Self::json_error(404, &format!("entity not found: {entity} id={id}")),
        }
    }

    async fn handle_json_update_local(
        &mut self,
        entity: &str,
        id: &str,
        payload: &[u8],
    ) -> Vec<u8> {
        let data: serde_json::Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(_) => return Self::json_error(400, "invalid JSON payload"),
        };

        let merged_data = if let Some(existing) = self.db_get(entity, id) {
            let mut existing_data: serde_json::Value = serde_json::from_slice(&existing.data)
                .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

            if let (serde_json::Value::Object(existing_obj), serde_json::Value::Object(updates)) =
                (&mut existing_data, data)
            {
                for (key, value) in updates {
                    existing_obj.insert(key, value);
                }
            }
            existing_data
        } else {
            return Self::json_error(404, &format!("entity not found: {entity} id={id}"));
        };

        let data_bytes = serde_json::to_vec(&merged_data).unwrap_or_default();
        let now_ms = Self::current_time_ms();

        match self.db_update(entity, id, &data_bytes, now_ms).await {
            Ok(db_entity) => {
                let result = serde_json::json!({
                    "status": "ok",
                    "id": db_entity.id_str(),
                    "entity": entity,
                    "data": merged_data
                });
                serde_json::to_vec(&result).unwrap_or_default()
            }
            Err(super::db::DbDataStoreError::NotFound) => {
                Self::json_error(404, &format!("entity not found: {entity} id={id}"))
            }
            Err(_) => Self::json_error(500, "internal error"),
        }
    }

    async fn handle_json_delete_local(&mut self, entity: &str, id: &str) -> Vec<u8> {
        match self.db_delete(entity, id).await {
            Ok(_) => {
                let result = serde_json::json!({
                    "status": "ok",
                    "id": id,
                    "entity": entity,
                    "deleted": true
                });
                serde_json::to_vec(&result).unwrap_or_default()
            }
            Err(super::db::DbDataStoreError::NotFound) => {
                Self::json_error(404, &format!("entity not found: {entity} id={id}"))
            }
            Err(_) => Self::json_error(500, "internal error"),
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_json_create_local(
        &mut self,
        partition: PartitionId,
        entity: &str,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut data: serde_json::Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(_) => return Self::json_error(400, "invalid JSON payload"),
        };

        if let serde_json::Value::Object(ref mut obj) = data
            && let Some(ttl_secs) = obj.get("ttl_secs").and_then(serde_json::Value::as_u64)
        {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let expires_at = now + ttl_secs;
            obj.insert(
                "_expires_at".to_string(),
                serde_json::Value::Number(expires_at.into()),
            );
            obj.remove("ttl_secs");
        }

        let id = self.generate_id_for_partition(entity, partition, payload);
        let request_id = uuid::Uuid::new_v4().to_string();
        let now_ms = Self::current_time_ms();

        let unique_fields = self.stores.constraint_get_unique_fields(entity);
        let mut local_reserved: Vec<(String, Vec<u8>)> = Vec::new();
        let mut remote_reserved: Vec<(String, Vec<u8>, NodeId)> = Vec::new();

        for field in &unique_fields {
            let value = match data.get(field) {
                Some(v) => serde_json::to_vec(v).unwrap_or_default(),
                None => continue,
            };

            let unique_part = super::db::unique_partition(entity, field, &value);
            let primary = self.partition_map.primary(unique_part);

            let is_conflict = if primary == Some(self.node_id) {
                let (result, write) = self.stores.unique_reserve_replicated(
                    entity,
                    field,
                    &value,
                    &id,
                    &request_id,
                    partition,
                    30_000,
                    now_ms,
                );

                match result {
                    super::db::ReserveResult::Reserved => {
                        if let Some(w) = write {
                            self.write_or_forward(w).await;
                        }
                        local_reserved.push((field.clone(), value));
                        false
                    }
                    super::db::ReserveResult::AlreadyReservedBySameRequest => {
                        local_reserved.push((field.clone(), value));
                        false
                    }
                    super::db::ReserveResult::Conflict => true,
                }
            } else if let Some(target_node) = primary {
                let result = self
                    .send_unique_reserve_request(
                        target_node,
                        entity,
                        field,
                        &value,
                        &id,
                        &request_id,
                        partition,
                        30_000,
                    )
                    .await;

                match result {
                    Ok(UniqueReserveStatus::Reserved | UniqueReserveStatus::AlreadyReserved) => {
                        remote_reserved.push((field.clone(), value, target_node));
                        false
                    }
                    Ok(UniqueReserveStatus::Conflict | UniqueReserveStatus::Error) | Err(_) => true,
                }
            } else {
                true
            };

            if is_conflict {
                for (f, v) in &local_reserved {
                    if let Some(w) =
                        self.stores
                            .unique_release_replicated(entity, f, v, &request_id)
                    {
                        self.write_or_forward(w).await;
                    }
                }
                for (f, v, target) in &remote_reserved {
                    let _ = self
                        .send_unique_release_request(*target, entity, f, v, &request_id)
                        .await;
                }
                return Self::json_error(
                    409,
                    &format!("unique constraint violation on field '{field}'"),
                );
            }
        }

        let data_bytes = serde_json::to_vec(&data).unwrap_or_default();

        match self.db_create(entity, &id, &data_bytes, now_ms).await {
            Ok(db_entity) => {
                for (field, value) in &local_reserved {
                    if let Ok((_, w)) =
                        self.stores
                            .unique_commit_replicated(entity, field, value, &request_id)
                    {
                        self.write_or_forward(w).await;
                    }
                }
                for (field, value, target) in &remote_reserved {
                    let _ = self
                        .send_unique_commit_request(*target, entity, field, value, &request_id)
                        .await;
                }

                let result = serde_json::json!({
                    "status": "ok",
                    "id": db_entity.id_str(),
                    "entity": entity,
                    "data": data
                });
                serde_json::to_vec(&result).unwrap_or_default()
            }
            Err(super::db::DbDataStoreError::AlreadyExists) => {
                for (f, v) in &local_reserved {
                    if let Some(w) =
                        self.stores
                            .unique_release_replicated(entity, f, v, &request_id)
                    {
                        self.write_or_forward(w).await;
                    }
                }
                for (f, v, target) in &remote_reserved {
                    let _ = self
                        .send_unique_release_request(*target, entity, f, v, &request_id)
                        .await;
                }
                Self::json_error(409, "entity already exists")
            }
            Err(_) => {
                for (f, v) in &local_reserved {
                    if let Some(w) =
                        self.stores
                            .unique_release_replicated(entity, f, v, &request_id)
                    {
                        self.write_or_forward(w).await;
                    }
                }
                for (f, v, target) in &remote_reserved {
                    let _ = self
                        .send_unique_release_request(*target, entity, f, v, &request_id)
                        .await;
                }
                Self::json_error(500, "internal error")
            }
        }
    }

    fn handle_json_list_local(&self, entity: &str, payload: &[u8]) -> Vec<u8> {
        let filters: Vec<crate::Filter> = if payload.is_empty() {
            Vec::new()
        } else if let Ok(data) = serde_json::from_slice::<serde_json::Value>(payload) {
            data.get("filters")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        let entities = self.db_list(entity);
        let items: Vec<serde_json::Value> = entities
            .iter()
            .filter_map(|e| {
                let data: serde_json::Value = serde_json::from_slice(&e.data).ok()?;
                if Self::matches_filters(&data, &filters) {
                    Some(serde_json::json!({
                        "id": e.id_str(),
                        "data": data
                    }))
                } else {
                    None
                }
            })
            .collect();

        let result = serde_json::json!({
            "status": "ok",
            "data": items
        });
        serde_json::to_vec(&result).unwrap_or_default()
    }

    fn matches_filters(entity: &serde_json::Value, filters: &[crate::Filter]) -> bool {
        for filter in filters {
            if let Some(field_value) = entity.get(&filter.field) {
                if !filter.matches(field_value) {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    fn json_error(code: u16, message: &str) -> Vec<u8> {
        let result = serde_json::json!({
            "status": "error",
            "code": code,
            "message": message
        });
        serde_json::to_vec(&result).unwrap_or_default()
    }

    fn current_time_ms() -> u64 {
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis()),
        )
        .unwrap_or(u64::MAX)
    }

    fn generate_id_for_partition(
        &self,
        entity: &str,
        partition: PartitionId,
        data: &[u8],
    ) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        entity.hash(&mut hasher);
        data.hash(&mut hasher);
        self.node_id.get().hash(&mut hasher);
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
            .hash(&mut hasher);

        let base_id = hasher.finish();

        for suffix in 0..1000_u16 {
            let id = format!("{base_id:016x}-{suffix:04x}");
            if super::db::data_partition(entity, &id) == partition {
                return id;
            }
        }

        format!("{base_id:016x}-p{}", partition.get())
    }

    #[must_use]
    pub fn query_local_retained_exact(&self, topic: &str) -> Option<RetainedMessage> {
        self.stores.retained.get(topic)
    }

    #[must_use]
    pub fn query_local_retained_pattern(&self, pattern: &str) -> Vec<RetainedMessage> {
        self.stores.retained.query_matching_pattern(pattern)
    }

    pub fn start_retained_query(
        &mut self,
        topic_filter: &str,
        timeout_ms: u32,
        now: u64,
    ) -> (u64, Vec<(PartitionId, QueryRequest)>) {
        let is_wildcard = topic_filter.contains('+') || topic_filter.contains('#');

        let partitions = if is_wildcard {
            QueryCoordinator::all_partitions()
        } else {
            vec![super::topic_partition(topic_filter)]
        };

        let filter = if is_wildcard {
            None
        } else {
            Some(format!("topic={topic_filter}"))
        };

        let (query_id, requests) = self.query_coordinator.start_query(
            "retained",
            filter.as_deref(),
            1000,
            None,
            Some(timeout_ms),
            partitions.clone(),
            now,
        );

        let requests_with_partitions: Vec<_> = partitions.into_iter().zip(requests).collect();

        (query_id, requests_with_partitions)
    }

    pub async fn send_retained_query(&self, partition: PartitionId, request: QueryRequest) {
        if let Some(primary) = self.partition_map.primary(partition) {
            if primary == self.node_id {
                return;
            }
            let msg = ClusterMessage::QueryRequest { partition, request };
            let _ = self.transport.send(primary, msg).await;
        }
    }

    #[must_use]
    pub fn is_partition_primary(&self, partition: PartitionId) -> bool {
        self.partition_map.primary(partition) == Some(self.node_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn forward_json_db_request(
        &self,
        partition: PartitionId,
        op: JsonDbOp,
        entity: &str,
        id: Option<&str>,
        payload: &[u8],
        response_topic: &str,
        correlation_data: Option<&[u8]>,
    ) -> bool {
        let Some(primary) = self.partition_map.primary(partition) else {
            tracing::warn!(?partition, "cannot forward JSON request: no primary known");
            return false;
        };

        if primary == self.node_id {
            return false;
        }

        #[allow(clippy::cast_possible_truncation)]
        let request_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);

        let request = JsonDbRequest::new(
            request_id,
            op,
            entity.to_string(),
            id.map(String::from),
            payload.to_vec(),
            response_topic.to_string(),
            correlation_data.map(<[u8]>::to_vec),
        );

        let msg = ClusterMessage::JsonDbRequest { partition, request };
        if let Err(e) = self.transport.send(primary, msg).await {
            tracing::warn!(
                ?partition,
                primary = primary.get(),
                ?e,
                "failed to forward JSON request"
            );
            return false;
        }

        tracing::debug!(
            ?partition,
            primary = primary.get(),
            "forwarded JSON request to partition primary"
        );
        true
    }

    #[must_use]
    pub fn check_query_complete(&mut self, query_id: u64) -> bool {
        !self.query_coordinator.has_pending(query_id)
    }

    pub fn check_query_timeouts(&mut self, now: u64) -> Vec<super::query_coordinator::QueryResult> {
        self.query_coordinator.check_timeouts(now)
    }

    pub async fn start_async_retained_query(
        &mut self,
        topic: &str,
    ) -> Option<oneshot::Receiver<Vec<RetainedMessage>>> {
        let partition = super::topic_partition(topic);
        let primary = self.partition_map.primary(partition)?;

        if primary == self.node_id {
            return None;
        }

        let query_id = self.current_time;
        let request = QueryRequest::new(
            query_id,
            5000,
            entity::RETAINED.to_string(),
            Some(format!("topic={topic}")),
            10,
            None,
        );

        let (tx, rx) = oneshot::channel();
        self.pending_retained_queries.insert(query_id, tx);

        let msg = ClusterMessage::QueryRequest { partition, request };
        if self.transport.send(primary, msg).await.is_err() {
            self.pending_retained_queries.remove(&query_id);
            return None;
        }

        tracing::debug!(
            topic,
            ?partition,
            ?primary,
            query_id,
            "started async retained query"
        );

        Some(rx)
    }

    pub fn complete_retained_query(&mut self, query_id: u64, results: Vec<RetainedMessage>) {
        if let Some(sender) = self.pending_retained_queries.remove(&query_id) {
            let _ = sender.send(results);
        }
    }

    #[allow(clippy::missing_panics_doc)]
    pub async fn start_scatter_list_query(
        &mut self,
        entity: &str,
        payload: &[u8],
        client_response_topic: String,
        filters: Vec<crate::Filter>,
    ) -> bool {
        let alive_nodes = self.heartbeat.alive_nodes();
        let remote_nodes: Vec<NodeId> = alive_nodes
            .into_iter()
            .filter(|&n| n != self.node_id)
            .collect();

        if remote_nodes.is_empty() {
            return false;
        }

        let sorts: Vec<crate::SortOrder> = if payload.is_empty() {
            Vec::new()
        } else if let Ok(data) = serde_json::from_slice::<serde_json::Value>(payload) {
            data.get("sort")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        #[allow(clippy::cast_possible_truncation)]
        let request_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);

        let local_results = self.handle_json_list_local(entity, payload);
        let local_items: Vec<serde_json::Value> =
            if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&local_results) {
                parsed
                    .get("data")
                    .and_then(|d| d.as_array())
                    .cloned()
                    .unwrap_or_default()
            } else {
                Vec::new()
            };

        let pending = PendingScatterRequest {
            expected_count: remote_nodes.len(),
            received: local_items,
            client_response_topic,
            created_at_ms: self.current_time,
            filters,
            sorts,
        };
        self.pending_scatter_requests.insert(request_id, pending);

        let scatter_response_topic = format!("_mqdb/scatter/{}/{request_id}", self.node_id.get());

        for &target_node in &remote_nodes {
            let request = JsonDbRequest::new(
                request_id,
                JsonDbOp::List,
                entity.to_string(),
                None,
                payload.to_vec(),
                scatter_response_topic.clone(),
                None,
            );

            let msg = ClusterMessage::JsonDbRequest {
                partition: PartitionId::new(0).expect("partition 0 is always valid"),
                request,
            };

            if let Err(e) = self.transport.send(target_node, msg).await {
                tracing::warn!(?target_node, ?e, "failed to send scatter LIST request");
            }
        }

        tracing::debug!(
            request_id,
            entity,
            remote_count = remote_nodes.len(),
            "started scatter LIST query"
        );

        true
    }

    pub async fn handle_scatter_list_response(
        &mut self,
        request_id: u64,
        items: Vec<serde_json::Value>,
    ) {
        let Some(pending) = self.pending_scatter_requests.get_mut(&request_id) else {
            tracing::debug!(request_id, "scatter response for unknown request");
            return;
        };

        pending.received.extend(items);
        pending.expected_count = pending.expected_count.saturating_sub(1);

        if pending.expected_count == 0
            && let Some(completed) = self.pending_scatter_requests.remove(&request_id)
        {
            let mut seen_ids = std::collections::HashSet::new();
            let deduped: Vec<serde_json::Value> = completed
                .received
                .into_iter()
                .filter(|item| {
                    if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                        seen_ids.insert(id.to_string())
                    } else {
                        true
                    }
                })
                .collect();

            let mut filtered: Vec<serde_json::Value> = deduped
                .into_iter()
                .filter(|item| {
                    if let Some(data) = item.get("data") {
                        Self::matches_filters(data, &completed.filters)
                    } else {
                        false
                    }
                })
                .collect();

            Self::sort_scatter_results(&mut filtered, &completed.sorts);

            let result = serde_json::json!({
                "status": "ok",
                "data": filtered
            });
            let payload = serde_json::to_vec(&result).unwrap_or_default();

            self.transport
                .queue_local_publish(completed.client_response_topic, payload, 0)
                .await;
        }
    }

    fn sort_scatter_results(results: &mut [serde_json::Value], sorts: &[crate::SortOrder]) {
        if sorts.is_empty() {
            return;
        }

        results.sort_by(|a, b| {
            let a_data = a.get("data");
            let b_data = b.get("data");

            for order in sorts {
                let a_val = a_data.and_then(|d| d.get(&order.field));
                let b_val = b_data.and_then(|d| d.get(&order.field));

                let cmp = match (a_val, b_val) {
                    (Some(av), Some(bv)) => Self::compare_json_values(av, bv),
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (None, None) => std::cmp::Ordering::Equal,
                };

                let cmp = match order.direction {
                    crate::SortDirection::Asc => cmp,
                    crate::SortDirection::Desc => cmp.reverse(),
                };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    fn compare_json_values(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
        use serde_json::Value;
        match (a, b) {
            (Value::Number(a_num), Value::Number(b_num)) => {
                let a_f64 = a_num.as_f64().unwrap_or(0.0);
                let b_f64 = b_num.as_f64().unwrap_or(0.0);
                a_f64
                    .partial_cmp(&b_f64)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
            (Value::String(a_str), Value::String(b_str)) => a_str.cmp(b_str),
            (Value::Bool(a_bool), Value::Bool(b_bool)) => a_bool.cmp(b_bool),
            _ => std::cmp::Ordering::Equal,
        }
    }

    fn parse_retained_query_response(response: &QueryResponse) -> Vec<RetainedMessage> {
        if !response.status.is_ok() || response.results.is_empty() {
            return Vec::new();
        }

        let mut messages = Vec::new();
        let mut offset = 0;
        while offset < response.results.len() {
            match RetainedMessage::try_from_be_bytes(&response.results[offset..]) {
                Ok((msg, consumed)) => {
                    messages.push(msg);
                    offset += consumed;
                }
                Err(_) => break,
            }
        }
        messages
    }

    pub async fn sync_retained_to_broker(&self, write: &ReplicationWrite) {
        if write.entity != entity::RETAINED {
            return;
        }

        if write.operation == Operation::Delete {
            let topic = write.id.clone();
            tracing::debug!(topic, "clearing retained message from local broker");
            self.transport
                .queue_local_publish_retained(topic, Vec::new(), 0)
                .await;
            return;
        }

        if let Ok((msg, _)) = RetainedMessage::try_from_be_bytes(&write.data) {
            let topic = msg.topic_str().to_string();
            let payload = msg.payload.clone();
            let qos = msg.qos;

            if let Some(ref synced_topics) = self.synced_retained_topics {
                let synced = synced_topics.clone();
                let topic_clone = topic.clone();
                tokio::spawn(async move {
                    synced.write().await.insert(topic_clone);
                });
            }

            tracing::debug!(
                topic,
                qos,
                payload_len = payload.len(),
                "syncing retained message to local broker"
            );
            self.transport
                .queue_local_publish_retained(topic, payload, qos)
                .await;
        }
    }

    async fn handle_unique_reserve_request(&mut self, from: NodeId, req: &UniqueReserveRequest) {
        let entity = req.entity_str();
        let field = req.field_str();
        let record_id = req.record_id_str();
        let idempotency_key = req.idempotency_key_str();

        let status = if let Some(data_partition) = req.data_partition() {
            let result = self.stores.unique_reserve(
                entity,
                field,
                &req.value,
                record_id,
                idempotency_key,
                data_partition,
                req.ttl_ms,
                Self::current_time_ms(),
            );
            match result {
                super::db::ReserveResult::Reserved => UniqueReserveStatus::Reserved,
                super::db::ReserveResult::AlreadyReservedBySameRequest => {
                    UniqueReserveStatus::AlreadyReserved
                }
                super::db::ReserveResult::Conflict => UniqueReserveStatus::Conflict,
            }
        } else {
            UniqueReserveStatus::Error
        };

        let response = UniqueReserveResponse::create(req.request_id, status);
        let _ = self
            .transport
            .send(from, ClusterMessage::UniqueReserveResponse(response))
            .await;
    }

    fn handle_unique_reserve_response(&mut self, resp: &UniqueReserveResponse) {
        if let Some(tx) = self.pending_unique_requests.remove(&resp.request_id) {
            let _ = tx.send(resp.status());
        }
    }

    async fn handle_unique_commit_request(&mut self, from: NodeId, req: &UniqueCommitRequest) {
        let entity = req.entity_str();
        let field = req.field_str();
        let idempotency_key = req.idempotency_key_str();

        let success = self
            .stores
            .unique_commit(entity, field, &req.value, idempotency_key);

        let response = UniqueCommitResponse::create(req.request_id, success);
        let _ = self
            .transport
            .send(from, ClusterMessage::UniqueCommitResponse(response))
            .await;
    }

    fn handle_unique_commit_response(&mut self, resp: &UniqueCommitResponse) {
        if let Some(tx) = self.pending_unique_commit_requests.remove(&resp.request_id) {
            let _ = tx.send(resp.is_success());
        }
    }

    async fn handle_unique_release_request(&mut self, from: NodeId, req: &UniqueReleaseRequest) {
        let entity = req.entity_str();
        let field = req.field_str();
        let idempotency_key = req.idempotency_key_str();

        let success = self
            .stores
            .unique_release(entity, field, &req.value, idempotency_key);

        let response = UniqueReleaseResponse::create(req.request_id, success);
        let _ = self
            .transport
            .send(from, ClusterMessage::UniqueReleaseResponse(response))
            .await;
    }

    fn handle_unique_release_response(&mut self, resp: &UniqueReleaseResponse) {
        if let Some(tx) = self
            .pending_unique_release_requests
            .remove(&resp.request_id)
        {
            let _ = tx.send(resp.is_success());
        }
    }

    fn allocate_unique_request_id(&mut self) -> u64 {
        let id = self.next_unique_request_id;
        self.next_unique_request_id += 1;
        id
    }

    #[allow(clippy::too_many_arguments, clippy::missing_errors_doc)]
    pub async fn send_unique_reserve_request(
        &mut self,
        target_node: NodeId,
        entity: &str,
        field: &str,
        value: &[u8],
        record_id: &str,
        idempotency_key: &str,
        data_partition: PartitionId,
        ttl_ms: u64,
    ) -> Result<UniqueReserveStatus, super::transport::TransportError> {
        let request_id = self.allocate_unique_request_id();
        let (tx, mut rx) = oneshot::channel();

        self.pending_unique_requests.insert(request_id, tx);

        let request = UniqueReserveRequest::create(
            request_id,
            entity,
            field,
            value,
            record_id,
            idempotency_key,
            data_partition,
            ttl_ms,
        );

        self.transport
            .send(target_node, ClusterMessage::UniqueReserveRequest(request))
            .await?;

        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_secs(UNIQUE_REQUEST_TIMEOUT_SECS);

        loop {
            match rx.try_recv() {
                Ok(status) => return Ok(status),
                Err(oneshot::error::TryRecvError::Closed) => return Ok(UniqueReserveStatus::Error),
                Err(oneshot::error::TryRecvError::Empty) => {}
            }

            let mut requeue_msgs = Vec::new();
            while let Some(msg) = self.transport.recv() {
                if let ClusterMessage::UniqueReserveResponse(ref resp) = msg.message {
                    self.handle_unique_reserve_response(resp);
                } else if let ClusterMessage::UniqueCommitResponse(ref resp) = msg.message {
                    self.handle_unique_commit_response(resp);
                } else if let ClusterMessage::UniqueReleaseResponse(ref resp) = msg.message {
                    self.handle_unique_release_response(resp);
                } else {
                    requeue_msgs.push(msg);
                }
            }
            for msg in requeue_msgs.into_iter().rev() {
                self.transport.requeue(msg);
            }

            if tokio::time::Instant::now() >= deadline {
                self.pending_unique_requests.remove(&request_id);
                return Ok(UniqueReserveStatus::Error);
            }

            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn send_unique_commit_request(
        &mut self,
        target_node: NodeId,
        entity: &str,
        field: &str,
        value: &[u8],
        idempotency_key: &str,
    ) -> Result<bool, super::transport::TransportError> {
        let request_id = self.allocate_unique_request_id();
        let (tx, mut rx) = oneshot::channel();

        self.pending_unique_commit_requests.insert(request_id, tx);

        let request =
            UniqueCommitRequest::create(request_id, entity, field, value, idempotency_key);

        self.transport
            .send(target_node, ClusterMessage::UniqueCommitRequest(request))
            .await?;

        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_secs(UNIQUE_REQUEST_TIMEOUT_SECS);

        loop {
            match rx.try_recv() {
                Ok(success) => return Ok(success),
                Err(oneshot::error::TryRecvError::Closed) => return Ok(false),
                Err(oneshot::error::TryRecvError::Empty) => {}
            }

            let mut requeue_msgs = Vec::new();
            while let Some(msg) = self.transport.recv() {
                if let ClusterMessage::UniqueReserveResponse(ref resp) = msg.message {
                    self.handle_unique_reserve_response(resp);
                } else if let ClusterMessage::UniqueCommitResponse(ref resp) = msg.message {
                    self.handle_unique_commit_response(resp);
                } else if let ClusterMessage::UniqueReleaseResponse(ref resp) = msg.message {
                    self.handle_unique_release_response(resp);
                } else {
                    requeue_msgs.push(msg);
                }
            }
            for msg in requeue_msgs.into_iter().rev() {
                self.transport.requeue(msg);
            }

            if tokio::time::Instant::now() >= deadline {
                self.pending_unique_commit_requests.remove(&request_id);
                return Ok(false);
            }

            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn send_unique_release_request(
        &mut self,
        target_node: NodeId,
        entity: &str,
        field: &str,
        value: &[u8],
        idempotency_key: &str,
    ) -> Result<bool, super::transport::TransportError> {
        let request_id = self.allocate_unique_request_id();
        let (tx, mut rx) = oneshot::channel();

        self.pending_unique_release_requests.insert(request_id, tx);

        let request =
            UniqueReleaseRequest::create(request_id, entity, field, value, idempotency_key);

        self.transport
            .send(target_node, ClusterMessage::UniqueReleaseRequest(request))
            .await?;

        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_secs(UNIQUE_REQUEST_TIMEOUT_SECS);

        loop {
            match rx.try_recv() {
                Ok(success) => return Ok(success),
                Err(oneshot::error::TryRecvError::Closed) => return Ok(false),
                Err(oneshot::error::TryRecvError::Empty) => {}
            }

            let mut requeue_msgs = Vec::new();
            while let Some(msg) = self.transport.recv() {
                if let ClusterMessage::UniqueReserveResponse(ref resp) = msg.message {
                    self.handle_unique_reserve_response(resp);
                } else if let ClusterMessage::UniqueCommitResponse(ref resp) = msg.message {
                    self.handle_unique_commit_response(resp);
                } else if let ClusterMessage::UniqueReleaseResponse(ref resp) = msg.message {
                    self.handle_unique_release_response(resp);
                } else {
                    requeue_msgs.push(msg);
                }
            }
            for msg in requeue_msgs.into_iter().rev() {
                self.transport.requeue(msg);
            }

            if tokio::time::Instant::now() >= deadline {
                self.pending_unique_release_requests.remove(&request_id);
                return Ok(false);
            }

            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
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

        async fn send(
            &self,
            to: NodeId,
            message: ClusterMessage,
        ) -> Result<(), super::super::transport::TransportError> {
            self.outbox.lock().unwrap().push((to, message));
            Ok(())
        }

        async fn broadcast(
            &self,
            message: ClusterMessage,
        ) -> Result<(), super::super::transport::TransportError> {
            self.outbox
                .lock()
                .unwrap()
                .push((NodeId::validated(0).unwrap_or(self.node_id), message));
            Ok(())
        }

        async fn send_to_partition_primary(
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

        fn requeue(&self, msg: InboundMessage) {
            self.inbox.lock().unwrap().push_front(msg);
        }

        async fn queue_local_publish(&self, _topic: String, _payload: Vec<u8>, _qos: u8) {}

        async fn queue_local_publish_retained(&self, _topic: String, _payload: Vec<u8>, _qos: u8) {}
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

    #[tokio::test]
    async fn replicate_write_sends_to_replicas() {
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

        let seq = ctrl
            .replicate_write(write, &[node2, node3], 1)
            .await
            .unwrap();
        assert_eq!(seq, 1);

        let sent = ctrl.transport.sent_messages();
        assert_eq!(sent.len(), 2);
    }

    #[tokio::test]
    async fn receive_heartbeat_marks_alive() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        ctrl.register_peer(node2);
        assert_eq!(ctrl.node_status(node2), NodeStatus::Unknown);

        let hb = Heartbeat::create(node2, 1000);
        ctrl.transport
            .inject_message(node2, ClusterMessage::Heartbeat(hb));
        ctrl.process_messages().await;

        assert_eq!(ctrl.node_status(node2), NodeStatus::Alive);
    }

    #[tokio::test]
    async fn handle_write_as_replica() {
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
        ctrl.process_messages().await;

        assert_eq!(ctrl.sequence(partition), Some(1));

        let sent = ctrl.transport.sent_messages();
        assert_eq!(sent.len(), 1);
        match &sent[0].1 {
            ClusterMessage::Ack(ack) => assert!(ack.is_ok()),
            _ => panic!("expected ack"),
        }
    }

    #[tokio::test]
    async fn create_session_quorum_requires_primary() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let result = ctrl.create_session_quorum("client1").await;
        assert!(matches!(result, Err(ReplicationError::NotPrimary)));
    }

    #[tokio::test]
    async fn create_session_quorum_succeeds_as_primary() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let partition = session_partition("client1");
        ctrl.become_primary(partition, Epoch::new(1));

        let (session, _rx) = ctrl.create_session_quorum("client1").await.unwrap();
        assert_eq!(session.client_id_str(), "client1");
        assert!(ctrl.stores().sessions.get("client1").is_some());
    }

    #[tokio::test]
    async fn replicate_write_async_sends_without_tracking() {
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

        let seq = ctrl
            .replicate_write_async(write, &[node2, node3])
            .await
            .unwrap();
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

    #[tokio::test]
    async fn broadcast_wildcard_writes_sends_to_replicas() {
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

        ctrl.broadcast_wildcard_writes(writes).await;

        let sent = ctrl.transport.sent_messages();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, node2);
    }

    #[tokio::test]
    async fn create_session_quorum_signals_completion_on_acks() {
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

        let (session, mut rx) = ctrl
            .create_session_quorum("quorum-test-client")
            .await
            .unwrap();
        assert_eq!(session.client_id_str(), "quorum-test-client");

        assert!(rx.try_recv().is_err());

        let ack = ReplicationAck::ok(partition, Epoch::new(1), 1, node2);
        ctrl.handle_message(InboundMessage {
            from: node2,
            message: ClusterMessage::Ack(ack),
            received_at: 0,
        })
        .await;

        let result = rx.try_recv().unwrap();
        assert_eq!(result, QuorumResult::Success);
    }

    #[tokio::test]
    async fn primary_has_local_data_after_replicate_write() {
        use crate::cluster::entity::SESSIONS;
        use bebytes::BeBytes;

        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let partition = PartitionId::new(0).unwrap();
        ctrl.become_primary(partition, Epoch::new(1));

        let session_data = crate::cluster::session::SessionData::create("test-client", node1);
        let write = ReplicationWrite::new(
            partition,
            Operation::Insert,
            Epoch::new(1),
            0,
            SESSIONS.to_string(),
            "test-client".to_string(),
            session_data.to_be_bytes(),
        );

        let seq = ctrl.replicate_write(write, &[node2], 1).await.unwrap();
        assert_eq!(seq, 1);

        let session = ctrl.stores().sessions.get("test-client");
        assert!(
            session.is_some(),
            "Primary should have session locally after write"
        );
        assert_eq!(session.unwrap().client_id_str(), "test-client");

        assert!(
            ctrl.write_log().can_catchup(partition, 1),
            "Write log should contain the write for catchup"
        );
    }

    #[tokio::test]
    async fn primary_has_local_data_after_replicate_write_async() {
        use crate::cluster::entity::SESSIONS;
        use bebytes::BeBytes;

        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();

        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let partition = PartitionId::new(0).unwrap();
        ctrl.become_primary(partition, Epoch::new(1));

        let session_data = crate::cluster::session::SessionData::create("async-client", node1);
        let write = ReplicationWrite::new(
            partition,
            Operation::Insert,
            Epoch::new(1),
            0,
            SESSIONS.to_string(),
            "async-client".to_string(),
            session_data.to_be_bytes(),
        );

        let seq = ctrl.replicate_write_async(write, &[node2]).await.unwrap();
        assert_eq!(seq, 1);

        let session = ctrl.stores().sessions.get("async-client");
        assert!(
            session.is_some(),
            "Primary should have session locally after async write"
        );

        assert!(
            ctrl.write_log().can_catchup(partition, 1),
            "Write log should contain the async write for catchup"
        );
    }

    #[tokio::test]
    async fn commit_offset_stores_locally_and_replicates() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();
        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let consumer_id = "test-consumer";
        let consumer_partition = session_partition(consumer_id);
        ctrl.become_primary(consumer_partition, Epoch::new(1));
        ctrl.register_peer(node2);

        let mut map = PartitionMap::new();
        map.set(
            consumer_partition,
            crate::cluster::PartitionAssignment {
                primary: Some(node1),
                replicas: vec![node2],
                epoch: Epoch::new(1),
            },
        );
        ctrl.update_partition_map(map);

        let data_partition = PartitionId::new(5).unwrap();
        let offset = ctrl
            .commit_offset(consumer_id, data_partition, 12345, 1000)
            .await;

        assert_eq!(offset.sequence, 12345);
        assert_eq!(offset.timestamp, 1000);
        assert_eq!(offset.consumer_id_str(), consumer_id);

        let stored = ctrl.get_offset(consumer_id, data_partition);
        assert_eq!(stored, Some(12345));

        let sent = ctrl.transport.sent_messages();
        assert!(!sent.is_empty(), "Should have sent replication messages");
    }

    #[tokio::test]
    async fn idempotency_check_and_commit() {
        use super::super::idempotency_store::IdempotencyCheck;

        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let partition = PartitionId::new(0).unwrap();
        ctrl.become_primary(partition, Epoch::new(1));

        let idem_key = "req-123";
        let entity = "users";
        let id = "user-1";
        let timestamp = 1000;

        let check1 = ctrl.check_idempotency(idem_key, partition, entity, id, timestamp);
        assert!(
            matches!(check1, Ok(IdempotencyCheck::Proceed)),
            "First check should return Proceed"
        );

        let check2 = ctrl.check_idempotency(idem_key, partition, entity, id, timestamp);
        assert!(
            check2.is_err(),
            "Second check while processing should error"
        );

        let response = b"success response";
        ctrl.commit_idempotency(partition, idem_key, response).await;

        let check3 = ctrl.check_idempotency(idem_key, partition, entity, id, timestamp + 100);
        match check3 {
            Ok(IdempotencyCheck::ReturnCached(cached)) => {
                assert_eq!(cached, response.to_vec());
            }
            other => panic!("Expected ReturnCached, got {other:?}"),
        }
    }

    #[test]
    fn idempotency_rollback() {
        use super::super::idempotency_store::IdempotencyCheck;

        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let partition = PartitionId::new(0).unwrap();

        let idem_key = "req-456";
        let entity = "orders";
        let id = "order-1";
        let timestamp = 2000;

        let check1 = ctrl.check_idempotency(idem_key, partition, entity, id, timestamp);
        assert!(matches!(check1, Ok(IdempotencyCheck::Proceed)));

        ctrl.rollback_idempotency(partition, idem_key);

        let check2 = ctrl.check_idempotency(idem_key, partition, entity, id, timestamp);
        assert!(
            matches!(check2, Ok(IdempotencyCheck::Proceed)),
            "After rollback, should be able to retry"
        );
    }

    #[tokio::test]
    async fn db_create_stores_entity_and_replicates() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();
        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let partition = crate::cluster::db::data_partition("users", "123");
        ctrl.become_primary(partition, Epoch::new(1));
        ctrl.register_peer(node2);

        let mut map = PartitionMap::new();
        map.set(
            partition,
            crate::cluster::PartitionAssignment {
                primary: Some(node1),
                replicas: vec![node2],
                epoch: Epoch::new(1),
            },
        );
        ctrl.update_partition_map(map);

        let result = ctrl
            .db_create("users", "123", b"{\"name\":\"Alice\"}", 1000)
            .await;
        assert!(result.is_ok());

        let entity = result.unwrap();
        assert_eq!(entity.entity_str(), "users");
        assert_eq!(entity.id_str(), "123");
        assert_eq!(entity.data, b"{\"name\":\"Alice\"}");

        let fetched = ctrl.db_get("users", "123");
        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().id_str(), "123");

        let sent = ctrl.transport.sent_messages();
        assert!(!sent.is_empty(), "Should replicate to peer");
    }

    #[tokio::test]
    async fn db_create_fails_if_exists() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let partition = crate::cluster::db::data_partition("users", "456");
        ctrl.become_primary(partition, Epoch::new(1));

        let result1 = ctrl.db_create("users", "456", b"{}", 1000).await;
        assert!(result1.is_ok());

        let result2 = ctrl.db_create("users", "456", b"{}", 2000).await;
        assert!(result2.is_err());
    }

    #[tokio::test]
    async fn db_update_modifies_existing_entity() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let partition = crate::cluster::db::data_partition("users", "789");
        ctrl.become_primary(partition, Epoch::new(1));

        ctrl.db_create("users", "789", b"{\"name\":\"Alice\"}", 1000)
            .await
            .unwrap();
        let updated = ctrl
            .db_update("users", "789", b"{\"name\":\"Bob\"}", 2000)
            .await
            .unwrap();

        assert_eq!(updated.data, b"{\"name\":\"Bob\"}");
        assert_eq!(updated.timestamp_ms, 2000);
    }

    #[tokio::test]
    async fn db_delete_removes_entity() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let partition = crate::cluster::db::data_partition("users", "del1");
        ctrl.become_primary(partition, Epoch::new(1));

        ctrl.db_create("users", "del1", b"{}", 1000).await.unwrap();
        assert!(ctrl.db_get("users", "del1").is_some());

        ctrl.db_delete("users", "del1").await.unwrap();
        assert!(ctrl.db_get("users", "del1").is_none());
    }

    #[tokio::test]
    async fn db_upsert_creates_or_updates() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        let partition = crate::cluster::db::data_partition("products", "p1");
        ctrl.become_primary(partition, Epoch::new(1));

        let entity1 = ctrl
            .db_upsert("products", "p1", b"{\"price\":100}", 1000)
            .await;
        assert_eq!(entity1.data, b"{\"price\":100}");

        let entity2 = ctrl
            .db_upsert("products", "p1", b"{\"price\":200}", 2000)
            .await;
        assert_eq!(entity2.data, b"{\"price\":200}");
        assert_eq!(entity2.timestamp_ms, 2000);
    }

    #[tokio::test]
    async fn db_list_returns_all_entities_of_type() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        for i in 0..3 {
            let id = format!("item{i}");
            let partition = crate::cluster::db::data_partition("items", &id);
            ctrl.become_primary(partition, Epoch::new(1));
        }

        ctrl.db_create("items", "item0", b"{}", 1000).await.unwrap();
        ctrl.db_create("items", "item1", b"{}", 1000).await.unwrap();
        ctrl.db_create("items", "item2", b"{}", 1000).await.unwrap();

        let items = ctrl.db_list("items");
        assert_eq!(items.len(), 3);
    }

    #[tokio::test]
    async fn schema_register_broadcasts_to_alive_nodes() {
        let node1 = NodeId::validated(1).unwrap();
        let node2 = NodeId::validated(2).unwrap();
        let node3 = NodeId::validated(3).unwrap();

        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        ctrl.heartbeat
            .receive_heartbeat(node2, &super::super::Heartbeat::create(node2, 0), 0);
        ctrl.heartbeat
            .receive_heartbeat(node3, &super::super::Heartbeat::create(node3, 0), 0);

        let schema = ctrl
            .schema_register("users", b"{\"fields\":[]}")
            .await
            .unwrap();
        assert_eq!(schema.entity_str(), "users");

        let sent = ctrl.transport.sent_messages();
        assert_eq!(sent.len(), 2);

        let targets: Vec<_> = sent.iter().map(|(n, _)| n.get()).collect();
        assert!(targets.contains(&2));
        assert!(targets.contains(&3));
    }

    #[tokio::test]
    async fn schema_register_duplicate_fails() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        ctrl.schema_register("users", b"{}").await.unwrap();
        let result = ctrl.schema_register("users", b"{}").await;

        assert!(matches!(
            result,
            Err(super::super::db::SchemaStoreError::AlreadyExists)
        ));
    }

    #[tokio::test]
    async fn schema_update_increments_version() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        ctrl.schema_register("users", b"{v1}").await.unwrap();
        let updated = ctrl.schema_update("users", b"{v2}").await.unwrap();

        assert_eq!(updated.schema_version, 2);
    }

    #[tokio::test]
    async fn schema_get_and_list() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        ctrl.schema_register("users", b"{}").await.unwrap();
        ctrl.schema_register("orders", b"{}").await.unwrap();

        assert!(ctrl.schema_get("users").is_some());
        assert!(ctrl.schema_get("products").is_none());
        assert_eq!(ctrl.schema_list().len(), 2);
    }

    #[tokio::test]
    async fn schema_is_valid_for_write_checks_active_state() {
        let node1 = NodeId::validated(1).unwrap();
        let transport = MockTransport::new(node1);
        let mut ctrl = NodeController::new(node1, transport, TransportConfig::default());

        assert!(!ctrl.schema_is_valid_for_write("users"));
        ctrl.schema_register("users", b"{}").await.unwrap();
        assert!(ctrl.schema_is_valid_for_write("users"));
    }
}
