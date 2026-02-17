// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod broadcast;
mod catchup;
mod db_ops;
mod query;
mod replication_ops;
mod retained;
mod session_ops;
mod snapshot;
pub(crate) mod unique;

#[cfg(test)]
mod tests;

use super::entity;
use super::heartbeat::{HeartbeatManager, NodeStatus};
use super::migration::{MigrationManager, MigrationPhase};
use super::protocol::{
    BatchReadRequest, CatchupRequest, ForwardedPublish, JsonDbOp, JsonDbRequest, JsonDbResponse,
    QueryRequest, QueryResponse, ReplicationWrite, UniqueCommitRequest, UniqueCommitResponse,
    UniqueReleaseRequest, UniqueReleaseResponse, UniqueReserveRequest, UniqueReserveResponse,
    UniqueReserveStatus,
};
use super::query_coordinator::QueryCoordinator;
use super::quorum::PendingWrites;
use super::raft::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use super::raft_task::RaftEvent;
use super::replication::{ReplicaRole, ReplicaState};
use super::retained_store::RetainedMessage;
use super::snapshot::{SnapshotBuilder, SnapshotRequest, SnapshotSender};
use super::store_manager::StoreManager;
use super::transport::{ClusterMessage, ClusterTransport, InboundMessage, TransportConfig};
use super::write_log::PartitionWriteLog;
use super::{Epoch, NodeId, PartitionId, PartitionMap};
use crate::storage::StorageBackend;
use crate::types::OwnershipConfig;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;
use tokio::sync::oneshot;

use super::SessionData;
use super::db;
use super::protocol;
use super::query_coordinator;
use super::replication;
use super::topic_partition;

static WRITE_COUNTER: AtomicU64 = AtomicU64::new(0);
static WRITE_REQUEST_SENT: AtomicU64 = AtomicU64::new(0);
static WRITE_RECEIVED: AtomicU64 = AtomicU64::new(0);

const FORWARD_DEDUP_CAPACITY: usize = 1000;
pub(crate) const MAX_LIST_RESULTS: usize = 10_000;
pub(crate) const MAX_FILTERS: usize = 16;
pub(crate) const MAX_SORT_FIELDS: usize = 4;

#[derive(Default)]
pub struct TickOutput {
    pub heartbeat: Option<ClusterMessage>,
    pub catchup_requests: Vec<(NodeId, ClusterMessage)>,
    pub local_publishes: Vec<(String, Vec<u8>)>,
}

pub struct PendingScatterRequest {
    pub expected_count: usize,
    pub received: Vec<serde_json::Value>,
    pub client_response_topic: String,
    pub created_at_ms: u64,
    pub filters: Vec<crate::Filter>,
    pub sorts: Vec<crate::SortOrder>,
}

struct UniqueReservationParams<'a> {
    entity: &'a str,
    field: &'a str,
    value: &'a [u8],
    id: &'a str,
    request_id: &'a str,
    partition: PartitionId,
    now_ms: u64,
}

pub struct PendingUniqueReserve {
    pub field: String,
    pub value: Vec<u8>,
    pub target_node: NodeId,
    pub receiver: oneshot::Receiver<UniqueReserveStatus>,
}

pub struct UniqueCheckPhase1Result {
    pub local_reserved: Vec<(String, Vec<u8>)>,
    pub pending_remote: Vec<PendingUniqueReserve>,
}

pub struct PendingUniqueWork {
    pub phase1: UniqueCheckPhase1Result,
    pub continuation: UniqueCheckContinuation,
}

pub enum UniqueCheckContinuation {
    CreateFromDbHandler {
        entity: String,
        id: String,
        data: serde_json::Value,
        data_bytes: Vec<u8>,
        partition: PartitionId,
        request_id: String,
        now_ms: u64,
        sender: Option<String>,
        client_id: Option<String>,
        response_topic: String,
        correlation_data: Option<Vec<u8>>,
    },
    UpdateFromDbHandler {
        entity: String,
        id: String,
        merged_data: serde_json::Value,
        data_bytes: Vec<u8>,
        partition: PartitionId,
        request_id: String,
        now_ms: u64,
        new_diff: serde_json::Value,
        old_diff: serde_json::Value,
        sender: Option<String>,
        client_id: Option<String>,
        response_topic: String,
        correlation_data: Option<Vec<u8>>,
    },
    CreateFromNodeController {
        from: NodeId,
        entity: String,
        id: String,
        data: serde_json::Value,
        data_bytes: Vec<u8>,
        partition: PartitionId,
        request_id: String,
        now_ms: u64,
        response_topic: String,
        correlation_data: Option<Vec<u8>>,
    },
    UpdateFromNodeController {
        from: NodeId,
        entity: String,
        id: String,
        merged_data: serde_json::Value,
        data_bytes: Vec<u8>,
        partition: PartitionId,
        request_id: String,
        now_ms: u64,
        new_diff: serde_json::Value,
        old_diff: serde_json::Value,
        response_topic: String,
        correlation_data: Option<Vec<u8>>,
    },
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
    pub(super) node_id: NodeId,
    pub(super) transport: T,
    pub(super) heartbeat: HeartbeatManager,
    pub(super) replicas: HashMap<u16, ReplicaState>,
    pub(super) pending: PendingWrites,
    pub(super) partition_map: PartitionMap,
    pub(super) current_time: u64,
    pub(super) stores: StoreManager,
    pub(super) write_log: PartitionWriteLog,
    pub(super) forward_dedup: HashSet<u64>,
    pub(super) forward_dedup_order: VecDeque<u64>,
    pub(super) tx_raft_messages: flume::Sender<RaftMessage>,
    pub(super) tx_raft_events: flume::Sender<RaftEvent>,
    pub(super) dead_nodes_for_session_update: VecDeque<NodeId>,
    pub(super) migration_manager: MigrationManager,
    pub(super) pending_snapshots: HashMap<PartitionId, SnapshotBuilder>,
    pub(super) requested_snapshots: HashSet<PartitionId>,
    pub(super) outgoing_snapshots: HashMap<(NodeId, PartitionId), SnapshotSender>,
    pub(super) draining: bool,
    pub(super) query_coordinator: QueryCoordinator,
    pub(super) synced_retained_topics: Option<Arc<tokio::sync::RwLock<HashMap<String, Instant>>>>,
    pub(super) pending_retained_queries: HashMap<u64, oneshot::Sender<Vec<RetainedMessage>>>,
    pub(super) pending_scatter_requests: HashMap<u64, PendingScatterRequest>,
    pub(super) pending_unique_requests: HashMap<u64, oneshot::Sender<UniqueReserveStatus>>,
    pub(super) next_unique_request_id: u64,
    pub(super) ownership: Arc<OwnershipConfig>,
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
    pub fn new(
        node_id: NodeId,
        transport: T,
        config: TransportConfig,
        tx_raft_messages: flume::Sender<RaftMessage>,
        tx_raft_events: flume::Sender<RaftEvent>,
    ) -> Self {
        Self::new_with_storage(
            node_id,
            transport,
            config,
            None,
            tx_raft_messages,
            tx_raft_events,
        )
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_storage(
        node_id: NodeId,
        transport: T,
        config: TransportConfig,
        storage: Option<Arc<dyn StorageBackend>>,
        tx_raft_messages: flume::Sender<RaftMessage>,
        tx_raft_events: flume::Sender<RaftEvent>,
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
            tx_raft_messages,
            tx_raft_events,
            dead_nodes_for_session_update: VecDeque::new(),
            migration_manager: MigrationManager::new(node_id),
            pending_snapshots: HashMap::new(),
            requested_snapshots: HashSet::new(),
            outgoing_snapshots: HashMap::new(),
            draining: false,
            query_coordinator: QueryCoordinator::new(node_id),
            synced_retained_topics: None,
            pending_retained_queries: HashMap::new(),
            pending_scatter_requests: HashMap::new(),
            pending_unique_requests: HashMap::new(),
            next_unique_request_id: 1,
            ownership: Arc::new(OwnershipConfig::default()),
        }
    }

    pub fn set_ownership(&mut self, ownership: Arc<OwnershipConfig>) {
        self.ownership = ownership;
    }

    pub fn set_synced_retained_topics(
        &mut self,
        topics: Arc<tokio::sync::RwLock<HashMap<String, Instant>>>,
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
        self.partition_map
            .replicas(partition)
            .contains(&self.node_id)
    }

    /// Pick a local partition for creating new entities (round-robin).
    ///
    /// # Panics
    /// Panics if no valid partition can be created.
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
        PartitionId::ZERO
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

    pub async fn become_replica_with_snapshot(
        &mut self,
        partition: PartitionId,
        epoch: Epoch,
        primary: NodeId,
    ) {
        let state = self
            .replicas
            .entry(partition.get())
            .or_insert_with(|| ReplicaState::new(partition, self.node_id));

        state.become_awaiting_snapshot(epoch);

        tracing::info!(
            ?partition,
            epoch = epoch.get(),
            source = primary.get(),
            "becoming replica, requesting initial snapshot"
        );

        self.request_snapshot(partition, primary).await;
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

    pub fn tick(&mut self, now: u64) -> TickOutput {
        let start = std::time::Instant::now();

        self.current_time = now;
        let mut output = TickOutput::default();

        if self.heartbeat.should_send(now) {
            output.heartbeat = Some(self.heartbeat.create_heartbeat(now));
        }

        let dead = self.heartbeat.check_timeouts(now);
        for node in dead {
            self.handle_node_death(node);
        }

        self.collect_catchup_requests(now, &mut output);
        self.collect_stale_scatter_responses(now, &mut output);

        let elapsed = start.elapsed();
        if elapsed.as_micros() > 1000 {
            tracing::warn!(
                elapsed_us = elapsed.as_micros(),
                replicas = self.replicas.len(),
                "slow node_controller tick"
            );
        }

        output
    }

    pub async fn send_tick_output(&self, output: TickOutput) {
        if let Some(hb) = output.heartbeat {
            let _ = self.transport.broadcast(hb).await;
        }
        for (target, msg) in output.catchup_requests {
            let _ = self.transport.send(target, msg).await;
        }
        for (topic, payload) in output.local_publishes {
            self.transport.queue_local_publish(topic, payload, 0).await;
        }
    }

    fn collect_stale_scatter_responses(&mut self, now: u64, output: &mut TickOutput) {
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

                let mut filtered: Vec<serde_json::Value> = deduped
                    .into_iter()
                    .filter(|item| {
                        if let Some(data) = item.get("data") {
                            Self::matches_filters(data, &pending.filters)
                        } else {
                            false
                        }
                    })
                    .collect();
                filtered.truncate(MAX_LIST_RESULTS);

                let result = serde_json::json!({
                    "status": "ok",
                    "data": filtered,
                    "partial": true
                });
                let payload = serde_json::to_vec(&result).unwrap_or_default();

                output
                    .local_publishes
                    .push((pending.client_response_topic, payload));
            }
        }
    }

    fn collect_catchup_requests(&mut self, now: u64, output: &mut TickOutput) {
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

            tracing::info!(
                ?partition,
                from_seq,
                to_seq,
                ?primary,
                current_seq = state.sequence(),
                epoch = state.epoch().get(),
                pending_writes = state.pending_count(),
                "requesting catchup"
            );

            let req = CatchupRequest::create(partition, from_seq, to_seq, self.node_id);
            output
                .catchup_requests
                .push((primary, ClusterMessage::CatchupRequest(req)));

            state.mark_catchup_requested(now);
        }

        self.collect_awaiting_snapshot_requests(output);
    }

    fn collect_awaiting_snapshot_requests(&mut self, output: &mut TickOutput) {
        for (&partition_id, state) in &self.replicas {
            if state.role() != ReplicaRole::AwaitingSnapshot {
                continue;
            }

            let Some(partition) = PartitionId::new(partition_id) else {
                continue;
            };

            if self.requested_snapshots.contains(&partition)
                || self.pending_snapshots.contains_key(&partition)
            {
                continue;
            }

            let Some(primary) = self.partition_map.get(partition).primary else {
                continue;
            };

            if primary == self.node_id {
                continue;
            }

            tracing::info!(
                ?partition,
                ?primary,
                "retrying snapshot request for awaiting replica"
            );

            self.requested_snapshots.insert(partition);
            let req = SnapshotRequest::create(partition, self.node_id);
            output
                .catchup_requests
                .push((primary, ClusterMessage::SnapshotRequest(req)));
        }
    }

    fn handle_node_death(&mut self, dead_node: NodeId) {
        let start = std::time::Instant::now();

        tracing::warn!(?dead_node, "node death detected");
        let _ = self.tx_raft_events.try_send(RaftEvent::NodeDead(dead_node));
        self.dead_nodes_for_session_update.push_back(dead_node);

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

        let elapsed = start.elapsed();
        if elapsed.as_micros() > 500 {
            tracing::warn!(
                elapsed_us = elapsed.as_micros(),
                dead_node = dead_node.get(),
                "slow handle_node_death"
            );
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    pub async fn process_messages(&mut self) {
        const BATCH_SIZE: u32 = 8;
        let queue_depth_start = self.transport.pending_count();
        let start = std::time::Instant::now();
        let mut count = 0u32;
        let mut heartbeat_count = 0u32;
        let mut write_count = 0u32;
        let mut ack_count = 0u32;
        let mut raft_count = 0u32;
        let mut other_count = 0u32;

        while let Some(msg) = self.transport.recv() {
            match &msg.message {
                ClusterMessage::Heartbeat(_) => heartbeat_count += 1,
                ClusterMessage::Write(_) | ClusterMessage::WriteRequest(_) => write_count += 1,
                ClusterMessage::Ack(_) => ack_count += 1,
                ClusterMessage::RequestVote(_)
                | ClusterMessage::RequestVoteResponse(_)
                | ClusterMessage::AppendEntries(_)
                | ClusterMessage::AppendEntriesResponse(_) => raft_count += 1,
                _ => other_count += 1,
            }
            self.handle_message(msg).await;
            count += 1;
            if count.is_multiple_of(BATCH_SIZE) {
                tokio::task::yield_now().await;
            }
        }
        if count > 0 {
            let elapsed_us = start.elapsed().as_micros() as u64;
            tracing::info!(
                node = self.node_id.get(),
                msg_count = count,
                queue_depth_start,
                heartbeat_count,
                write_count,
                ack_count,
                raft_count,
                other_count,
                elapsed_us,
                "process_messages_timing"
            );
        }
    }

    pub fn drain_dead_nodes_for_session_update(&mut self) -> impl Iterator<Item = NodeId> + '_ {
        self.dead_nodes_for_session_update.drain(..)
    }

    pub fn apply_dead_nodes(&mut self, dead_nodes: &[NodeId]) {
        for &node in dead_nodes {
            self.heartbeat.handle_death_notice(node);
            self.handle_node_death(node);
        }
    }

    pub fn apply_heartbeat_updates(
        &mut self,
        updates: &[super::message_processor::HeartbeatUpdate],
    ) {
        for update in updates {
            self.heartbeat
                .receive_heartbeat(update.from, &update.heartbeat, update.received_at);
        }
    }

    pub async fn handle_filtered_message(
        &mut self,
        msg: InboundMessage,
    ) -> Option<PendingUniqueWork> {
        match msg.message {
            ClusterMessage::Heartbeat(_)
            | ClusterMessage::RequestVote(_)
            | ClusterMessage::RequestVoteResponse(_)
            | ClusterMessage::AppendEntries(_)
            | ClusterMessage::AppendEntriesResponse(_) => None,

            ClusterMessage::Write(ref write) => {
                self.handle_write(msg.from, write).await;
                None
            }
            ClusterMessage::WriteRequest(write) => {
                self.handle_write_request(msg.from, write).await;
                None
            }
            ClusterMessage::Ack(ack) => {
                self.handle_ack(ack).await;
                None
            }
            ClusterMessage::DeathNotice { node_id } => {
                self.heartbeat.handle_death_notice(node_id);
                self.handle_node_death(node_id);
                None
            }
            ClusterMessage::DrainNotification { node_id } => {
                tracing::info!(?node_id, "received drain notification");
                let _ = self
                    .tx_raft_events
                    .send(RaftEvent::DrainNotification(node_id));
                None
            }
            ClusterMessage::CatchupRequest(req) => {
                self.handle_catchup_request(
                    msg.from,
                    req.partition(),
                    req.from_sequence(),
                    req.to_sequence(),
                )
                .await;
                None
            }
            ClusterMessage::CatchupResponse(resp) => {
                self.handle_catchup_response(resp).await;
                None
            }
            ClusterMessage::ForwardedPublish(ref fwd) => {
                self.handle_forwarded_publish_no_dedup(msg.from, fwd).await;
                None
            }
            ref data_plane => self.dispatch_data_plane_message(msg.from, data_plane).await,
        }
    }

    async fn handle_partition_update_received(&mut self, update: &super::raft::PartitionUpdate) {
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
                let dominated = self.replicas.get(&partition.get()).is_some_and(|s| {
                    matches!(
                        s.role(),
                        ReplicaRole::Replica | ReplicaRole::AwaitingSnapshot
                    ) && s.epoch() >= epoch
                });
                if !dominated {
                    self.become_replica_with_snapshot(partition, epoch, primary)
                        .await;
                }
            } else {
                self.step_down(partition);
            }
        }

        self.partition_map.apply_update(update);
        self.heartbeat.partition_map_mut().apply_update(update);
        let _ = self
            .tx_raft_events
            .try_send(RaftEvent::ExternalUpdate(*update));
        tracing::info!(
            partition = update.partition,
            primary = update.primary,
            "received partition update from cluster"
        );
    }

    fn handle_query_response_received(&mut self, response: &QueryResponse) {
        if self
            .pending_retained_queries
            .contains_key(&response.query_id)
        {
            let results = Self::parse_retained_query_response(response);
            self.complete_retained_query(response.query_id, results);
        } else if let Some(result) = self.query_coordinator.receive_response(response.clone()) {
            tracing::debug!(
                query_id = result.query_id,
                partial = result.partial,
                "query completed"
            );
        }
    }

    async fn handle_query_request_and_respond(
        &mut self,
        from: NodeId,
        partition: PartitionId,
        request: &QueryRequest,
    ) {
        let response = self.handle_query_request(partition, request);
        let response_msg = ClusterMessage::QueryResponse(response);
        let _ = self.transport.send(from, response_msg).await;
    }

    async fn handle_batch_read_and_respond(&mut self, from: NodeId, request: &BatchReadRequest) {
        let response = self.handle_batch_read_request(request);
        let response_msg = ClusterMessage::BatchReadResponse(response);
        let _ = self.transport.send(from, response_msg).await;
    }

    async fn handle_forwarded_publish_no_dedup(&mut self, from: NodeId, fwd: &ForwardedPublish) {
        tracing::debug!(
            local_node = ?self.node_id,
            origin = ?fwd.origin_node,
            topic = %fwd.topic,
            qos = fwd.qos,
            retain = fwd.retain,
            payload_len = fwd.payload.len(),
            target_count = fwd.targets.len(),
            ?from,
            "received forwarded publish (processor mode)"
        );

        self.transport
            .queue_local_publish(fwd.topic.clone(), fwd.payload.clone(), fwd.qos)
            .await;
    }

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
                let _ = self
                    .tx_raft_events
                    .try_send(RaftEvent::DrainNotification(node_id));
            }
            ClusterMessage::RequestVote(req) => {
                self.forward_raft_vote(msg.from, req);
            }
            ClusterMessage::RequestVoteResponse(resp) => {
                self.forward_raft_vote_response(msg.from, resp);
            }
            ClusterMessage::AppendEntries(req) => {
                self.forward_raft_append(msg.from, req);
            }
            ClusterMessage::AppendEntriesResponse(resp) => {
                self.forward_raft_append_response(msg.from, resp);
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
            ref data_plane => {
                self.dispatch_data_plane_message(msg.from, data_plane).await;
            }
        }
    }

    fn forward_raft_vote(&self, from: NodeId, request: super::raft::RequestVoteRequest) {
        let _ = self
            .tx_raft_messages
            .try_send(RaftMessage::RequestVote { from, request });
    }

    fn forward_raft_vote_response(&self, from: NodeId, response: super::raft::RequestVoteResponse) {
        let _ = self
            .tx_raft_messages
            .try_send(RaftMessage::RequestVoteResponse { from, response });
    }

    fn forward_raft_append(&self, from: NodeId, request: super::raft::AppendEntriesRequest) {
        let _ = self
            .tx_raft_messages
            .try_send(RaftMessage::AppendEntries { from, request });
    }

    fn forward_raft_append_response(
        &self,
        from: NodeId,
        response: super::raft::AppendEntriesResponse,
    ) {
        let _ = self
            .tx_raft_messages
            .try_send(RaftMessage::AppendEntriesResponse { from, response });
    }

    async fn dispatch_data_plane_message(
        &mut self,
        from: NodeId,
        message: &ClusterMessage,
    ) -> Option<PendingUniqueWork> {
        match message {
            ClusterMessage::SnapshotRequest(req) => {
                self.handle_snapshot_request(from, req).await;
            }
            ClusterMessage::SnapshotChunk(chunk) => {
                self.handle_snapshot_chunk(from, chunk).await;
            }
            ClusterMessage::SnapshotComplete(complete) => {
                self.handle_snapshot_complete(from, complete);
            }
            ClusterMessage::QueryRequest { partition, request } => {
                self.handle_query_request_and_respond(from, *partition, request)
                    .await;
            }
            ClusterMessage::QueryResponse(response) => {
                self.handle_query_response_received(response);
            }
            ClusterMessage::BatchReadRequest(request) => {
                self.handle_batch_read_and_respond(from, request).await;
            }
            ClusterMessage::WildcardBroadcast(broadcast) => {
                self.handle_wildcard_broadcast(from, broadcast);
            }
            ClusterMessage::TopicSubscriptionBroadcast(broadcast) => {
                self.handle_topic_subscription_broadcast(from, broadcast);
            }
            ClusterMessage::PartitionUpdate(update) => {
                self.handle_partition_update_received(update).await;
            }
            ClusterMessage::JsonDbRequest { partition, request } => {
                return self.handle_json_db_request(from, *partition, request).await;
            }
            ClusterMessage::JsonDbResponse(response) => {
                self.handle_json_db_response(response).await;
            }
            ClusterMessage::UniqueReserveRequest(req) => {
                self.handle_unique_reserve_request(from, req).await;
            }
            ClusterMessage::UniqueReserveResponse(resp) => {
                self.handle_unique_reserve_response(resp);
            }
            ClusterMessage::UniqueCommitRequest(req) => {
                self.handle_unique_commit_request(from, req).await;
            }
            ClusterMessage::UniqueReleaseRequest(req) => {
                self.handle_unique_release_request(from, req).await;
            }
            _ => {}
        }
        None
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

    #[allow(clippy::too_many_lines, clippy::type_complexity)]
    pub async fn complete_pending_unique_work(
        &mut self,
        local_reserved: Vec<(String, Vec<u8>)>,
        remote_results: Result<
            Vec<(String, Vec<u8>, NodeId)>,
            (String, Vec<(String, Vec<u8>, NodeId)>),
        >,
        continuation: UniqueCheckContinuation,
    ) {
        match continuation {
            UniqueCheckContinuation::CreateFromNodeController {
                from,
                entity,
                id,
                data,
                data_bytes,
                partition,
                request_id,
                now_ms,
                response_topic,
                correlation_data,
            } => {
                let response_payload = match remote_results {
                    Err((conflict_field, confirmed)) => {
                        self.release_unique_check_reservations(
                            &entity,
                            &request_id,
                            &local_reserved,
                            &confirmed,
                        )
                        .await;
                        Self::json_error(
                            409,
                            &format!("unique constraint violation on field '{conflict_field}'"),
                        )
                    }
                    Ok(confirmed_remotes) => {
                        match self.db_create(&entity, &id, &data_bytes, now_ms).await {
                            Ok(db_entity) => {
                                self.commit_unique_constraints(
                                    &entity,
                                    &id,
                                    &data,
                                    partition,
                                    &request_id,
                                    now_ms,
                                )
                                .await;
                                let result = serde_json::json!({
                                    "status": "ok",
                                    "id": db_entity.id_str(),
                                    "entity": entity,
                                    "data": data
                                });
                                serde_json::to_vec(&result).unwrap_or_default()
                            }
                            Err(super::db::DbDataStoreError::AlreadyExists) => {
                                self.release_unique_check_reservations(
                                    &entity,
                                    &request_id,
                                    &local_reserved,
                                    &confirmed_remotes,
                                )
                                .await;
                                Self::json_error(409, "entity already exists")
                            }
                            Err(_) => {
                                self.release_unique_check_reservations(
                                    &entity,
                                    &request_id,
                                    &local_reserved,
                                    &confirmed_remotes,
                                )
                                .await;
                                Self::json_error(500, "internal error")
                            }
                        }
                    }
                };

                let response =
                    JsonDbResponse::new(0, response_payload, response_topic, correlation_data);
                let _ = self
                    .transport
                    .send(from, ClusterMessage::JsonDbResponse(response))
                    .await;
            }
            UniqueCheckContinuation::UpdateFromNodeController {
                from,
                entity,
                id,
                merged_data,
                data_bytes,
                partition,
                request_id,
                now_ms,
                new_diff,
                old_diff,
                response_topic,
                correlation_data,
            } => {
                let response_payload = match remote_results {
                    Err((conflict_field, confirmed)) => {
                        self.release_unique_check_reservations(
                            &entity,
                            &request_id,
                            &local_reserved,
                            &confirmed,
                        )
                        .await;
                        Self::json_error(
                            409,
                            &format!("unique constraint violation on field '{conflict_field}'"),
                        )
                    }
                    Ok(confirmed_remotes) => {
                        match self.db_update(&entity, &id, &data_bytes, now_ms).await {
                            Ok(db_entity) => {
                                self.commit_unique_constraints(
                                    &entity,
                                    &id,
                                    &new_diff,
                                    partition,
                                    &request_id,
                                    now_ms,
                                )
                                .await;
                                self.release_unique_constraints(
                                    &entity, &id, &old_diff, partition, &id, now_ms,
                                )
                                .await;
                                let result = serde_json::json!({
                                    "status": "ok",
                                    "id": db_entity.id_str(),
                                    "entity": entity,
                                    "data": merged_data
                                });
                                serde_json::to_vec(&result).unwrap_or_default()
                            }
                            Err(super::db::DbDataStoreError::NotFound) => {
                                self.release_unique_check_reservations(
                                    &entity,
                                    &request_id,
                                    &local_reserved,
                                    &confirmed_remotes,
                                )
                                .await;
                                Self::json_error(
                                    404,
                                    &format!("entity not found: {entity} id={id}"),
                                )
                            }
                            Err(_) => {
                                self.release_unique_check_reservations(
                                    &entity,
                                    &request_id,
                                    &local_reserved,
                                    &confirmed_remotes,
                                )
                                .await;
                                Self::json_error(500, "internal error")
                            }
                        }
                    }
                };

                let response =
                    JsonDbResponse::new(0, response_payload, response_topic, correlation_data);
                let _ = self
                    .transport
                    .send(from, ClusterMessage::JsonDbResponse(response))
                    .await;
            }
            UniqueCheckContinuation::CreateFromDbHandler { .. }
            | UniqueCheckContinuation::UpdateFromDbHandler { .. } => {}
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
