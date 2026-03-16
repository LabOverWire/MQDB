// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod client_location;
mod cursor;
pub mod db;
mod db_handler;
pub mod db_protocol;
mod db_topic;
mod dedicated_executor;
pub mod entity;
mod event_handler;
mod heartbeat;
mod idempotency_store;
mod inflight_store;
mod lwt;
mod message_processor;
mod migration;
#[cfg(feature = "mqtt-bridge")]
mod mqtt_transport;
pub(crate) mod node_controller;
mod offset_store;
mod partition_storage;
mod protocol;
mod publish_router;
mod qos2_store;
mod query_coordinator;
mod quic_transport;
mod quorum;
pub mod raft;
mod raft_task;
mod rebalance_coordinator;
mod rebalancer;
mod replication;
mod retained_store;
mod session;
mod snapshot;
mod store_manager;
pub(crate) mod store_utils;
mod subscription_cache;
mod topic_index;
mod topic_trie;
mod transport;
mod wildcard_pending;
mod wildcard_store;
mod write_log;

pub use mqdb_core::partition::{
    Epoch, NUM_PARTITIONS, NodeId, PartitionAssignment, PartitionId, PartitionMap, PartitionRole,
};

pub use client_location::{
    ClientLocationEntry, ClientLocationError, ClientLocationStore, client_location_key,
};
pub use cursor::{PartitionCursor, ScatterCursor};
pub use db::{DbDataStore, DbDataStoreError, DbEntity, data_partition, db_data_key};
pub use db_handler::{DbPublishResponse, DbRequestHandler};
pub use db_protocol::{
    DbReadRequest, DbResponse, DbStatus, DbWriteRequest, FkValidateRequest, FkValidateResponse,
    FkValidateStatus, IndexUpdateRequest,
};
pub use db_topic::{DbTopicOperation, ParsedDbTopic};
pub use dedicated_executor::DedicatedExecutor;
pub use event_handler::ClusterEventHandler;
pub use heartbeat::{HeartbeatManager, NodeStatus};
pub use idempotency_store::{
    IdempotencyCheck, IdempotencyError, IdempotencyRecord, IdempotencyStatus, IdempotencyStore,
    idempotency_storage_key,
};
pub use inflight_store::{InflightMessage, InflightStore, InflightStoreError, inflight_key};
pub use lwt::{
    LwtAction, LwtError, LwtPrepared, LwtPublisher, determine_lwt_action, generate_lwt_token,
};
pub use message_processor::{HeartbeatUpdate, MessageProcessor, ProcessingBatch};
pub use migration::{
    MigrationCheckpoint, MigrationError, MigrationManager, MigrationPhase, MigrationState,
};
#[cfg(feature = "mqtt-bridge")]
#[allow(deprecated)]
pub use mqtt_transport::MqttTransport;
pub use node_controller::{NodeController, RaftMessage, TickOutput};
pub use offset_store::{ConsumerOffset, OffsetStore, OffsetStoreError, offset_key};
pub use partition_storage::PartitionStorage;
pub use protocol::{
    AckStatus, BatchReadRequest, BatchReadResponse, CatchupRequest, CatchupResponse, ForwardTarget,
    ForwardedPublish, Heartbeat, JsonDbOp, JsonDbRequest, JsonDbResponse, MessageType, Operation,
    QueryRequest, QueryResponse, QueryStatus, ReplicationAck, ReplicationWrite,
    TopicSubscriptionBroadcast, UniqueCommitRequest, UniqueCommitResponse, UniqueReleaseRequest,
    UniqueReleaseResponse, UniqueReserveRequest, UniqueReserveResponse, UniqueReserveStatus,
    WildcardBroadcast, WildcardOp,
};
pub use publish_router::{PublishRouteResult, PublishRouter, RoutingTarget, effective_qos};
pub use qos2_store::{
    Qos2Direction, Qos2Phase, Qos2State, Qos2Store, Qos2StoreError, qos2_state_key,
};
pub use query_coordinator::{QueryCoordinator, QueryResult};
pub use quic_transport::{LocalPublishRequest, QuicDirectTransport};
pub use quorum::{PendingWrites, QuorumResult, QuorumTracker};
pub use raft_task::{RaftAdminCommand, RaftEvent, RaftStatus, RaftTask};
pub use rebalance_coordinator::{
    RebalanceAck, RebalanceCommit, RebalanceCoordinator, RebalanceError, RebalanceProposal,
    RebalanceState,
};
pub use rebalancer::{
    PartitionReassignment, RebalanceConfig, compute_balanced_assignments,
    compute_incremental_assignments, compute_removal_assignments,
};
pub use replication::{ReplicaRole, ReplicaState, ReplicationError};
pub use retained_store::{
    RetainedMessage, RetainedStore, RetainedStoreError, retained_message_key,
};
pub use session::{SessionData, SessionError, SessionStore, session_key, session_partition};
pub use snapshot::{
    SnapshotBuilder, SnapshotChunk, SnapshotComplete, SnapshotRequest, SnapshotSender,
    SnapshotStatus,
};
pub use store_manager::outbox::{CascadePendingEntry, CascadeRemoteOp, ClusterOutbox};
pub use store_manager::{RecoveryStats, StoreApplyError, StoreManager};
pub use subscription_cache::{
    MqttSubscriptionSnapshot, MqttTopicEntry, ReconciliationResult,
    SUBSCRIPTION_RECONCILIATION_INTERVAL_MS, SubscriptionCache, SubscriptionCacheError,
    mqtt_subscription_key,
};
pub use topic_index::{
    SubscriberLocation, TopicIndex, TopicIndexEntry, TopicIndexError, topic_index_key,
    topic_partition,
};
pub use topic_trie::{
    SubscriptionType, TopicTrie, WildcardSubscriber, is_wildcard_pattern, validate_pattern,
};
pub use transport::{
    ClusterMessage, ClusterTransport, InboundMessage, TransportConfig, TransportError,
};
pub use wildcard_pending::{
    PendingWildcard, WILDCARD_RECONCILIATION_INTERVAL_MS, WildcardPendingStore,
};
pub use wildcard_store::{WildcardEntry, WildcardStore, WildcardStoreError, wildcard_key};
pub use write_log::PartitionWriteLog;

pub trait PartitionMapExt {
    fn apply_update(&mut self, update: &raft::PartitionUpdate) -> bool;
}

impl PartitionMapExt for PartitionMap {
    fn apply_update(&mut self, update: &raft::PartitionUpdate) -> bool {
        let Some(partition) = PartitionId::new(u16::from(update.partition)) else {
            return false;
        };
        let Some(primary) = NodeId::validated(update.primary) else {
            return false;
        };

        let mut replicas = Vec::new();
        if let Some(r1) = NodeId::validated(update.replica1) {
            replicas.push(r1);
        }
        if let Some(r2) = NodeId::validated(update.replica2) {
            replicas.push(r2);
        }

        let epoch = Epoch::new(u64::from(update.epoch));
        self.set(
            partition,
            PartitionAssignment::new(primary, replicas, epoch),
        );
        true
    }
}
