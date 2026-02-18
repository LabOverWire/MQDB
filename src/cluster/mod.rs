// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod partition_map;
mod types;

#[cfg(feature = "agent")]
mod client_location;
#[cfg(feature = "agent")]
mod cursor;
#[cfg(feature = "agent")]
pub mod db;
#[cfg(feature = "agent")]
mod db_handler;
#[cfg(feature = "agent")]
pub mod db_protocol;
#[cfg(feature = "agent")]
mod db_topic;
#[cfg(feature = "agent")]
mod dedicated_executor;
#[cfg(feature = "agent")]
pub mod entity;
#[cfg(feature = "agent")]
mod event_handler;
#[cfg(feature = "agent")]
mod heartbeat;
#[cfg(feature = "agent")]
mod idempotency_store;
#[cfg(feature = "agent")]
mod inflight_store;
#[cfg(feature = "agent")]
mod lwt;
#[cfg(feature = "agent")]
mod message_processor;
#[cfg(feature = "agent")]
mod migration;
#[cfg(feature = "mqtt-bridge")]
mod mqtt_transport;
#[cfg(feature = "agent")]
pub(crate) mod node_controller;
#[cfg(feature = "agent")]
mod offset_store;
#[cfg(feature = "agent")]
mod partition_storage;
#[cfg(feature = "agent")]
mod protocol;
#[cfg(feature = "agent")]
mod publish_router;
#[cfg(feature = "agent")]
mod qos2_store;
#[cfg(feature = "agent")]
mod query_coordinator;
#[cfg(feature = "cluster")]
mod quic_transport;
#[cfg(feature = "agent")]
mod quorum;
#[cfg(feature = "agent")]
pub mod raft;
#[cfg(feature = "agent")]
mod raft_task;
#[cfg(feature = "agent")]
mod rebalance_coordinator;
#[cfg(feature = "agent")]
mod rebalancer;
#[cfg(feature = "agent")]
mod replication;
#[cfg(feature = "agent")]
mod retained_store;
#[cfg(feature = "agent")]
mod session;
#[cfg(feature = "agent")]
mod snapshot;
#[cfg(feature = "agent")]
mod store_manager;
#[cfg(feature = "agent")]
pub(crate) mod store_utils;
#[cfg(feature = "agent")]
mod subscription_cache;
#[cfg(feature = "agent")]
mod topic_index;
#[cfg(feature = "agent")]
mod topic_trie;
#[cfg(feature = "agent")]
mod transport;
#[cfg(feature = "agent")]
mod wildcard_pending;
#[cfg(feature = "agent")]
mod wildcard_store;
#[cfg(feature = "agent")]
mod write_log;

pub use partition_map::{PartitionAssignment, PartitionMap, PartitionRole};
pub use types::{Epoch, NUM_PARTITIONS, NodeId, PartitionId};

#[cfg(feature = "agent")]
pub use client_location::{
    ClientLocationEntry, ClientLocationError, ClientLocationStore, client_location_key,
};
#[cfg(feature = "agent")]
pub use cursor::{PartitionCursor, ScatterCursor};
#[cfg(feature = "agent")]
pub use db::{DbDataStore, DbDataStoreError, DbEntity, data_partition, db_data_key};
#[cfg(feature = "agent")]
pub use db_handler::{DbPublishResponse, DbRequestHandler};
#[cfg(feature = "agent")]
pub use db_protocol::{
    DbReadRequest, DbResponse, DbStatus, DbWriteRequest, FkValidateRequest, FkValidateResponse,
    FkValidateStatus, IndexUpdateRequest,
};
#[cfg(feature = "agent")]
pub use db_topic::{DbTopicOperation, ParsedDbTopic};
#[cfg(feature = "agent")]
pub use dedicated_executor::DedicatedExecutor;
#[cfg(feature = "agent")]
pub use event_handler::ClusterEventHandler;
#[cfg(feature = "agent")]
pub use heartbeat::{HeartbeatManager, NodeStatus};
#[cfg(feature = "agent")]
pub use idempotency_store::{
    IdempotencyCheck, IdempotencyError, IdempotencyRecord, IdempotencyStatus, IdempotencyStore,
    idempotency_storage_key,
};
#[cfg(feature = "agent")]
pub use inflight_store::{InflightMessage, InflightStore, InflightStoreError, inflight_key};
#[cfg(feature = "agent")]
pub use lwt::{
    LwtAction, LwtError, LwtPrepared, LwtPublisher, determine_lwt_action, generate_lwt_token,
};
#[cfg(feature = "agent")]
pub use message_processor::{HeartbeatUpdate, MessageProcessor, ProcessingBatch};
#[cfg(feature = "agent")]
pub use migration::{
    MigrationCheckpoint, MigrationError, MigrationManager, MigrationPhase, MigrationState,
};
#[cfg(feature = "mqtt-bridge")]
#[allow(deprecated)]
pub use mqtt_transport::MqttTransport;
#[cfg(feature = "agent")]
pub use node_controller::{NodeController, RaftMessage, TickOutput};
#[cfg(feature = "agent")]
pub use offset_store::{ConsumerOffset, OffsetStore, OffsetStoreError, offset_key};
#[cfg(feature = "agent")]
pub use partition_storage::PartitionStorage;
#[cfg(feature = "agent")]
pub use protocol::{
    AckStatus, BatchReadRequest, BatchReadResponse, CatchupRequest, CatchupResponse, ForwardTarget,
    ForwardedPublish, Heartbeat, JsonDbOp, JsonDbRequest, JsonDbResponse, MessageType, Operation,
    QueryRequest, QueryResponse, QueryStatus, ReplicationAck, ReplicationWrite,
    TopicSubscriptionBroadcast, UniqueCommitRequest, UniqueCommitResponse, UniqueReleaseRequest,
    UniqueReleaseResponse, UniqueReserveRequest, UniqueReserveResponse, UniqueReserveStatus,
    WildcardBroadcast, WildcardOp,
};
#[cfg(feature = "agent")]
pub use publish_router::{PublishRouteResult, PublishRouter, RoutingTarget, effective_qos};
#[cfg(feature = "agent")]
pub use qos2_store::{
    Qos2Direction, Qos2Phase, Qos2State, Qos2Store, Qos2StoreError, qos2_state_key,
};
#[cfg(feature = "agent")]
pub use query_coordinator::{QueryCoordinator, QueryResult};
#[cfg(feature = "cluster")]
pub use quic_transport::{LocalPublishRequest, QuicDirectTransport};
#[cfg(feature = "agent")]
pub use quorum::{PendingWrites, QuorumResult, QuorumTracker};
#[cfg(feature = "agent")]
pub use raft_task::{RaftAdminCommand, RaftEvent, RaftStatus, RaftTask};
#[cfg(feature = "agent")]
pub use rebalance_coordinator::{
    RebalanceAck, RebalanceCommit, RebalanceCoordinator, RebalanceError, RebalanceProposal,
    RebalanceState,
};
#[cfg(feature = "agent")]
pub use rebalancer::{
    PartitionReassignment, RebalanceConfig, compute_balanced_assignments,
    compute_incremental_assignments, compute_removal_assignments,
};
#[cfg(feature = "agent")]
pub use replication::{ReplicaRole, ReplicaState, ReplicationError};
#[cfg(feature = "agent")]
pub use retained_store::{
    RetainedMessage, RetainedStore, RetainedStoreError, retained_message_key,
};
#[cfg(feature = "agent")]
pub use session::{SessionData, SessionError, SessionStore, session_key, session_partition};
#[cfg(feature = "agent")]
pub use snapshot::{
    SnapshotBuilder, SnapshotChunk, SnapshotComplete, SnapshotRequest, SnapshotSender,
    SnapshotStatus,
};
#[cfg(feature = "agent")]
pub use store_manager::outbox::CascadeRemoteOp;
#[cfg(feature = "agent")]
pub use store_manager::{RecoveryStats, StoreApplyError, StoreManager};
#[cfg(feature = "agent")]
pub use subscription_cache::{
    MqttSubscriptionSnapshot, MqttTopicEntry, ReconciliationResult,
    SUBSCRIPTION_RECONCILIATION_INTERVAL_MS, SubscriptionCache, SubscriptionCacheError,
    mqtt_subscription_key,
};
#[cfg(feature = "agent")]
pub use topic_index::{
    SubscriberLocation, TopicIndex, TopicIndexEntry, TopicIndexError, topic_index_key,
    topic_partition,
};
#[cfg(feature = "agent")]
pub use topic_trie::{
    SubscriptionType, TopicTrie, WildcardSubscriber, is_wildcard_pattern, validate_pattern,
};
#[cfg(feature = "agent")]
pub use transport::{
    ClusterMessage, ClusterTransport, InboundMessage, TransportConfig, TransportError,
};
#[cfg(feature = "agent")]
pub use wildcard_pending::{
    PendingWildcard, WILDCARD_RECONCILIATION_INTERVAL_MS, WildcardPendingStore,
};
#[cfg(feature = "agent")]
pub use wildcard_store::{WildcardEntry, WildcardStore, WildcardStoreError, wildcard_key};
#[cfg(feature = "agent")]
pub use write_log::PartitionWriteLog;
