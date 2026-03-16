// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

pub mod cluster;
pub mod cluster_agent;

pub use cluster::PartitionMapExt;
pub use cluster::db;
pub use cluster::db_protocol;
pub use cluster::entity;
pub use cluster::raft;
pub use cluster::{
    AckStatus, BatchReadRequest, BatchReadResponse, CascadePendingEntry, CascadeRemoteOp,
    CatchupRequest, CatchupResponse, ClientLocationEntry, ClientLocationError,
    ClientLocationStore, ClusterEventHandler, ClusterMessage, ClusterOutbox, ClusterTransport,
    ConsumerOffset, DbDataStore, DbDataStoreError, DbEntity, DbPublishResponse, DbReadRequest,
    DbRequestHandler, DbResponse, DbStatus, DbTopicOperation, DbWriteRequest, DedicatedExecutor,
    Epoch, FkValidateRequest, FkValidateResponse, FkValidateStatus, ForwardTarget,
    ForwardedPublish, HeartbeatManager, HeartbeatUpdate, Heartbeat, IdempotencyCheck,
    IdempotencyError, IdempotencyRecord, IdempotencyStatus, IdempotencyStore,
    InboundMessage, IndexUpdateRequest, InflightMessage, InflightStore, InflightStoreError,
    JsonDbOp, JsonDbRequest, JsonDbResponse, LocalPublishRequest, LwtAction, LwtError,
    LwtPrepared, LwtPublisher, MessageProcessor, MessageType, MigrationCheckpoint,
    MigrationError, MigrationManager, MigrationPhase, MigrationState, MqttSubscriptionSnapshot,
    MqttTopicEntry, NodeController, NodeId, NodeStatus, NUM_PARTITIONS, OffsetStore,
    OffsetStoreError, Operation, ParsedDbTopic, PartitionAssignment, PartitionCursor,
    PartitionId, PartitionMap, PartitionReassignment, PartitionRole, PartitionStorage,
    PartitionWriteLog, PendingWrites, PendingWildcard, ProcessingBatch, PublishRouteResult,
    PublishRouter, Qos2Direction, Qos2Phase, Qos2State, Qos2Store, Qos2StoreError,
    QueryCoordinator, QueryRequest, QueryResponse, QueryResult, QueryStatus, QuicDirectTransport,
    QuorumResult, QuorumTracker, RaftAdminCommand, RaftEvent, RaftMessage, RaftStatus, RaftTask,
    RebalanceAck, RebalanceCoordinator, RebalanceCommit, RebalanceConfig, RebalanceError,
    RebalanceProposal, RebalanceState, ReconciliationResult, RecoveryStats, ReplicaRole,
    ReplicaState, ReplicationAck, ReplicationError, ReplicationWrite, RetainedMessage,
    RetainedStore, RetainedStoreError, RoutingTarget, ScatterCursor, SessionData, SessionError,
    SessionStore, SnapshotBuilder, SnapshotChunk, SnapshotComplete, SnapshotRequest,
    SnapshotSender, SnapshotStatus, StoreApplyError, StoreManager, SubscriberLocation,
    SubscriptionCache, SubscriptionCacheError, SubscriptionType, TickOutput, TopicIndex,
    TopicIndexEntry, TopicIndexError, TopicSubscriptionBroadcast, TopicTrie, TransportConfig,
    TransportError, UniqueCommitRequest, UniqueCommitResponse, UniqueReleaseRequest,
    UniqueReleaseResponse, UniqueReserveRequest, UniqueReserveResponse, UniqueReserveStatus,
    WildcardBroadcast, WildcardEntry, WildcardOp, WildcardPendingStore, WildcardStore,
    WildcardStoreError, WildcardSubscriber,
};
pub use cluster::{
    client_location_key, compute_balanced_assignments, compute_incremental_assignments,
    compute_removal_assignments, data_partition, db_data_key, determine_lwt_action,
    effective_qos, generate_lwt_token, idempotency_storage_key, inflight_key,
    is_wildcard_pattern, mqtt_subscription_key, offset_key, qos2_state_key,
    retained_message_key, session_key, session_partition, topic_index_key, topic_partition,
    validate_pattern, wildcard_key, SUBSCRIPTION_RECONCILIATION_INTERVAL_MS,
    WILDCARD_RECONCILIATION_INTERVAL_MS,
};
#[cfg(feature = "mqtt-bridge")]
#[allow(deprecated)]
pub use cluster::MqttTransport;
pub use cluster_agent::ClusterTransportKind;
pub use cluster_agent::{ClusterConfig, ClusterInitError, ClusteredAgent, PeerConfig, QuicConfig};
