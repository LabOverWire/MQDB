mod epoch;
mod node;
mod partition;
mod partition_map;

#[cfg(feature = "native")]
mod client_location;
#[cfg(feature = "native")]
mod cursor;
#[cfg(feature = "native")]
pub mod db;
#[cfg(feature = "native")]
mod db_handler;
#[cfg(feature = "native")]
pub mod db_protocol;
#[cfg(feature = "native")]
mod db_topic;
#[cfg(feature = "native")]
mod dedicated_executor;
#[cfg(feature = "native")]
pub mod entity;
#[cfg(feature = "native")]
mod event_handler;
#[cfg(feature = "native")]
mod heartbeat;
#[cfg(feature = "native")]
mod idempotency_store;
#[cfg(feature = "native")]
mod inflight_store;
#[cfg(feature = "native")]
mod lwt;
#[cfg(feature = "native")]
mod message_processor;
#[cfg(feature = "native")]
mod migration;
#[cfg(feature = "native")]
mod mqtt_transport;
#[cfg(feature = "native")]
mod node_controller;
#[cfg(feature = "native")]
mod offset_store;
#[cfg(feature = "native")]
mod partition_storage;
#[cfg(feature = "native")]
mod protocol;
#[cfg(feature = "native")]
mod publish_router;
#[cfg(feature = "native")]
mod qos2_store;
#[cfg(feature = "native")]
mod query_coordinator;
#[cfg(feature = "native")]
mod quic_transport;
#[cfg(feature = "native")]
mod quorum;
#[cfg(feature = "native")]
pub mod raft;
#[cfg(feature = "native")]
mod raft_task;
#[cfg(feature = "native")]
mod rebalance_coordinator;
#[cfg(feature = "native")]
mod rebalancer;
#[cfg(feature = "native")]
mod replication;
#[cfg(feature = "native")]
mod retained_store;
#[cfg(feature = "native")]
mod session;
#[cfg(feature = "native")]
mod snapshot;
#[cfg(feature = "native")]
mod store_manager;
#[cfg(feature = "native")]
mod subscription_cache;
#[cfg(feature = "native")]
mod topic_index;
#[cfg(feature = "native")]
mod topic_trie;
#[cfg(feature = "native")]
mod transport;
#[cfg(feature = "native")]
mod wildcard_pending;
#[cfg(feature = "native")]
mod wildcard_store;
#[cfg(feature = "native")]
mod write_log;

pub use epoch::Epoch;
pub use node::NodeId;
pub use partition::{NUM_PARTITIONS, PartitionId};
pub use partition_map::{PartitionAssignment, PartitionMap, PartitionRole};

#[cfg(feature = "native")]
pub use client_location::{
    ClientLocationEntry, ClientLocationError, ClientLocationStore, client_location_key,
};
#[cfg(feature = "native")]
pub use cursor::{PartitionCursor, ScatterCursor};
#[cfg(feature = "native")]
pub use db::{DbDataStore, DbDataStoreError, DbEntity, data_partition, db_data_key};
#[cfg(feature = "native")]
pub use db_handler::{DbPublishResponse, DbRequestHandler};
#[cfg(feature = "native")]
pub use db_protocol::{
    DbReadRequest, DbResponse, DbStatus, DbWriteRequest, FkValidateRequest, FkValidateResponse,
    FkValidateStatus, IndexUpdateRequest,
};
#[cfg(feature = "native")]
pub use db_topic::{DbTopicOperation, ParsedDbTopic};
#[cfg(feature = "native")]
pub use dedicated_executor::DedicatedExecutor;
#[cfg(feature = "native")]
pub use event_handler::ClusterEventHandler;
#[cfg(feature = "native")]
pub use heartbeat::{HeartbeatManager, NodeStatus};
#[cfg(feature = "native")]
pub use idempotency_store::{
    IdempotencyCheck, IdempotencyError, IdempotencyRecord, IdempotencyStatus, IdempotencyStore,
    idempotency_storage_key,
};
#[cfg(feature = "native")]
pub use inflight_store::{InflightMessage, InflightStore, InflightStoreError, inflight_key};
#[cfg(feature = "native")]
pub use lwt::{
    LwtAction, LwtError, LwtPrepared, LwtPublisher, determine_lwt_action, generate_lwt_token,
};
#[cfg(feature = "native")]
pub use message_processor::{HeartbeatUpdate, MessageProcessor, ProcessingBatch};
#[cfg(feature = "native")]
pub use migration::{
    MigrationCheckpoint, MigrationError, MigrationManager, MigrationPhase, MigrationState,
};
#[cfg(feature = "native")]
#[allow(deprecated)]
pub use mqtt_transport::MqttTransport;
#[cfg(feature = "native")]
pub use node_controller::{NodeController, RaftMessage, TickOutput};
#[cfg(feature = "native")]
pub use offset_store::{ConsumerOffset, OffsetStore, OffsetStoreError, offset_key};
#[cfg(feature = "native")]
pub use partition_storage::PartitionStorage;
#[cfg(feature = "native")]
pub use protocol::{
    AckStatus, BatchReadRequest, BatchReadResponse, CatchupRequest, CatchupResponse, ForwardTarget,
    ForwardedPublish, Heartbeat, JsonDbOp, JsonDbRequest, JsonDbResponse, MessageType, Operation,
    QueryRequest, QueryResponse, QueryStatus, ReplicationAck, ReplicationWrite,
    TopicSubscriptionBroadcast, UniqueCommitRequest, UniqueCommitResponse, UniqueReleaseRequest,
    UniqueReleaseResponse, UniqueReserveRequest, UniqueReserveResponse, UniqueReserveStatus,
    WildcardBroadcast, WildcardOp,
};
#[cfg(feature = "native")]
pub use publish_router::{PublishRouteResult, PublishRouter, RoutingTarget, effective_qos};
#[cfg(feature = "native")]
pub use qos2_store::{
    Qos2Direction, Qos2Phase, Qos2State, Qos2Store, Qos2StoreError, qos2_state_key,
};
#[cfg(feature = "native")]
pub use query_coordinator::{QueryCoordinator, QueryResult};
#[cfg(feature = "native")]
pub use quic_transport::{LocalPublishRequest, QuicDirectTransport};
#[cfg(feature = "native")]
pub use quorum::{PendingWrites, QuorumResult, QuorumTracker};
#[cfg(feature = "native")]
pub use raft_task::{RaftAdminCommand, RaftEvent, RaftStatus, RaftTask};
#[cfg(feature = "native")]
pub use rebalance_coordinator::{
    RebalanceAck, RebalanceCommit, RebalanceCoordinator, RebalanceError, RebalanceProposal,
    RebalanceState,
};
#[cfg(feature = "native")]
pub use rebalancer::{
    PartitionReassignment, RebalanceConfig, compute_balanced_assignments,
    compute_incremental_assignments, compute_removal_assignments,
};
#[cfg(feature = "native")]
pub use replication::{ReplicaRole, ReplicaState, ReplicationError};
#[cfg(feature = "native")]
pub use retained_store::{
    RetainedMessage, RetainedStore, RetainedStoreError, retained_message_key,
};
#[cfg(feature = "native")]
pub use session::{SessionData, SessionError, SessionStore, session_key, session_partition};
#[cfg(feature = "native")]
pub use snapshot::{
    SnapshotBuilder, SnapshotChunk, SnapshotComplete, SnapshotRequest, SnapshotSender,
    SnapshotStatus,
};
#[cfg(feature = "native")]
pub use store_manager::{RecoveryStats, StoreApplyError, StoreManager};
#[cfg(feature = "native")]
pub use subscription_cache::{
    MqttSubscriptionSnapshot, MqttTopicEntry, ReconciliationResult,
    SUBSCRIPTION_RECONCILIATION_INTERVAL_MS, SubscriptionCache, SubscriptionCacheError,
    mqtt_subscription_key,
};
#[cfg(feature = "native")]
pub use topic_index::{
    SubscriberLocation, TopicIndex, TopicIndexEntry, TopicIndexError, topic_index_key,
    topic_partition,
};
#[cfg(feature = "native")]
pub use topic_trie::{
    SubscriptionType, TopicTrie, WildcardSubscriber, is_wildcard_pattern, validate_pattern,
};
#[cfg(feature = "native")]
pub use transport::{
    ClusterMessage, ClusterTransport, InboundMessage, TransportConfig, TransportError,
};
#[cfg(feature = "native")]
pub use wildcard_pending::{
    PendingWildcard, WILDCARD_RECONCILIATION_INTERVAL_MS, WildcardPendingStore,
};
#[cfg(feature = "native")]
pub use wildcard_store::{WildcardEntry, WildcardStore, WildcardStoreError, wildcard_key};
#[cfg(feature = "native")]
pub use write_log::PartitionWriteLog;
