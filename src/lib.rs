pub mod checksum;
pub mod cluster;
pub mod config;
pub mod constraint;
pub mod entity;
pub mod error;
pub mod events;
pub mod index;
pub mod keys;
pub mod protocol;
pub mod relationship;
pub mod schema;
pub mod storage;
pub mod subscription;
pub mod types;

#[cfg(any(feature = "agent", feature = "wasm"))]
pub mod runtime;

#[cfg(feature = "agent")]
pub mod agent;
#[cfg(feature = "agent")]
pub mod auth_config;
#[cfg(feature = "agent")]
pub mod broker_defaults;
#[cfg(feature = "cluster")]
pub mod cluster_agent;
#[cfg(feature = "agent")]
pub mod consumer_group;
#[cfg(feature = "agent")]
pub mod cursor;
#[cfg(feature = "agent")]
pub mod database;
#[cfg(feature = "agent")]
pub mod dedup;
#[cfg(feature = "agent")]
pub mod dispatcher;
#[cfg(feature = "http-api")]
pub mod http;
pub mod outbox;
#[cfg(feature = "agent")]
pub mod session;
#[cfg(feature = "agent")]
pub mod topic_protection;
#[cfg(feature = "agent")]
pub mod topic_rules;
pub mod transport;

pub use constraint::{ForeignKeyConstraint, NotNullConstraint, OnDeleteAction, UniqueConstraint};
pub use error::{Error, Result};
pub use events::{ChangeEvent, Operation};
pub use schema::{FieldDefinition, FieldType, Schema};
pub use storage::{BatchWriter, MemoryBackend, Storage, StorageBackend};
pub use types::{Filter, FilterOp, Pagination, SortDirection, SortOrder};

#[cfg(feature = "agent")]
pub use agent::MqdbAgent;
#[cfg(feature = "cluster")]
pub use cluster_agent::{ClusterConfig, ClusterInitError, ClusteredAgent, PeerConfig};
#[cfg(feature = "agent")]
pub use config::DatabaseConfig;
pub use config::{DurabilityMode, OutboxConfig, SharedSubscriptionConfig};
#[cfg(feature = "agent")]
pub use consumer_group::{
    ConsumerGroup, ConsumerGroupDetails, ConsumerGroupInfo, ConsumerMember, ConsumerMemberInfo,
};
#[cfg(feature = "agent")]
pub use cursor::{Cursor, Query};
#[cfg(feature = "agent")]
pub use database::Database;
#[cfg(feature = "agent")]
pub use database::SubscriptionResult;
#[cfg(feature = "agent")]
pub use dedup::DedupStore;
#[cfg(feature = "agent")]
pub use outbox::OutboxProcessor;
pub use outbox::{Outbox, OutboxEntry};
#[cfg(feature = "agent")]
pub use session::{ClientSession, EventRouter, SessionManager};
#[cfg(feature = "agent")]
pub use subscription::SubscriptionRegistry;
pub use subscription::{Subscription, SubscriptionMode, match_pattern, match_wildcard};
pub use transport::{ErrorCode, ErrorResponse, Request, Response};

pub use protocol::{
    AdminOperation, DbOp, DbOperation, ProtocolError, build_request, parse_admin_topic,
    parse_db_topic,
};

pub use cluster::{
    Epoch, NUM_PARTITIONS, NodeId, PartitionAssignment, PartitionId, PartitionMap, PartitionRole,
};
