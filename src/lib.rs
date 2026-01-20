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

#[cfg(any(feature = "native", feature = "wasm"))]
pub mod runtime;

#[cfg(feature = "native")]
pub mod agent;
#[cfg(feature = "native")]
pub mod cluster_agent;
#[cfg(feature = "native")]
pub mod consumer_group;
#[cfg(feature = "native")]
pub mod cursor;
#[cfg(feature = "native")]
pub mod database;
#[cfg(feature = "native")]
pub mod dedup;
#[cfg(feature = "native")]
pub mod dispatcher;
pub mod outbox;
#[cfg(feature = "native")]
pub mod session;
pub mod transport;

pub use constraint::{ForeignKeyConstraint, NotNullConstraint, OnDeleteAction, UniqueConstraint};
pub use error::{Error, Result};
pub use events::{ChangeEvent, Operation};
pub use schema::{FieldDefinition, FieldType, Schema};
pub use storage::{BatchWriter, MemoryBackend, Storage, StorageBackend};
pub use types::{Filter, FilterOp, Pagination, SortDirection, SortOrder};

#[cfg(feature = "native")]
pub use agent::MqdbAgent;
#[cfg(feature = "native")]
pub use cluster_agent::{ClusterConfig, ClusteredAgent, PeerConfig};
#[cfg(feature = "native")]
pub use config::DatabaseConfig;
pub use config::{DurabilityMode, OutboxConfig, SharedSubscriptionConfig};
#[cfg(feature = "native")]
pub use consumer_group::{
    ConsumerGroup, ConsumerGroupDetails, ConsumerGroupInfo, ConsumerMember, ConsumerMemberInfo,
};
#[cfg(feature = "native")]
pub use cursor::{Cursor, Query};
#[cfg(feature = "native")]
pub use database::Database;
#[cfg(feature = "native")]
pub use database::SubscriptionResult;
#[cfg(feature = "native")]
pub use dedup::DedupStore;
#[cfg(feature = "native")]
pub use outbox::OutboxProcessor;
pub use outbox::{Outbox, OutboxEntry};
#[cfg(feature = "native")]
pub use session::{ClientSession, EventRouter, SessionManager};
#[cfg(feature = "native")]
pub use subscription::SubscriptionRegistry;
pub use subscription::{Subscription, SubscriptionMode, match_pattern, match_wildcard};
pub use transport::{ErrorCode, ErrorResponse, Request, Response};

pub use protocol::{build_request, parse_admin_topic, parse_db_topic, AdminOperation, DbOperation};

pub use cluster::{
    Epoch, NUM_PARTITIONS, NodeId, PartitionAssignment, PartitionId, PartitionMap, PartitionRole,
};
