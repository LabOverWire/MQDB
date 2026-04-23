// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

pub mod checksum;
pub mod config;
pub mod constraint;
pub mod entity;
pub mod error;
pub mod events;
pub mod index;
pub mod keys;
pub mod license;
pub mod outbox;
pub mod partition;
pub mod protocol;
pub mod query;
pub mod relationship;
pub mod schema;
pub mod storage;
pub mod subscription;
pub mod transport;
pub mod types;

pub use constraint::{ForeignKeyConstraint, NotNullConstraint, OnDeleteAction, UniqueConstraint};
pub use error::{Error, Result};
pub use events::{ChangeEvent, Operation};
pub use schema::{FieldDefinition, FieldType, Schema};
pub use storage::{BatchWriter, MemoryBackend, Storage, StorageBackend};
pub use types::{
    Filter, FilterOp, OwnershipConfig, Pagination, ScopeConfig, SortDirection, SortOrder,
    project_fields,
};

#[cfg(feature = "native")]
pub use config::DatabaseConfig;
pub use config::{DurabilityMode, OutboxConfig, SharedSubscriptionConfig};
pub use outbox::{Outbox, OutboxEntry};
pub use subscription::{Subscription, SubscriptionMode, match_pattern, match_wildcard};
pub use transport::{ErrorCode, ErrorResponse, Request, Response, VaultConstraintData};

pub use protocol::{
    AdminOperation, DbOp, DbOperation, ProtocolError, build_request, parse_admin_topic,
    parse_db_topic,
};

pub use partition::{
    Epoch, NUM_PARTITIONS, NodeId, PartitionAssignment, PartitionId, PartitionMap, PartitionRole,
    data_partition, generate_id_for_partition, index_partition, schema_partition, unique_partition,
};
