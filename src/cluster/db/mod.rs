// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod constraint_store;
mod data_store;
mod fk_store;
mod index_store;
mod partition;
mod schema_store;
mod unique_store;

pub use constraint_store::{
    ClusterConstraint, ConstraintStore, ConstraintStoreError, ConstraintType, OnDeleteAction,
    constraint_key,
};
pub use data_store::{DbDataStore, DbDataStoreError, DbEntity, FkReverseIndex, db_data_key};
pub use fk_store::{
    FkStoreError, FkValidationRequest, FkValidationResponse, FkValidationResult, FkValidationStore,
};
pub use index_store::{IndexEntry, IndexStore, IndexStoreError, index_key};
pub use partition::{
    data_partition, generate_id_for_partition, index_partition, schema_partition, unique_partition,
};
pub use schema_store::{ClusterSchema, SchemaState, SchemaStore, SchemaStoreError, schema_key};
pub use unique_store::{
    ReserveResult, UniqueReservation, UniqueStore, UniqueStoreError, unique_key,
};
