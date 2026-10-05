// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod coordinator;
mod node;
mod rpc;
mod state;
mod storage;
#[cfg(test)]
pub(crate) mod test_support;

pub use coordinator::{CoordinatorError, RaftCoordinator};
pub use node::{RaftConfig, RaftNode, RaftOutput};
pub use rpc::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, RaftSnapshot,
    RequestVoteRequest, RequestVoteResponse,
};
pub use state::{LogEntry, PartitionUpdate, PreparedAppend, RaftCommand, RaftRole, RaftState};
pub use storage::{RaftPersistentState, RaftStorage};
