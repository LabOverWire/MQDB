// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

mod agent;
mod auth;
mod base;
mod bench;
mod db;
#[cfg(feature = "cluster")]
mod dev;

pub(crate) use agent::AgentAction;
#[cfg(feature = "cluster")]
pub(crate) use agent::ClusterAction;
pub(crate) use auth::{AclAction, AuthArgs, OAuthArgs};
pub(crate) use base::{
    Cli, Commands, ConnectionArgs, DurabilityArg, JwtAlgorithmArg, OutputFormat,
    SubscriptionModeArg,
};
pub(crate) use bench::BenchAction;
#[cfg(feature = "cluster")]
pub(crate) use db::DbAction;
pub(crate) use db::{
    BackupAction, ConstraintAction, ConsumerGroupAction, IndexAction, SchemaAction,
};
#[cfg(feature = "cluster")]
pub(crate) use dev::{DevAction, DevBaselineAction, DevBenchScenario};
