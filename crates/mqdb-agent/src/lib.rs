// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

pub mod agent;
pub mod auth_config;
pub mod broker_defaults;
pub mod consumer_group;
pub mod cursor;
pub mod database;
pub mod dedup;
pub mod dispatcher;
pub mod outbox_processor;
pub mod runtime;
pub mod session;
pub mod subscription_registry;
pub mod topic_protection;
pub mod topic_rules;
pub mod transport_execute;

#[cfg(feature = "http-api")]
pub mod http;
#[cfg(feature = "http-api")]
pub mod vault_ops;
#[cfg(feature = "http-api")]
pub mod vault_transform;

pub use agent::MqdbAgent;
pub use consumer_group::{
    ConsumerGroup, ConsumerGroupDetails, ConsumerGroupInfo, ConsumerMember, ConsumerMemberInfo,
};
pub use cursor::{Cursor, Query};
pub use database::{CallerContext, Database, SubscriptionResult};
pub use dedup::DedupStore;
pub use outbox_processor::OutboxProcessor;
pub use session::{ClientSession, EventRouter, SessionManager};
pub use subscription_registry::SubscriptionRegistry;
