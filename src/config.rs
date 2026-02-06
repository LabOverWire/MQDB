// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

#[derive(Debug, Clone, Copy)]
pub enum DurabilityMode {
    Immediate,
    PeriodicMs(u64),
    None,
}

#[derive(Debug, Clone)]
pub struct OutboxConfig {
    pub enabled: bool,
    pub retry_interval_ms: u64,
    pub max_retries: u32,
    pub batch_size: usize,
}

impl Default for OutboxConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            retry_interval_ms: 5000,
            max_retries: 10,
            batch_size: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SharedSubscriptionConfig {
    pub num_partitions: u8,
    pub consumer_timeout_ms: u64,
}

impl Default for SharedSubscriptionConfig {
    fn default() -> Self {
        Self {
            num_partitions: 8,
            consumer_timeout_ms: 30_000,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use super::{DurabilityMode, OutboxConfig, SharedSubscriptionConfig};
    use std::path::PathBuf;

    #[derive(Debug, Clone)]
    pub struct DatabaseConfig {
        pub path: PathBuf,
        pub durability: DurabilityMode,
        pub event_channel_capacity: usize,
        pub max_list_results: Option<usize>,
        pub max_subscriptions: Option<usize>,
        pub ttl_cleanup_interval_secs: Option<u64>,
        pub max_cursor_buffer: usize,
        pub max_sort_buffer: usize,
        pub outbox: OutboxConfig,
        pub shared_subscription: SharedSubscriptionConfig,
        pub spawn_background_tasks: bool,
    }

    impl DatabaseConfig {
        pub fn new<P: Into<PathBuf>>(path: P) -> Self {
            Self {
                path: path.into(),
                durability: DurabilityMode::Immediate,
                event_channel_capacity: 1000,
                max_list_results: Some(10_000),
                max_subscriptions: Some(1_000),
                ttl_cleanup_interval_secs: Some(60),
                max_cursor_buffer: 100,
                max_sort_buffer: 10_000,
                outbox: OutboxConfig::default(),
                shared_subscription: SharedSubscriptionConfig::default(),
                spawn_background_tasks: true,
            }
        }

        #[must_use]
        pub fn without_background_tasks(mut self) -> Self {
            self.spawn_background_tasks = false;
            self
        }

        #[must_use]
        pub fn with_durability(mut self, mode: DurabilityMode) -> Self {
            self.durability = mode;
            self
        }

        #[must_use]
        pub fn with_event_capacity(mut self, capacity: usize) -> Self {
            self.event_channel_capacity = capacity;
            self
        }

        #[must_use]
        pub fn with_max_list_results(mut self, max: Option<usize>) -> Self {
            self.max_list_results = max;
            self
        }

        #[must_use]
        pub fn with_max_subscriptions(mut self, max: Option<usize>) -> Self {
            self.max_subscriptions = max;
            self
        }

        #[must_use]
        pub fn with_ttl_cleanup_interval(mut self, interval_secs: Option<u64>) -> Self {
            self.ttl_cleanup_interval_secs = interval_secs;
            self
        }

        #[must_use]
        pub fn with_outbox(mut self, outbox: OutboxConfig) -> Self {
            self.outbox = outbox;
            self
        }

        #[must_use]
        pub fn with_shared_subscription(mut self, config: SharedSubscriptionConfig) -> Self {
            self.shared_subscription = config;
            self
        }
    }

    impl Default for DatabaseConfig {
        fn default() -> Self {
            Self::new("./data/mqdb")
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub use native::DatabaseConfig;
