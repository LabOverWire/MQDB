use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum DurabilityMode {
    Immediate,
    PeriodicMs(u64),
    None,
}

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub path: PathBuf,
    pub durability: DurabilityMode,
    pub event_channel_capacity: usize,
    pub max_list_results: Option<usize>,
    pub max_subscriptions: Option<usize>,
    pub ttl_cleanup_interval_secs: Option<u64>,
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
        }
    }

    pub fn with_durability(mut self, mode: DurabilityMode) -> Self {
        self.durability = mode;
        self
    }

    pub fn with_event_capacity(mut self, capacity: usize) -> Self {
        self.event_channel_capacity = capacity;
        self
    }

    pub fn with_max_list_results(mut self, max: Option<usize>) -> Self {
        self.max_list_results = max;
        self
    }

    pub fn with_max_subscriptions(mut self, max: Option<usize>) -> Self {
        self.max_subscriptions = max;
        self
    }

    pub fn with_ttl_cleanup_interval(mut self, interval_secs: Option<u64>) -> Self {
        self.ttl_cleanup_interval_secs = interval_secs;
        self
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self::new("./data/mqdb")
    }
}
