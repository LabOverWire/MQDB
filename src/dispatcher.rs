use crate::consumer_group::ConsumerGroup;
use crate::error::Result;
use crate::events::ChangeEvent;
use crate::subscription::{Subscription, SubscriptionMode, SubscriptionRegistry};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::RwLock;

pub struct EventDispatcher {
    sender: broadcast::Sender<ChangeEvent>,
    registry: Arc<SubscriptionRegistry>,
    listeners: Arc<RwLock<Vec<EventListener>>>,
    consumer_groups: Arc<RwLock<HashMap<String, ConsumerGroup>>>,
    round_robin_counters: Arc<RwLock<HashMap<String, AtomicUsize>>>,
    num_partitions: u8,
}

impl EventDispatcher {
    pub fn new(
        registry: Arc<SubscriptionRegistry>,
        capacity: usize,
        consumer_groups: Arc<RwLock<HashMap<String, ConsumerGroup>>>,
        num_partitions: u8,
    ) -> Self {
        let (sender, _) = broadcast::channel(capacity);

        Self {
            sender,
            registry,
            listeners: Arc::new(RwLock::new(Vec::new())),
            consumer_groups,
            round_robin_counters: Arc::new(RwLock::new(HashMap::new())),
            num_partitions,
        }
    }

    /// # Errors
    /// Returns an error if dispatching fails.
    pub async fn dispatch(&self, event: ChangeEvent) -> Result<()> {
        let matching = self.registry.find_matching(&event).await;

        if matching.is_empty() {
            return Ok(());
        }

        let mut broadcast_subs = Vec::new();
        let mut share_groups: HashMap<String, Vec<Subscription>> = HashMap::new();

        for sub in matching {
            match (&sub.share_group, sub.mode) {
                (None, _) | (_, SubscriptionMode::Broadcast) => {
                    broadcast_subs.push(sub);
                }
                (Some(group), _) => {
                    share_groups.entry(group.clone()).or_default().push(sub);
                }
            }
        }

        if !broadcast_subs.is_empty()
            && let Err(e) = self.sender.send(event.clone()) {
                tracing::warn!(
                    "failed to dispatch event to broadcast channel: {} (channel full or no receivers)",
                    e
                );
            }

        let listeners = self.listeners.read().await;

        for sub in &broadcast_subs {
            if let Some(listener) = listeners.iter().find(|l| l.subscription.id == sub.id)
                && let Err(e) = listener.sender.send(event.clone()) {
                    tracing::warn!(
                        "failed to dispatch event to listener {}: {} (channel full or no receivers)",
                        listener.subscription.id,
                        e
                    );
                }
        }

        for (group_name, subs) in share_groups {
            let mode = subs.first().map(|s| s.mode).unwrap_or_default();
            let target_sub_id = match mode {
                SubscriptionMode::LoadBalanced => {
                    self.select_round_robin(&group_name, &subs).await
                }
                SubscriptionMode::Ordered => {
                    self.select_by_partition(&group_name, &event).await
                }
                SubscriptionMode::Broadcast => continue,
            };

            if let Some(target_id) = target_sub_id
                && let Some(listener) = listeners.iter().find(|l| l.subscription.id == target_id)
                    && let Err(e) = listener.sender.send(event.clone()) {
                        tracing::warn!(
                            "failed to dispatch event to shared listener {}: {}",
                            target_id,
                            e
                        );
                    }
        }

        Ok(())
    }

    async fn select_round_robin(&self, group: &str, subs: &[Subscription]) -> Option<String> {
        if subs.is_empty() {
            return None;
        }
        let mut counters = self.round_robin_counters.write().await;
        let counter = counters
            .entry(group.to_string())
            .or_insert_with(|| AtomicUsize::new(0));
        let idx = counter.fetch_add(1, Ordering::Relaxed) % subs.len();
        Some(subs[idx].id.clone())
    }

    async fn select_by_partition(&self, group: &str, event: &ChangeEvent) -> Option<String> {
        if self.num_partitions == 0 {
            return None;
        }
        let partition = event.partition(self.num_partitions);
        let groups = self.consumer_groups.read().await;
        groups
            .get(group)
            .and_then(|cg| cg.get_partition_owner(partition))
            .map(|s| s.to_string())
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<ChangeEvent> {
        self.sender.subscribe()
    }

    pub async fn add_listener(&self, subscription: Subscription) -> EventListener {
        let (tx, _rx) = broadcast::channel(100);
        let listener = EventListener {
            subscription: subscription.clone(),
            sender: tx,
        };

        let mut listeners = self.listeners.write().await;
        listeners.push(listener.clone());

        listener
    }

    pub async fn remove_listener(&self, sub_id: &str) {
        let mut listeners = self.listeners.write().await;
        listeners.retain(|l| l.subscription.id != sub_id);
    }
}

#[derive(Clone)]
pub struct EventListener {
    pub subscription: Subscription,
    sender: broadcast::Sender<ChangeEvent>,
}

impl EventListener {
    #[must_use]
    pub fn receiver(&self) -> broadcast::Receiver<ChangeEvent> {
        self.sender.subscribe()
    }
}
