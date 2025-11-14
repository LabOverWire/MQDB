use crate::error::Result;
use crate::events::ChangeEvent;
use crate::subscription::{Subscription, SubscriptionRegistry};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::RwLock;

pub struct EventDispatcher {
    sender: broadcast::Sender<ChangeEvent>,
    registry: Arc<SubscriptionRegistry>,
    listeners: Arc<RwLock<Vec<EventListener>>>,
}

impl EventDispatcher {
    pub fn new(registry: Arc<SubscriptionRegistry>, capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);

        Self {
            sender,
            registry,
            listeners: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn dispatch(&self, event: ChangeEvent) -> Result<()> {
        let matching = self.registry.find_matching(&event).await;

        if !matching.is_empty() {
            if let Err(e) = self.sender.send(event.clone()) {
                tracing::warn!(
                    "failed to dispatch event to broadcast channel: {} (channel full or no receivers)",
                    e
                );
            }
        }

        let listeners = self.listeners.read().await;
        for listener in listeners.iter() {
            if listener.subscription.matches(&event) {
                if let Err(e) = listener.sender.send(event.clone()) {
                    tracing::warn!(
                        "failed to dispatch event to listener {}: {} (channel full or no receivers)",
                        listener.subscription.id,
                        e
                    );
                }
            }
        }

        Ok(())
    }

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
    pub fn receiver(&self) -> broadcast::Receiver<ChangeEvent> {
        self.sender.subscribe()
    }
}
