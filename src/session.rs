use crate::Database;
use crate::events::ChangeEvent;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{RwLock, mpsc};

#[derive(Debug, Clone)]
pub struct ClientSession {
    pub id: String,
    pub created_at: Instant,
    pub subscriptions: HashSet<String>,
}

impl ClientSession {
    #[must_use]
    pub fn new(id: String) -> Self {
        Self {
            id,
            created_at: Instant::now(),
            subscriptions: HashSet::new(),
        }
    }
}

pub struct SessionManager {
    sessions: RwLock<HashMap<String, ClientSession>>,
    subscription_to_client: RwLock<HashMap<String, String>>,
}

impl SessionManager {
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            subscription_to_client: RwLock::new(HashMap::new()),
        }
    }

    pub async fn create_session(&self, client_id: String) -> String {
        let session = ClientSession::new(client_id.clone());
        self.sessions
            .write()
            .await
            .insert(client_id.clone(), session);
        client_id
    }

    pub async fn destroy_session(&self, client_id: &str) -> Vec<String> {
        let mut sessions = self.sessions.write().await;
        let mut sub_to_client = self.subscription_to_client.write().await;

        if let Some(session) = sessions.remove(client_id) {
            let subs: Vec<String> = session.subscriptions.into_iter().collect();
            for sub_id in &subs {
                sub_to_client.remove(sub_id);
            }
            subs
        } else {
            vec![]
        }
    }

    pub async fn add_subscription(&self, client_id: &str, subscription_id: String) {
        let mut sessions = self.sessions.write().await;
        let mut sub_to_client = self.subscription_to_client.write().await;

        if let Some(session) = sessions.get_mut(client_id) {
            session.subscriptions.insert(subscription_id.clone());
            sub_to_client.insert(subscription_id, client_id.to_string());
        }
    }

    pub async fn remove_subscription(&self, client_id: &str, subscription_id: &str) {
        let mut sessions = self.sessions.write().await;
        let mut sub_to_client = self.subscription_to_client.write().await;

        if let Some(session) = sessions.get_mut(client_id) {
            session.subscriptions.remove(subscription_id);
            sub_to_client.remove(subscription_id);
        }
    }

    pub async fn get_client_for_subscription(&self, subscription_id: &str) -> Option<String> {
        self.subscription_to_client
            .read()
            .await
            .get(subscription_id)
            .cloned()
    }

    pub async fn get_subscriptions_for_client(&self, client_id: &str) -> Vec<String> {
        self.sessions
            .read()
            .await
            .get(client_id)
            .map(|s| s.subscriptions.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub async fn session_exists(&self, client_id: &str) -> bool {
        self.sessions.read().await.contains_key(client_id)
    }

    pub async fn session_count(&self) -> usize {
        self.sessions.read().await.len()
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

pub struct EventRouter {
    db: Database,
    sessions: Arc<SessionManager>,
    client_senders: RwLock<HashMap<String, mpsc::Sender<ChangeEvent>>>,
    channel_capacity: usize,
}

impl EventRouter {
    pub fn new(db: Database, sessions: Arc<SessionManager>) -> Self {
        Self {
            db,
            sessions,
            client_senders: RwLock::new(HashMap::new()),
            channel_capacity: 256,
        }
    }

    #[must_use]
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    pub async fn register_client(&self, client_id: &str) -> mpsc::Receiver<ChangeEvent> {
        let (tx, rx) = mpsc::channel(self.channel_capacity);
        self.client_senders
            .write()
            .await
            .insert(client_id.to_string(), tx);
        rx
    }

    pub async fn unregister_client(&self, client_id: &str) {
        self.client_senders.write().await.remove(client_id);
    }

    pub async fn run(&self) {
        let mut rx = self.db.event_receiver();

        while let Ok(event) = rx.recv().await {
            self.route_event(event).await;
        }
    }

    async fn route_event(&self, event: ChangeEvent) {
        let senders = self.client_senders.read().await;
        let sessions = self.sessions.sessions.read().await;

        for (client_id, session) in sessions.iter() {
            if Self::event_matches_subscriptions(&event, &session.subscriptions)
                && let Some(sender) = senders.get(client_id)
            {
                let _ = sender.try_send(event.clone());
            }
        }
    }

    fn event_matches_subscriptions(
        event: &ChangeEvent,
        subscriptions: &HashSet<String>,
    ) -> bool {
        let event_path = format!("{}/{}", event.entity, event.id);

        for pattern in subscriptions {
            if Self::pattern_matches(&event_path, pattern) {
                return true;
            }
        }
        false
    }

    fn pattern_matches(path: &str, pattern: &str) -> bool {
        let path_parts: Vec<&str> = path.split('/').collect();
        let pattern_parts: Vec<&str> = pattern.split('/').collect();

        let mut path_idx = 0;
        let mut pattern_idx = 0;

        while path_idx < path_parts.len() && pattern_idx < pattern_parts.len() {
            match pattern_parts[pattern_idx] {
                "#" => return true,
                "+" => {
                    path_idx += 1;
                    pattern_idx += 1;
                }
                part if part == path_parts[path_idx] => {
                    path_idx += 1;
                    pattern_idx += 1;
                }
                _ => return false,
            }
        }

        path_idx == path_parts.len() && pattern_idx == pattern_parts.len()
    }

    pub fn database(&self) -> &Database {
        &self.db
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_session_lifecycle() {
        let manager = SessionManager::new();

        manager.create_session("client1".to_string()).await;
        assert!(manager.session_exists("client1").await);
        assert_eq!(manager.session_count().await, 1);

        manager.destroy_session("client1").await;
        assert!(!manager.session_exists("client1").await);
        assert_eq!(manager.session_count().await, 0);
    }

    #[tokio::test]
    async fn test_subscription_tracking() {
        let manager = SessionManager::new();

        manager.create_session("client1".to_string()).await;
        manager
            .add_subscription("client1", "sub1".to_string())
            .await;
        manager
            .add_subscription("client1", "sub2".to_string())
            .await;

        let subs = manager.get_subscriptions_for_client("client1").await;
        assert_eq!(subs.len(), 2);
        assert!(subs.contains(&"sub1".to_string()));
        assert!(subs.contains(&"sub2".to_string()));

        let client = manager.get_client_for_subscription("sub1").await;
        assert_eq!(client, Some("client1".to_string()));

        manager.remove_subscription("client1", "sub1").await;
        let subs = manager.get_subscriptions_for_client("client1").await;
        assert_eq!(subs.len(), 1);
        assert!(!subs.contains(&"sub1".to_string()));
    }

    #[tokio::test]
    async fn test_destroy_session_cleans_subscriptions() {
        let manager = SessionManager::new();

        manager.create_session("client1".to_string()).await;
        manager
            .add_subscription("client1", "sub1".to_string())
            .await;
        manager
            .add_subscription("client1", "sub2".to_string())
            .await;

        let removed = manager.destroy_session("client1").await;
        assert_eq!(removed.len(), 2);

        assert!(manager.get_client_for_subscription("sub1").await.is_none());
        assert!(manager.get_client_for_subscription("sub2").await.is_none());
    }

    #[tokio::test]
    async fn test_pattern_matching() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = Database::open(tmp.path()).await.unwrap();
        let sessions = Arc::new(SessionManager::new());
        let router = EventRouter::new(db, sessions);

        let _ = router;
        assert!(EventRouter::pattern_matches("users/123", "users/123"));
        assert!(EventRouter::pattern_matches("users/123", "users/+"));
        assert!(EventRouter::pattern_matches("users/123", "#"));
        assert!(EventRouter::pattern_matches("users/123/profile", "users/#"));
        assert!(!EventRouter::pattern_matches("users/123", "posts/+"));
        assert!(!EventRouter::pattern_matches("users/123", "users/456"));
    }
}
