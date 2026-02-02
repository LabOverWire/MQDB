use crate::topic_rules::check_topic_access;
use mqtt5::broker::auth::{AuthProvider, AuthResult, EnhancedAuthResult};
use mqtt5::error::Result;
use mqtt5::packet::connect::ConnectPacket;
use std::collections::HashSet;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tracing::debug;

pub struct TopicProtectionAuthProvider {
    inner: Arc<dyn AuthProvider>,
    admin_users: Arc<HashSet<String>>,
    internal_service_username: Option<String>,
}

impl TopicProtectionAuthProvider {
    #[must_use]
    pub fn new(inner: Arc<dyn AuthProvider>, admin_users: HashSet<String>) -> Self {
        Self {
            inner,
            admin_users: Arc::new(admin_users),
            internal_service_username: None,
        }
    }

    #[must_use]
    pub fn with_internal_service_username(mut self, username: Option<String>) -> Self {
        self.internal_service_username = username;
        self
    }

    fn is_internal_service(&self, user_id: Option<&str>) -> bool {
        match (&self.internal_service_username, user_id) {
            (Some(internal), Some(user)) => internal == user,
            _ => false,
        }
    }

    fn is_admin_user(&self, user_id: Option<&str>) -> bool {
        user_id.is_some_and(|u| self.admin_users.contains(u))
    }
}

impl std::fmt::Debug for TopicProtectionAuthProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicProtectionAuthProvider")
            .field("admin_users", &self.admin_users)
            .finish_non_exhaustive()
    }
}

impl AuthProvider for TopicProtectionAuthProvider {
    fn authenticate<'a>(
        &'a self,
        connect: &'a ConnectPacket,
        client_addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = Result<AuthResult>> + Send + 'a>> {
        let username = connect.username.clone();
        Box::pin(async move {
            let result = self.inner.authenticate(connect, client_addr).await?;
            if result.authenticated
                && result.user_id.is_none()
                && let Some(user) = username
            {
                return Ok(AuthResult::success_with_user(user));
            }
            Ok(result)
        })
    }

    fn authorize_publish<'a>(
        &'a self,
        client_id: &str,
        user_id: Option<&'a str>,
        topic: &'a str,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        let client_id_owned = client_id.to_string();
        let is_admin = self.is_admin_user(user_id);
        let is_internal = self.is_internal_service(user_id);

        Box::pin(async move {
            if is_internal {
                return self
                    .inner
                    .authorize_publish(&client_id_owned, user_id, topic)
                    .await;
            }

            if let Err(reason) = check_topic_access(topic, true, is_admin) {
                debug!(
                    client_id = %client_id_owned,
                    topic = %topic,
                    reason = %reason,
                    "publish blocked by topic protection"
                );
                return false;
            }

            self.inner
                .authorize_publish(&client_id_owned, user_id, topic)
                .await
        })
    }

    fn authorize_subscribe<'a>(
        &'a self,
        client_id: &str,
        user_id: Option<&'a str>,
        topic_filter: &'a str,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        let client_id_owned = client_id.to_string();
        let is_admin = self.is_admin_user(user_id);
        let is_internal = self.is_internal_service(user_id);

        Box::pin(async move {
            if is_internal {
                return self
                    .inner
                    .authorize_subscribe(&client_id_owned, user_id, topic_filter)
                    .await;
            }

            if let Err(reason) = check_topic_access(topic_filter, false, is_admin) {
                debug!(
                    client_id = %client_id_owned,
                    topic_filter = %topic_filter,
                    reason = %reason,
                    "subscribe blocked by topic protection"
                );
                return false;
            }

            self.inner
                .authorize_subscribe(&client_id_owned, user_id, topic_filter)
                .await
        })
    }

    fn supports_enhanced_auth(&self) -> bool {
        self.inner.supports_enhanced_auth()
    }

    fn authenticate_enhanced<'a>(
        &'a self,
        auth_method: &'a str,
        auth_data: Option<&'a [u8]>,
        client_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<EnhancedAuthResult>> + Send + 'a>> {
        self.inner
            .authenticate_enhanced(auth_method, auth_data, client_id)
    }

    fn reauthenticate<'a>(
        &'a self,
        auth_method: &'a str,
        auth_data: Option<&'a [u8]>,
        client_id: &'a str,
        user_id: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<EnhancedAuthResult>> + Send + 'a>> {
        self.inner
            .reauthenticate(auth_method, auth_data, client_id, user_id)
    }

    fn cleanup_session<'a>(
        &'a self,
        user_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        self.inner.cleanup_session(user_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mqtt5::broker::auth::AllowAllAuthProvider;

    fn create_test_provider(admin_users: HashSet<String>) -> TopicProtectionAuthProvider {
        TopicProtectionAuthProvider::new(Arc::new(AllowAllAuthProvider), admin_users)
    }

    fn create_test_provider_with_internal(internal_username: &str) -> TopicProtectionAuthProvider {
        TopicProtectionAuthProvider::new(Arc::new(AllowAllAuthProvider), HashSet::new())
            .with_internal_service_username(Some(internal_username.to_string()))
    }

    #[tokio::test]
    async fn internal_service_user_bypasses_protection() {
        let provider = create_test_provider_with_internal("mqdb-internal-abc123");

        let result = provider
            .authorize_publish(
                "any-client-id",
                Some("mqdb-internal-abc123"),
                "_mqdb/cluster/heartbeat",
            )
            .await;
        assert!(result);

        let result = provider
            .authorize_subscribe("any-client-id", Some("mqdb-internal-abc123"), "$DB/_idx/#")
            .await;
        assert!(result);
    }

    #[tokio::test]
    async fn spoofed_client_id_blocked() {
        let provider = create_test_provider_with_internal("mqdb-internal-abc123");

        let result = provider
            .authorize_publish("mqdb-hacker", None, "_mqdb/cluster/heartbeat")
            .await;
        assert!(!result);

        let result = provider
            .authorize_publish(
                "mqdb-internal-handler",
                Some("attacker"),
                "_mqdb/cluster/heartbeat",
            )
            .await;
        assert!(!result);
    }

    #[tokio::test]
    async fn external_client_blocked_on_internal_topics() {
        let provider = create_test_provider(HashSet::new());

        let result = provider
            .authorize_publish("client-1", None, "_mqdb/cluster/heartbeat")
            .await;
        assert!(!result);

        let result = provider
            .authorize_subscribe("client-1", None, "$DB/_idx/users")
            .await;
        assert!(!result);
    }

    #[tokio::test]
    async fn external_client_can_subscribe_to_sys() {
        let provider = create_test_provider(HashSet::new());

        let result = provider
            .authorize_subscribe("client-1", None, "$SYS/#")
            .await;
        assert!(result);
    }

    #[tokio::test]
    async fn external_client_cannot_publish_to_sys() {
        let provider = create_test_provider(HashSet::new());

        let result = provider
            .authorize_publish("client-1", None, "$SYS/broker/uptime")
            .await;
        assert!(!result);
    }

    #[tokio::test]
    async fn admin_user_can_access_admin_topics() {
        let mut admin_users = HashSet::new();
        admin_users.insert("admin".to_string());
        let provider = create_test_provider(admin_users);

        let result = provider
            .authorize_publish("client-1", Some("admin"), "$DB/_admin/backup")
            .await;
        assert!(result);

        let result = provider
            .authorize_subscribe("client-1", Some("admin"), "$DB/_admin/#")
            .await;
        assert!(result);
    }

    #[tokio::test]
    async fn non_admin_user_blocked_on_admin_topics() {
        let mut admin_users = HashSet::new();
        admin_users.insert("admin".to_string());
        let provider = create_test_provider(admin_users);

        let result = provider
            .authorize_publish("client-1", Some("regular"), "$DB/_admin/backup")
            .await;
        assert!(!result);

        let result = provider
            .authorize_publish("client-1", None, "$DB/_admin/backup")
            .await;
        assert!(!result);
    }

    #[tokio::test]
    async fn regular_topics_allowed() {
        let provider = create_test_provider(HashSet::new());

        let result = provider
            .authorize_publish("client-1", None, "$DB/users/create")
            .await;
        assert!(result);

        let result = provider
            .authorize_subscribe("client-1", None, "sensors/temperature")
            .await;
        assert!(result);
    }
}
