// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::topic_rules::{BlockReason, check_topic_access};
use mqtt5::broker::auth::{AuthProvider, AuthResult, EnhancedAuthResult};
use mqtt5::error::Result;
use mqtt5::packet::connect::ConnectPacket;
use mqtt5::protocol::v5::reason_codes::ReasonCode;
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
    event_scope_entity: Option<String>,
    all_users_admin: bool,
    scoped_events: bool,
    sparkplug_aware: bool,
}

const RESERVED_CLIENT_ID_PREFIX: &str = "mqdb-";
const CLI_CLIENT_ID_PREFIX: &str = "mqdb-cli-";

#[must_use]
pub fn is_reserved_client_id(client_id: &str) -> bool {
    client_id.starts_with(RESERVED_CLIENT_ID_PREFIX) && !client_id.starts_with(CLI_CLIENT_ID_PREFIX)
}

fn is_scoped_event_topic(topic: &str, scope_entity: &str) -> bool {
    let mut levels = topic.split('/');
    levels.next() == Some("$DB")
        && levels.next() == Some(scope_entity)
        && levels.next().is_some()
        && levels.take(2).any(|level| level == "events")
}

/// If `topic` is a per-user event namespace `$DB/u/{user}/...`, return `{user}`.
fn user_scope_segment(topic: &str) -> Option<&str> {
    let mut parts = topic.split('/');
    if parts.next()? != "$DB" {
        return None;
    }
    if parts.next()? != "u" {
        return None;
    }
    parts.next()
}

impl TopicProtectionAuthProvider {
    #[must_use]
    pub fn new(inner: Arc<dyn AuthProvider>, admin_users: HashSet<String>) -> Self {
        Self {
            inner,
            admin_users: Arc::new(admin_users),
            internal_service_username: None,
            event_scope_entity: None,
            all_users_admin: false,
            scoped_events: false,
            sparkplug_aware: false,
        }
    }

    #[must_use]
    pub fn with_sparkplug_aware(mut self, enabled: bool) -> Self {
        self.sparkplug_aware = enabled;
        self
    }

    #[must_use]
    pub fn with_internal_service_username(mut self, username: Option<String>) -> Self {
        self.internal_service_username = username;
        self
    }

    #[must_use]
    pub fn with_event_scope_entity(mut self, scope_entity: Option<String>) -> Self {
        self.event_scope_entity = scope_entity.filter(|entity| !entity.is_empty());
        self
    }

    #[must_use]
    pub fn with_all_users_admin(mut self, enabled: bool) -> Self {
        self.all_users_admin = enabled;
        self
    }

    #[must_use]
    pub fn with_scoped_events(mut self, enabled: bool) -> Self {
        self.scoped_events = enabled;
        self
    }

    fn is_internal_service(&self, user_id: Option<&str>) -> bool {
        match (&self.internal_service_username, user_id) {
            (Some(internal), Some(user)) => internal == user,
            _ => false,
        }
    }

    fn reserves_client_id(&self, client_id: &str, user_id: Option<&str>) -> bool {
        self.internal_service_username.is_some()
            && is_reserved_client_id(client_id)
            && !self.is_internal_service(user_id)
    }

    fn is_admin_user(&self, user_id: Option<&str>) -> bool {
        self.all_users_admin || user_id.is_some_and(|u| self.admin_users.contains(u))
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
            let result = match username {
                Some(user) if result.authenticated && result.user_id.is_none() => {
                    AuthResult::success_with_user(user)
                }
                _ => result,
            };
            if result.authenticated
                && self.reserves_client_id(&connect.client_id, result.user_id.as_deref())
            {
                debug!(
                    client_id = %connect.client_id,
                    "connect with a reserved client id denied"
                );
                return Ok(AuthResult::fail(ReasonCode::ClientIdentifierNotValid));
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
            if self.sparkplug_aware && crate::sparkplug::is_reserved_topic(topic) {
                debug!(
                    client_id = %client_id_owned,
                    topic = %topic,
                    "publish to the Sparkplug certificate namespace denied (broker-only)"
                );
                return false;
            }

            if is_internal {
                return true;
            }

            if self
                .event_scope_entity
                .as_deref()
                .is_some_and(|scope_entity| is_scoped_event_topic(topic, scope_entity))
            {
                debug!(
                    client_id = %client_id_owned,
                    topic = %topic,
                    "publish to scoped event topic denied (service-only)"
                );
                return false;
            }

            if self.scoped_events && user_scope_segment(topic).is_some() {
                debug!(
                    client_id = %client_id_owned,
                    topic = %topic,
                    "publish to per-user event namespace denied (service-only)"
                );
                return false;
            }

            if let Err(reason) = check_topic_access(topic, true, is_admin) {
                if reason == BlockReason::AdminRequired {
                    let allowed = self
                        .inner
                        .authorize_publish(&client_id_owned, user_id, topic)
                        .await;
                    if !allowed {
                        debug!(
                            client_id = %client_id_owned,
                            topic = %topic,
                            "publish on AdminRequired topic denied by ACL"
                        );
                    }
                    return allowed;
                }
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
                return true;
            }

            if self.scoped_events
                && let Some(seg) = user_scope_segment(topic_filter)
                && !(is_admin || user_id == Some(seg))
            {
                debug!(
                    client_id = %client_id_owned,
                    topic_filter = %topic_filter,
                    "subscribe to another user's event namespace denied"
                );
                return false;
            }

            if let Err(reason) = check_topic_access(topic_filter, false, is_admin) {
                if reason == BlockReason::AdminRequired {
                    let allowed = self
                        .inner
                        .authorize_subscribe(&client_id_owned, user_id, topic_filter)
                        .await;
                    if !allowed {
                        debug!(
                            client_id = %client_id_owned,
                            topic_filter = %topic_filter,
                            "subscribe on AdminRequired topic denied by ACL"
                        );
                    }
                    return allowed;
                }
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
        if self.reserves_client_id(client_id, None) {
            debug!(client_id, "enhanced auth with a reserved client id denied");
            return Box::pin(async move {
                Ok(EnhancedAuthResult::fail(
                    auth_method,
                    ReasonCode::ClientIdentifierNotValid,
                ))
            });
        }
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

    struct DenyAllAuthProvider;

    impl AuthProvider for DenyAllAuthProvider {
        fn authenticate<'a>(
            &'a self,
            _connect: &'a ConnectPacket,
            _client_addr: SocketAddr,
        ) -> Pin<Box<dyn Future<Output = Result<AuthResult>> + Send + 'a>> {
            Box::pin(async { Ok(AuthResult::success()) })
        }

        fn authorize_publish<'a>(
            &'a self,
            _client_id: &str,
            _user_id: Option<&'a str>,
            _topic: &'a str,
        ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
            Box::pin(async { false })
        }

        fn authorize_subscribe<'a>(
            &'a self,
            _client_id: &str,
            _user_id: Option<&'a str>,
            _topic_filter: &'a str,
        ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
            Box::pin(async { false })
        }
    }

    fn create_test_provider(admin_users: HashSet<String>) -> TopicProtectionAuthProvider {
        TopicProtectionAuthProvider::new(Arc::new(AllowAllAuthProvider), admin_users)
    }

    fn create_deny_provider() -> TopicProtectionAuthProvider {
        TopicProtectionAuthProvider::new(Arc::new(DenyAllAuthProvider), HashSet::new())
    }

    fn create_test_provider_with_internal(internal_username: &str) -> TopicProtectionAuthProvider {
        TopicProtectionAuthProvider::new(Arc::new(AllowAllAuthProvider), HashSet::new())
            .with_internal_service_username(Some(internal_username.to_string()))
    }

    #[tokio::test]
    async fn sparkplug_aware_reserves_the_certificate_namespace_for_the_broker() {
        let admins: HashSet<String> = ["admin".to_string()].into_iter().collect();
        let provider = TopicProtectionAuthProvider::new(Arc::new(AllowAllAuthProvider), admins)
            .with_internal_service_username(Some("mqdb-internal-abc123".to_string()))
            .with_sparkplug_aware(true);
        let certificate = "$sparkplug/certificates/spBv1.0/G1/NBIRTH/E1";

        for user in [
            Some("edge"),
            Some("admin"),
            Some("mqdb-internal-abc123"),
            None,
        ] {
            assert!(
                !provider
                    .authorize_publish("client", user, certificate)
                    .await,
                "{user:?} must not publish a certificate when the broker stores them"
            );
        }
        assert!(
            provider
                .authorize_subscribe("host", Some("edge"), "$sparkplug/certificates/#")
                .await,
            "host applications must still be able to read certificates"
        );
        assert!(
            provider
                .authorize_publish("edge", Some("edge"), "spBv1.0/G1/NBIRTH/E1")
                .await,
            "births themselves stay publishable"
        );
    }

    #[tokio::test]
    async fn sparkplug_namespace_is_ordinary_without_sparkplug_aware() {
        let provider = create_test_provider(HashSet::new());
        assert!(
            provider
                .authorize_publish(
                    "bridge",
                    Some("bridge"),
                    "$sparkplug/certificates/spBv1.0/G1/NBIRTH/E1"
                )
                .await
        );
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
    async fn internal_service_user_is_not_subject_to_the_acl() {
        let provider = create_deny_provider()
            .with_internal_service_username(Some("mqdb-internal-abc123".to_string()));

        for topic in [
            "$DB/notes/create",
            "$DB/_presence/peer-1",
            "$DB/notes/events/n1",
        ] {
            assert!(
                provider
                    .authorize_publish("mqdb-internal-handler", Some("mqdb-internal-abc123"), topic)
                    .await,
                "{topic}"
            );
            assert!(
                provider
                    .authorize_subscribe(
                        "mqdb-internal-handler",
                        Some("mqdb-internal-abc123"),
                        topic
                    )
                    .await,
                "{topic}"
            );
        }
        assert!(
            provider
                .authorize_subscribe(
                    "mqdb-internal-handler",
                    Some("mqdb-internal-abc123"),
                    "$DB/#"
                )
                .await
        );
        assert!(
            !provider
                .authorize_subscribe("mqdb-internal-handler", Some("app"), "$DB/#")
                .await
        );
    }

    #[tokio::test]
    async fn internal_service_bypasses_verify_topics() {
        let provider = create_test_provider_with_internal("mqdb-internal-abc123");

        let result = provider
            .authorize_publish(
                "mqdb-http-oauth",
                Some("mqdb-internal-abc123"),
                "$DB/_verify/challenges/email",
            )
            .await;
        assert!(result);

        let result = provider
            .authorize_subscribe(
                "mqdb-http-oauth",
                Some("mqdb-internal-abc123"),
                "$DB/_verify/receipts/#",
            )
            .await;
        assert!(result);
    }

    #[tokio::test]
    async fn acl_grant_allows_admin_required_topics() {
        let provider = create_test_provider(HashSet::new());

        let result = provider
            .authorize_publish(
                "client-1",
                Some("email-verifier"),
                "$DB/_verify/challenges/email",
            )
            .await;
        assert!(result);

        let result = provider
            .authorize_subscribe("client-1", Some("email-verifier"), "$DB/_verify/receipts/#")
            .await;
        assert!(result);
    }

    #[tokio::test]
    async fn no_acl_grant_blocks_admin_required_topics() {
        let provider = create_deny_provider();

        let result = provider
            .authorize_publish("client-1", Some("alice"), "$DB/_verify/receipts/abc-123")
            .await;
        assert!(!result);

        let result = provider
            .authorize_subscribe("client-1", Some("alice"), "$DB/_verify/challenges/email")
            .await;
        assert!(!result);
    }

    #[tokio::test]
    async fn acl_grant_does_not_bypass_hard_blocks() {
        let provider = create_test_provider(HashSet::new());

        let result = provider
            .authorize_publish("client-1", Some("alice"), "_mqdb/cluster/heartbeat")
            .await;
        assert!(!result);

        let result = provider
            .authorize_subscribe("client-1", Some("alice"), "$DB/_idx/users")
            .await;
        assert!(!result);

        let result = provider
            .authorize_subscribe("client-1", Some("alice"), "$DB/_sessions/list")
            .await;
        assert!(!result);
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
    async fn non_admin_user_blocked_on_admin_topics_without_acl() {
        let provider = create_deny_provider();

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
    async fn external_client_can_subscribe_to_events() {
        let provider = create_test_provider(HashSet::new());

        let result = provider
            .authorize_subscribe("client-1", Some("alice"), "$DB/users/events/#")
            .await;
        assert!(result);
    }

    #[tokio::test]
    async fn external_client_cannot_publish_to_events() {
        let provider = create_test_provider(HashSet::new());

        let result = provider
            .authorize_publish("client-1", Some("alice"), "$DB/users/events/created")
            .await;
        assert!(!result);
    }

    #[tokio::test]
    async fn internal_service_can_publish_to_events() {
        let provider = create_test_provider_with_internal("mqdb-internal-abc123");

        let result = provider
            .authorize_publish(
                "mqdb-event-publisher",
                Some("mqdb-internal-abc123"),
                "$DB/users/events/created",
            )
            .await;
        assert!(result);
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

    fn create_scoped_provider() -> TopicProtectionAuthProvider {
        TopicProtectionAuthProvider::new(Arc::new(AllowAllAuthProvider), HashSet::new())
            .with_scoped_events(true)
    }

    #[tokio::test]
    async fn scoped_events_user_can_subscribe_own_namespace() {
        let provider = create_scoped_provider();
        assert!(
            provider
                .authorize_subscribe("c", Some("alice"), "$DB/u/alice/events/#")
                .await
        );
    }

    #[tokio::test]
    async fn scoped_events_user_cannot_subscribe_other_namespace() {
        let provider = create_scoped_provider();
        assert!(
            !provider
                .authorize_subscribe("c", Some("alice"), "$DB/u/bob/events/#")
                .await
        );
        assert!(
            !provider
                .authorize_subscribe("c", Some("alice"), "$DB/u/+/events/#")
                .await
        );
    }

    #[tokio::test]
    async fn scoped_events_non_service_cannot_publish_user_namespace() {
        let provider = create_scoped_provider();
        assert!(
            !provider
                .authorize_publish("c", Some("alice"), "$DB/u/alice/events/diagrams/1")
                .await
        );
    }

    #[tokio::test]
    async fn scoped_events_internal_service_publishes_user_namespace() {
        let provider =
            TopicProtectionAuthProvider::new(Arc::new(AllowAllAuthProvider), HashSet::new())
                .with_internal_service_username(Some("mqdb-internal".to_string()))
                .with_scoped_events(true);
        assert!(
            provider
                .authorize_publish("c", Some("mqdb-internal"), "$DB/u/alice/events/diagrams/1")
                .await
        );
    }

    #[tokio::test]
    async fn scoped_events_admin_can_subscribe_any_namespace() {
        let mut admins = HashSet::new();
        admins.insert("root".to_string());
        let provider = TopicProtectionAuthProvider::new(Arc::new(AllowAllAuthProvider), admins)
            .with_scoped_events(true);
        assert!(
            provider
                .authorize_subscribe("c", Some("root"), "$DB/u/alice/events/#")
                .await
        );
    }

    #[tokio::test]
    async fn scoped_events_off_does_not_restrict_user_namespace() {
        let provider = create_test_provider(HashSet::new());
        assert!(
            provider
                .authorize_subscribe("c", Some("alice"), "$DB/u/bob/events/#")
                .await
        );
    }

    #[tokio::test]
    async fn user_namespace_publish_blocked_even_with_scoped_events_off() {
        let provider = create_test_provider(HashSet::new());
        assert!(
            !provider
                .authorize_publish("c", Some("alice"), "$DB/u/bob/events/diagrams/1")
                .await
        );
        assert!(
            !provider
                .authorize_publish("c", Some("alice"), "$DB/u/alice/events/diagrams/1")
                .await
        );
    }

    #[tokio::test]
    async fn internal_service_publishes_user_namespace_with_scoped_events_off() {
        let provider = create_test_provider_with_internal("mqdb-internal");
        assert!(
            provider
                .authorize_publish("c", Some("mqdb-internal"), "$DB/u/alice/events/diagrams/1")
                .await
        );
    }

    #[tokio::test]
    async fn non_admin_can_publish_sub_channel() {
        let provider = create_test_provider(HashSet::new());
        assert!(
            provider
                .authorize_publish("c", Some("alice"), "$DB/_sub/subscribe")
                .await
        );
        assert!(
            provider
                .authorize_publish("c", Some("alice"), "$DB/_sub/abc123/heartbeat")
                .await
        );
    }

    #[tokio::test]
    async fn non_admin_cannot_subscribe_sub_channel() {
        let provider = create_test_provider(HashSet::new());
        assert!(
            !provider
                .authorize_subscribe("c", Some("alice"), "$DB/_sub/subscribe")
                .await
        );
        assert!(
            !provider
                .authorize_subscribe("c", Some("alice"), "$DB/_sub/#")
                .await
        );
    }

    #[tokio::test]
    async fn internal_service_can_consume_sub_channel() {
        let provider = create_test_provider_with_internal("mqdb-internal");
        assert!(
            provider
                .authorize_subscribe("c", Some("mqdb-internal"), "$DB/_sub/subscribe")
                .await
        );
    }

    fn connect_as(client_id: &str, username: &str) -> ConnectPacket {
        ConnectPacket {
            protocol_version: 5,
            clean_start: true,
            keep_alive: 60,
            client_id: client_id.to_string(),
            username: Some(username.to_string()),
            password: Some(b"secret".to_vec()),
            will: None,
            properties: mqtt5::Properties::default(),
            will_properties: mqtt5::Properties::default(),
        }
    }

    fn client_addr() -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 50000))
    }

    #[test]
    fn reserved_client_ids_cover_internal_clients_but_not_the_cli() {
        assert!(is_reserved_client_id("mqdb-admin-1"));
        assert!(is_reserved_client_id("mqdb-forward-2"));
        assert!(is_reserved_client_id("mqdb-internal-handler"));
        assert!(!is_reserved_client_id("mqdb-cli-0b5c"));
        assert!(!is_reserved_client_id("seat-holder-7"));
    }

    #[tokio::test]
    async fn only_the_service_user_connects_with_a_reserved_client_id() {
        let provider = create_test_provider_with_internal("mqdb-internal-abc123");

        let forged = provider
            .authenticate(&connect_as("mqdb-forward-bob", "bob"), client_addr())
            .await
            .unwrap();
        assert!(!forged.authenticated);
        assert_eq!(forged.reason_code, ReasonCode::ClientIdentifierNotValid);

        let internal = provider
            .authenticate(
                &connect_as("mqdb-admin-1", "mqdb-internal-abc123"),
                client_addr(),
            )
            .await
            .unwrap();
        assert!(internal.authenticated);

        for client_id in ["mqdb-cli-0b5c", "bob-phone"] {
            let result = provider
                .authenticate(&connect_as(client_id, "bob"), client_addr())
                .await
                .unwrap();
            assert!(result.authenticated, "{client_id} must connect");
        }
    }

    #[tokio::test]
    async fn reserved_client_ids_stay_open_without_a_service_user() {
        let provider = create_test_provider(HashSet::new());
        let result = provider
            .authenticate(&connect_as("mqdb-admin-1", "bob"), client_addr())
            .await
            .unwrap();
        assert!(result.authenticated);
    }

    #[tokio::test]
    async fn enhanced_auth_rejects_a_reserved_client_id() {
        let provider = create_test_provider_with_internal("mqdb-internal-abc123");
        let result = provider
            .authenticate_enhanced("SCRAM-SHA-256", None, "mqdb-forward-bob")
            .await
            .unwrap();
        assert_eq!(
            result.status,
            mqtt5::broker::auth::EnhancedAuthStatus::Failed
        );
        assert_eq!(result.reason_code, ReasonCode::ClientIdentifierNotValid);
    }

    fn create_event_scope_provider() -> TopicProtectionAuthProvider {
        create_test_provider_with_internal("mqdb-internal")
            .with_event_scope_entity(Some("lists".to_string()))
    }

    #[tokio::test]
    async fn scoped_event_topics_are_service_only() {
        let provider = create_event_scope_provider();
        for topic in [
            "$DB/lists/L1/events/created",
            "$DB/lists/L1/items/events/updated",
        ] {
            assert!(
                !provider.authorize_publish("c", Some("bob"), topic).await,
                "{topic} must be read-only"
            );
            assert!(
                provider
                    .authorize_publish("c", Some("mqdb-internal"), topic)
                    .await,
                "{topic} must stay open to the service"
            );
            assert!(provider.authorize_subscribe("c", Some("bob"), topic).await);
        }
    }

    #[tokio::test]
    async fn scope_entity_requests_stay_writable() {
        let provider = create_event_scope_provider();
        for topic in [
            "$DB/lists/create",
            "$DB/lists/L1/update",
            "$DB/lists/L1/delete",
            "$DB/items/L1/events/created",
        ] {
            assert!(
                provider.authorize_publish("c", Some("bob"), topic).await,
                "{topic} must be writable"
            );
        }
    }
}
