// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::database::Database;
use crate::topic_rules::{BlockReason, check_topic_access, is_change_event_topic, matches_pattern};
use mqdb_core::events::scope_value_from_level;
use mqdb_core::protocol::RESERVED_RECORD_ID;
use mqdb_core::types::OwnershipConfig;
use mqtt5::broker::auth::{AuthProvider, AuthResult, EnhancedAuthResult};
use mqtt5::error::Result;
use mqtt5::packet::connect::ConnectPacket;
use mqtt5::protocol::v5::reason_codes::ReasonCode;
use mqtt5::validation::{parse_shared_subscription, topic_matches_filter};
use std::collections::HashSet;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tracing::debug;

#[derive(Clone)]
pub struct ScopeAccess {
    pub db: Arc<Database>,
    pub ownership: Arc<OwnershipConfig>,
}

pub struct TopicProtectionAuthProvider {
    inner: Arc<dyn AuthProvider>,
    admin_users: Arc<HashSet<String>>,
    internal_service_username: Option<String>,
    event_scope_entity: Option<String>,
    scope_access: Option<ScopeAccess>,
    ownership: Arc<OwnershipConfig>,
    acl_configured: bool,
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
    topic
        .strip_prefix("$DB/")
        .and_then(|levels| levels.strip_prefix(scope_entity))
        .is_some_and(|levels| levels.starts_with('/'))
        && is_change_event_topic(topic)
}

#[derive(Debug, PartialEq, Eq)]
enum ScopedFilter<'a> {
    Elsewhere,
    AnyScope,
    OneScope(&'a str),
}

const SCOPE_RECORD_EVENT_LEVELS: [Option<&str>; 2] = [Some("events"), None];
const CHILD_RECORD_EVENT_LEVELS: [Option<&str>; 3] = [None, Some("events"), None];

fn filter_reaches_levels<'a>(
    mut filter: impl Iterator<Item = &'a str>,
    levels: &[Option<&str>],
) -> bool {
    for expected in levels {
        match filter.next() {
            Some("#") => return true,
            Some(level) if level == "+" || expected.is_none_or(|exact| exact == level) => {}
            _ => return false,
        }
    }
    matches!(filter.next(), None | Some("#"))
}

fn scoped_filter<'a>(filter: &'a str, scope_entity: &str) -> ScopedFilter<'a> {
    let mut levels = parse_shared_subscription(filter).0.split('/');
    if levels.next() != Some("$DB") {
        return ScopedFilter::Elsewhere;
    }
    match levels.next() {
        Some("#") => return ScopedFilter::AnyScope,
        Some(entity) if entity == "+" || entity == scope_entity => {}
        _ => return ScopedFilter::Elsewhere,
    }
    match levels.next() {
        None | Some("events") => ScopedFilter::Elsewhere,
        Some("#") => ScopedFilter::AnyScope,
        Some(level)
            if filter_reaches_levels(levels.clone(), &SCOPE_RECORD_EVENT_LEVELS)
                || filter_reaches_levels(levels, &CHILD_RECORD_EVENT_LEVELS) =>
        {
            if level == "+" {
                ScopedFilter::AnyScope
            } else {
                ScopedFilter::OneScope(level)
            }
        }
        Some(_) => ScopedFilter::Elsewhere,
    }
}

const COLLECTION_REQUEST_OPERATIONS: [&str; 3] = ["create", "list", "shared"];
const RECORD_REQUEST_OPERATIONS: [&str; 5] = ["update", "delete", "share", "unshare", "shares"];

fn filter_reaches_request(filter: &str, ownership: &OwnershipConfig) -> bool {
    let mut levels = parse_shared_subscription(filter).0.split('/');
    if levels.next() != Some("$DB") {
        return false;
    }
    if !levels
        .next()
        .is_some_and(|entity| ownership.controls_access_to(entity))
    {
        return false;
    }
    match levels.next() {
        Some("#") => true,
        None | Some(RESERVED_RECORD_ID) => false,
        Some(_) => {
            filter_reaches_levels(levels.clone(), &[])
                || RECORD_REQUEST_OPERATIONS
                    .iter()
                    .any(|operation| filter_reaches_levels(levels.clone(), &[Some(*operation)]))
        }
    }
}

const SECRET_REQUEST_TOPICS: [&str; 9] = [
    "$DB/_auth/password/change",
    "$DB/_auth/password/reset/start",
    "$DB/_auth/password/reset/submit",
    "$DB/_vault/enable",
    "$DB/_vault/unlock",
    "$DB/_vault/lock",
    "$DB/_vault/status",
    "$DB/_vault/change",
    "$DB/_vault/disable",
];
const VERIFICATION_TOPICS: &str = "$DB/_verify/#";

fn filter_reaches_secret_request(filter: &str) -> bool {
    SECRET_REQUEST_TOPICS
        .iter()
        .any(|topic| topic_matches_filter(topic, filter))
}

fn filter_crosses_entities_outside_events(filter: &str) -> bool {
    let mut levels = filter.split('/');
    if levels.next() != Some("$DB") {
        return false;
    }
    match levels.next() {
        Some("#") => true,
        Some("+") => {
            let mut below_entity = levels.peekable();
            below_entity.peek().is_some() && !below_entity.any(|level| level == "events")
        }
        _ => false,
    }
}

fn is_scope_record_filter(filter: &str, scope_entity: &str) -> bool {
    matches!(
        scoped_filter(filter, scope_entity),
        ScopedFilter::OneScope(level) if !COLLECTION_REQUEST_OPERATIONS.contains(&level)
    )
}

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
            scope_access: None,
            ownership: Arc::default(),
            acl_configured: false,
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
    pub fn with_scope_access(mut self, access: ScopeAccess) -> Self {
        self.scope_access = Some(access);
        self
    }

    #[must_use]
    pub fn with_ownership(mut self, ownership: Arc<OwnershipConfig>) -> Self {
        self.ownership = ownership;
        self
    }

    #[must_use]
    pub fn with_acl_configured(mut self, configured: bool) -> Self {
        self.acl_configured = configured;
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

    fn has_no_acl_grant(&self, topic: &str) -> bool {
        !self.acl_configured && matches_pattern(topic, VERIFICATION_TOPICS)
    }

    fn is_admin_user(&self, user_id: Option<&str>) -> bool {
        self.all_users_admin || user_id.is_some_and(|u| self.admin_users.contains(u))
    }

    async fn may_receive_scope_events(&self, user_id: Option<&str>, filter: &str) -> bool {
        let (Some(scope_entity), Some(access)) =
            (self.event_scope_entity.as_deref(), &self.scope_access)
        else {
            return true;
        };
        if !access.ownership.controls_access_to(scope_entity) {
            return true;
        }
        match (scoped_filter(filter, scope_entity), user_id) {
            (ScopedFilter::Elsewhere, _) => true,
            (ScopedFilter::OneScope(level), Some(user)) => access
                .db
                .check_read_access(
                    scope_entity,
                    &scope_value_from_level(level),
                    Some(user),
                    &access.ownership,
                )
                .await
                .is_ok(),
            _ => false,
        }
    }

    fn may_receive_requests(&self, filter: &str) -> bool {
        !filter_reaches_request(filter, &self.ownership)
            || (self.scope_access.is_some()
                && self
                    .event_scope_entity
                    .as_deref()
                    .is_some_and(|scope_entity| is_scope_record_filter(filter, scope_entity)))
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
                    let allowed = !self.has_no_acl_grant(topic)
                        && self
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

            let plain_filter = parse_shared_subscription(topic_filter).0;

            if filter_reaches_secret_request(plain_filter) {
                debug!(
                    client_id = %client_id_owned,
                    topic_filter = %topic_filter,
                    "subscribe to auth or vault requests denied"
                );
                return false;
            }

            if !is_admin && filter_crosses_entities_outside_events(plain_filter) {
                debug!(
                    client_id = %client_id_owned,
                    topic_filter = %topic_filter,
                    "subscribe across entities outside change events denied"
                );
                return false;
            }

            if !is_admin && !self.may_receive_scope_events(user_id, topic_filter).await {
                debug!(
                    client_id = %client_id_owned,
                    topic_filter = %topic_filter,
                    "subscribe to scoped events without view access denied"
                );
                return false;
            }

            if !is_admin && !self.may_receive_requests(topic_filter) {
                debug!(
                    client_id = %client_id_owned,
                    topic_filter = %topic_filter,
                    "subscribe to request topics of an entity with ownership denied"
                );
                return false;
            }

            if self.scoped_events
                && let Some(seg) = user_scope_segment(plain_filter)
                && !(is_admin || user_id == Some(seg))
            {
                debug!(
                    client_id = %client_id_owned,
                    topic_filter = %topic_filter,
                    "subscribe to another user's event namespace denied"
                );
                return false;
            }

            if let Err(reason) = check_topic_access(plain_filter, false, is_admin) {
                if reason == BlockReason::AdminRequired {
                    let allowed = !self.has_no_acl_grant(plain_filter)
                        && self
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
        let provider = create_test_provider(HashSet::new()).with_acl_configured(true);

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
    async fn scoped_event_topics_are_service_only_whatever_the_depth_of_the_scope() {
        let provider = create_event_scope_provider();
        for topic in [
            "$DB/lists/a/b/items/events/FORGED",
            "$DB/lists/a/b/events/FORGED",
            "$DB/lists/a/b/c/d/e/items/events/created",
            "$DB/lists/a%2Fb/items/events/created",
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
        }
    }

    #[test]
    fn every_real_scoped_topic_is_classified_under_its_own_scope() {
        use mqdb_core::events::{ChangeEvent, scope_topic_level};
        for value in ["L1", "a/b", "a+b", "#", "50%", "events", "a%2Fb", ""] {
            let scope = Some(("lists".to_string(), value.to_string()));
            let level = scope_topic_level(value);
            for entity in ["lists", "items", "events"] {
                let event = ChangeEvent::create(entity.into(), "r1".into(), serde_json::json!({}))
                    .with_scope(scope.clone());
                let topic = event.event_topic(0);
                assert!(is_scoped_event_topic(&topic, "lists"), "{topic}");
                assert_eq!(
                    scoped_filter(&topic, "lists"),
                    ScopedFilter::OneScope(&level),
                    "{topic}"
                );
                assert_eq!(scope_value_from_level(&level), value, "{topic}");
            }
        }
    }

    #[test]
    fn a_filter_reaching_a_scoped_event_is_never_classified_elsewhere() {
        use mqdb_core::events::ChangeEvent;
        use mqtt5::validation::topic_matches_filter;
        let topics: Vec<String> = ["L1", "a/b", "events"]
            .into_iter()
            .flat_map(|value| {
                ["lists", "items"].map(|entity| {
                    ChangeEvent::create(entity.into(), "r1".into(), serde_json::json!({}))
                        .with_scope(Some(("lists".to_string(), value.to_string())))
                        .event_topic(0)
                })
            })
            .collect();
        let mut filters = vec!["#".to_string(), "+/lists/L1/#".to_string()];
        for entity in ["lists", "+"] {
            filters.push(format!("$DB/{entity}/#"));
            for value in ["L1", "+", "events", "%65vents", "a%2Fb"] {
                for tail in [
                    "#",
                    "events/#",
                    "+/#",
                    "events/created",
                    "events/+",
                    "+/created",
                    "+/+",
                    "items/events/#",
                    "items/events/created",
                    "items/+/created",
                    "+/events/#",
                    "+/events/+",
                    "+/+/created",
                    "+/+/#",
                    "+/+/+",
                    "items/#",
                ] {
                    filters.push(format!("$DB/{entity}/{value}/{tail}"));
                }
            }
        }
        filters.push("$DB/#".to_string());
        filters.push("$DB/+/#".to_string());
        let mut reached = 0;
        for filter in &filters {
            for topic in &topics {
                if !topic_matches_filter(topic, filter) {
                    continue;
                }
                reached += 1;
                let scope_level = topic.split('/').nth(2).unwrap();
                match scoped_filter(filter, "lists") {
                    ScopedFilter::Elsewhere => panic!("{filter} reaches {topic}"),
                    ScopedFilter::AnyScope => {}
                    ScopedFilter::OneScope(level) => {
                        assert_eq!(level, scope_level, "{filter} reaches {topic}");
                    }
                }
                assert_eq!(
                    scoped_filter(&format!("$share/g/{filter}"), "lists"),
                    scoped_filter(filter, "lists"),
                    "{filter}"
                );
            }
        }
        assert!(
            reached > 100,
            "only {reached} filter and topic pairs matched"
        );
    }

    #[test]
    fn filters_that_reach_no_scoped_event_are_elsewhere() {
        for filter in [
            "$DB/lists",
            "$DB/+",
            "$DB/lists/create",
            "$DB/lists/L1",
            "$DB/lists/L1/update",
            "$DB/lists/+/update",
            "$DB/lists/L1/items/update",
            "$DB/lists/events/#",
            "$DB/+/events/#",
            "$DB/lists/events/L1",
            "$DB/items/events/#",
            "$DB/items/L1/events/created",
            "$DB/u/alice/events/#",
            "$share/g/$DB/items/events/#",
            "resp/alice/1",
            "#",
            "+/lists/L1/#",
        ] {
            assert_eq!(
                scoped_filter(filter, "lists"),
                ScopedFilter::Elsewhere,
                "{filter}"
            );
        }
    }

    async fn scope_access_provider() -> (tempfile::TempDir, TopicProtectionAuthProvider, Database) {
        use mqdb_core::types::ScopeConfig;
        use serde_json::json;
        let tmp = tempfile::TempDir::new().unwrap();
        let db = Database::open_without_background_tasks(tmp.path())
            .await
            .unwrap();
        let ownership = OwnershipConfig::parse("lists=ownerId")
            .unwrap()
            .with_admin_users(HashSet::from(["root".to_string()]))
            .with_derivations("items=listId>lists")
            .unwrap();
        for (id, owner) in [("L1", "alice"), ("a/b", "alice"), ("a", "mallory")] {
            db.create(
                "lists".to_string(),
                json!({"id": id, "ownerId": owner}),
                None,
                None,
                None,
                &ScopeConfig::default(),
            )
            .await
            .unwrap();
        }
        let provider = TopicProtectionAuthProvider::new(
            Arc::new(AllowAllAuthProvider),
            HashSet::from(["root".to_string()]),
        )
        .with_internal_service_username(Some("mqdb-internal".to_string()))
        .with_event_scope_entity(Some("lists".to_string()))
        .with_ownership(Arc::new(ownership.clone()))
        .with_scope_access(ScopeAccess {
            db: Arc::new(db.clone()),
            ownership: Arc::new(ownership),
        });
        (tmp, provider, db)
    }

    #[tokio::test]
    async fn scoped_events_need_view_access_to_the_scope_record() {
        let (_tmp, provider, _db) = scope_access_provider().await;
        for filter in [
            "$DB/lists/L1/#",
            "$DB/lists/L1/events/#",
            "$DB/lists/L1/items/events/created",
            "$share/g/$DB/lists/L1/#",
            "$DB/lists/a%2Fb/#",
        ] {
            assert!(
                provider
                    .authorize_subscribe("c", Some("alice"), filter)
                    .await,
                "the owner must receive {filter}"
            );
            assert!(
                !provider
                    .authorize_subscribe("c", Some("mallory"), filter)
                    .await,
                "a stranger must not receive {filter}"
            );
            assert!(
                !provider.authorize_subscribe("c", None, filter).await,
                "a client with no user must not receive {filter}"
            );
            assert!(
                provider
                    .authorize_subscribe("c", Some("root"), filter)
                    .await
            );
            assert!(
                provider
                    .authorize_subscribe("c", Some("mqdb-internal"), filter)
                    .await
            );
        }
    }

    #[tokio::test]
    async fn a_share_opens_scoped_events_and_a_revoke_closes_new_subscriptions() {
        use crate::database::{GranteeIdentity, ShareActor};
        let (_tmp, provider, db) = scope_access_provider().await;
        let filter = "$DB/lists/L1/#";
        let ownership = OwnershipConfig::parse("lists=ownerId").unwrap();
        for (grantee, permission) in [("vera", "view"), ("ed", "edit")] {
            assert!(
                !provider
                    .authorize_subscribe("c", Some(grantee), filter)
                    .await
            );
            db.share_grant(
                "lists",
                "L1",
                GranteeIdentity {
                    key: grantee,
                    resolved: Some(grantee),
                    display: None,
                },
                permission,
                ShareActor {
                    sender: Some("alice"),
                    ownership: &ownership,
                },
                false,
            )
            .await
            .unwrap();
            assert!(
                provider
                    .authorize_subscribe("c", Some(grantee), filter)
                    .await,
                "a {permission} grantee must receive scoped events"
            );
        }
        let vera = GranteeIdentity {
            key: "vera",
            resolved: Some("vera"),
            display: None,
        };
        db.share_revoke(
            "lists",
            "L1",
            vera,
            ShareActor {
                sender: Some("alice"),
                ownership: &ownership,
            },
            false,
        )
        .await
        .unwrap();
        assert!(
            !provider
                .authorize_subscribe("c", Some("vera"), filter)
                .await
        );
    }

    #[tokio::test]
    async fn a_scope_that_does_not_exist_yet_is_closed_to_non_admins() {
        let (_tmp, provider, _db) = scope_access_provider().await;
        let filter = "$DB/lists/not-created-yet/#";
        assert!(
            !provider
                .authorize_subscribe("c", Some("alice"), filter)
                .await
        );
        assert!(
            provider
                .authorize_subscribe("c", Some("root"), filter)
                .await
        );
    }

    #[tokio::test]
    async fn wildcards_across_scopes_are_admin_only() {
        let (_tmp, provider, _db) = scope_access_provider().await;
        for filter in [
            "$DB/lists/#",
            "$DB/lists/+/#",
            "$DB/+/+/events/#",
            "$DB/lists/+/events/#",
            "$DB/lists/+/items/events/created",
            "$DB/+/+/+/events/#",
            "$DB/lists/+/+/+",
            "$share/g/$DB/lists/+/#",
        ] {
            assert!(
                !provider
                    .authorize_subscribe("c", Some("alice"), filter)
                    .await,
                "{filter} must be closed to non-admins"
            );
            assert!(
                provider
                    .authorize_subscribe("c", Some("root"), filter)
                    .await
            );
        }
    }

    #[tokio::test]
    async fn owning_a_prefix_of_a_scope_value_grants_nothing_over_it() {
        let (_tmp, provider, _db) = scope_access_provider().await;
        assert!(
            provider
                .authorize_subscribe("c", Some("mallory"), "$DB/lists/a/#")
                .await
        );
        assert!(
            !provider
                .authorize_subscribe("c", Some("mallory"), "$DB/lists/a%2Fb/#")
                .await
        );
        assert!(!mqtt5::validation::topic_matches_filter(
            &mqdb_core::events::ChangeEvent::create(
                "items".into(),
                "r1".into(),
                serde_json::json!({})
            )
            .with_scope(Some(("lists".to_string(), "a/b".to_string())))
            .event_topic(0),
            "$DB/lists/a/#"
        ));
    }

    #[tokio::test]
    async fn other_subscriptions_are_untouched_by_scope_access() {
        let (_tmp, provider, _db) = scope_access_provider().await;
        for filter in [
            "$DB/items/events/#",
            "$DB/+/events/#",
            "$DB/lists/events/#",
            "$DB/u/alice/events/#",
            "resp/alice/1",
        ] {
            assert!(
                provider
                    .authorize_subscribe("c", Some("mallory"), filter)
                    .await,
                "{filter} must stay open"
            );
        }
    }

    #[tokio::test]
    async fn a_scope_entity_without_access_control_stays_open() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = Database::open_without_background_tasks(tmp.path())
            .await
            .unwrap();
        let provider = create_event_scope_provider().with_scope_access(ScopeAccess {
            db: Arc::new(db),
            ownership: Arc::new(OwnershipConfig::parse("items=ownerId").unwrap()),
        });
        for filter in ["$DB/lists/L1/#", "$DB/lists/+/#", "$DB/lists/#"] {
            assert!(
                provider.authorize_subscribe("c", Some("bob"), filter).await,
                "{filter}"
            );
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

    fn notes_ownership() -> Arc<OwnershipConfig> {
        Arc::new(
            OwnershipConfig::parse("notes=ownerId,events=ownerId")
                .unwrap()
                .with_derivations("items=noteId>notes")
                .unwrap(),
        )
    }

    fn request_protection_provider() -> TopicProtectionAuthProvider {
        TopicProtectionAuthProvider::new(
            Arc::new(AllowAllAuthProvider),
            HashSet::from(["root".to_string()]),
        )
        .with_internal_service_username(Some("mqdb-internal".to_string()))
        .with_ownership(notes_ownership())
    }

    const LITERAL_REQUEST_FILTERS: [&str; 18] = [
        "$DB/notes/create",
        "$DB/notes/list",
        "$DB/notes/shared",
        "$DB/notes/n1",
        "$DB/notes/n1/update",
        "$DB/notes/n1/delete",
        "$DB/notes/n1/share",
        "$DB/notes/n1/unshare",
        "$DB/notes/n1/shares",
        "$DB/notes/+",
        "$DB/notes/#",
        "$DB/notes/+/#",
        "$DB/notes/+/update",
        "$DB/notes/+/+",
        "$DB/notes/n1/#",
        "$DB/notes/n1/+",
        "$DB/items/create",
        "$DB/events/create",
    ];
    const ENTITY_WILDCARD_FILTERS: [&str; 6] = [
        "$DB/+/create",
        "$DB/+/list",
        "$DB/+/n1",
        "$DB/+/n1/update",
        "$DB/+/+/share",
        "$DB/+/n1/#",
    ];
    const SECRET_REACHING_FILTERS: [&str; 14] = [
        "$DB/_auth/#",
        "$DB/_auth/password/change",
        "$DB/_auth/password/reset/+",
        "$DB/_vault/#",
        "$DB/_vault/unlock",
        "$DB/_vault/+",
        "$DB/#",
        "$DB/+/#",
        "$DB/+/+",
        "$DB/+/+/+",
        "$DB/+/unlock",
        "$DB/+/password/#",
        "$DB/+/+/change",
        "$DB/+/+/reset/submit",
    ];

    async fn assert_subscribe(
        provider: &TopicProtectionAuthProvider,
        user: Option<&str>,
        filter: &str,
        expected: bool,
    ) {
        let shared = format!("$share/g/{filter}");
        for candidate in [filter, shared.as_str()] {
            assert_eq!(
                provider.authorize_subscribe("c", user, candidate).await,
                expected,
                "{user:?} on {candidate}"
            );
        }
    }

    #[tokio::test]
    async fn request_filters_are_refused_to_non_admins_on_any_acl() {
        let provider = request_protection_provider();
        for filter in LITERAL_REQUEST_FILTERS
            .iter()
            .chain(&ENTITY_WILDCARD_FILTERS)
        {
            for user in [Some("mallory"), None] {
                assert_subscribe(&provider, user, filter, false).await;
            }
            for user in ["root", "mqdb-internal"] {
                assert_subscribe(&provider, Some(user), filter, true).await;
            }
        }
    }

    #[tokio::test]
    async fn secret_requests_are_closed_to_every_client_but_the_service() {
        let with_ownership = request_protection_provider();
        let without_ownership = TopicProtectionAuthProvider::new(
            Arc::new(AllowAllAuthProvider),
            HashSet::from(["root".to_string()]),
        )
        .with_internal_service_username(Some("mqdb-internal".to_string()));
        let all_admins =
            TopicProtectionAuthProvider::new(Arc::new(AllowAllAuthProvider), HashSet::new())
                .with_all_users_admin(true);
        for filter in SECRET_REACHING_FILTERS {
            for provider in [&with_ownership, &without_ownership] {
                assert_subscribe(provider, Some("mallory"), filter, false).await;
                assert_subscribe(provider, None, filter, false).await;
                assert_subscribe(provider, Some("root"), filter, false).await;
                assert_subscribe(provider, Some("mqdb-internal"), filter, true).await;
            }
            assert_subscribe(&all_admins, Some("anyone"), filter, false).await;
        }
        for topic in SECRET_REQUEST_TOPICS {
            assert!(
                without_ownership
                    .authorize_publish("c", Some("mallory"), topic)
                    .await,
                "{topic} must stay publishable"
            );
            assert!(
                without_ownership
                    .authorize_publish("c", Some("root"), topic)
                    .await,
                "{topic} must stay publishable"
            );
        }
    }

    #[test]
    fn the_secret_topic_list_names_every_auth_and_vault_operation() {
        use mqdb_core::protocol::{AdminOperation, parse_admin_topic};
        let mut seen = HashSet::new();
        for topic in SECRET_REQUEST_TOPICS {
            let operation = parse_admin_topic(topic);
            let name = match operation {
                Some(AdminOperation::PasswordChange) => "password change",
                Some(AdminOperation::PasswordResetStart) => "reset start",
                Some(AdminOperation::PasswordResetSubmit) => "reset submit",
                Some(AdminOperation::VaultEnable) => "vault enable",
                Some(AdminOperation::VaultUnlock) => "vault unlock",
                Some(AdminOperation::VaultLock) => "vault lock",
                Some(AdminOperation::VaultStatus) => "vault status",
                Some(AdminOperation::VaultChange) => "vault change",
                Some(AdminOperation::VaultDisable) => "vault disable",
                _ => panic!("{topic} is not an auth or vault request"),
            };
            assert!(seen.insert(name), "{topic} is listed twice");
        }
        assert_eq!(seen.len(), SECRET_REQUEST_TOPICS.len());
    }

    #[tokio::test]
    async fn every_protection_tier_applies_to_shared_subscriptions() {
        let admins = HashSet::from(["root".to_string()]);
        let provider = TopicProtectionAuthProvider::new(Arc::new(AllowAllAuthProvider), admins)
            .with_internal_service_username(Some("mqdb-internal".to_string()))
            .with_scoped_events(true);
        let mut filters: Vec<String> = crate::topic_rules::PROTECTED_TOPICS
            .iter()
            .map(|rule| rule.pattern.replace("p+", "p3"))
            .collect();
        filters.extend(
            [
                "$DB/_admin/#",
                "$DB/_sessions/list",
                "$DB/_oauth_tokens/#",
                "$DB/_identities/#",
                "$DB/_identity_links/#",
                "$DB/_credentials/#",
                "$DB/_verification_challenges/#",
                "$DB/_shares/#",
                "$DB/u/alice/events/#",
                "sensors/room1",
                "sensors/#",
            ]
            .map(str::to_string),
        );
        for filter in &filters {
            for user in [Some("mallory"), Some("root"), Some("mqdb-internal"), None] {
                assert_eq!(
                    provider
                        .authorize_subscribe("c", user, &format!("$share/g/{filter}"))
                        .await,
                    provider.authorize_subscribe("c", user, filter).await,
                    "{user:?} on {filter}"
                );
            }
        }
        assert!(
            !provider
                .authorize_subscribe("c", Some("mallory"), "$share/g/$DB/_admin/#")
                .await
        );
        assert!(
            !provider
                .authorize_subscribe("c", Some("mallory"), "$share/g/$DB/u/alice/events/#")
                .await
        );
        assert!(
            provider
                .authorize_subscribe("c", Some("mallory"), "$share/g/sensors/#")
                .await
        );
    }

    #[tokio::test]
    async fn verification_topics_need_a_real_acl_grant() {
        for (acl_configured, expected) in [(false, false), (true, true)] {
            let provider = create_test_provider_with_internal("mqdb-internal")
                .with_acl_configured(acl_configured);
            for filter in ["$DB/_verify/#", "$DB/_verify/challenges/email"] {
                assert_subscribe(&provider, Some("mallory"), filter, expected).await;
                assert_subscribe(&provider, Some("mqdb-internal"), filter, true).await;
            }
            assert_eq!(
                provider
                    .authorize_publish("c", Some("mallory"), "$DB/_verify/receipts/r1")
                    .await,
                expected
            );
        }
        let admin_provider = create_test_provider(HashSet::from(["root".to_string()]));
        assert_subscribe(&admin_provider, Some("root"), "$DB/_verify/#", true).await;
    }

    #[tokio::test]
    async fn entity_wildcards_reach_no_internal_traffic_without_ownership() {
        let provider = create_test_provider(HashSet::new());
        let internal_topics = [
            "$DB/_verify/challenges/email",
            "$DB/_verify/receipts/r1",
            "$DB/_oauth_tokens/k1/update",
            "$DB/_oauth_tokens/k1/delete",
            "$DB/_identity_links/k1/update",
            "$DB/_identity_links/k1/delete",
            "$DB/_admin/users/add",
            "$DB/_admin/acl/rules/add",
            "$DB/_admin/schema/notes/set",
            "$DB/_sub/subscribe",
        ];
        let filters = [
            "$DB/#",
            "$DB/+/#",
            "$DB/+/+",
            "$DB/+/+/+",
            "$DB/+/+/+/+",
            "$DB/+/+/#",
            "$DB/+/k1/#",
            "$DB/+/k1/update",
            "$DB/+/+/update",
            "$DB/+/+/delete",
            "$DB/+/challenges/email",
            "$DB/+/challenges/+",
            "$DB/+/receipts/#",
            "$DB/+/users/add",
            "$DB/+/acl/#",
            "$DB/+/schema/+/set",
            "$DB/+/subscribe",
        ];
        for filter in filters {
            assert!(
                internal_topics
                    .iter()
                    .any(|topic| topic_matches_filter(topic, filter)),
                "{filter} reaches nothing, so this check proves nothing"
            );
            assert_subscribe(&provider, Some("mallory"), filter, false).await;
        }
    }

    #[tokio::test]
    async fn filters_that_reach_no_request_of_an_owned_entity_stay_open() {
        let provider = request_protection_provider().with_scoped_events(true);
        for filter in [
            "$DB/notes/events/#",
            "$DB/notes/events/+",
            "$DB/notes/events/created",
            "$DB/notes/events/p3/n1",
            "$DB/notes/events",
            "$DB/notes/+/events/#",
            "$DB/notes/n1/events/#",
            "$DB/notes/n1/items/events/created",
            "$DB/notes/n1/replies/#",
            "$DB/notes/replies/r1/x",
            "$DB/notes",
            "$DB/+",
            "$DB/+/events/#",
            "$DB/+/events/+",
            "$DB/+/+/events/#",
            "$DB/+/+/+/events/#",
            "$DB/events/events/#",
            "$DB/u/mallory/events/#",
            "$DB/_presence/#",
            "$DB/public/create",
            "$DB/public/#",
            "$DB/clients/mallory/#",
            "$DB/events2/create",
            "resp/mallory/1",
            "resp/#",
            "#",
            "+/notes/create",
            "+/+/+",
        ] {
            assert_subscribe(&provider, Some("mallory"), filter, true).await;
        }
    }

    #[tokio::test]
    async fn literal_request_filters_stay_open_without_ownership() {
        let provider = TopicProtectionAuthProvider::new(
            Arc::new(AllowAllAuthProvider),
            HashSet::from(["root".to_string()]),
        );
        for filter in LITERAL_REQUEST_FILTERS {
            assert_subscribe(&provider, Some("mallory"), filter, true).await;
        }
    }

    #[tokio::test]
    async fn derivation_alone_protects_requests() {
        let provider = TopicProtectionAuthProvider::new(
            Arc::new(AllowAllAuthProvider),
            HashSet::from(["root".to_string()]),
        )
        .with_ownership(Arc::new(
            OwnershipConfig::default()
                .with_derivations("items=noteId>notes")
                .unwrap(),
        ));
        assert_subscribe(&provider, Some("mallory"), "$DB/items/create", false).await;
        assert_subscribe(&provider, Some("mallory"), "$DB/notes/create", true).await;
    }

    fn request_topics(entity: &str, id: &str) -> Vec<String> {
        let mut topics = vec![
            format!("$DB/{entity}/create"),
            format!("$DB/{entity}/list"),
            format!("$DB/{entity}/shared"),
            format!("$DB/{entity}/{id}"),
        ];
        topics.extend(
            RECORD_REQUEST_OPERATIONS
                .iter()
                .map(|operation| format!("$DB/{entity}/{id}/{operation}")),
        );
        topics
    }

    #[test]
    fn filter_reach_agrees_with_the_broker_matcher_and_the_request_parser() {
        use mqdb_core::protocol::parse_db_topic;

        let ownership = notes_ownership();
        let mut topics = vec![
            "$DB/notes/events/created".to_string(),
            "$DB/notes/events/p3/n1".to_string(),
            "$DB/notes/n1/events/created".to_string(),
            "$DB/notes/n1/items/events/created".to_string(),
            "$DB/notes/replies/r1/x".to_string(),
        ];
        for entity in ["notes", "items", "events", "public"] {
            for id in [
                "n1", "create", "list", "shared", "events", "update", "x", "y",
            ] {
                topics.extend(request_topics(entity, id));
            }
        }

        let first_levels = ["notes", "items", "events", "public", "+", "#"];
        let second_levels = [
            "create", "list", "shared", "n1", "events", "update", "+", "#",
        ];
        let tails = [
            "update", "delete", "share", "unshare", "shares", "events", "x", "+", "#", "x/y",
            "+/+", "+/#", "update/#", "events/#", "x/#",
        ];
        let mut filters = vec!["$DB".to_string(), "#".to_string(), "+/+/+".to_string()];
        for first in first_levels {
            filters.push(format!("$DB/{first}"));
            if first == "#" {
                continue;
            }
            for second in second_levels {
                filters.push(format!("$DB/{first}/{second}"));
                if second != "#" {
                    filters.extend(
                        tails
                            .iter()
                            .map(|tail| format!("$DB/{first}/{second}/{tail}")),
                    );
                }
            }
        }

        let mut refused_filters = 0;
        let mut open_filters = 0;
        for filter in &filters {
            let reaches_owned_request = topics.iter().any(|topic| {
                topic_matches_filter(topic, filter)
                    && !is_change_event_topic(topic)
                    && parse_db_topic(topic)
                        .is_some_and(|request| ownership.controls_access_to(&request.entity))
            });
            let refused = filter_reaches_request(filter, &ownership)
                || filter_crosses_entities_outside_events(filter);
            assert_eq!(
                refused,
                reaches_owned_request || filter_crosses_entities_outside_events(filter),
                "{filter}"
            );
            assert_eq!(
                filter_reaches_request(&format!("$share/g/{filter}"), &ownership),
                filter_reaches_request(filter, &ownership),
                "$share/g/{filter}"
            );
            if refused {
                refused_filters += 1;
            } else {
                open_filters += 1;
            }
            assert!(
                !filter_reaches_request(filter, &OwnershipConfig::default()),
                "{filter}"
            );
        }
        assert!(
            refused_filters > 100 && open_filters > 100,
            "{refused_filters} filters refused and {open_filters} left open"
        );
    }

    #[tokio::test]
    async fn a_scope_viewer_keeps_the_record_filter_and_nothing_wider() {
        let (_tmp, provider, db) = scope_access_provider().await;
        for filter in [
            "$DB/lists/L1/#",
            "$DB/lists/L1/+/#",
            "$DB/lists/L1/events/#",
            "$DB/lists/L1/+/events/#",
            "$share/g/$DB/lists/L1/#",
        ] {
            assert!(
                provider
                    .authorize_subscribe("c", Some("alice"), filter)
                    .await,
                "the owner must keep {filter}"
            );
        }
        for filter in [
            "$DB/lists/L1",
            "$DB/lists/L1/update",
            "$DB/lists/L1/+",
            "$DB/+/L1/#",
            "$share/g/$DB/+/L1/#",
        ] {
            assert!(
                !provider
                    .authorize_subscribe("c", Some("alice"), filter)
                    .await,
                "{filter} reaches requests the view check does not cover"
            );
            assert!(
                provider
                    .authorize_subscribe("c", Some("root"), filter)
                    .await,
                "{filter}"
            );
        }

        for id in ["create", "list", "shared"] {
            db.create(
                "lists".to_string(),
                serde_json::json!({"id": id, "ownerId": "mallory"}),
                None,
                None,
                None,
                &mqdb_core::types::ScopeConfig::default(),
            )
            .await
            .unwrap();
            assert!(
                !provider
                    .authorize_subscribe("c", Some("mallory"), &format!("$DB/lists/{id}/#"))
                    .await,
                "a record named {id} must not open everyone's {id} requests"
            );
            assert!(
                provider
                    .authorize_subscribe("c", Some("mallory"), &format!("$DB/lists/{id}/events/#"))
                    .await,
                "its owner must keep its events"
            );
        }
    }

    #[tokio::test]
    async fn without_a_view_check_the_record_filter_is_refused() {
        let provider =
            request_protection_provider().with_event_scope_entity(Some("notes".to_string()));
        assert!(
            !provider
                .authorize_subscribe("c", Some("alice"), "$DB/notes/n1/#")
                .await
        );
        assert!(
            provider
                .authorize_subscribe("c", Some("alice"), "$DB/notes/n1/events/#")
                .await
        );
    }
}
