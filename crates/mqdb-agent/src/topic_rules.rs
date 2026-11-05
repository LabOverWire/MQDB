// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use mqdb_core::protocol::parse_admin_topic;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtectionTier {
    BlockAll,
    ReadOnly,
    WriteOnly,
    AdminRequired,
}

#[derive(Debug, Clone, Copy)]
pub struct TopicRule {
    pub pattern: &'static str,
    pub tier: ProtectionTier,
}

pub const PROTECTED_TOPICS: &[TopicRule] = &[
    TopicRule {
        pattern: "_mqdb/#",
        tier: ProtectionTier::BlockAll,
    },
    TopicRule {
        pattern: "$DB/_idx/#",
        tier: ProtectionTier::BlockAll,
    },
    TopicRule {
        pattern: "$DB/_unique/#",
        tier: ProtectionTier::BlockAll,
    },
    TopicRule {
        pattern: "$DB/_fk/#",
        tier: ProtectionTier::BlockAll,
    },
    TopicRule {
        pattern: "$DB/_query/#",
        tier: ProtectionTier::BlockAll,
    },
    TopicRule {
        pattern: "$DB/p+/#",
        tier: ProtectionTier::BlockAll,
    },
    TopicRule {
        pattern: "$DB/+/events/#",
        tier: ProtectionTier::ReadOnly,
    },
    TopicRule {
        pattern: "$DB/u/#",
        tier: ProtectionTier::ReadOnly,
    },
    TopicRule {
        pattern: "$DB/_presence/#",
        tier: ProtectionTier::ReadOnly,
    },
    TopicRule {
        pattern: "$DB/_sub/#",
        tier: ProtectionTier::WriteOnly,
    },
    TopicRule {
        pattern: "$DB/_auth/#",
        tier: ProtectionTier::WriteOnly,
    },
    TopicRule {
        pattern: "$DB/_vault/#",
        tier: ProtectionTier::WriteOnly,
    },
    TopicRule {
        pattern: "$SYS/mqdb/cluster/#",
        tier: ProtectionTier::AdminRequired,
    },
    TopicRule {
        pattern: "$SYS/#",
        tier: ProtectionTier::ReadOnly,
    },
    TopicRule {
        pattern: "$DB/_verify/#",
        tier: ProtectionTier::AdminRequired,
    },
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockReason {
    InternalTopicBlocked,
    ReadOnlyTopic,
    WriteOnlyTopic,
    AdminRequired,
    InternalEntityAccess,
}

impl fmt::Display for BlockReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InternalTopicBlocked => write!(f, "internal topic blocked"),
            Self::ReadOnlyTopic => write!(f, "read-only topic"),
            Self::WriteOnlyTopic => write!(f, "write-only topic"),
            Self::AdminRequired => write!(f, "admin role required"),
            Self::InternalEntityAccess => write!(f, "internal entity access denied"),
        }
    }
}

#[must_use]
pub fn matches_pattern(topic: &str, pattern: &str) -> bool {
    let topic_parts: Vec<&str> = topic.split('/').collect();
    let pattern_parts: Vec<&str> = pattern.split('/').collect();

    let mut t_idx = 0;
    let mut p_idx = 0;

    while p_idx < pattern_parts.len() {
        let p_part = pattern_parts[p_idx];

        if p_part == "#" {
            return true;
        }

        if t_idx >= topic_parts.len() {
            return false;
        }

        let t_part = topic_parts[t_idx];

        if p_part == "+" {
            t_idx += 1;
            p_idx += 1;
            continue;
        }

        if p_part.contains('+') {
            if let Some(prefix) = p_part.strip_suffix('+') {
                if !t_part.starts_with(prefix) {
                    return false;
                }
                let remainder = &t_part[prefix.len()..];
                if remainder.is_empty() || !remainder.chars().all(|c| c.is_ascii_digit()) {
                    return false;
                }
            } else {
                return false;
            }
            t_idx += 1;
            p_idx += 1;
            continue;
        }

        if t_part != p_part {
            return false;
        }

        t_idx += 1;
        p_idx += 1;
    }

    t_idx == topic_parts.len()
}

#[must_use]
pub fn is_change_event_topic(topic: &str) -> bool {
    let Some(levels) = topic.strip_prefix("$DB/") else {
        return false;
    };
    levels.split('/').skip(1).any(|level| level == "events")
        && !(levels.starts_with("_admin/") && parse_admin_topic(topic).is_some())
}

fn find_matching_rule(topic: &str) -> Option<&'static TopicRule> {
    PROTECTED_TOPICS
        .iter()
        .find(|rule| matches_pattern(topic, rule.pattern))
}

fn is_internal_entity_topic(topic: &str) -> bool {
    if !topic.starts_with("$DB/") {
        return false;
    }
    let rest = &topic[4..];
    let entity = rest.split('/').next().unwrap_or("");
    if !entity.starts_with('_') || entity.is_empty() {
        return false;
    }
    !matches!(entity, "_health" | "_vault" | "_auth" | "_verify")
}

/// # Errors
///
/// Returns `Err(BlockReason)` if the topic is protected and access is denied.
pub fn check_topic_access(
    topic: &str,
    is_publish: bool,
    is_admin: bool,
) -> Result<(), BlockReason> {
    if let Some(rule) = find_matching_rule(topic) {
        return match rule.tier {
            ProtectionTier::BlockAll => Err(BlockReason::InternalTopicBlocked),
            ProtectionTier::ReadOnly => {
                if is_publish {
                    Err(BlockReason::ReadOnlyTopic)
                } else {
                    Ok(())
                }
            }
            ProtectionTier::WriteOnly => {
                if is_publish {
                    Ok(())
                } else {
                    Err(BlockReason::WriteOnlyTopic)
                }
            }
            ProtectionTier::AdminRequired => {
                if is_admin {
                    Ok(())
                } else {
                    Err(BlockReason::AdminRequired)
                }
            }
        };
    }

    if is_internal_entity_topic(topic) && !is_admin {
        return Err(BlockReason::InternalEntityAccess);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_pattern_exact() {
        assert!(matches_pattern("$DB/users/create", "$DB/users/create"));
        assert!(!matches_pattern("$DB/users/create", "$DB/posts/create"));
    }

    #[test]
    fn matches_pattern_single_wildcard() {
        assert!(matches_pattern(
            "$DB/p0/users/create",
            "$DB/p+/users/create"
        ));
        assert!(matches_pattern(
            "$DB/p63/users/create",
            "$DB/p+/users/create"
        ));
        assert!(!matches_pattern("$DB/users/create", "$DB/p+/users/create"));
    }

    #[test]
    fn matches_pattern_multi_wildcard() {
        assert!(matches_pattern("_mqdb/cluster/heartbeat", "_mqdb/#"));
        assert!(matches_pattern("_mqdb/anything/here/nested", "_mqdb/#"));
        assert!(matches_pattern("$DB/_idx/users/email", "$DB/_idx/#"));
        assert!(matches_pattern("$SYS/broker/uptime", "$SYS/#"));
    }

    #[test]
    fn matches_pattern_p_plus() {
        assert!(matches_pattern("$DB/p0/users/create", "$DB/p+/#"));
        assert!(matches_pattern("$DB/p63/sessions/abc123", "$DB/p+/#"));
        assert!(!matches_pattern("$DB/users/create", "$DB/p+/#"));
    }

    #[test]
    fn check_access_internal_blocked() {
        assert_eq!(
            check_topic_access("_mqdb/cluster/heartbeat", true, false),
            Err(BlockReason::InternalTopicBlocked)
        );
        assert_eq!(
            check_topic_access("_mqdb/cluster/heartbeat", false, false),
            Err(BlockReason::InternalTopicBlocked)
        );
        assert_eq!(
            check_topic_access("_mqdb/cluster/heartbeat", true, true),
            Err(BlockReason::InternalTopicBlocked)
        );
    }

    #[test]
    fn check_access_index_topics_blocked() {
        assert_eq!(
            check_topic_access("$DB/_idx/users/email", true, false),
            Err(BlockReason::InternalTopicBlocked)
        );
        assert_eq!(
            check_topic_access("$DB/_unique/users/email", true, false),
            Err(BlockReason::InternalTopicBlocked)
        );
        assert_eq!(
            check_topic_access("$DB/_fk/orders/user_id", true, false),
            Err(BlockReason::InternalTopicBlocked)
        );
    }

    #[test]
    fn check_access_partition_topics_blocked() {
        assert_eq!(
            check_topic_access("$DB/p0/users/create", true, false),
            Err(BlockReason::InternalTopicBlocked)
        );
        assert_eq!(
            check_topic_access("$DB/p63/sessions/abc123", true, false),
            Err(BlockReason::InternalTopicBlocked)
        );
    }

    #[test]
    fn check_access_sys_read_only() {
        assert_eq!(
            check_topic_access("$SYS/broker/uptime", false, false),
            Ok(())
        );
        assert_eq!(
            check_topic_access("$SYS/broker/uptime", true, false),
            Err(BlockReason::ReadOnlyTopic)
        );
        assert_eq!(
            check_topic_access("$SYS/broker/uptime", true, true),
            Err(BlockReason::ReadOnlyTopic)
        );
    }

    #[test]
    fn check_access_sys_cluster_admin_required() {
        assert_eq!(
            check_topic_access("$SYS/mqdb/cluster/status", true, false),
            Err(BlockReason::AdminRequired)
        );
        assert_eq!(
            check_topic_access("$SYS/mqdb/cluster/status", true, true),
            Ok(())
        );
        assert_eq!(
            check_topic_access("$SYS/mqdb/cluster/rebalance", true, true),
            Ok(())
        );
        assert_eq!(
            check_topic_access("$SYS/mqdb/cluster/rebalance", true, false),
            Err(BlockReason::AdminRequired)
        );
    }

    #[test]
    fn check_access_admin_topics_block_non_admin() {
        assert_eq!(
            check_topic_access("$DB/_admin/backup", true, false),
            Err(BlockReason::InternalEntityAccess)
        );
        assert_eq!(
            check_topic_access("$DB/_admin/backup", false, false),
            Err(BlockReason::InternalEntityAccess)
        );
        assert_eq!(check_topic_access("$DB/_admin/backup", true, true), Ok(()));
        assert_eq!(check_topic_access("$DB/_admin/backup", false, true), Ok(()));
    }

    #[test]
    fn check_access_oauth_tokens_block_non_admin() {
        assert_eq!(
            check_topic_access("$DB/_oauth_tokens/list", true, false),
            Err(BlockReason::InternalEntityAccess)
        );
        assert_eq!(
            check_topic_access("$DB/_oauth_tokens/abc123", true, true),
            Ok(())
        );
    }

    #[test]
    fn check_access_identities_block_non_admin() {
        assert_eq!(
            check_topic_access("$DB/_identities/list", true, false),
            Err(BlockReason::InternalEntityAccess)
        );
        assert_eq!(
            check_topic_access("$DB/_identities/create", true, true),
            Ok(())
        );
    }

    #[test]
    fn check_access_identity_links_block_non_admin() {
        assert_eq!(
            check_topic_access("$DB/_identity_links/list", true, false),
            Err(BlockReason::InternalEntityAccess)
        );
        assert_eq!(
            check_topic_access("$DB/_identity_links/google:123", true, true),
            Ok(())
        );
    }

    #[test]
    fn check_access_event_topics_read_only() {
        assert_eq!(
            check_topic_access("$DB/users/events/created", false, false),
            Ok(())
        );
        assert_eq!(
            check_topic_access("$DB/users/events/p3/abc123", false, false),
            Ok(())
        );
        assert_eq!(
            check_topic_access("$DB/orders/events/updated", true, false),
            Err(BlockReason::ReadOnlyTopic)
        );
        assert_eq!(
            check_topic_access("$DB/users/events/created", true, true),
            Err(BlockReason::ReadOnlyTopic)
        );
        for topic in [
            "$DB/ok/events",
            "$DB/ok/events/update",
            "$DB/events/events/e1",
            "$DB/events/events/p3/e1",
        ] {
            assert_eq!(
                check_topic_access(topic, true, true),
                Err(BlockReason::ReadOnlyTopic),
                "{topic}"
            );
            assert_eq!(check_topic_access(topic, false, false), Ok(()), "{topic}");
        }
        for topic in ["$DB/events/list", "$DB/events/e1", "$DB/ok/events-1/update"] {
            assert_eq!(check_topic_access(topic, true, false), Ok(()), "{topic}");
        }
    }

    #[test]
    fn every_emitted_event_topic_is_a_change_event_topic() {
        use mqdb_core::events::ChangeEvent;
        use serde_json::json;

        let scope_of = |entity: &str, value: &str| Some((entity.to_string(), value.to_string()));
        for entity in ["users", "events", "events2", "_identities"] {
            for id in ["u1", "events", "update", "delete", "p3", "events-1", "a/b"] {
                let create = || ChangeEvent::create(entity.into(), id.into(), json!({}));
                let topics = [
                    create().event_topic(0),
                    create().event_topic(8),
                    create().with_scope(scope_of(entity, "L1")).event_topic(8),
                    create()
                        .with_scope(scope_of(entity, "events"))
                        .event_topic(0),
                    create().with_scope(scope_of(entity, "a/b")).event_topic(0),
                    create().with_scope(scope_of("lists", "L1")).event_topic(8),
                    create().with_scope(scope_of("events", "L1")).event_topic(8),
                    create()
                        .with_scope(scope_of("lists", "events"))
                        .event_topic(0),
                    create().with_scope(scope_of("lists", "a/b")).event_topic(0),
                ];
                for topic in topics {
                    assert!(is_change_event_topic(&topic), "{topic}");
                }
            }
        }
    }

    #[test]
    fn requests_are_not_change_event_topics() {
        for topic in [
            "$DB/events/create",
            "$DB/events/list",
            "$DB/events/shared",
            "$DB/events/e1",
            "$DB/events/e1/update",
            "$DB/events/e1/delete",
            "$DB/events/e1/share",
            "$DB/events/e1/unshare",
            "$DB/events/e1/shares",
            "$DB/events2/list",
            "$DB/ok/events-1",
            "$DB/ok/events-1/update",
            "$DB/ok/eventsabc/delete",
            "$DB/ok/event",
            "$DB/ok/myevents",
            "$DB/ok/Events",
            "$DB/_admin/schema/events/set",
            "$DB/_admin/schema/events/get",
            "$DB/_admin/constraint/events/add",
            "$DB/_admin/constraint/events/list",
            "$DB/_admin/index/events/add",
            "$DB/_admin/consumer-groups/events",
            "$DB/_health",
            "app/events",
        ] {
            assert!(!is_change_event_topic(topic), "{topic}");
        }
    }

    #[test]
    fn a_topic_under_the_event_level_is_never_a_request() {
        for topic in [
            "$DB/ok/events",
            "$DB/ok/events/update",
            "$DB/ok/events/delete",
            "$DB/events/events/update",
            "$DB/_admin/events/delete",
            "$DB/_sub/events/heartbeat",
            "$DB/u/alice/events/ok/r1",
        ] {
            assert!(is_change_event_topic(topic), "{topic}");
        }
    }

    #[test]
    fn check_access_presence_read_only() {
        assert_eq!(
            check_topic_access("$DB/_presence/seat-holder-7", false, false),
            Ok(()),
            "a non-admin janitor must be able to subscribe to presence"
        );
        assert_eq!(check_topic_access("$DB/_presence/#", false, false), Ok(()));
        assert_eq!(
            check_topic_access("$DB/_presence/seat-holder-7", true, false),
            Err(BlockReason::ReadOnlyTopic),
            "a client must not be able to forge a presence message"
        );
        assert_eq!(
            check_topic_access("$DB/_presence/seat-holder-7", true, true),
            Err(BlockReason::ReadOnlyTopic),
            "not even an admin may publish presence; only the internal service bypass may"
        );
    }

    #[test]
    fn check_access_user_namespace_read_only() {
        assert_eq!(
            check_topic_access("$DB/u/bob/events/created", true, false),
            Err(BlockReason::ReadOnlyTopic)
        );
        assert_eq!(
            check_topic_access("$DB/u/bob/events/diagrams/1", true, false),
            Err(BlockReason::ReadOnlyTopic)
        );
        assert_eq!(
            check_topic_access("$DB/u/bob/events/created", true, true),
            Err(BlockReason::ReadOnlyTopic)
        );
        assert_eq!(
            check_topic_access("$DB/u/bob/events/created", false, false),
            Ok(())
        );
    }

    #[test]
    fn check_access_sub_channel_write_only() {
        assert_eq!(
            check_topic_access("$DB/_sub/subscribe", true, false),
            Ok(())
        );
        assert_eq!(
            check_topic_access("$DB/_sub/abc123/heartbeat", true, false),
            Ok(())
        );
        assert_eq!(
            check_topic_access("$DB/_sub/abc123/unsubscribe", true, false),
            Ok(())
        );
        assert_eq!(
            check_topic_access("$DB/_sub/subscribe", false, false),
            Err(BlockReason::WriteOnlyTopic)
        );
        assert_eq!(
            check_topic_access("$DB/_sub/subscribe", false, true),
            Err(BlockReason::WriteOnlyTopic)
        );
    }

    #[test]
    fn check_access_regular_topics_allowed() {
        assert_eq!(check_topic_access("$DB/users/create", true, false), Ok(()));
        assert_eq!(check_topic_access("$DB/posts/abc123", true, false), Ok(()));
        assert_eq!(
            check_topic_access("sensors/temperature", true, false),
            Ok(())
        );
        assert_eq!(
            check_topic_access("app/notifications", false, false),
            Ok(())
        );
    }

    #[test]
    fn check_access_internal_entity_topic() {
        assert_eq!(
            check_topic_access("$DB/_sessions/list", true, false),
            Err(BlockReason::InternalEntityAccess)
        );
        assert_eq!(
            check_topic_access("$DB/_sessions/abc123", false, false),
            Err(BlockReason::InternalEntityAccess)
        );
        assert_eq!(
            check_topic_access("$DB/_mqtt_subs/create", true, false),
            Err(BlockReason::InternalEntityAccess)
        );
        assert_eq!(
            check_topic_access("$DB/_topic_index/list", true, false),
            Err(BlockReason::InternalEntityAccess)
        );
    }

    #[test]
    fn check_access_internal_entity_admin_allowed() {
        assert_eq!(check_topic_access("$DB/_sessions/list", true, true), Ok(()));
        assert_eq!(
            check_topic_access("$DB/_sessions/abc123", false, true),
            Ok(())
        );
        assert_eq!(
            check_topic_access("$DB/_mqtt_subs/create", true, true),
            Ok(())
        );
    }

    #[test]
    fn check_access_health_endpoint_allowed() {
        assert_eq!(check_topic_access("$DB/_health", true, false), Ok(()));
        assert_eq!(check_topic_access("$DB/_health", false, false), Ok(()));
    }

    #[test]
    fn secret_request_namespaces_are_write_only_for_everyone() {
        for topic in [
            "$DB/_auth/password/change",
            "$DB/_auth/password/reset/start",
            "$DB/_auth/#",
            "$DB/_vault/unlock",
            "$DB/_vault/change",
            "$DB/_vault/#",
        ] {
            for is_admin in [false, true] {
                assert_eq!(
                    check_topic_access(topic, false, is_admin),
                    Err(BlockReason::WriteOnlyTopic),
                    "{topic}"
                );
                assert_eq!(check_topic_access(topic, true, is_admin), Ok(()), "{topic}");
            }
        }
    }

    #[test]
    fn check_access_vault_endpoints_allowed() {
        assert_eq!(check_topic_access("$DB/_vault/enable", true, false), Ok(()));
        assert_eq!(check_topic_access("$DB/_vault/unlock", true, false), Ok(()));
        assert_eq!(check_topic_access("$DB/_vault/lock", true, false), Ok(()));
        assert_eq!(
            check_topic_access("$DB/_vault/disable", true, false),
            Ok(())
        );
        assert_eq!(check_topic_access("$DB/_vault/change", true, false), Ok(()));
        assert_eq!(check_topic_access("$DB/_vault/status", true, false), Ok(()));
    }

    #[test]
    fn check_access_auth_endpoints_allowed() {
        assert_eq!(
            check_topic_access("$DB/_auth/password/change", true, false),
            Ok(())
        );
    }

    #[test]
    fn prefix_lookalikes_blocked_for_non_admin() {
        assert_eq!(
            check_topic_access("$DB/_healthcheck/list", false, false),
            Err(BlockReason::InternalEntityAccess)
        );
        assert_eq!(
            check_topic_access("$DB/_healthy/data", true, false),
            Err(BlockReason::InternalEntityAccess)
        );
        assert_eq!(
            check_topic_access("$DB/_vaulted/secrets", true, false),
            Err(BlockReason::InternalEntityAccess)
        );
        assert_eq!(
            check_topic_access("$DB/_authentication/tokens", false, false),
            Err(BlockReason::InternalEntityAccess)
        );
        assert_eq!(
            check_topic_access("$DB/_authority/keys", true, false),
            Err(BlockReason::InternalEntityAccess)
        );
        assert_eq!(
            check_topic_access("$DB/_verifier/status", false, false),
            Err(BlockReason::InternalEntityAccess)
        );
    }

    #[test]
    fn check_access_verify_endpoints_admin_required() {
        assert_eq!(
            check_topic_access("$DB/_verify/challenges/email", true, false),
            Err(BlockReason::AdminRequired)
        );
        assert_eq!(
            check_topic_access("$DB/_verify/challenges/email", false, false),
            Err(BlockReason::AdminRequired)
        );
        assert_eq!(
            check_topic_access("$DB/_verify/receipts/abc-123", true, false),
            Err(BlockReason::AdminRequired)
        );
        assert_eq!(
            check_topic_access("$DB/_verify/receipts/abc-123", false, false),
            Err(BlockReason::AdminRequired)
        );
        assert_eq!(
            check_topic_access("$DB/_verify/challenges/email", true, true),
            Ok(())
        );
        assert_eq!(
            check_topic_access("$DB/_verify/challenges/email", false, true),
            Ok(())
        );
        assert_eq!(
            check_topic_access("$DB/_verify/receipts/abc-123", true, true),
            Ok(())
        );
        assert_eq!(
            check_topic_access("$DB/_verify/receipts/abc-123", false, true),
            Ok(())
        );
    }
}
