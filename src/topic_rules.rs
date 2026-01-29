use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtectionTier {
    BlockAll,
    ReadOnly,
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
        pattern: "$SYS/#",
        tier: ProtectionTier::ReadOnly,
    },
    TopicRule {
        pattern: "$DB/_admin/#",
        tier: ProtectionTier::AdminRequired,
    },
    TopicRule {
        pattern: "$DB/_oauth_tokens/#",
        tier: ProtectionTier::AdminRequired,
    },
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockReason {
    InternalTopicBlocked,
    ReadOnlyTopic,
    AdminRequired,
    InternalEntityAccess,
}

impl fmt::Display for BlockReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InternalTopicBlocked => write!(f, "internal topic blocked"),
            Self::ReadOnlyTopic => write!(f, "read-only topic"),
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
    if rest.starts_with('_') && !rest.starts_with("_health") {
        let entity = rest.split('/').next().unwrap_or("");
        !entity.is_empty() && entity.starts_with('_')
    } else {
        false
    }
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

#[must_use]
pub fn is_internal_entity(entity: &str) -> bool {
    entity.starts_with('_')
}

/// # Errors
///
/// Returns `Err(BlockReason::InternalEntityAccess)` if the entity is internal and user is not admin.
pub fn check_entity_access(entity: &str, is_admin: bool) -> Result<(), BlockReason> {
    if is_internal_entity(entity) && !is_admin {
        Err(BlockReason::InternalEntityAccess)
    } else {
        Ok(())
    }
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
        assert!(matches_pattern("$DB/p0/users/create", "$DB/p+/users/create"));
        assert!(matches_pattern("$DB/p63/users/create", "$DB/p+/users/create"));
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
        assert_eq!(check_topic_access("$SYS/broker/uptime", false, false), Ok(()));
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
    fn check_access_admin_required() {
        assert_eq!(
            check_topic_access("$DB/_admin/backup", true, false),
            Err(BlockReason::AdminRequired)
        );
        assert_eq!(
            check_topic_access("$DB/_admin/backup", false, false),
            Err(BlockReason::AdminRequired)
        );
        assert_eq!(check_topic_access("$DB/_admin/backup", true, true), Ok(()));
        assert_eq!(check_topic_access("$DB/_admin/backup", false, true), Ok(()));
    }

    #[test]
    fn check_access_oauth_tokens_admin_required() {
        assert_eq!(
            check_topic_access("$DB/_oauth_tokens/list", true, false),
            Err(BlockReason::AdminRequired)
        );
        assert_eq!(
            check_topic_access("$DB/_oauth_tokens/abc123", true, true),
            Ok(())
        );
    }

    #[test]
    fn check_access_regular_topics_allowed() {
        assert_eq!(check_topic_access("$DB/users/create", true, false), Ok(()));
        assert_eq!(check_topic_access("$DB/posts/abc123", true, false), Ok(()));
        assert_eq!(check_topic_access("sensors/temperature", true, false), Ok(()));
        assert_eq!(check_topic_access("app/notifications", false, false), Ok(()));
    }

    #[test]
    fn check_entity_access_internal_blocked() {
        assert_eq!(
            check_entity_access("_sessions", false),
            Err(BlockReason::InternalEntityAccess)
        );
        assert_eq!(
            check_entity_access("_mqtt_subs", false),
            Err(BlockReason::InternalEntityAccess)
        );
        assert_eq!(
            check_entity_access("_topic_index", false),
            Err(BlockReason::InternalEntityAccess)
        );
    }

    #[test]
    fn check_entity_access_admin_allowed() {
        assert_eq!(check_entity_access("_sessions", true), Ok(()));
        assert_eq!(check_entity_access("_mqtt_subs", true), Ok(()));
    }

    #[test]
    fn check_entity_access_regular_allowed() {
        assert_eq!(check_entity_access("users", false), Ok(()));
        assert_eq!(check_entity_access("posts", false), Ok(()));
        assert_eq!(check_entity_access("orders", true), Ok(()));
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
        assert_eq!(check_topic_access("$DB/_sessions/abc123", false, true), Ok(()));
        assert_eq!(check_topic_access("$DB/_mqtt_subs/create", true, true), Ok(()));
    }

    #[test]
    fn check_access_health_endpoint_allowed() {
        assert_eq!(check_topic_access("$DB/_health", true, false), Ok(()));
        assert_eq!(check_topic_access("$DB/_health", false, false), Ok(()));
    }
}
