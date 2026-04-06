// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use std::fmt;

use crate::transport::Request;
use crate::types::{Filter, Pagination, SortOrder};
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("missing required ID for {0} operation")]
    MissingId(DbOp),
    #[error("invalid JSON payload: {0}")]
    InvalidPayload(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbOp {
    Create,
    Read,
    Update,
    Delete,
    List,
}

impl fmt::Display for DbOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Create => f.write_str("create"),
            Self::Read => f.write_str("read"),
            Self::Update => f.write_str("update"),
            Self::Delete => f.write_str("delete"),
            Self::List => f.write_str("list"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DbOperation {
    pub entity: String,
    pub operation: DbOp,
    pub id: Option<String>,
}

#[derive(Debug, Clone)]
pub enum AdminOperation {
    SchemaSet { entity: String },
    SchemaGet { entity: String },
    ConstraintAdd { entity: String },
    ConstraintList { entity: String },
    Backup,
    Restore,
    BackupList,
    Subscribe,
    Heartbeat { sub_id: String },
    Unsubscribe { sub_id: String },
    ConsumerGroupList,
    ConsumerGroupShow { name: String },
    Health,
    UserAdd,
    UserDelete,
    UserList,
    AclRuleAdd,
    AclRuleRemove,
    AclRuleList,
    AclRoleAdd,
    AclRoleDelete,
    AclRoleList,
    AclAssignmentAssign,
    AclAssignmentUnassign,
    AclAssignmentList,
    IndexAdd { entity: String },
    Catalog,
    VaultEnable,
    VaultUnlock,
    VaultLock,
    VaultDisable,
    VaultChange,
    VaultStatus,
    PasswordChange,
    PasswordResetStart,
    PasswordResetSubmit,
}

type ListOptions = (
    Vec<Filter>,
    Vec<SortOrder>,
    Option<Pagination>,
    Vec<String>,
    Option<Vec<String>>,
);

#[allow(clippy::must_use_candidate)]
pub fn parse_admin_topic(topic: &str) -> Option<AdminOperation> {
    if topic == "$DB/_health" {
        return Some(AdminOperation::Health);
    }

    if let Some(rest) = topic.strip_prefix("$DB/_sub/") {
        let parts: Vec<&str> = rest.split('/').collect();
        return match parts.as_slice() {
            ["subscribe"] => Some(AdminOperation::Subscribe),
            [id, "heartbeat"] => Some(AdminOperation::Heartbeat {
                sub_id: (*id).to_string(),
            }),
            [id, "unsubscribe"] => Some(AdminOperation::Unsubscribe {
                sub_id: (*id).to_string(),
            }),
            _ => None,
        };
    }

    if let Some(rest) = topic.strip_prefix("$DB/_vault/") {
        return match rest {
            "enable" => Some(AdminOperation::VaultEnable),
            "unlock" => Some(AdminOperation::VaultUnlock),
            "lock" => Some(AdminOperation::VaultLock),
            "disable" => Some(AdminOperation::VaultDisable),
            "change" => Some(AdminOperation::VaultChange),
            "status" => Some(AdminOperation::VaultStatus),
            _ => None,
        };
    }

    if let Some(rest) = topic.strip_prefix("$DB/_auth/") {
        return match rest {
            "password/change" => Some(AdminOperation::PasswordChange),
            "password/reset/start" => Some(AdminOperation::PasswordResetStart),
            "password/reset/submit" => Some(AdminOperation::PasswordResetSubmit),
            _ => None,
        };
    }

    let parts: Vec<&str> = topic.strip_prefix("$DB/_admin/")?.split('/').collect();

    match parts.as_slice() {
        ["schema", entity, "set"] => Some(AdminOperation::SchemaSet {
            entity: (*entity).to_string(),
        }),
        ["schema", entity, "get"] => Some(AdminOperation::SchemaGet {
            entity: (*entity).to_string(),
        }),
        ["constraint", entity, "add"] => Some(AdminOperation::ConstraintAdd {
            entity: (*entity).to_string(),
        }),
        ["constraint", entity, "list"] => Some(AdminOperation::ConstraintList {
            entity: (*entity).to_string(),
        }),
        ["index", entity, "add"] => Some(AdminOperation::IndexAdd {
            entity: (*entity).to_string(),
        }),
        ["backup"] => Some(AdminOperation::Backup),
        ["backup", "list"] => Some(AdminOperation::BackupList),
        ["restore"] => Some(AdminOperation::Restore),
        ["consumer-groups"] => Some(AdminOperation::ConsumerGroupList),
        ["consumer-groups", name] => Some(AdminOperation::ConsumerGroupShow {
            name: (*name).to_string(),
        }),
        ["users", "add"] => Some(AdminOperation::UserAdd),
        ["users", "delete"] => Some(AdminOperation::UserDelete),
        ["users", "list"] => Some(AdminOperation::UserList),
        ["acl", "rules", "add"] => Some(AdminOperation::AclRuleAdd),
        ["acl", "rules", "remove"] => Some(AdminOperation::AclRuleRemove),
        ["acl", "rules", "list"] => Some(AdminOperation::AclRuleList),
        ["acl", "roles", "add"] => Some(AdminOperation::AclRoleAdd),
        ["acl", "roles", "delete"] => Some(AdminOperation::AclRoleDelete),
        ["acl", "roles", "list"] => Some(AdminOperation::AclRoleList),
        ["acl", "assignments", "assign"] => Some(AdminOperation::AclAssignmentAssign),
        ["acl", "assignments", "unassign"] => Some(AdminOperation::AclAssignmentUnassign),
        ["acl", "assignments", "list"] => Some(AdminOperation::AclAssignmentList),
        ["catalog"] => Some(AdminOperation::Catalog),
        _ => None,
    }
}

#[allow(clippy::must_use_candidate)]
pub fn parse_db_topic(topic: &str) -> Option<DbOperation> {
    let parts: Vec<&str> = topic.strip_prefix("$DB/")?.split('/').collect();

    match parts.as_slice() {
        [entity, "create"] => Some(DbOperation {
            entity: (*entity).to_string(),
            operation: DbOp::Create,
            id: None,
        }),
        [entity, "list"] => Some(DbOperation {
            entity: (*entity).to_string(),
            operation: DbOp::List,
            id: None,
        }),
        [entity, id] => Some(DbOperation {
            entity: (*entity).to_string(),
            operation: DbOp::Read,
            id: Some((*id).to_string()),
        }),
        [entity, id, "update"] => Some(DbOperation {
            entity: (*entity).to_string(),
            operation: DbOp::Update,
            id: Some((*id).to_string()),
        }),
        [entity, id, "delete"] => Some(DbOperation {
            entity: (*entity).to_string(),
            operation: DbOp::Delete,
            id: Some((*id).to_string()),
        }),
        _ => None,
    }
}

/// Builds a database request from an operation descriptor and payload.
///
/// # Errors
/// Returns an error if JSON deserialization fails or a required ID is missing.
pub fn build_request(op: DbOperation, payload: &[u8]) -> Result<Request, ProtocolError> {
    let data: Value = if payload.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(payload)?
    };

    match op.operation {
        DbOp::Create => Ok(Request::Create {
            entity: op.entity,
            data,
        }),
        DbOp::Read => {
            let id = op.id.ok_or(ProtocolError::MissingId(DbOp::Read))?;
            let (includes, projection) = extract_read_options(&data);
            Ok(Request::Read {
                entity: op.entity,
                id,
                includes,
                projection,
            })
        }
        DbOp::Update => {
            let id = op.id.ok_or(ProtocolError::MissingId(DbOp::Update))?;
            Ok(Request::Update {
                entity: op.entity,
                id,
                fields: data,
            })
        }
        DbOp::Delete => {
            let id = op.id.ok_or(ProtocolError::MissingId(DbOp::Delete))?;
            Ok(Request::Delete {
                entity: op.entity,
                id,
            })
        }
        DbOp::List => {
            let (filters, sort, pagination, includes, projection) = extract_list_options(&data);
            Ok(Request::List {
                entity: op.entity,
                filters,
                sort,
                pagination,
                includes,
                projection,
            })
        }
    }
}

fn extract_read_options(data: &Value) -> (Vec<String>, Option<Vec<String>>) {
    let includes = data
        .get("includes")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let projection = data
        .get("projection")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        });

    (includes, projection)
}

#[allow(clippy::cast_possible_truncation)]
fn extract_list_options(data: &Value) -> ListOptions {
    let filters: Vec<Filter> = data
        .get("filters")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();

    let sort: Vec<SortOrder> = data
        .get("sort")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();

    let pagination: Option<Pagination> = data
        .get("pagination")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .or_else(|| {
            let limit = data
                .get("limit")
                .and_then(serde_json::Value::as_u64)
                .map(|v| v as usize);
            let offset = data
                .get("offset")
                .and_then(serde_json::Value::as_u64)
                .map(|v| v as usize);
            match (limit, offset) {
                (Some(l), Some(o)) => Some(Pagination::new(l, o)),
                (Some(l), None) => Some(Pagination::new(l, 0)),
                (None, Some(o)) => Some(Pagination::new(usize::MAX, o)),
                (None, None) => None,
            }
        });

    let includes = data
        .get("includes")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let projection = data
        .get("projection")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        });

    (filters, sort, pagination, includes, projection)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_db_topic_create() {
        let op = parse_db_topic("$DB/users/create").unwrap();
        assert_eq!(op.entity, "users");
        assert_eq!(op.operation, DbOp::Create);
        assert!(op.id.is_none());
    }

    #[test]
    fn test_parse_db_topic_read() {
        let op = parse_db_topic("$DB/users/123").unwrap();
        assert_eq!(op.entity, "users");
        assert_eq!(op.operation, DbOp::Read);
        assert_eq!(op.id, Some("123".to_string()));
    }

    #[test]
    fn test_parse_db_topic_update() {
        let op = parse_db_topic("$DB/users/123/update").unwrap();
        assert_eq!(op.entity, "users");
        assert_eq!(op.operation, DbOp::Update);
        assert_eq!(op.id, Some("123".to_string()));
    }

    #[test]
    fn test_parse_db_topic_delete() {
        let op = parse_db_topic("$DB/users/123/delete").unwrap();
        assert_eq!(op.entity, "users");
        assert_eq!(op.operation, DbOp::Delete);
        assert_eq!(op.id, Some("123".to_string()));
    }

    #[test]
    fn test_parse_db_topic_list() {
        let op = parse_db_topic("$DB/users/list").unwrap();
        assert_eq!(op.entity, "users");
        assert_eq!(op.operation, DbOp::List);
        assert!(op.id.is_none());
    }

    #[test]
    fn test_parse_db_topic_invalid() {
        assert!(parse_db_topic("invalid/topic").is_none());
        assert!(parse_db_topic("$DB").is_none());
        assert!(parse_db_topic("$DB/").is_none());
    }

    #[test]
    fn test_build_create_request() {
        let op = DbOperation {
            entity: "users".to_string(),
            operation: DbOp::Create,
            id: None,
        };
        let payload = br#"{"name": "Alice"}"#;
        let request = build_request(op, payload).unwrap();

        match request {
            Request::Create { entity, data } => {
                assert_eq!(entity, "users");
                assert_eq!(data["name"], "Alice");
            }
            _ => panic!("expected Create request"),
        }
    }

    #[test]
    fn test_build_read_request() {
        let op = DbOperation {
            entity: "users".to_string(),
            operation: DbOp::Read,
            id: Some("123".to_string()),
        };
        let payload = br#"{"projection": ["name", "email"]}"#;
        let request = build_request(op, payload).unwrap();

        match request {
            Request::Read {
                entity,
                id,
                projection,
                ..
            } => {
                assert_eq!(entity, "users");
                assert_eq!(id, "123");
                assert_eq!(
                    projection,
                    Some(vec!["name".to_string(), "email".to_string()])
                );
            }
            _ => panic!("expected Read request"),
        }
    }

    #[test]
    fn test_build_list_request() {
        let op = DbOperation {
            entity: "users".to_string(),
            operation: DbOp::List,
            id: None,
        };
        let payload = br#"{"filters": [{"field": "age", "op": "gt", "value": 18}]}"#;
        let request = build_request(op, payload).unwrap();

        match request {
            Request::List {
                entity, filters, ..
            } => {
                assert_eq!(entity, "users");
                assert_eq!(filters.len(), 1);
                assert_eq!(filters[0].field, "age");
            }
            _ => panic!("expected List request"),
        }
    }

    #[test]
    fn test_read_without_id_fails() {
        let op = DbOperation {
            entity: "users".to_string(),
            operation: DbOp::Read,
            id: None,
        };
        let result = build_request(op, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_admin_topic_health() {
        let op = parse_admin_topic("$DB/_health").unwrap();
        assert!(matches!(op, AdminOperation::Health));
    }

    #[test]
    fn test_parse_admin_topic_catalog() {
        let op = parse_admin_topic("$DB/_admin/catalog").unwrap();
        assert!(matches!(op, AdminOperation::Catalog));
    }

    #[test]
    fn test_parse_vault_topics() {
        assert!(matches!(
            parse_admin_topic("$DB/_vault/enable"),
            Some(AdminOperation::VaultEnable)
        ));
        assert!(matches!(
            parse_admin_topic("$DB/_vault/unlock"),
            Some(AdminOperation::VaultUnlock)
        ));
        assert!(matches!(
            parse_admin_topic("$DB/_vault/lock"),
            Some(AdminOperation::VaultLock)
        ));
        assert!(matches!(
            parse_admin_topic("$DB/_vault/disable"),
            Some(AdminOperation::VaultDisable)
        ));
        assert!(matches!(
            parse_admin_topic("$DB/_vault/change"),
            Some(AdminOperation::VaultChange)
        ));
        assert!(matches!(
            parse_admin_topic("$DB/_vault/status"),
            Some(AdminOperation::VaultStatus)
        ));
        assert!(parse_admin_topic("$DB/_vault/unknown").is_none());
    }

    #[test]
    fn test_parse_auth_topics() {
        assert!(matches!(
            parse_admin_topic("$DB/_auth/password/change"),
            Some(AdminOperation::PasswordChange)
        ));
        assert!(matches!(
            parse_admin_topic("$DB/_auth/password/reset/start"),
            Some(AdminOperation::PasswordResetStart)
        ));
        assert!(matches!(
            parse_admin_topic("$DB/_auth/password/reset/submit"),
            Some(AdminOperation::PasswordResetSubmit)
        ));
        assert!(parse_admin_topic("$DB/_auth/unknown").is_none());
        assert!(parse_admin_topic("$DB/_auth/password/reset/other").is_none());
    }

    #[test]
    fn test_extract_list_options_with_eq_filter() {
        let payload = serde_json::json!({
            "filters": [{"field": "email", "op": "eq", "value": "alice@example.com"}]
        });
        let (filters, sort, pagination, includes, projection) = extract_list_options(&payload);
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].field, "email");
        assert!(matches!(filters[0].op, crate::FilterOp::Eq));
        assert_eq!(filters[0].value, serde_json::json!("alice@example.com"));
        assert!(sort.is_empty());
        assert!(pagination.is_none());
        assert!(includes.is_empty());
        assert!(projection.is_none());
    }
}
