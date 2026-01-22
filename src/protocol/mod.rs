use crate::transport::Request;
use crate::types::{Filter, Pagination, SortOrder};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct DbOperation {
    pub entity: String,
    pub operation: String,
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
        ["backup"] => Some(AdminOperation::Backup),
        ["backup", "list"] => Some(AdminOperation::BackupList),
        ["restore"] => Some(AdminOperation::Restore),
        ["consumer-groups"] => Some(AdminOperation::ConsumerGroupList),
        ["consumer-groups", name] => Some(AdminOperation::ConsumerGroupShow {
            name: (*name).to_string(),
        }),
        _ => None,
    }
}

#[allow(clippy::must_use_candidate)]
pub fn parse_db_topic(topic: &str) -> Option<DbOperation> {
    let parts: Vec<&str> = topic.strip_prefix("$DB/")?.split('/').collect();

    match parts.as_slice() {
        [entity, op] if *op == "create" || *op == "list" => Some(DbOperation {
            entity: (*entity).to_string(),
            operation: (*op).to_string(),
            id: None,
        }),
        [entity, id] => Some(DbOperation {
            entity: (*entity).to_string(),
            operation: "read".to_string(),
            id: Some((*id).to_string()),
        }),
        [entity, id, op] if *op == "update" || *op == "delete" => Some(DbOperation {
            entity: (*entity).to_string(),
            operation: (*op).to_string(),
            id: Some((*id).to_string()),
        }),
        _ => None,
    }
}

/// Builds a database request from an operation descriptor and payload.
///
/// # Errors
/// Returns an error if JSON deserialization fails, required ID is missing, or operation is unknown.
pub fn build_request(op: DbOperation, payload: &[u8]) -> Result<Request, String> {
    let data: Value = if payload.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(payload).map_err(|e| e.to_string())?
    };

    match op.operation.as_str() {
        "create" => Ok(Request::Create {
            entity: op.entity,
            data,
        }),
        "read" => {
            let id = op.id.ok_or("read requires id in topic")?;
            let (includes, projection) = extract_read_options(&data);
            Ok(Request::Read {
                entity: op.entity,
                id,
                includes,
                projection,
            })
        }
        "update" => {
            let id = op.id.ok_or("update requires id in topic")?;
            Ok(Request::Update {
                entity: op.entity,
                id,
                fields: data,
            })
        }
        "delete" => {
            let id = op.id.ok_or("delete requires id in topic")?;
            Ok(Request::Delete {
                entity: op.entity,
                id,
            })
        }
        "list" => {
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
        _ => Err(format!("unknown operation: {}", op.operation)),
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
        assert_eq!(op.operation, "create");
        assert!(op.id.is_none());
    }

    #[test]
    fn test_parse_db_topic_read() {
        let op = parse_db_topic("$DB/users/123").unwrap();
        assert_eq!(op.entity, "users");
        assert_eq!(op.operation, "read");
        assert_eq!(op.id, Some("123".to_string()));
    }

    #[test]
    fn test_parse_db_topic_update() {
        let op = parse_db_topic("$DB/users/123/update").unwrap();
        assert_eq!(op.entity, "users");
        assert_eq!(op.operation, "update");
        assert_eq!(op.id, Some("123".to_string()));
    }

    #[test]
    fn test_parse_db_topic_delete() {
        let op = parse_db_topic("$DB/users/123/delete").unwrap();
        assert_eq!(op.entity, "users");
        assert_eq!(op.operation, "delete");
        assert_eq!(op.id, Some("123".to_string()));
    }

    #[test]
    fn test_parse_db_topic_list() {
        let op = parse_db_topic("$DB/users/list").unwrap();
        assert_eq!(op.entity, "users");
        assert_eq!(op.operation, "list");
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
            operation: "create".to_string(),
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
            operation: "read".to_string(),
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
            operation: "list".to_string(),
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
            operation: "read".to_string(),
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
}
