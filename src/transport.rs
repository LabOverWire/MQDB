// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::subscription::SubscriptionMode;
use crate::{Error, Filter, Pagination, SortOrder};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub enum VaultConstraintData {
    Create(Value),
    Update(Value, Value),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    Create {
        entity: String,
        data: Value,
    },
    Read {
        entity: String,
        id: String,
        #[serde(default)]
        includes: Vec<String>,
        #[serde(default)]
        projection: Option<Vec<String>>,
    },
    Update {
        entity: String,
        id: String,
        fields: Value,
    },
    Delete {
        entity: String,
        id: String,
    },
    List {
        entity: String,
        #[serde(default)]
        filters: Vec<Filter>,
        #[serde(default)]
        sort: Vec<SortOrder>,
        #[serde(default)]
        pagination: Option<Pagination>,
        #[serde(default)]
        includes: Vec<String>,
        #[serde(default)]
        projection: Option<Vec<String>>,
    },
    Subscribe {
        pattern: String,
        #[serde(default)]
        entity: Option<String>,
        #[serde(default)]
        share_group: Option<String>,
        #[serde(default)]
        mode: Option<SubscriptionMode>,
    },
    Unsubscribe {
        id: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    BadRequest = 400,
    Forbidden = 403,
    NotFound = 404,
    Conflict = 409,
    Internal = 500,
}

impl ErrorCode {
    #[must_use]
    pub fn as_u16(self) -> u16 {
        self as u16
    }

    #[must_use]
    pub fn as_grpc_code(self) -> i32 {
        match self {
            ErrorCode::BadRequest => 3,
            ErrorCode::Forbidden => 7,
            ErrorCode::NotFound => 5,
            ErrorCode::Conflict => 6,
            ErrorCode::Internal => 13,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub code: ErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Response {
    Ok { data: Value },
    Error { code: u16, message: String },
}

impl Response {
    #[must_use]
    pub fn ok(data: Value) -> Self {
        Response::Ok { data }
    }

    pub fn error(code: ErrorCode, message: impl Into<String>) -> Self {
        Response::Error {
            code: code.as_u16(),
            message: message.into(),
        }
    }

    #[must_use]
    pub fn is_ok(&self) -> bool {
        matches!(self, Response::Ok { .. })
    }

    #[must_use]
    pub fn is_error(&self) -> bool {
        matches!(self, Response::Error { .. })
    }
}

impl From<Error> for Response {
    fn from(e: Error) -> Self {
        let (code, message) = match &e {
            Error::NotFound { entity, .. } => (ErrorCode::NotFound, format!("not found: {entity}")),
            Error::Validation(msg) => (ErrorCode::BadRequest, format!("validation error: {msg}")),
            Error::SchemaViolation { field, reason, .. } => (
                ErrorCode::BadRequest,
                format!("schema validation failed: {field} - {reason}"),
            ),
            Error::Forbidden(_) => (ErrorCode::Forbidden, "forbidden".to_string()),
            Error::ConstraintViolation(msg) => {
                (ErrorCode::Conflict, format!("constraint violation: {msg}"))
            }
            Error::UniqueViolation { entity, field, .. } => (
                ErrorCode::Conflict,
                format!("unique constraint violation: {entity}.{field}"),
            ),
            Error::ForeignKeyViolation { entity, field, .. } => (
                ErrorCode::Conflict,
                format!("foreign key violation: {entity}.{field}"),
            ),
            Error::ForeignKeyRestrict { entity, .. } => (
                ErrorCode::Conflict,
                format!("cannot delete {entity}: referenced by other entities"),
            ),
            Error::NotNullViolation { entity, field } => (
                ErrorCode::Conflict,
                format!("not null violation: {entity}.{field}"),
            ),
            Error::Conflict(msg) => (ErrorCode::Conflict, format!("conflict: {msg}")),
            _ => {
                tracing::error!(error = %e, "internal error in client request");
                (ErrorCode::Internal, "internal error".to_string())
            }
        };
        Response::error(code, message)
    }
}

#[cfg(feature = "agent")]
mod execute {
    use super::{Request, Response, VaultConstraintData};
    use crate::Database;
    use crate::types::{OwnershipConfig, ScopeConfig};
    use serde_json::Value;

    fn value_from_unit(_: ()) -> Value {
        Value::Null
    }

    fn value_from_vec(v: Vec<Value>) -> Value {
        Value::Array(v)
    }

    fn value_from_string(s: String) -> Value {
        Value::String(s)
    }

    impl Database {
        pub async fn execute(&self, request: Request) -> Response {
            self.execute_with_sender(
                request,
                None,
                None,
                &OwnershipConfig::default(),
                &ScopeConfig::default(),
                None,
            )
            .await
        }

        #[allow(clippy::too_many_lines)]
        pub async fn execute_with_sender(
            &self,
            request: Request,
            sender: Option<&str>,
            client_id: Option<&str>,
            ownership: &OwnershipConfig,
            scope_config: &ScopeConfig,
            vault_constraint: Option<VaultConstraintData>,
        ) -> Response {
            match request {
                Request::Create { entity, data } => {
                    let constraint_data = match vault_constraint {
                        Some(VaultConstraintData::Create(cd)) => Some(cd),
                        _ => None,
                    };
                    match self
                        .create(
                            entity,
                            data,
                            constraint_data,
                            sender,
                            client_id,
                            scope_config,
                        )
                        .await
                    {
                        Ok(v) => Response::ok(v),
                        Err(e) => e.into(),
                    }
                }
                Request::Read {
                    entity,
                    id,
                    includes,
                    projection,
                } => {
                    if let Some(uid) = sender
                        && !ownership.is_admin(uid)
                        && let Some(owner_field) = ownership.owner_field(&entity)
                        && let Err(e) = self.check_ownership(&entity, &id, owner_field, uid)
                    {
                        return e.into();
                    }
                    match self.read(entity, id, includes, projection).await {
                        Ok(v) => Response::ok(v),
                        Err(e) => e.into(),
                    }
                }
                Request::Update {
                    entity,
                    id,
                    mut fields,
                } => {
                    if let Some(uid) = sender
                        && !ownership.is_admin(uid)
                        && let Some(owner_field) = ownership.owner_field(&entity)
                    {
                        if let Err(e) = self.check_ownership(&entity, &id, owner_field, uid) {
                            return e.into();
                        }
                        if let Value::Object(ref mut map) = fields {
                            map.remove(owner_field);
                        }
                    }
                    let update_constraint = match vault_constraint {
                        Some(VaultConstraintData::Update(new_data, old_data)) => {
                            Some((new_data, old_data))
                        }
                        _ => None,
                    };
                    let caller = crate::database::CallerContext {
                        sender,
                        client_id,
                        scope_config,
                    };
                    match self
                        .update(entity, id, fields, update_constraint, &caller)
                        .await
                    {
                        Ok(v) => Response::ok(v),
                        Err(e) => e.into(),
                    }
                }
                Request::Delete { entity, id } => {
                    if let Some(uid) = sender
                        && !ownership.is_admin(uid)
                        && let Some(owner_field) = ownership.owner_field(&entity)
                        && let Err(e) = self.check_ownership(&entity, &id, owner_field, uid)
                    {
                        return e.into();
                    }
                    match self
                        .delete(entity, id, sender, client_id, scope_config)
                        .await
                    {
                        Ok(()) => Response::ok(value_from_unit(())),
                        Err(e) => e.into(),
                    }
                }
                Request::List {
                    entity,
                    mut filters,
                    sort,
                    pagination,
                    includes,
                    projection,
                } => {
                    if let Err(e) = self
                        .validate_list_fields(&entity, &filters, &sort, projection.as_deref())
                        .await
                    {
                        return e.into();
                    }
                    if let Some(uid) = sender
                        && !ownership.is_admin(uid)
                        && let Some(owner_field) = ownership.owner_field(&entity)
                    {
                        filters.push(crate::Filter::new(
                            owner_field.to_string(),
                            crate::FilterOp::Eq,
                            Value::String(uid.to_string()),
                        ));
                    }
                    match self
                        .list_core(entity, filters, sort, pagination, includes, projection)
                        .await
                    {
                        Ok(v) => Response::ok(value_from_vec(v)),
                        Err(e) => e.into(),
                    }
                }
                Request::Subscribe {
                    pattern,
                    entity,
                    share_group,
                    mode,
                } => match (share_group, mode) {
                    (Some(group), Some(m)) => {
                        match self.subscribe_shared(pattern, entity, group, m).await {
                            Ok(result) => Response::ok(serde_json::json!({
                                "id": result.id,
                                "assigned_partitions": result.assigned_partitions
                            })),
                            Err(e) => e.into(),
                        }
                    }
                    _ => match self.subscribe(pattern, entity).await {
                        Ok(id) => Response::ok(value_from_string(id)),
                        Err(e) => e.into(),
                    },
                },
                Request::Unsubscribe { id } => match self.unsubscribe(&id).await {
                    Ok(()) => Response::ok(value_from_unit(())),
                    Err(e) => e.into(),
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{OwnershipConfig, ScopeConfig};

    #[test]
    fn test_request_serialization() {
        let request = Request::Create {
            entity: "users".to_string(),
            data: serde_json::json!({"name": "Alice"}),
        };
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"op\":\"create\""));
        assert!(json.contains("\"entity\":\"users\""));
    }

    #[test]
    fn test_request_deserialization() {
        let json = r#"{"op": "read", "entity": "users", "id": "123"}"#;
        let request: Request = serde_json::from_str(json).unwrap();
        match request {
            Request::Read { entity, id, .. } => {
                assert_eq!(entity, "users");
                assert_eq!(id, "123");
            }
            _ => panic!("expected Read request"),
        }
    }

    #[test]
    fn test_response_ok() {
        let response = Response::ok(serde_json::json!({"id": "1", "name": "Alice"}));
        assert!(response.is_ok());
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"status\":\"ok\""));
    }

    #[test]
    fn test_response_error() {
        let response = Response::error(ErrorCode::NotFound, "User not found");
        assert!(response.is_error());
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"status\":\"error\""));
        assert!(json.contains("\"code\":404"));
    }

    #[test]
    fn test_error_code_mapping() {
        assert_eq!(ErrorCode::NotFound.as_u16(), 404);
        assert_eq!(ErrorCode::BadRequest.as_u16(), 400);
        assert_eq!(ErrorCode::Conflict.as_u16(), 409);
        assert_eq!(ErrorCode::Internal.as_u16(), 500);

        assert_eq!(ErrorCode::NotFound.as_grpc_code(), 5);
        assert_eq!(ErrorCode::BadRequest.as_grpc_code(), 3);
    }

    #[test]
    fn test_error_conversion() {
        let error = Error::NotFound {
            entity: "users".to_string(),
            id: "123".to_string(),
        };
        let response: Response = error.into();
        match response {
            Response::Error { code, message } => {
                assert_eq!(code, 404);
                assert!(message.contains("not found"));
            }
            Response::Ok { .. } => panic!("expected error response"),
        }
    }

    #[cfg(feature = "agent")]
    #[tokio::test]
    async fn execute_with_sender_update_forbidden_for_non_owner() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = crate::Database::open(tmp.path()).await.unwrap();
        let ownership = OwnershipConfig::parse("diagrams=userId").unwrap();

        let create_req = Request::Create {
            entity: "diagrams".to_string(),
            data: serde_json::json!({"userId": "alice", "title": "My Diagram"}),
        };
        let create_resp = db.execute(create_req).await;
        let id = match &create_resp {
            Response::Ok { data } => data["id"].as_str().unwrap().to_string(),
            Response::Error { .. } => panic!("expected ok response from create"),
        };

        let update_req = Request::Update {
            entity: "diagrams".to_string(),
            id: id.clone(),
            fields: serde_json::json!({"title": "Stolen"}),
        };
        let resp = db
            .execute_with_sender(
                update_req,
                Some("bob"),
                None,
                &ownership,
                &ScopeConfig::default(),
                None,
            )
            .await;
        match resp {
            Response::Error { code, message } => {
                assert_eq!(code, 403);
                assert!(message.contains("forbidden"));
            }
            Response::Ok { .. } => panic!("expected forbidden"),
        }
    }

    #[cfg(feature = "agent")]
    #[tokio::test]
    async fn execute_with_sender_update_allowed_for_owner() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = crate::Database::open(tmp.path()).await.unwrap();
        let ownership = OwnershipConfig::parse("diagrams=userId").unwrap();

        let create_resp = db
            .execute(Request::Create {
                entity: "diagrams".to_string(),
                data: serde_json::json!({"userId": "alice", "title": "Orig"}),
            })
            .await;
        let id = match &create_resp {
            Response::Ok { data } => data["id"].as_str().unwrap().to_string(),
            Response::Error { .. } => panic!("expected ok"),
        };

        let resp = db
            .execute_with_sender(
                Request::Update {
                    entity: "diagrams".to_string(),
                    id,
                    fields: serde_json::json!({"title": "Updated"}),
                },
                Some("alice"),
                None,
                &ownership,
                &ScopeConfig::default(),
                None,
            )
            .await;
        assert!(resp.is_ok());
    }

    #[cfg(feature = "agent")]
    #[tokio::test]
    async fn execute_with_sender_delete_forbidden_for_non_owner() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = crate::Database::open(tmp.path()).await.unwrap();
        let ownership = OwnershipConfig::parse("diagrams=userId").unwrap();

        let create_resp = db
            .execute(Request::Create {
                entity: "diagrams".to_string(),
                data: serde_json::json!({"userId": "alice", "title": "Private"}),
            })
            .await;
        let id = match &create_resp {
            Response::Ok { data } => data["id"].as_str().unwrap().to_string(),
            Response::Error { .. } => panic!("expected ok"),
        };

        let resp = db
            .execute_with_sender(
                Request::Delete {
                    entity: "diagrams".to_string(),
                    id,
                },
                Some("bob"),
                None,
                &ownership,
                &ScopeConfig::default(),
                None,
            )
            .await;
        match resp {
            Response::Error { code, .. } => assert_eq!(code, 403),
            Response::Ok { .. } => panic!("expected forbidden"),
        }
    }

    #[cfg(feature = "agent")]
    #[tokio::test]
    async fn execute_with_sender_list_filters_by_owner() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = crate::Database::open(tmp.path()).await.unwrap();
        let ownership = OwnershipConfig::parse("diagrams=userId").unwrap();

        db.execute(Request::Create {
            entity: "diagrams".to_string(),
            data: serde_json::json!({"userId": "alice", "title": "Alice's"}),
        })
        .await;
        db.execute(Request::Create {
            entity: "diagrams".to_string(),
            data: serde_json::json!({"userId": "bob", "title": "Bob's"}),
        })
        .await;

        let resp = db
            .execute_with_sender(
                Request::List {
                    entity: "diagrams".to_string(),
                    filters: vec![],
                    sort: vec![],
                    pagination: None,
                    includes: vec![],
                    projection: None,
                },
                Some("alice"),
                None,
                &ownership,
                &ScopeConfig::default(),
                None,
            )
            .await;

        match resp {
            Response::Ok { data } => {
                let items = data.as_array().unwrap();
                assert_eq!(items.len(), 1);
                assert_eq!(items[0]["userId"], "alice");
            }
            Response::Error { .. } => panic!("expected ok"),
        }
    }

    #[cfg(feature = "agent")]
    #[tokio::test]
    async fn execute_with_sender_none_bypasses_ownership() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = crate::Database::open(tmp.path()).await.unwrap();
        let ownership = OwnershipConfig::parse("diagrams=userId").unwrap();

        let create_resp = db
            .execute(Request::Create {
                entity: "diagrams".to_string(),
                data: serde_json::json!({"userId": "alice", "title": "Internal"}),
            })
            .await;
        let id = match &create_resp {
            Response::Ok { data } => data["id"].as_str().unwrap().to_string(),
            Response::Error { .. } => panic!("expected ok"),
        };

        let resp = db
            .execute_with_sender(
                Request::Update {
                    entity: "diagrams".to_string(),
                    id,
                    fields: serde_json::json!({"title": "System Override"}),
                },
                None,
                None,
                &ownership,
                &ScopeConfig::default(),
                None,
            )
            .await;
        assert!(resp.is_ok());
    }

    #[cfg(feature = "agent")]
    #[tokio::test]
    async fn list_succeeds_when_ownership_field_not_in_schema() {
        use crate::schema::{FieldDefinition, FieldType, Schema};

        let tmp = tempfile::TempDir::new().unwrap();
        let db = crate::Database::open(tmp.path()).await.unwrap();

        let schema =
            Schema::new("diagrams").add_field(FieldDefinition::new("title", FieldType::String));
        db.add_schema(schema).await.unwrap();

        let ownership = OwnershipConfig::parse("diagrams=userId").unwrap();

        db.execute(Request::Create {
            entity: "diagrams".to_string(),
            data: serde_json::json!({"userId": "alice", "title": "Test"}),
        })
        .await;

        let resp = db
            .execute_with_sender(
                Request::List {
                    entity: "diagrams".to_string(),
                    filters: vec![],
                    sort: vec![],
                    pagination: None,
                    includes: vec![],
                    projection: None,
                },
                Some("alice"),
                None,
                &ownership,
                &ScopeConfig::default(),
                None,
            )
            .await;

        match resp {
            Response::Ok { data } => {
                let items = data.as_array().unwrap();
                assert_eq!(items.len(), 1);
            }
            Response::Error { code, message } => {
                panic!("expected ok, got error {code}: {message}");
            }
        }
    }

    #[cfg(feature = "agent")]
    #[tokio::test]
    async fn list_works_when_ownership_field_in_schema() {
        use crate::schema::{FieldDefinition, FieldType, Schema};

        let tmp = tempfile::TempDir::new().unwrap();
        let db = crate::Database::open(tmp.path()).await.unwrap();

        let schema = Schema::new("diagrams")
            .add_field(FieldDefinition::new("title", FieldType::String))
            .add_field(FieldDefinition::new("userId", FieldType::String));
        db.add_schema(schema).await.unwrap();

        let ownership = OwnershipConfig::parse("diagrams=userId").unwrap();

        db.execute(Request::Create {
            entity: "diagrams".to_string(),
            data: serde_json::json!({"userId": "alice", "title": "Test"}),
        })
        .await;

        let resp = db
            .execute_with_sender(
                Request::List {
                    entity: "diagrams".to_string(),
                    filters: vec![],
                    sort: vec![],
                    pagination: None,
                    includes: vec![],
                    projection: None,
                },
                Some("alice"),
                None,
                &ownership,
                &ScopeConfig::default(),
                None,
            )
            .await;

        match resp {
            Response::Ok { data } => {
                let items = data.as_array().unwrap();
                assert_eq!(items.len(), 1);
            }
            Response::Error { code, message } => {
                panic!("expected ok, got error {code}: {message}");
            }
        }
    }

    #[cfg(feature = "agent")]
    #[tokio::test]
    async fn list_rejects_filter_on_nonexistent_schema_field() {
        use crate::schema::{FieldDefinition, FieldType, Schema};

        let tmp = tempfile::TempDir::new().unwrap();
        let db = crate::Database::open(tmp.path()).await.unwrap();

        let schema =
            Schema::new("users").add_field(FieldDefinition::new("name", FieldType::String));
        db.add_schema(schema).await.unwrap();

        db.execute(Request::Create {
            entity: "users".to_string(),
            data: serde_json::json!({"name": "Alice"}),
        })
        .await;

        let resp = db
            .execute(Request::List {
                entity: "users".to_string(),
                filters: vec![crate::Filter::new(
                    "bogus".to_string(),
                    crate::FilterOp::Eq,
                    serde_json::json!("x"),
                )],
                sort: vec![],
                pagination: None,
                includes: vec![],
                projection: None,
            })
            .await;

        match resp {
            Response::Error { code, message } => {
                assert_eq!(code, 400);
                assert!(
                    message.contains("bogus"),
                    "expected error to mention 'bogus', got: {message}"
                );
            }
            Response::Ok { .. } => panic!("expected schema validation error for bogus field"),
        }
    }

    #[cfg(feature = "agent")]
    #[tokio::test]
    async fn update_strips_ownership_field_for_non_admin() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = crate::Database::open(tmp.path()).await.unwrap();
        let ownership = OwnershipConfig::parse("diagrams=userId").unwrap();

        let create_resp = db
            .execute(Request::Create {
                entity: "diagrams".to_string(),
                data: serde_json::json!({"userId": "alice", "title": "Orig"}),
            })
            .await;
        let id = match &create_resp {
            Response::Ok { data } => data["id"].as_str().unwrap().to_string(),
            Response::Error { .. } => panic!("expected ok"),
        };

        let resp = db
            .execute_with_sender(
                Request::Update {
                    entity: "diagrams".to_string(),
                    id: id.clone(),
                    fields: serde_json::json!({"userId": "bob", "title": "Hijacked"}),
                },
                Some("alice"),
                None,
                &ownership,
                &ScopeConfig::default(),
                None,
            )
            .await;
        assert!(resp.is_ok());

        let read_resp = db
            .execute(Request::Read {
                entity: "diagrams".to_string(),
                id,
                includes: vec![],
                projection: None,
            })
            .await;
        match read_resp {
            Response::Ok { data } => {
                assert_eq!(data["userId"], "alice");
                assert_eq!(data["title"], "Hijacked");
            }
            Response::Error { .. } => panic!("expected ok"),
        }
    }

    #[cfg(feature = "agent")]
    #[tokio::test]
    async fn read_forbidden_for_non_owner() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = crate::Database::open(tmp.path()).await.unwrap();
        let ownership = OwnershipConfig::parse("diagrams=userId").unwrap();

        let create_resp = db
            .execute(Request::Create {
                entity: "diagrams".to_string(),
                data: serde_json::json!({"userId": "alice", "title": "Private"}),
            })
            .await;
        let id = match &create_resp {
            Response::Ok { data } => data["id"].as_str().unwrap().to_string(),
            Response::Error { .. } => panic!("expected ok"),
        };

        let resp = db
            .execute_with_sender(
                Request::Read {
                    entity: "diagrams".to_string(),
                    id: id.clone(),
                    includes: vec![],
                    projection: None,
                },
                Some("bob"),
                None,
                &ownership,
                &ScopeConfig::default(),
                None,
            )
            .await;
        match resp {
            Response::Error { code, .. } => assert_eq!(code, 403),
            Response::Ok { .. } => panic!("expected forbidden"),
        }

        let resp = db
            .execute_with_sender(
                Request::Read {
                    entity: "diagrams".to_string(),
                    id,
                    includes: vec![],
                    projection: None,
                },
                Some("alice"),
                None,
                &ownership,
                &ScopeConfig::default(),
                None,
            )
            .await;
        assert!(resp.is_ok());
    }

    #[cfg(feature = "agent")]
    #[tokio::test]
    async fn two_users_see_only_their_own_records() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = crate::Database::open(tmp.path()).await.unwrap();
        let ownership = OwnershipConfig::parse("diagrams=userId").unwrap();

        db.execute(Request::Create {
            entity: "diagrams".to_string(),
            data: serde_json::json!({"userId": "alice", "title": "Alice Doc 1"}),
        })
        .await;

        db.execute(Request::Create {
            entity: "diagrams".to_string(),
            data: serde_json::json!({"userId": "alice", "title": "Alice Doc 2"}),
        })
        .await;

        db.execute(Request::Create {
            entity: "diagrams".to_string(),
            data: serde_json::json!({"userId": "bob", "title": "Bob Doc 1"}),
        })
        .await;

        let alice_items = list_as_user(&db, "diagrams", vec![], "alice", &ownership).await;
        assert_eq!(alice_items.len(), 2);
        for item in &alice_items {
            assert_eq!(item["userId"], "alice");
        }

        let bob_items = list_as_user(&db, "diagrams", vec![], "bob", &ownership).await;
        assert_eq!(bob_items.len(), 1);
        assert_eq!(bob_items[0]["userId"], "bob");
        assert_eq!(bob_items[0]["title"], "Bob Doc 1");
    }

    #[cfg(feature = "agent")]
    #[tokio::test]
    async fn ownership_list_composes_with_user_filter() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = crate::Database::open(tmp.path()).await.unwrap();
        let ownership = OwnershipConfig::parse("diagrams=userId").unwrap();

        db.execute(Request::Create {
            entity: "diagrams".to_string(),
            data: serde_json::json!({"userId": "alice", "title": "Alice Doc 1"}),
        })
        .await;

        db.execute(Request::Create {
            entity: "diagrams".to_string(),
            data: serde_json::json!({"userId": "alice", "title": "Alice Doc 2"}),
        })
        .await;

        db.execute(Request::Create {
            entity: "diagrams".to_string(),
            data: serde_json::json!({"userId": "bob", "title": "Bob Doc 1"}),
        })
        .await;

        let title_filter = vec![crate::Filter::new(
            "title".to_string(),
            crate::FilterOp::Eq,
            serde_json::json!("Alice Doc 1"),
        )];
        let filtered = list_as_user(&db, "diagrams", title_filter, "alice", &ownership).await;
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0]["title"], "Alice Doc 1");
        assert_eq!(filtered[0]["userId"], "alice");
    }

    #[cfg(feature = "agent")]
    async fn list_as_user(
        db: &crate::Database,
        entity: &str,
        filters: Vec<crate::Filter>,
        user: &str,
        ownership: &OwnershipConfig,
    ) -> Vec<serde_json::Value> {
        let resp = db
            .execute_with_sender(
                Request::List {
                    entity: entity.to_string(),
                    filters,
                    sort: vec![],
                    pagination: None,
                    includes: vec![],
                    projection: None,
                },
                Some(user),
                None,
                ownership,
                &ScopeConfig::default(),
                None,
            )
            .await;
        match resp {
            Response::Ok { data } => data.as_array().unwrap().clone(),
            Response::Error { code, message } => {
                panic!("expected ok for {user}, got {code}: {message}")
            }
        }
    }
}
