use crate::subscription::SubscriptionMode;
use crate::{Error, Filter, Pagination, SortOrder};
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    NotFound = 404,
    BadRequest = 400,
    Conflict = 409,
    Internal = 500,
}

impl ErrorCode {
    pub fn as_u16(self) -> u16 {
        self as u16
    }

    pub fn as_grpc_code(self) -> i32 {
        match self {
            ErrorCode::NotFound => 5,
            ErrorCode::BadRequest => 3,
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
    pub fn ok(data: Value) -> Self {
        Response::Ok { data }
    }

    pub fn error(code: ErrorCode, message: impl Into<String>) -> Self {
        Response::Error {
            code: code.as_u16(),
            message: message.into(),
        }
    }

    pub fn is_ok(&self) -> bool {
        matches!(self, Response::Ok { .. })
    }

    pub fn is_error(&self) -> bool {
        matches!(self, Response::Error { .. })
    }
}

impl From<Error> for Response {
    fn from(e: Error) -> Self {
        let (code, message) = match &e {
            Error::NotFound { .. } => (ErrorCode::NotFound, e.to_string()),
            Error::Validation(_) | Error::SchemaViolation { .. } => {
                (ErrorCode::BadRequest, e.to_string())
            }
            Error::ConstraintViolation(_)
            | Error::UniqueViolation { .. }
            | Error::ForeignKeyViolation { .. }
            | Error::NotNullViolation { .. }
            | Error::Conflict(_) => (ErrorCode::Conflict, e.to_string()),
            _ => (ErrorCode::Internal, e.to_string()),
        };
        Response::error(code, message)
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod execute {
    use super::{Request, Response};
    use crate::Database;
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
            match request {
                Request::Create { entity, data } => match self.create(entity, data).await {
                    Ok(v) => Response::ok(v),
                    Err(e) => e.into(),
                },
                Request::Read {
                    entity,
                    id,
                    includes,
                    projection,
                } => match self.read(entity, id, includes, projection).await {
                    Ok(v) => Response::ok(v),
                    Err(e) => e.into(),
                },
                Request::Update { entity, id, fields } => {
                    match self.update(entity, id, fields).await {
                        Ok(v) => Response::ok(v),
                        Err(e) => e.into(),
                    }
                }
                Request::Delete { entity, id } => match self.delete(entity, id).await {
                    Ok(()) => Response::ok(value_from_unit(())),
                    Err(e) => e.into(),
                },
                Request::List {
                    entity,
                    filters,
                    sort,
                    pagination,
                    includes,
                    projection,
                } => {
                    match self
                        .list(entity, filters, sort, pagination, includes, projection)
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
            _ => panic!("expected error response"),
        }
    }
}
