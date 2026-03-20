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
            Response::Ok { .. } => panic!("expected error response"),
        }
    }
}
