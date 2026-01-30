use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[cfg(feature = "agent")]
    #[error("storage error: {0}")]
    Storage(#[from] fjall::Error),

    #[error("storage error: {0}")]
    StorageGeneric(String),

    #[error("entity not found: {entity} id={id}")]
    NotFound { entity: String, id: String },

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("invalid key format: {0}")]
    InvalidKey(String),

    #[error("validation error: {0}")]
    Validation(String),

    #[error("constraint violation: {0}")]
    ConstraintViolation(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("system time error: {0}")]
    SystemTime(String),

    #[error("data corruption detected: {entity}/{id} - checksum mismatch")]
    Corruption { entity: String, id: String },

    #[error("backup/restore failed: {0}")]
    BackupFailed(String),

    #[error("concurrent modification conflict: {0}")]
    Conflict(String),

    #[error("schema validation failed: {entity}.{field} - {reason}")]
    SchemaViolation {
        entity: String,
        field: String,
        reason: String,
    },

    #[error("unique constraint violation: {entity}.{field} value '{value}' already exists")]
    UniqueViolation {
        entity: String,
        field: String,
        value: String,
    },

    #[error(
        "foreign key violation: {entity}.{field} references non-existent {target_entity}/{target_id}"
    )]
    ForeignKeyViolation {
        entity: String,
        field: String,
        target_entity: String,
        target_id: String,
    },

    #[error(
        "foreign key restrict: cannot delete {entity}/{id} - referenced by {referencing_entity}"
    )]
    ForeignKeyRestrict {
        entity: String,
        id: String,
        referencing_entity: String,
    },

    #[error("not null constraint violation: {entity}.{field} cannot be null")]
    NotNullViolation { entity: String, field: String },

    #[error("invalid foreign key value type")]
    InvalidForeignKey,
}

pub type Result<T> = std::result::Result<T, Error>;
