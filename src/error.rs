use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("storage error: {0}")]
    Storage(#[from] fjall::Error),

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
}

pub type Result<T> = std::result::Result<T, Error>;
