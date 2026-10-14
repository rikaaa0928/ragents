use openresponses_rust::ClientError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("openresponses client error: {0}")]
    Client(Box<ClientError>),
    #[error("model response failed: {0}")]
    ResponseFailed(String),
    #[error("tool not found: {0}")]
    ToolNotFound(String),
    #[error("tool error: {0}")]
    ToolError(String),
    #[error("hook '{hook}' rejected request: {reason}")]
    HookRejected { hook: String, reason: String },
    #[error("configuration error: {0}")]
    ConfigError(String),
    #[error("host error: {0}")]
    HostError(String),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("activation cancelled")]
    Cancelled,
}

impl From<ClientError> for CoreError {
    fn from(err: ClientError) -> Self {
        CoreError::Client(Box::new(err))
    }
}
