pub mod http;
pub mod tunnel;

pub use http::*;
pub use ragents_protocol;
pub use tunnel::*;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("websocket error: {0}")]
    WebSocket(#[from] Box<tokio_tungstenite::tungstenite::Error>),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("api server error ({status}): {message}")]
    Api { status: u16, message: String },
    #[error("daemon tunnel error: {0}")]
    Tunnel(String),
}
