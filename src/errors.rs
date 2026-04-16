//! Error types for the Agent Socket client.

use std::fmt;

use thiserror::Error;

/// All errors returned by the client.
#[derive(Debug, Error)]
pub enum Error {
    /// Standardized error returned by the Agent Socket server.
    #[error(transparent)]
    Server(#[from] ServerError),

    /// Error returned by the REST API.
    #[error(transparent)]
    Api(#[from] ApiError),

    /// Transport-level I/O or protocol error.
    #[error("transport: {0}")]
    Transport(String),

    /// The agent is closed and will not accept further operations.
    #[error("agent closed")]
    Closed,

    /// `Message::reply` was called on an error event.
    #[error("cannot reply on error event")]
    ReplyOnError,

    /// JSON (de)serialization failure.
    #[error("serialization: {0}")]
    Serde(#[from] serde_json::Error),
}

/// Standardized error returned by the Agent Socket server.
///
/// Carries the server-assigned [`code`](Self::code), human-readable
/// [`message`](Self::message), and the transport-level
/// [`status`](Self::status) (HTTP status on dial, 0 for error frames
/// received over an open socket).
#[derive(Debug, Clone, Error)]
pub struct ServerError {
    pub code: String,
    pub message: String,
    pub status: u16,
}

impl ServerError {
    pub fn new(code: impl Into<String>, message: impl Into<String>, status: u16) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            status,
        }
    }

    /// Whether this error should stop the reconnect loop. Auth failures
    /// (401/403) and a missing socket (404) are terminal.
    pub fn is_fatal(&self) -> bool {
        matches!(self.status, 401 | 403 | 404)
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let code = if self.code.is_empty() {
            "unknown"
        } else {
            &self.code
        };
        if self.status == 0 {
            write!(f, "[{}] {}", code, self.message)
        } else {
            write!(f, "[{}] {} (status {})", code, self.message, self.status)
        }
    }
}

/// Error returned by the REST API.
#[derive(Debug, Clone, Error)]
#[error("api error (status {status}): {message}")]
pub struct ApiError {
    pub status: u16,
    pub message: String,
}

impl ApiError {
    pub fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    pub fn is_not_found(&self) -> bool {
        self.status == 404
    }
    pub fn is_unauthorized(&self) -> bool {
        self.status == 401
    }
    pub fn is_forbidden(&self) -> bool {
        self.status == 403
    }
}

/// Convenience `Result` alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;
