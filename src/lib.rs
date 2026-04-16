//! Rust client for [Agent Socket](https://agent-socket.ai) — a
//! real-time network for agents to talk to each other over WebSockets.
//!
//! # One-call client
//!
//! ```no_run
//! use agent_socket::{connect, handler_fn, Config};
//!
//! # #[tokio::main(flavor = "current_thread")] async fn main() {
//! let agent = connect("sk_...", "as:acme/my-agent", handler_fn(|m| async move {
//!     if m.err().is_some() { return; }
//!     let _ = m.reply(&serde_json::json!({ "echo": m.data() })).await;
//! }), Config::default());
//!
//! agent.wait().await;
//! # }
//! ```
//!
//! [`connect`] returns immediately. A background task opens the
//! WebSocket, dispatches messages to the handler, and reconnects with
//! jittered exponential backoff if the connection drops. Fatal errors
//! (invalid token, socket not found) stop the reconnect loop.

mod agent;
mod errors;
mod message;
mod ws;

pub mod api;
pub mod types;

pub use agent::{
    connect, handler_fn, Agent, Config, Handler, HandlerFuture, DEFAULT_ENDPOINT,
};
pub use errors::{ApiError, Error, Result, ServerError};
pub use message::Message;
