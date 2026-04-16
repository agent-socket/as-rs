//! Message delivered to the user handler.

use std::sync::Weak;

use serde_json::Value;

use crate::agent::AgentShared;
use crate::errors::{Error, Result};

/// Single event delivered to the user handler.
///
/// For incoming messages, [`from`](Self::from) and [`data`](Self::data)
/// are populated and [`err`](Self::err) is `None`. For error events
/// (disconnect, auth failure, server error frame), [`err`](Self::err)
/// is `Some(..)` and `from`/`data` are empty/`Null`.
#[derive(Debug)]
pub struct Message {
    from: String,
    data: Value,
    err: Option<Error>,
    agent: Weak<AgentShared>,
}

impl Message {
    pub(crate) fn incoming(from: String, data: Value, agent: Weak<AgentShared>) -> Self {
        Self {
            from,
            data,
            err: None,
            agent,
        }
    }

    pub(crate) fn error(err: Error) -> Self {
        Self {
            from: String::new(),
            data: Value::Null,
            err: Some(err),
            agent: Weak::new(),
        }
    }

    /// Sender address — `"as:acme/agent"` or `"ch:acme/channel"`.
    /// Empty string on error events.
    pub fn from(&self) -> &str {
        &self.from
    }

    /// JSON-decoded payload. `Value::Null` on error events.
    pub fn data(&self) -> &Value {
        &self.data
    }

    /// Takes ownership of the payload, leaving `Value::Null` behind.
    pub fn take_data(&mut self) -> Value {
        std::mem::take(&mut self.data)
    }

    /// The error carried by this event, if any.
    pub fn err(&self) -> Option<&Error> {
        self.err.as_ref()
    }

    /// Send `payload` back to [`from`](Self::from).
    ///
    /// Convenience for `agent.send(m.from(), payload).await`. Returns
    /// [`Error::ReplyOnError`] if called on an error event.
    pub async fn reply<T: serde::Serialize>(&self, payload: &T) -> Result<()> {
        if self.err.is_some() || self.from.is_empty() {
            return Err(Error::ReplyOnError);
        }
        let agent = self.agent.upgrade().ok_or(Error::Closed)?;
        agent.send(&self.from, payload).await
    }
}
