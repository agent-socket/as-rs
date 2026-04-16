//! WebSocket transport for the Agent Socket wire protocol.
//!
//! Wraps `tokio-tungstenite`. Exposes a simple message-in / message-out
//! interface — the supervisor is responsible for reconnect and event
//! dispatch.

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use http::Request;
use serde::Serialize;
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{self, client::IntoClientRequest, protocol::CloseFrame, Message as WsMessage};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use crate::errors::{Error, Result, ServerError};

const WIRE_ERROR: &str = "error";

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(crate) struct WsConn {
    pub sink: SplitSink<WsStream, WsMessage>,
    pub stream: SplitStream<WsStream>,
}

/// An event read off the WebSocket.
#[derive(Debug)]
pub(crate) enum Event {
    /// Incoming message from another socket or channel.
    Message { from: String, data: Value },
    /// Server-sent error frame (mid-connection, not fatal by itself).
    ServerError(ServerError),
    /// Non-fatal decode failure on a frame.
    Decode(Error),
    /// Connection closed cleanly.
    Closed,
    /// Connection closed abnormally.
    ConnectionLost(Error),
}

/// Open a connection to `endpoint/socket_id` with Bearer auth.
///
/// Returns a typed [`ServerError`] when the server rejects the upgrade
/// with a known status, or a transport error otherwise.
pub(crate) async fn dial(endpoint: &str, token: &str, socket_id: &str) -> Result<WsConn> {
    let url = format!("{}/{}", endpoint.trim_end_matches('/'), socket_id);

    let mut req: Request<()> = url
        .into_client_request()
        .map_err(|e| Error::Transport(format!("build request: {}", e)))?;
    req.headers_mut().insert(
        http::header::AUTHORIZATION,
        format!("Bearer {}", token)
            .parse()
            .map_err(|e| Error::Transport(format!("invalid token: {}", e)))?,
    );

    match connect_async(req).await {
        Ok((stream, _response)) => {
            let (sink, stream) = stream.split();
            Ok(WsConn { sink, stream })
        }
        Err(err) => Err(map_dial_error(err)),
    }
}

fn map_dial_error(err: tungstenite::Error) -> Error {
    if let tungstenite::Error::Http(resp) = &err {
        let status = resp.status().as_u16();
        let body = resp.body();
        if let Some(bytes) = body {
            if let Ok(parsed) = serde_json::from_slice::<ServerErrorBody>(bytes) {
                return Error::Server(ServerError::new(
                    parsed.error_code.unwrap_or_default(),
                    parsed.error_message.unwrap_or_default(),
                    status,
                ));
            }
        }
        return Error::Server(ServerError::new(
            String::new(),
            format!("websocket dial failed with HTTP {}", status),
            status,
        ));
    }
    Error::Transport(err.to_string())
}

#[derive(serde::Deserialize)]
struct ServerErrorBody {
    error_code: Option<String>,
    error_message: Option<String>,
}

/// Parse one incoming frame into an [`Event`].
pub(crate) fn decode_frame(frame: WsMessage) -> Option<Event> {
    match frame {
        WsMessage::Text(txt) => Some(decode_payload(txt.as_str())),
        WsMessage::Binary(bytes) => match std::str::from_utf8(&bytes) {
            Ok(txt) => Some(decode_payload(txt)),
            Err(err) => Some(Event::Decode(Error::Transport(format!(
                "non-utf8 binary frame: {}",
                err
            )))),
        },
        WsMessage::Close(frame) => Some(map_close(frame)),
        WsMessage::Ping(_) | WsMessage::Pong(_) | WsMessage::Frame(_) => None,
    }
}

fn decode_payload(txt: &str) -> Event {
    let parsed: Value = match serde_json::from_str(txt) {
        Ok(v) => v,
        Err(err) => return Event::Decode(Error::Serde(err)),
    };

    if parsed.get("type").and_then(|v| v.as_str()) == Some(WIRE_ERROR) {
        let code = parsed
            .get("code")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let message = parsed
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        return Event::ServerError(ServerError::new(code, message, 0));
    }

    let from = parsed
        .get("from")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if from.is_empty() {
        return Event::Decode(Error::Transport(
            "server frame missing 'from' field".into(),
        ));
    }
    let data = parsed
        .get("data")
        .cloned()
        .unwrap_or(Value::Null);
    Event::Message {
        from: from.to_string(),
        data,
    }
}

fn map_close(frame: Option<CloseFrame<'_>>) -> Event {
    match frame {
        Some(f) => {
            let code: u16 = f.code.into();
            if code == 1000 || code == 1001 {
                Event::Closed
            } else {
                Event::ConnectionLost(Error::Transport(format!(
                    "connection closed: {} {}",
                    code, f.reason
                )))
            }
        }
        None => Event::Closed,
    }
}

/// Serialize `payload` into an outgoing `{to, data}` frame and send it.
pub(crate) async fn send_frame<T: Serialize>(
    sink: &mut SplitSink<WsStream, WsMessage>,
    to: &str,
    payload: &T,
) -> Result<()> {
    let frame = serde_json::json!({ "to": to, "data": payload });
    let text = serde_json::to_string(&frame)?;
    sink.send(WsMessage::Text(text.into()))
        .await
        .map_err(|e| Error::Transport(e.to_string()))
}
