//! Supervisor behaviour tests — reconnect, fatal-error detection,
//! send-blocking, reply. Uses a lightweight local WebSocket server
//! built on `tokio-tungstenite` to exercise the real wire protocol.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_socket::{connect, handler_fn, Config, Error, Message};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Instant};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::protocol::{frame::coding::CloseCode, CloseFrame};
use tokio_tungstenite::tungstenite::Message as WsMessage;

/// Spawn a local WS server that invokes `handler(attempt, ws)` on
/// every connection. Returns `(ws_url, attempts_counter)`.
async fn serve<F, Fut>(handler: F) -> (String, Arc<Mutex<u32>>, mpsc::Sender<()>)
where
    F: Fn(u32, tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>) -> Fut
        + Send
        + Sync
        + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let attempts = Arc::new(Mutex::new(0u32));
    let attempts_ret = Arc::clone(&attempts);
    let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
    let handler = Arc::new(handler);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = stop_rx.recv() => break,
                accepted = listener.accept() => {
                    if let Ok((sock, _)) = accepted {
                        let n = {
                            let mut a = attempts.lock().unwrap();
                            *a += 1;
                            *a
                        };
                        let h = Arc::clone(&handler);
                        tokio::spawn(async move {
                            match tokio_tungstenite::accept_async(sock).await {
                                Ok(ws) => h(n, ws).await,
                                Err(_) => {}
                            }
                        });
                    }
                }
            }
        }
    });

    (format!("ws://127.0.0.1:{}", port), attempts_ret, stop_tx)
}

/// Spawn a server that rejects the WS handshake with a typed body.
async fn serve_rejecting(status: StatusCode, body: &'static str) -> (String, mpsc::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = stop_rx.recv() => break,
                accepted = listener.accept() => {
                    if let Ok((sock, _)) = accepted {
                        let status_code = status;
                        tokio::spawn(async move {
                            let callback = |_req: &Request, _resp: Response| -> Result<Response, ErrorResponse> {
                                let resp = tokio_tungstenite::tungstenite::http::Response::builder()
                                    .status(status_code)
                                    .header("content-type", "application/json")
                                    .body(Some(body.to_string()))
                                    .unwrap();
                                Err(resp)
                            };
                            let _ = tokio_tungstenite::accept_hdr_async(sock, callback).await;
                        });
                    }
                }
            }
        }
    });

    (format!("ws://127.0.0.1:{}", port), stop_tx)
}

fn collector() -> (Arc<Mutex<Vec<(Option<String>, serde_json::Value, bool)>>>, impl agent_socket::Handler) {
    let events: Arc<Mutex<Vec<(Option<String>, serde_json::Value, bool)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let events2 = Arc::clone(&events);
    let handler = handler_fn(move |m: Message| {
        let events = Arc::clone(&events2);
        async move {
            let mut v = events.lock().unwrap();
            if let Some(err) = m.err() {
                v.push((None, serde_json::json!(err.to_string()), true));
            } else {
                v.push((Some(m.from().to_string()), m.data().clone(), false));
            }
        }
    });
    (events, handler)
}

#[tokio::test]
async fn reconnects_after_drop() {
    let (url, attempts, stop) = serve(|n, mut ws| async move {
        if n == 1 {
            let _ = ws
                .send(WsMessage::Text(
                    serde_json::to_string(&serde_json::json!({
                        "from": "as:test/sender", "data": "first"
                    }))
                    .unwrap().into(),
                ))
                .await;
            sleep(Duration::from_millis(20)).await;
            let _ = ws
                .close(Some(CloseFrame {
                    code: CloseCode::Error,
                    reason: "drop".into(),
                }))
                .await;
        } else {
            let _ = ws
                .send(WsMessage::Text(
                    serde_json::to_string(&serde_json::json!({
                        "from": "as:test/sender", "data": "second"
                    }))
                    .unwrap().into(),
                ))
                .await;
            // Hold open.
            while ws.next().await.is_some() {}
        }
    })
    .await;

    let (events, handler) = collector();
    let mut agent = connect(
        "tok",
        "as:test/me",
        handler,
        Config {
            endpoint: url.clone(),
            min_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(100),
        },
    );

    // Wait up to 5s for at least 3 events.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let n = events.lock().unwrap().len();
        if n >= 3 || Instant::now() >= deadline {
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }

    let v = events.lock().unwrap().clone();
    let msgs: Vec<_> = v.iter().filter(|(_, _, is_err)| !is_err).collect();
    let errs: Vec<_> = v.iter().filter(|(_, _, is_err)| *is_err).collect();
    assert!(msgs.len() >= 2, "want >=2 messages, got {}: {:?}", msgs.len(), v);
    assert!(errs.len() >= 1, "want >=1 error, got {}: {:?}", errs.len(), v);
    assert!(*attempts.lock().unwrap() >= 2);

    agent.close().await;
    let _ = stop.send(()).await;
}

#[tokio::test]
async fn stops_on_auth_error() {
    let (url, stop) = serve_rejecting(
        StatusCode::UNAUTHORIZED,
        r#"{"error_code":"E1001","error_message":"bad token"}"#,
    )
    .await;

    let (_events, handler) = collector();
    let mut agent = connect(
        "bad-token",
        "as:test/me",
        handler,
        Config {
            endpoint: url,
            min_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(100),
        },
    );

    // Must stop within 3s.
    timeout(Duration::from_secs(3), agent.wait())
        .await
        .expect("agent never gave up on 401");

    let err = agent.err().expect("want fatal error");
    match err {
        Error::Server(se) => {
            assert_eq!(se.status, 401);
            assert!(se.is_fatal());
        }
        other => panic!("want ServerError, got {:?}", other),
    }

    let _ = stop.send(()).await;
}

#[tokio::test]
async fn send_blocks_until_connected() {
    let received: Arc<Mutex<Option<serde_json::Value>>> = Arc::new(Mutex::new(None));
    let received_srv = Arc::clone(&received);

    let (url, _attempts, stop) = serve(move |_n, mut ws| {
        let received = Arc::clone(&received_srv);
        async move {
            while let Some(frame) = ws.next().await {
                if let Ok(WsMessage::Text(txt)) = frame {
                    *received.lock().unwrap() = Some(serde_json::from_str(&txt).unwrap());
                    break;
                }
            }
        }
    })
    .await;

    let (_events, handler) = collector();
    let agent = connect(
        "tok",
        "as:test/me",
        handler,
        Config {
            endpoint: url,
            min_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(100),
        },
    );

    // Fire send immediately — before dial could complete.
    timeout(
        Duration::from_secs(3),
        agent.send("as:test/peer", &serde_json::json!({ "ping": 1 })),
    )
    .await
    .expect("send timed out")
    .expect("send failed");

    // Give the server a moment to record.
    let deadline = Instant::now() + Duration::from_secs(3);
    while received.lock().unwrap().is_none() && Instant::now() < deadline {
        sleep(Duration::from_millis(20)).await;
    }

    let got = received.lock().unwrap().clone().expect("never received");
    assert_eq!(
        got,
        serde_json::json!({ "to": "as:test/peer", "data": { "ping": 1 } })
    );

    agent.close().await;
    let _ = stop.send(()).await;
}

#[tokio::test]
async fn reply_from_handler() {
    let replies: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
    let replies_srv = Arc::clone(&replies);

    let (url, _attempts, stop) = serve(move |_n, mut ws| {
        let replies = Arc::clone(&replies_srv);
        async move {
            let _ = ws
                .send(WsMessage::Text(
                    serde_json::to_string(&serde_json::json!({
                        "from": "as:test/peer", "data": { "ping": 1 }
                    }))
                    .unwrap().into(),
                ))
                .await;
            while let Some(frame) = ws.next().await {
                if let Ok(WsMessage::Text(txt)) = frame {
                    replies.lock().unwrap().push(serde_json::from_str(&txt).unwrap());
                    break;
                }
            }
        }
    })
    .await;

    let handler = handler_fn(|m: Message| async move {
        if m.err().is_some() {
            return;
        }
        let _ = m
            .reply(&serde_json::json!({ "pong": m.data() }))
            .await;
    });

    let agent = connect(
        "tok",
        "as:test/me",
        handler,
        Config {
            endpoint: url,
            min_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(100),
        },
    );

    let deadline = Instant::now() + Duration::from_secs(3);
    while replies.lock().unwrap().is_empty() && Instant::now() < deadline {
        sleep(Duration::from_millis(20)).await;
    }

    let v = replies.lock().unwrap().clone();
    assert!(!v.is_empty(), "no reply received");
    assert_eq!(
        v[0],
        serde_json::json!({ "to": "as:test/peer", "data": { "pong": { "ping": 1 } } })
    );

    agent.close().await;
    let _ = stop.send(()).await;
}
