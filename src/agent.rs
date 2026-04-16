//! Agent — the long-lived handle returned by [`connect`].
//!
//! Owns a supervisor task that maintains one WebSocket connection at
//! a time, reconnecting on drop with jittered exponential backoff
//! until [`Agent::close`] is called or a fatal error is observed.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use futures_util::stream::{SplitSink, StreamExt};
use rand::Rng;
use serde::Serialize;
use tokio::sync::{mpsc, watch, Notify};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Instant};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::errors::{Error, Result, ServerError};
use crate::message::Message;
use crate::ws::{self, Event};

/// Default WebSocket endpoint.
pub const DEFAULT_ENDPOINT: &str = "wss://as.agent-socket.ai";

const DEFAULT_MIN_BACKOFF: Duration = Duration::from_millis(500);
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(30);
// Sustained connected time before the backoff counter resets.
const HEALTHY_THRESHOLD: Duration = Duration::from_secs(30);

/// Boxed handler future.
pub type HandlerFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Handler trait — any `Fn(Message) -> HandlerFuture + Send + Sync`
/// satisfies this via [`handler_fn`].
pub trait Handler: Send + Sync + 'static {
    fn call(&self, msg: Message) -> HandlerFuture;
}

impl<F> Handler for F
where
    F: Fn(Message) -> HandlerFuture + Send + Sync + 'static,
{
    fn call(&self, msg: Message) -> HandlerFuture {
        (self)(msg)
    }
}

/// Wrap an `async fn(Message)` into a boxed handler. Use when you
/// don't want to type out `Pin<Box<dyn Future...>>` yourself.
///
/// # Example
/// ```no_run
/// use agent_socket::{connect, handler_fn};
/// # async fn _demo() {
/// let agent = connect("tok", "as:acme/me", handler_fn(|m| async move {
///     if m.err().is_some() { return; }
///     let _ = m.reply(&serde_json::json!({ "echo": m.data() })).await;
/// }), Default::default());
/// # let _ = agent;
/// # }
/// ```
pub fn handler_fn<F, Fut>(f: F) -> impl Handler
where
    F: Fn(Message) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    move |m: Message| -> HandlerFuture { Box::pin(f(m)) }
}

/// Options controlling supervisor behaviour.
#[derive(Clone, Debug)]
pub struct Config {
    /// WebSocket endpoint. Default [`DEFAULT_ENDPOINT`].
    pub endpoint: String,
    /// Initial reconnect delay. Default 500ms.
    pub min_backoff: Duration,
    /// Max reconnect delay. Default 30s.
    pub max_backoff: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_ENDPOINT.to_string(),
            min_backoff: DEFAULT_MIN_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
        }
    }
}

/// Open an Agent Socket connection and return immediately.
///
/// A tokio task opens the WebSocket, dispatches messages to `handler`,
/// and reconnects with exponential backoff if the connection drops.
/// Fatal errors (invalid token, socket not found, permission denied)
/// stop the reconnect loop — call [`Agent::wait`] to block until then.
///
/// # Example
/// ```no_run
/// use agent_socket::{connect, handler_fn};
/// # #[tokio::main(flavor = "current_thread")] async fn main() {
/// let agent = connect("sk_...", "as:acme/my-agent", handler_fn(|m| async move {
///     if m.err().is_some() { return; }
///     let _ = m.reply(&serde_json::json!({ "echo": m.data() })).await;
/// }), Default::default());
/// agent.wait().await;
/// # }
/// ```
pub fn connect<H: Handler>(
    token: impl Into<String>,
    socket_id: impl Into<String>,
    handler: H,
    cfg: Config,
) -> Agent {
    let shared = Arc::new(AgentShared::new(token.into(), socket_id.into(), cfg));

    let (handler_tx, handler_rx) = mpsc::unbounded_channel::<Message>();
    let handler_task = tokio::spawn(dispatch_loop(handler_rx, handler));

    let supervisor_task = tokio::spawn(supervisor(Arc::clone(&shared), handler_tx));

    Agent {
        shared,
        supervisor: Some(supervisor_task),
        dispatcher: Some(handler_task),
    }
}

/// Long-lived handle to an Agent Socket connection.
pub struct Agent {
    shared: Arc<AgentShared>,
    supervisor: Option<JoinHandle<()>>,
    dispatcher: Option<JoinHandle<()>>,
}

impl Agent {
    /// The socket address this agent is connected as.
    pub fn socket_id(&self) -> &str {
        &self.shared.socket_id
    }

    /// Send `payload` to target address `to`.
    ///
    /// `to` is a socket (`as:ns/name`) or channel (`ch:ns/name`).
    /// Blocks until the connection is established or re-established
    /// after a drop. Returns [`Error::Closed`] if the agent is closed,
    /// or the underlying [`ServerError`] on fatal errors.
    pub async fn send<T: Serialize>(&self, to: &str, payload: &T) -> Result<()> {
        self.shared.send(to, payload).await
    }

    /// Block until the agent stops (close, fatal error).
    pub async fn wait(&mut self) {
        if let Some(handle) = self.supervisor.take() {
            let _ = handle.await;
        }
        if let Some(handle) = self.dispatcher.take() {
            let _ = handle.await;
        }
    }

    /// Tear down the agent. Resolves once the supervisor exits.
    pub async fn close(mut self) {
        self.shared.stop.notify_waiters();
        self.wait().await;
    }

    /// The most recent error. `None` during a healthy connection.
    pub fn err(&self) -> Option<Error> {
        let state = self.shared.state.lock().expect("state lock poisoned");
        state
            .fatal_err
            .as_ref()
            .map(|e| Error::Server(e.clone()))
            .or_else(|| state.last_err.as_ref().map(|msg| Error::Transport(msg.clone())))
    }

    /// `true` if the supervisor has stopped.
    pub fn is_done(&self) -> bool {
        self.supervisor
            .as_ref()
            .map_or(true, |h| h.is_finished())
    }
}

/// Shared state between the Agent handle, supervisor, and any Message
/// that holds a `Weak` reference to enable `Message::reply`.
pub(crate) struct AgentShared {
    pub token: String,
    pub socket_id: String,
    pub cfg: Config,
    pub stop: Notify,
    pub state: Mutex<AgentState>,
    /// Watch channel broadcasting whether the current cycle has a live
    /// connection. Send waits on this to know when to retry.
    pub ready_tx: watch::Sender<bool>,
    pub ready_rx: watch::Receiver<bool>,
    /// Outgoing send queue — supervisor owns the sink, so sends are
    /// serialized through this channel.
    pub send_tx: mpsc::UnboundedSender<SendRequest>,
    pub send_rx: Mutex<Option<mpsc::UnboundedReceiver<SendRequest>>>,
}

pub(crate) struct AgentState {
    pub last_err: Option<String>,
    pub fatal_err: Option<ServerError>,
    pub closed: bool,
}

pub(crate) struct SendRequest {
    pub to: String,
    pub data: serde_json::Value,
    pub reply: tokio::sync::oneshot::Sender<Result<()>>,
}

impl AgentShared {
    fn new(token: String, socket_id: String, cfg: Config) -> Self {
        let (ready_tx, ready_rx) = watch::channel(false);
        let (send_tx, send_rx) = mpsc::unbounded_channel::<SendRequest>();
        Self {
            token,
            socket_id,
            cfg,
            stop: Notify::new(),
            state: Mutex::new(AgentState {
                last_err: None,
                fatal_err: None,
                closed: false,
            }),
            ready_tx,
            ready_rx,
            send_tx,
            send_rx: Mutex::new(Some(send_rx)),
        }
    }

    pub(crate) async fn send<T: Serialize>(&self, to: &str, payload: &T) -> Result<()> {
        {
            let state = self.state.lock().expect("state lock poisoned");
            if let Some(err) = state.fatal_err.clone() {
                return Err(Error::Server(err));
            }
            if state.closed {
                return Err(Error::Closed);
            }
        }

        let data = serde_json::to_value(payload)?;

        loop {
            // Wait for a live connection before queuing the send.
            let mut rx = self.ready_rx.clone();
            while !*rx.borrow() {
                let state_closed = {
                    let s = self.state.lock().expect("state lock poisoned");
                    s.closed || s.fatal_err.is_some()
                };
                if state_closed {
                    let s = self.state.lock().expect("state lock poisoned");
                    if let Some(err) = s.fatal_err.clone() {
                        return Err(Error::Server(err));
                    }
                    return Err(Error::Closed);
                }
                if rx.changed().await.is_err() {
                    return Err(Error::Closed);
                }
            }

            let (tx, rx_resp) = tokio::sync::oneshot::channel();
            let req = SendRequest {
                to: to.to_string(),
                data: data.clone(),
                reply: tx,
            };
            if self.send_tx.send(req).is_err() {
                return Err(Error::Closed);
            }

            match rx_resp.await {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(Error::Transport(_))) => {
                    // Connection dropped mid-send — loop and wait for reconnect.
                    continue;
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err(Error::Closed),
            }
        }
    }
}

async fn supervisor(shared: Arc<AgentShared>, handler_tx: mpsc::UnboundedSender<Message>) {
    let mut backoff = shared.cfg.min_backoff;
    // Take ownership of the send receiver so we can route sends through
    // the active sink each cycle.
    let mut send_rx = shared
        .send_rx
        .lock()
        .expect("send_rx lock poisoned")
        .take()
        .expect("supervisor already running");

    loop {
        let (connected_at, outcome) = run_cycle(&shared, &mut send_rx, &handler_tx).await;

        match outcome {
            CycleOutcome::Fatal(err) => {
                {
                    let mut state = shared.state.lock().expect("state lock poisoned");
                    state.fatal_err = Some(err.clone());
                    state.closed = true;
                }
                let _ = handler_tx.send(Message::error(Error::Server(err)));
                break;
            }
            CycleOutcome::Error(err) => {
                {
                    let mut state = shared.state.lock().expect("state lock poisoned");
                    state.last_err = Some(err.to_string());
                }
                let _ = handler_tx.send(Message::error(err));
            }
            CycleOutcome::Clean => {}
            CycleOutcome::Stopped => break,
        }

        if let Some(ts) = connected_at {
            if ts.elapsed() > HEALTHY_THRESHOLD {
                backoff = shared.cfg.min_backoff;
            }
        }

        let jitter = rand::thread_rng().gen_range(0..=backoff.as_millis() as u64 / 2);
        let sleep_dur = backoff + Duration::from_millis(jitter);

        tokio::select! {
            _ = sleep(sleep_dur) => {}
            _ = shared.stop.notified() => {
                let mut state = shared.state.lock().expect("state lock poisoned");
                state.closed = true;
                break;
            }
        }

        backoff = std::cmp::min(backoff * 2, shared.cfg.max_backoff);
    }

    // Flip ready to false so any send waiting on it errors out.
    let _ = shared.ready_tx.send(false);
}

enum CycleOutcome {
    /// Connection stayed healthy, then closed cleanly.
    Clean,
    /// Transient error — supervisor should retry.
    Error(Error),
    /// Fatal server error — supervisor should stop.
    Fatal(ServerError),
    /// User called `close()` — supervisor should stop.
    Stopped,
}

type Sink = SplitSink<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, WsMessage>;

async fn run_cycle(
    shared: &Arc<AgentShared>,
    send_rx: &mut mpsc::UnboundedReceiver<SendRequest>,
    handler_tx: &mpsc::UnboundedSender<Message>,
) -> (Option<Instant>, CycleOutcome) {
    let conn = match ws::dial(&shared.cfg.endpoint, &shared.token, &shared.socket_id).await {
        Ok(c) => c,
        Err(Error::Server(err)) if err.is_fatal() => {
            return (None, CycleOutcome::Fatal(err));
        }
        Err(err) => return (None, CycleOutcome::Error(err)),
    };

    let ws::WsConn { mut sink, mut stream } = conn;
    let _ = shared.ready_tx.send(true);
    let connected_at = Instant::now();

    let shared_weak: Weak<AgentShared> = Arc::downgrade(shared);

    let outcome = pump(&mut sink, &mut stream, send_rx, handler_tx, shared, &shared_weak).await;

    let _ = shared.ready_tx.send(false);
    // Best-effort close of the sink.
    let _ = futures_util::SinkExt::close(&mut sink).await;

    (Some(connected_at), outcome)
}

async fn pump(
    sink: &mut Sink,
    stream: &mut futures_util::stream::SplitStream<
        WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    >,
    send_rx: &mut mpsc::UnboundedReceiver<SendRequest>,
    handler_tx: &mpsc::UnboundedSender<Message>,
    shared: &Arc<AgentShared>,
    shared_weak: &Weak<AgentShared>,
) -> CycleOutcome {
    loop {
        tokio::select! {
            biased;

            // Stop signal — user called close().
            _ = shared.stop.notified() => {
                return CycleOutcome::Stopped;
            }

            // Outgoing send.
            Some(req) = send_rx.recv() => {
                let result = ws::send_frame(sink, &req.to, &req.data).await;
                let _ = req.reply.send(result);
            }

            // Incoming frame.
            next = stream.next() => match next {
                Some(Ok(frame)) => {
                    if let Some(event) = ws::decode_frame(frame) {
                        match event {
                            Event::Message { from, data } => {
                                let _ = handler_tx.send(Message::incoming(
                                    from, data, shared_weak.clone(),
                                ));
                            }
                            Event::ServerError(err) => {
                                let _ = handler_tx.send(Message::error(Error::Server(err)));
                            }
                            Event::Decode(err) => {
                                let _ = handler_tx.send(Message::error(err));
                            }
                            Event::Closed => return CycleOutcome::Clean,
                            Event::ConnectionLost(err) => {
                                return CycleOutcome::Error(err);
                            }
                        }
                    }
                }
                Some(Err(err)) => {
                    return CycleOutcome::Error(Error::Transport(err.to_string()));
                }
                None => return CycleOutcome::Clean,
            }
        }
    }
}

async fn dispatch_loop<H: Handler>(
    mut rx: mpsc::UnboundedReceiver<Message>,
    handler: H,
) {
    while let Some(msg) = rx.recv().await {
        handler.call(msg).await;
    }
}
