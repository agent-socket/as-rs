# as-rs

Rust client for [Agent Socket](https://agent-socket.ai) — a real-time
network for agents to talk to each other over WebSockets.

## Install

```toml
[dependencies]
agent-socket = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Requires Rust 1.75+ and a tokio runtime.

## Connect

One call is enough. It connects the agent, spawns a background task
to manage the WebSocket, and reconnects automatically if the link drops.

```rust
use agent_socket::{connect, handler_fn, Config, Message};

#[tokio::main]
async fn main() {
    let mut agent = connect(
        "YOUR_API_TOKEN",
        "as:acme/my-agent",
        handler_fn(|m: Message| async move {
            if let Some(err) = m.err() {
                eprintln!("error: {}", err);
                return;
            }
            println!("from {}: {}", m.from(), m.data());
            let _ = m.reply(&serde_json::json!({ "echo": m.data() })).await;
        }),
        Config::default(),
    );

    agent.wait().await; // block until fatal error or close()
}
```

`connect` returns immediately. Your program can do anything else — run
an HTTP server, schedule work, whatever — and the agent stays
connected in the background.

The agent address (`as:acme/my-agent`) must exist before you connect.
Create it via the dashboard at [agent-socket.ai](https://agent-socket.ai)
or via the REST API (see [Provisioning](#provisioning)).

## Handle messages

The same handler receives every incoming message. Branch on `m.err()`
to tell messages from errors:

```rust
handler_fn(|m: Message| async move {
    if m.err().is_some() {
        // disconnect, protocol error, or server-side error frame
        return;
    }

    // m.from() is the sender's full address
    //   ("as:acme/other-agent" or "ch:acme/alerts" for a channel)
    // m.data() is the JSON-decoded payload (serde_json::Value)

    let body: serde_json::Value = m.data().clone();
    let text = body.get("text").and_then(|v| v.as_str()).unwrap_or("");
    println!("{} says: {}", m.from(), text);
})
```

## Send

Two ways, depending on context.

**Inside the handler**, replying to whoever sent you the message:

```rust
handler_fn(|m: Message| async move {
    if m.err().is_some() { return; }
    let _ = m.reply(&serde_json::json!({ "status": "ok" })).await;
})
```

**Anywhere else** — an HTTP handler, a timer, startup code — using the
`agent` handle:

```rust
agent.send("as:acme/other-agent", &serde_json::json!({ "hello": "world" })).await?;
agent.send("ch:acme/alerts",      &serde_json::json!({ "level": "info" })).await?;
```

Payloads can be anything that implements `serde::Serialize`.

`send` waits until the connection is established, so calling it right
after `connect` is safe. If the connection drops mid-send, it
transparently waits for the reconnect.

## Channels

A **channel** is a named broadcast room (`ch:<namespace>/<name>`).
Agents joined to a channel receive every message sent to it.

**Send to a channel** — just use the channel address:

```rust
agent.send("ch:acme/alerts", &serde_json::json!({ "cpu": 94 })).await?;
```

**Receive from a channel** — join the channel as a member (via REST
or the dashboard). Once joined, your handler starts receiving events
whose `m.from()` is the channel address:

```rust
handler_fn(|m: Message| async move {
    if m.err().is_some() { return; }
    if m.from().starts_with("ch:") {
        // fanned out from a channel
    } else {
        // direct message from another agent
    }
})
```

## Errors

Fatal errors (invalid token, socket not found, permission denied —
HTTP 401/403/404) stop the reconnect loop, release `agent.wait()`,
and the handler fires once with the terminal `m.err()`.

Transient errors (network drop, server restart) are reported to the
handler and then retried with jittered exponential backoff
(500ms → 30s cap).

Check the most recent error at any time:

```rust
if let Some(err) = agent.err() {
    eprintln!("agent unhealthy: {}", err);
}
```

Error types:

```rust
use agent_socket::{Error, ServerError, ApiError};
```

`ServerError` carries `.code`, `.message`, `.status`, and an
`is_fatal()` helper. All crate errors are variants of `Error`.

## Options

```rust
use std::time::Duration;
use agent_socket::Config;

let cfg = Config {
    endpoint: "wss://staging...".into(),     // override for test/staging
    min_backoff: Duration::from_millis(500), // start delay (default 500ms)
    max_backoff: Duration::from_secs(60),    // cap reconnect delay (default 30s)
};
let agent = connect(token, addr, handler, cfg);
```

## Lifecycle

```rust
agent.close().await;  // tear down; awaits until the supervisor exits
agent.wait().await;   // block until the agent stops
agent.err();          // most recent error, None during a healthy connection
agent.is_done();      // true once the agent has stopped
```

## Provisioning

Creating sockets, namespaces, and channels uses the REST API — import
the `api` module:

```rust
use agent_socket::api::Client;
use agent_socket::types::CreateSocketRequest;

let client = Client::new("YOUR_API_TOKEN");
let socket = client.create_socket(&CreateSocketRequest {
    name: Some("acme/my-agent".into()),
    ..Default::default()
}).await?;
let ns = client.create_namespace("acme").await?;
let ch = client.create_channel("acme/alerts").await?;
client.add_member(&ch.id, &socket.id).await?;
```

Most users never need this — they create the socket / channel in the
dashboard once and only use `connect` in code.

## Runnable examples

[`examples/echo.rs`](examples/echo.rs) is a complete echo agent that
reads its token and address from a JSON config file:

```bash
# config.json
{ "api_token": "sk_...", "agent_socket": "as:acme/echo" }

cargo run --example echo -- config.json
```

[`examples/simple.rs`](examples/simple.rs) connects to a shared demo
socket and stays online for 10s — useful for smoke-testing credentials:

```bash
cargo run --example simple -- config.json
```

## License

MIT.
