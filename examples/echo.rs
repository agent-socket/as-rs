//! Echo agent — the canonical Agent Socket demo.
//!
//! Reads its token and address from a two-field JSON config file and
//! echoes every message it receives back to the sender.
//!
//!     { "api_token": "sk_...", "agent_socket": "as:acme/echo" }
//!
//!     cargo run --example echo -- config.json

use std::env;
use std::fs;
use std::process::ExitCode;

use agent_socket::{connect, handler_fn, Config, Message};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct FileConfig {
    api_token: String,
    agent_socket: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let path = env::args().nth(1).unwrap_or_else(|| "config.json".into());
    let raw = match fs::read_to_string(&path) {
        Ok(s) => s,
        Err(err) => {
            eprintln!("read {}: {}", path, err);
            return ExitCode::from(1);
        }
    };
    let cfg: FileConfig = match serde_json::from_str(&raw) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("parse {}: {}", path, err);
            return ExitCode::from(1);
        }
    };

    let agent = connect(
        cfg.api_token,
        cfg.agent_socket.clone(),
        handler_fn(|m: Message| async move {
            if let Some(err) = m.err() {
                eprintln!("error: {}", err);
                return;
            }
            println!("from {}: {}", m.from(), m.data());
            if let Err(err) = m
                .reply(&serde_json::json!({ "echo": m.data() }))
                .await
            {
                eprintln!("reply: {}", err);
            }
        }),
        Config::default(),
    );

    println!("listening as {} (ctrl+c to quit)", cfg.agent_socket);

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = {
            let mut agent = agent;
            async move { agent.wait().await }
        } => {}
    }

    ExitCode::from(0)
}
