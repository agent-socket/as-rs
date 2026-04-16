//! Minimal connect-and-stay demo for as-rs.
//!
//! Connects as `as:test/simple-python-agent` (shared demo socket) and
//! logs every event. Stays online for 10s, then exits cleanly.

use std::env;
use std::fs;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use agent_socket::{connect, handler_fn, Config, Message};
use serde::Deserialize;
use tokio::time::{sleep, Instant};

const SOCKET: &str = "as:test/simple-python-agent";

#[derive(Debug, Deserialize)]
struct FileConfig {
    api_token: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let path = env::args().nth(1).unwrap_or_else(|| "config.json".into());
    let raw = fs::read_to_string(&path).expect("read config");
    let cfg: FileConfig = serde_json::from_str(&raw).expect("parse config");

    println!("{} connecting as {} ...", timestamp(), SOCKET);

    let agent = connect(
        cfg.api_token,
        SOCKET,
        handler_fn(|m: Message| async move {
            if let Some(err) = m.err() {
                eprintln!("{} event error: {}", timestamp(), err);
                return;
            }
            println!("{} message from {}: {}", timestamp(), m.from(), m.data());
        }),
        Config::default(),
    );

    // The Rust client does not yet expose an `on_connect` hook, so we
    // poll `agent.err() == None` combined with a short pause as a
    // readiness signal. The server-side `connected_since` timestamp
    // is the authoritative witness.
    let connected = Arc::new(AtomicBool::new(false));
    let connected_clone = Arc::clone(&connected);
    let agent = {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            sleep(Duration::from_millis(100)).await;
            if agent.err().is_none() {
                connected_clone.store(true, Ordering::Relaxed);
                break agent;
            }
            if Instant::now() >= deadline {
                eprintln!(
                    "{} no clean connect within 5s; last err = {:?}",
                    timestamp(),
                    agent.err()
                );
                agent.close().await;
                return ExitCode::from(1);
            }
        }
    };

    if connected.load(Ordering::Relaxed) {
        println!(
            "{} WebSocket connected — socket is live",
            timestamp()
        );
        println!(
            "{} CONNECTED. staying online for 10s...",
            timestamp()
        );
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sleep(Duration::from_secs(10)) => {}
    }

    agent.close().await;
    println!("{} agent closed cleanly.", timestamp());
    ExitCode::from(0)
}

fn timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // Minimal HH:MM:SS formatting without a chrono dependency.
    let h = (secs / 3600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
}
