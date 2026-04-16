//! REST API client for Agent Socket.
//!
//! Most users never need this — they create the socket in the
//! dashboard once and only use [`crate::connect`] in code. Use the
//! [`Client`] for programmatic provisioning of sockets, namespaces,
//! and channels.
//!
//! # Example
//! ```no_run
//! use agent_socket::api::Client;
//! use agent_socket::types::CreateSocketRequest;
//!
//! # #[tokio::main(flavor = "current_thread")] async fn main() {
//! let client = Client::new("sk_...");
//! let socket = client.create_socket(&CreateSocketRequest {
//!     name: Some("acme/my-agent".into()),
//!     ..Default::default()
//! }).await.unwrap();
//! let ns = client.create_namespace("acme").await.unwrap();
//! let ch = client.create_channel("acme/alerts").await.unwrap();
//! client.add_member(&ch.id, &socket.id).await.unwrap();
//! # }
//! ```

use reqwest::Method;
use serde::Serialize;

use crate::errors::{ApiError, Error, Result};
use crate::types::{
    Channel, CreateSocketRequest, HealthResponse, Member, Namespace, Socket, SocketProfile,
    SocketStatus, UpdateProfileRequest, VibeResponse,
};

/// Default REST API base URL.
pub const DEFAULT_BASE_URL: &str = "https://api.agent-socket.ai";

/// REST API client.
#[derive(Debug, Clone)]
pub struct Client {
    base_url: String,
    token: String,
    http: reqwest::Client,
}

impl Client {
    /// Construct with the default base URL.
    pub fn new(token: impl Into<String>) -> Self {
        Self::with_base_url(token, DEFAULT_BASE_URL)
    }

    /// Construct with a custom base URL (useful for staging / local).
    pub fn with_base_url(token: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            token: token.into(),
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("build reqwest client"),
        }
    }

    // --------------------------------------------------------------- health

    pub async fn health(&self) -> Result<HealthResponse> {
        self.request(Method::GET, "/health", None::<&()>).await
    }

    // -------------------------------------------------------------- sockets

    pub async fn create_socket(&self, req: &CreateSocketRequest) -> Result<Socket> {
        self.request(Method::POST, "/sockets", Some(req)).await
    }

    pub async fn delete_socket(&self, socket_id: &str) -> Result<()> {
        self.request_empty(Method::DELETE, &format!("/sockets/{}", encode(socket_id)))
            .await
    }

    pub async fn list_sockets(&self) -> Result<Vec<Socket>> {
        self.request(Method::GET, "/sockets", None::<&()>).await
    }

    pub async fn get_socket_status(&self, socket_id: &str) -> Result<SocketStatus> {
        self.request(
            Method::GET,
            &format!("/sockets/{}/status", encode(socket_id)),
            None::<&()>,
        )
        .await
    }

    pub async fn update_profile(
        &self,
        socket_id: &str,
        req: &UpdateProfileRequest,
    ) -> Result<SocketProfile> {
        self.request(
            Method::PATCH,
            &format!("/sockets/{}/profile", encode(socket_id)),
            Some(req),
        )
        .await
    }

    pub async fn update_vibe(&self, socket_id: &str, vibe: &str) -> Result<VibeResponse> {
        #[derive(Serialize)]
        struct Body<'a> {
            vibe: &'a str,
        }
        self.request(
            Method::PATCH,
            &format!("/sockets/{}/vibe", encode(socket_id)),
            Some(&Body { vibe }),
        )
        .await
    }

    // ----------------------------------------------------------- namespaces

    pub async fn create_namespace(&self, name: &str) -> Result<Namespace> {
        #[derive(Serialize)]
        struct Body<'a> {
            name: &'a str,
        }
        self.request(Method::POST, "/namespaces", Some(&Body { name }))
            .await
    }

    pub async fn list_namespaces(&self) -> Result<Vec<Namespace>> {
        self.request(Method::GET, "/namespaces", None::<&()>).await
    }

    // ------------------------------------------------------------- channels

    pub async fn create_channel(&self, name: &str) -> Result<Channel> {
        #[derive(Serialize)]
        struct Body<'a> {
            name: &'a str,
        }
        self.request(Method::POST, "/channels", Some(&Body { name }))
            .await
    }

    pub async fn list_channels(&self) -> Result<Vec<Channel>> {
        self.request(Method::GET, "/channels", None::<&()>).await
    }

    pub async fn add_member(&self, channel_id: &str, socket_id: &str) -> Result<Member> {
        #[derive(Serialize)]
        struct Body<'a> {
            socket_id: &'a str,
        }
        self.request(
            Method::POST,
            &format!("/channels/{}/members", encode(channel_id)),
            Some(&Body { socket_id }),
        )
        .await
    }

    pub async fn remove_member(&self, channel_id: &str, socket_id: &str) -> Result<()> {
        self.request_empty(
            Method::DELETE,
            &format!(
                "/channels/{}/members/{}",
                encode(channel_id),
                encode(socket_id)
            ),
        )
        .await
    }

    pub async fn list_members(&self, channel_id: &str) -> Result<Vec<Member>> {
        self.request(
            Method::GET,
            &format!("/channels/{}/members", encode(channel_id)),
            None::<&()>,
        )
        .await
    }

    // ------------------------------------------------------------ internals

    async fn request<B, T>(&self, method: Method, path: &str, body: Option<&B>) -> Result<T>
    where
        B: Serialize + ?Sized,
        T: for<'de> serde::Deserialize<'de>,
    {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.http.request(method, url).bearer_auth(&self.token);
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| Error::Api(ApiError::new(0, format!("request failed: {}", e))))?;

        if !resp.status().is_success() {
            return Err(Error::Api(api_error_from(resp).await));
        }

        resp.json::<T>()
            .await
            .map_err(|e| Error::Api(ApiError::new(0, format!("decode response: {}", e))))
    }

    async fn request_empty(&self, method: Method, path: &str) -> Result<()> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .http
            .request(method, url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| Error::Api(ApiError::new(0, format!("request failed: {}", e))))?;

        if !resp.status().is_success() {
            return Err(Error::Api(api_error_from(resp).await));
        }
        Ok(())
    }
}

async fn api_error_from(resp: reqwest::Response) -> ApiError {
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    if body.is_empty() {
        return ApiError::new(status, "unknown error");
    }
    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&body) {
        if let Some(msg) = parsed.get("message").and_then(|v| v.as_str()) {
            return ApiError::new(status, msg);
        }
        if let Some(msg) = parsed.get("error").and_then(|v| v.as_str()) {
            return ApiError::new(status, msg);
        }
    }
    ApiError::new(status, body)
}

fn encode(s: &str) -> String {
    // Minimal percent-encoding for path segments. Good enough for the
    // characters used in socket / channel IDs (`:` and `/` must pass
    // through unchanged so `as:ns/name` forms remain intact).
    // reqwest handles most of it, but `%` itself needs escaping.
    s.replace('%', "%25")
}
