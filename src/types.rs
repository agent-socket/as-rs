//! Resource types for the Agent Socket REST API.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct Socket {
    pub id: String,
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub agent_description: Option<String>,
    #[serde(default)]
    pub vibe: Option<String>,
    #[serde(default)]
    pub connected_since: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SocketStatus {
    pub id: String,
    pub status: String,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub agent_description: Option<String>,
    #[serde(default)]
    pub vibe: Option<String>,
    #[serde(default)]
    pub connected_since: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SocketProfile {
    pub id: String,
    #[serde(default)]
    pub agent_name: String,
    #[serde(default)]
    pub agent_description: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VibeResponse {
    pub id: String,
    pub vibe: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Namespace {
    pub name: String,
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Channel {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub member_count: u64,
    #[serde(default)]
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Member {
    pub channel_id: String,
    pub socket_id: String,
    #[serde(default)]
    pub added_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HealthResponse {
    pub status: String,
}

// ---------------------------------------------------------------- requests

#[derive(Debug, Clone, Default, Serialize)]
pub struct CreateSocketRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vibe: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct UpdateProfileRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_description: Option<String>,
}
