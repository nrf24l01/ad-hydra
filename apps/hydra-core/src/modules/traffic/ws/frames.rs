//! Client frame parsing and the live subscription query.
//!
//! `LiveQuery` carries every `repository::Query` filter so a Streams
//! subscription is exactly as expressive as the HTTP list route, and it is
//! validated by the same function before the snapshot is queried.
use super::super::{Query, repository::validate_query};
use serde::Deserialize;
use serde_json::{Value, json};

pub(super) const PROTOCOL: &str = "hydra.traffic.v1";
/// Inbound text frame cap (server frames may legitimately be larger).
pub(super) const MAX_INBOUND: usize = 32 * 1024;

#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LiveQuery {
    #[serde(default)]
    pub(super) include_blocked: Option<bool>,
    #[serde(default)]
    pub(super) favorite: Option<bool>,
    #[serde(default)]
    pub(super) after: Option<i64>,
    #[serde(default)]
    pub(super) limit: Option<usize>,
    #[serde(default)]
    pub(super) newest_first: Option<bool>,
    #[serde(default)]
    pub(super) service_id: Option<String>,
    #[serde(default)]
    pub(super) client_ip: Option<String>,
    #[serde(default)]
    pub(super) since_ms: Option<i64>,
    #[serde(default)]
    pub(super) until_ms: Option<i64>,
    #[serde(default)]
    pub(super) transport: Option<i32>,
    #[serde(default)]
    pub(super) source_ip: Option<String>,
    #[serde(default)]
    pub(super) destination_ip: Option<String>,
    #[serde(default)]
    pub(super) direction: Option<i32>,
    #[serde(default)]
    pub(super) field_path: Option<String>,
    #[serde(default)]
    pub(super) field_value: Option<String>,
    #[serde(default)]
    pub(super) matched_rule: Option<String>,
    #[serde(default)]
    pub(super) has_flag: Option<bool>,
}

impl LiveQuery {
    pub(super) fn query(&self) -> Query {
        Query {
            include_blocked: self.include_blocked,
            favorite: self.favorite,
            after: self.after,
            limit: Some(self.limit.unwrap_or(100).clamp(1, 100)),
            newest_first: Some(self.newest_first.unwrap_or(true)),
            service_id: self.service_id.clone(),
            client_ip: self.client_ip.clone(),
            since_ms: self.since_ms,
            until_ms: self.until_ms,
            transport: self.transport,
            source_ip: self.source_ip.clone(),
            destination_ip: self.destination_ip.clone(),
            direction: self.direction,
            field_path: self.field_path.clone(),
            field_value: self.field_value.clone(),
            matched_rule: self.matched_rule.clone(),
            has_flag: self.has_flag,
        }
    }

    /// The snapshot and every live event use the same validated query.
    pub(super) fn validated(&self) -> anyhow::Result<Query> {
        let query = self.query();
        validate_query(&query)?;
        Ok(query)
    }

    pub(super) fn to_json(&self) -> Value {
        let query = self.query();
        json!({
            "include_blocked": query.include_blocked,
            "favorite": query.favorite,
            "after": query.after,
            "limit": query.limit,
            "newest_first": query.newest_first,
            "service_id": query.service_id,
            "client_ip": query.client_ip,
            "since_ms": query.since_ms,
            "until_ms": query.until_ms,
            "transport": query.transport,
            "source_ip": query.source_ip,
            "destination_ip": query.destination_ip,
            "direction": query.direction,
            "field_path": query.field_path,
            "field_value": query.field_value,
            "matched_rule": query.matched_rule,
            "has_flag": query.has_flag,
        })
    }
}

#[derive(Deserialize)]
pub(super) struct AuthFrame {
    pub(super) request_id: String,
    pub(super) token: String,
}

#[derive(Deserialize)]
pub(super) struct SubscribeFrame {
    pub(super) request_id: String,
    pub(super) query: LiveQuery,
}

#[derive(Deserialize)]
pub(super) struct PageFrame {
    pub(super) request_id: String,
    #[serde(default)]
    pub(super) after: Option<i64>,
    #[serde(default)]
    pub(super) limit: Option<usize>,
}

#[derive(Deserialize)]
pub(super) struct SelectFrame {
    pub(super) request_id: String,
    #[serde(default)]
    pub(super) flow_id: Option<String>,
    #[serde(default)]
    pub(super) active_bytes: bool,
}

#[derive(Deserialize)]
pub(super) struct MessagesFrame {
    pub(super) request_id: String,
    pub(super) flow_id: String,
    #[serde(default)]
    pub(super) after: Option<i64>,
    #[serde(default)]
    pub(super) limit: Option<usize>,
}

#[derive(Deserialize)]
pub(super) struct SearchFrame {
    pub(super) request_id: String,
    pub(super) q: String,
    #[serde(default)]
    pub(super) scope: Option<String>,
    #[serde(default)]
    pub(super) service_id: Option<String>,
    #[serde(default)]
    pub(super) after: Option<String>,
    #[serde(default)]
    pub(super) limit: Option<usize>,
    #[serde(default)]
    pub(super) since_ms: Option<i64>,
    #[serde(default)]
    pub(super) until_ms: Option<i64>,
    #[serde(default)]
    pub(super) has_flag: Option<bool>,
}

#[derive(Deserialize)]
pub(super) struct ResyncFrame {
    #[serde(default)]
    pub(super) request_id: Option<String>,
    #[serde(default)]
    pub(super) reason: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct ResumeFrame {
    pub(super) request_id: String,
}
