//! Resumable, bounded, literal search over decoded Core-retained traffic.
//!
//! No FTS table, no projection column and no migration: every request scans the
//! zstd backing rows in small chunks through `Database::run`, so the shared
//! SQLite mutex is never held for the whole budget. Rows that fail zstd/UTF-8
//! decoding are skipped and counted, never fatal.
use super::repository::Traffic;
use serde::Serialize;
use serde_json::{Value, json};
use std::time::Instant;

/// Rows visited per request (hard cap).
const MAX_SCANNED: u64 = 5_000;
/// Wall-clock budget per request.
const MAX_MS: u128 = 750;
/// Decoded-text budget per request.
const MAX_DECODED_BYTES: usize = 32 * 1024 * 1024;
/// Small chunk so one `Database::run` call yields quickly.
const CHUNK_ROWS: usize = 100;
/// Per-row decoded bound; matches the retained 64 KiB row CHECK.
const MAX_ROW_BYTES: usize = 65_536;
const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 50;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchScope {
    Messages,
    Flows,
    Both,
}

impl SearchScope {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "messages" => Some(Self::Messages),
            "flows" => Some(Self::Flows),
            "both" => Some(Self::Both),
            _ => None,
        }
    }
    fn messages(self) -> bool {
        matches!(self, Self::Messages | Self::Both)
    }
    fn flows(self) -> bool {
        matches!(self, Self::Flows | Self::Both)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchKind {
    Message,
    Flow,
}

impl SearchKind {
    fn literal(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::Flow => "flow",
        }
    }
    fn prefix(self) -> char {
        match self {
            Self::Message => 'm',
            Self::Flow => 'f',
        }
    }
}

/// Opaque continuation: the last **scanned** row, hit or not.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SearchCursor {
    pub kind: SearchKind,
    pub seq: i64,
    pub occurred_ms: i64,
}

impl SearchCursor {
    pub fn format(self) -> String {
        format!("{}:{}:{}", self.kind.prefix(), self.seq, self.occurred_ms)
    }
    pub fn parse(raw: &str) -> Option<Self> {
        let mut parts = raw.split(':');
        let kind = match parts.next()? {
            "m" => SearchKind::Message,
            "f" => SearchKind::Flow,
            _ => return None,
        };
        let seq = parts.next()?.parse().ok().filter(|seq| *seq >= 0)?;
        let occurred_ms = parts.next()?.parse().ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some(Self {
            kind,
            seq,
            occurred_ms,
        })
    }
}

#[derive(Clone, Debug)]
pub struct SearchQuery {
    pub q: String,
    pub scope: SearchScope,
    pub service_id: Option<String>,
    pub since_ms: Option<i64>,
    pub until_ms: Option<i64>,
    pub has_flag: Option<bool>,
    pub after: Option<SearchCursor>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct SearchCoverage {
    pub scanned: u64,
    pub truncated: bool,
    pub continuation: bool,
    pub undecodable: u64,
    /// Explicit contract constants for this implementation.
    pub decoded_only: bool,
    pub raw_history_searched: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct SearchPage {
    pub items: Vec<Value>,
    pub next_cursor: Option<String>,
    pub coverage: SearchCoverage,
    pub took_ms: u64,
}

struct Candidate {
    kind: SearchKind,
    seq: i64,
    occurred_ms: i64,
    id: String,
    flow_id: String,
    service_id: String,
    data: Vec<u8>,
    flagged: bool,
}

impl Traffic {
    /// Bounded search over all retained decoded rows. One call is one unit;
    /// `next_cursor` continues strictly after the last scanned row.
    pub async fn search(&self, q: SearchQuery) -> anyhow::Result<SearchPage> {
        validate(&q)?;
        let limit = q.limit.unwrap_or(DEFAULT_LIMIT);
        let matcher = regex::RegexBuilder::new(&regex::escape(&q.q))
            .case_insensitive(true)
            .size_limit(1024 * 1024)
            .dfa_size_limit(1024 * 1024)
            .build()?;
        // At most two global searches; saturation is retryable, never queued.
        let _permit = self
            .searches
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow::anyhow!("search_busy"))?;
        let started = Instant::now();
        // Live-refreshed compiled highlight pattern (settings save hook).
        let pattern = self.highlight();
        let mut scanned = 0u64;
        let mut undecodable = 0u64;
        let mut decoded_bytes = 0usize;
        let mut items = Vec::new();
        let mut cursor = q.after;
        let mut truncated = false;
        let mut exhausted = false;
        'chunks: while items.len() < limit {
            if scanned >= MAX_SCANNED
                || decoded_bytes >= MAX_DECODED_BYTES
                || started.elapsed().as_millis() >= MAX_MS
            {
                truncated = true;
                break;
            }
            let after = cursor;
            let scope = q.scope;
            let service = q.service_id.clone();
            let since = q.since_ms;
            let until = q.until_ms;
            let has_flag = q.has_flag;
            let (sql, values) = chunk_sql(scope, after, service, since, until, has_flag);
            let rows = self
                .auth
                .db
                .run(move |db| {
                    let mut statement = db.prepare(&sql)?;
                    let rows = statement
                        .query_map(rusqlite::params_from_iter(values), |row| {
                            Ok(Candidate {
                                kind: match row.get::<_, String>(0)?.as_str() {
                                    "message" => SearchKind::Message,
                                    _ => SearchKind::Flow,
                                },
                                seq: row.get(1)?,
                                occurred_ms: row.get(2)?,
                                id: row.get(3)?,
                                flow_id: row.get(4)?,
                                service_id: row.get(5)?,
                                data: row.get(6)?,
                                flagged: row.get(7)?,
                            })
                        })?
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(rows)
                })
                .await?;
            let chunk_len = rows.len();
            let short = chunk_len < CHUNK_ROWS;
            let mut processed = 0usize;
            for row in rows {
                // The cursor advances for every row actually visited, hit or not,
                // so a budget break never re-scans or skips a row.
                scanned += 1;
                processed += 1;
                cursor = Some(SearchCursor {
                    kind: row.kind,
                    seq: row.seq,
                    occurred_ms: row.occurred_ms,
                });
                let decoded = match hydra_compression::decode(&row.data, MAX_ROW_BYTES)
                    .ok()
                    .and_then(|bytes| String::from_utf8(bytes).ok())
                {
                    Some(text) => text,
                    None => {
                        undecodable += 1;
                        continue;
                    }
                };
                decoded_bytes = decoded_bytes.saturating_add(decoded.len());
                let Ok(data) = serde_json::from_str::<Value>(&decoded) else {
                    undecodable += 1;
                    continue;
                };
                // Full decoded row text (values and field names), not the
                // bounded display projection.
                let text = super::text::search_text(row.kind.literal(), &data);
                let Some(snippet) = super::text::snippet(&text, &matcher) else {
                    if decoded_bytes >= MAX_DECODED_BYTES || started.elapsed().as_millis() >= MAX_MS
                    {
                        break;
                    }
                    continue;
                };
                let flag_spans = super::text::flag_spans(&snippet.text, pattern.as_ref());
                let message_id = match row.kind {
                    SearchKind::Message => json!(row.id),
                    SearchKind::Flow => Value::Null,
                };
                items.push(json!({
                    "kind": row.kind.literal(),
                    "flow_id": row.flow_id,
                    "message_id": message_id,
                    "service_id": row.service_id,
                    "occurred_ms": row.occurred_ms,
                    "direction": data.get("direction").and_then(Value::as_i64),
                    "protocol_id": data.get("protocol_id").cloned().unwrap_or(Value::Null),
                    "snippet": snippet.text,
                    "snippet_truncated": snippet.truncated,
                    "spans": snippet.spans,
                    "flag_spans": flag_spans,
                    "has_flag": row.flagged,
                }));
                if items.len() >= limit
                    || scanned >= MAX_SCANNED
                    || decoded_bytes >= MAX_DECODED_BYTES
                    || started.elapsed().as_millis() >= MAX_MS
                {
                    break;
                }
            }
            if items.len() >= limit {
                break;
            }
            if processed < chunk_len {
                // A row/time/byte budget stopped this chunk early: rows remain.
                truncated = true;
                break;
            }
            if short {
                // Every retained row was scanned and the page is not full.
                exhausted = true;
                break 'chunks;
            }
            // Yield so auth/ingest/SSE work between bounded chunks.
            tokio::task::yield_now().await;
        }
        let continuation = !exhausted;
        let next_cursor = if exhausted {
            None
        } else {
            cursor.map(SearchCursor::format)
        };
        Ok(SearchPage {
            items,
            next_cursor,
            coverage: SearchCoverage {
                scanned,
                truncated,
                continuation,
                undecodable,
                decoded_only: true,
                raw_history_searched: false,
            },
            took_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
        })
    }
}

fn validate(q: &SearchQuery) -> anyhow::Result<()> {
    anyhow::ensure!(
        (2..=256).contains(&q.q.len()) && !q.q.chars().any(char::is_control),
        "search text must be 2..=256 bytes without control characters"
    );
    anyhow::ensure!(
        q.service_id
            .as_ref()
            .is_none_or(|s| !s.is_empty() && s.len() <= 128),
        "invalid search service"
    );
    anyhow::ensure!(
        q.since_ms.zip(q.until_ms).is_none_or(|(a, b)| a <= b),
        "invalid search time range"
    );
    anyhow::ensure!(
        (1..=MAX_LIMIT).contains(&q.limit.unwrap_or(DEFAULT_LIMIT)),
        "invalid search limit"
    );
    // A continuation cursor is only valid for the scope that produced it;
    // changing scope mid-scan would silently skip or duplicate rows.
    anyhow::ensure!(
        q.after.is_none_or(|after| match q.scope {
            SearchScope::Both => true,
            SearchScope::Messages => after.kind == SearchKind::Message,
            SearchScope::Flows => after.kind == SearchKind::Flow,
        }),
        "invalid search cursor for scope"
    );
    Ok(())
}

fn chunk_sql(
    scope: SearchScope,
    cursor: Option<SearchCursor>,
    service: Option<String>,
    since: Option<i64>,
    until: Option<i64>,
    has_flag: Option<bool>,
) -> (String, Vec<rusqlite::types::Value>) {
    let mut parts = Vec::new();
    let mut values = Vec::new();
    if scope.messages() {
        let (sql, mut branch_values) = branch(
            SearchKind::Message,
            "traffic_messages_zstd",
            "id",
            "t.flow_id",
            cursor,
            service.as_deref(),
            since,
            until,
            has_flag,
        );
        parts.push(sql);
        values.append(&mut branch_values);
    }
    if scope.flows() {
        let (sql, mut branch_values) = branch(
            SearchKind::Flow,
            "traffic_flows_zstd",
            "id",
            "t.id",
            cursor,
            service.as_deref(),
            since,
            until,
            has_flag,
        );
        parts.push(sql);
        values.append(&mut branch_values);
    }
    values.push(rusqlite::types::Value::Integer(CHUNK_ROWS as i64));
    (
        format!(
            "{} ORDER BY occurred_ms DESC,kind ASC,seq DESC LIMIT ?",
            parts.join(" UNION ALL ")
        ),
        values,
    )
}

#[allow(clippy::too_many_arguments)]
fn branch(
    kind: SearchKind,
    table: &str,
    id_column: &str,
    flow_ref: &str,
    cursor: Option<SearchCursor>,
    service: Option<&str>,
    since: Option<i64>,
    until: Option<i64>,
    has_flag: Option<bool>,
) -> (String, Vec<rusqlite::types::Value>) {
    use rusqlite::types::Value as Sql;
    let flagged = format!(
        "(EXISTS(SELECT 1 FROM flag_events fe WHERE fe.flow_id={flow_ref}) OR EXISTS(SELECT 1 FROM traffic_favorites fv WHERE fv.flow_id={flow_ref} AND fv.has_flag=1))"
    );
    let flow_column = if kind == SearchKind::Flow {
        "t.id"
    } else {
        "t.flow_id"
    };
    let mut sql = format!(
        "SELECT '{}' AS kind,t.seq,t.occurred_ms,t.{id_column},{flow_column} AS flow_id,t.service_id,t.data,{flagged} AS flagged FROM {table} t WHERE 1=1",
        kind.literal()
    );
    let mut values = Vec::new();
    if let Some(cursor) = cursor {
        match kind {
            // Same kind: resume strictly after the scanned row.
            _ if cursor.kind == kind => {
                sql.push_str(" AND (t.occurred_ms<? OR (t.occurred_ms=? AND t.seq<?))");
                values.push(Sql::Integer(cursor.occurred_ms));
                values.push(Sql::Integer(cursor.occurred_ms));
                values.push(Sql::Integer(cursor.seq));
            }
            // Messages sort after flows within one millisecond.
            SearchKind::Message => {
                sql.push_str(" AND t.occurred_ms<=?");
                values.push(Sql::Integer(cursor.occurred_ms));
            }
            // Flows sort before messages within one millisecond.
            SearchKind::Flow => {
                sql.push_str(" AND t.occurred_ms<?");
                values.push(Sql::Integer(cursor.occurred_ms));
            }
        }
    }
    if let Some(service) = service {
        sql.push_str(" AND t.service_id=?");
        values.push(Sql::Text(service.to_owned()));
    }
    if let Some(since) = since {
        sql.push_str(" AND t.occurred_ms>=?");
        values.push(Sql::Integer(since));
    }
    if let Some(until) = until {
        sql.push_str(" AND t.occurred_ms<=?");
        values.push(Sql::Integer(until));
    }
    if let Some(has_flag) = has_flag {
        sql.push_str(if has_flag { " AND " } else { " AND NOT " });
        sql.push_str(&flagged);
    }
    (sql, values)
}
