//! Bounded retained search executed off the socket loop.
//!
//! The search future runs in its own task with a hard 5 s timeout, so ticks
//! (expiry, revocation, counters, heartbeat) and cancellation stay responsive.
//! Every attempt is audited with the query length only — never its content.
use super::super::{SearchPage, SearchQuery, Traffic};
use super::{
    outbound::{Outbound, error_frame},
    session::Session,
};
use crate::modules::{audit, auth::now};
use serde_json::json;
use std::time::Duration;
use tokio::{sync::oneshot, task::JoinHandle};

pub(super) const SEARCH_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) enum SearchOutcome {
    Completed(anyhow::Result<SearchPage>),
    TimedOut,
}

pub(super) struct SearchJob {
    pub(super) request_id: String,
    pub(super) query_bytes: usize,
    pub(super) rx: oneshot::Receiver<SearchOutcome>,
    pub(super) task: JoinHandle<()>,
}

impl SearchJob {
    pub(super) fn abort(self) {
        self.task.abort();
    }
}

pub(super) fn start(t: &Traffic, request_id: String, query: SearchQuery) -> SearchJob {
    let query_bytes = query.q.len();
    let traffic = t.clone();
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let outcome = match tokio::time::timeout(SEARCH_TIMEOUT, traffic.search(query)).await {
            Ok(result) => SearchOutcome::Completed(result),
            Err(_) => SearchOutcome::TimedOut,
        };
        let _ = tx.send(outcome);
    });
    SearchJob {
        request_id,
        query_bytes,
        rx,
        task,
    }
}

pub(super) async fn finish(
    t: &Traffic,
    session: &Session,
    out: &Outbound,
    job: SearchJob,
    outcome: SearchOutcome,
) {
    let generation = session.generation;
    let request_id = job.request_id;
    match outcome {
        SearchOutcome::TimedOut => {
            audit_search(t, session, job.query_bytes, "timeout", 0, None).await;
            error_frame(out, generation, &request_id, "search_timeout", true);
        }
        SearchOutcome::Completed(Ok(page)) => {
            let audited = audit_search(
                t,
                session,
                job.query_bytes,
                "ok",
                page.coverage.scanned,
                None,
            )
            .await;
            if !audited {
                // The sensitive read happened; do not hand back results whose
                // audit row is missing.
                error_frame(out, generation, &request_id, "audit_unavailable", false);
                return;
            }
            out.emit(
                generation,
                json!({
                    "type": "search_page",
                    "request_id": request_id,
                    "generation": generation,
                    "items": page.items,
                    "next_cursor": page.next_cursor,
                    "coverage": page.coverage,
                    "took_ms": page.took_ms,
                }),
            );
        }
        SearchOutcome::Completed(Err(error)) => {
            let message = error.to_string();
            let code = if message == "search_busy" {
                "search_busy"
            } else if message.contains("search text") || message.contains("invalid search") {
                "invalid_query"
            } else {
                "search_failed"
            };
            audit_search(t, session, job.query_bytes, "error", 0, Some(code)).await;
            error_frame(out, generation, &request_id, code, code != "invalid_query");
        }
    }
}

/// Audit one search attempt/result. Metadata is bounded and content-free.
async fn audit_search(
    t: &Traffic,
    session: &Session,
    query_bytes: usize,
    status: &'static str,
    scanned: u64,
    code: Option<&'static str>,
) -> bool {
    let Some(actor) = session.actor.clone() else {
        return false;
    };
    let metadata = json!({
        "source": "streams_ws",
        "query_bytes": query_bytes,
        "status": status,
        "scanned": scanned,
        "code": code,
    });
    t.auth
        .db
        .run(move |db| {
            let tx = db.transaction()?;
            audit::append_change(
                &tx,
                &actor,
                "traffic.search",
                "traffic",
                &uuid::Uuid::new_v4().to_string(),
                status == "ok",
                now(),
                &metadata,
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .is_ok()
}
