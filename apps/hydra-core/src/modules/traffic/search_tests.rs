//! Contract tests for bounded, resumable retained-stream search (`Traffic::search`).
//!
//! These tests intentionally use only the frozen search interface from the streams
//! design (SearchQuery/SearchScope/SearchPage/SearchCursor) plus the real ingest and
//! raw SQLite backing rows. They never touch an FTS table or a projection column.
use super::*;
use super::{SearchCursor, SearchPage, SearchQuery, SearchScope};
use crate::{
    infrastructure::{crypto::Crypto, db::Database},
    modules::auth::AuthState,
};
use hydra_wire::v1::{self as proto, event::Payload};
use std::collections::HashMap;

async fn setup_with_pattern(pattern: Option<&str>) -> (Traffic, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!("hydra-search-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&path).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    let db = Database::open(path.join("db")).await.unwrap();
    let crypto = Crypto::load(&path.join("key")).unwrap();
    let traffic = Traffic::open(AuthState::new(db, crypto, "https://hydra.test".into()))
        .await
        .unwrap()
        .with_runtime(path.join("proxy.sock"), pattern.map(str::to_owned));
    (traffic, path)
}

async fn setup() -> (Traffic, std::path::PathBuf) {
    setup_with_pattern(None).await
}

fn flow_opened(
    event_id: &str,
    flow: &str,
    service: &str,
    seconds: i64,
    upstream: &str,
) -> proto::Event {
    proto::Event {
        event_id: event_id.into(),
        flow_id: flow.into(),
        service_id: service.into(),
        occurred_at: {
            let mut t = proto::Event::default().occurred_at.unwrap_or_default();
            t.seconds = seconds;
            Some(t)
        },
        payload: Some(Payload::FlowOpened(proto::FlowOpened {
            transport: 1,
            upstream: Some(proto::Endpoint {
                ip: upstream.into(),
                port: 443,
            }),
            ..Default::default()
        })),
    }
}

fn message(event_id: &str, flow: &str, service: &str, seconds: i64, text: &str) -> proto::Event {
    proto::Event {
        event_id: event_id.into(),
        flow_id: flow.into(),
        service_id: service.into(),
        occurred_at: {
            let mut t = proto::Event::default().occurred_at.unwrap_or_default();
            t.seconds = seconds;
            Some(t)
        },
        payload: Some(Payload::Message(proto::ParsedMessage {
            message_id: event_id.into(),
            protocol_id: "utf8".into(),
            message_type: "request".into(),
            direction: 1,
            fields: Some(proto::FieldObject {
                fields: HashMap::from([(
                    "body".into(),
                    proto::FieldValue {
                        value: Some(proto::field_value::Value::StringValue(text.into())),
                    },
                )]),
            }),
            ..Default::default()
        })),
    }
}

async fn ingest(t: &Traffic, events: Vec<proto::Event>) {
    assert!(events.len() <= 256);
    let accepted = t
        .ingest(proto::EventBatch {
            events,
            ..Default::default()
        })
        .await
        .unwrap()
        .accepted;
    assert!(accepted > 0);
}

fn query(q: &str) -> SearchQuery {
    SearchQuery {
        q: q.into(),
        scope: SearchScope::Both,
        service_id: None,
        since_ms: None,
        until_ms: None,
        has_flag: None,
        after: None,
        limit: Some(50),
    }
}

fn ids(page: &SearchPage) -> Vec<String> {
    page.items
        .iter()
        .filter_map(|v| v["message_id"].as_str().or_else(|| v["flow_id"].as_str()))
        .map(str::to_owned)
        .collect()
}

fn cursor(page: &SearchPage) -> SearchCursor {
    page.next_cursor
        .as_deref()
        .and_then(SearchCursor::parse)
        .expect("opaque cursor parses into SearchCursor")
}

#[tokio::test]
async fn global_search_finds_old_flow_beyond_first_page_and_another_service() {
    let (t, path) = setup().await;
    // 150 flows: the first page is capped at 100 newest, so flow0 is outside it.
    for chunk in 0..3 {
        let mut events = Vec::new();
        for i in chunk * 50..(chunk + 1) * 50 {
            events.push(flow_opened(
                &format!("f{i}-open"),
                &format!("flow{i}"),
                "svc-a",
                100 + i,
                "upstream.example",
            ));
        }
        ingest(&t, events).await;
    }
    ingest(
        &t,
        vec![message(
            "m-old",
            "flow0",
            "svc-other",
            1,
            "OLD_NEEDLE_IN_RETENTION",
        )],
    )
    .await;

    let page = t
        .flows(Query {
            newest_first: Some(true),
            limit: Some(100),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.items.len(), 100);
    assert!(page.items.iter().all(|v| v["flow_id"] != "flow0"));

    let found = t.search(query("OLD_NEEDLE_IN_RETENTION")).await.unwrap();
    assert_eq!(found.items.len(), 1);
    assert_eq!(found.items[0]["flow_id"], "flow0");
    assert_eq!(found.items[0]["service_id"], "svc-other");
    assert!(found.coverage.decoded_only);
    assert!(!found.coverage.raw_history_searched);

    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn full_text_beyond_4096_bytes_is_searched_not_a_prefix() {
    let (t, path) = setup().await;
    let mut text = "A".repeat(5000);
    text.push_str("LATE_FIELD_NEEDLE_9");
    ingest(&t, vec![message("m-late", "flow-late", "svc", 10, &text)]).await;

    let page = t.search(query("LATE_FIELD_NEEDLE_9")).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0]["message_id"], "m-late");

    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn full_text_beyond_the_display_cap_is_still_searched() {
    // Design §5.1: matching sees the full retained decoded row (up to the 65 536-byte
    // CHECK), not a display projection. `text::MAX_DISPLAY_CHARS` is a display bound;
    // if search reuses it, this needle beyond that bound is lost.
    let (t, path) = setup().await;
    let mut text = "A".repeat(20_000);
    text.push_str("BEYOND_DISPLAY_CAP_NEEDLE");
    ingest(&t, vec![message("m-deep", "flow-deep", "svc", 11, &text)]).await;

    let page = t.search(query("BEYOND_DISPLAY_CAP_NEEDLE")).await.unwrap();
    assert_eq!(ids(&page), vec!["m-deep"]);

    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn matching_is_literal_case_insensitive_never_user_regex() {
    let (t, path) = setup().await;
    ingest(
        &t,
        vec![
            message("m-path", "flow-p", "svc", 1, r"C:\x\invalid-regex-escape"),
            message("m-dot", "flow-d", "svc", 2, "axb"),
            message("m-case", "flow-c", "svc", 3, "MixedCaseFLAG{abc}"),
        ],
    )
    .await;

    // A backslash query must be accepted as literal text, not compiled as a regex.
    let backslash = t.search(query(r"C:\x")).await.unwrap();
    assert_eq!(ids(&backslash), vec!["m-path"]);
    // `.` is literal: `a.b` must not match `axb`.
    assert!(t.search(query("a.b")).await.unwrap().items.is_empty());
    // Case-insensitive literal substring.
    let case = t.search(query("mixedcaseflag{abc}")).await.unwrap();
    assert_eq!(ids(&case), vec!["m-case"]);

    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn control_bytes_and_out_of_range_queries_are_rejected() {
    let (t, path) = setup().await;
    for invalid in ["fl\u{0}ag", "flag\u{1}", "a"] {
        assert!(t.search(query(invalid)).await.is_err(), "query {invalid:?}");
    }
    assert!(t.search(query(&"x".repeat(257))).await.is_err());

    let mut over = query("flag");
    over.limit = Some(0);
    assert!(t.search(over).await.is_err());
    let mut over = query("flag");
    over.limit = Some(51);
    assert!(t.search(over).await.is_err());

    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn cap_returns_zero_hit_page_with_cursor_at_last_scanned_row() {
    let (t, path) = setup().await;
    // 5100 rows exceed the 5000-row per-request cap. The only match is the oldest
    // row, so the first bounded unit must finish with zero hits but a cursor.
    for chunk in 0..20 {
        let mut events = Vec::new();
        for i in chunk * 255..(chunk + 1) * 255 {
            let text = if i == 0 {
                "NEEDLE_AT_OLDEST_ROW".to_string()
            } else {
                format!("unrelated row {i}")
            };
            events.push(message(
                &format!("cap-{i}"),
                &format!("cap-flow-{i}"),
                "svc",
                1000 + i as i64,
                &text,
            ));
        }
        ingest(&t, events).await;
    }

    let mut page = t
        .search(SearchQuery {
            limit: Some(1),
            ..query("NEEDLE_AT_OLDEST_ROW")
        })
        .await
        .unwrap();
    assert!(
        page.items.is_empty(),
        "first unit must stop before the oldest row"
    );
    assert!(page.coverage.truncated, "row cap must be reported");
    assert!(
        page.coverage.continuation,
        "continuation must be advertised"
    );
    assert!(
        page.next_cursor.is_some(),
        "cursor is the last scanned row, not the last hit"
    );

    // Follow the cursor a bounded number of times until the match is reached.
    let mut found = false;
    for _ in 0..6 {
        let after = cursor(&page);
        page = t
            .search(SearchQuery {
                limit: Some(1),
                after: Some(after),
                ..query("NEEDLE_AT_OLDEST_ROW")
            })
            .await
            .unwrap();
        if !page.items.is_empty() {
            assert_eq!(page.items[0]["message_id"], "cap-0");
            found = true;
            break;
        }
        if page.next_cursor.is_none() {
            break;
        }
    }
    assert!(found, "cursor continuation must reach the retained match");

    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn cursor_pagination_is_stable_when_newer_rows_arrive() {
    let (t, path) = setup().await;
    for i in 1..=3 {
        ingest(
            &t,
            vec![message(
                &format!("page-{i}"),
                &format!("flow-{i}"),
                "svc",
                10 * i,
                &format!("PAG_NEEDLE_{i}"),
            )],
        )
        .await;
    }
    let first = t
        .search(SearchQuery {
            limit: Some(2),
            ..query("PAG_NEEDLE_")
        })
        .await
        .unwrap();
    assert_eq!(ids(&first), vec!["page-3", "page-2"]);

    // A newer row must not duplicate or skip the older page.
    ingest(
        &t,
        vec![message("page-4", "flow-4", "svc", 40, "PAG_NEEDLE_4")],
    )
    .await;
    let second = t
        .search(SearchQuery {
            limit: Some(2),
            after: Some(cursor(&first)),
            ..query("PAG_NEEDLE_")
        })
        .await
        .unwrap();
    assert_eq!(ids(&second), vec!["page-1"]);

    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn scope_selects_messages_flows_or_both() {
    let (t, path) = setup().await;
    ingest(
        &t,
        vec![
            flow_opened("f-open", "flow-s", "svc", 1, "SCOPEFLOWNEEDLE.example"),
            message("m-msg", "flow-s", "svc", 2, "SCOPEMSGNEEDLE"),
        ],
    )
    .await;

    let messages = t
        .search(SearchQuery {
            scope: SearchScope::Messages,
            ..query("SCOPENEEDLE")
        })
        .await;
    // Neither row contains the combined token; this guards against accidental matching.
    assert!(messages.unwrap().items.is_empty());

    let flows = t
        .search(SearchQuery {
            scope: SearchScope::Flows,
            ..query("SCOPEFLOWNEEDLE")
        })
        .await
        .unwrap();
    assert_eq!(flows.items.len(), 1);
    assert_eq!(flows.items[0]["kind"], "flow");
    assert_eq!(flows.items[0]["flow_id"], "flow-s");

    let only_messages = t
        .search(SearchQuery {
            scope: SearchScope::Messages,
            ..query("SCOPEFLOWNEEDLE")
        })
        .await
        .unwrap();
    assert!(only_messages.items.is_empty());

    let both = t
        .search(SearchQuery {
            scope: SearchScope::Both,
            ..query("SCOPEFLOWNEEDLE")
        })
        .await
        .unwrap();
    assert_eq!(both.items.len(), 1);

    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn undecodable_backing_row_is_counted_and_does_not_abort_the_query() {
    let (t, path) = setup().await;
    ingest(
        &t,
        vec![message(
            "m-good",
            "flow-good",
            "svc",
            5,
            "STILL_FINDABLE_NEEDLE",
        )],
    )
    .await;
    // A compressed row that cannot be decoded must be skipped, never fail the request.
    // The payload is produced by the real compression API so it is a valid, bounded
    // zstd frame; only its decoded bytes are invalid UTF-8, which the per-row reader
    // rejects. The backing table's expression index evaluates `hydra_text(data)` on
    // write, so it is dropped in this throwaway test DB to persist the corrupt row
    // without going through the validating view write path.
    let corrupt =
        hydra_compression::encode(&[0xff, 0xfe, 0xfd, 0xff, 0xfe]).expect("encode corrupt payload");
    t.auth
        .db
        .run(move |db| {
            db.execute_batch("DROP INDEX traffic_message_direction")?;
            db.execute(
                "INSERT INTO traffic_messages_zstd(id,flow_id,service_id,occurred_ms,data) VALUES(?1,?2,?3,?4,?5)",
                rusqlite::params!["m-bad", "flow-bad", "svc", 6i64, corrupt],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let page = t.search(query("STILL_FINDABLE_NEEDLE")).await.unwrap();
    assert_eq!(ids(&page), vec!["m-good"]);
    assert!(
        page.coverage.undecodable >= 1,
        "undecodable rows are counted, not fatal"
    );

    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn retention_deleted_rows_are_no_longer_searchable() {
    let (t, path) = setup().await;
    ingest(
        &t,
        vec![message(
            "m-retained",
            "flow-r",
            "svc",
            7,
            "RETENTION_NEEDLE",
        )],
    )
    .await;
    assert_eq!(
        t.search(query("RETENTION_NEEDLE"))
            .await
            .unwrap()
            .items
            .len(),
        1
    );

    t.auth
        .db
        .run(move |db| {
            db.execute(
                "DELETE FROM traffic_messages_zstd WHERE id=?1",
                ["m-retained"],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    assert!(
        t.search(query("RETENTION_NEEDLE"))
            .await
            .unwrap()
            .items
            .is_empty()
    );

    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn flag_spans_are_utf16_over_the_snippet_and_never_change_finding_truth() {
    let pattern = r"FLAG\{[a-z]+\}";
    let (t, path) = setup_with_pattern(Some(pattern)).await;
    assert_eq!(t.flag_pattern().as_deref(), Some(pattern));
    // An astral character before the flag makes UTF-16 offsets differ from bytes.
    ingest(
        &t,
        vec![message("m-emoji", "flow-e", "svc", 8, "😀FLAG{abc}<b>&")],
    )
    .await;
    // Canonical finding for a flow whose text does not match any highlight pattern.
    ingest(
        &t,
        vec![message(
            "m-flagged",
            "flow-flagged",
            "svc",
            9,
            "plain text with no pattern",
        )],
    )
    .await;
    t.auth
        .db
        .run(move |db| {
            // Migration 0018 (nullable flag_id) is applied by Flags::open, which this
            // Traffic-only harness does not run, so reference a real flags row instead
            // of a defensive NULL.
            db.execute(
                "INSERT INTO flags(id,digest,category,value,initiated_by,created_at) VALUES(?1,?2,'captured',?3,'test',0)",
                rusqlite::params!["flag-1", "digest-flag-1", "FLAG{abc}"],
            )?;
            db.execute(
                "INSERT INTO flag_events(event_id,flag_id,flow_id,service_id) VALUES(?1,?2,?3,?4)",
                rusqlite::params!["flag-evt-1", "flag-1", "flow-flagged", "svc"],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let page = t.search(query("FLAG{abc}")).await.unwrap();
    let item = page
        .items
        .iter()
        .find(|v| v["message_id"] == "m-emoji")
        .expect("emoji row found");
    let snippet = item["snippet"].as_str().expect("snippet is text");
    assert!(
        snippet.contains("FLAG{abc}"),
        "snippet is raw text, got {snippet:?}"
    );
    assert!(
        !snippet.contains("<mark"),
        "server never returns rendered HTML"
    );
    assert!(
        !snippet.contains("&lt;"),
        "server does not HTML-escape the snippet"
    );

    let utf16_len = snippet.encode_utf16().count();
    let spans = item["flag_spans"].as_array().cloned().unwrap_or_default();
    assert!(
        !spans.is_empty(),
        "configured pattern must highlight the flag"
    );
    assert!(spans.len() <= 64);
    let mut last_end = 0;
    for span in &spans {
        let start = span[0].as_u64().unwrap() as usize;
        let end = span[1].as_u64().unwrap() as usize;
        assert!(
            start < end && end <= utf16_len,
            "span within UTF-16 snippet"
        );
        assert!(start >= last_end, "spans sorted and non-overlapping");
        last_end = end;
        // Both offsets must be valid UTF-16 boundaries, never splitting a surrogate pair.
        let units: Vec<u16> = snippet.encode_utf16().collect();
        assert!(
            char::decode_utf16(units[..start].iter().copied()).all(|c| c.is_ok()),
            "span start is a UTF-16 boundary"
        );
        assert!(
            char::decode_utf16(units[..end].iter().copied()).all(|c| c.is_ok()),
            "span end is a UTF-16 boundary"
        );
    }
    let units: Vec<u16> = snippet.encode_utf16().collect();
    let start = spans[0][0].as_u64().unwrap() as usize;
    let end = spans[0][1].as_u64().unwrap() as usize;
    let hit = String::from_utf16(&units[start..end]).unwrap();
    assert_eq!(hit, "FLAG{abc}");

    // A highlight match never creates a finding.
    assert!(!item["has_flag"].as_bool().unwrap());
    // The canonical finding survives even with no highlight match.
    let flagged = t
        .search(SearchQuery {
            has_flag: Some(true),
            ..query("plain text")
        })
        .await
        .unwrap();
    assert_eq!(ids(&flagged), vec!["m-flagged"]);
    assert!(flagged.items[0]["has_flag"].as_bool().unwrap());

    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn configured_highlight_pattern_is_not_the_default_flag_shape() {
    // A deliberately non-default pattern: the default `flag{...}` text must not be
    // highlighted, and the configured custom token must be.
    let pattern = r"CUSTOM_[0-9]+";
    let (t, path) = setup_with_pattern(Some(pattern)).await;
    assert_eq!(t.flag_pattern().as_deref(), Some(pattern));
    ingest(
        &t,
        vec![
            message("m-custom", "flow-c", "svc", 1, "prefix CUSTOM_42 suffix"),
            message(
                "m-default",
                "flow-d",
                "svc",
                2,
                "looks like flag{decoy} but is not",
            ),
        ],
    )
    .await;

    let custom = t.search(query("CUSTOM_42")).await.unwrap();
    assert_eq!(ids(&custom), vec!["m-custom"]);
    let spans = custom.items[0]["flag_spans"].as_array().cloned().unwrap();
    let snippet = custom.items[0]["snippet"].as_str().unwrap();
    let units: Vec<u16> = snippet.encode_utf16().collect();
    let hit = String::from_utf16(
        &units[spans[0][0].as_u64().unwrap() as usize..spans[0][1].as_u64().unwrap() as usize],
    )
    .unwrap();
    assert_eq!(hit, "CUSTOM_42");

    let default_shape = t.search(query("flag{decoy}")).await.unwrap();
    assert_eq!(ids(&default_shape), vec!["m-default"]);
    assert!(
        default_shape.items[0]["flag_spans"]
            .as_array()
            .unwrap()
            .is_empty(),
        "default flag shape is not the configured highlight pattern"
    );

    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn search_uses_no_fts_table_or_projection() {
    let (t, path) = setup().await;
    let fts: i64 = t
        .auth
        .db
        .run(|db| {
            Ok(db.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND (name LIKE '%fts%' OR name LIKE '%search%')",
                [],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(fts, 0, "search must scan zstd backing rows directly");

    let page = t.search(query("anything")).await.unwrap();
    assert!(page.coverage.decoded_only);
    assert!(!page.coverage.raw_history_searched);

    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}
