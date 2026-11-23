//! Canonical visible text, snippets and UTF-16 flag spans for Streams frames.
//!
//! Display annotation only: a span never creates, suppresses or reclassifies a
//! finding. `has_flag` stays the flow-level finding predicate.
use regex::{Regex, RegexBuilder};
use serde_json::Value;

/// Upper bound for one annotated visible text, in Unicode scalar values.
pub(crate) const MAX_DISPLAY_CHARS: usize = 16_384;
/// Snippet window bounds, in Unicode scalar values around the first match.
const SNIPPET_BEFORE: usize = 80;
const SNIPPET_AFTER: usize = 160;
/// Hard cap for returned highlight spans.
pub(crate) const MAX_FLAG_SPANS: usize = 64;

/// Compile the configured highlight pattern with bounded regex resources.
/// Empty, oversized or empty-matching patterns are unavailable, never errors.
pub(crate) fn compile_pattern(pattern: Option<&str>) -> Option<Regex> {
    let pattern = pattern?;
    if pattern.is_empty() || pattern.len() > 4096 {
        return None;
    }
    RegexBuilder::new(pattern)
        .size_limit(1024 * 1024)
        .dfa_size_limit(1024 * 1024)
        .build()
        .ok()
        .filter(|regex| !regex.is_match(""))
}

/// UTF-16 code units in `text`; JavaScript `String#slice` offsets are UTF-16.
pub(crate) fn utf16_len(text: &str) -> usize {
    text.encode_utf16().count()
}

/// UTF-16 offset of a byte index inside `text` (byte index must be a boundary).
fn utf16_offset(text: &str, byte: usize) -> usize {
    utf16_len(&text[..byte])
}

/// Canonical visible text for one decoded flow/message row.
///
/// Bounded and deterministic: only UTF-8 string/scalar field values and known
/// flow endpoints are rendered. Binary fields are deliberately omitted.
pub(crate) fn display_text(kind: &str, data: &Value) -> String {
    let mut lines = Vec::new();
    match kind {
        "message" => {
            if let Some(fields) = data.get("fields").and_then(Value::as_object) {
                for value in fields.values() {
                    push_field(value, 0, &mut lines);
                }
            }
            if let Some(protocol) = data.get("protocol_id").and_then(Value::as_str) {
                lines.push(protocol.to_owned());
            }
            if let Some(message_type) = data.get("message_type").and_then(Value::as_str) {
                lines.push(message_type.to_owned());
            }
        }
        _ => {
            for key in ["client", "upstream"] {
                if let Some(endpoint) = data.get(key) {
                    push_endpoint(endpoint, &mut lines);
                }
            }
            if let Some(transport) = data.get("transport").and_then(Value::as_i64) {
                lines.push(if transport == 2 { "udp" } else { "tcp" }.into());
            }
            if let Some(reason) = data
                .get("closed")
                .and_then(|closed| closed.get("reason"))
                .and_then(Value::as_str)
            {
                lines.push(reason.to_owned());
            }
            if let Some(service) = data.get("service_id").and_then(Value::as_str) {
                lines.push(service.to_owned());
            }
        }
    }
    let mut text = String::new();
    let mut chars = 0usize;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if !text.is_empty() {
            text.push('\n');
            chars += 1;
        }
        for ch in line.chars() {
            if chars >= MAX_DISPLAY_CHARS {
                return text;
            }
            text.push(ch);
            chars += 1;
        }
    }
    text
}

fn push_endpoint(value: &Value, lines: &mut Vec<String>) {
    let ip = value.get("ip").and_then(Value::as_str).unwrap_or_default();
    if ip.is_empty() {
        return;
    }
    match value.get("port").and_then(Value::as_u64) {
        Some(port) => lines.push(format!("{ip}:{port}")),
        None => lines.push(ip.to_owned()),
    }
}

fn push_field(value: &Value, depth: usize, lines: &mut Vec<String>) {
    if depth > 8 || lines.len() >= 512 {
        return;
    }
    match value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "string" => {
            if let Some(text) = value.get("value").and_then(Value::as_str) {
                lines.push(text.to_owned());
            }
        }
        "int" | "uint" | "double" | "bool" => {
            if let Some(scalar) = value.get("value") {
                match scalar {
                    Value::String(text) => lines.push(text.clone()),
                    Value::Number(number) => lines.push(number.to_string()),
                    Value::Bool(flag) => lines.push(flag.to_string()),
                    _ => {}
                }
            }
        }
        "object" => {
            if let Some(fields) = value.get("value").and_then(Value::as_object) {
                for nested in fields.values() {
                    push_field(nested, depth + 1, lines);
                }
            }
        }
        "list" => {
            if let Some(items) = value.get("value").and_then(Value::as_array) {
                for nested in items {
                    push_field(nested, depth + 1, lines);
                }
            }
        }
        // Binary fields are not visible text and never matched.
        _ => {}
    }
}

/// Full retained search text for one decoded row: every decoded scalar value
/// and field name, with **literal** (unescaped) string contents so a needle
/// that contains a backslash matches the decoded value, not its JSON escape.
///
/// Deliberately separate from `display_text`: this is the search haystack and
/// is bounded only by the retained 64 KiB row, never by `MAX_DISPLAY_CHARS`.
pub(crate) fn search_text(kind: &str, data: &Value) -> String {
    let mut out = String::new();
    if kind == "message" {
        if let Some(fields) = data.get("fields").and_then(Value::as_object) {
            for (key, value) in fields {
                push_search_line(&mut out, key);
                push_search_field(value, 0, &mut out);
            }
        }
        for key in ["protocol_id", "message_type", "message_id", "sample_id"] {
            if let Some(text) = data.get(key).and_then(Value::as_str) {
                push_search_line(&mut out, text);
            }
        }
    } else {
        push_search_value(data, 0, &mut out);
    }
    out
}

fn push_search_line(out: &mut String, text: &str) {
    if text.is_empty() {
        return;
    }
    if !out.is_empty() {
        out.push('\n');
    }
    out.push_str(text);
}

fn push_search_scalar(out: &mut String, value: &Value) {
    match value {
        Value::String(text) => push_search_line(out, text),
        Value::Number(number) => push_search_line(out, &number.to_string()),
        Value::Bool(flag) => push_search_line(out, &flag.to_string()),
        _ => {}
    }
}

/// Typed message field: names and decoded values only; binary payloads are
/// excluded from the searchable text (they are not text and never decoded).
fn push_search_field(value: &Value, depth: usize, out: &mut String) {
    if depth > 8 {
        return;
    }
    match value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "string" | "int" | "uint" | "double" | "bool" => {
            if let Some(scalar) = value.get("value") {
                push_search_scalar(out, scalar);
            }
        }
        "object" => {
            if let Some(fields) = value.get("value").and_then(Value::as_object) {
                for (key, nested) in fields {
                    push_search_line(out, key);
                    push_search_field(nested, depth + 1, out);
                }
            }
        }
        "list" => {
            if let Some(items) = value.get("value").and_then(Value::as_array) {
                for nested in items {
                    push_search_field(nested, depth + 1, out);
                }
            }
        }
        _ => {}
    }
}

/// Plain decoded JSON (flow rows): every key and scalar value, recursively.
fn push_search_value(value: &Value, depth: usize, out: &mut String) {
    if depth > 8 {
        return;
    }
    match value {
        Value::Object(object) => {
            for (key, nested) in object {
                push_search_line(out, key);
                push_search_value(nested, depth + 1, out);
            }
        }
        Value::Array(items) => {
            for nested in items {
                push_search_value(nested, depth + 1, out);
            }
        }
        other => push_search_scalar(out, other),
    }
}

/// Snippet around the first literal match plus the match spans in the snippet.
pub(crate) struct Snippet {
    pub text: String,
    pub spans: Vec<[usize; 2]>,
    pub truncated: bool,
}

/// Build a bounded display window around the first match of `matcher`.
pub(crate) fn snippet(text: &str, matcher: &Regex) -> Option<Snippet> {
    let found = matcher.find(text)?;
    let mut start = found.start().saturating_sub(SNIPPET_BEFORE);
    while start > 0 && !text.is_char_boundary(start) {
        start -= 1;
    }
    let mut end = (found.end() + SNIPPET_AFTER).min(text.len());
    while end < text.len() && !text.is_char_boundary(end) {
        end += 1;
    }
    let mut display = String::new();
    let mut spans = Vec::new();
    let prefix = start > 0;
    if prefix {
        display.push('…');
    }
    let match_start =
        prefix as usize + utf16_offset(text, found.start()) - utf16_offset(text, start);
    let match_end =
        match_start + utf16_offset(text, found.end()) - utf16_offset(text, found.start());
    display.push_str(&text[start..end]);
    if end < text.len() {
        display.push('…');
    }
    spans.push([match_start, match_end]);
    Some(Snippet {
        text: display,
        spans,
        truncated: start > 0 || end < text.len(),
    })
}

/// Highlight spans for `text`, UTF-16 offsets relative to `text`, capped,
/// sorted, non-overlapping and validated against the UTF-16 length.
pub(crate) fn flag_spans(text: &str, pattern: Option<&Regex>) -> Vec<[usize; 2]> {
    let Some(pattern) = pattern else {
        return Vec::new();
    };
    let limit = utf16_len(text);
    let mut spans = Vec::new();
    for found in pattern.find_iter(text) {
        if found.start() == found.end() {
            continue;
        }
        if spans.len() >= MAX_FLAG_SPANS {
            break;
        }
        let start = utf16_offset(text, found.start());
        let end = utf16_offset(text, found.end());
        if start < end && end <= limit {
            spans.push([start, end]);
        }
    }
    spans
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spans_are_utf16_and_handle_astral_text() {
        let pattern = compile_pattern(Some("flag")).unwrap();
        let text = "😀 flag";
        let spans = flag_spans(text, Some(&pattern));
        assert_eq!(spans, vec![[3, 7]]);
        assert_eq!(utf16_len(text), 7);
    }

    #[test]
    fn invalid_or_empty_matching_pattern_is_unavailable() {
        assert!(compile_pattern(Some("(")).is_none());
        assert!(compile_pattern(Some("")).is_none());
        assert!(compile_pattern(Some("a*")).is_none());
        assert!(compile_pattern(None).is_none());
    }

    #[test]
    fn snippet_offsets_are_relative_to_the_window() {
        let matcher = RegexBuilder::new(&regex::escape("needle"))
            .case_insensitive(true)
            .build()
            .unwrap();
        let text = format!("{}needle{}", "a".repeat(200), "b".repeat(200));
        let snippet = snippet(&text, &matcher).unwrap();
        assert!(snippet.truncated);
        assert!(snippet.text.contains("needle"));
        assert_eq!(snippet.spans.len(), 1);
        assert!(snippet.spans[0][0] < snippet.spans[0][1]);
        assert_eq!(
            snippet.spans[0][1] - snippet.spans[0][0],
            utf16_len("needle")
        );
    }

    #[test]
    fn display_text_omits_binary_fields() {
        let data = serde_json::json!({"fields":{"a":{"type":"string","value":"alpha"},"b":{"type":"bytes","value":[1,2,3]}}});
        assert_eq!(display_text("message", &data), "alpha");
    }
}
