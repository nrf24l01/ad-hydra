//! Bounded, explicit diagnostics against configured service upstreams only.
use super::{Rpc, control::check_provenance};
use hydra_domain::Transport;
use hydra_wire::v1::{self, proxy_diagnostics_server::ProxyDiagnostics};
use std::collections::BTreeMap;
use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, UdpSocket},
};
use tonic::{Request, Response, Status};

#[allow(clippy::result_large_err)]
fn preview_fields(
    target: &v1::FilterPreviewTarget,
    direction: &str,
) -> Result<BTreeMap<String, hydra_domain::FieldValue>, Status> {
    let mut fields = BTreeMap::new();
    if let Some(input) = &target.network_fields {
        if input.fields.len() > 8 {
            return Err(Status::invalid_argument("preview context field limit"));
        }
        for (key, value) in &input.fields {
            if !matches!(
                key.as_str(),
                "network.src_ip"
                    | "network.src_port"
                    | "network.dst_ip"
                    | "network.dst_port"
                    | "transport.kind"
            ) {
                return Err(Status::invalid_argument(
                    "unsupported preview context field",
                ));
            }
            fields.insert(
                key.clone(),
                hydra_wire::rules::value_from_wire(value, 0)
                    .map_err(|_| Status::invalid_argument("invalid preview context field"))?,
            );
        }
    }
    fields.insert(
        "direction".into(),
        hydra_domain::FieldValue::String(direction.into()),
    );
    Ok(fields)
}

#[allow(clippy::result_large_err)]
async fn preview_history(
    rpc: &Rpc,
    service: &hydra_domain::Service,
    target: &v1::FilterPreviewTarget,
    compiled: &hydra_filter_engine::CompiledSnapshot,
) -> Result<v1::FilterPreviewResult, Status> {
    let mut result = v1::FilterPreviewResult {
        flow_id: target.flow_id.clone(),
        state: "not_retained".into(),
        ..Default::default()
    };
    let store = rpc
        .state
        .telemetry
        .history
        .read()
        .map_err(|_| Status::internal("history store unavailable"))?
        .clone()
        .ok_or_else(|| Status::unavailable("history is disabled"))?;
    let history = match store.get(&target.flow_id).await {
        Ok(Some(history)) => history,
        Ok(None) => return Ok(result),
        Err(crate::history::HistoryReadError::Active) => {
            result.state = "active".into();
            return Ok(result);
        }
        Err(error) => return Err(history_status(error)),
    };
    if history.service_id != service.id {
        return Err(Status::invalid_argument(
            "flow does not belong to selected service",
        ));
    }
    let mut decoder = rpc
        .protocols
        .session(&service.id, &service.protocol_id)
        .map_err(|_| Status::failed_precondition("decoder unavailable"))?;
    let mut sizes = crate::inspection::body_size::BodySizes::default();
    sizes.set_http(decoder.http_mut().is_some());
    if (history.transport == "udp") != (service.transport == Transport::Udp) {
        result.state = "decoder_error".into();
        return Ok(result);
    }
    let udp = history.transport == "udp";
    let mut frames = Vec::new();
    if udp || !history.datagrams.is_empty() {
        frames.extend(history.datagrams.iter().map(|d| {
            (
                if d.direction == 1 {
                    hydra_protocol_sdk::Direction::ClientToServer
                } else {
                    hydra_protocol_sdk::Direction::ServerToClient
                },
                d.data.as_slice(),
            )
        }));
    } else {
        for (direction, bytes) in [
            (
                hydra_protocol_sdk::Direction::ClientToServer,
                &history.client_to_server,
            ),
            (
                hydra_protocol_sdk::Direction::ServerToClient,
                &history.server_to_client,
            ),
        ] {
            frames.extend(bytes.chunks(16_384).map(|chunk| (direction, chunk)));
        }
    }
    for (direction, chunk) in frames {
        let matched_before = result.matched_messages;
        let mut size_blocked = false;
        let direction_name = if direction == hydra_protocol_sdk::Direction::ClientToServer {
            "client_to_server"
        } else {
            "server_to_client"
        };
        let mut base_fields = preview_fields(target, direction_name)?;
        let mut check_size =
            |size_fields: &std::collections::BTreeMap<String, hydra_domain::FieldValue>| {
                let mut all = base_fields.clone();
                all.extend(size_fields.clone());
                let eval = compiled.evaluate_stream_sizes(&hydra_domain::EvaluationContext {
                    service_id: &service.id,
                    transport: service.transport,
                    fields: &all,
                    now_ms: crate::state::now().saturating_mul(1000),
                    safe_http: service.transport == Transport::Tcp
                        && service.protocol_id == "http"
                        && direction == hydra_protocol_sdk::Direction::ClientToServer,
                });
                size_blocked |= matches!(eval.action, Some(hydra_domain::Action::DropDatagram));
                if !eval.hits.is_empty() {
                    result.matched = true;
                    result.matched_messages = result.matched_messages.saturating_add(1);
                    result.would_block |= eval.hits.iter().any(|hit| hit.blocked);
                }
                false
            };
        if udp {
            check_size(&sizes.datagram(direction, chunk.len()));
        } else {
            sizes.feed(direction, chunk, check_size);
        }
        if udp && size_blocked {
            // Live enforcement drops before advancing the stateful decoder.
            continue;
        }
        let decoded = if udp && !decoder.is_stream_decoder() {
            // Live UDP does not apply the TCP HTTP parser to datagrams.
            Ok(Vec::new())
        } else if udp {
            decoder.feed_datagram(direction, chunk)
        } else {
            decoder.feed(direction, chunk)
        };
        let messages = match decoded {
            Ok(messages) => messages,
            Err(_) => {
                result.state = "decoder_error".into();
                return Ok(result);
            }
        };
        let messages = if udp && messages.is_empty() {
            vec![hydra_protocol_sdk::ParsedMessage {
                protocol: "raw".into(),
                message_type: "datagram".into(),
                direction,
                fields: Default::default(),
            }]
        } else {
            messages
        };
        for mut message in messages {
            sizes.annotate(
                &mut message,
                if service.transport == Transport::Udp {
                    "datagram"
                } else {
                    "stream"
                },
            );
            let direction_name =
                if message.direction == hydra_protocol_sdk::Direction::ClientToServer {
                    "client_to_server"
                } else {
                    "server_to_client"
                };
            base_fields = preview_fields(target, direction_name)?;
            base_fields.insert(
                "packet.size".into(),
                hydra_domain::FieldValue::Uint(chunk.len() as u64),
            );
            base_fields.extend(
                message
                    .fields
                    .iter()
                    .map(|(key, value)| (key.clone(), crate::inspection::domain_value(value))),
            );
            let safe_http = service.transport == Transport::Tcp
                && service.protocol_id == "http"
                && message.direction == hydra_protocol_sdk::Direction::ClientToServer;
            let eval = compiled.evaluate(&hydra_domain::EvaluationContext {
                service_id: &service.id,
                transport: service.transport,
                fields: &base_fields,
                now_ms: crate::state::now().saturating_mul(1000),
                safe_http,
            });
            if !eval.hits.is_empty() {
                result.matched = true;
                result.matched_messages = result.matched_messages.saturating_add(1);
                result.would_block |= eval.hits.iter().any(|hit| hit.blocked);
            }
        }
        if udp && result.matched_messages > matched_before {
            // One datagram can hit the byte stage and the decoder stage.
            result.matched_messages = matched_before.saturating_add(1);
        }
    }
    result.state = if history.history_truncated {
        "partial"
    } else {
        "ready"
    }
    .into();
    Ok(result)
}

fn history_status(error: crate::history::HistoryReadError) -> Status {
    match error {
        crate::history::HistoryReadError::Active => {
            Status::failed_precondition("flow history is not complete")
        }
        crate::history::HistoryReadError::Storage(_) => {
            Status::internal("flow history is unavailable")
        }
    }
}

#[cfg(test)]
mod history_tests {
    use super::*;

    #[tokio::test]
    async fn udp_history_preview_preserves_empty_datagrams_sizes_and_raw_boundaries() {
        use hydra_domain::{
            Action, Comparison, Expression, FieldValue, Mode, Rule, RuleSnapshot, Service,
            ServiceSnapshot,
        };
        use hydra_protocol_sdk::Direction;
        use std::sync::Arc;
        let dir = std::env::temp_dir().join(format!("hydra-udp-preview-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let telemetry = crate::telemetry::Telemetry::new(4);
        let history = crate::history::HistoryStore::open(&dir).await.unwrap();
        *telemetry.history.write().unwrap() = Some(history.clone());
        let shutdown = tokio_util::sync::CancellationToken::new();
        let manager = crate::transport::Manager::new(
            crate::config::Limits::default(),
            telemetry.clone(),
            shutdown.clone(),
        );
        let state = crate::state::State::start(
            dir.join("state"),
            ServiceSnapshot::default(),
            manager,
            telemetry,
        )
        .await
        .unwrap();
        let rpc = Rpc {
            state,
            shutdown,
            diagnostics: Arc::new(tokio::sync::Semaphore::new(1)),
            protocols: crate::inspection::protocols::Protocols::open(dir.join("plugins"))
                .await
                .unwrap(),
        };
        let service = Service {
            forwarding: None,
            id: "udp".into(),
            transport: Transport::Udp,
            listen_address: "127.0.0.1:23001".parse().unwrap(),
            upstream_address: "127.0.0.1:23002".parse().unwrap(),
            protocol_id: "raw".into(),
            enabled: true,
            retain_history: true,
            protection: None,
            tls: None,
        };
        history.apply_services(std::slice::from_ref(&service));
        let flow_id = uuid::Uuid::new_v4().to_string();
        let mut writer = history
            .begin_udp(flow_id.clone(), service.id.clone(), "raw".into())
            .unwrap();
        writer.record(Direction::ClientToServer, b"A");
        writer.record(Direction::ServerToClient, b"");
        writer.record(Direction::ClientToServer, b"B");
        writer.finish(i64::MAX, 2, 0);
        let target = v1::FilterPreviewTarget {
            flow_id,
            network_fields: None,
        };
        for (field, op, value, expected) in [
            (
                "raw.bytes",
                Comparison::Contains,
                FieldValue::Bytes(b"AB".to_vec()),
                false,
            ),
            (
                "raw.bytes",
                Comparison::Contains,
                FieldValue::Bytes(b"B".to_vec()),
                true,
            ),
            ("packet.size", Comparison::Eq, FieldValue::Uint(0), true),
            ("packet.size", Comparison::Eq, FieldValue::Uint(2), false),
            ("body.size", Comparison::Eq, FieldValue::Uint(1), true),
            ("body.size", Comparison::Eq, FieldValue::Uint(2), false),
            ("transfer.bytes", Comparison::Eq, FieldValue::Uint(2), true),
        ] {
            let compiled = hydra_filter_engine::CompiledSnapshot::compile(
                RuleSnapshot {
                    revision: 1,
                    rules: vec![Rule {
                        id: "__preview__".into(),
                        service_id: service.id.clone(),
                        additional_service_ids: vec![],
                        name: "test".into(),
                        description: String::new(),
                        enabled: true,
                        mode: Mode::Enforce,
                        priority: 1,
                        expression: Expression::Predicate {
                            field: field.into(),
                            op,
                            value,
                        },
                        action: Action::DropDatagram,
                        backend: "userspace".into(),
                        expires_at_ms: None,
                    }],
                },
                std::slice::from_ref(&service),
            )
            .unwrap();
            let preview = preview_history(&rpc, &service, &target, &compiled)
                .await
                .unwrap();
            assert_eq!(preview.state, "ready", "{field}");
            assert_eq!(preview.matched, expected, "{field}");
            assert_eq!(preview.would_block, expected, "{field}");
            assert_eq!(
                preview.matched_messages,
                if !expected {
                    0
                } else if field == "body.size" {
                    2
                } else {
                    1
                },
                "{field}"
            );
        }
        drop(rpc);
        drop(history);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn history_store_errors_map_to_bounded_rpc_statuses() {
        assert_eq!(
            history_status(crate::history::HistoryReadError::Active).code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            history_status(crate::history::HistoryReadError::Storage(anyhow::anyhow!(
                "corrupt"
            )))
            .code(),
            tonic::Code::Internal
        );
    }
}

#[tonic::async_trait]
impl ProxyDiagnostics for Rpc {
    async fn set_flow_favorite(
        &self,
        request: Request<v1::SetFlowFavoriteRequest>,
    ) -> Result<Response<v1::SetFlowFavoriteReply>, Status> {
        let input = request.into_inner();
        check_provenance(input.actor.as_ref(), &input.request_id)?;
        if uuid::Uuid::parse_str(&input.flow_id).is_err() {
            return Err(Status::invalid_argument("invalid flow id"));
        }
        let store = self
            .state
            .telemetry
            .history
            .read()
            .map_err(|_| Status::internal("history unavailable"))?
            .clone()
            .ok_or_else(|| Status::unavailable("history disabled"))?;
        let state = store
            .set_favorite(&input.flow_id, input.favorite)
            .await
            .map_err(|e| {
                if e.to_string().contains("capacity") {
                    Status::resource_exhausted("favorite archive capacity exhausted")
                } else {
                    Status::unavailable("favorite storage failed")
                }
            })?;
        Ok(Response::new(v1::SetFlowFavoriteReply { state }))
    }

    async fn preview_filter(
        &self,
        request: Request<v1::PreviewFilterRequest>,
    ) -> Result<Response<v1::PreviewFilterReply>, Status> {
        let mut input = request.into_inner();
        check_provenance(input.actor.as_ref(), &input.request_id)?;
        if input.targets.is_empty() || input.targets.len() > 50 {
            return Err(Status::invalid_argument("preview stream limit"));
        }
        let _permit = self
            .diagnostics
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("diagnostics busy"))?;
        let rule = input
            .rule
            .take()
            .ok_or_else(|| Status::invalid_argument("missing candidate filter"))?;
        if rule.id != "__preview__" {
            return Err(Status::invalid_argument("invalid preview filter identity"));
        }
        let service = self
            .state
            .get_service(&rule.service_id)
            .await
            .ok_or_else(|| Status::not_found("service unavailable"))?;
        let snapshot_wire = v1::ApplyRuleSnapshotRequest {
            revision: 1,
            rules: vec![rule],
            ..Default::default()
        };
        let snapshot = hydra_domain::RuleSnapshot::try_from(&snapshot_wire)
            .map_err(|_| Status::invalid_argument("invalid candidate filter"))?;
        let compiled = hydra_filter_engine::CompiledSnapshot::compile(
            snapshot,
            std::slice::from_ref(&service),
        )
        .map_err(|_| Status::invalid_argument("candidate filter is invalid for this service"))?;
        let mut results = Vec::with_capacity(input.targets.len());
        for target in &input.targets {
            if target.flow_id.len() > 128 || uuid::Uuid::parse_str(&target.flow_id).is_err() {
                return Err(Status::invalid_argument("invalid stream identifier"));
            }
            results.push(preview_history(self, &service, target, &compiled).await?);
        }
        Ok(Response::new(v1::PreviewFilterReply { results }))
    }
    async fn test_protocol(
        &self,
        request: Request<v1::TestProtocolRequest>,
    ) -> Result<Response<v1::TestProtocolReply>, Status> {
        let r = request.into_inner();
        check_provenance(r.actor.as_ref(), &r.request_id)?;
        if r.protocol_id.len() > 64 || r.version.len() > 64 || r.sample_id.len() > 128 {
            return Err(Status::invalid_argument("invalid test identifiers"));
        }
        let _permit = self
            .diagnostics
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("diagnostics busy"))?;
        let direction = match r.direction {
            1 => hydra_protocol_sdk::Direction::ClientToServer,
            2 => hydra_protocol_sdk::Direction::ServerToClient,
            _ => return Err(Status::invalid_argument("invalid direction")),
        };
        let ring = self
            .state
            .telemetry
            .samples
            .read()
            .map_err(|_| Status::internal("samples unavailable"))?
            .clone()
            .ok_or_else(|| Status::unavailable("samples disabled"))?;
        let sample = ring
            .get(&r.sample_id)
            .await
            .map_err(|_| Status::internal("sample read failed"))?
            .ok_or_else(|| Status::not_found("sample expired or unavailable"))?;
        let messages = self
            .protocols
            .test(&r.protocol_id, &r.version, direction, &sample.data)
            .map_err(|_| Status::invalid_argument("decoder test failed"))?
            .into_iter()
            .map(|m| v1::ParsedMessage {
                message_id: uuid::Uuid::new_v4().to_string(),
                protocol_id: m.protocol,
                message_type: m.message_type,
                direction: r.direction,
                fields: Some(v1::FieldObject {
                    fields: m
                        .fields
                        .iter()
                        .map(|(k, v)| {
                            (
                                k.clone(),
                                hydra_wire::rules::value_to_wire(&crate::inspection::domain_value(
                                    v,
                                )),
                            )
                        })
                        .collect(),
                }),
                original_size: 0,
                ..Default::default()
            })
            .collect();
        Ok(Response::new(v1::TestProtocolReply { messages }))
    }
    async fn capture_sample(
        &self,
        request: Request<v1::CaptureSampleRequest>,
    ) -> Result<Response<v1::CaptureSampleReply>, Status> {
        let r = request.into_inner();
        check_provenance(r.actor.as_ref(), &r.request_id)?;
        if r.flow_id.is_empty() || r.flow_id.len() > 128 {
            return Err(Status::invalid_argument("invalid flow identifier"));
        }
        let t = &self.state.telemetry;
        if !t
            .flows
            .lock()
            .map_err(|_| Status::internal("flow registry unavailable"))?
            .contains(&r.flow_id)
        {
            return Err(Status::not_found("active flow not found"));
        }
        let ring = t
            .samples
            .read()
            .map_err(|_| Status::internal("sample storage unavailable"))?
            .clone()
            .ok_or_else(|| Status::failed_precondition("raw samples disabled"))?;
        let sample_id = ring
            .request_capture(r.flow_id)
            .map_err(|_| Status::resource_exhausted("capture admission unavailable"))?;
        Ok(Response::new(v1::CaptureSampleReply { sample_id }))
    }
    async fn get_sample(
        &self,
        request: Request<v1::GetSampleRequest>,
    ) -> Result<Response<v1::GetSampleReply>, Status> {
        let id = request.into_inner().sample_id;
        if id.is_empty() || id.len() > 128 {
            return Err(Status::invalid_argument("invalid sample identifier"));
        }
        let ring = self
            .state
            .telemetry
            .samples
            .read()
            .map_err(|_| Status::internal("sample storage unavailable"))?
            .clone()
            .ok_or_else(|| Status::failed_precondition("raw samples disabled"))?;
        let sample = ring
            .get(&id)
            .await
            .map_err(|_| Status::internal("sample read failed"))?
            .ok_or_else(|| Status::not_found("sample unavailable or expired"))?;
        Ok(Response::new(v1::GetSampleReply {
            data: sample.data,
            truncated: sample.truncated,
            expires_at: Some(prost_types::Timestamp {
                seconds: sample.expires_at,
                nanos: 0,
            }),
        }))
    }

    async fn get_flow_history(
        &self,
        request: Request<v1::GetFlowHistoryRequest>,
    ) -> Result<Response<v1::GetFlowHistoryReply>, Status> {
        let flow_id = request.into_inner().flow_id;
        if flow_id.len() > 128 || uuid::Uuid::parse_str(&flow_id).is_err() {
            return Err(Status::invalid_argument("invalid flow identifier"));
        }
        let _permit = self
            .diagnostics
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("diagnostics busy"))?;
        if self
            .state
            .telemetry
            .flows
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&flow_id)
        {
            return Err(Status::failed_precondition("flow is still active"));
        }
        let store = self
            .state
            .telemetry
            .history
            .read()
            .map_err(|_| Status::internal("history store unavailable"))?
            .clone()
            .ok_or_else(|| Status::unavailable("history store disabled"))?;
        let history = store
            .get(&flow_id)
            .await
            .map_err(history_status)?
            .ok_or_else(|| Status::not_found("flow history is unavailable or expired"))?;
        let total = if history.datagrams.is_empty() {
            history.client_to_server.len() + history.server_to_client.len()
        } else {
            history.datagrams.iter().map(|d| d.data.len()).sum()
        };
        if total > crate::history::MAX_HISTORY_BYTES {
            return Err(Status::resource_exhausted("history exceeds response limit"));
        }
        Ok(Response::new(v1::GetFlowHistoryReply {
            flow_id: history.flow_id,
            service_id: history.service_id,
            protocol_id: history.protocol_id,
            opened_at_ms: history.opened_at_ms,
            closed_at_ms: history.closed_at_ms,
            bytes_client_to_server: history.bytes_client_to_server,
            bytes_server_to_client: history.bytes_server_to_client,
            transport: if history.transport == "udp" { 2 } else { 1 },
            datagrams_client_to_server: history.datagrams_client_to_server,
            datagrams_server_to_client: history.datagrams_server_to_client,
            segments: if history.transport == "udp" || !history.datagrams.is_empty() {
                history
                    .datagrams
                    .into_iter()
                    .map(|d| v1::HistorySegment {
                        direction: d.direction.into(),
                        data: d.data,
                        datagram: history.transport == "udp",
                        occurred_at_ms: d.occurred_at_ms,
                        sequence: d.sequence,
                    })
                    .collect()
            } else {
                vec![
                    v1::HistorySegment {
                        direction: 1,
                        data: history.client_to_server,
                        ..Default::default()
                    },
                    v1::HistorySegment {
                        direction: 2,
                        data: history.server_to_client,
                        ..Default::default()
                    },
                ]
            },
            truncated_client_to_server: history.truncated_client_to_server,
            truncated_server_to_client: history.truncated_server_to_client,
            history_truncated: history.history_truncated,
        }))
    }
    async fn get_active_flow_history(
        &self,
        request: Request<v1::GetActiveFlowHistoryRequest>,
    ) -> Result<Response<v1::GetActiveFlowHistoryReply>, Status> {
        let input = request.into_inner();
        check_provenance(input.actor.as_ref(), &input.request_id)?;
        if input.flow_id.is_empty()
            || input.flow_id.len() > 128
            || uuid::Uuid::parse_str(&input.flow_id).is_err()
        {
            return Err(Status::invalid_argument("invalid flow identifier"));
        }
        let store = self
            .state
            .telemetry
            .history
            .read()
            .map_err(|_| Status::internal("history store unavailable"))?
            .clone()
            .ok_or_else(|| Status::unavailable("history store disabled"))?;
        let cursor = input.cursor.map(|cursor| crate::history::ActiveCursor {
            client_sequence: cursor.client_sequence,
            server_sequence: cursor.server_sequence,
            client_head_dropped: cursor.client_head_dropped,
            server_head_dropped: cursor.server_head_dropped,
        });
        let snapshot = store
            .active_snapshot(&input.flow_id, cursor)
            .map_err(history_status)?;
        let generated_at_ms = snapshot
            .as_ref()
            .map(|snapshot| snapshot.generated_at_ms)
            .unwrap_or_else(crate::history::now_ms);
        let Some(snapshot) = snapshot else {
            // Closed/unknown flows are not an error: Core falls back to the
            // persisted GetFlowHistory path.
            return Ok(Response::new(v1::GetActiveFlowHistoryReply {
                flow_id: input.flow_id,
                active: false,
                generated_at_ms,
                ..Default::default()
            }));
        };
        let datagram = snapshot.udp;
        Ok(Response::new(v1::GetActiveFlowHistoryReply {
            flow_id: snapshot.flow_id,
            service_id: snapshot.service_id,
            protocol_id: snapshot.protocol_id,
            opened_at_ms: snapshot.opened_at_ms,
            active: true,
            segments: snapshot
                .segments
                .into_iter()
                .map(|segment| v1::HistorySegment {
                    direction: segment.direction.into(),
                    data: segment.data,
                    datagram,
                    occurred_at_ms: segment.occurred_at_ms,
                    sequence: segment.sequence,
                })
                .collect(),
            truncated_client_to_server: snapshot.truncated_client_to_server,
            truncated_server_to_client: snapshot.truncated_server_to_client,
            retained_client_to_server_bytes: snapshot.retained_client_to_server_bytes,
            retained_server_to_client_bytes: snapshot.retained_server_to_client_bytes,
            reset: snapshot.reset,
            client_sequence: snapshot.client_sequence,
            server_sequence: snapshot.server_sequence,
            client_head_dropped: snapshot.client_head_dropped,
            server_head_dropped: snapshot.server_head_dropped,
            generated_at_ms: snapshot.generated_at_ms,
        }))
    }
    async fn probe_service(
        &self,
        request: Request<v1::ProbeServiceRequest>,
    ) -> Result<Response<v1::ProbeServiceReply>, Status> {
        let r = request.into_inner();
        check_provenance(r.actor.as_ref(), &r.request_id)?;
        if r.service_id.is_empty()
            || r.service_id.len() > 128
            || r.payload.len() > 4096
            || !(50..=5000).contains(&r.timeout_ms)
        {
            return Err(Status::invalid_argument("probe bounds exceeded"));
        }
        let _permit = self
            .diagnostics
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("diagnostic probe busy"))?;
        let service = self
            .state
            .get_service(&r.service_id)
            .await
            .ok_or_else(|| Status::not_found("configured service not found"))?;
        let transport = match r.transport {
            1 => Transport::Tcp,
            2 => Transport::Udp,
            _ => return Err(Status::invalid_argument("invalid probe transport")),
        };
        if !service.enabled || transport != service.transport {
            return Err(Status::failed_precondition(
                "service disabled or transport mismatch",
            ));
        }
        let started = Instant::now();
        let operation = probe(service.upstream_address, transport, &r.payload);
        let (response, truncated, error_code) = tokio::select! {
            _=self.shutdown.cancelled()=>(Vec::new(),false,"shutdown".into()),
            result=tokio::time::timeout(Duration::from_millis(r.timeout_ms.into()),operation)=>match result {Ok(Ok((bytes,truncated)))=>(bytes,truncated,String::new()),Ok(Err(_))=>(Vec::new(),false,"network_error".into()),Err(_)=>(Vec::new(),false,"timeout".into())}
        };
        Ok(Response::new(v1::ProbeServiceReply {
            response,
            truncated,
            elapsed_ms: started.elapsed().as_millis().min(u32::MAX as u128) as u32,
            error_code,
        }))
    }
}
async fn probe(
    address: SocketAddr,
    transport: Transport,
    payload: &[u8],
) -> std::io::Result<(Vec<u8>, bool)> {
    let mut response = vec![0; 4097];
    let n = match transport {
        Transport::Tcp => {
            let mut stream = TcpStream::connect(address).await?;
            stream.write_all(payload).await?;
            stream.shutdown().await?;
            stream.read(&mut response).await?
        }
        Transport::Udp => {
            let socket = UdpSocket::bind(if address.is_ipv4() {
                "0.0.0.0:0"
            } else {
                "[::]:0"
            })
            .await?;
            socket.connect(address).await?;
            socket.send(payload).await?;
            socket.recv(&mut response).await?
        }
    };
    response.truncate(n.min(4096));
    Ok((response, n > 4096))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn tcp_probe_preserves_bytes_and_bounds_response() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            s.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, b"\0probe\xff");
            s.write_all(&vec![7; 8192]).await.unwrap();
        });
        let (bytes, truncated) = probe(address, Transport::Tcp, b"\0probe\xff")
            .await
            .unwrap();
        assert_eq!(bytes, vec![7; 4096]);
        assert!(truncated);
        server.await.unwrap();
    }
    #[tokio::test]
    async fn udp_probe_preserves_bytes() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut bytes = [0; 64];
            let (n, from) = socket.recv_from(&mut bytes).await.unwrap();
            socket.send_to(&bytes[..n], from).await.unwrap();
        });
        let (bytes, truncated) = probe(address, Transport::Udp, b"\0udp\xff").await.unwrap();
        assert_eq!(bytes, b"\0udp\xff");
        assert!(!truncated);
        server.await.unwrap();
    }
}
