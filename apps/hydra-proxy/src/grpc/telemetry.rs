use super::Rpc;
use hydra_wire::v1::{self, event::Payload, proxy_telemetry_server::ProxyTelemetry};
use prost::Message;
use std::sync::atomic::Ordering;
use tokio::sync::{
    broadcast::error::{RecvError, TryRecvError},
    mpsc,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

const MAX_BATCH_EVENTS: u32 = 128;
/// Same bound as the diagnostics encoding cap; 1/16 of tonic's 4 MiB receive default.
pub(crate) const MAX_BATCH_BYTES: usize = 256 * 1024;
/// Core's per-event *field budget* (`traffic::repository::field`), not its 64 KiB
/// JSON-row limit: `encoded_len` bounds field bytes but not JSON escaping or
/// bytes-array expansion, so a JSON row can still be rejected as a whole batch.
pub(crate) const CORE_EVENT_BUDGET: usize = 49_152;

fn loss_event(n: u64) -> v1::Event {
    v1::Event {
        event_id: uuid::Uuid::new_v4().to_string(),
        occurred_at: Some(crate::telemetry::timestamp()),
        payload: Some(Payload::Loss(v1::EventLoss {
            count: n,
            reason: "subscriber_lag".into(),
        })),
        ..Default::default()
    }
}

/// Encoded size of one `EventBatch.events` entry: tag (field 4, one byte) + length + body.
fn event_field_len(event: &v1::Event) -> usize {
    let body = event.encoded_len();
    1 + prost::encoding::encoded_len_varint(body as u64) + body
}

#[tonic::async_trait]
impl ProxyTelemetry for Rpc {
    type StreamTelemetryStream = ReceiverStream<Result<v1::EventBatch, Status>>;
    async fn stream_telemetry(
        &self,
        request: Request<v1::StreamTelemetryRequest>,
    ) -> Result<Response<Self::StreamTelemetryStream>, Status> {
        let requested = request.into_inner().max_events_per_batch;
        if requested > MAX_BATCH_EVENTS {
            return Err(Status::invalid_argument("batch cap is 128"));
        }
        let t = self.state.telemetry.clone();
        let permit = t
            .subscribers
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("at most four telemetry subscribers"))?;
        let mut events = t.events.subscribe();
        let (sender, receiver) = mpsc::channel(16);
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let _permit = permit;
            // proto3 default 0 keeps the legacy one-event-per-batch behaviour.
            let max_events = requested.max(1) as usize;
            let base_bytes = v1::EventBatch {
                proxy_epoch_id: t.epoch.clone(),
                ..Default::default()
            }
            .encoded_len();
            let mut carry: Option<v1::Event> = None;
            loop {
                // One awaited receive starts the batch; no timer, so a lone event
                // still ships immediately. A carried event keeps FIFO order.
                let first = match carry.take() {
                    Some(event) => event,
                    None => {
                        let next = tokio::select! {
                            _ = shutdown.cancelled() => break,
                            _ = sender.closed() => break,
                            event = events.recv() => event,
                        };
                        match next {
                            Ok(event) => event,
                            Err(RecvError::Lagged(n)) => {
                                t.dropped.fetch_add(n, Ordering::Relaxed);
                                loss_event(n)
                            }
                            Err(RecvError::Closed) => break,
                        }
                    }
                };
                let mut batch = vec![first];
                let mut bytes = base_bytes + event_field_len(&batch[0]);
                let mut closed = false;
                // Drain only already-ready events, bounded by count and encoded bytes.
                // An event above Core's field budget travels alone so it cannot poison
                // up to 127 healthy neighbours through Core's all-or-nothing batch.
                // Residual: Core also rejects any event whose JSON row exceeds 64 KiB
                // (escaping / bytes-array expansion), which this guard does not bound.
                if batch[0].encoded_len() <= CORE_EVENT_BUDGET {
                    while batch.len() < max_events && bytes < MAX_BATCH_BYTES {
                        if shutdown.is_cancelled() {
                            break;
                        }
                        match events.try_recv() {
                            Ok(event) => {
                                let size = event_field_len(&event);
                                if event.encoded_len() > CORE_EVENT_BUDGET
                                    || bytes.saturating_add(size) > MAX_BATCH_BYTES
                                {
                                    carry = Some(event);
                                    break;
                                }
                                bytes += size;
                                batch.push(event);
                            }
                            Err(TryRecvError::Lagged(n)) => {
                                // counted exactly once, where the gap is observed
                                t.dropped.fetch_add(n, Ordering::Relaxed);
                                let marker = loss_event(n);
                                let size = event_field_len(&marker);
                                if bytes.saturating_add(size) > MAX_BATCH_BYTES {
                                    carry = Some(marker);
                                    break;
                                }
                                bytes += size;
                                batch.push(marker);
                            }
                            Err(TryRecvError::Empty) => break,
                            Err(TryRecvError::Closed) => {
                                closed = true;
                                break;
                            }
                        }
                    }
                }
                let out = v1::EventBatch {
                    proxy_epoch_id: t.epoch.clone(),
                    events: batch,
                    ..Default::default()
                };
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    result = sender.send(Ok(out)) => if result.is_err() { break; }
                }
                if closed {
                    break;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
    type StreamCriticalStream = ReceiverStream<Result<v1::EventBatch, Status>>;
    async fn stream_critical(
        &self,
        request: Request<v1::StreamCriticalRequest>,
    ) -> Result<Response<Self::StreamCriticalStream>, Status> {
        let request = request.into_inner();
        if request.proxy_epoch_id.len() > 64 {
            return Err(Status::invalid_argument("invalid epoch"));
        }
        let t = self.state.telemetry.clone();
        let permit = t
            .subscribers
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("subscriber limit"))?;
        let spool = t
            .critical
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| Status::unavailable("critical spool unavailable"))?;
        let (sender, receiver) = mpsc::channel(16);
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let mut epoch = request.proxy_epoch_id;
            let mut after = request.after_sequence;
            loop {
                let batches = match spool.replay(&epoch, after, 16).await {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = sender.send(Err(Status::unavailable(e.to_string()))).await;
                        break;
                    }
                };
                for batch in batches {
                    epoch = batch.proxy_epoch_id.clone();
                    after = batch.first_sequence + batch.events.len() as u64 - 1;
                    tokio::select! {_=shutdown.cancelled()=>return,result=sender.send(Ok(batch))=>if result.is_err(){return}}
                }
                tokio::select! {_=shutdown.cancelled()=>break,_=sender.closed()=>break,_=tokio::time::sleep(std::time::Duration::from_millis(100))=>{}}
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
    async fn ack_critical(
        &self,
        request: Request<v1::AckCriticalRequest>,
    ) -> Result<Response<v1::AckCriticalReply>, Status> {
        let request = request.into_inner();
        let spool = self
            .state
            .telemetry
            .critical
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| Status::unavailable("critical spool unavailable"))?;
        let acknowledged_through = spool
            .ack(&request.proxy_epoch_id, request.through_sequence)
            .await
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        Ok(Response::new(v1::AckCriticalReply {
            acknowledged_through,
        }))
    }
}
