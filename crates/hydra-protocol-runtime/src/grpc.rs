//! gRPC over HTTP/2 framing.
//!
//! HPACK is not decoded, so headers are reported as frames without their fields;
//! DATA frames are split into length-prefixed gRPC messages, which is where the
//! protobuf payload and the flags live. Decoding is bounded: a frame larger than
//! [`MAX_FRAME_BODY`] is skipped without being buffered.
use crate::{DecodeError, Direction, ParsedMessage, ProtocolDecoder, Value};
use hydra_protocol_sdk::{FieldSchema, FieldType, MAX_INPUT};
use std::collections::BTreeMap;

/// Largest HTTP/2 frame body kept for inspection.
pub const MAX_FRAME_BODY: usize = 32 * 1024;
/// Largest gRPC message published as bytes; longer ones report the length only.
pub const MAX_MESSAGE_BODY: usize = 16 * 1024;
const FRAME_HEADER: usize = 9;
const MESSAGE_HEADER: usize = 5;

/// Frames are framed on both directions independently.
#[derive(Default)]
pub struct GrpcDecoder {
    buffers: [Vec<u8>; 2],
    /// Bytes still to discard from a frame that is too large to inspect.
    skipping: [usize; 2],
}

fn frame_type(kind: u8) -> &'static str {
    match kind {
        0 => "data",
        1 => "headers",
        2 => "priority",
        3 => "rst_stream",
        4 => "settings",
        5 => "push_promise",
        6 => "ping",
        7 => "goaway",
        8 => "window_update",
        9 => "continuation",
        _ => "unknown",
    }
}

fn message(
    direction: Direction,
    message_type: &str,
    fields: BTreeMap<String, Value>,
) -> ParsedMessage {
    ParsedMessage {
        protocol: "grpc".into(),
        message_type: message_type.into(),
        direction,
        fields,
    }
}

impl ProtocolDecoder for GrpcDecoder {
    fn feed(
        &mut self,
        direction: Direction,
        bytes: &[u8],
    ) -> Result<Vec<ParsedMessage>, DecodeError> {
        if bytes.len() > MAX_INPUT {
            return Err(DecodeError("gRPC decoder input exceeds 64 KiB".into()));
        }
        let index = usize::from(direction == Direction::ServerToClient);
        self.buffers[index].extend_from_slice(bytes);
        let mut messages = Vec::new();
        loop {
            if self.skipping[index] > 0 {
                let buffer = &mut self.buffers[index];
                let take = self.skipping[index].min(buffer.len());
                buffer.drain(..take);
                self.skipping[index] -= take;
                if self.skipping[index] > 0 {
                    break;
                }
            }
            let buffer = &mut self.buffers[index];
            if buffer.len() < FRAME_HEADER {
                break;
            }
            let length = u32::from_be_bytes([0, buffer[0], buffer[1], buffer[2]]) as usize;
            let kind = buffer[3];
            let flags = buffer[4];
            let stream = u32::from_be_bytes([buffer[5] & 0x7f, buffer[6], buffer[7], buffer[8]]);
            let end_stream = kind == 0 && flags & 0x1 != 0;
            if length > MAX_FRAME_BODY {
                messages.push(message(
                    direction,
                    "frame",
                    BTreeMap::from([
                        (
                            "grpc.frame_type".into(),
                            Value::String(frame_type(kind).into()),
                        ),
                        ("grpc.frame_length".into(), Value::Unsigned(length as u64)),
                        ("grpc.stream_id".into(), Value::Unsigned(stream.into())),
                        ("grpc.end_stream".into(), Value::Bool(end_stream)),
                        ("grpc.truncated".into(), Value::Bool(true)),
                    ]),
                ));
                buffer.drain(..FRAME_HEADER);
                self.skipping[index] = length;
                continue;
            }
            if buffer.len() < FRAME_HEADER + length {
                break;
            }
            let body: Vec<u8> = buffer[FRAME_HEADER..FRAME_HEADER + length].to_vec();
            buffer.drain(..FRAME_HEADER + length);
            if kind != 0 {
                messages.push(message(
                    direction,
                    "frame",
                    BTreeMap::from([
                        (
                            "grpc.frame_type".into(),
                            Value::String(frame_type(kind).into()),
                        ),
                        ("grpc.frame_length".into(), Value::Unsigned(length as u64)),
                        ("grpc.stream_id".into(), Value::Unsigned(stream.into())),
                        ("grpc.end_stream".into(), Value::Bool(end_stream)),
                    ]),
                ));
                continue;
            }
            let mut offset = 0;
            let mut emitted = 0;
            while body.len() - offset >= MESSAGE_HEADER {
                let compressed = body[offset] == 1;
                let size = u32::from_be_bytes([
                    body[offset + 1],
                    body[offset + 2],
                    body[offset + 3],
                    body[offset + 4],
                ]) as usize;
                if body.len() - offset - MESSAGE_HEADER < size {
                    break;
                }
                let payload = &body[offset + MESSAGE_HEADER..offset + MESSAGE_HEADER + size];
                let mut fields = BTreeMap::from([
                    ("grpc.stream_id".into(), Value::Unsigned(stream.into())),
                    ("grpc.compressed".into(), Value::Bool(compressed)),
                    ("grpc.message_length".into(), Value::Unsigned(size as u64)),
                    (
                        "grpc.message_text".into(),
                        Value::String(
                            String::from_utf8_lossy(&payload[..size.min(MAX_MESSAGE_BODY)])
                                .into_owned(),
                        ),
                    ),
                    (
                        "grpc.message".into(),
                        Value::Bytes(payload[..size.min(MAX_MESSAGE_BODY)].to_vec()),
                    ),
                ]);
                if size > MAX_MESSAGE_BODY {
                    fields.insert("grpc.truncated".into(), Value::Bool(true));
                }
                messages.push(message(direction, "message", fields));
                offset += MESSAGE_HEADER + size;
                emitted += 1;
            }
            if emitted == 0 {
                messages.push(message(
                    direction,
                    "frame",
                    BTreeMap::from([
                        ("grpc.frame_type".into(), Value::String("data".into())),
                        ("grpc.frame_length".into(), Value::Unsigned(length as u64)),
                        ("grpc.stream_id".into(), Value::Unsigned(stream.into())),
                        ("grpc.end_stream".into(), Value::Bool(end_stream)),
                    ]),
                ));
            } else if end_stream && let Some(last) = messages.last_mut() {
                last.fields
                    .insert("grpc.end_stream".into(), Value::Bool(true));
            }
        }
        Ok(messages)
    }

    fn field_schema(&self) -> Vec<FieldSchema> {
        [
            ("grpc.stream_id", FieldType::Unsigned),
            ("grpc.frame_type", FieldType::String),
            ("grpc.frame_length", FieldType::Unsigned),
            ("grpc.end_stream", FieldType::Bool),
            ("grpc.compressed", FieldType::Bool),
            ("grpc.message_length", FieldType::Unsigned),
            ("grpc.message_text", FieldType::String),
            ("grpc.message", FieldType::Bytes),
            ("grpc.truncated", FieldType::Bool),
        ]
        .into_iter()
        .map(|(path, field_type)| FieldSchema {
            path: path.into(),
            field_type,
        })
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(kind: u8, flags: u8, stream: u32, body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(body.len() as u32).to_be_bytes()[1..]);
        out.push(kind);
        out.push(flags);
        out.extend_from_slice(&stream.to_be_bytes());
        out.extend_from_slice(body);
        out
    }

    fn grpc_message(payload: &[u8]) -> Vec<u8> {
        let mut out = vec![0u8];
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    #[test]
    fn data_frame_yields_one_message_with_its_payload() {
        let mut decoder = GrpcDecoder::default();
        let messages = decoder
            .feed(
                Direction::ClientToServer,
                &frame(0, 0, 1, &grpc_message(b"flag=ABC")),
            )
            .unwrap();
        assert_eq!(messages.len(), 1);
        let message = &messages[0];
        assert_eq!(message.protocol, "grpc");
        assert_eq!(message.message_type, "message");
        assert_eq!(message.fields["grpc.stream_id"], Value::Unsigned(1));
        assert_eq!(message.fields["grpc.message_length"], Value::Unsigned(8));
        assert_eq!(
            message.fields["grpc.message_text"],
            Value::String("flag=ABC".into())
        );
        assert_eq!(message.fields["grpc.compressed"], Value::Bool(false));
    }

    #[test]
    fn several_messages_in_one_frame_are_split() {
        let mut body = grpc_message(b"one");
        body.extend_from_slice(&grpc_message(b"two"));
        let mut decoder = GrpcDecoder::default();
        let messages = decoder
            .feed(Direction::ServerToClient, &frame(0, 1, 3, &body))
            .unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(
            messages[1].fields["grpc.message_text"],
            Value::String("two".into())
        );
        assert_eq!(messages[1].fields["grpc.end_stream"], Value::Bool(true));
    }

    #[test]
    fn frames_split_across_reads_are_reassembled() {
        let bytes = frame(0, 0, 1, &grpc_message(b"split me"));
        let mut decoder = GrpcDecoder::default();
        let mut produced = 0;
        for chunk in bytes.chunks(4) {
            produced += decoder
                .feed(Direction::ClientToServer, chunk)
                .unwrap()
                .len();
        }
        assert_eq!(produced, 1);
    }

    #[test]
    fn headers_are_reported_as_frames_without_hpack_decoding() {
        let mut decoder = GrpcDecoder::default();
        let messages = decoder
            .feed(Direction::ClientToServer, &frame(1, 4, 1, &[0x82, 0x86]))
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].message_type, "frame");
        assert_eq!(
            messages[0].fields["grpc.frame_type"],
            Value::String("headers".into())
        );
        assert_eq!(messages[0].fields["grpc.frame_length"], Value::Unsigned(2));
    }

    #[test]
    fn oversized_frames_are_skipped_without_losing_the_next_one() {
        let mut decoder = GrpcDecoder::default();
        let mut input = Vec::new();
        let header = {
            let mut out = Vec::new();
            out.extend_from_slice(&((MAX_FRAME_BODY + 10) as u32).to_be_bytes()[1..]);
            out.push(0);
            out.push(0);
            out.extend_from_slice(&7u32.to_be_bytes());
            out
        };
        input.extend_from_slice(&header);
        input.extend_from_slice(&vec![b'x'; MAX_FRAME_BODY + 10]);
        input.extend_from_slice(&frame(0, 0, 1, &grpc_message(b"after")));
        let messages = decoder.feed(Direction::ServerToClient, &input).unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].fields["grpc.truncated"], Value::Bool(true));
        assert_eq!(messages[0].fields["grpc.stream_id"], Value::Unsigned(7));
        assert_eq!(
            messages[1].fields["grpc.message_text"],
            Value::String("after".into())
        );
    }

    #[test]
    fn partial_message_inside_a_complete_frame_is_not_emitted() {
        let mut body = grpc_message(b"complete");
        body.extend_from_slice(&[0, 0, 0, 0, 99, b'x']);
        let mut decoder = GrpcDecoder::default();
        let messages = decoder
            .feed(Direction::ClientToServer, &frame(0, 0, 1, &body))
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0].fields["grpc.message_text"],
            Value::String("complete".into())
        );
    }

    #[test]
    fn oversized_messages_report_the_length_and_mark_truncation() {
        let payload = vec![b'a'; MAX_MESSAGE_BODY + 5];
        let mut decoder = GrpcDecoder::default();
        let messages = decoder
            .feed(
                Direction::ClientToServer,
                &frame(0, 0, 1, &grpc_message(&payload)),
            )
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0].fields["grpc.message_length"],
            Value::Unsigned((MAX_MESSAGE_BODY + 5) as u64)
        );
        assert_eq!(messages[0].fields["grpc.truncated"], Value::Bool(true));
        match &messages[0].fields["grpc.message"] {
            Value::Bytes(bytes) => assert_eq!(bytes.len(), MAX_MESSAGE_BODY),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn input_above_the_limit_is_rejected() {
        let mut decoder = GrpcDecoder::default();
        assert!(
            decoder
                .feed(Direction::ClientToServer, &vec![0u8; MAX_INPUT + 1])
                .is_err()
        );
    }

    #[test]
    fn directions_buffer_independently() {
        let mut decoder = GrpcDecoder::default();
        let request = frame(0, 0, 1, &grpc_message(b"request"));
        let response = frame(0, 0, 1, &grpc_message(b"response"));
        assert_eq!(
            decoder
                .feed(Direction::ClientToServer, &request)
                .unwrap()
                .len(),
            1
        );
        let messages = decoder.feed(Direction::ServerToClient, &response).unwrap();
        assert_eq!(
            messages[0].fields["grpc.message_text"],
            Value::String("response".into())
        );
    }
}
