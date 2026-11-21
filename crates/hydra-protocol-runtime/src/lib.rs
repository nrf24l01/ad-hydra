//! Bounded decoders; decoding never owns or edits transport buffers.
mod decoder;
mod framing;
mod grpc;
mod http;
mod http_response;
mod raw;
mod utf8;
pub use decoder::ProtocolDecoder;
pub use framing::FramingDecoder;
pub use grpc::{GrpcDecoder, MAX_FRAME_BODY, MAX_MESSAGE_BODY};
mod wasm;
pub use http::{HttpBatch, HttpDecoder, HttpRequest};
pub use hydra_protocol_sdk::{Direction, ParsedMessage, Value};
pub use raw::{MAX_RAW_PATTERN, RawDecoder};
pub use utf8::Utf8Decoder;
pub use wasm::{PluginRegistry, PluginVersion, WasmSession};
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError(pub String);
impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for DecodeError {}
pub enum SessionDecoder {
    Raw(RawDecoder),
    Framing(FramingDecoder),
    Grpc(GrpcDecoder),
    Http(HttpDecoder),
    Utf8(Utf8Decoder),
    Wasm(Box<WasmSession>),
}
impl SessionDecoder {
    pub fn feed(
        &mut self,
        direction: Direction,
        bytes: &[u8],
    ) -> Result<Vec<ParsedMessage>, DecodeError> {
        match self {
            Self::Framing(d) => ProtocolDecoder::feed(d, direction, bytes),
            Self::Grpc(d) => ProtocolDecoder::feed(d, direction, bytes),
            Self::Raw(d) => ProtocolDecoder::feed(d, direction, bytes),
            Self::Http(d) => ProtocolDecoder::feed(d, direction, bytes),
            Self::Utf8(d) => ProtocolDecoder::feed(d, direction, bytes),
            Self::Wasm(d) => ProtocolDecoder::feed(d.as_mut(), direction, bytes),
        }
    }

    pub fn field_schema(&self) -> Vec<hydra_protocol_sdk::FieldSchema> {
        match self {
            Self::Raw(d) => d.field_schema(),
            Self::Framing(d) => d.field_schema(),
            Self::Grpc(d) => d.field_schema(),
            Self::Http(d) => d.field_schema(),
            Self::Utf8(d) => d.field_schema(),
            Self::Wasm(d) => d.field_schema(),
        }
    }
    pub fn feed_datagram(
        &mut self,
        direction: Direction,
        bytes: &[u8],
    ) -> Result<Vec<ParsedMessage>, DecodeError> {
        if let Self::Raw(decoder) = self {
            decoder.feed_datagram(direction, bytes)
        } else {
            self.feed(direction, bytes)
        }
    }
}
