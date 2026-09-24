use bytes::Bytes;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio_util::codec::LengthDelimitedCodec;

const MAX_FRAME_LEN: usize = 1024;
pub const MAX_TEXT_LEN: usize = 500;

#[derive(Serialize, Deserialize, Clone)]
pub enum ClientMessage {
    Ping,
    Text(String),
    Nick(String),
}

#[derive(Serialize, Deserialize, Clone)]
pub enum ServerMessage {
    Chat { from: String, text: String },
    Nick(String),
    System(String),
    Join(String),
    Leave(String),
}

pub fn encode<T: Serialize>(message: &T) -> Bytes {
    postcard::to_allocvec(message).unwrap().into()
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, postcard::Error> {
    postcard::from_bytes(bytes)
}

pub fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_LEN)
        .new_codec()
}
