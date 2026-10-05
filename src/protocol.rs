use bytes::Bytes;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::time::Duration;
use tokio_util::codec::LengthDelimitedCodec;

const MAX_FRAME_LEN: usize = 1024;
pub const MAX_TEXT_LEN: usize = 500;

pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(40);

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ClientMessage {
    Pong,
    Text(String),
    Nick(String),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ServerMessage {
    Chat { from: String, text: String },
    Nick(String),
    System(String),
    Welcome(String),
    Join(String),
    Leave(String),
    Ping,
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

pub fn is_valid_text_char(char: char) -> bool {
    char.is_ascii_graphic() || char == ' '
}

pub fn is_valid_text(text: &str) -> bool {
    text.len() <= MAX_TEXT_LEN && text.chars().all(is_valid_text_char)
}

pub fn is_valid_nickname(nick: &str) -> bool {
    (3..=20).contains(&nick.len()) && nick.chars().all(|c| c.is_ascii_alphanumeric())
}
