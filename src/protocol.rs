use bytes::Bytes;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

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
