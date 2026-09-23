use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum Message {
    Ping,
    Text(String),
    Nick(String),
}

pub fn encode(message: Message) -> Bytes {
    postcard::to_allocvec(&message).unwrap().into()
}

pub fn decode(bytes: Bytes) -> Result<Message, Box<dyn std::error::Error>> {
    Ok(postcard::from_bytes(&bytes)?)
}
