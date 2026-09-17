use bytes::{Buf, BufMut, Bytes, BytesMut};

pub enum Message {
    Ping,
    Text(String),
    Nick(String),
}

pub fn encode(message: Message) -> Bytes {
    let mut buffer = BytesMut::new();

    match message {
        Message::Ping => buffer.put_u8(0),

        Message::Text(msg) => {
            buffer.put_u8(1);

            buffer.put(msg.as_bytes());
        }

        Message::Nick(nick) => {
            buffer.put_u8(2);

            buffer.put(nick.as_bytes());
        }
    }

    buffer.freeze()
}

pub fn decode(mut bytes: Bytes) -> Result<Message, Box<dyn std::error::Error>> {
    match bytes.try_get_u8()? {
        0 => Ok(Message::Ping),
        1 => Ok(Message::Text(std::str::from_utf8(&bytes)?.to_owned())),
        2 => Ok(Message::Nick(std::str::from_utf8(&bytes)?.to_owned())),
        other => Err(format!("unknown message type: {other}").into()),
    }
}
