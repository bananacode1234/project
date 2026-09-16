use chat::protocol::{self, Message};
use futures::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncBufReadExt, BufReader, stdin},
    net::TcpStream,
    time::Duration,
    time::interval,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or(String::from("127.0.0.1:8080"));

    let socket = TcpStream::connect(&addr).await?;
    let mut framed = Framed::new(socket, LengthDelimitedCodec::new());

    let mut heartbeat_timer = interval(Duration::from_secs(30));

    println!("Connected to {addr}");
    println!("Type and press enter to send");

    let mut lines = BufReader::new(stdin()).lines();

    loop {
        tokio::select! {
            _ = heartbeat_timer.tick() => {
                if framed.send(protocol::encode(Message::Ping)).await.is_err() {
                    break;
                }
            }
            result = framed.next() => {
                // server -> stdout
                let Some(Ok(frame)) = result else {
                    break;
                };

                let Ok(message) = protocol::decode(frame.freeze()) else {
                    break;
                };

                match message {
                    Message::Ping => (),
                    Message::Text(msg) => {
                        println!("{msg}");
                    }
                }
            }
            result = lines.next_line() => {
                // stdin -> server
                let Ok(Some(msg)) = result else {
                    break;
                };

                if msg.is_empty() {
                    continue;
                }

                if framed.send(protocol::encode(Message::Text(msg))).await.is_err() {
                    break;
                }
            }
        }
    }

    println!("Disconnected");

    Ok(())
}
