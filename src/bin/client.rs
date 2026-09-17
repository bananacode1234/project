use chat::protocol::{self, Message};
use futures::{SinkExt, StreamExt};
use std::io::BufRead;
use tokio::{net::TcpStream, sync::mpsc, time::Duration, time::interval};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or(String::from("127.0.0.1:8080"));

    println!("Connecting to {addr}...");

    let socket = TcpStream::connect(&addr).await?;
    let mut framed = Framed::new(socket, LengthDelimitedCodec::new());

    let mut heartbeat_timer = interval(Duration::from_secs(30));

    let (tx, mut rx) = mpsc::channel::<String>(32);

    std::thread::spawn(move || {
        let stdin = std::io::stdin();

        let mut lines = std::io::BufReader::new(stdin).lines();

        while let Some(Ok(input)) = lines.next() {
            if tx.blocking_send(input).is_err() {
                break;
            }
        }
    });

    let mut nickname: String;

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
                    Message::Nick(new) => {
                        println!("Nickname updated to {new}");
                        nickname = new;
                    }
                }
            }
            result = rx.recv() => {
                // stdin -> server
                let Some(msg) = result else {
                    break;
                };

                if msg.trim().is_empty() {
                    continue;
                }

                if let Some(command) = msg.strip_prefix('/') {
                    let mut args = command.split_whitespace();

                    match args.next() {
                        Some("nick") => {
                            let Some(nick) = args.next() else {
                                println!("Missing argument");
                                continue;
                            };

                            if framed.send(protocol::encode(Message::Nick(nick.to_owned()))).await.is_err() {
                                break;
                            }
                        }
                        Some("exit") | Some("quit") => break,
                        _ => println!("Invalid command"),
                    }

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
