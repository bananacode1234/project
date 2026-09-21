use chat::protocol::{self, Message};
use futures::{SinkExt, StreamExt};
use tokio::{
    net::TcpListener,
    sync::broadcast,
    time::{Duration, Instant, interval},
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or(String::from("0.0.0.0:8080"));

    let listener = TcpListener::bind(&addr).await?;

    println!("Listening on {addr}");

    let (tx, _rx) = broadcast::channel::<(String, Option<std::net::SocketAddr>)>(32);

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        println!("{} connected", peer_addr);

        let tx = tx.clone();
        let mut rx = tx.subscribe();

        tokio::spawn(async move {
            let mut framed = Framed::new(socket, LengthDelimitedCodec::new());

            let mut heartbeat_timer = interval(Duration::from_secs(10));
            let mut last_seen = Instant::now();

            let mut nickname = peer_addr.to_string();

            let _ = tx.send((format!("[server] {nickname} connected"), None));

            loop {
                tokio::select! {
                    _ = heartbeat_timer.tick() => {
                        if last_seen.elapsed() > Duration::from_secs(40) {
                            break;
                        }
                    }
                    result = framed.next() => {
                        // client message -> broadcast
                        let Some(Ok(frame)) = result else {
                            break;
                        };

                        let Ok(message) = protocol::decode(frame.freeze()) else {
                            break;
                        };

                        last_seen = Instant::now();

                        match message {
                            Message::Ping => (),
                            Message::Text(msg) => {
                                if msg.trim().is_empty() {
                                    continue;
                                }

                                let _ = tx.send((format!("<{nickname}> {msg}"), Some(peer_addr)));
                            }
                            Message::Nick(new) => {
                                if new == nickname {
                                    continue;
                                }

                                if !(3..=20).contains(&new.len()) || new.chars().any(|c| !c.is_ascii_alphanumeric()) {
                                    if framed.send(protocol::encode(
                                        Message::Text("[server] invalid nickname (3-20 letters/numbers only)".to_owned())
                                    )).await.is_err() {
                                        break;
                                    }

                                    continue;
                                }

                                if framed.send(protocol::encode(Message::Nick(new.clone()))).await.is_err() || framed.send(protocol::encode(Message::Text(format!("[server] your nickname has been updated to {new}")))).await.is_err() {
                                    break;
                                }

                                let _ = tx.send((format!("[server] {nickname} has changed their nickname to {new}"), Some(peer_addr)));
                                nickname = new;
                            }
                        }
                    }
                    result = rx.recv() => {
                        // broadcast message -> client
                        let (msg, addr) = match result {
                            Ok((msg, addr)) => (msg, addr),
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                if framed.send(protocol::encode(Message::Text(format!("[server] you missed {n} messages")))).await.is_err() {
                                    break;
                                }

                                continue;
                            }
                            _ => {
                                continue;
                            }
                        };

                        if Some(peer_addr) == addr {
                            continue;
                        }

                        if framed.send(protocol::encode(Message::Text(msg))).await.is_err() {
                            break;
                        }
                    }
                }
            }

            println!("{} disconnected", peer_addr);
            let _ = tx.send((format!("[server] {nickname} disconnected"), Some(peer_addr)));
        });
    }
}
