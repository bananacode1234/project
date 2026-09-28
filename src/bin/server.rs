use chat::protocol::{self, ClientMessage, ServerMessage};
use futures::{SinkExt, StreamExt};
use std::{collections::HashSet, sync::Arc};
use tokio::{
    net::TcpListener,
    sync::{Mutex, broadcast},
    time::{Duration, Instant, interval},
};
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args().nth(1).unwrap_or("0.0.0.0:8080".to_owned());

    let listener = TcpListener::bind(&addr).await?;

    println!("Listening on {addr}");

    let (tx, _rx) = broadcast::channel::<(ServerMessage, Option<std::net::SocketAddr>)>(32);

    let nicknames = Arc::new(Mutex::new(HashSet::<String>::new()));

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        println!("{peer_addr} connected");

        let tx = tx.clone();
        let mut rx = tx.subscribe();

        let set = Arc::clone(&nicknames);
        tokio::spawn(async move {
            let mut framed = Framed::new(socket, protocol::codec());

            let mut heartbeat_timer = interval(Duration::from_secs(10));
            let mut last_seen = Instant::now();

            let mut nickname = peer_addr.to_string();

            {
                let mut nicknames = set.lock().await;
                nicknames.insert(nickname.to_lowercase());
            }

            let _ = tx.send((ServerMessage::Join(nickname.clone()), None));

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

                        let Ok(message) = protocol::decode(&frame) else {
                            break;
                        };

                        last_seen = Instant::now();

                        match message {
                            ClientMessage::Ping => {}
                            ClientMessage::Text(msg) => {
                                if msg.len() > protocol::MAX_TEXT_LEN || !msg.chars().all(|c| c.is_ascii_graphic() || c == ' ') {
                                    break;
                                }

                                if msg.trim().is_empty() {
                                    continue;
                                }

                                let _ = tx.send((ServerMessage::Chat { from: nickname.clone(), text: msg }, None));
                            }
                            ClientMessage::Nick(new) => {
                                if new == nickname {
                                    if framed.send(protocol::encode(&ServerMessage::System("That is already your nickname".to_owned()))).await.is_err() {
                                        break;
                                    }

                                    continue;
                                }

                                if !(3..=20).contains(&new.len()) || new.chars().any(|c| !c.is_ascii_alphanumeric()) {
                                    if framed.send(protocol::encode(
                                        &ServerMessage::System("Invalid nickname (3-20 letters/numbers only)".to_owned())
                                    )).await.is_err() {
                                        break;
                                    }

                                    continue;
                                }

                                let taken = {
                                    let mut nicknames = set.lock().await;

                                    if nickname.to_lowercase() == new.to_lowercase() {
                                        false
                                    } else if nicknames.contains(&new.to_lowercase()) {
                                        true
                                    } else {
                                        nicknames.insert(new.to_lowercase());
                                        nicknames.remove(&nickname.to_lowercase());
                                        false
                                    }
                                };

                                if taken {
                                    if framed.send(protocol::encode(&ServerMessage::System("That nickname is taken".to_owned()))).await.is_err() {
                                        break;
                                    }

                                    continue;
                                }

                                let old = std::mem::replace(&mut nickname, new);

                                let _ = tx.send((ServerMessage::System(format!("{old} has changed their nickname to {nickname}")), Some(peer_addr)));

                                if framed.send(protocol::encode(&ServerMessage::Nick(nickname.clone()))).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    result = rx.recv() => {
                        // broadcast message -> client
                        let (msg, addr) = match result {
                            Ok(pair) => pair,
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                if framed.send(protocol::encode(&ServerMessage::System(format!("You missed {n} messages")))).await.is_err() {
                                    break;
                                }

                                continue;
                            }
                            _ => continue,
                        };

                        if Some(peer_addr) == addr {
                            continue;
                        }

                        if framed.send(protocol::encode(&msg)).await.is_err() {
                            break;
                        }
                    }
                }
            }

            {
                let mut nicknames = set.lock().await;
                nicknames.remove(&nickname.to_lowercase());
            }

            println!("{peer_addr} disconnected");
            let _ = tx.send((ServerMessage::Leave(nickname), None));
        });
    }
}
