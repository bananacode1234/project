use chat::protocol::{self, ClientMessage, ServerMessage};
use futures::{SinkExt, StreamExt};
use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
};
use tokio::{
    net::TcpListener,
    sync::broadcast,
    time::{Instant, interval},
};
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:8080".to_owned());

    let listener = TcpListener::bind(&addr).await?;

    println!("Listening on {addr}");

    let (tx, _rx) = broadcast::channel::<(ServerMessage, Option<std::net::SocketAddr>)>(256);

    let nicknames = Arc::new(Mutex::new(HashSet::<String>::new()));

    let next_guest = Arc::new(AtomicU32::new(1));

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        println!("{peer_addr} connected");

        let tx = tx.clone();
        let mut rx = tx.subscribe();

        let nicknames = Arc::clone(&nicknames);

        let id = next_guest.fetch_add(1, Ordering::Relaxed);

        tokio::spawn(async move {
            let mut framed = Framed::new(socket, protocol::codec());

            let mut heartbeat_timer = interval(protocol::HEARTBEAT_INTERVAL);
            let mut last_seen = Instant::now();

            let mut nickname = format!("Guest#{id}");

            if framed
                .send(protocol::encode(&ServerMessage::Welcome(nickname.clone())))
                .await
                .is_err()
            {
                return;
            }

            {
                let mut guard = nicknames.lock().unwrap();
                guard.insert(nickname.to_lowercase());
            }

            let _ = tx.send((ServerMessage::Join(nickname.clone()), Some(peer_addr)));

            loop {
                tokio::select! {
                    _ = heartbeat_timer.tick() => {
                        if last_seen.elapsed() > protocol::HEARTBEAT_TIMEOUT || framed.send(protocol::encode(&ServerMessage::Ping)).await.is_err() {
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
                            ClientMessage::Pong => {}
                            ClientMessage::Text(text) => {
                                if !protocol::is_valid_text(&text) {
                                    break;
                                }

                                if text.trim().is_empty() {
                                    continue;
                                }

                                let _ = tx.send((ServerMessage::Chat { from: nickname.clone(), text }, None));
                            }
                            ClientMessage::Nick(new) => {
                                if new == nickname {
                                    if framed.send(protocol::encode(&ServerMessage::System("That is already your nickname".to_owned()))).await.is_err() {
                                        break;
                                    }

                                    continue;
                                }

                                if !protocol::is_valid_nickname(&new) {
                                    if framed.send(protocol::encode(
                                        &ServerMessage::System("Invalid nickname (3-20 letters/numbers only)".to_owned())
                                    )).await.is_err() {
                                        break;
                                    }

                                    continue;
                                }

                                let taken = {
                                    let old_key = nickname.to_lowercase();
                                    let new_key = new.to_lowercase();

                                    let mut guard = nicknames.lock().unwrap();

                                    if old_key == new_key {
                                        false
                                    } else if guard.insert(new_key) {
                                        guard.remove(&old_key);
                                        false
                                    } else {
                                        true
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
                let mut guard = nicknames.lock().unwrap();
                guard.remove(&nickname.to_lowercase());
            }

            println!("{peer_addr} disconnected");
            let _ = tx.send((ServerMessage::Leave(nickname), None));
        });
    }
}
