use chat::protocol;
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

    let (tx, _rx) = broadcast::channel::<(String, std::net::SocketAddr)>(32);

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        println!("{} connected", peer_addr);

        let tx = tx.clone();
        let mut rx = tx.subscribe();

        tokio::spawn(async move {
            let mut framed = Framed::new(socket, LengthDelimitedCodec::new());

            let mut heartbeat_timer = interval(Duration::from_secs(30));
            let mut last_seen = Instant::now();

            let _ = tx.send((format!("{peer_addr} connected"), peer_addr));

            loop {
                tokio::select! {
                    _ = heartbeat_timer.tick() => {
                        if last_seen.elapsed() > Duration::from_secs(60) {
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
                            protocol::Message::Ping => (),
                            protocol::Message::Text(msg) => {
                                let _ = tx.send((msg, peer_addr));
                            }
                        }
                    }
                    result = rx.recv() => {
                        // broadcast message -> client
                        let Ok((msg, addr)) = result else {
                            continue;
                        };

                        if peer_addr == addr {
                            continue;
                        }

                        if framed.send(protocol::encode(protocol::Message::Text(msg))).await.is_err() {
                            break;
                        }
                    }
                }
            }

            println!("{} disconnected", peer_addr);
            let _ = tx.send((format!("{peer_addr} disconnected"), peer_addr));
        });
    }
}
