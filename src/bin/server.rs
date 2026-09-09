use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::{net::TcpListener, sync::broadcast};
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

            let _ = tx.send((format!("{peer_addr} connected"), peer_addr));

            loop {
                tokio::select! {
                    result = framed.next() => {
                        // client message -> broadcast
                        match result {
                            Some(Ok(result)) => {
                                if let Ok(msg) = std::str::from_utf8(&result) {
                                    let _ = tx.send((String::from(msg), peer_addr));
                                } else {
                                    break;
                                }
                            }
                            _ => {
                                break;
                            }
                        }
                    }
                    result = rx.recv() => {
                        // broadcast message -> client
                        match result {
                            Ok((msg, addr)) => {
                                if peer_addr != addr && framed.send(Bytes::from(msg)).await.is_err() {
                                    break;
                                }
                            }
                            _ => {
                                continue;
                            }
                        }
                    }
                }
            }

            println!("{} disconnected", peer_addr);
            let _ = tx.send((format!("{peer_addr} disconnected"), peer_addr));
        });
    }
}
