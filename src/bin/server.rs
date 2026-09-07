use tokio::{io::AsyncWriteExt, io::AsyncBufReadExt, io::BufReader, net::TcpListener, sync::broadcast};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args().nth(1).unwrap_or(String::from("0.0.0.0:8080"));

    let listener = TcpListener::bind(&addr).await?;

    println!("Listening on {addr}");

    let (tx, _rx) = broadcast::channel::<(String, std::net::SocketAddr)>(32);

    loop {
        let (mut socket, peer_addr) = listener.accept().await?;
        println!("{} connected", peer_addr);

        let tx = tx.clone();
        let mut rx = tx.subscribe();

        tokio::spawn(async move {
            let (reader, mut writer) = socket.split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();

            loop {
                tokio::select! {
                    result = reader.read_line(&mut line) => {
                        if result.unwrap_or(0) == 0 {
                            break;
                        }

                        let _ = tx.send((line.to_string(), peer_addr));

                        line.clear();
                    }
                    result = rx.recv() => {
                        if let Ok((msg, addr)) = result {
                            if addr == peer_addr {
                                continue;
                            }
                            if writer.write_all(msg.as_bytes()).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }

            println!("{} disconnected", peer_addr);
        });
    }
}
