use tokio::{io::AsyncWriteExt, io::AsyncBufReadExt, io::BufReader, net::TcpListener, sync::broadcast};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:8080";

    let listener = TcpListener::bind(addr).await?;

    println!("Listening on {addr}");

    let (tx, _rx) = broadcast::channel::<String>(32);

    loop {
        let (mut socket, _) = listener.accept().await?;
        println!("Client connected");

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

                        let _ = tx.send(line.to_string());

                        line.clear();
                    }
                    result = rx.recv() => {
                        if let Ok(msg) = result {
                            if writer.write_all(msg.as_bytes()).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }

            println!("Client disconnected")
        });
    }
}
