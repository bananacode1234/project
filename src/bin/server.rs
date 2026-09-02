use tokio::{io::AsyncWriteExt, net::TcpListener};
use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    let addr = "127.0.0.1:8080";

    let listener = TcpListener::bind(addr).await?;

    println!("Listening on {}", addr);

    let (mut socket, _) = listener.accept().await?;

    println!("New connection");

    socket.write_all(b"Bonjour du local 149").await?;

    Ok(())
}
