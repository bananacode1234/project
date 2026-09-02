use tokio::{io::AsyncReadExt, net::TcpStream};
use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    let addr = "127.0.0.1:8080";

    let mut socket = TcpStream::connect(addr).await?;

    println!("Connecting to {}", addr);

    let mut buf = [0; 1024];

    let n = socket.read(&mut buf).await?;

    println!("{} bytes received", n);
    
    let message = str::from_utf8(&buf[0..n]).unwrap();

    println!("{}", message);

    Ok(())
}
