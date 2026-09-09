use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncBufReadExt, BufReader, stdin},
    net::TcpStream,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or(String::from("127.0.0.1:8080"));

    let socket = TcpStream::connect(&addr).await?;
    let mut framed = Framed::new(socket, LengthDelimitedCodec::new());

    println!("Connected to {addr}");
    println!("Type and press enter to send");

    let mut lines = BufReader::new(stdin()).lines();

    loop {
        tokio::select! {
            result = framed.next() => {
                // server -> stdout
                match result {
                    Some(Ok(result)) => {
                        if let Ok(msg) = std::str::from_utf8(&result) {
                            println!("{msg}");
                        } else {
                            break;
                        }
                    }
                    _ => {
                        break;
                    }
                }
            }
            result = lines.next_line() => {
                // stdin -> server
                match result {
                    Ok(Some(result)) => {
                        if !result.is_empty() && framed.send(Bytes::from(result)).await.is_err() {
                            break;
                        }
                    }
                    _ => {
                        break;
                    }
                }
            }
        }
    }

    println!("Disconnected");

    Ok(())
}
