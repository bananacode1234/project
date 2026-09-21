use chat::protocol::{self, Message};
use futures::{SinkExt, StreamExt};
use reedline::{DefaultPrompt, DefaultPromptSegment, ExternalPrinter, Reedline, Signal};
use tokio::{net::TcpStream, sync::mpsc, time::Duration, time::interval};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or(String::from("127.0.0.1:8080"));

    println!("Connecting to {addr}...");

    let socket = TcpStream::connect(&addr).await?;
    let mut framed = Framed::new(socket, LengthDelimitedCodec::new());

    let mut heartbeat_timer = interval(Duration::from_secs(15));

    let (tx, mut rx) = mpsc::channel::<String>(32);

    let printer = ExternalPrinter::new(1024);
    let sender = printer.sender();

    std::thread::spawn(move || {
        let mut line_editor = Reedline::create().with_external_printer(printer);

        let prompt = DefaultPrompt::new(DefaultPromptSegment::Empty, DefaultPromptSegment::Empty);

        while let Ok(Signal::Success(input)) = line_editor.read_line(&prompt) {
            if tx.blocking_send(input).is_err() {
                break;
            }
        }
    });

    let mut nickname: String;

    loop {
        tokio::select! {
            _ = heartbeat_timer.tick() => {
                if framed.send(protocol::encode(Message::Ping)).await.is_err() {
                    break;
                }
            }
            result = framed.next() => {
                // server -> stdout
                let Some(Ok(frame)) = result else {
                    break;
                };

                let Ok(message) = protocol::decode(frame.freeze()) else {
                    break;
                };

                match message {
                    Message::Ping => (),
                    Message::Text(msg) => {
                        let _ = sender.send(msg.to_owned());
                    }
                    Message::Nick(new) => {
                        nickname = new;
                    }
                }
            }
            result = rx.recv() => {
                // stdin -> server
                let Some(msg) = result else {
                    break;
                };

                if msg.trim().is_empty() {
                    continue;
                }

                if let Some(command) = msg.strip_prefix('/') {
                    let mut args = command.split_whitespace();

                    match args.next() {
                        Some("nick") => {
                            let Some(nick) = args.next() else {
                                let _ = sender.send("Missing argument".to_owned());
                                continue;
                            };

                            if framed.send(protocol::encode(Message::Nick(nick.to_owned()))).await.is_err() {
                                break;
                            }
                        }
                        Some("exit") | Some("quit") => break,
                        _ => {
                            let _ = sender.send("Invalid command".to_owned());
                        }
                    }

                    continue;
                }

                if framed.send(protocol::encode(Message::Text(msg))).await.is_err() {
                    break;
                }
            }
        }
    }

    let _ = sender.send("Disconnected".to_owned());

    Ok(())
}
