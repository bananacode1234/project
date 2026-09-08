use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, stdin},
    net::TcpStream,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or(String::from("127.0.0.1:8080"));

    let mut socket = TcpStream::connect(&addr).await?;

    println!("Connected to {addr}");

    let (reader, mut writer) = socket.split();

    let mut server_reader = BufReader::new(reader);
    let mut stdin_reader = BufReader::new(stdin());

    let mut server_line = String::new();
    let mut stdin_line = String::new();

    loop {
        tokio::select! {
            result = server_reader.read_line(&mut server_line) => {
                if result.unwrap_or(0) == 0 {
                    break;
                }

                print!("{server_line}");
                server_line.clear();
            }

            result = stdin_reader.read_line(&mut stdin_line) => {
                if result.unwrap_or(0) == 0 {
                    break;
                }

                writer.write_all(stdin_line.as_bytes()).await?;
                stdin_line.clear();
            }
        }
    }

    Ok(())
}
