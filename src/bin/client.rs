use chat::protocol::{self, ClientMessage, ServerMessage};
use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind};
use futures::{SinkExt, StreamExt};
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Layout, Position},
    style::Stylize,
    text::{Line, Text},
    widgets::{Block, Paragraph},
};
use tokio::{
    net::TcpStream,
    time::{Duration, interval},
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let terminal = ratatui::init();

    let result = App::new().run(terminal).await;

    ratatui::restore();

    result
}

struct App {
    messages: Vec<String>,
    input: Vec<char>,
    cursor_index: usize,
    nickname: String,
}

impl App {
    const fn new() -> Self {
        Self {
            messages: Vec::new(),
            input: Vec::new(),
            cursor_index: 0,
            nickname: String::new(),
        }
    }

    fn cursor_left(&mut self) {
        self.cursor_index = self.cursor_index.saturating_sub(1);
    }

    fn cursor_right(&mut self) {
        self.cursor_index = (self.cursor_index + 1).min(self.input.len());
    }

    async fn run(
        mut self,
        mut terminal: DefaultTerminal,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let addr = std::env::args()
            .nth(1)
            .unwrap_or("127.0.0.1:8080".to_owned());

        let socket = TcpStream::connect(&addr).await?;
        let mut framed = Framed::new(socket, LengthDelimitedCodec::new());

        let mut heartbeat_timer = interval(Duration::from_secs(15));

        let mut event_stream = EventStream::new();

        loop {
            terminal.draw(|frame| self.render(frame))?;
            tokio::select! {
                _ = heartbeat_timer.tick() => {
                    if framed.send(protocol::encode(&ClientMessage::Ping)).await.is_err() {
                        break;
                    }
                }
                result = framed.next() => {
                    let Some(Ok(frame)) = result else {
                        break;
                    };

                    let Ok(message) = protocol::decode(&frame) else {
                        break;
                    };

                    match message {
                        ServerMessage::Chat { from, text } => {
                            self.messages.push(format!("<{from}> {text}"));
                        }
                        ServerMessage::Nick(new) => {
                            self.nickname = new;
                        }
                        ServerMessage::System(msg) => {
                            self.messages.push(format!("[server] {msg}"));
                        }
                        ServerMessage::Join(nick) => {
                            self.messages.push(format!("{nick} has joined"));
                        }
                        ServerMessage::Leave(nick) => {
                            self.messages.push(format!("{nick} has left"));
                        }
                    }
                }
                result = event_stream.next() => {
                    match result {
                        Some(Ok(Event::Key(key_event))) if key_event.kind == KeyEventKind::Press => {
                            match key_event.code {
                                KeyCode::Char(c) => {
                                    self.input.insert(self.cursor_index, c);
                                    self.cursor_right();
                                }
                                KeyCode::Backspace => {
                                    if self.cursor_index != 0 {
                                        self.input.remove(self.cursor_index - 1);
                                        self.cursor_left();
                                    }
                                }
                                KeyCode::Delete => {
                                    if self.input.len() > self.cursor_index {
                                        self.input.remove(self.cursor_index);
                                    }
                                }
                                KeyCode::Left => {
                                    self.cursor_left();
                                }
                                KeyCode::Right => {
                                    self.cursor_right();
                                }
                                KeyCode::Enter => {
                                    let line: String = self.input.iter().collect();

                                    self.input.clear();
                                    self.cursor_index = 0;

                                    if line.trim().is_empty() {
                                        continue;
                                    }

                                    if let Some(command) = line.strip_prefix('/') {
                                        let mut args = command.split_whitespace();

                                        match args.next() {
                                            Some("nick") => {
                                                let Some(nick) = args.next() else {
                                                    self.messages.push("Missing argument".to_owned());
                                                    continue;
                                                };

                                                if framed.send(protocol::encode(&ClientMessage::Nick(nick.to_owned()))).await.is_err() {
                                                    break;
                                                }
                                            }
                                            Some("exit" | "quit") => break,
                                            Some(cmd) => self.messages.push(format!("Unknown command: /{cmd}")),
                                            None => {}
                                        }

                                        continue;
                                    }

                                    if framed.send(protocol::encode(&ClientMessage::Text(line))).await.is_err() {
                                        break;
                                    }
                                }
                                _ => {}
                            }
                        }
                        Some(Err(_)) | None => break,
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    fn render(&self, frame: &mut Frame) {
        let [messages_area, input_area] =
            Layout::vertical([Constraint::Fill(1), Constraint::Length(3)]).areas(frame.area());

        // messages widget
        frame.render_widget(
            Paragraph::new(self.messages.iter().map(String::as_str).collect::<Text>())
                .scroll((
                    self.messages
                        .len()
                        .try_into()
                        .unwrap_or(u16::MAX)
                        .saturating_sub(messages_area.height.saturating_sub(2)),
                    0,
                ))
                .block(Block::bordered().title(Line::from(" Chat App ").bold().centered())),
            messages_area,
        );

        // input widget
        frame.render_widget(
            Paragraph::new(Line::from(format!(
                "> {}",
                self.input.iter().collect::<String>()
            )))
            .block(Block::bordered().title(Line::from(self.nickname.as_str()))),
            input_area,
        );

        frame.set_cursor_position(Position::new(
            input_area
                .x
                .saturating_add(self.cursor_index.try_into().unwrap_or(u16::MAX))
                .saturating_add(3),
            input_area.y + 1,
        ));
    }
}
