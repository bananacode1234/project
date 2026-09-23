use chat::protocol::{self, Message};
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

    result?;

    Ok(())
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
            .unwrap_or(String::from("127.0.0.1:8080"));

        let socket = TcpStream::connect(&addr).await?;
        let mut framed = Framed::new(socket, LengthDelimitedCodec::new());

        let mut heartbeat_timer = interval(Duration::from_secs(15));

        let mut event_stream = EventStream::new();

        loop {
            terminal.draw(|frame| self.render(frame))?;
            tokio::select! {
                _ = heartbeat_timer.tick() => {
                    if framed.send(protocol::encode(Message::Ping)).await.is_err() {
                        break;
                    }
                }
                result = framed.next() => {
                    let Some(Ok(frame)) = result else {
                        break;
                    };

                    let Ok(message) = protocol::decode(frame.freeze()) else {
                        break;
                    };

                    match message {
                        Message::Ping => {},
                        Message::Text(msg) => {
                            self.messages.push(msg);
                        }
                        Message::Nick(new) => {
                            self.nickname = new;
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

                                                if framed.send(protocol::encode(Message::Nick(nick.to_owned()))).await.is_err() {
                                                    break;
                                                }
                                            }
                                            Some("exit") | Some("quit") => break,
                                            Some(cmd) => self.messages.push(format!("Unknown command: /{cmd}")),
                                            None => {},
                                        }

                                        continue;
                                    }

                                    if framed.send(protocol::encode(Message::Text(line))).await.is_err() {
                                        break;
                                    }
                                }
                                _ => {},
                            }
                        }
                        Some(Err(_)) | None => break,
                        _ => {},
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
            Paragraph::new(
                self.messages
                    .iter()
                    .map(|m| m.to_string())
                    .collect::<Text>(),
            )
            .scroll((
                self.messages
                    .len()
                    .try_into()
                    .unwrap_or(0_u16)
                    .saturating_sub(messages_area.height.saturating_sub(2)),
                0,
            ))
            .block(Block::bordered().title(Line::from(" Chat App ").bold().centered())),
            messages_area,
        );

        // input widget
        frame.render_widget(
            Paragraph::new(Line::from(self.input.iter().collect::<String>()))
                .block(Block::bordered().title(Line::from(self.nickname.clone()))),
            input_area,
        );

        frame.set_cursor_position(Position::new(
            input_area.x + self.cursor_index as u16 + 1,
            input_area.y + 1,
        ));
    }
}
