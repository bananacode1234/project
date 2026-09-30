use chat::protocol::{self, ClientMessage, ServerMessage};
use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use futures::{SinkExt, StreamExt};
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Layout, Position},
    style::{Style, Stylize},
    text::{Line, Text},
    widgets::{Block, Paragraph},
};
use tokio::{
    net::TcpStream,
    time::{Instant, interval},
};
use tokio_util::codec::Framed;

const MIN_WIDTH: u16 = 40;
const MIN_HEIGHT: u16 = 10;

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

    fn wrap_chars(s: &str, width: usize) -> Vec<Line<'static>> {
        let chars: Vec<char> = s.chars().collect();

        if chars.is_empty() {
            return vec![Line::default()];
        }

        chars
            .chunks(width.max(1))
            .map(|c| Line::from(c.iter().collect::<String>()))
            .collect()
    }

    async fn run(
        mut self,
        mut terminal: DefaultTerminal,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let addr = std::env::args()
            .nth(1)
            .unwrap_or_else(|| "127.0.0.1:8080".to_owned());

        let socket = TcpStream::connect(&addr).await?;
        let mut framed = Framed::new(socket, protocol::codec());

        let mut heartbeat_timer = interval(protocol::HEARTBEAT_INTERVAL);
        let mut last_seen = Instant::now();

        let mut event_stream = EventStream::new();

        loop {
            terminal.draw(|frame| self.render(frame))?;
            tokio::select! {
                _ = heartbeat_timer.tick() => {
                    if last_seen.elapsed() > protocol::HEARTBEAT_TIMEOUT {
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

                    last_seen = Instant::now();

                    match message {
                        ServerMessage::Chat { from, text } => {
                            self.messages.push(format!("<{from}> {text}"));
                        }
                        ServerMessage::Nick(new) => {
                            self.messages.push(format!("Your nickname has been changed to {new}"));
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
                        ServerMessage::Ping => {
                            if framed.send(protocol::encode(&ClientMessage::Pong)).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                result = event_stream.next() => {
                    match result {
                        Some(Ok(Event::Key(key_event))) if key_event.kind == KeyEventKind::Press => {
                            if key_event.modifiers.contains(KeyModifiers::CONTROL) && key_event.code == KeyCode::Char('c') {
                                break;
                            }

                            let size = terminal.size()?;
                            if size.width < MIN_WIDTH || size.height < MIN_HEIGHT {
                                continue;
                            }

                            if key_event.modifiers.contains(KeyModifiers::CONTROL) {
                                match key_event.code {
                                    KeyCode::Char('l') => self.messages.clear(),
                                    KeyCode::Char('u') => {
                                        self.input.clear();
                                        self.cursor_index = 0;
                                    }
                                    KeyCode::Char('a') => self.cursor_index = 0,
                                    KeyCode::Char('e') => self.cursor_index = self.input.len(),
                                    KeyCode::Char('k') => self.input.truncate(self.cursor_index),
                                    _ => {}
                                }

                                continue;
                            }

                            match key_event.code {
                                KeyCode::Char(c) => {
                                    if self.input.len() < protocol::MAX_TEXT_LEN && (c.is_ascii_graphic() || c == ' ') {
                                        self.input.insert(self.cursor_index, c);
                                        self.cursor_right();
                                    }
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
                                            Some("clear") => {
                                                self.messages.clear();
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
        let area = frame.area();

        if area.width < MIN_WIDTH || area.height < MIN_HEIGHT {
            let notice = Text::from(vec![
                Line::from("Terminal too small").bold(),
                Line::from(format!(
                    "Need {MIN_WIDTH}x{MIN_HEIGHT}, have {}x{}",
                    area.width, area.height,
                )),
                Line::from("Ctrl+C to exit").gray(),
            ]);

            frame.render_widget(
                Paragraph::new(notice).centered(),
                area.centered_vertically(Constraint::Length(3)),
            );

            return;
        }

        let inner_width = area.width.saturating_sub(2).max(1);

        // input widget
        let input_count = if self.input.len() * 10 >= protocol::MAX_TEXT_LEN * 9 {
            Line::from(format!("{}/{}", self.input.len(), protocol::MAX_TEXT_LEN)).style(
                if self.input.len() >= protocol::MAX_TEXT_LEN {
                    Style::new().red()
                } else {
                    Style::default()
                },
            )
        } else {
            Line::default()
        };

        let wrapped_input =
            App::wrap_chars(&self.input.iter().collect::<String>(), inner_width.into());

        let rows = wrapped_input.len().max(1).saturating_add(2) as u16;
        let input_height = rows.min(area.height / 2);

        let input_widget = Paragraph::new(wrapped_input).block(
            Block::bordered()
                .title(self.nickname.as_str())
                .title(input_count.right_aligned()),
        );

        // layout
        let [messages_area, input_area] =
            Layout::vertical([Constraint::Fill(1), Constraint::Length(input_height)]).areas(area);

        // messages widget
        let messages_height = messages_area.height.saturating_sub(2).max(1);

        let wrapped_messages = self
            .messages
            .iter()
            .flat_map(|s| App::wrap_chars(s, inner_width.into()))
            .collect::<Vec<Line>>();

        let first_visible = wrapped_messages
            .len()
            .saturating_sub(messages_height.into());

        let messages_widget = Paragraph::new(&wrapped_messages[first_visible..])
            .block(Block::bordered().title(Line::from(" Chat App ").bold().centered()));

        // render widgets
        frame.render_widget(messages_widget, messages_area);
        frame.render_widget(input_widget, input_area);

        // cursor positioning
        frame.set_cursor_position(Position::new(
            input_area.x + 1 + self.cursor_index as u16 % inner_width,
            input_area.y + 1 + self.cursor_index as u16 / inner_width,
        ));
    }
}
