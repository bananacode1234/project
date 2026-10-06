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
use std::collections::VecDeque;
use tokio::{
    net::TcpStream,
    time::{Instant, interval},
};
use tokio_util::codec::Framed;

const MIN_WIDTH: u16 = 40;
const MIN_HEIGHT: u16 = 10;

const MAX_MESSAGES: usize = 1000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let terminal = ratatui::init();

    let result = App::new().run(terminal).await;

    ratatui::restore();

    result
}

struct App {
    messages: VecDeque<String>,
    input: Vec<char>,
    cursor_index: usize,
    nickname: String,
}

impl App {
    const fn new() -> Self {
        Self {
            messages: VecDeque::new(),
            input: Vec::new(),
            cursor_index: 0,
            nickname: String::new(),
        }
    }

    fn push_message(&mut self, msg: impl Into<String>) {
        if self.messages.len() >= MAX_MESSAGES {
            self.messages.pop_front();
        }

        self.messages.push_back(msg.into());
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
                            self.push_message(format!("<{from}> {text}"));
                        }
                        ServerMessage::Nick(new) => {
                            self.push_message(format!("* Your nickname has been changed to {new}"));
                            self.nickname = new;
                        }
                        ServerMessage::System(msg) => {
                            self.push_message(format!("* {msg}"));
                        }
                        ServerMessage::Welcome(nick) => {
                            self.push_message(format!("* Welcome, {nick}. Type /help for commands."));
                            self.nickname = nick;
                        }
                        ServerMessage::Join(nick) => {
                            self.push_message(format!("* {nick} has joined"));
                        }
                        ServerMessage::Leave(nick) => {
                            self.push_message(format!("* {nick} has left"));
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

                            let terminal_size = terminal.size()?;
                            if terminal_size.width < MIN_WIDTH || terminal_size.height < MIN_HEIGHT {
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
                                    KeyCode::Char('w') | KeyCode::Backspace => {
                                        let start = word_start_before(&self.input, self.cursor_index);

                                        self.input.drain(start..self.cursor_index);
                                        self.cursor_index = start;
                                    }
                                    KeyCode::Delete => {
                                        let end = word_end_after(&self.input, self.cursor_index);

                                        self.input.drain(self.cursor_index..end);
                                    }
                                    KeyCode::Left => self.cursor_index = word_start_before(&self.input, self.cursor_index),
                                    KeyCode::Right => self.cursor_index = word_end_after(&self.input, self.cursor_index),
                                    _ => {}
                                }

                                continue;
                            }

                            if key_event.modifiers.contains(KeyModifiers::ALT) {
                                match key_event.code {
                                    KeyCode::Backspace => {
                                        let start = word_start_before(&self.input, self.cursor_index);

                                        self.input.drain(start..self.cursor_index);
                                        self.cursor_index = start;
                                    }
                                    KeyCode::Char('d') => {
                                        let end = word_end_after(&self.input, self.cursor_index);

                                        self.input.drain(self.cursor_index..end);
                                    }
                                    KeyCode::Char('b') | KeyCode::Left => self.cursor_index = word_start_before(&self.input, self.cursor_index),
                                    KeyCode::Char('f') | KeyCode::Right => self.cursor_index = word_end_after(&self.input, self.cursor_index),
                                    _ => {}
                                }

                                continue;
                            }

                            match key_event.code {
                                KeyCode::Char(c) => {
                                    if self.input.len() < protocol::MAX_TEXT_LEN && protocol::is_valid_text_char(c) {
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
                                KeyCode::Home => self.cursor_index = 0,
                                KeyCode::End => self.cursor_index = self.input.len(),
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
                                            Some("help") => {
                                                for line in [
                                                    "Commands:",
                                                    "  /nick <name>  change your nickname",
                                                    "  /clear        clear the screen (Ctrl+L)",
                                                    "  /exit         exit the app (Ctrl+C)",
                                                    "  /help         show this message",
                                                ] { self.push_message(line) }
                                            }
                                            Some("nick") => {
                                                let Some(nick) = args.next() else {
                                                    self.push_message("* Usage: /nick <name>");
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
                                            Some(cmd) => self.push_message(format!("* Unknown command: /{cmd} (try /help)")),
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

        let inner_width = area.width - 2;

        // input widget
        let length_indicator = if self.input.len() * 10 >= protocol::MAX_TEXT_LEN * 9 {
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

        let input_lines = wrap_chars(
            &format!("> {}", self.input.iter().collect::<String>()),
            inner_width.into(),
        );

        let cursor_col = (self.cursor_index as u16 + 2) % inner_width;
        let cursor_row = (self.cursor_index as u16 + 2) / inner_width;

        let input_rows = (input_lines.len() as u16).max(cursor_row + 1);
        let input_height = (input_rows + 2).min(area.height / 2);
        let visible_input_rows = input_height - 2;

        let input_scroll = (cursor_row + 1).saturating_sub(visible_input_rows);

        let input_widget = Paragraph::new(input_lines)
            .block(
                Block::bordered()
                    .title(self.nickname.as_str())
                    .title(length_indicator.right_aligned()),
            )
            .scroll((input_scroll, 0));

        // layout
        let [messages_area, input_area] =
            Layout::vertical([Constraint::Fill(1), Constraint::Length(input_height)]).areas(area);

        // messages widget
        let visible_message_rows = messages_area.height - 2;

        let messages_lines = self
            .messages
            .iter()
            .flat_map(|s| wrap_chars(s, inner_width.into()))
            .collect::<Vec<Line>>();

        let first_visible_line = messages_lines
            .len()
            .saturating_sub(visible_message_rows.into());

        let messages_widget = Paragraph::new(&messages_lines[first_visible_line..])
            .block(Block::bordered().title(Line::from(" Chat App ").bold().centered()));

        // render widgets
        frame.render_widget(messages_widget, messages_area);
        frame.render_widget(input_widget, input_area);

        // cursor positioning
        frame.set_cursor_position(Position::new(
            input_area.x + 1 + cursor_col,
            input_area.y + 1 + cursor_row - input_scroll,
        ));
    }
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

fn word_start_before(input: &[char], index: usize) -> usize {
    let mut i = index;

    while i > 0 && input[i - 1] == ' ' {
        i -= 1;
    }

    while i > 0 && input[i - 1] != ' ' {
        i -= 1;
    }

    i
}

fn word_end_after(input: &[char], index: usize) -> usize {
    let mut i = index;
    let len = input.len();

    while i < len && input[i] == ' ' {
        i += 1;
    }

    while i < len && input[i] != ' ' {
        i += 1;
    }

    i
}
