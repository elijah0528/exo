//! The excode terminal: layout, focus, and the async event loop.
//!
//! The app owns state and routes events; every pixel is drawn by a primitive
//! from [`super::ui`]. Adding a view means adding a component and one arm to
//! the dispatch below, not editing a monolithic draw function.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::event::{
    DisableMouseCapture, EnableMouseCapture, Event, EventStream, KeyCode, KeyEvent, KeyEventKind,
    KeyModifiers,
};
use excode::{CodingAgent, CodingAgentEvent, CodingResult};
use executor::RouterModelClient;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::{Line, Span};
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio_stream::StreamExt;

use super::command::{COMMANDS, Command, parse};
use super::dashboard::{DebugPanel, SandboxTable, SnapshotList};
use super::session::Session;
use super::ui::cells::{CommandCell, DiffCell, Level, NoteCell};
use super::ui::component::{Component, RenderCtx};
use super::ui::diff::DiffView;
use super::ui::overlay::render_overlay;
use super::ui::panel::Panel;
use super::ui::status::{KeyHint, Spinner, render_status_bar};
use super::ui::text_input::{InputEvent, TextInput};
use super::ui::theme::Theme;
use super::ui::transcript::Transcript;

type ChatTask = tokio::task::JoinHandle<Result<CodingResult>>;

const HINTS: &[KeyHint] = &[
    KeyHint::new("enter", "send"),
    KeyHint::new("/help", "commands"),
    KeyHint::new("ctrl-d", "diff"),
    KeyHint::new("ctrl-g", "debug"),
    KeyHint::new("ctrl-c", "quit"),
];

/// Which surface receives keys that the input does not claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Focus {
    Input,
    Transcript,
}

/// A modal surface drawn over the main view.
enum Overlay {
    Diff(DiffView),
    Help,
}

pub struct App {
    session: Session,
    theme: Theme,
    transcript: Transcript,
    input: TextInput,
    table: SandboxTable,
    focus: Focus,
    overlay: Option<Overlay>,
    debug: bool,
    spinner: Spinner,
    agent: CodingAgent<RouterModelClient>,
    chat_task: Option<ChatTask>,
    chat_events: Option<UnboundedReceiver<CodingAgentEvent>>,
    thinking_since: Option<Instant>,
    streaming: String,
    should_quit: bool,
}

impl App {
    pub fn new(session: Session, agent: CodingAgent<RouterModelClient>, debug: bool) -> Self {
        let mut app = Self {
            session,
            theme: Theme::dark(),
            transcript: Transcript::new(),
            input: TextInput::new("› "),
            table: SandboxTable::new(),
            focus: Focus::Input,
            overlay: None,
            debug,
            spinner: Spinner::new(),
            agent,
            chat_task: None,
            chat_events: None,
            thinking_since: None,
            streaming: String::new(),
            should_quit: false,
        };
        app.note(Level::Info, "excode ready — /help lists commands");
        if !app.session.has_snapshot_store() {
            app.note(
                Level::Muted,
                "local-process pool: snapshots are in-memory only",
            );
        }
        app
    }

    pub async fn run(mut self) -> Result<()> {
        self.session.refresh().await?;
        let mut terminal = ratatui::init();
        crossterm::execute!(std::io::stdout(), EnableMouseCapture)?;
        let mut events = EventStream::new();
        let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
        let mut refresh = tokio::time::interval(Duration::from_secs(1));
        let result = async {
            loop {
                self.table.update(&self.session.rows(), &self.theme);
                terminal.draw(|frame| self.draw(frame))?;
                if self.should_quit {
                    break Ok(());
                }
                tokio::select! {
                    chat = Self::next_chat_result(&mut self.chat_task) => self.finish_chat(chat),
                    event = Self::next_chat_event(&mut self.chat_events) => {
                        if let Some(event) = event {
                            self.on_chat_event(event);
                        }
                    }
                    event = events.next() => {
                        let Some(event) = event else { break Ok(()); };
                        match event? {
                            Event::Key(key) if key.kind == KeyEventKind::Press => {
                                self.on_key(key).await?;
                            }
                            Event::Mouse(mouse) => {
                                self.transcript.handle_mouse(mouse);
                            }
                            _ => {}
                        }
                    }
                    _ = heartbeat.tick() => {
                        for failure in self.session.heartbeat().await {
                            self.note(Level::Error, failure);
                        }
                    }
                    _ = refresh.tick() => self.session.refresh().await?,
                }
            }
        }
        .await;
        crossterm::execute!(std::io::stdout(), DisableMouseCapture)?;
        ratatui::restore();
        result
    }

    fn draw(&self, frame: &mut ratatui::Frame) {
        let area = frame.area();
        let debug_height = if self.debug { 8 } else { 0 };
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(6),
                Constraint::Length(debug_height),
                Constraint::Length(3),
                Constraint::Length(1),
            ])
            .split(area);
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Min(40), Constraint::Length(60)])
            .split(rows[0]);
        let sidebar = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
            .split(body[1]);

        let ctx = RenderCtx::new(&self.theme);
        let buf = frame.buffer_mut();

        let transcript_panel = Panel::new(self.transcript_title());
        let transcript_area = transcript_panel.inner(body[0]);
        transcript_panel.render(body[0], buf, ctx.focused(self.focus == Focus::Transcript));
        self.transcript.render(transcript_area, buf, ctx);

        let table_panel = Panel::new(Line::from(Span::styled("Sandboxes", self.theme.title)));
        let table_area = table_panel.inner(sidebar[0]);
        table_panel.render(sidebar[0], buf, ctx);
        self.table.render(table_area, buf, ctx);

        let snapshot_panel = Panel::new(Line::from(Span::styled("Snapshots", self.theme.title)));
        let snapshot_area = snapshot_panel.inner(sidebar[1]);
        snapshot_panel.render(sidebar[1], buf, ctx);
        SnapshotList::new(self.session.snapshots(), self.session.has_snapshot_store()).render(
            snapshot_area,
            buf,
            ctx,
        );

        if self.debug {
            let debug_panel = Panel::new(Line::from(Span::styled(
                "Sandbox utilization",
                self.theme.title,
            )));
            let debug_area = debug_panel.inner(rows[1]);
            debug_panel.render(rows[1], buf, ctx);
            DebugPanel::new(
                self.session.utilization(),
                self.session.detached_count(),
                self.session.is_dirty(),
            )
            .render(debug_area, buf, ctx);
        }

        let input_panel = Panel::new(Line::from(Span::styled(
            self.input_title(),
            self.theme.accent,
        )));
        let input_area = input_panel.inner(rows[2]);
        input_panel.render(rows[2], buf, ctx.focused(self.focus == Focus::Input));
        self.input.render(input_area, buf, ctx);

        render_status_bar(rows[3], buf, ctx, self.status_line(), HINTS);

        match &self.overlay {
            Some(Overlay::Diff(view)) => {
                let (added, removed) = view.stats();
                render_overlay(
                    area,
                    buf,
                    ctx,
                    Line::from(vec![
                        Span::styled("Diff  ", self.theme.title),
                        Span::styled(format!("{} file(s)  ", view.files()), self.theme.dim),
                        Span::styled(format!("+{added} "), self.theme.added),
                        Span::styled(format!("-{removed}"), self.theme.removed),
                    ]),
                    Some(Line::from(Span::styled(
                        " esc close   ↑↓ pgup/pgdn scroll ",
                        self.theme.dim,
                    ))),
                    /*width_pct*/ 90,
                    /*height_pct*/ 85,
                    view,
                );
            }
            Some(Overlay::Help) => render_overlay(
                area,
                buf,
                ctx,
                Line::from(Span::styled("Commands", self.theme.title)),
                Some(Line::from(Span::styled(" esc close ", self.theme.dim))),
                /*width_pct*/ 60,
                /*height_pct*/ 60,
                &HelpView,
            ),
            None => {}
        }

        if self.overlay.is_none()
            && self.focus == Focus::Input
            && let Some((x, y)) = self.input.cursor(input_area)
        {
            frame.set_cursor_position((x, y));
        }
    }

    fn transcript_title(&self) -> Line<'static> {
        let mut spans = vec![Span::styled("Session", self.theme.title)];
        if let Some(started) = self.thinking_since {
            spans.push(Span::styled(
                format!(
                    "  {} thinking {}s",
                    self.spinner.frame(),
                    started.elapsed().as_secs()
                ),
                self.theme.warn,
            ));
        }
        if !self.transcript.is_following_tail() {
            spans.push(Span::styled("  scrolled", self.theme.dim));
        }
        Line::from(spans)
    }

    fn input_title(&self) -> String {
        match self.session.active() {
            Some(active) => format!("attached {}", super::session::short_id(active.sandbox.id())),
            None => "no sandbox attached".to_string(),
        }
    }

    fn status_line(&self) -> Line<'static> {
        let used = self.session.utilization();
        Line::from(vec![
            Span::styled(
                format!(" ready {}/{} ", used.ready, used.warm_size),
                self.theme.success,
            ),
            Span::styled(
                format!("leased {}/{} ", used.leased, used.max_total),
                self.theme.warn,
            ),
            Span::styled(
                if self.session.is_dirty() {
                    "dirty"
                } else {
                    "clean"
                },
                self.theme.dim,
            ),
        ])
    }

    async fn on_key(&mut self, key: KeyEvent) -> Result<()> {
        let control = key.modifiers.contains(KeyModifiers::CONTROL);
        if control && key.code == KeyCode::Char('c') {
            self.cancel_or_quit();
            return Ok(());
        }
        if let Some(overlay) = self.overlay.as_mut() {
            match key.code {
                KeyCode::Esc | KeyCode::Char('q') => self.overlay = None,
                _ => {
                    if let Overlay::Diff(view) = overlay {
                        view.handle_key(key);
                    }
                }
            }
            return Ok(());
        }
        if control {
            match key.code {
                KeyCode::Char('d') => return self.show_diff().await,
                KeyCode::Char('g') => {
                    self.debug = !self.debug;
                    return Ok(());
                }
                _ => {}
            }
        }
        match key.code {
            KeyCode::Tab => {
                self.focus = match self.focus {
                    Focus::Input => Focus::Transcript,
                    Focus::Transcript => Focus::Input,
                };
                return Ok(());
            }
            KeyCode::PageUp | KeyCode::PageDown => {
                self.transcript.handle_key(key);
                return Ok(());
            }
            _ => {}
        }
        if self.focus == Focus::Transcript {
            self.transcript.handle_key(key);
            return Ok(());
        }
        if let InputEvent::Submitted(line) = self.input.input(key) {
            self.dispatch(parse(&line)).await?;
        }
        Ok(())
    }

    /// Ctrl-C cancels a running agent turn first, and only then exits.
    fn cancel_or_quit(&mut self) {
        match self.chat_task.take() {
            Some(task) => {
                task.abort();
                self.thinking_since = None;
                self.chat_events = None;
                self.note(Level::Warn, "agent request cancelled");
            }
            None => self.should_quit = true,
        }
    }

    async fn dispatch(&mut self, command: Command) -> Result<()> {
        match command {
            Command::Shell(command) if command.is_empty() => {}
            Command::Shell(command) => match self.session.exec(command).await {
                Ok(output) => self.transcript.push(CommandCell::new(
                    output.command,
                    output.output,
                    output.exit_code,
                )),
                Err(error) => self.note(Level::Error, format!("{error:#}")),
            },
            Command::Chat(prompt) => self.start_chat(prompt),
            Command::Acquire => match self.session.acquire().await {
                Ok(id) => self.note(Level::Success, format!("attached {id}")),
                Err(error) => self.note(Level::Error, format!("{error:#}")),
            },
            Command::Detach => match self.session.detach() {
                Ok(entry) => self.note(
                    Level::Info,
                    format!("detached {entry}; the lease stays claimed"),
                ),
                Err(error) => self.note(Level::Error, format!("{error:#}")),
            },
            Command::Release => match self.session.release().await {
                Ok(Some(snapshot)) => {
                    self.note(Level::Success, format!("released; checkpoint {snapshot}"))
                }
                Ok(None) => self.note(Level::Success, "released; no changes to checkpoint"),
                Err(error) => self.note(Level::Error, format!("{error:#}")),
            },
            Command::Diff => self.show_diff().await?,
            Command::Restore(number) => match self.session.restore(number).await {
                Ok(message) => self.note(Level::Success, message),
                Err(error) => self.note(Level::Error, format!("{error:#}")),
            },
            Command::Debug => self.debug = !self.debug,
            Command::Clear => self.transcript.clear(),
            Command::Help => self.overlay = Some(Overlay::Help),
            Command::Quit => self.should_quit = true,
            Command::Unknown(name) => {
                self.note(Level::Error, format!("unknown command /{name}; try /help"))
            }
            Command::MissingArgument(usage) => self.note(Level::Warn, format!("usage: {usage}")),
        }
        Ok(())
    }

    async fn show_diff(&mut self) -> Result<()> {
        match self.session.diff().await {
            Ok(diff) if diff.trim().is_empty() => {
                self.note(Level::Info, "no changes in the sandbox working tree");
            }
            Ok(diff) => {
                let cell = DiffCell::from_unified(&diff);
                if cell.is_empty() {
                    self.note(Level::Info, "no changes in the sandbox working tree");
                } else {
                    self.transcript.push(cell);
                    self.overlay = Some(Overlay::Diff(DiffView::from_unified(&diff)));
                }
            }
            Err(error) => self.note(Level::Error, format!("{error:#}")),
        }
        Ok(())
    }

    fn start_chat(&mut self, prompt: String) {
        if self.chat_task.is_some() {
            self.note(Level::Warn, "an agent request is already running");
            return;
        }
        let Some(active) = self.session.active() else {
            self.note(Level::Error, "attach a sandbox before chatting (/acquire)");
            return;
        };
        let agent = self.agent.clone();
        let lease = active.lease.clone();
        let sandbox = Arc::clone(&active.sandbox);
        let (events, receiver) = mpsc::unbounded_channel();
        self.transcript
            .push(NoteCell::new(Level::Info, format!("you: {prompt}")));
        self.thinking_since = Some(Instant::now());
        self.chat_events = Some(receiver);
        self.streaming.clear();
        self.chat_task = Some(tokio::spawn(async move {
            agent
                .run_on_lease_streaming(&lease, sandbox.as_ref(), &prompt, events)
                .await
        }));
    }

    fn on_chat_event(&mut self, event: CodingAgentEvent) {
        match event {
            CodingAgentEvent::TextChunk(text) => self.streaming.push_str(&text),
            CodingAgentEvent::ToolCall { name } => {
                self.flush_streaming();
                self.note(Level::Muted, format!("tool {name}"));
                self.session.mark_dirty();
            }
            CodingAgentEvent::ToolResult { name, output } => {
                self.transcript
                    .push(CommandCell::new(name, output, Some(0)));
            }
        }
    }

    fn flush_streaming(&mut self) {
        let text = std::mem::take(&mut self.streaming);
        if !text.trim().is_empty() {
            self.transcript
                .push(NoteCell::new(Level::Success, text.trim_end().to_string()));
        }
    }

    fn finish_chat(
        &mut self,
        result: Option<std::result::Result<Result<CodingResult>, tokio::task::JoinError>>,
    ) {
        let Some(result) = result else {
            return;
        };
        self.drain_chat_events();
        self.thinking_since = None;
        self.chat_events = None;
        match result {
            Ok(Ok(result)) => {
                let streamed = !self.streaming.trim().is_empty();
                self.flush_streaming();
                if !streamed && !result.response.trim().is_empty() {
                    self.transcript.push(NoteCell::new(
                        Level::Success,
                        result.response.trim_end().to_string(),
                    ));
                }
                if !result.tools.is_empty() {
                    self.session.mark_dirty();
                }
                self.note(
                    Level::Muted,
                    format!("agent completed in {} round(s)", result.rounds),
                );
            }
            Ok(Err(error)) => self.note(Level::Error, format!("agent failed: {error:#}")),
            Err(error) => self.note(Level::Error, format!("agent task failed: {error}")),
        }
    }

    fn drain_chat_events(&mut self) {
        while let Some(receiver) = self.chat_events.as_mut() {
            match receiver.try_recv() {
                Ok(event) => self.on_chat_event(event),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.chat_events = None;
                    break;
                }
            }
        }
    }

    fn note(&mut self, level: Level, text: impl Into<String>) {
        self.transcript.push(NoteCell::new(level, text));
    }

    /// Resolves only once a chat task finishes; pends forever otherwise so it
    /// can sit in `select!` without busy-looping.
    async fn next_chat_result(
        task: &mut Option<ChatTask>,
    ) -> Option<std::result::Result<Result<CodingResult>, tokio::task::JoinError>> {
        let Some(task) = task.take() else {
            return std::future::pending().await;
        };
        Some(task.await)
    }

    async fn next_chat_event(
        receiver: &mut Option<UnboundedReceiver<CodingAgentEvent>>,
    ) -> Option<CodingAgentEvent> {
        let Some(mut current) = receiver.take() else {
            return std::future::pending().await;
        };
        match current.recv().await {
            Some(event) => {
                *receiver = Some(current);
                Some(event)
            }
            None => std::future::pending().await,
        }
    }
}

/// Static list of slash commands shown by `/help`.
struct HelpView;

impl Component for HelpView {
    fn render(&self, area: Rect, buf: &mut ratatui::buffer::Buffer, ctx: RenderCtx<'_>) {
        for (row, (name, description)) in COMMANDS.iter().take(usize::from(area.height)).enumerate()
        {
            let line = Line::from(vec![
                Span::styled(format!("{name:<18}"), ctx.theme.key),
                Span::styled(*description, ctx.theme.dim),
            ]);
            buf.set_line(area.x, area.y + row as u16, &line, area.width);
        }
    }
}
