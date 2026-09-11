//! The excode terminal: layout, focus, and the async event loop.
//!
//! The app owns state and routes events; every pixel is drawn by a primitive
//! from [`super::ui`]. Adding a view means adding a component and one arm to
//! the dispatch below, not editing a monolithic draw function.

use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::event::{
    DisableMouseCapture, EnableMouseCapture, Event, EventStream, KeyCode, KeyEvent, KeyEventKind,
    KeyModifiers,
};
use excode::{CodingAgent, CodingAgentEvent, CodingResult};
use executor::RouterModelClient;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::{Line, Span};
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio_stream::StreamExt;

use super::clipboard;
use super::command::{COMMANDS, Command, parse};
use super::dashboard::SnapshotList;
use super::session::Session;
use super::ui::activity::Activity;
use super::ui::cells::{
    AssistantCell, CommandCell, DiffCell, Level, NoteCell, StreamingCell, ToolCell, ToolState,
    UserCell,
};
use super::ui::component::{Component, RenderCtx};
use super::ui::diff::DiffView;
use super::ui::overlay::render_overlay;
use super::ui::panel::Panel;
use super::ui::status::{KeyHint, render_status_bar};
use super::ui::text_input::{InputEvent, TextInput};
use super::ui::theme::Theme;
use super::ui::transcript::Transcript;

type ChatTask = tokio::task::JoinHandle<Result<CodingResult>>;

const HINTS: &[KeyHint] = &[
    KeyHint::new("enter", "send"),
    KeyHint::new("ctrl-o", "copy"),
    KeyHint::new("alt-r", "select"),
    KeyHint::new("/help", "commands"),
    KeyHint::new("ctrl-d", "diff"),
    KeyHint::new("ctrl-c", "quit"),
];

const HORIZONTAL_PADDING: u16 = 3;
const TOP_PADDING: u16 = 2;
const BOTTOM_PADDING: u16 = 1;

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
    Snapshots,
}

pub struct App {
    session: Session,
    theme: Theme,
    transcript: Transcript,
    input: TextInput,
    activity: Option<Activity>,
    focus: Focus,
    overlay: Option<Overlay>,
    agent: CodingAgent<RouterModelClient>,
    chat_task: Option<ChatTask>,
    chat_events: Option<UnboundedReceiver<CodingAgentEvent>>,
    streaming_cell: Option<Rc<RefCell<String>>>,
    streamed_response: bool,
    active_tool: Option<Rc<RefCell<ToolState>>>,
    latest_response: Option<String>,
    last_frame: Option<Buffer>,
    mouse_capture: bool,
    should_quit: bool,
}

impl App {
    pub fn new(session: Session, agent: CodingAgent<RouterModelClient>) -> Self {
        let mut app = Self {
            session,
            theme: Theme::dark(),
            transcript: Transcript::new(),
            input: TextInput::new("› "),
            activity: None,
            focus: Focus::Input,
            overlay: None,
            agent,
            chat_task: None,
            chat_events: None,
            streaming_cell: None,
            streamed_response: false,
            active_tool: None,
            latest_response: None,
            last_frame: None,
            mouse_capture: true,
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
        let mut ui_tick = tokio::time::interval(Duration::from_millis(16));
        let result = async {
            loop {
                self.redraw(&mut terminal)?;
                if self.should_quit {
                    break Ok(());
                }
                tokio::select! {
                    chat = Self::next_chat_result(&mut self.chat_task) => self.finish_chat(chat),
                    event = Self::next_chat_event(&mut self.chat_events) => {
                        if let Some(event) = event {
                            self.on_chat_event(event);
                        } else {
                            self.chat_events = None;
                        }
                    }
                    event = events.next() => {
                        let Some(event) = event else { break Ok(()); };
                        match event? {
                            Event::Key(key) if key.kind == KeyEventKind::Press => {
                                self.on_key(key, &mut terminal).await?;
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
                    _ = ui_tick.tick() => self.transcript.tick(),
                }
            }
        }
        .await;
        crossterm::execute!(std::io::stdout(), DisableMouseCapture)?;
        ratatui::restore();
        result
    }

    fn draw(&self, frame: &mut ratatui::Frame) {
        let terminal_area = frame.area();
        let area = content_area(terminal_area);
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(8),
                Constraint::Length(1),
                Constraint::Length(3),
                Constraint::Length(1),
            ])
            .split(area);
        let ctx = RenderCtx::new(&self.theme);
        let buf = frame.buffer_mut();

        self.transcript.render(rows[0], buf, ctx);

        if let Some(activity) = &self.activity {
            activity.render(rows[1], buf, ctx);
        }
        let input_area = Panel::new(Line::from(Span::styled(
            " message or /command ",
            self.theme.title,
        )))
        .footer(Line::from(Span::styled(" enter send ", self.theme.dim)))
        .render(rows[2], buf, ctx.focused(self.focus == Focus::Input));
        self.input
            .render(input_area, buf, ctx.focused(self.focus == Focus::Input));

        render_status_bar(rows[3], buf, ctx, self.status_line(), HINTS);

        match &self.overlay {
            Some(Overlay::Diff(view)) => {
                let (added, removed) = view.stats();
                render_overlay(
                    terminal_area,
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
                terminal_area,
                buf,
                ctx,
                Line::from(Span::styled("Commands", self.theme.title)),
                Some(Line::from(Span::styled(" esc close ", self.theme.dim))),
                /*width_pct*/ 60,
                /*height_pct*/ 60,
                &HelpView,
            ),
            Some(Overlay::Snapshots) => render_overlay(
                terminal_area,
                buf,
                ctx,
                Line::from(Span::styled("Snapshots", self.theme.title)),
                Some(Line::from(Span::styled(
                    " esc close   /c [#] connect ",
                    self.theme.dim,
                ))),
                78,
                68,
                &SnapshotList::new(self.session.snapshots(), self.session.has_snapshot_store()),
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

    fn redraw(&mut self, terminal: &mut ratatui::DefaultTerminal) -> Result<()> {
        let frame = terminal.draw(|frame| self.draw(frame))?;
        self.last_frame = Some(frame.buffer.clone());
        Ok(())
    }

    fn status_line(&self) -> Line<'static> {
        let connection = if self.session.active().is_some() {
            "connected"
        } else {
            "disconnected"
        };
        Line::from(vec![
            Span::styled(format!(" {connection} "), self.theme.accent),
            Span::styled(
                format!("snapshots {} ", self.session.snapshots().len()),
                self.theme.dim,
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

    async fn on_key(
        &mut self,
        key: KeyEvent,
        terminal: &mut ratatui::DefaultTerminal,
    ) -> Result<()> {
        let control = key.modifiers.contains(KeyModifiers::CONTROL);
        if control && key.code == KeyCode::Char('c') {
            self.cancel_or_quit();
            return Ok(());
        }
        if key.modifiers.contains(KeyModifiers::ALT) && key.code == KeyCode::Char('r') {
            self.toggle_selection_mode();
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
                KeyCode::Char('o') => {
                    self.copy_latest_response();
                    return Ok(());
                }
                KeyCode::Char('d') => return self.show_diff().await,
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
            self.dispatch(parse(&line), terminal).await?;
        }
        Ok(())
    }

    /// Ctrl-C cancels a running agent turn first, and only then exits.
    fn cancel_or_quit(&mut self) {
        match self.chat_task.take() {
            Some(task) => {
                task.abort();
                self.chat_events = None;
                self.activity = None;
                self.streaming_cell = None;
                self.streamed_response = false;
                self.active_tool = None;
                self.note(Level::Warn, "agent request cancelled");
            }
            None => self.should_quit = true,
        }
    }

    async fn dispatch(
        &mut self,
        command: Command,
        terminal: &mut ratatui::DefaultTerminal,
    ) -> Result<()> {
        match command {
            Command::Shell(command) if command.is_empty() => {}
            Command::Shell(command) => {
                self.begin_activity("Running command", terminal)?;
                let activity = self.activity.as_ref();
                let theme = &self.theme;
                let base = self
                    .last_frame
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("missing rendered terminal frame"))?;
                let result = await_with_activity(
                    terminal,
                    activity,
                    theme,
                    base,
                    self.session.exec(command),
                )
                .await;
                self.activity = None;
                match result {
                    Ok(output) => self.transcript.push(CommandCell::new(
                        output.command,
                        output.output,
                        output.exit_code,
                    )),
                    Err(error) => self.note(Level::Error, format!("{error:#}")),
                }
            }
            Command::Chat(prompt) => self.start_chat(prompt),
            Command::Connect(number) => {
                let pending = self.session.prepare_connect(number)?;
                self.begin_activity("Connecting to snapshot", terminal)?;
                let activity = self.activity.as_ref();
                let theme = &self.theme;
                let base = self
                    .last_frame
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("missing rendered terminal frame"))?;
                let result = await_with_activity(terminal, activity, theme, base, async move {
                    tokio::spawn(pending.run()).await.map_err(|error| {
                        anyhow::anyhow!("snapshot connection task failed: {error}")
                    })?
                })
                .await;
                self.activity = None;
                match result {
                    Ok(connected) => match self.session.finish_connect(connected).await {
                        Ok(message) => self.note(Level::Success, message),
                        Err(error) => self.note(Level::Error, format!("{error:#}")),
                    },
                    Err(error) => self.note(Level::Error, format!("{error:#}")),
                }
            }
            Command::Disconnect => {
                self.begin_activity("Checkpointing snapshot", terminal)?;
                let activity = self.activity.as_ref();
                let theme = &self.theme;
                let base = self
                    .last_frame
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("missing rendered terminal frame"))?;
                let result =
                    await_with_activity(terminal, activity, theme, base, self.session.disconnect())
                        .await;
                self.activity = None;
                match result {
                    Ok(Some(snapshot)) => self.note(
                        Level::Success,
                        format!("disconnected; checkpoint {snapshot}"),
                    ),
                    Ok(None) => self.note(Level::Success, "disconnected; no changes to checkpoint"),
                    Err(error) => self.note(Level::Error, format!("{error:#}")),
                }
            }
            Command::Diff => self.show_diff().await?,
            Command::Snapshots => self.overlay = Some(Overlay::Snapshots),
            Command::Copy => self.copy_latest_response(),
            Command::ToggleRaw => self.toggle_selection_mode(),
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
            self.note(Level::Error, "connect a snapshot before chatting (/c)");
            return;
        };
        let agent = self.agent.clone();
        let lease = active.lease.clone();
        let sandbox = Arc::clone(&active.sandbox);
        let (events, receiver) = mpsc::unbounded_channel();
        self.transcript.push(UserCell::new(prompt.clone()));
        self.activity = Some(Activity::new("Thinking"));
        self.chat_events = Some(receiver);
        self.streaming_cell = None;
        self.streamed_response = false;
        self.active_tool = None;
        self.chat_task = Some(tokio::spawn(async move {
            agent
                .run_on_lease_streaming(&lease, sandbox.as_ref(), &prompt, events)
                .await
        }));
    }

    fn on_chat_event(&mut self, event: CodingAgentEvent) {
        match event {
            CodingAgentEvent::TextChunk(text) => {
                let streaming = match &self.streaming_cell {
                    Some(streaming) => Rc::clone(streaming),
                    None => {
                        let streaming = Rc::new(RefCell::new(String::new()));
                        self.transcript
                            .push(StreamingCell::new(Rc::clone(&streaming)));
                        self.streaming_cell = Some(Rc::clone(&streaming));
                        streaming
                    }
                };
                streaming.borrow_mut().push_str(&text);
                self.streamed_response = true;
                self.transcript.invalidate();
            }
            CodingAgentEvent::ToolCall { name } => {
                self.streaming_cell = None;
                let state = Rc::new(RefCell::new(ToolState::running(name)));
                self.transcript.push(ToolCell::new(Rc::clone(&state)));
                self.active_tool = Some(state);
                self.session.mark_dirty();
            }
            CodingAgentEvent::ToolResult { name, output } => {
                if let Some(tool) = self.active_tool.take() {
                    tool.borrow_mut().complete(output);
                    self.transcript.invalidate();
                } else {
                    let state = Rc::new(RefCell::new(ToolState::running(name)));
                    state.borrow_mut().complete(output);
                    self.transcript.push(ToolCell::new(state));
                }
            }
        }
    }

    fn finish_chat(
        &mut self,
        result: Option<std::result::Result<Result<CodingResult>, tokio::task::JoinError>>,
    ) {
        let Some(result) = result else {
            return;
        };
        self.chat_task = None;
        self.drain_chat_events();
        let streamed = self.streamed_response;
        let streamed_text = self
            .streaming_cell
            .as_ref()
            .map(|text| text.borrow().clone());
        self.activity = None;
        self.chat_events = None;
        self.streaming_cell = None;
        self.streamed_response = false;
        self.active_tool = None;
        match result {
            Ok(Ok(result)) => {
                let response = if streamed {
                    streamed_text.unwrap_or_default()
                } else {
                    result.response.clone()
                };
                if !response.trim().is_empty() {
                    self.latest_response = Some(response);
                }
                if !streamed && !result.response.trim().is_empty() {
                    self.transcript
                        .push(AssistantCell::new(result.response.trim_end()));
                }
                if !result.tools.is_empty() {
                    self.session.mark_dirty();
                }
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

    fn copy_latest_response(&mut self) {
        let Some(response) = self.latest_response.as_deref() else {
            self.note(Level::Warn, "no agent response to copy yet");
            return;
        };
        match clipboard::copy(response) {
            Ok(()) => self.note(Level::Success, "copied latest agent response"),
            Err(error) => self.note(Level::Error, format!("copy failed: {error:#}")),
        }
    }

    fn toggle_selection_mode(&mut self) {
        let result = if self.mouse_capture {
            crossterm::execute!(std::io::stdout(), DisableMouseCapture)
        } else {
            crossterm::execute!(std::io::stdout(), EnableMouseCapture)
        };
        match result {
            Ok(()) => {
                self.mouse_capture = !self.mouse_capture;
                if self.mouse_capture {
                    self.note(
                        Level::Info,
                        "terminal selection mode off; mouse scrolling enabled",
                    );
                } else {
                    self.note(
                        Level::Info,
                        "terminal selection mode on; drag to highlight and copy",
                    );
                }
            }
            Err(error) => self.note(
                Level::Error,
                format!("could not change selection mode: {error}"),
            ),
        }
    }

    fn begin_activity(
        &mut self,
        label: impl Into<String>,
        terminal: &mut ratatui::DefaultTerminal,
    ) -> Result<()> {
        self.activity = Some(Activity::new(label));
        self.redraw(terminal)?;
        Ok(())
    }

    /// Resolves only once a chat task finishes; pends forever otherwise so it
    /// can sit in `select!` without busy-looping.
    async fn next_chat_result(
        task: &mut Option<ChatTask>,
    ) -> Option<std::result::Result<Result<CodingResult>, tokio::task::JoinError>> {
        let Some(task) = task.as_mut() else {
            return std::future::pending().await;
        };
        Some(task.await)
    }

    async fn next_chat_event(
        receiver: &mut Option<UnboundedReceiver<CodingAgentEvent>>,
    ) -> Option<CodingAgentEvent> {
        let Some(receiver) = receiver.as_mut() else {
            return std::future::pending().await;
        };
        receiver.recv().await
    }
}

async fn await_with_activity<T, F>(
    terminal: &mut ratatui::DefaultTerminal,
    activity: Option<&Activity>,
    theme: &Theme,
    base: Buffer,
    operation: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    tokio::pin!(operation);
    let mut tick = tokio::time::interval(Duration::from_millis(16));
    let started = Instant::now();
    let mut completed = None;
    loop {
        if started.elapsed() >= Duration::from_secs(1)
            && let Some(result) = completed
        {
            return result;
        }
        tokio::select! {
            result = &mut operation, if completed.is_none() => completed = Some(result),
            _ = tick.tick() => {
                terminal.draw(|frame| {
                    *frame.buffer_mut() = base.clone();
                    let rows = Layout::default()
                        .direction(Direction::Vertical)
                        .constraints([
                            Constraint::Min(8),
                            Constraint::Length(1),
                            Constraint::Length(3),
                            Constraint::Length(1),
                        ])
                    .split(content_area(frame.area()));
                    if let Some(activity) = activity {
                        activity.render(
                            rows[1],
                            frame.buffer_mut(),
                            RenderCtx::new(theme),
                        );
                    }
                })?;
            },
        }
    }
}

fn content_area(area: Rect) -> Rect {
    Rect::new(
        area.x.saturating_add(HORIZONTAL_PADDING),
        area.y.saturating_add(TOP_PADDING),
        area.width
            .saturating_sub(HORIZONTAL_PADDING.saturating_mul(2)),
        area.height
            .saturating_sub(TOP_PADDING.saturating_add(BOTTOM_PADDING)),
    )
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
