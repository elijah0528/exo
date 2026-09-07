use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Args, ValueEnum};
use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use excode::{
    EmptySandboxPoolProvisioner, LocalSandboxPool, LocalSandboxPoolStore, ManagedSandboxLease,
    PoolCapacity, PoolEntryState, SandboxPoolKey, SandboxPoolSnapshotStore,
    SandboxPoolSnapshotView, SnapshotRetentionPolicy,
};
use exoharness::{
    CliContainerSandboxBackend, LocalProcessSandboxBackend, ManagedSandboxBackend, SandboxCommand,
    SandboxNetworkPolicy, SandboxSpec, default_docker_image,
};
use tokio_stream::StreamExt;

#[derive(Debug, Clone, Args)]
pub struct SandboxPoolArgs {
    /// Provider used for pool runtimes.
    #[arg(long, value_enum, default_value_t = PoolBackend::LocalProcess)]
    backend: PoolBackend,
    /// Number of warm entries to maintain.
    #[arg(long, default_value_t = 2)]
    workers: usize,
    /// Maximum total entries, including leased entries. Defaults to --workers.
    #[arg(long)]
    max_workers: Option<usize>,
    /// Container image used by the Docker backend.
    #[arg(long, default_value_t = default_docker_image())]
    image: String,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PoolBackend {
    LocalProcess,
    Docker,
}

pub async fn run(root: &Path, args: SandboxPoolArgs) -> Result<()> {
    if args.workers == 0 {
        bail!("--workers must be positive");
    }
    let max_workers = args.max_workers.unwrap_or(args.workers);
    if max_workers < args.workers {
        bail!("--max-workers must be at least --workers");
    }
    let (backend, default_workdir): (Arc<dyn ManagedSandboxBackend>, String) = match args.backend {
        PoolBackend::LocalProcess => (
            Arc::new(LocalProcessSandboxBackend::new()),
            std::env::current_dir()
                .context("determining the local sandbox working directory")?
                .to_string_lossy()
                .into_owned(),
        ),
        PoolBackend::Docker => (
            Arc::new(CliContainerSandboxBackend::docker()),
            "/".to_string(),
        ),
    };
    let local_snapshot_store = match args.backend {
        PoolBackend::LocalProcess => None,
        PoolBackend::Docker => Some(Arc::new(LocalSandboxPoolStore::new(
            root.join("sandbox-pool/snapshots"),
            SnapshotRetentionPolicy {
                max_snapshots: Some(20),
                max_bytes: Some(10 * 1024 * 1024 * 1024),
                ..Default::default()
            },
        ))),
    };
    if let Some(store) = &local_snapshot_store {
        store.clear().await?;
    }
    let snapshot_store: Option<Arc<dyn SandboxPoolSnapshotStore>> =
        local_snapshot_store.map(|store| store as Arc<dyn SandboxPoolSnapshotStore>);
    let pool_id = match args.backend {
        PoolBackend::LocalProcess => "cli-local-process",
        PoolBackend::Docker => "cli-docker",
    };
    let pool = Arc::new(LocalSandboxPool::new(
        SandboxPoolKey {
            pool_id: pool_id.to_string(),
            recipe_id: "empty".to_string(),
            spec: SandboxSpec {
                image: args.image,
                resources: Default::default(),
                mounts: Vec::new(),
                durable_file_systems: Vec::new(),
                network: SandboxNetworkPolicy::Enabled,
                default_workdir,
            },
        },
        backend,
        PoolCapacity {
            warm_size: args.workers,
            max_total: max_workers,
            lease_ttl: Duration::from_secs(300),
            idle_ttl: Duration::from_secs(600),
        },
        Arc::new(EmptySandboxPoolProvisioner),
        snapshot_store.clone(),
    )?);

    pool.reconcile_once()
        .await
        .context("creating initial sandbox pool entries")?;
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let reconciler = tokio::spawn({
        let pool = Arc::clone(&pool);
        async move { pool.run_reconciler(receiver).await }
    });

    let result = PoolTui::new(pool.clone(), snapshot_store, pool_id.to_string())
        .run()
        .await;
    let _ = shutdown.send(true);
    reconciler
        .await
        .context("sandbox pool reconciler task failed")?;
    pool.drain().await.context("draining sandbox pool")?;
    result
}

struct PoolTui {
    pool: Arc<LocalSandboxPool>,
    entries: Vec<excode::SandboxPoolEntryView>,
    selected: usize,
    active: Option<ManagedSandboxLease>,
    detached: HashMap<String, ManagedSandboxLease>,
    input: String,
    command_mode: bool,
    log: VecDeque<String>,
    terminal: VecDeque<String>,
    terminal_scroll: usize,
    snapshot_store: Option<Arc<dyn SandboxPoolSnapshotStore>>,
    pool_id: String,
    snapshots: Vec<SandboxPoolSnapshotView>,
    snapshot_selected: usize,
    snapshot_focus: bool,
    terminal_focus: bool,
    dirty: bool,
}

impl PoolTui {
    fn new(
        pool: Arc<LocalSandboxPool>,
        snapshot_store: Option<Arc<dyn SandboxPoolSnapshotStore>>,
        pool_id: String,
    ) -> Self {
        let mut terminal = VecDeque::from(["Pool starting...".to_string()]);
        terminal.push_back(if snapshot_store.is_some() {
            "snapshot store: configured (Docker snapshots)".to_string()
        } else {
            "snapshot store: unavailable for local-process sandboxes".to_string()
        });
        Self {
            pool,
            entries: Vec::new(),
            selected: 0,
            active: None,
            detached: HashMap::new(),
            input: String::new(),
            command_mode: false,
            log: VecDeque::from(["Starting sandbox pool...".to_string()]),
            terminal,
            terminal_scroll: 0,
            snapshot_store,
            pool_id,
            snapshots: Vec::new(),
            snapshot_selected: 0,
            snapshot_focus: false,
            terminal_focus: false,
            dirty: false,
        }
    }

    async fn run(mut self) -> Result<()> {
        self.refresh().await?;
        let mut terminal = ratatui::init();
        let mut events = EventStream::new();
        let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
        let mut refresh = tokio::time::interval(Duration::from_secs(1));
        let result = async {
            loop {
                terminal.draw(|frame| self.draw(frame))?;
                tokio::select! {
                    event = events.next() => {
                        let Some(event) = event else { break Ok(()); };
                        if let Event::Key(key) = event?
                            && key.kind == KeyEventKind::Press
                            && self.handle_key(key).await?
                        {
                            break Ok(());
                        }
                    }
                    _ = heartbeat.tick() => self.heartbeat_claims().await?,
                    _ = refresh.tick() => self.refresh().await?,
                }
            }
        }
        .await;
        ratatui::restore();
        result
    }

    async fn handle_key(&mut self, key: crossterm::event::KeyEvent) -> Result<bool> {
        if self.command_mode {
            match key.code {
                KeyCode::Esc => self.command_mode = false,
                KeyCode::Enter => {
                    let command = std::mem::take(&mut self.input);
                    self.command_mode = false;
                    self.handle_input(command).await?;
                }
                KeyCode::Backspace => {
                    self.input.pop();
                }
                KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.input.push(ch);
                }
                _ => {}
            }
            return Ok(false);
        }

        if self.terminal_focus {
            match key.code {
                KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.input.push(ch);
                    self.command_mode = true;
                }
                _ => {}
            }
            if self.command_mode {
                return Ok(false);
            }
        }

        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return Ok(true),
            KeyCode::Char('r') => self.refresh().await?,
            KeyCode::Down | KeyCode::Char('j') => self.select(1),
            KeyCode::Up | KeyCode::Char('k') => self.select_up(),
            KeyCode::Enter if self.snapshot_focus => self.restore_snapshot().await?,
            KeyCode::Enter => self.acquire_selected().await?,
            KeyCode::Char('s') => self.restore_snapshot().await?,
            KeyCode::Char('e') => {
                if self.active.is_some() {
                    self.input.clear();
                    self.command_mode = true;
                } else {
                    self.note("Acquire an entry before running a command");
                }
            }
            KeyCode::Char('/') => {
                self.input.clear();
                self.input.push('/');
                self.command_mode = true;
            }
            KeyCode::Char('x') => self.release().await?,
            KeyCode::Char('R') => self.reset().await?,
            KeyCode::Tab => self.focus_next(),
            KeyCode::PageUp => self.terminal_scroll = self.terminal_scroll.saturating_add(5),
            KeyCode::PageDown => self.terminal_scroll = self.terminal_scroll.saturating_sub(5),
            KeyCode::Home => self.terminal_scroll = usize::MAX,
            KeyCode::End => self.terminal_scroll = 0,
            _ => {}
        }
        Ok(false)
    }

    fn focus_next(&mut self) {
        if self.terminal_focus {
            self.terminal_focus = false;
            self.snapshot_focus = true;
        } else if self.snapshot_focus {
            self.snapshot_focus = false;
            self.terminal_focus = false;
        } else {
            self.terminal_focus = true;
        }
    }

    async fn handle_input(&mut self, input: String) -> Result<()> {
        if let Some(command) = input.strip_prefix('/') {
            self.handle_slash_command(command).await
        } else {
            self.execute(input).await
        }
    }

    async fn handle_slash_command(&mut self, command: &str) -> Result<()> {
        match command.trim() {
            "attach" => self.restore_snapshot().await,
            command if command.starts_with("attach ") => {
                self.restore_snapshot_argument(command, "attach").await
            }
            "detach" => {
                self.detach_active();
                Ok(())
            }
            "release" => self.release().await,
            "retire" => self.retire_selected().await,
            "dirty" => self.execute("touch temp.txt".to_string()).await,
            command if command == "restore" => self.restore_snapshot().await,
            command if command.starts_with("restore ") => {
                self.restore_snapshot_argument(command, "restore").await
            }
            "" => Ok(()),
            other => {
                self.note(format!("unknown command: /{other}"));
                Ok(())
            }
        }
    }

    fn select(&mut self, delta: usize) {
        if self.snapshot_focus {
            if !self.snapshots.is_empty() {
                self.snapshot_selected = (self.snapshot_selected + delta) % self.snapshots.len();
            }
        } else if !self.entries.is_empty() {
            self.selected = (self.selected + delta) % self.entries.len();
        }
    }

    fn select_up(&mut self) {
        if self.snapshot_focus {
            if !self.snapshots.is_empty() {
                self.snapshot_selected = self
                    .snapshot_selected
                    .checked_sub(1)
                    .unwrap_or(self.snapshots.len() - 1);
            }
        } else if !self.entries.is_empty() {
            self.selected = self
                .selected
                .checked_sub(1)
                .unwrap_or(self.entries.len() - 1);
        }
    }

    async fn refresh(&mut self) -> Result<()> {
        self.entries = self.pool.entries().await;
        self.selected = self.selected.min(self.entries.len().saturating_sub(1));
        if let Some(store) = &self.snapshot_store {
            let baseline_owner = self.pool.baseline_owner_id();
            self.snapshots = store.list(&self.pool_id, &baseline_owner).await?;
            self.snapshots
                .extend(store.list(&self.pool_id, "cli").await?);
            self.snapshots
                .sort_by_key(|snapshot| std::cmp::Reverse(snapshot.last_accessed_at_ms));
            self.snapshot_selected = self
                .snapshot_selected
                .min(self.snapshots.len().saturating_sub(1));
        }
        Ok(())
    }

    async fn acquire_selected(&mut self) -> Result<()> {
        let Some(entry) = self.entries.get(self.selected) else {
            self.note("The pool has no entries yet");
            return Ok(());
        };
        let entry_id = entry.entry_id.clone();
        let entry_dirty = entry.dirty;
        if self.active.is_some() {
            self.note("Detach the active sandbox before acquiring another");
            return Ok(());
        }
        if let Some(lease) = self.detached.remove(&entry_id) {
            self.note(format!("Attached to claimed {}", lease.sandbox.id()));
            self.active = Some(lease);
            self.dirty = entry_dirty;
            self.refresh().await?;
            return Ok(());
        }
        match self.pool.acquire_entry(&entry_id, "cli").await {
            Ok(lease) => {
                self.note(format!("Connected to {}", lease.sandbox.id()));
                self.dirty = entry_dirty;
                self.active = Some(lease);
            }
            Err(error) => self.note(format!("Acquire failed: {error:#}")),
        }
        self.refresh().await?;
        Ok(())
    }

    fn detach_active(&mut self) {
        let Some(lease) = self.active.take() else {
            self.note("No sandbox is attached");
            return;
        };
        let entry_id = lease.lease.entry_id.clone();
        self.detached.insert(entry_id, lease);
        self.dirty = false;
        self.note("Detached; lease remains claimed and heartbeated");
    }

    async fn retire_selected(&mut self) -> Result<()> {
        let Some(entry) = self.entries.get(self.selected) else {
            self.note("The pool has no entries yet");
            return Ok(());
        };
        let entry_id = entry.entry_id.clone();
        let lease = if self
            .active
            .as_ref()
            .is_some_and(|lease| lease.lease.entry_id == entry_id)
        {
            self.active.take()
        } else {
            self.detached.remove(&entry_id)
        };
        let Some(lease) = lease else {
            self.note("Claim the selected sandbox before retiring it");
            return Ok(());
        };
        self.pool.reset(&lease.lease).await?;
        self.dirty = false;
        self.note(format!("Retired {}", lease.sandbox.id()));
        self.refresh().await?;
        Ok(())
    }

    async fn heartbeat_claims(&mut self) -> Result<()> {
        let mut leases = self
            .detached
            .values()
            .map(|lease| lease.lease.clone())
            .collect::<Vec<_>>();
        if let Some(active) = &self.active {
            leases.push(active.lease.clone());
        }
        for lease in leases {
            if let Err(error) = self.pool.heartbeat(&lease).await {
                self.note(format!("Lease heartbeat failed: {error:#}"));
                self.detached.remove(&lease.entry_id);
                if self
                    .active
                    .as_ref()
                    .is_some_and(|active| active.lease.entry_id == lease.entry_id)
                {
                    self.active = None;
                    self.dirty = false;
                }
            }
        }
        Ok(())
    }

    async fn execute(&mut self, command: String) -> Result<()> {
        if command.trim().is_empty() {
            return Ok(());
        }
        let Some(active) = &self.active else {
            self.note("No sandbox is connected");
            return Ok(());
        };
        let sandbox_command = SandboxCommand {
            argv: vec!["/bin/sh".to_string(), "-lc".to_string(), command.clone()],
            env: Default::default(),
            display_argv: Some(vec![command.clone()]),
            cwd: None,
            timeout: Some(Duration::from_secs(300)),
        };
        let result = active.sandbox.exec(&sandbox_command).await?;
        self.terminal_line(format!("$ {command}"));
        if !result.stdout.trim().is_empty() {
            self.terminal_text(result.stdout.trim_end());
        }
        if !result.stderr.trim().is_empty() {
            self.terminal_text(&format!("stderr: {}", result.stderr.trim_end()));
        }
        if !result.ok {
            self.terminal_line(format!("command failed with {:?}", result.exit_code));
        }
        self.dirty = true;
        self.terminal_line("sandbox filesystem marked dirty");
        self.terminal_scroll = 0;
        self.refresh().await?;
        Ok(())
    }

    async fn release(&mut self) -> Result<()> {
        let Some(active) = self.active.take() else {
            return Ok(());
        };
        if self.dirty {
            self.terminal_line("releasing sandbox: discarding changes and restoring baseline...");
        } else {
            self.terminal_line("releasing clean sandbox: restoring baseline...");
        }
        let checkpoint = self.pool.release(&active.lease).await?;
        if let Some(store) = &self.snapshot_store {
            store.prune().await?;
        }
        self.dirty = false;
        if let Some(snapshot_id) = checkpoint {
            self.note(format!("checkpoint saved: {snapshot_id}"));
            self.terminal_line(format!("snapshot store updated: {snapshot_id}"));
        } else {
            self.note("no filesystem changes; no checkpoint created");
        }
        self.note("released and restored the recipe baseline");
        self.terminal_line("sandbox is ready for the next lease");
        self.refresh().await?;
        Ok(())
    }

    async fn restore_snapshot(&mut self) -> Result<()> {
        self.restore_snapshot_number(self.snapshot_selected + 1)
            .await
    }

    async fn restore_snapshot_number(&mut self, number: usize) -> Result<()> {
        if number == 0 || number > self.snapshots.len() {
            self.note(format!(
                "Snapshot {number} does not exist; choose 1-{}",
                self.snapshots.len()
            ));
            return Ok(());
        }
        self.snapshot_selected = number - 1;
        let Some(snapshot) = self.snapshots.get(self.snapshot_selected) else {
            self.note("No saved snapshot is available");
            return Ok(());
        };
        let snapshot_id = snapshot.snapshot_id;
        let owner_id = snapshot.owner_id.clone();
        if self.active.is_some() {
            self.note("Release the active sandbox before restoring a snapshot");
            return Ok(());
        }
        self.terminal_line(format!("restoring snapshot {snapshot_id}..."));
        match self
            .pool
            .acquire_any_from_snapshot(owner_id, snapshot_id)
            .await
        {
            Ok(lease) => {
                self.dirty = false;
                self.note(format!("restored snapshot {snapshot_id}"));
                self.active = Some(lease);
                self.terminal_line("snapshot restored; sandbox is connected");
            }
            Err(error) => self.terminal_line(format!("snapshot restore failed: {error:#}")),
        }
        self.refresh().await?;
        Ok(())
    }

    async fn restore_snapshot_argument(&mut self, command: &str, name: &str) -> Result<()> {
        let number = command
            .split_whitespace()
            .nth(1)
            .and_then(|value| value.parse::<usize>().ok());
        let Some(number) = number else {
            self.note(format!("Usage: /{name} <snapshot #>"));
            return Ok(());
        };
        self.restore_snapshot_number(number).await
    }

    async fn reset(&mut self) -> Result<()> {
        let Some(active) = self.active.take() else {
            self.note("No sandbox is connected");
            return Ok(());
        };
        self.pool.reset(&active.lease).await?;
        self.note("Sandbox reset and removed from the pool");
        self.refresh().await?;
        Ok(())
    }

    fn note(&mut self, message: impl Into<String>) {
        let message = message.into();
        self.log.push_back(message.clone());
        while self.log.len() > 6 {
            self.log.pop_front();
        }
        self.terminal_line(format!("status: {message}"));
    }

    fn terminal_text(&mut self, text: &str) {
        for line in text.lines() {
            self.terminal_line(format!("       {line}"));
        }
    }

    fn terminal_line(&mut self, line: impl Into<String>) {
        self.terminal.push_back(format_terminal_line(&line.into()));
        while self.terminal.len() > 10_000 {
            self.terminal.pop_front();
        }
    }

    fn draw(&self, frame: &mut ratatui::Frame) {
        use ratatui::layout::{Alignment, Constraint, Direction, Layout};
        use ratatui::style::{Color, Modifier, Style};
        use ratatui::text::{Line, Span};
        use ratatui::widgets::{Block, Borders, Paragraph, Row, Table, TableState, Wrap};

        let areas = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Length(7),
                Constraint::Min(12),
            ])
            .split(frame.area());
        let title = Paragraph::new(Line::from(vec![
            Span::styled(
                " Excode Sandbox Pool ",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(
                "  Enter attach  /attach /detach /release /retire /dirty  s restore  x release  Tab focus  q quit",
            ),
        ]))
        .block(Block::default().borders(Borders::ALL).title("Pool"));
        frame.render_widget(title, areas[0]);

        let selectors = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(48), Constraint::Percentage(52)])
            .split(areas[1]);

        let ready_count = self
            .entries
            .iter()
            .filter(|entry| entry.state == PoolEntryState::Ready)
            .count();
        let capacity = self.pool.capacity();
        let sandbox_title = format!(
            "Sandboxes [Tab]  Warm Sandboxes available {ready_count}  Total sandboxes {}  Max sandboxes {}",
            self.entries.len(),
            capacity.max_total,
        );
        let rows = self.entries.iter().enumerate().map(|(index, entry)| {
            let color = match entry.state {
                PoolEntryState::Ready => Color::Green,
                PoolEntryState::Leased => Color::Yellow,
                PoolEntryState::Retiring => Color::Red,
                _ => Color::DarkGray,
            };
            Row::new(vec![
                format!("{}", index + 1),
                entry
                    .sandbox_id
                    .as_deref()
                    .map(short_id)
                    .unwrap_or_else(|| "starting".to_string()),
                if entry.dirty {
                    format!("{:?} ●", entry.state)
                } else {
                    format!("{:?}", entry.state)
                },
                entry.lease_owner.as_deref().unwrap_or("-").to_string(),
                format!("{}s", entry.idle_for.as_secs()),
                entry
                    .snapshot_id
                    .map(|id| self.snapshot_number(id))
                    .unwrap_or_else(|| "-".to_string()),
            ])
            .style(Style::default().fg(color))
        });
        let table = Table::new(
            rows,
            [
                Constraint::Length(4),
                Constraint::Length(18),
                Constraint::Length(12),
                Constraint::Length(10),
                Constraint::Length(8),
                Constraint::Length(18),
            ],
        )
        .header(
            Row::new(["#", "Sandbox", "State", "Owner", "Idle", "Snapshot"]).style(
                Style::default()
                    .fg(Color::Blue)
                    .add_modifier(Modifier::BOLD),
            ),
        )
        .row_highlight_style(Style::default().bg(Color::DarkGray))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(sandbox_title)
                .border_style(if self.snapshot_focus {
                    Style::default()
                } else {
                    Style::default().fg(Color::Cyan)
                }),
        );
        let mut state = TableState::default();
        state.select((!self.entries.is_empty()).then_some(self.selected));
        frame.render_stateful_widget(table, selectors[0], &mut state);

        let snapshot_rows = self.snapshots.iter().enumerate().map(|(index, snapshot)| {
            Row::new(vec![
                format!("{}", index + 1),
                short_id(&snapshot.snapshot_id.to_string()),
                format_bytes(snapshot.size_bytes),
                if snapshot.owner_id.starts_with(excode::RECIPE_BASELINE_OWNER) {
                    "baseline".to_string()
                } else {
                    "checkpoint".to_string()
                },
            ])
            .style(Style::default().fg(Color::Magenta))
        });
        let snapshot_table = Table::new(
            snapshot_rows,
            [
                Constraint::Length(4),
                Constraint::Length(18),
                Constraint::Length(10),
                Constraint::Length(12),
            ],
        )
        .header(
            Row::new(["#", "Snapshot", "Size", "Kind"]).style(
                Style::default()
                    .fg(Color::Blue)
                    .add_modifier(Modifier::BOLD),
            ),
        )
        .row_highlight_style(Style::default().bg(Color::DarkGray))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Snapshot Store [Tab]")
                .border_style(if self.snapshot_focus {
                    Style::default().fg(Color::Magenta)
                } else {
                    Style::default()
                }),
        );
        let mut snapshot_state = TableState::default();
        snapshot_state.select((!self.snapshots.is_empty()).then_some(self.snapshot_selected));
        frame.render_stateful_widget(snapshot_table, selectors[1], &mut snapshot_state);

        let mut lines = self
            .terminal
            .iter()
            .map(|line| styled_terminal_line(line))
            .collect::<Vec<_>>();
        if lines.is_empty() {
            lines.push(Line::from(
                "No command output yet. Press e to run a command.",
            ));
        }
        if self.command_mode {
            lines.push(Line::from(Span::styled(
                format!("$ {}▌", self.input),
                Style::default().fg(Color::Yellow),
            )));
        } else if let Some(active) = &self.active {
            lines.push(Line::from(format!("connected: {}", active.sandbox.id())));
        }
        let visible_lines = usize::from(areas[2].height.saturating_sub(2));
        let max_scroll = lines.len().saturating_sub(visible_lines);
        let scroll = max_scroll.saturating_sub(self.terminal_scroll.min(max_scroll));
        let terminal_title = if self.dirty {
            Line::from(vec![
                Span::raw("Terminal  "),
                Span::styled("●", Style::default().fg(Color::Yellow)),
            ])
        } else {
            Line::from("Terminal")
        };
        frame.render_widget(
            Paragraph::new(lines)
                .scroll((scroll.try_into().unwrap_or(u16::MAX), 0))
                .wrap(Wrap { trim: false })
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(terminal_title)
                        .title_alignment(Alignment::Right)
                        .border_style(if self.terminal_focus {
                            Style::default().fg(Color::Cyan)
                        } else {
                            Style::default()
                        }),
                ),
            areas[2],
        );
    }

    fn snapshot_number(&self, snapshot_id: exoharness::SnapshotId) -> String {
        self.snapshots
            .iter()
            .position(|snapshot| snapshot.snapshot_id == snapshot_id)
            .map(|index| (index + 1).to_string())
            .unwrap_or_else(|| short_id(&snapshot_id.to_string()))
    }
}

fn short_id(id: &str) -> String {
    id.strip_prefix("local:")
        .or_else(|| id.strip_prefix("docker:"))
        .unwrap_or(id)
        .chars()
        .take(16)
        .collect()
}

fn styled_terminal_line(line: &str) -> ratatui::text::Line<'static> {
    use ratatui::style::{Color, Style};
    use ratatui::text::Span;

    let color = if line.starts_with("[pool]") {
        Color::Green
    } else if line.starts_with("[fs]") {
        Color::Yellow
    } else if line.starts_with("[error]") {
        Color::Red
    } else if line.starts_with("[save]") {
        Color::Magenta
    } else if line.starts_with("$") {
        Color::Yellow
    } else if line.starts_with("       ") {
        Color::White
    } else {
        Color::Cyan
    };
    ratatui::text::Line::from(Span::styled(line.to_string(), Style::default().fg(color)))
}

fn format_terminal_line(line: &str) -> String {
    if let Some(message) = line.strip_prefix("status: ") {
        let category = if message.contains("failed")
            || message.starts_with("No ")
            || message.starts_with("unknown ")
        {
            "[error]"
        } else if message.contains("snapshot") || message.contains("checkpoint") {
            "[save]"
        } else if message.contains("dirty") || message.contains("filesystem") {
            "[fs]"
        } else {
            "[pool]"
        };
        return format!("{category} {message}");
    }
    if let Some(command) = line.strip_prefix("$ ") {
        return format!("$      {command}");
    }
    if line.starts_with("sandbox filesystem") {
        return format!("[fs]   {line}");
    }
    if line.contains("snapshot") || line.contains("checkpoint") {
        return format!("[save] {line}");
    }
    if line.contains("failed") {
        return format!("[error] {line}");
    }
    if line.starts_with("Pool ") || line.starts_with("snapshot store:") {
        return format!("[pool] {line}");
    }
    line.to_string()
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.1}M", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1}K", bytes as f64 / 1024.0)
    } else {
        format!("{bytes}B")
    }
}
