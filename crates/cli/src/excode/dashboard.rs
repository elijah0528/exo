//! Pool-facing views: the sandbox table, the snapshot list, and the debug
//! utilization panel. Each is a [`Component`] built from primitives only.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};

use excode::{PoolEntryState, SandboxPoolSnapshotView};

use super::session::{EntryRow, Utilization, format_bytes, short_id};
use super::ui::component::{Component, RenderCtx};
use super::ui::gauge::gauge_line;
use super::ui::list::{SelectionList, columns};
use super::ui::theme::Theme;

fn state_style(state: PoolEntryState, theme: &Theme) -> Style {
    match state {
        PoolEntryState::Ready => theme.success,
        PoolEntryState::Leased => theme.warn,
        PoolEntryState::Retiring => theme.error,
        _ => theme.dim,
    }
}

/// The sandbox table. Rows are rebuilt from the session on every refresh.
#[derive(Default)]
pub struct SandboxTable {
    list: SelectionList,
}

impl SandboxTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, rows: &[EntryRow], theme: &Theme) {
        self.list.set_rows(
            rows.iter()
                .map(|row| {
                    let style = state_style(row.state, theme);
                    let state = if row.dirty {
                        format!("{:?} ●", row.state)
                    } else {
                        format!("{:?}", row.state)
                    };
                    columns(&[
                        (Span::styled(row.index.to_string(), theme.dim), 3),
                        (Span::styled(row.sandbox_id.clone(), theme.text), 18),
                        (Span::styled(state, style), 12),
                        (
                            Span::styled(
                                row.owner.clone().unwrap_or_else(|| "-".to_string()),
                                theme.dim,
                            ),
                            8,
                        ),
                        (Span::styled(format!("{}s", row.idle_secs), theme.dim), 6),
                        (
                            Span::styled(
                                row.snapshot.clone().unwrap_or_else(|| "-".to_string()),
                                theme.info,
                            ),
                            8,
                        ),
                    ])
                })
                .collect(),
        );
    }

    pub fn header(theme: &Theme) -> Line<'static> {
        columns(&[
            (Span::styled("#", theme.title), 3),
            (Span::styled("SANDBOX", theme.title), 18),
            (Span::styled("STATE", theme.title), 12),
            (Span::styled("OWNER", theme.title), 8),
            (Span::styled("IDLE", theme.title), 6),
            (Span::styled("SNAP", theme.title), 8),
        ])
    }
}

impl Component for SandboxTable {
    fn render(&self, area: Rect, buf: &mut Buffer, ctx: RenderCtx<'_>) {
        if area.height == 0 {
            return;
        }
        buf.set_line(area.x, area.y, &Self::header(ctx.theme), area.width);
        let body = Rect {
            y: area.y + 1,
            height: area.height - 1,
            ..area
        };
        self.list.render(body, buf, ctx);
    }

    fn handle_key(&mut self, key: crossterm::event::KeyEvent) -> super::ui::component::EventFlow {
        self.list.handle_key(key)
    }
}

/// The read-only snapshot store listing.
pub struct SnapshotList<'a> {
    snapshots: &'a [SandboxPoolSnapshotView],
    /// Local-process pools have no store; the baseline is in memory.
    has_store: bool,
}

impl<'a> SnapshotList<'a> {
    pub fn new(snapshots: &'a [SandboxPoolSnapshotView], has_store: bool) -> Self {
        Self {
            snapshots,
            has_store,
        }
    }
}

impl Component for SnapshotList<'_> {
    fn render(&self, area: Rect, buf: &mut Buffer, ctx: RenderCtx<'_>) {
        let theme = ctx.theme;
        let mut lines = vec![columns(&[
            (Span::styled("#", theme.title), 3),
            (Span::styled("SNAPSHOT", theme.title), 18),
            (Span::styled("SIZE", theme.title), 8),
            (Span::styled("KIND", theme.title), 11),
        ])];
        if self.has_store {
            lines.extend(self.snapshots.iter().enumerate().map(|(index, snapshot)| {
                let kind = if snapshot.owner_id.starts_with(excode::RECIPE_BASELINE_OWNER) {
                    "baseline"
                } else {
                    "checkpoint"
                };
                columns(&[
                    (Span::styled((index + 1).to_string(), theme.dim), 3),
                    (
                        Span::styled(short_id(&snapshot.snapshot_id.to_string()), theme.text),
                        18,
                    ),
                    (
                        Span::styled(format_bytes(snapshot.size_bytes), theme.dim),
                        8,
                    ),
                    (Span::styled(kind, theme.info), 11),
                ])
            }));
        } else {
            lines.push(Line::from(Span::styled(
                "  recipe baseline (in-memory)",
                theme.dim,
            )));
        }
        for (row, line) in lines.iter().take(usize::from(area.height)).enumerate() {
            buf.set_line(area.x, area.y + row as u16, line, area.width);
        }
    }
}

/// `--debug` panel: pool utilization plus the counters behind it.
pub struct DebugPanel {
    utilization: Utilization,
    detached: usize,
    dirty: bool,
}

impl DebugPanel {
    pub fn new(utilization: Utilization, detached: usize, dirty: bool) -> Self {
        Self {
            utilization,
            detached,
            dirty,
        }
    }
}

impl Component for DebugPanel {
    fn render(&self, area: Rect, buf: &mut Buffer, ctx: RenderCtx<'_>) {
        let used = self.utilization;
        let theme = ctx.theme;
        let lines = [
            gauge_line("leased ", used.leased, used.max_total, area.width, ctx),
            gauge_line("warm   ", used.ready, used.warm_size, area.width, ctx),
            gauge_line("live   ", used.total, used.max_total, area.width, ctx),
            Line::default(),
            Line::from(vec![
                Span::styled("starting ", theme.dim),
                Span::styled(used.starting.to_string(), theme.text),
                Span::styled("   retiring ", theme.dim),
                Span::styled(used.retiring.to_string(), theme.text),
                Span::styled("   detached leases ", theme.dim),
                Span::styled(self.detached.to_string(), theme.text),
            ]),
            Line::from(vec![
                Span::styled("attached filesystem ", theme.dim),
                if self.dirty {
                    Span::styled("dirty", theme.warn)
                } else {
                    Span::styled("clean", theme.success)
                },
            ]),
        ];
        for (row, line) in lines.iter().take(usize::from(area.height)).enumerate() {
            buf.set_line(area.x, area.y + row as u16, line, area.width);
        }
    }

    fn desired_height(&self, _width: u16) -> u16 {
        6
    }
}
