//! Snapshot-facing views for the Excode terminal.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};

use excode::SandboxPoolSnapshotView;

use super::session::{format_bytes, short_id};
use super::ui::component::{Component, RenderCtx};
use super::ui::list::columns;

/// The snapshot list is the primary navigation surface. Pool entries remain
/// an implementation detail and are intentionally not rendered.
pub struct SnapshotList<'a> {
    snapshots: &'a [SandboxPoolSnapshotView],
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
        if area.height == 0 {
            return;
        }
        let theme = ctx.theme;
        let mut lines = vec![Line::from(Span::styled("Snapshots", theme.title))];
        if self.has_store {
            lines.push(columns(&[
                (Span::styled("#", theme.title), 3),
                (Span::styled("SNAPSHOT", theme.title), 18),
                (Span::styled("SIZE", theme.title), 8),
                (Span::styled("KIND", theme.title), 11),
            ]));
            if self.snapshots.is_empty() {
                lines.push(Line::from(Span::styled(
                    "  recipe baseline appears after /c",
                    theme.dim,
                )));
            } else {
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
            }
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
