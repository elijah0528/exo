//! Status bar, key hints, and the busy spinner.

use std::time::{Duration, Instant};

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};

use super::component::RenderCtx;

/// One `key label` pair in the footer.
#[derive(Debug, Clone, Copy)]
pub struct KeyHint {
    pub key: &'static str,
    pub label: &'static str,
}

impl KeyHint {
    pub const fn new(key: &'static str, label: &'static str) -> Self {
        Self { key, label }
    }
}

/// Render hints as `key label   key label`, styled by the theme.
pub fn key_hints(hints: &[KeyHint], ctx: RenderCtx<'_>) -> Line<'static> {
    let mut spans = Vec::with_capacity(hints.len() * 3);
    for (index, hint) in hints.iter().enumerate() {
        if index > 0 {
            spans.push(Span::raw("   "));
        }
        spans.push(Span::styled(hint.key, ctx.theme.key));
        spans.push(Span::raw(" "));
        spans.push(Span::styled(hint.label, ctx.theme.dim));
    }
    Line::from(spans)
}

/// A single-row bar with left content and right-aligned key hints.
pub fn render_status_bar(
    area: Rect,
    buf: &mut Buffer,
    ctx: RenderCtx<'_>,
    left: Line<'static>,
    hints: &[KeyHint],
) {
    if area.height == 0 {
        return;
    }
    let row = Rect { height: 1, ..area };
    buf.set_line(row.x, row.y, &left, row.width);
    let hints = key_hints(hints, ctx);
    let width = hints.width() as u16;
    if width < row.width {
        buf.set_line(row.x + row.width - width, row.y, &hints, width);
    }
}

/// Braille spinner driven by wall-clock time, so it animates on redraws
/// without owning a timer.
#[derive(Debug, Clone, Copy)]
pub struct Spinner {
    started: Instant,
    interval: Duration,
}

const SPINNER_FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

impl Spinner {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            interval: Duration::from_millis(80),
        }
    }

    pub fn frame_at(&self, now: Instant) -> &'static str {
        let elapsed = now.saturating_duration_since(self.started).as_millis();
        let step = (elapsed / self.interval.as_millis().max(1)) as usize;
        SPINNER_FRAMES[step % SPINNER_FRAMES.len()]
    }

    pub fn frame(&self) -> &'static str {
        self.frame_at(Instant::now())
    }
}

impl Default for Spinner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "status_tests.rs"]
mod tests;
