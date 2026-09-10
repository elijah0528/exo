//! Live activity indicators for work that continues across redraws.

use std::time::Instant;

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};

use super::component::{Component, RenderCtx};
use super::status::Spinner;

/// A small live status line with a spinner and elapsed time.
pub struct Activity {
    label: String,
    started: Instant,
    spinner: Spinner,
}

impl Activity {
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            started: Instant::now(),
            spinner: Spinner::new(),
        }
    }
}

impl Component for Activity {
    fn render(&self, area: Rect, buf: &mut Buffer, ctx: RenderCtx<'_>) {
        if area.width == 0 || area.height == 0 {
            return;
        }
        let elapsed = self.started.elapsed().as_secs();
        let line = Line::from(vec![
            Span::styled(self.spinner.frame(), ctx.theme.accent),
            Span::raw(" "),
            Span::styled(self.label.clone(), ctx.theme.dim),
            Span::raw(" "),
            Span::styled(format!("{elapsed}s"), ctx.theme.accent),
        ]);
        buf.set_line(area.x, area.y, &line, area.width);
    }
}
