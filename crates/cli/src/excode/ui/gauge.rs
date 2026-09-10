//! A one-row utilization bar.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};

use super::component::RenderCtx;

/// Build `label ███░░░ 3/8` for `filled` out of `total`.
pub fn gauge_line(
    label: &str,
    filled: usize,
    total: usize,
    width: u16,
    ctx: RenderCtx<'_>,
) -> Line<'static> {
    let suffix = format!(" {filled}/{total}");
    let bar_width = usize::from(width)
        .saturating_sub(label.chars().count() + suffix.chars().count() + 1)
        .min(40);
    let filled_cells = if total == 0 {
        0
    } else {
        (filled * bar_width).div_ceil(total).min(bar_width)
    };
    Line::from(vec![
        Span::styled(format!("{label} "), ctx.theme.dim),
        Span::styled("█".repeat(filled_cells), ctx.theme.gauge_filled),
        Span::styled("░".repeat(bar_width - filled_cells), ctx.theme.gauge_empty),
        Span::styled(suffix, ctx.theme.dim),
    ])
}

pub fn render_gauge(
    area: Rect,
    buf: &mut Buffer,
    ctx: RenderCtx<'_>,
    label: &str,
    filled: usize,
    total: usize,
) {
    if area.height == 0 {
        return;
    }
    let line = gauge_line(label, filled, total, area.width, ctx);
    buf.set_line(area.x, area.y, &line, area.width);
}

#[cfg(test)]
#[path = "gauge_tests.rs"]
mod tests;
