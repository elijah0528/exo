//! Viewport scrolling shared by every scrollable surface.
//!
//! The state is just "which row is at the top" plus a sticky flag for
//! following new output. Content length and viewport height are passed in on
//! every call: the scroll state never caches them, so a component can re-wrap,
//! filter, or resize without invalidating anything.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;

use super::theme::Theme;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scroll {
    top: usize,
    /// While pinned, the viewport follows the end of the content.
    pinned_to_bottom: bool,
}

impl Default for Scroll {
    fn default() -> Self {
        Self {
            top: 0,
            pinned_to_bottom: true,
        }
    }
}

impl Scroll {
    /// The first visible row, clamped to the current content and viewport.
    pub fn top(&self, total: usize, viewport: usize) -> usize {
        let max = max_top(total, viewport);
        if self.pinned_to_bottom {
            max
        } else {
            self.top.min(max)
        }
    }

    pub fn is_pinned_to_bottom(&self) -> bool {
        self.pinned_to_bottom
    }

    /// Scroll by `delta` rows; negative scrolls towards the top.
    pub fn scroll_by(&mut self, delta: isize, total: usize, viewport: usize) {
        let max = max_top(total, viewport);
        let current = self.top(total, viewport) as isize;
        let next = current.saturating_add(delta).clamp(0, max as isize) as usize;
        self.top = next;
        self.pinned_to_bottom = next >= max;
    }

    pub fn page_up(&mut self, total: usize, viewport: usize) {
        self.scroll_by(-(viewport.max(1) as isize), total, viewport);
    }

    pub fn page_down(&mut self, total: usize, viewport: usize) {
        self.scroll_by(viewport.max(1) as isize, total, viewport);
    }

    pub fn scroll_to_top(&mut self) {
        self.top = 0;
        self.pinned_to_bottom = false;
    }

    pub fn scroll_to_bottom(&mut self) {
        self.pinned_to_bottom = true;
    }
}

fn max_top(total: usize, viewport: usize) -> usize {
    total.saturating_sub(viewport)
}

/// Draw a one-column scrollbar on the right edge of `area`.
///
/// Nothing is drawn when everything fits, so callers can render it
/// unconditionally.
pub fn render_scrollbar(
    area: Rect,
    buf: &mut Buffer,
    theme: &Theme,
    total: usize,
    top: usize,
) -> bool {
    let viewport = usize::from(area.height);
    if area.width == 0 || viewport == 0 || total <= viewport {
        return false;
    }
    let thumb_height = ((viewport * viewport) / total).max(1);
    let scrollable = total - viewport;
    let travel = viewport - thumb_height;
    let thumb_top = if scrollable == 0 {
        0
    } else {
        (top * travel).div_ceil(scrollable)
    };
    let x = area.x + area.width - 1;
    for row in 0..viewport {
        let inside = row >= thumb_top && row < thumb_top + thumb_height;
        let (symbol, style) = if inside {
            ("█", theme.accent)
        } else {
            ("│", theme.gauge_empty)
        };
        buf[(x, area.y + row as u16)]
            .set_symbol(symbol)
            .set_style(style);
    }
    true
}

#[cfg(test)]
#[path = "scroll_tests.rs"]
mod tests;
