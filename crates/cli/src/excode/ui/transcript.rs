//! An append-only, scrollable log of rendered cells.
//!
//! A transcript owns a list of [`Cell`]s rather than a list of lines. Cells
//! decide how they look at a given width, which keeps wrapping correct on
//! resize and lets new content types (diffs, tool calls, tables) be added
//! without touching the viewport. Laid-out lines are cached per width.

use std::cell::RefCell;

use crossterm::event::{KeyCode, KeyEvent, MouseEvent, MouseEventKind};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::Line;

use super::component::{Component, EventFlow, RenderCtx};
use super::scroll::{Scroll, render_scrollbar};
use super::theme::Theme;

/// One entry in a transcript.
pub trait Cell {
    /// Render the entry at `width`. Implementations must wrap their own text;
    /// [`super::wrap::wrap_line`] is the shared helper for that.
    fn lines(&self, width: u16, theme: &Theme) -> Vec<Line<'static>>;
}

#[derive(Default)]
struct LayoutCache {
    width: u16,
    cells: usize,
    lines: Vec<Line<'static>>,
}

#[derive(Default)]
pub struct Transcript {
    cells: Vec<Box<dyn Cell>>,
    scroll: Scroll,
    cache: RefCell<LayoutCache>,
    /// Height of the last rendered viewport, so key handling can page by
    /// exactly what the user is looking at.
    viewport: std::cell::Cell<usize>,
}

impl Transcript {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, cell: impl Cell + 'static) {
        self.cells.push(Box::new(cell));
    }

    pub fn clear(&mut self) {
        self.cells.clear();
        self.invalidate();
        self.scroll.scroll_to_bottom();
    }

    pub fn len(&self) -> usize {
        self.cells.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    pub fn is_following_tail(&self) -> bool {
        self.scroll.is_pinned_to_bottom()
    }

    /// Drop the cached layout, e.g. after the theme changes.
    pub fn invalidate(&mut self) {
        *self.cache.borrow_mut() = LayoutCache::default();
    }

    /// Laid-out lines at `width`, recomputing only when the width or the cell
    /// count changed. Cells are expected to be immutable once pushed.
    fn lines(&self, width: u16, theme: &Theme) -> std::cell::Ref<'_, LayoutCache> {
        {
            let cache = self.cache.borrow();
            if cache.width == width && cache.cells == self.cells.len() {
                return cache;
            }
        }
        let lines = self
            .cells
            .iter()
            .flat_map(|cell| cell.lines(width, theme))
            .collect();
        *self.cache.borrow_mut() = LayoutCache {
            width,
            cells: self.cells.len(),
            lines,
        };
        self.cache.borrow()
    }

    /// Rows the content occupies at `width`.
    pub fn line_count(&self, width: u16, theme: &Theme) -> usize {
        self.lines(width, theme).lines.len()
    }

    /// Content length and viewport as of the last render.
    fn metrics(&self) -> (usize, usize) {
        (self.cache.borrow().lines.len(), self.viewport.get().max(1))
    }
}

impl Component for Transcript {
    fn render(&self, area: Rect, buf: &mut Buffer, ctx: RenderCtx<'_>) {
        if area.width == 0 || area.height == 0 {
            return;
        }
        // Leave a column for the scrollbar so wrapping matches what is drawn.
        let text_width = area.width.saturating_sub(1).max(1);
        let cache = self.lines(text_width, ctx.theme);
        let viewport = usize::from(area.height);
        self.viewport.set(viewport);
        let top = self.scroll.top(cache.lines.len(), viewport);
        for (row, line) in cache.lines.iter().skip(top).take(viewport).enumerate() {
            buf.set_line(area.x, area.y + row as u16, line, text_width);
        }
        render_scrollbar(area, buf, ctx.theme, cache.lines.len(), top);
    }

    fn handle_key(&mut self, key: KeyEvent) -> EventFlow {
        let (total, viewport) = self.metrics();
        match key.code {
            KeyCode::PageUp => self.scroll.page_up(total, viewport),
            KeyCode::PageDown => self.scroll.page_down(total, viewport),
            KeyCode::Home => self.scroll.scroll_to_top(),
            KeyCode::End => self.scroll.scroll_to_bottom(),
            KeyCode::Up => self.scroll.scroll_by(-1, total, viewport),
            KeyCode::Down => self.scroll.scroll_by(1, total, viewport),
            _ => return EventFlow::Ignored,
        }
        EventFlow::Consumed
    }

    fn handle_mouse(&mut self, mouse: MouseEvent) -> EventFlow {
        let (total, viewport) = self.metrics();
        match mouse.kind {
            MouseEventKind::ScrollUp => self.scroll.scroll_by(-3, total, viewport),
            MouseEventKind::ScrollDown => self.scroll.scroll_by(3, total, viewport),
            _ => return EventFlow::Ignored,
        }
        EventFlow::Consumed
    }
}

#[cfg(test)]
#[path = "transcript_tests.rs"]
mod tests;
