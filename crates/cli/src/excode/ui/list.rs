//! A selectable, scrolling list of pre-styled rows.
//!
//! The list owns selection and scroll only. Rows are plain [`Line`]s built by
//! the caller, so a "table" is just rows whose spans are padded to columns
//! (see [`columns`]).

use crossterm::event::{KeyCode, KeyEvent, MouseEvent, MouseEventKind};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};

use super::component::{Component, EventFlow, RenderCtx};
use super::scroll::render_scrollbar;

#[derive(Debug, Default)]
pub struct SelectionList {
    rows: Vec<Line<'static>>,
    selected: usize,
    /// First visible row. Resolved during render, which only has `&self`.
    top: std::cell::Cell<usize>,
}

impl SelectionList {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the rows, keeping the selected index where possible.
    pub fn set_rows(&mut self, rows: Vec<Line<'static>>) {
        self.rows = rows;
        self.selected = self.selected.min(self.rows.len().saturating_sub(1));
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn selected(&self) -> Option<usize> {
        (!self.rows.is_empty()).then_some(self.selected)
    }

    pub fn select(&mut self, index: usize) {
        if !self.rows.is_empty() {
            self.selected = index.min(self.rows.len() - 1);
        }
    }

    /// Move the selection, wrapping at both ends.
    pub fn select_wrapping(&mut self, delta: isize) {
        if self.rows.is_empty() {
            return;
        }
        let len = self.rows.len() as isize;
        self.selected = (self.selected as isize + delta).rem_euclid(len) as usize;
    }

    /// First visible row for a window of `visible` rows, keeping the selected
    /// row on screen. Stored so scrolling and selection stay in sync.
    pub fn window_top(&self, visible: usize) -> usize {
        if visible == 0 {
            return 0;
        }
        let mut top = self.top.get();
        if self.selected < top {
            top = self.selected;
        } else if self.selected >= top + visible {
            top = self.selected + 1 - visible;
        }
        let top = top.min(self.rows.len().saturating_sub(visible));
        self.top.set(top);
        top
    }
}

impl Component for SelectionList {
    fn render(&self, area: Rect, buf: &mut Buffer, ctx: RenderCtx<'_>) {
        let visible = usize::from(area.height);
        if visible == 0 || area.width == 0 {
            return;
        }
        let top = self.window_top(visible);
        for (row, line) in self.rows.iter().skip(top).take(visible).enumerate() {
            let y = area.y + row as u16;
            let selected = ctx.focused && top + row == self.selected;
            let line = if selected {
                line.clone().style(ctx.theme.selection)
            } else {
                line.clone()
            };
            buf.set_line(area.x, y, &line, area.width);
            if selected {
                buf.set_style(Rect::new(area.x, y, area.width, 1), ctx.theme.selection);
            }
        }
        render_scrollbar(area, buf, ctx.theme, self.rows.len(), top);
    }

    fn desired_height(&self, _width: u16) -> u16 {
        self.rows.len().min(u16::MAX as usize) as u16
    }

    fn handle_key(&mut self, key: KeyEvent) -> EventFlow {
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => self.select_wrapping(-1),
            KeyCode::Down | KeyCode::Char('j') => self.select_wrapping(1),
            KeyCode::Home => self.select(0),
            KeyCode::End => self.select(usize::MAX),
            _ => return EventFlow::Ignored,
        }
        EventFlow::Consumed
    }

    fn handle_mouse(&mut self, mouse: MouseEvent) -> EventFlow {
        match mouse.kind {
            MouseEventKind::ScrollUp => self.top.set(self.top.get().saturating_sub(1)),
            MouseEventKind::ScrollDown => self
                .top
                .set((self.top.get() + 1).min(self.rows.len().saturating_sub(1))),
            _ => return EventFlow::Ignored,
        }
        EventFlow::Consumed
    }
}

/// Pad `cells` into fixed-width columns, producing one row.
///
/// A cell wider than its column is truncated so columns never drift.
pub fn columns(cells: &[(Span<'static>, u16)]) -> Line<'static> {
    let mut spans = Vec::with_capacity(cells.len() * 2);
    for (span, width) in cells {
        let width = usize::from(*width);
        let text: String = span.content.chars().take(width).collect();
        let padding = width.saturating_sub(text.chars().count());
        spans.push(Span::styled(text, span.style));
        if padding > 0 {
            spans.push(Span::raw(" ".repeat(padding)));
        }
    }
    Line::from(spans)
}

#[cfg(test)]
#[path = "list_tests.rs"]
mod tests;
