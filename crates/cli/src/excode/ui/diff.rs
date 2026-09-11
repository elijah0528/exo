//! Unified diff parsing and rendering.
//!
//! Parsing is separated from rendering so the same model can be shown inline
//! in the transcript ([`super::cells::DiffCell`]) or full screen
//! ([`DiffView`]), and so the parser is testable without a terminal.

use crossterm::event::{KeyEvent, MouseEvent};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};

use super::component::{Component, EventFlow, RenderCtx};
use super::scroll::Scroll;
use super::theme::Theme;
use super::wrap::wrap_line;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileDiff {
    pub path: String,
    pub hunks: Vec<Hunk>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hunk {
    pub header: String,
    pub lines: Vec<DiffLine>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiffLine {
    Context { old: u32, new: u32, text: String },
    Added { new: u32, text: String },
    Removed { old: u32, text: String },
}

/// Parse `git diff` style output. Unknown metadata lines are skipped, so both
/// `git diff` and `diff -u` output work.
pub fn parse_unified_diff(diff: &str) -> Vec<FileDiff> {
    let mut files: Vec<FileDiff> = Vec::new();
    let mut old_no = 0u32;
    let mut new_no = 0u32;
    for line in diff.lines() {
        if let Some(path) = line.strip_prefix("+++ ") {
            let path = path.strip_prefix("b/").unwrap_or(path);
            match files.last_mut() {
                Some(file) if file.path.is_empty() => file.path = path.to_string(),
                _ => files.push(FileDiff {
                    path: path.to_string(),
                    hunks: Vec::new(),
                }),
            }
            continue;
        }
        if line.starts_with("diff --git") {
            files.push(FileDiff {
                path: String::new(),
                hunks: Vec::new(),
            });
            continue;
        }
        if line.starts_with("--- ") {
            continue;
        }
        if line.starts_with("@@") {
            let Some(file) = files.last_mut() else {
                continue;
            };
            let (old, new) = parse_hunk_range(line);
            old_no = old;
            new_no = new;
            file.hunks.push(Hunk {
                header: line.to_string(),
                lines: Vec::new(),
            });
            continue;
        }
        let Some(hunk) = files.last_mut().and_then(|file| file.hunks.last_mut()) else {
            continue;
        };
        let Some(first) = line.chars().next() else {
            hunk.lines.push(DiffLine::Context {
                old: old_no,
                new: new_no,
                text: String::new(),
            });
            old_no += 1;
            new_no += 1;
            continue;
        };
        let text = line[first.len_utf8()..].to_string();
        match first {
            '+' => {
                hunk.lines.push(DiffLine::Added { new: new_no, text });
                new_no += 1;
            }
            '-' => {
                hunk.lines.push(DiffLine::Removed { old: old_no, text });
                old_no += 1;
            }
            ' ' => {
                hunk.lines.push(DiffLine::Context {
                    old: old_no,
                    new: new_no,
                    text,
                });
                old_no += 1;
                new_no += 1;
            }
            // "\ No newline at end of file" and other markers.
            _ => {}
        }
    }
    files.retain(|file| !file.hunks.is_empty());
    files
}

/// Starting old/new line numbers from an `@@ -a,b +c,d @@` header.
fn parse_hunk_range(header: &str) -> (u32, u32) {
    let mut old = 1;
    let mut new = 1;
    for token in header.split_whitespace() {
        let Some(rest) = token.strip_prefix('-').or_else(|| token.strip_prefix('+')) else {
            continue;
        };
        let start = rest
            .split(',')
            .next()
            .and_then(|value| value.parse::<u32>().ok());
        let Some(start) = start else {
            continue;
        };
        if token.starts_with('-') {
            old = start;
        } else {
            new = start;
        }
    }
    (old, new)
}

/// Total additions and deletions across `files`.
pub fn diff_stats(files: &[FileDiff]) -> (usize, usize) {
    files
        .iter()
        .flat_map(|file| file.hunks.iter())
        .flat_map(|hunk| hunk.lines.iter())
        .fold((0, 0), |(added, removed), line| match line {
            DiffLine::Added { .. } => (added + 1, removed),
            DiffLine::Removed { .. } => (added, removed + 1),
            DiffLine::Context { .. } => (added, removed),
        })
}

const GUTTER: u16 = 9;

/// Render files as styled lines: `old new ± text`, wrapped to `width`.
pub fn diff_lines(files: &[FileDiff], width: u16, theme: &Theme) -> Vec<Line<'static>> {
    let text_width = width.saturating_sub(GUTTER).max(8);
    let mut lines = Vec::new();
    for file in files {
        let (added, removed) = diff_stats(std::slice::from_ref(file));
        lines.push(Line::from(vec![
            Span::styled(file.path.clone(), theme.title),
            Span::raw("  "),
            Span::styled(format!("+{added}"), theme.added),
            Span::raw(" "),
            Span::styled(format!("-{removed}"), theme.removed),
        ]));
        for hunk in &file.hunks {
            lines.push(Line::from(Span::styled(hunk.header.clone(), theme.hunk)));
            for line in &hunk.lines {
                let (marker, numbers, text, style) = match line {
                    DiffLine::Added { new, text } => {
                        ("+", format!("{:>4} ", new), text, theme.added)
                    }
                    DiffLine::Removed { old, text } => {
                        ("-", format!("{:>4} ", old), text, theme.removed)
                    }
                    DiffLine::Context { old, .. } => {
                        (" ", format!("{:>4} ", old), text_of(line), theme.dim)
                    }
                };
                let gutter = Span::styled(format!("{numbers}{marker} "), theme.line_number);
                let wrapped = wrap_line(&Line::from(Span::styled(text.clone(), style)), text_width);
                for (index, part) in wrapped.into_iter().enumerate() {
                    let prefix = if index == 0 {
                        gutter.clone()
                    } else {
                        Span::raw(" ".repeat(usize::from(GUTTER)))
                    };
                    let mut spans = vec![prefix];
                    spans.extend(part.spans);
                    lines.push(Line::from(spans));
                }
            }
        }
        lines.push(Line::default());
    }
    lines
}

fn text_of(line: &DiffLine) -> &String {
    match line {
        DiffLine::Context { text, .. }
        | DiffLine::Added { text, .. }
        | DiffLine::Removed { text, .. } => text,
    }
}

/// A scrollable full-surface diff viewer.
pub struct DiffView {
    files: Vec<FileDiff>,
    scroll: Scroll,
    viewport: std::cell::Cell<usize>,
    lines: std::cell::Cell<usize>,
}

impl DiffView {
    pub fn from_unified(diff: &str) -> Self {
        Self {
            files: parse_unified_diff(diff),
            scroll: {
                let mut scroll = Scroll::default();
                scroll.scroll_to_top();
                scroll
            },
            viewport: std::cell::Cell::new(0),
            lines: std::cell::Cell::new(0),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    pub fn stats(&self) -> (usize, usize) {
        diff_stats(&self.files)
    }

    pub fn files(&self) -> usize {
        self.files.len()
    }
}

impl Component for DiffView {
    fn render(&self, area: Rect, buf: &mut Buffer, ctx: RenderCtx<'_>) {
        if area.width == 0 || area.height == 0 {
            return;
        }
        let text_width = area.width.max(1);
        let lines = diff_lines(&self.files, text_width, ctx.theme);
        self.lines.set(lines.len());
        let viewport = usize::from(area.height);
        self.viewport.set(viewport);
        let top = self.scroll.top(lines.len(), viewport);
        for (row, line) in lines.iter().skip(top).take(viewport).enumerate() {
            buf.set_line(area.x, area.y + row as u16, line, text_width);
        }
    }

    fn handle_key(&mut self, key: KeyEvent) -> EventFlow {
        let total = self.lines.get();
        let viewport = self.viewport.get().max(1);
        match key.code {
            crossterm::event::KeyCode::Up => self.scroll.scroll_by(-1, total, viewport),
            crossterm::event::KeyCode::Down => self.scroll.scroll_by(1, total, viewport),
            crossterm::event::KeyCode::PageUp => self.scroll.page_up(total, viewport),
            crossterm::event::KeyCode::PageDown => self.scroll.page_down(total, viewport),
            crossterm::event::KeyCode::Home => self.scroll.scroll_to_top(),
            crossterm::event::KeyCode::End => self.scroll.scroll_to_bottom(),
            _ => return EventFlow::Ignored,
        }
        EventFlow::Consumed
    }

    fn handle_mouse(&mut self, mouse: MouseEvent) -> EventFlow {
        let total = self.lines.get();
        let viewport = self.viewport.get().max(1);
        match mouse.kind {
            crossterm::event::MouseEventKind::ScrollUp => {
                self.scroll.scroll_by(-3, total, viewport)
            }
            crossterm::event::MouseEventKind::ScrollDown => {
                self.scroll.scroll_by(3, total, viewport)
            }
            _ => return EventFlow::Ignored,
        }
        EventFlow::Consumed
    }
}

#[cfg(test)]
#[path = "diff_tests.rs"]
mod tests;
