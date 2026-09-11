//! Ready-made [`Cell`] implementations for the transcript.
//!
//! These cover the shapes every terminal agent needs: a styled note, a command
//! with its output, and a diff. New cell types live next to these and only
//! need to produce lines.

use ratatui::style::Style;
use ratatui::text::{Line, Span};
use std::cell::RefCell;
use std::rc::Rc;

use super::diff::{FileDiff, diff_lines, parse_unified_diff};
use super::theme::Theme;
use super::transcript::Cell;
use super::wrap::wrap_line;

/// Severity of a [`NoteCell`], mapped to a theme role at render time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Info,
    Success,
    Warn,
    Error,
    Muted,
}

impl Level {
    fn style(self, theme: &Theme) -> Style {
        match self {
            Level::Info => theme.info,
            Level::Success => theme.success,
            Level::Warn => theme.warn,
            Level::Error => theme.error,
            Level::Muted => theme.dim,
        }
    }

    fn marker(self) -> &'static str {
        match self {
            Level::Info => "· ",
            Level::Success => "✔ ",
            Level::Warn => "! ",
            Level::Error => "✖ ",
            Level::Muted => "  ",
        }
    }
}

/// A one-off status message.
pub struct NoteCell {
    level: Level,
    text: String,
}

/// A user-authored prompt with a distinct conversational prefix.
pub struct UserCell {
    text: String,
}

impl UserCell {
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }
}

impl Cell for UserCell {
    fn lines(&self, width: u16, theme: &Theme) -> Vec<Line<'static>> {
        let prefix = Span::styled("› ", theme.user_prompt);
        let continuation = Span::styled("  ", theme.user_prompt);
        let content_width = block_content_width(width);
        let wrapped = wrap_line(
            &Line::from(vec![
                prefix,
                Span::styled(self.text.clone(), theme.user_text),
            ]),
            content_width,
        );
        let mut lines = Vec::with_capacity(wrapped.len() + 2);
        lines.push(filled_block_line(width, theme.user_background));
        lines.extend(wrapped.into_iter().enumerate().map(|(index, mut line)| {
            if index > 0 {
                line.spans.insert(0, continuation.clone());
            }
            padded_block_line(line, width, theme.user_background)
        }));
        lines.push(filled_block_line(width, theme.user_background));
        lines
    }
}

/// A completed assistant response, using Codex's quiet bullet/indent pattern.
pub struct AssistantCell {
    text: String,
}

impl AssistantCell {
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }
}

impl Cell for AssistantCell {
    fn lines(&self, width: u16, theme: &Theme) -> Vec<Line<'static>> {
        assistant_lines(&self.text, width, theme)
    }
}

/// The assistant cell used while a response is streaming.
pub struct StreamingCell {
    text: Rc<RefCell<String>>,
}

/// State shared by a compact tool activity cell while the agent is running.
#[derive(Debug, Default)]
pub struct ToolState {
    pub name: String,
    pub output: Option<String>,
}

impl ToolState {
    pub fn running(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            output: None,
        }
    }

    pub fn complete(&mut self, output: impl Into<String>) {
        self.output = Some(output.into());
    }
}

/// A compact tool activity row. Tool payloads are intentionally summarized so
/// request/response JSON does not take over the conversation transcript.
pub struct ToolCell {
    state: Rc<RefCell<ToolState>>,
}

impl ToolCell {
    pub fn new(state: Rc<RefCell<ToolState>>) -> Self {
        Self { state }
    }
}

impl Cell for ToolCell {
    fn lines(&self, width: u16, theme: &Theme) -> Vec<Line<'static>> {
        let state = self.state.borrow();
        let content_width = block_content_width(width);
        let (marker, marker_style, label) = match &state.output {
            Some(_) => ("✓ ", theme.success, "completed"),
            None => ("· ", theme.accent, "running"),
        };
        let header = Line::from(vec![
            Span::styled(marker, marker_style),
            Span::styled("tool ", theme.dim),
            Span::styled(state.name.clone(), theme.tool_background),
            Span::styled(format!("  {label}"), theme.dim),
        ]);
        let mut lines = vec![padded_block_line(header, width, theme.tool_background)];
        if let Some(output) = &state.output
            && let Some(summary) = summarize_tool_output(output)
        {
            let summary = Line::from(vec![
                Span::styled("  └ ", theme.dim),
                Span::styled(summary, theme.dim),
            ]);
            lines.extend(
                wrap_line(&summary, content_width)
                    .into_iter()
                    .map(|line| padded_block_line(line, width, theme.tool_background)),
            );
        }
        lines
    }
}

fn summarize_tool_output(output: &str) -> Option<String> {
    let summary = output
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())?
        .replace(['{', '}', '[', ']', '"'], "");
    let summary = summary.split_whitespace().collect::<Vec<_>>().join(" ");
    (!summary.is_empty()).then(|| {
        summary
            .char_indices()
            .nth(120)
            .map_or(summary.clone(), |(index, _)| {
                format!("{}…", &summary[..index])
            })
    })
}

impl StreamingCell {
    pub fn new(text: Rc<RefCell<String>>) -> Self {
        Self { text }
    }
}

impl Cell for StreamingCell {
    fn lines(&self, width: u16, theme: &Theme) -> Vec<Line<'static>> {
        assistant_lines(&self.text.borrow(), width, theme)
    }
}

fn assistant_lines(text: &str, width: u16, theme: &Theme) -> Vec<Line<'static>> {
    let content_width = block_content_width(width);
    let mut lines = Vec::new();
    for (index, source) in text.lines().enumerate() {
        let prefix = if index == 0 { "• " } else { "  " };
        let line = Line::from(vec![
            Span::styled(prefix, theme.dim),
            Span::styled(source.to_string(), theme.text),
        ]);
        lines.extend(
            wrap_line(&line, content_width)
                .into_iter()
                .map(|line| padded_block_line(line, width, theme.assistant_background)),
        );
    }
    if lines.is_empty() {
        lines.push(Line::default());
    }
    lines
}

fn block_content_width(width: u16) -> u16 {
    width.saturating_sub(4).max(1)
}

fn padded_block_line(line: Line<'static>, width: u16, style: Style) -> Line<'static> {
    let used = line.width() as u16;
    let trailing = width.saturating_sub(used.saturating_add(2));
    let mut spans = Vec::with_capacity(line.spans.len() + 2);
    spans.push(Span::raw("  "));
    spans.extend(line.spans);
    spans.push(Span::raw(" ".repeat(usize::from(trailing))));
    Line::from(spans).style(style)
}

fn filled_block_line(width: u16, style: Style) -> Line<'static> {
    Line::from(Span::raw(" ".repeat(usize::from(width)))).style(style)
}

impl NoteCell {
    pub fn new(level: Level, text: impl Into<String>) -> Self {
        Self {
            level,
            text: text.into(),
        }
    }

    pub fn info(text: impl Into<String>) -> Self {
        Self::new(Level::Info, text)
    }

    pub fn success(text: impl Into<String>) -> Self {
        Self::new(Level::Success, text)
    }

    pub fn warn(text: impl Into<String>) -> Self {
        Self::new(Level::Warn, text)
    }

    pub fn error(text: impl Into<String>) -> Self {
        Self::new(Level::Error, text)
    }
}

impl Cell for NoteCell {
    fn lines(&self, width: u16, theme: &Theme) -> Vec<Line<'static>> {
        let style = self.level.style(theme);
        let line = Line::from(vec![
            Span::styled(self.level.marker(), style),
            Span::styled(self.text.clone(), style),
        ]);
        wrap_line(&line, width)
    }
}

/// A command, its output, and how it exited.
pub struct CommandCell {
    command: String,
    output: String,
    exit_code: Option<i32>,
}

impl CommandCell {
    pub fn new(
        command: impl Into<String>,
        output: impl Into<String>,
        exit_code: Option<i32>,
    ) -> Self {
        Self {
            command: command.into(),
            output: output.into(),
            exit_code,
        }
    }

    fn failed(&self) -> bool {
        !matches!(self.exit_code, Some(0) | None)
    }
}

impl Cell for CommandCell {
    fn lines(&self, width: u16, theme: &Theme) -> Vec<Line<'static>> {
        let mut lines = wrap_line(
            &Line::from(vec![
                Span::styled("$ ", theme.prompt),
                Span::styled(self.command.clone(), theme.text),
            ])
            .style(theme.tool_background),
            width,
        );
        for output in self.output.lines() {
            lines.extend(wrap_line(
                &Line::from(vec![
                    Span::styled("  ", theme.dim),
                    Span::styled(output.to_string(), theme.dim),
                ])
                .style(theme.tool_background),
                width,
            ));
        }
        if self.failed() {
            let code = self
                .exit_code
                .map(|code| code.to_string())
                .unwrap_or_else(|| "signal".to_string());
            lines.push(
                Line::from(Span::styled(format!("  exited with {code}"), theme.error))
                    .style(theme.tool_background),
            );
        }
        lines
    }
}

/// A unified diff rendered inline in the transcript.
pub struct DiffCell {
    files: Vec<FileDiff>,
}

impl DiffCell {
    pub fn from_unified(diff: &str) -> Self {
        Self {
            files: parse_unified_diff(diff),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
}

impl Cell for DiffCell {
    fn lines(&self, width: u16, theme: &Theme) -> Vec<Line<'static>> {
        diff_lines(&self.files, width, theme)
    }
}

#[cfg(test)]
#[path = "cells_tests.rs"]
mod tests;
