//! Ready-made [`Cell`] implementations for the transcript.
//!
//! These cover the shapes every terminal agent needs: a styled note, a command
//! with its output, and a diff. New cell types live next to these and only
//! need to produce lines.

use ratatui::style::Style;
use ratatui::text::{Line, Span};

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
            ]),
            width,
        );
        for output in self.output.lines() {
            lines.extend(wrap_line(
                &Line::from(vec![
                    Span::styled("  ", theme.dim),
                    Span::styled(output.to_string(), theme.dim),
                ]),
                width,
            ));
        }
        if self.failed() {
            let code = self
                .exit_code
                .map(|code| code.to_string())
                .unwrap_or_else(|| "signal".to_string());
            lines.push(Line::from(Span::styled(
                format!("  exited with {code}"),
                theme.error,
            )));
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
