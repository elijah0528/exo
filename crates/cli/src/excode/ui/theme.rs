//! Semantic styling tokens.
//!
//! Components never name a concrete color. They ask the theme for a role
//! (`border_focused`, `added`, `warn`, ...) so a new palette is one struct away
//! and terminals without color support can be handled in one place.

use ratatui::style::{Color, Modifier, Style};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Theme {
    pub text: Style,
    pub dim: Style,
    pub title: Style,
    pub accent: Style,
    pub border: Style,
    pub border_focused: Style,
    pub selection: Style,
    pub success: Style,
    pub warn: Style,
    pub error: Style,
    pub info: Style,
    pub key: Style,
    pub prompt: Style,
    pub added: Style,
    pub removed: Style,
    pub hunk: Style,
    pub line_number: Style,
    pub gauge_filled: Style,
    pub gauge_empty: Style,
}

impl Theme {
    /// The palette used by the app. Only 16-color ANSI is used so the terminal
    /// keeps ownership of the actual hues.
    pub fn dark() -> Self {
        Self {
            text: Style::default(),
            dim: Style::default().add_modifier(Modifier::DIM),
            title: Style::default().add_modifier(Modifier::BOLD),
            accent: Style::default().fg(Color::Cyan),
            border: Style::default().add_modifier(Modifier::DIM),
            border_focused: Style::default().fg(Color::Cyan),
            selection: Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
            success: Style::default().fg(Color::Green),
            warn: Style::default().fg(Color::Yellow),
            error: Style::default().fg(Color::Red),
            info: Style::default().fg(Color::Blue),
            key: Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
            prompt: Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
            added: Style::default().fg(Color::Green),
            removed: Style::default().fg(Color::Red),
            hunk: Style::default().fg(Color::Magenta),
            line_number: Style::default().add_modifier(Modifier::DIM),
            gauge_filled: Style::default().fg(Color::Cyan),
            gauge_empty: Style::default().add_modifier(Modifier::DIM),
        }
    }

    /// A palette for terminals where color is unavailable or unwanted.
    pub fn monochrome() -> Self {
        let plain = Style::default();
        let dim = Style::default().add_modifier(Modifier::DIM);
        let bold = Style::default().add_modifier(Modifier::BOLD);
        Self {
            text: plain,
            dim,
            title: bold,
            accent: bold,
            border: dim,
            border_focused: bold,
            selection: Style::default().add_modifier(Modifier::REVERSED),
            success: plain,
            warn: bold,
            error: bold,
            info: plain,
            key: bold,
            prompt: bold,
            added: bold,
            removed: dim,
            hunk: bold,
            line_number: dim,
            gauge_filled: Style::default().add_modifier(Modifier::REVERSED),
            gauge_empty: dim,
        }
    }
}

impl Default for Theme {
    fn default() -> Self {
        Self::dark()
    }
}
