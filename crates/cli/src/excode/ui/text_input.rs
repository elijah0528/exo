//! A single-line editor with readline-style keys and submit history.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::Widget;

use super::component::{Component, RenderCtx};

/// What the input produced from a key press.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputEvent {
    /// Enter was pressed with a non-empty value; the value has been taken and
    /// pushed onto the history.
    Submitted(String),
    /// The buffer or cursor changed.
    Changed,
    /// The key was not for this component.
    Ignored,
}

pub struct TextInput {
    prompt: String,
    value: String,
    /// Cursor position measured in characters, not bytes.
    cursor: usize,
    history: Vec<String>,
    /// Index into `history` while browsing; `None` means "editing the draft".
    history_index: Option<usize>,
    draft: String,
}

impl TextInput {
    pub fn new(prompt: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            value: String::new(),
            cursor: 0,
            history: Vec::new(),
            history_index: None,
            draft: String::new(),
        }
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }

    pub fn clear(&mut self) {
        self.value.clear();
        self.cursor = 0;
        self.history_index = None;
    }

    pub fn set_value(&mut self, value: impl Into<String>) {
        self.value = value.into();
        self.cursor = self.len();
    }

    pub fn history(&self) -> &[String] {
        &self.history
    }

    /// Feed a key press to the editor.
    ///
    /// This is deliberately not `Component::handle_key`: the caller needs the
    /// returned [`InputEvent`] to see submissions.
    pub fn input(&mut self, key: KeyEvent) -> InputEvent {
        let control = key.modifiers.contains(KeyModifiers::CONTROL);
        let alt = key.modifiers.contains(KeyModifiers::ALT);
        match key.code {
            KeyCode::Char(ch) if control => match ch {
                'a' => self.move_to(0),
                'e' => self.move_to(self.len()),
                'u' => self.delete_to_start(),
                'k' => self.delete_to_end(),
                'w' => self.delete_word_before(),
                _ => return InputEvent::Ignored,
            },
            KeyCode::Char(ch) if alt => match ch {
                'b' => self.move_to(self.word_start()),
                'f' => self.move_to(self.word_end()),
                _ => return InputEvent::Ignored,
            },
            KeyCode::Char(ch) => self.insert(ch),
            KeyCode::Backspace => self.backspace(),
            KeyCode::Delete => self.delete(),
            KeyCode::Left => self.move_to(self.cursor.saturating_sub(1)),
            KeyCode::Right => self.move_to((self.cursor + 1).min(self.len())),
            KeyCode::Home => self.move_to(0),
            KeyCode::End => self.move_to(self.len()),
            KeyCode::Up => self.history_prev(),
            KeyCode::Down => self.history_next(),
            KeyCode::Enter => {
                if self.value.trim().is_empty() {
                    return InputEvent::Ignored;
                }
                let submitted = std::mem::take(&mut self.value).trim().to_string();
                self.cursor = 0;
                self.history_index = None;
                self.draft.clear();
                if self.history.last() != Some(&submitted) {
                    self.history.push(submitted.clone());
                }
                return InputEvent::Submitted(submitted);
            }
            _ => return InputEvent::Ignored,
        }
        InputEvent::Changed
    }

    fn len(&self) -> usize {
        self.value.chars().count()
    }

    fn byte_index(&self, cursor: usize) -> usize {
        self.value
            .char_indices()
            .nth(cursor)
            .map(|(index, _)| index)
            .unwrap_or(self.value.len())
    }

    fn move_to(&mut self, cursor: usize) {
        self.cursor = cursor.min(self.len());
    }

    fn insert(&mut self, ch: char) {
        let at = self.byte_index(self.cursor);
        self.value.insert(at, ch);
        self.cursor += 1;
    }

    fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let at = self.byte_index(self.cursor - 1);
        self.value.remove(at);
        self.cursor -= 1;
    }

    fn delete(&mut self) {
        if self.cursor >= self.len() {
            return;
        }
        let at = self.byte_index(self.cursor);
        self.value.remove(at);
    }

    fn delete_to_start(&mut self) {
        let at = self.byte_index(self.cursor);
        self.value.drain(..at);
        self.cursor = 0;
    }

    fn delete_to_end(&mut self) {
        let at = self.byte_index(self.cursor);
        self.value.truncate(at);
    }

    fn delete_word_before(&mut self) {
        let start = self.word_start();
        let from = self.byte_index(start);
        let to = self.byte_index(self.cursor);
        self.value.drain(from..to);
        self.cursor = start;
    }

    /// Start of the word to the left of the cursor, skipping trailing spaces.
    fn word_start(&self) -> usize {
        let chars: Vec<char> = self.value.chars().collect();
        let mut index = self.cursor;
        while index > 0 && chars[index - 1].is_whitespace() {
            index -= 1;
        }
        while index > 0 && !chars[index - 1].is_whitespace() {
            index -= 1;
        }
        index
    }

    /// End of the word to the right of the cursor.
    fn word_end(&self) -> usize {
        let chars: Vec<char> = self.value.chars().collect();
        let mut index = self.cursor;
        while index < chars.len() && chars[index].is_whitespace() {
            index += 1;
        }
        while index < chars.len() && !chars[index].is_whitespace() {
            index += 1;
        }
        index
    }

    fn history_prev(&mut self) {
        if self.history.is_empty() {
            return;
        }
        let next = match self.history_index {
            None => {
                self.draft = self.value.clone();
                self.history.len() - 1
            }
            Some(0) => 0,
            Some(index) => index - 1,
        };
        self.history_index = Some(next);
        self.value = self.history[next].clone();
        self.cursor = self.len();
    }

    fn history_next(&mut self) {
        let Some(index) = self.history_index else {
            return;
        };
        if index + 1 >= self.history.len() {
            self.history_index = None;
            self.value = std::mem::take(&mut self.draft);
        } else {
            self.history_index = Some(index + 1);
            self.value = self.history[index + 1].clone();
        }
        self.cursor = self.len();
    }

    /// First visible character, so a long value scrolls with the cursor.
    fn window_start(&self, width: u16) -> usize {
        let width = usize::from(width).saturating_sub(self.prompt.chars().count() + 1);
        self.cursor.saturating_sub(width)
    }
}

impl Component for TextInput {
    fn render(&self, area: Rect, buf: &mut Buffer, ctx: RenderCtx<'_>) {
        if area.height == 0 {
            return;
        }
        let start = self.window_start(area.width);
        let visible: String = self.value.chars().skip(start).collect();
        let line = Line::from(vec![
            Span::styled(self.prompt.clone(), ctx.theme.prompt),
            Span::styled(visible, ctx.theme.text),
        ]);
        line.render(area, buf);
    }

    fn desired_height(&self, _width: u16) -> u16 {
        1
    }

    fn cursor(&self, area: Rect) -> Option<(u16, u16)> {
        let prompt = self.prompt.chars().count() as u16;
        let offset = (self.cursor - self.window_start(area.width)) as u16;
        Some((area.x + prompt + offset, area.y))
    }
}

#[cfg(test)]
#[path = "text_input_tests.rs"]
mod tests;
