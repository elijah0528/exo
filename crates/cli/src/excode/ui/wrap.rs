//! Word wrapping for styled [`Line`]s.
//!
//! Wrapping happens on the styled representation, not on plain strings, so a
//! cell can build one richly styled line and let the viewport decide how it
//! breaks. Words longer than the width are hard-split rather than overflowing.

use ratatui::text::{Line, Span};

/// Wrap a line to `width`, preserving per-span styling.
///
/// Returns at least one (possibly empty) line so callers can rely on the row
/// count matching the number of rendered rows.
pub fn wrap_line(line: &Line<'_>, width: u16) -> Vec<Line<'static>> {
    if width == 0 {
        return vec![Line::default()];
    }
    let width = usize::from(width);
    let mut rows: Vec<Vec<Span<'static>>> = Vec::new();
    let mut row: Vec<Span<'static>> = Vec::new();
    let mut used = 0usize;

    for span in &line.spans {
        for word in split_words(span.content.as_ref()) {
            let word_width = word.chars().count();
            if word.trim().is_empty() && used == 0 && !rows.is_empty() {
                // Never start a wrapped row with the whitespace we broke on.
                // Leading indentation on the first row is content, not a break.
                continue;
            }
            if used + word_width <= width {
                push_span(&mut row, word.to_string(), span.style);
                used += word_width;
                continue;
            }
            if word_width > width {
                // Hard-split a word that cannot fit on any row.
                let mut remaining = word;
                loop {
                    let (head, tail) = split_at_chars(remaining, width - used);
                    push_span(&mut row, head.to_string(), span.style);
                    used += head.chars().count();
                    if tail.is_empty() {
                        break;
                    }
                    rows.push(std::mem::take(&mut row));
                    used = 0;
                    remaining = tail;
                }
                continue;
            }
            rows.push(std::mem::take(&mut row));
            used = word_width;
            push_span(&mut row, word.to_string(), span.style);
        }
    }
    rows.push(row);
    rows.into_iter()
        .map(|spans| Line::from(trim_end(spans)).style(line.style))
        .collect()
}

/// Drop the whitespace a row ends on: it is a word separator, not content.
fn trim_end(mut spans: Vec<Span<'static>>) -> Vec<Span<'static>> {
    while let Some(last) = spans.last_mut() {
        let trimmed = last.content.trim_end();
        if trimmed.len() == last.content.len() {
            break;
        }
        if trimmed.is_empty() {
            spans.pop();
            continue;
        }
        last.content = trimmed.to_string().into();
        break;
    }
    spans
}

/// Wrap every line of a block, keeping their order.
pub fn wrap_lines(lines: &[Line<'_>], width: u16) -> Vec<Line<'static>> {
    lines
        .iter()
        .flat_map(|line| wrap_line(line, width))
        .collect()
}

/// Split into alternating word and whitespace chunks so breaks land on spaces.
fn split_words(text: &str) -> Vec<&str> {
    let mut chunks = Vec::new();
    let mut start = 0;
    let mut whitespace = None;
    for (index, ch) in text.char_indices() {
        let is_whitespace = ch.is_whitespace();
        match whitespace {
            Some(previous) if previous == is_whitespace => {}
            Some(_) => {
                chunks.push(&text[start..index]);
                start = index;
            }
            None => {}
        }
        whitespace = Some(is_whitespace);
    }
    if start < text.len() {
        chunks.push(&text[start..]);
    }
    chunks
}

fn split_at_chars(text: &str, chars: usize) -> (&str, &str) {
    let index = text
        .char_indices()
        .nth(chars)
        .map(|(index, _)| index)
        .unwrap_or(text.len());
    text.split_at(index)
}

fn push_span(row: &mut Vec<Span<'static>>, text: String, style: ratatui::style::Style) {
    if text.is_empty() {
        return;
    }
    match row.last_mut() {
        Some(last) if last.style == style => last.content.to_mut().push_str(&text),
        _ => row.push(Span::styled(text, style)),
    }
}

#[cfg(test)]
#[path = "wrap_tests.rs"]
mod tests;
