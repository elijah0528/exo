use super::wrap_line;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};

fn texts(lines: &[Line<'static>]) -> Vec<String> {
    lines
        .iter()
        .map(|line| {
            line.spans
                .iter()
                .map(|span| span.content.as_ref())
                .collect()
        })
        .collect()
}

#[test]
fn wraps_on_word_boundaries() {
    let line = Line::from("the quick brown fox jumps");
    assert_eq!(
        texts(&wrap_line(&line, 10)),
        vec!["the quick", "brown fox", "jumps"]
    );
}

#[test]
fn hard_splits_words_longer_than_the_width() {
    let line = Line::from("supercalifragilistic");
    assert_eq!(
        texts(&wrap_line(&line, 6)),
        vec!["superc", "alifra", "gilist", "ic"]
    );
}

#[test]
fn preserves_span_styles_across_a_break() {
    let red = Style::default().fg(Color::Red);
    let line = Line::from(vec![Span::raw("hello "), Span::styled("world", red)]);
    let wrapped = wrap_line(&line, 6);
    assert_eq!(texts(&wrapped), vec!["hello", "world"]);
    assert_eq!(wrapped[1].spans[0].style, red);
}

#[test]
fn empty_input_still_produces_one_row() {
    assert_eq!(wrap_line(&Line::default(), 10).len(), 1);
    assert_eq!(wrap_line(&Line::from("anything"), 0).len(), 1);
}
