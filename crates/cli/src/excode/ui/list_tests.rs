use super::{SelectionList, columns};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};

fn list_of(count: usize) -> SelectionList {
    let mut list = SelectionList::new();
    list.set_rows(
        (0..count)
            .map(|index| Line::from(index.to_string()))
            .collect(),
    );
    list
}

#[test]
fn selection_wraps_at_both_ends() {
    let mut list = list_of(3);
    list.select_wrapping(-1);
    assert_eq!(list.selected(), Some(2));
    list.select_wrapping(1);
    assert_eq!(list.selected(), Some(0));
}

#[test]
fn empty_list_has_no_selection() {
    let mut list = SelectionList::new();
    list.select_wrapping(1);
    assert_eq!(list.selected(), None);
}

#[test]
fn replacing_rows_clamps_the_selection() {
    let mut list = list_of(10);
    list.select(9);
    list.set_rows(vec![Line::from("only")]);
    assert_eq!(list.selected(), Some(0));
}

#[test]
fn window_follows_the_selection() {
    let list = list_of(20);
    assert_eq!(list.window_top(5), 0);
    let mut list = list;
    list.select(12);
    assert_eq!(list.window_top(5), 8);
    list.select(1);
    assert_eq!(list.window_top(5), 1);
}

#[test]
fn columns_pad_and_truncate_to_fixed_widths() {
    let red = Style::default().fg(Color::Red);
    let line = columns(&[(Span::styled("abcdef", red), 4), (Span::raw("x"), 3)]);
    let rendered: String = line
        .spans
        .iter()
        .map(|span| span.content.as_ref())
        .collect();
    assert_eq!(rendered, "abcdx  ");
    assert_eq!(line.spans[0].style, red);
}
