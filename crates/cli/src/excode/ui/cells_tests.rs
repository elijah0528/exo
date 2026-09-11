use super::{CommandCell, Level, NoteCell};
use crate::excode::ui::theme::Theme;
use crate::excode::ui::transcript::Cell;

fn text(lines: &[ratatui::text::Line<'static>]) -> Vec<String> {
    lines
        .iter()
        .map(|line| {
            line.spans
                .iter()
                .map(|span| span.content.as_ref())
                .collect::<String>()
        })
        .collect()
}

#[test]
fn notes_are_marked_and_wrapped() {
    let theme = Theme::dark();
    let cell = NoteCell::new(Level::Error, "boom happened here");
    assert_eq!(
        text(&cell.lines(12, &theme)),
        vec!["✖ boom", "happened", "here"]
    );
}

#[test]
fn commands_show_output_and_failures() {
    let theme = Theme::dark();
    let cell = CommandCell::new("ls", "a\nb", Some(1));
    assert_eq!(
        text(&cell.lines(40, &theme)),
        vec!["$ ls", "  a", "  b", "  exited with 1"]
    );
}

#[test]
fn successful_commands_omit_the_exit_line() {
    let theme = Theme::dark();
    let cell = CommandCell::new("ls", "a", Some(0));
    assert_eq!(text(&cell.lines(40, &theme)), vec!["$ ls", "  a"]);
}
