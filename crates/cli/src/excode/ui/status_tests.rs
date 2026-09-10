use super::{KeyHint, Spinner, key_hints};
use crate::excode::ui::component::RenderCtx;
use crate::excode::ui::theme::Theme;
use std::time::{Duration, Instant};

#[test]
fn hints_are_separated() {
    let theme = Theme::dark();
    let line = key_hints(
        &[KeyHint::new("^C", "quit"), KeyHint::new("^D", "diff")],
        RenderCtx {
            theme: &theme,
            focused: true,
        },
    );
    let rendered: String = line
        .spans
        .iter()
        .map(|span| span.content.as_ref())
        .collect();
    assert_eq!(rendered, "^C quit   ^D diff");
}

#[test]
fn spinner_advances_with_time() {
    let spinner = Spinner::new();
    let start = Instant::now();
    let first = spinner.frame_at(start);
    let later = spinner.frame_at(start + Duration::from_millis(80));
    assert_ne!(first, later);
}
