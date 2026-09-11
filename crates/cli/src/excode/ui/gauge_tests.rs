use super::gauge_line;
use crate::excode::ui::component::RenderCtx;
use crate::excode::ui::theme::Theme;

fn rendered(filled: usize, total: usize, width: u16) -> String {
    let theme = Theme::dark();
    let ctx = RenderCtx {
        theme: &theme,
        focused: true,
    };
    gauge_line("busy", filled, total, width, ctx)
        .spans
        .iter()
        .map(|span| span.content.as_ref())
        .collect()
}

#[test]
fn empty_and_full_gauges() {
    assert_eq!(rendered(0, 4, 20), "busy ░░░░░░░░░░░ 0/4");
    assert_eq!(rendered(4, 4, 20), "busy ███████████ 4/4");
}

#[test]
fn partial_use_always_shows_at_least_one_cell() {
    assert_eq!(rendered(1, 100, 20), "busy █░░░░░░░░ 1/100");
}

#[test]
fn zero_total_renders_empty() {
    assert_eq!(rendered(0, 0, 20), "busy ░░░░░░░░░░░ 0/0");
}
