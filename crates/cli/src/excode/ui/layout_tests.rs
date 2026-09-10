use super::{Insets, RectExt};
use ratatui::layout::Rect;

#[test]
fn inset_shrinks_from_every_edge() {
    let area = Rect::new(2, 3, 20, 10);
    let inner = area.inset(Insets {
        top: 1,
        right: 2,
        bottom: 3,
        left: 4,
    });
    assert_eq!(inner, Rect::new(6, 4, 14, 6));
}

#[test]
fn inset_saturates_instead_of_underflowing() {
    let area = Rect::new(0, 0, 3, 2);
    assert_eq!(area.inset(Insets::all(5)), Rect::new(5, 5, 0, 0));
}

#[test]
fn centered_clamps_to_the_parent() {
    let area = Rect::new(0, 0, 20, 10);
    assert_eq!(area.centered(10, 4), Rect::new(5, 3, 10, 4));
    assert_eq!(area.centered(40, 40), area);
}
