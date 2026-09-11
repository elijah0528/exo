use super::Scroll;

#[test]
fn follows_the_tail_until_the_user_scrolls_up() {
    let mut scroll = Scroll::default();
    assert_eq!(scroll.top(100, 10), 90);

    scroll.scroll_by(-5, 100, 10);
    for _ in 0..5 {
        scroll.tick(100, 10);
    }
    assert_eq!(scroll.top(100, 10), 85);
    assert!(!scroll.is_pinned_to_bottom());

    // New content does not move a viewport the user is holding.
    assert_eq!(scroll.top(140, 10), 85);
}

#[test]
fn scrolling_back_to_the_end_re_pins() {
    let mut scroll = Scroll::default();
    scroll.scroll_by(-20, 100, 10);
    scroll.page_down(100, 10);
    scroll.page_down(100, 10);
    assert!(scroll.is_pinned_to_bottom());
    assert_eq!(scroll.top(100, 10), 90);
}

#[test]
fn clamps_at_both_ends() {
    let mut scroll = Scroll::default();
    scroll.scroll_by(-1000, 30, 10);
    for _ in 0..20 {
        scroll.tick(30, 10);
    }
    assert_eq!(scroll.top(30, 10), 0);
    scroll.scroll_by(1000, 30, 10);
    for _ in 0..20 {
        scroll.tick(30, 10);
    }
    assert_eq!(scroll.top(30, 10), 20);
}

#[test]
fn content_shorter_than_the_viewport_never_scrolls() {
    let mut scroll = Scroll::default();
    scroll.scroll_to_top();
    assert_eq!(scroll.top(3, 10), 0);
    scroll.scroll_by(5, 3, 10);
    assert_eq!(scroll.top(3, 10), 0);
}
