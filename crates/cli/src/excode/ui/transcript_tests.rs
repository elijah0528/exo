use super::{Cell, Transcript};
use crate::excode::ui::component::{Component, RenderCtx};
use crate::excode::ui::theme::Theme;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::Line;

struct Fixed(Vec<&'static str>);

impl Cell for Fixed {
    fn lines(&self, _width: u16, _theme: &Theme) -> Vec<Line<'static>> {
        self.0.iter().map(|text| Line::from(*text)).collect()
    }
}

fn render(transcript: &Transcript, theme: &Theme, area: Rect) -> Vec<String> {
    let mut buf = Buffer::empty(area);
    transcript.render(
        area,
        &mut buf,
        RenderCtx {
            theme,
            focused: true,
        },
    );
    (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<String>()
                .trim_end()
                .to_string()
        })
        .collect()
}

#[test]
fn tail_is_visible_by_default() {
    let theme = Theme::dark();
    let mut transcript = Transcript::new();
    transcript.push(Fixed(vec!["one", "two", "three", "four"]));
    let rendered = render(&transcript, &theme, Rect::new(0, 0, 10, 2));
    // The last column is the scrollbar: four lines do not fit in two rows.
    assert_eq!(
        rendered,
        vec!["three    │".to_string(), "four     █".to_string()]
    );
    assert!(transcript.is_following_tail());
}

#[test]
fn scrolling_up_unpins_and_end_repins() {
    let theme = Theme::dark();
    let mut transcript = Transcript::new();
    transcript.push(Fixed(vec!["one", "two", "three", "four"]));
    let area = Rect::new(0, 0, 10, 2);
    render(&transcript, &theme, area);

    transcript.handle_key(crossterm::event::KeyEvent::from(
        crossterm::event::KeyCode::Up,
    ));
    assert!(!transcript.is_following_tail());
    assert_eq!(
        render(&transcript, &theme, area),
        vec!["two      │".to_string(), "three    █".to_string()]
    );

    transcript.handle_key(crossterm::event::KeyEvent::from(
        crossterm::event::KeyCode::End,
    ));
    assert!(transcript.is_following_tail());
    assert_eq!(
        render(&transcript, &theme, area),
        vec!["three    │".to_string(), "four     █".to_string()]
    );
}

#[test]
fn new_cells_extend_the_layout() {
    let theme = Theme::dark();
    let mut transcript = Transcript::new();
    transcript.push(Fixed(vec!["one"]));
    assert_eq!(transcript.line_count(20, &theme), 1);
    transcript.push(Fixed(vec!["two", "three"]));
    assert_eq!(transcript.line_count(20, &theme), 3);
    transcript.clear();
    assert_eq!(transcript.line_count(20, &theme), 0);
}
