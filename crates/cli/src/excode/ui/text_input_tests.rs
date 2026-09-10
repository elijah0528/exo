use super::{InputEvent, TextInput};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

fn key(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::NONE)
}

fn ctrl(ch: char) -> KeyEvent {
    KeyEvent::new(KeyCode::Char(ch), KeyModifiers::CONTROL)
}

fn type_text(input: &mut TextInput, text: &str) {
    for ch in text.chars() {
        input.input(key(KeyCode::Char(ch)));
    }
}

#[test]
fn edits_at_the_cursor() {
    let mut input = TextInput::new("› ");
    type_text(&mut input, "hello");
    input.input(key(KeyCode::Left));
    input.input(key(KeyCode::Char('!')));
    assert_eq!(input.value(), "hell!o");
    input.input(key(KeyCode::Backspace));
    assert_eq!(input.value(), "hello");
    input.input(key(KeyCode::Delete));
    assert_eq!(input.value(), "hell");
}

#[test]
fn readline_shortcuts() {
    let mut input = TextInput::new("› ");
    type_text(&mut input, "one two three");
    input.input(ctrl('w'));
    assert_eq!(input.value(), "one two ");
    input.input(ctrl('a'));
    input.input(ctrl('k'));
    assert_eq!(input.value(), "");

    type_text(&mut input, "keep this");
    input.input(ctrl('u'));
    assert_eq!(input.value(), "");
}

#[test]
fn submit_returns_the_value_and_records_history() {
    let mut input = TextInput::new("› ");
    type_text(&mut input, "  ");
    assert_eq!(input.input(key(KeyCode::Enter)), InputEvent::Ignored);

    type_text(&mut input, "run tests");
    assert_eq!(
        input.input(key(KeyCode::Enter)),
        InputEvent::Submitted("run tests".to_string())
    );
    assert!(input.is_empty());
    assert_eq!(input.history(), ["run tests".to_string()]);
}

#[test]
fn history_browses_back_to_the_draft() {
    let mut input = TextInput::new("› ");
    type_text(&mut input, "first");
    input.input(key(KeyCode::Enter));
    type_text(&mut input, "second");
    input.input(key(KeyCode::Enter));
    type_text(&mut input, "draft");

    input.input(key(KeyCode::Up));
    assert_eq!(input.value(), "second");
    input.input(key(KeyCode::Up));
    assert_eq!(input.value(), "first");
    input.input(key(KeyCode::Down));
    assert_eq!(input.value(), "second");
    input.input(key(KeyCode::Down));
    assert_eq!(input.value(), "draft");
}

#[test]
fn cursor_movement_is_character_aware() {
    let mut input = TextInput::new("› ");
    type_text(&mut input, "héllo→");
    input.input(key(KeyCode::Backspace));
    assert_eq!(input.value(), "héllo");
    input.input(key(KeyCode::Home));
    input.input(key(KeyCode::Char('x')));
    assert_eq!(input.value(), "xhéllo");
}
