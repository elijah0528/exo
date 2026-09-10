use super::{Command, parse};

#[test]
fn bare_lines_are_chat_prompts() {
    assert_eq!(
        parse("  fix the failing test  "),
        Command::Chat("fix the failing test".to_string())
    );
}

#[test]
fn slash_commands_take_arguments() {
    assert_eq!(
        parse("/chat fix the test"),
        Command::Chat("fix the test".to_string())
    );
    assert_eq!(
        parse("/cmd  echo hi"),
        Command::Shell("echo hi".to_string())
    );
    assert_eq!(parse("/c 3"), Command::Connect(Some(3)));
    assert_eq!(parse("/c"), Command::Connect(None));
}

#[test]
fn missing_and_unknown_arguments_are_reported() {
    assert_eq!(parse("/chat"), Command::MissingArgument("/chat <message>"));
    assert_eq!(parse("/c x"), Command::MissingArgument("/c [#]"));
    assert_eq!(parse("/nope"), Command::Unknown("nope".to_string()));
}

#[test]
fn control_commands_parse() {
    assert_eq!(parse("/c"), Command::Connect(None));
    assert_eq!(parse("/dc"), Command::Disconnect);
    assert_eq!(parse("/diff"), Command::Diff);
    assert_eq!(parse("/snapshots"), Command::Snapshots);
    assert_eq!(parse("/copy"), Command::Copy);
    assert_eq!(parse("/raw"), Command::ToggleRaw);
    assert_eq!(parse("/exit"), Command::Quit);
}

#[test]
fn command_arguments_are_required() {
    assert_eq!(parse("/cmd"), Command::MissingArgument("/cmd <command>"));
}
