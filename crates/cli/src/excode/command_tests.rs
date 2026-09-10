use super::{Command, parse};

#[test]
fn bare_lines_are_shell_commands() {
    assert_eq!(parse("  ls -la  "), Command::Shell("ls -la".to_string()));
}

#[test]
fn slash_commands_take_arguments() {
    assert_eq!(
        parse("/chat fix the test"),
        Command::Chat("fix the test".to_string())
    );
    assert_eq!(
        parse("/run  echo hi"),
        Command::Shell("echo hi".to_string())
    );
    assert_eq!(parse("/restore 3"), Command::Restore(Some(3)));
    assert_eq!(parse("/restore"), Command::Restore(None));
}

#[test]
fn missing_and_unknown_arguments_are_reported() {
    assert_eq!(parse("/chat"), Command::MissingArgument("/chat <message>"));
    assert_eq!(
        parse("/restore x"),
        Command::MissingArgument("/restore [#]")
    );
    assert_eq!(parse("/nope"), Command::Unknown("nope".to_string()));
}

#[test]
fn control_commands_parse() {
    assert_eq!(parse("/diff"), Command::Diff);
    assert_eq!(parse("/debug"), Command::Debug);
    assert_eq!(parse("/exit"), Command::Quit);
}
