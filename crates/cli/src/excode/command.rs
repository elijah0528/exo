//! Slash-command parsing.
//!
//! Parsing is a pure function so the command surface can be tested without a
//! terminal or a sandbox pool, and so the app loop only matches on an enum.

/// A line the user submitted, resolved to an action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Run a shell command in the attached sandbox.
    Shell(String),
    /// Send a prompt to the coding agent.
    Chat(String),
    Connect(Option<usize>),
    Disconnect,
    /// Show `git diff` from the attached sandbox.
    Diff,
    Snapshots,
    Copy,
    ToggleRaw,
    Clear,
    Help,
    Quit,
    Unknown(String),
    /// A slash command that needs an argument it was not given.
    MissingArgument(&'static str),
}

/// Everything `/help` lists, and the source of truth for dispatch.
pub const COMMANDS: &[(&str, &str)] = &[
    ("/c [#]", "connect to a snapshot"),
    ("/dc", "disconnect and checkpoint the current snapshot"),
    ("<message>", "send a prompt to the coding agent"),
    (
        "/chat <message>",
        "send a prompt to the coding agent (alias)",
    ),
    ("/cmd <command>", "run a shell command in the sandbox"),
    ("/diff", "show git diff from the sandbox"),
    ("/snapshots", "show available snapshots"),
    ("/copy", "copy the latest agent response"),
    ("/raw", "toggle terminal selection mode"),
    ("/clear", "clear the transcript"),
    ("/help", "list commands"),
    ("/quit", "exit"),
];

/// Parse a submitted line. Anything without a leading `/` is a prompt for the
/// coding agent, which keeps the common case one keystroke shorter.
pub fn parse(line: &str) -> Command {
    let line = line.trim();
    if line.is_empty() {
        return Command::Shell(String::new());
    }
    let Some(rest) = line.strip_prefix('/') else {
        return Command::Chat(line.to_string());
    };
    let (name, argument) = match rest.split_once(char::is_whitespace) {
        Some((name, argument)) => (name, argument.trim()),
        None => (rest, ""),
    };
    match name {
        "chat" if argument.is_empty() => Command::MissingArgument("/chat <message>"),
        "chat" => Command::Chat(argument.to_string()),
        "cmd" if argument.is_empty() => Command::MissingArgument("/cmd <command>"),
        "cmd" => Command::Shell(argument.to_string()),
        "c" if argument.is_empty() => Command::Connect(None),
        "c" => match argument.parse::<usize>() {
            Ok(number) => Command::Connect(Some(number)),
            Err(_) => Command::MissingArgument("/c [#]"),
        },
        "diff" => Command::Diff,
        "snapshots" | "snapshot" | "s" => Command::Snapshots,
        "copy" => Command::Copy,
        "raw" => Command::ToggleRaw,
        "dc" => Command::Disconnect,
        "clear" => Command::Clear,
        "help" => Command::Help,
        "quit" | "exit" => Command::Quit,
        other => Command::Unknown(other.to_string()),
    }
}

#[cfg(test)]
#[path = "command_tests.rs"]
mod tests;
