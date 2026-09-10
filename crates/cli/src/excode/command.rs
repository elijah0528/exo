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
    Acquire,
    Detach,
    Release,
    /// Show `git diff` from the attached sandbox.
    Diff,
    /// Restore a snapshot by its 1-based number, or the selected one.
    Restore(Option<usize>),
    Debug,
    Clear,
    Help,
    Quit,
    Unknown(String),
    /// A slash command that needs an argument it was not given.
    MissingArgument(&'static str),
}

/// Everything `/help` lists, and the source of truth for dispatch.
pub const COMMANDS: &[(&str, &str)] = &[
    ("/chat <message>", "ask the coding agent"),
    ("/run <command>", "run a shell command in the sandbox"),
    ("/acquire", "lease a warm sandbox"),
    ("/detach", "keep the lease but stop using it"),
    ("/release", "release the lease and checkpoint"),
    ("/diff", "show git diff from the sandbox"),
    ("/restore [#]", "restore a snapshot"),
    ("/debug", "toggle the sandbox utilization panel"),
    ("/clear", "clear the transcript"),
    ("/help", "list commands"),
    ("/quit", "exit"),
];

/// Parse a submitted line. Anything without a leading `/` is a shell command,
/// which keeps the common case one keystroke shorter.
pub fn parse(line: &str) -> Command {
    let line = line.trim();
    let Some(rest) = line.strip_prefix('/') else {
        return Command::Shell(line.to_string());
    };
    let (name, argument) = match rest.split_once(char::is_whitespace) {
        Some((name, argument)) => (name, argument.trim()),
        None => (rest, ""),
    };
    match name {
        "chat" if argument.is_empty() => Command::MissingArgument("/chat <message>"),
        "chat" => Command::Chat(argument.to_string()),
        "run" if argument.is_empty() => Command::MissingArgument("/run <command>"),
        "run" => Command::Shell(argument.to_string()),
        "acquire" => Command::Acquire,
        "detach" => Command::Detach,
        "release" => Command::Release,
        "diff" => Command::Diff,
        "restore" if argument.is_empty() => Command::Restore(None),
        "restore" => match argument.parse::<usize>() {
            Ok(number) => Command::Restore(Some(number)),
            Err(_) => Command::MissingArgument("/restore [#]"),
        },
        "debug" => Command::Debug,
        "clear" => Command::Clear,
        "help" => Command::Help,
        "quit" | "exit" => Command::Quit,
        other => Command::Unknown(other.to_string()),
    }
}

#[cfg(test)]
#[path = "command_tests.rs"]
mod tests;
