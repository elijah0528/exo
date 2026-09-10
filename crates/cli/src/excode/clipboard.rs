//! Clipboard support for copying agent responses from the terminal UI.

use std::io::Write;
use std::process::{Command as ProcessCommand, Stdio};

use anyhow::{Result, bail};
use base64::{Engine, engine::general_purpose::STANDARD};

const MAX_COPY_BYTES: usize = 100_000;

/// Copy text using a native clipboard command when possible, then OSC 52.
/// OSC 52 matters for SSH sessions because the clipboard belongs to the local
/// terminal rather than the remote host.
pub fn copy(text: &str) -> Result<()> {
    if text.trim().is_empty() {
        bail!("there is no agent response to copy");
    }
    if text.len() > MAX_COPY_BYTES {
        bail!("agent response is too large to copy ({MAX_COPY_BYTES} byte limit)");
    }

    if !is_ssh_session() && copy_with_native_command(text) {
        return Ok(());
    }

    let encoded = STANDARD.encode(text);
    let mut stdout = std::io::stdout().lock();
    write!(stdout, "\x1b]52;c;{encoded}\x07")?;
    stdout.flush()?;
    Ok(())
}

fn copy_with_native_command(text: &str) -> bool {
    #[cfg(target_os = "macos")]
    let command = "pbcopy";
    #[cfg(target_os = "linux")]
    let command = if command_available("wl-copy") {
        "wl-copy"
    } else if command_available("xclip") {
        "xclip"
    } else {
        return false;
    };
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    let command = return false;

    let mut child = match ProcessCommand::new(command)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return false,
    };
    let Some(mut stdin) = child.stdin.take() else {
        return false;
    };
    if stdin.write_all(text.as_bytes()).is_err() {
        return false;
    }
    drop(stdin);
    child.wait().is_ok_and(|status| status.success())
}

#[cfg(target_os = "linux")]
fn command_available(command: &str) -> bool {
    ProcessCommand::new("sh")
        .args(["-c", "command -v \"$1\" >/dev/null 2>&1", "exo", command])
        .status()
        .is_ok_and(|status| status.success())
}

fn is_ssh_session() -> bool {
    std::env::var_os("SSH_TTY").is_some() || std::env::var_os("SSH_CONNECTION").is_some()
}
