use std::time::{Duration, Instant};

use async_trait::async_trait;
use executor::{ToolDefinition, ensure_shell_sandbox};
use exoharness::{Result, RunInSandboxRequest, SandboxProcess, ToolResult};
use futures::io::AsyncReadExt;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use super::registry::{ToolContext, ToolHandler};

const DEFAULT_TIMEOUT_MS: u64 = 120_000;
const DEFAULT_READ_LIMIT: usize = 500;
const DEFAULT_LIST_DEPTH: u32 = 2;
const OUTPUT_LIMIT: usize = 16 * 1024;
const OUTPUT_HEAD: usize = 8 * 1024;

#[derive(Debug, Deserialize)]
struct ShellArguments {
    command: String,
    workdir: Option<String>,
    timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ReadFileArguments {
    path: String,
    offset: Option<usize>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ListFilesArguments {
    path: Option<String>,
    max_depth: Option<u32>,
}

pub struct ShellTool;
pub struct ReadFileTool;
pub struct ListFilesTool;

fn definition(
    name: &str,
    description: &str,
    properties: Value,
    required: &[&str],
) -> ToolDefinition {
    ToolDefinition {
        name: name.to_string(),
        description: description.to_string(),
        parameters: json!({
            "type": "object",
            "additionalProperties": false,
            "properties": properties,
            "required": required,
        }),
    }
}

#[async_trait]
impl ToolHandler for ShellTool {
    fn definition(&self) -> ToolDefinition {
        definition(
            "shell",
            "Run a shell command in the conversation sandbox.",
            json!({
                "command": {"type": "string"},
                "workdir": {"type": ["string", "null"]},
                "timeout_ms": {"type": ["integer", "null"]},
            }),
            &["command"],
        )
    }

    async fn execute(
        &self,
        ctx: &ToolContext<'_>,
        arguments: Map<String, Value>,
    ) -> Result<ToolResult> {
        let args: ShellArguments = serde_json::from_value(Value::Object(arguments))?;
        let program = ctx
            .conversation_config
            .shell_program
            .clone()
            .ok_or_else(|| anyhow::anyhow!("shell tool is not enabled for this conversation"))?;
        let timeout_ms = args.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS);
        let command = match args.workdir {
            Some(workdir) => {
                let quoted = shlex::try_quote(&workdir)
                    .map_err(|_| anyhow::anyhow!("workdir cannot be represented safely"))?;
                format!("cd -- {quoted} && {}", args.command)
            }
            None => args.command,
        };
        let sandbox_id =
            ensure_shell_sandbox(ctx.conversation, ctx.agent_config, ctx.conversation_config)
                .await?;
        let started = Instant::now();
        let process = ctx
            .conversation
            .run_in_sandbox(RunInSandboxRequest {
                id: sandbox_id,
                command: vec![program, "-lc".to_string(), command],
                env: Default::default(),
            })
            .await?;
        let result = tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            read_process_output(process),
        )
        .await;
        let duration_ms = started.elapsed().as_millis() as u64;
        match result {
            Ok(result) => {
                let (stdout, stderr, exit_code) = result?;
                let (stdout, stdout_truncated) = truncate_output(&stdout);
                let (stderr, stderr_truncated) = truncate_output(&stderr);
                Ok(json!({
                    "stdout": stdout,
                    "stderr": stderr,
                    "exit_code": exit_code,
                    "duration_ms": duration_ms,
                    "truncated": stdout_truncated || stderr_truncated,
                }))
            }
            Err(_) => Ok(json!({
                "stdout": "",
                "stderr": format!("timed out after {timeout_ms}ms"),
                "exit_code": -1,
                "duration_ms": duration_ms,
                "truncated": false,
            })),
        }
    }
}

#[async_trait]
impl ToolHandler for ReadFileTool {
    fn definition(&self) -> ToolDefinition {
        definition(
            "read_file",
            "Read a line-numbered range from a file in the conversation sandbox.",
            json!({
                "path": {"type": "string"},
                "offset": {"type": ["integer", "null"]},
                "limit": {"type": ["integer", "null"]},
            }),
            &["path"],
        )
    }

    async fn execute(
        &self,
        ctx: &ToolContext<'_>,
        arguments: Map<String, Value>,
    ) -> Result<ToolResult> {
        let args: ReadFileArguments = serde_json::from_value(Value::Object(arguments))?;
        let program = ctx
            .conversation_config
            .shell_program
            .clone()
            .ok_or_else(|| anyhow::anyhow!("read_file requires a shell program"))?;
        let sandbox_id =
            ensure_shell_sandbox(ctx.conversation, ctx.agent_config, ctx.conversation_config)
                .await?;
        let path = shlex::try_quote(&args.path)
            .map_err(|_| anyhow::anyhow!("path cannot be represented safely"))?;
        let process = ctx
            .conversation
            .run_in_sandbox(RunInSandboxRequest {
                id: sandbox_id,
                command: vec![program, "-lc".to_string(), format!("cat -- {path}")],
                env: Default::default(),
            })
            .await?;
        let (stdout, stderr, exit_code) = read_process_output(process).await?;
        if exit_code != 0 {
            return Err(anyhow::anyhow!("read_file failed: {stderr}"));
        }
        let (content, total_lines) = format_file_content(
            &stdout,
            args.offset.unwrap_or(0),
            args.limit.unwrap_or(DEFAULT_READ_LIMIT),
        );
        Ok(json!({"content": content, "total_lines": total_lines}))
    }
}

#[async_trait]
impl ToolHandler for ListFilesTool {
    fn definition(&self) -> ToolDefinition {
        definition(
            "list_files",
            "List files and directories under a path in the conversation sandbox.",
            json!({
                "path": {"type": ["string", "null"]},
                "max_depth": {"type": ["integer", "null"]},
            }),
            &[],
        )
    }

    async fn execute(
        &self,
        ctx: &ToolContext<'_>,
        arguments: Map<String, Value>,
    ) -> Result<ToolResult> {
        let args: ListFilesArguments = serde_json::from_value(Value::Object(arguments))?;
        let program = ctx
            .conversation_config
            .shell_program
            .clone()
            .ok_or_else(|| anyhow::anyhow!("list_files requires a shell program"))?;
        let path = shlex::try_quote(args.path.as_deref().unwrap_or("."))
            .map_err(|_| anyhow::anyhow!("path cannot be represented safely"))?;
        let depth = args.max_depth.unwrap_or(DEFAULT_LIST_DEPTH);
        let sandbox_id =
            ensure_shell_sandbox(ctx.conversation, ctx.agent_config, ctx.conversation_config)
                .await?;
        let process = ctx
            .conversation
            .run_in_sandbox(RunInSandboxRequest {
                id: sandbox_id,
                command: vec![
                    program,
                    "-lc".to_string(),
                    format!("find {path} -maxdepth {depth} -not -path '*/.git/*'"),
                ],
                env: Default::default(),
            })
            .await?;
        let (stdout, stderr, exit_code) = read_process_output(process).await?;
        if exit_code != 0 {
            return Err(anyhow::anyhow!("list_files failed: {stderr}"));
        }
        Ok(json!({"entries": stdout.lines().map(str::to_string).collect::<Vec<_>>() }))
    }
}

async fn read_process_output(process: Box<dyn SandboxProcess>) -> Result<(String, String, i32)> {
    let parts = process.into_parts();
    let mut stdout = parts.stdout;
    let mut stderr = parts.stderr;
    drop(parts.stdin);
    let mut stdout_bytes = Vec::new();
    let mut stderr_bytes = Vec::new();
    let (stdout_result, stderr_result, wait_result) = tokio::join!(
        stdout.read_to_end(&mut stdout_bytes),
        stderr.read_to_end(&mut stderr_bytes),
        parts.wait,
    );
    stdout_result?;
    stderr_result?;
    Ok((
        String::from_utf8_lossy(&stdout_bytes).into_owned(),
        String::from_utf8_lossy(&stderr_bytes).into_owned(),
        wait_result?,
    ))
}

pub fn truncate_output(value: &str) -> (String, bool) {
    let bytes = value.as_bytes();
    if bytes.len() <= OUTPUT_LIMIT {
        return (value.to_string(), false);
    }
    let omitted = bytes.len() - OUTPUT_LIMIT;
    let marker = format!("\n[... {omitted} bytes truncated ...]\n");
    let head_end = OUTPUT_HEAD.min(bytes.len());
    let tail_start = bytes.len().saturating_sub(OUTPUT_HEAD);
    let mut output = String::from_utf8_lossy(&bytes[..head_end]).into_owned();
    output.push_str(&marker);
    output.push_str(&String::from_utf8_lossy(&bytes[tail_start..]));
    (output, true)
}

pub fn format_file_content(value: &str, offset: usize, limit: usize) -> (String, usize) {
    let lines: Vec<&str> = value.lines().collect();
    let total_lines = lines.len();
    let content = lines
        .iter()
        .enumerate()
        .skip(offset)
        .take(limit)
        .map(|(index, line)| format!("{}\t{line}", index + 1))
        .collect::<Vec<_>>()
        .join("\n");
    (content, total_lines)
}
