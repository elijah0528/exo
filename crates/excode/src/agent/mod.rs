//! A small coding-agent loop built from Exo's model and sandbox abstractions.

mod prompts;
mod tools;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use executor::{ModelClient, ModelRequest, ModelResponse, PendingToolCall};
use lingua::{
    Message,
    universal::{
        AssistantContent, AssistantContentPart, ToolContentPart, ToolResultContentPart, UserContent,
    },
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc::UnboundedSender;

use crate::{ManagedSandboxCapability, ManagedSandboxPool};

pub use prompts::SYSTEM_PROMPT;
pub use tools::coding_tool_definitions;

#[derive(Debug, Clone)]
pub struct CodingAgentConfig {
    pub model: String,
    pub api_key: Option<String>,
    pub base_url: Option<String>,
    pub max_output_tokens: Option<i64>,
    pub max_tool_rounds: u32,
    pub command_timeout: Duration,
}

impl Default for CodingAgentConfig {
    fn default() -> Self {
        Self {
            model: "default".to_string(),
            api_key: None,
            base_url: None,
            max_output_tokens: None,
            max_tool_rounds: 20,
            command_timeout: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CodingTask {
    pub worker_id: String,
    pub prompt: String,
}

#[derive(Debug, Clone)]
pub struct CodingResult {
    pub response: String,
    pub rounds: u32,
    pub tools: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum CodingAgentEvent {
    TextChunk(String),
    ToolCall { name: String },
    ToolResult { name: String, output: String },
}

pub struct CodingAgent<M> {
    model: Arc<M>,
    pool: Arc<dyn ManagedSandboxPool>,
    config: CodingAgentConfig,
}

impl<M> Clone for CodingAgent<M> {
    fn clone(&self) -> Self {
        Self {
            model: Arc::clone(&self.model),
            pool: Arc::clone(&self.pool),
            config: self.config.clone(),
        }
    }
}

impl<M> CodingAgent<M>
where
    M: ModelClient + 'static,
{
    pub fn new(
        model: Arc<M>,
        pool: Arc<dyn ManagedSandboxPool>,
        config: CodingAgentConfig,
    ) -> Self {
        Self {
            model,
            pool,
            config,
        }
    }

    pub async fn run(&self, task: CodingTask) -> Result<CodingResult> {
        let leased = self.pool.acquire_any(task.worker_id).await?;
        let result = self
            .run_on_lease(&leased.lease, leased.sandbox.as_ref(), &task.prompt)
            .await;
        let release = self.pool.release(&leased.lease).await;

        match (result, release) {
            (Ok(result), Ok(_)) => Ok(result),
            (Err(error), Ok(_)) => Err(error),
            (Ok(_), Err(error)) => Err(error).context("failed to release coding sandbox"),
            (Err(error), Err(release_error)) => Err(error.context(format!(
                "also failed to release coding sandbox: {release_error}"
            ))),
        }
    }

    /// Run a prompt against a sandbox that the caller already owns.
    ///
    /// The lease remains attached to the caller. This is the entry point for
    /// interactive clients that want the agent to edit their active sandbox.
    pub async fn run_on_lease(
        &self,
        lease: &crate::SandboxLease,
        sandbox: &dyn ManagedSandboxCapability,
        prompt: &str,
    ) -> Result<CodingResult> {
        self.run_with_sandbox(lease, sandbox, prompt).await
    }

    pub async fn run_on_lease_streaming(
        &self,
        lease: &crate::SandboxLease,
        sandbox: &dyn ManagedSandboxCapability,
        prompt: &str,
        events: UnboundedSender<CodingAgentEvent>,
    ) -> Result<CodingResult> {
        self.run_with_sandbox_and_events(lease, sandbox, prompt, Some(&events))
            .await
    }

    async fn run_with_sandbox(
        &self,
        lease: &crate::SandboxLease,
        sandbox: &dyn ManagedSandboxCapability,
        prompt: &str,
    ) -> Result<CodingResult> {
        self.run_with_sandbox_and_events(lease, sandbox, prompt, None)
            .await
    }

    async fn run_with_sandbox_and_events(
        &self,
        lease: &crate::SandboxLease,
        sandbox: &dyn ManagedSandboxCapability,
        prompt: &str,
        events: Option<&UnboundedSender<CodingAgentEvent>>,
    ) -> Result<CodingResult> {
        let mut messages = vec![
            Message::System {
                content: UserContent::String(SYSTEM_PROMPT.to_string()),
            },
            Message::User {
                content: UserContent::String(prompt.to_string()),
            },
        ];
        let mut tools = Vec::new();

        for round in 0..self.config.max_tool_rounds {
            self.pool.heartbeat(lease).await?;
            let request = ModelRequest {
                model: self.config.model.clone(),
                api_key: self.config.api_key.clone(),
                base_url: self.config.base_url.clone(),
                messages: messages.clone(),
                tools: coding_tool_definitions(),
                max_output_tokens: self.config.max_output_tokens,
            };
            let response = self.complete_round(request, events).await?;

            messages.extend(response.messages.clone());
            if response.tool_calls.is_empty() {
                return Ok(CodingResult {
                    response: assistant_text(&response.messages),
                    rounds: round + 1,
                    tools,
                });
            }

            for call in response.tool_calls {
                tools.push(call.request.function_name.clone());
                if let Some(events) = events {
                    events
                        .send(CodingAgentEvent::ToolCall {
                            name: call.request.function_name.clone(),
                        })
                        .map_err(|_| anyhow::anyhow!("coding agent event receiver dropped"))?;
                }
                let output = execute_tool(sandbox, &call, self.config.command_timeout).await;
                if let Some(events) = events {
                    let output = serde_json::to_string(&output)
                        .context("failed to serialize tool output")?;
                    events
                        .send(CodingAgentEvent::ToolResult {
                            name: call.request.function_name.clone(),
                            output,
                        })
                        .map_err(|_| anyhow::anyhow!("coding agent event receiver dropped"))?;
                }
                messages.push(Message::Tool {
                    content: vec![ToolContentPart::ToolResult(ToolResultContentPart {
                        tool_call_id: call.tool_call_id,
                        tool_name: call.request.function_name,
                        output: lingua::serde_json::to_value(output)
                            .context("failed to convert tool output")?,
                        provider_options: None,
                    })],
                });
            }
        }

        bail!(
            "coding agent exceeded {} tool rounds",
            self.config.max_tool_rounds
        )
    }

    async fn complete_round(
        &self,
        request: ModelRequest,
        events: Option<&UnboundedSender<CodingAgentEvent>>,
    ) -> Result<ModelResponse> {
        let Some(events) = events else {
            return self.model.complete(request).await;
        };
        let mut stream = self.model.complete_stream(request).await?;
        while let Some(chunk) = stream.next_chunk().await? {
            let text = stream_chunk_text(&chunk);
            if !text.is_empty() {
                events
                    .send(CodingAgentEvent::TextChunk(text))
                    .map_err(|_| anyhow::anyhow!("coding agent event receiver dropped"))?;
            }
        }
        stream.finish().await
    }
}

fn stream_chunk_text(chunk: &lingua::UniversalStreamChunk) -> String {
    let mut text = String::new();
    for choice in &chunk.choices {
        if let Some(delta) = choice.delta_view()
            && let Some(content) = delta.content
        {
            text.push_str(&content);
        }
    }
    text
}

fn assistant_text(messages: &[Message]) -> String {
    messages
        .iter()
        .filter_map(|message| match message {
            Message::Assistant { content, .. } => match content {
                AssistantContent::String(text) => Some(text.clone()),
                AssistantContent::Array(parts) => Some(
                    parts
                        .iter()
                        .filter_map(|part| match part {
                            AssistantContentPart::Text(text) => Some(text.text.clone()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join("\n"),
                ),
            },
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

async fn execute_tool(
    sandbox: &dyn ManagedSandboxCapability,
    call: &PendingToolCall,
    timeout: Duration,
) -> Value {
    let result = match call.request.function_name.as_str() {
        "shell" => run_shell(sandbox, &call.request.arguments, timeout).await,
        "read_file" => run_read_file(sandbox, &call.request.arguments, timeout).await,
        "list_files" => run_list_files(sandbox, &call.request.arguments, timeout).await,
        name => Err(anyhow::anyhow!("unknown coding tool: {name}")),
    };

    match result {
        Ok(output) => json!({
            "ok": output.ok,
            "exit_code": output.exit_code,
            "stdout": output.stdout,
            "stderr": output.stderr,
            "cwd": output.cwd,
        }),
        Err(error) => json!({ "ok": false, "error": error.to_string() }),
    }
}

#[derive(Debug, Deserialize)]
struct ShellArguments {
    command: String,
    cwd: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PathArguments {
    path: Option<String>,
}

async fn run_shell(
    sandbox: &dyn ManagedSandboxCapability,
    arguments: &serde_json::Map<String, Value>,
    timeout: Duration,
) -> Result<exoharness::SandboxCommandOutput> {
    let args: ShellArguments = serde_json::from_value(Value::Object(arguments.clone()))
        .context("shell arguments must contain a command")?;
    sandbox
        .exec(&exoharness::SandboxCommand {
            argv: vec!["/bin/sh".to_string(), "-lc".to_string(), args.command],
            env: Default::default(),
            display_argv: None,
            cwd: args.cwd,
            timeout: Some(timeout),
        })
        .await
}

async fn run_read_file(
    sandbox: &dyn ManagedSandboxCapability,
    arguments: &serde_json::Map<String, Value>,
    timeout: Duration,
) -> Result<exoharness::SandboxCommandOutput> {
    let args: PathArguments = serde_json::from_value(Value::Object(arguments.clone()))
        .context("read_file arguments must contain a path")?;
    let path = args.path.context("read_file path is required")?;
    sandbox
        .exec(&exoharness::SandboxCommand {
            argv: vec!["cat".to_string(), "--".to_string(), path],
            env: Default::default(),
            display_argv: None,
            cwd: None,
            timeout: Some(timeout),
        })
        .await
}

async fn run_list_files(
    sandbox: &dyn ManagedSandboxCapability,
    arguments: &serde_json::Map<String, Value>,
    timeout: Duration,
) -> Result<exoharness::SandboxCommandOutput> {
    let args: PathArguments = serde_json::from_value(Value::Object(arguments.clone()))
        .context("list_files arguments are invalid")?;
    sandbox
        .exec(&exoharness::SandboxCommand {
            argv: vec![
                "find".to_string(),
                args.path.unwrap_or_else(|| ".".to_string()),
                "-maxdepth".to_string(),
                "2".to_string(),
                "-type".to_string(),
                "f".to_string(),
            ],
            env: Default::default(),
            display_argv: None,
            cwd: None,
            timeout: Some(timeout),
        })
        .await
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use executor::{ModelResponse, ModelResponseStream};
    use exoharness::{SandboxCommand, SandboxCommandOutput, ToolRequest};
    use serde_json::Map;

    use super::*;
    use crate::{ManagedSandboxLease, SandboxLease};

    struct FakeModel {
        calls: Mutex<usize>,
    }

    #[async_trait]
    impl ModelClient for FakeModel {
        async fn complete(&self, _request: ModelRequest) -> Result<ModelResponse> {
            let mut calls = self.calls.lock().expect("fake model lock");
            *calls += 1;
            if *calls == 1 {
                let mut arguments = Map::new();
                arguments.insert(
                    "command".to_string(),
                    Value::String("printf hello".to_string()),
                );
                Ok(ModelResponse {
                    response_id: None,
                    messages: Vec::new(),
                    tool_calls: vec![PendingToolCall {
                        tool_call_id: "call-1".to_string(),
                        request: ToolRequest {
                            function_name: "shell".to_string(),
                            arguments,
                            namespace: None,
                        },
                    }],
                    usage: None,
                    model: None,
                    ttft: None,
                    duration: None,
                    provider_cost_usd: None,
                })
            } else {
                Ok(ModelResponse {
                    response_id: None,
                    messages: vec![Message::Assistant {
                        content: AssistantContent::String("finished".to_string()),
                        id: None,
                    }],
                    tool_calls: Vec::new(),
                    usage: None,
                    model: None,
                    ttft: None,
                    duration: None,
                    provider_cost_usd: None,
                })
            }
        }

        async fn complete_stream(
            &self,
            _request: ModelRequest,
        ) -> Result<Box<dyn ModelResponseStream>> {
            bail!("streaming is not used by this test")
        }
    }

    struct FakeSandbox {
        commands: Mutex<Vec<SandboxCommand>>,
    }

    #[async_trait]
    impl ManagedSandboxCapability for FakeSandbox {
        fn id(&self) -> &str {
            "fake-sandbox"
        }

        async fn exec(&self, command: &SandboxCommand) -> exoharness::Result<SandboxCommandOutput> {
            self.commands
                .lock()
                .expect("fake sandbox lock")
                .push(command.clone());
            Ok(SandboxCommandOutput {
                ok: true,
                exit_code: Some(0),
                stdout: "hello".to_string(),
                stderr: String::new(),
                command: command.argv.clone(),
                cwd: command
                    .cwd
                    .clone()
                    .unwrap_or_else(|| "/workspace".to_string()),
            })
        }
    }

    struct FakePool {
        sandbox: Arc<dyn ManagedSandboxCapability>,
        heartbeats: Mutex<usize>,
        releases: Mutex<usize>,
    }

    #[async_trait]
    impl ManagedSandboxPool for FakePool {
        async fn acquire_any(&self, _worker_id: String) -> Result<ManagedSandboxLease> {
            Ok(ManagedSandboxLease {
                lease: SandboxLease {
                    entry_id: "entry-1".to_string(),
                    worker_id: "worker-1".to_string(),
                    fencing_token: "fence-1".to_string(),
                    expires_at: std::time::Instant::now() + Duration::from_secs(60),
                },
                sandbox: Arc::clone(&self.sandbox),
            })
        }

        async fn heartbeat(&self, _lease: &SandboxLease) -> Result<()> {
            *self.heartbeats.lock().expect("heartbeat lock") += 1;
            Ok(())
        }

        async fn release(&self, _lease: &SandboxLease) -> Result<Option<exoharness::SnapshotId>> {
            *self.releases.lock().expect("release lock") += 1;
            Ok(None)
        }

        async fn reset(&self, _lease: &SandboxLease) -> Result<()> {
            Ok(())
        }

        async fn drain(&self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn assistant_text_reads_string_and_parts() {
        let messages = vec![Message::Assistant {
            content: AssistantContent::String("done".to_string()),
            id: None,
        }];
        assert_eq!(assistant_text(&messages), "done");
    }

    #[tokio::test]
    async fn coding_agent_runs_tools_and_releases_the_sandbox() {
        let sandbox = Arc::new(FakeSandbox {
            commands: Mutex::new(Vec::new()),
        });
        let pool = Arc::new(FakePool {
            sandbox: sandbox.clone(),
            heartbeats: Mutex::new(0),
            releases: Mutex::new(0),
        });
        let agent = CodingAgent::new(
            Arc::new(FakeModel {
                calls: Mutex::new(0),
            }),
            pool.clone(),
            CodingAgentConfig::default(),
        );

        let result = agent
            .run(CodingTask {
                worker_id: "worker-1".to_string(),
                prompt: "say hello".to_string(),
            })
            .await
            .expect("coding agent should finish");

        assert_eq!(result.response, "finished");
        assert_eq!(result.rounds, 2);
        assert_eq!(sandbox.commands.lock().expect("commands lock").len(), 1);
        assert_eq!(*pool.heartbeats.lock().expect("heartbeat lock"), 2);
        assert_eq!(*pool.releases.lock().expect("release lock"), 1);
    }
}
