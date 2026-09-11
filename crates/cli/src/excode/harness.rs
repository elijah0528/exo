//! Runs chat turns through an exoharness conversation, so `exo excode
//! --harness codex` (or any TypeScript harness module) plugs an external agent
//! loop into the sandbox-pool terminal. The conversation's sandbox mirrors the
//! pool's workspace: the checkout mounted at `/workspace/exo` for Docker, or
//! the checkout itself for host-local processes.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use excode::{CodingAgent, CodingAgentEvent, CodingResult, agent::stream_chunk_text};
use executor::{
    AgentHarnessKind, BasicExoHarness, BasicExoHarnessConfig, BasicToolRuntime, CreateAgentRequest,
    CreateConversationRequest, ExecutionStreamEvent, ExoHarness, FileSystemMount,
    FileSystemMountMode, Harness, HarnessConversation, RouterModelClient,
    SandboxBackendRegistration, SandboxProvider, SecretBackendChoice, SendRequest,
    TypeScriptHarness,
};
use lingua::{Message, universal::UserContent};
use tokio::sync::{Mutex, mpsc::UnboundedSender};
use tokio_stream::StreamExt;

use crate::{
    HarnessSelection, build_typescript_harness_config, ensure_agent_matches_harness_selection,
    ensure_existing_repl_agent_model, ensure_repl_model, format_harness_selection,
};

use super::args::{ExcodeArgs, PoolBackend};
use super::session::RECIPE_WORKDIR;

const CONVERSATION_SLUG: &str = "excode";

/// The interactive chat driver selected by `--harness`.
#[derive(Clone)]
pub enum ChatDriver {
    Builtin(CodingAgent<RouterModelClient>),
    Harness(HarnessChatDriver),
}

/// Sends each prompt through the conversation's `send_stream` and maps the
/// executor's stream events onto the TUI's existing `CodingAgentEvent`s.
#[derive(Clone)]
pub struct HarnessChatDriver {
    conversation: Arc<dyn HarnessConversation>,
    session_id: Arc<Mutex<Option<executor::SessionId>>>,
}

impl HarnessChatDriver {
    pub async fn build(
        root: &Path,
        selection: &HarnessSelection,
        args: &ExcodeArgs,
        env_vars: HashMap<String, String>,
    ) -> Result<Self> {
        if matches!(selection, HarnessSelection::Kind(_)) {
            bail!(
                "excode --harness accepts codex, claude-code, cursor, pi, or a TypeScript harness module path"
            );
        }
        // Resolve the module path before touching models so a bad path errors
        // clearly instead of being masked by model-registration checks.
        let typescript =
            build_typescript_harness_config(Some(selection), None, &[]).with_context(|| {
                format!("invalid --harness {}", format_harness_selection(selection))
            })?;
        let workspace_root =
            std::env::current_dir().context("determining the TypeScript workspace root")?;
        if !workspace_root
            .join("exoharness/typescript/harness/runner.ts")
            .exists()
        {
            bail!(
                "excode --harness must run from the exo repository checkout (exoharness/typescript is missing)"
            );
        }
        let exoharness: Arc<dyn ExoHarness> = Arc::new(
            BasicExoHarness::new(BasicExoHarnessConfig {
                root: root.join("exoharness"),
                secret_backend: SecretBackendChoice::File { path: None },
                sandbox_default: SandboxProvider::LocalProcess,
                sandbox_backends: vec![
                    SandboxBackendRegistration::local_process(),
                    SandboxBackendRegistration::docker(),
                ],
            })
            .await?,
        );
        let harness: Arc<dyn Harness> = Arc::new(
            TypeScriptHarness::<BasicToolRuntime>::from_exoharness(exoharness, None, env_vars)?,
        );

        let agent_slug = selection
            .default_agent_slug()
            .unwrap_or_else(|| "excode".to_string());
        let agent = match harness.get_agent(&agent_slug).await? {
            Some(agent) => {
                ensure_agent_matches_harness_selection(agent.as_ref(), selection).await?;
                ensure_existing_repl_agent_model(
                    harness.as_ref(),
                    agent.as_ref(),
                    args.model.clone(),
                )
                .await?;
                agent
            }
            None => {
                let model = ensure_repl_model(harness.as_ref(), args.model.clone()).await?;
                harness
                    .create_agent(CreateAgentRequest {
                        slug: agent_slug.clone(),
                        name: Some(agent_slug),
                        harness: AgentHarnessKind::TypeScript,
                        typescript: typescript.clone(),
                        enable_agent_tool_creation: false,
                        sandbox_image: selection
                            .default_sandbox_image()
                            .map(str::to_string)
                            .or_else(|| {
                                (args.backend == PoolBackend::Docker).then(|| args.image.clone())
                            }),
                        sandbox_provider: sandbox_provider(args.backend),
                        sandbox_scope: None,
                        enable_networking: selection.default_enable_networking(),
                        model,
                        max_output_tokens: None,
                        max_tool_round_trips: None,
                        braintrust: None,
                    })
                    .await?
            }
        };

        // Force the conversation's sandbox provider/image to follow the pool
        // backend even when the agent predates this flag.
        let conversation = match agent.get_conversation(CONVERSATION_SLUG).await? {
            Some(conversation) => conversation,
            None => {
                agent
                    .create_conversation(CreateConversationRequest {
                        slug: Some(CONVERSATION_SLUG.to_string()),
                        name: Some(CONVERSATION_SLUG.to_string()),
                        sandbox_image: None,
                        sandbox_provider: Some(sandbox_provider(args.backend)),
                        shell_program: Some("/bin/bash".to_string()),
                    })
                    .await?
            }
        };
        let mut config = conversation.config().await?;
        config.sandbox_provider = Some(sandbox_provider(args.backend));
        config.mounts = vec![workspace_mount(args.backend)?];
        conversation.put_config(config).await?;

        Ok(Self {
            conversation,
            session_id: Arc::new(Mutex::new(None)),
        })
    }

    pub async fn run(
        &self,
        prompt: &str,
        events: UnboundedSender<CodingAgentEvent>,
    ) -> Result<CodingResult> {
        let session_id = *self.session_id.lock().await;
        let mut stream = self
            .conversation
            .send_stream(SendRequest {
                input: vec![Message::User {
                    content: UserContent::String(prompt.to_string()),
                }],
                session_id,
            })
            .await
            .context("starting the harness turn")?;
        let mut response = String::new();
        let mut tools = Vec::new();
        let mut pending_tool_names: HashMap<String, String> = HashMap::new();
        while let Some(event) = stream.next().await {
            let event = event.context("harness turn stream failed")?;
            match event {
                ExecutionStreamEvent::FirstChunk { .. } => {}
                ExecutionStreamEvent::Chunk(chunk) => {
                    let text = stream_chunk_text(&chunk);
                    if !text.is_empty() {
                        response.push_str(&text);
                        send_chat_event(&events, CodingAgentEvent::TextChunk(text))?;
                    }
                }
                ExecutionStreamEvent::ToolCall {
                    tool_call_id,
                    tool_name,
                    ..
                } => {
                    pending_tool_names.insert(tool_call_id, tool_name.clone());
                    tools.push(tool_name.clone());
                    send_chat_event(&events, CodingAgentEvent::ToolCall { name: tool_name })?;
                }
                ExecutionStreamEvent::ToolResult {
                    tool_call_id,
                    result,
                } => {
                    let name = pending_tool_names
                        .remove(&tool_call_id)
                        .unwrap_or_else(|| "tool".to_string());
                    send_chat_event(
                        &events,
                        CodingAgentEvent::ToolResult {
                            name,
                            output: result.to_string(),
                        },
                    )?;
                }
                ExecutionStreamEvent::Completed(result) => {
                    *self.session_id.lock().await = Some(result.session_id);
                }
            }
        }
        Ok(CodingResult {
            response,
            rounds: 1,
            tools,
        })
    }
}

fn sandbox_provider(backend: PoolBackend) -> SandboxProvider {
    match backend {
        PoolBackend::Docker => SandboxProvider::Docker,
        PoolBackend::LocalProcess => SandboxProvider::LocalProcess,
    }
}

fn workspace_mount(backend: PoolBackend) -> Result<FileSystemMount> {
    let host_path = std::env::current_dir()
        .context("determining the workspace directory")?
        .to_string_lossy()
        .into_owned();
    let mount_path = match backend {
        PoolBackend::Docker => RECIPE_WORKDIR.to_string(),
        PoolBackend::LocalProcess => host_path.clone(),
    };
    Ok(FileSystemMount {
        host_path,
        mount_path,
        mode: FileSystemMountMode::ReadWrite,
        internal: None,
    })
}

fn send_chat_event(
    events: &UnboundedSender<CodingAgentEvent>,
    event: CodingAgentEvent,
) -> Result<()> {
    events
        .send(event)
        .map_err(|_| anyhow!("coding agent event receiver dropped"))
}
