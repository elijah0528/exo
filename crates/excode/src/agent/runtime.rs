use std::sync::Arc;

use async_trait::async_trait;
use executor::{AgentConfig, ConversationConfig, ToolRuntime, ensure_conversation_sandbox};
use exoharness::{AgentHandle, ConversationHandle, Result, ToolRequest, ToolResult, TurnHandle};

use super::registry::{ToolContext, ToolRegistry};

pub struct CodingToolRuntime {
    registry: Arc<ToolRegistry>,
}

impl CodingToolRuntime {
    pub fn new(registry: Arc<ToolRegistry>) -> Self {
        Self { registry }
    }

    pub fn with_default_tools() -> Self {
        let mut registry = ToolRegistry::new();
        registry.register(Arc::new(super::coding_tools::ShellTool));
        registry.register(Arc::new(super::coding_tools::ReadFileTool));
        registry.register(Arc::new(super::coding_tools::ListFilesTool));
        Self::new(Arc::new(registry))
    }

    pub fn definitions(&self) -> Vec<executor::ToolDefinition> {
        self.registry.definitions()
    }
}

#[async_trait]
impl ToolRuntime for CodingToolRuntime {
    async fn prepare_conversation(
        &self,
        _agent: &dyn AgentHandle,
        conversation: &dyn ConversationHandle,
        agent_config: &AgentConfig,
        conversation_config: &ConversationConfig,
    ) -> Result<()> {
        ensure_conversation_sandbox(conversation, agent_config, conversation_config).await?;
        Ok(())
    }

    async fn execute(
        &self,
        agent: &dyn AgentHandle,
        conversation: &dyn ConversationHandle,
        turn: Option<&dyn TurnHandle>,
        agent_config: &AgentConfig,
        conversation_config: &ConversationConfig,
        request: &ToolRequest,
    ) -> Result<ToolResult> {
        let handler = self.registry.get(&request.function_name).ok_or_else(|| {
            anyhow::anyhow!(
                "tool execution is not configured for {}",
                request.function_name
            )
        })?;
        handler
            .execute(
                &ToolContext {
                    agent,
                    conversation,
                    turn,
                    agent_config,
                    conversation_config,
                },
                request.arguments.clone(),
            )
            .await
    }
}
