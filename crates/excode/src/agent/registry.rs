use std::sync::Arc;

use async_trait::async_trait;
use executor::{AgentConfig, ConversationConfig, ToolDefinition};
use exoharness::{AgentHandle, ConversationHandle, Result, ToolResult, TurnHandle};
use serde_json::{Map, Value};

#[async_trait]
pub trait ToolHandler: Send + Sync {
    fn definition(&self) -> ToolDefinition;

    async fn execute(
        &self,
        ctx: &ToolContext<'_>,
        arguments: Map<String, Value>,
    ) -> Result<ToolResult>;
}

pub struct ToolContext<'a> {
    pub agent: &'a dyn AgentHandle,
    pub conversation: &'a dyn ConversationHandle,
    pub turn: Option<&'a dyn TurnHandle>,
    pub agent_config: &'a AgentConfig,
    pub conversation_config: &'a ConversationConfig,
}

#[derive(Default)]
pub struct ToolRegistry {
    handlers: Vec<Arc<dyn ToolHandler>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, handler: Arc<dyn ToolHandler>) {
        self.handlers.push(handler);
    }

    pub fn definitions(&self) -> Vec<ToolDefinition> {
        self.handlers
            .iter()
            .map(|handler| handler.definition())
            .collect()
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn ToolHandler>> {
        self.handlers
            .iter()
            .find(|handler| handler.definition().name == name)
            .cloned()
    }
}
