use std::collections::HashMap;
use std::sync::RwLock;

use executor::{AgentConfig, HistoryCacheEntry, materialize_event_history};
use exoharness::{ConversationHandle, ConversationId, Result};
use lingua::{Message, universal::UserContent};

use super::prompts::SYSTEM_PROMPT;

pub struct ContextProjection {
    cache: RwLock<HashMap<ConversationId, HistoryCacheEntry>>,
}

impl Default for ContextProjection {
    fn default() -> Self {
        Self::new()
    }
}

impl ContextProjection {
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
        }
    }

    pub async fn materialize(
        &self,
        conversation: &dyn ConversationHandle,
        agent_config: &AgentConfig,
    ) -> Result<Vec<Message>> {
        let mut messages = vec![Message::System {
            content: UserContent::String(SYSTEM_PROMPT.to_string()),
        }];
        messages.extend(agent_config.instructions.clone());
        messages.extend(materialize_event_history(conversation, &self.cache).await?);
        Ok(messages)
    }
}
