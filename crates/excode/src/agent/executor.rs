use std::sync::Arc;

use async_trait::async_trait;
use cost::PricingTable;
use executor::{
    AgentConfig, ConversationConfig, ExecutionStreamEvent, ExecutorStreamMode, HarnessExecutor,
    ModelClient, ToolRuntime, TurnExecutionTrace, build_model_request, collect_tool_requests,
    complete_model_round, interpret_model_response, try_send_stream_event,
};
use exoharness::{AgentHandle, ConversationHandle, EventData, Result, TurnHandle};
use serde_json::json;

use super::{context::ContextProjection, runtime::CodingToolRuntime};

pub struct CodingExecutor<M> {
    model: Arc<M>,
    tools: Arc<CodingToolRuntime>,
    context: Arc<ContextProjection>,
    pricing: Arc<PricingTable>,
}

impl<M> Clone for CodingExecutor<M> {
    fn clone(&self) -> Self {
        Self {
            model: Arc::clone(&self.model),
            tools: Arc::clone(&self.tools),
            context: Arc::clone(&self.context),
            pricing: Arc::clone(&self.pricing),
        }
    }
}

impl<M> CodingExecutor<M> {
    pub fn new(model: Arc<M>, tools: Arc<CodingToolRuntime>) -> Self {
        Self::with_pricing(model, tools, Arc::new(PricingTable::empty()))
    }

    pub fn with_pricing(
        model: Arc<M>,
        tools: Arc<CodingToolRuntime>,
        pricing: Arc<PricingTable>,
    ) -> Self {
        Self {
            model,
            tools,
            context: Arc::new(ContextProjection::new()),
            pricing,
        }
    }
}

#[async_trait]
impl<M> HarnessExecutor for CodingExecutor<M>
where
    M: ModelClient + 'static,
{
    type Prepared = ();

    async fn prepare_conversation(
        &self,
        agent: &dyn AgentHandle,
        conversation: &dyn ConversationHandle,
        agent_config: &AgentConfig,
        conversation_config: &ConversationConfig,
    ) -> Result<()> {
        self.tools
            .prepare_conversation(agent, conversation, agent_config, conversation_config)
            .await
    }

    fn prepare_request(&self, _request: &executor::SendRequest) -> Result<Self::Prepared> {
        Ok(())
    }

    async fn execute_turn(
        &self,
        agent: &dyn AgentHandle,
        conversation: &dyn ConversationHandle,
        turn: Arc<dyn TurnHandle>,
        agent_config: &AgentConfig,
        conversation_config: &ConversationConfig,
        _prepared: &Self::Prepared,
        stream_mode: ExecutorStreamMode<'_>,
        turn_trace: Option<&dyn TurnExecutionTrace>,
    ) -> Result<()> {
        for round in 0u32.. {
            if agent_config
                .max_tool_round_trips
                .is_some_and(|limit| round > limit)
            {
                return Ok(());
            }

            let messages = self.context.materialize(conversation, agent_config).await?;
            let request = build_model_request(
                conversation,
                agent_config,
                messages,
                self.tools.definitions(),
            )
            .await?;
            let response = complete_model_round(
                self.model.as_ref(),
                request,
                round as usize,
                stream_mode,
                turn_trace,
            )
            .await?;
            let events = interpret_model_response(response, &self.pricing);
            turn.add_events(events.clone()).await?;
            let tool_requests = collect_tool_requests(&events);
            if tool_requests.is_empty() {
                return Ok(());
            }

            let mut results = Vec::with_capacity(tool_requests.len());
            for tool_request in tool_requests {
                if let ExecutorStreamMode::Enabled(event_tx) = stream_mode {
                    try_send_stream_event(
                        event_tx,
                        ExecutionStreamEvent::ToolCall {
                            tool_call_id: tool_request.tool_call_id.clone(),
                            tool_name: tool_request.request.function_name.clone(),
                            arguments: tool_request.request.arguments.clone(),
                        },
                    );
                }

                let mut tool_trace = match turn_trace {
                    Some(trace) => {
                        trace
                            .start_tool_call(&tool_request.request, round as usize)
                            .await
                    }
                    None => None,
                };
                let (result, succeeded) = match self
                    .tools
                    .execute(
                        agent,
                        conversation,
                        Some(turn.as_ref()),
                        agent_config,
                        conversation_config,
                        &tool_request.request,
                    )
                    .await
                {
                    Ok(result) => (result, true),
                    Err(error) => {
                        if let Some(trace) = tool_trace.take() {
                            trace.finish_error(&error).await;
                        }
                        (
                            json!({
                                "ok": false,
                                "error": error.to_string(),
                            }),
                            false,
                        )
                    }
                };
                if succeeded && let Some(trace) = tool_trace.take() {
                    trace.finish_success(&result).await;
                }
                if let ExecutorStreamMode::Enabled(event_tx) = stream_mode {
                    try_send_stream_event(
                        event_tx,
                        ExecutionStreamEvent::ToolResult {
                            tool_call_id: tool_request.tool_call_id.clone(),
                            result: result.clone(),
                        },
                    );
                }
                results.push(EventData::ToolResult {
                    tool_call_id: tool_request.tool_call_id,
                    result,
                });
            }
            turn.add_events(results).await?;
        }
        Ok(())
    }
}
