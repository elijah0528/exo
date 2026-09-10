use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use async_trait::async_trait;
use executor::{
    AgentHarnessKind, CreateAgentRequest, CreateConversationRequest, Harness, ModelClient,
    ModelRequest, ModelResponse, PendingToolCall, SendRequest,
};
use exoharness::{
    BasicExoHarness, BasicExoHarnessConfig, Binding, EventData, EventKind, EventQuery, ExoHarness,
    PutSecretRequest, SandboxBackendRegistration, SandboxProvider, Secret, Uuid7,
};
use lingua::Message;
use lingua::universal::{AssistantContent, UserContent};
use tempfile::TempDir;

use super::coding_tools::{format_file_content, truncate_output};
use crate::CodingHarness;

#[derive(Default)]
struct FakeModel {
    responses: Mutex<VecDeque<ModelResponse>>,
    requests: Mutex<Vec<ModelRequest>>,
}

impl FakeModel {
    fn new(responses: Vec<ModelResponse>) -> Self {
        Self {
            responses: Mutex::new(VecDeque::from(responses)),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<ModelRequest> {
        self.requests
            .lock()
            .expect("model requests lock should not be poisoned")
            .clone()
    }
}

#[async_trait]
impl ModelClient for FakeModel {
    async fn complete(&self, request: ModelRequest) -> exoharness::Result<ModelResponse> {
        self.requests
            .lock()
            .expect("model requests lock should not be poisoned")
            .push(request);
        self.responses
            .lock()
            .expect("model responses lock should not be poisoned")
            .pop_front()
            .ok_or_else(|| anyhow!("no model response configured"))
    }

    async fn complete_stream(
        &self,
        _request: ModelRequest,
    ) -> exoharness::Result<Box<dyn executor::ModelResponseStream>> {
        Ok(Box::new(FakeModelResponseStream))
    }
}

struct FakeModelResponseStream;

#[async_trait]
impl executor::ModelResponseStream for FakeModelResponseStream {
    async fn next_chunk(&mut self) -> exoharness::Result<Option<lingua::UniversalStreamChunk>> {
        Ok(None)
    }

    async fn finish(self: Box<Self>) -> exoharness::Result<ModelResponse> {
        Err(anyhow!("streaming is not configured"))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn no_tool_turn_persists_history_for_the_next_send() {
    let tempdir = TempDir::new().expect("tempdir should exist");
    let exoharness = test_exoharness(tempdir.path()).await;
    let model = Arc::new(FakeModel::new(vec![
        response_with_text("first answer"),
        response_with_text("second answer"),
    ]));
    let harness = CodingHarness::new(
        Arc::clone(&exoharness) as Arc<dyn ExoHarness>,
        model.clone(),
    );
    register_model(exoharness.as_ref()).await;
    let conversation = create_conversation(&harness).await;

    conversation
        .send(SendRequest {
            input: vec![user_message("first question")],
            session_id: None,
        })
        .await
        .expect("first send should succeed");
    conversation
        .send(SendRequest {
            input: vec![user_message("second question")],
            session_id: None,
        })
        .await
        .expect("second send should succeed");

    let events = conversation
        .exoharness_handle()
        .get_events(Some(EventQuery {
            types: Some(vec![
                EventKind::TURN_STARTED,
                EventKind::MESSAGES,
                EventKind::TURN_ENDED,
            ]),
            ..Default::default()
        }))
        .await
        .expect("events should load")
        .events;
    assert_eq!(
        events
            .iter()
            .map(|event| event.data.kind())
            .collect::<Vec<_>>(),
        vec![
            EventKind::TURN_STARTED,
            EventKind::MESSAGES,
            EventKind::MESSAGES,
            EventKind::TURN_ENDED,
            EventKind::TURN_STARTED,
            EventKind::MESSAGES,
            EventKind::MESSAGES,
            EventKind::TURN_ENDED,
        ]
    );

    let requests = model.requests();
    assert_eq!(requests.len(), 2);
    let second_request = serde_json::to_string(&requests[1].messages).expect("messages serialize");
    for text in ["first question", "first answer", "second question"] {
        assert!(
            second_request.contains(text),
            "second request should contain {text}: {second_request}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_tool_is_recorded_as_an_error_result() {
    let tempdir = TempDir::new().expect("tempdir should exist");
    let exoharness = test_exoharness(tempdir.path()).await;
    let model = Arc::new(FakeModel::new(vec![
        ModelResponse {
            response_id: Some(Uuid7::now()),
            messages: Vec::new(),
            tool_calls: vec![PendingToolCall {
                tool_call_id: "call-1".to_string(),
                request: executor::ToolRequest {
                    function_name: "nope".to_string(),
                    arguments: serde_json::Map::new(),
                    namespace: None,
                },
            }],
            usage: None,
            model: None,
            ttft: None,
            duration: None,
            provider_cost_usd: None,
        },
        response_with_text("recovered"),
    ]));
    let harness = CodingHarness::new(
        Arc::clone(&exoharness) as Arc<dyn ExoHarness>,
        model.clone(),
    );
    register_model(exoharness.as_ref()).await;
    let conversation = create_conversation(&harness).await;

    conversation
        .send(SendRequest {
            input: vec![user_message("use nope")],
            session_id: None,
        })
        .await
        .expect("send should succeed after unknown tool");

    let events = conversation
        .exoharness_handle()
        .get_events(Some(EventQuery {
            types: Some(vec![EventKind::TOOL_RESULT]),
            ..Default::default()
        }))
        .await
        .expect("tool result events should load")
        .events;
    assert_eq!(events.len(), 1);
    let EventData::ToolResult { result, .. } = &events[0].data else {
        panic!("expected a tool result event");
    };
    assert_eq!(result["ok"], false);
    assert!(
        result["error"]
            .as_str()
            .expect("error should be a string")
            .contains("not configured")
    );
    assert_eq!(model.requests().len(), 2);
}

#[test]
fn shell_output_truncates_to_head_and_tail() {
    let input = format!("{}{}", "a".repeat(8192), "b".repeat(8192 + 17));
    let (output, truncated) = truncate_output(&input);
    assert!(truncated);
    assert!(output.starts_with(&"a".repeat(8192)));
    assert!(output.ends_with(&"b".repeat(8192)));
    assert!(output.contains("[... 17 bytes truncated ...]"));
}

#[test]
fn read_file_format_numbers_offset_and_limit() {
    let (content, total_lines) = format_file_content("one\ntwo\nthree\nfour\n", 1, 2);
    assert_eq!(total_lines, 4);
    assert_eq!(content, "2\ttwo\n3\tthree");
}

async fn test_exoharness(root: &std::path::Path) -> Arc<BasicExoHarness> {
    Arc::new(
        BasicExoHarness::new(BasicExoHarnessConfig {
            root: root.join("exoharness"),
            secret_backend: exoharness::SecretBackendChoice::Static([7u8; 32]),
            sandbox_default: SandboxProvider::LocalProcess,
            sandbox_backends: vec![SandboxBackendRegistration::local_process()],
        })
        .await
        .expect("exoharness should initialize"),
    )
}

async fn register_model(exoharness: &dyn ExoHarness) {
    let secret_id = exoharness
        .put_secret(PutSecretRequest {
            name: "test-key".to_string(),
            secret: Secret::Key {
                value: "test-key".to_string(),
            },
        })
        .await
        .expect("secret should be created");
    exoharness
        .put_binding(Binding::Llm {
            name: "test-model".to_string(),
            model: "test-model".to_string(),
            base_url: None,
            secret_id: Some(secret_id),
        })
        .await
        .expect("model binding should be created");
}

async fn create_conversation<M: ModelClient + 'static>(
    harness: &CodingHarness<M>,
) -> Arc<dyn executor::HarnessConversation> {
    let agent = harness
        .create_agent(CreateAgentRequest {
            slug: "coding".to_string(),
            name: None,
            harness: AgentHarnessKind::Coding,
            typescript: None,
            enable_agent_tool_creation: false,
            sandbox_image: None,
            sandbox_provider: SandboxProvider::LocalProcess,
            sandbox_scope: None,
            enable_networking: false,
            model: "test-model".to_string(),
            max_output_tokens: None,
            max_tool_round_trips: Some(3),
            braintrust: None,
        })
        .await
        .expect("agent should be created");
    agent
        .create_conversation(CreateConversationRequest {
            sandbox_provider: Some(SandboxProvider::LocalProcess),
            ..Default::default()
        })
        .await
        .expect("conversation should be created")
}

fn response_with_text(text: &str) -> ModelResponse {
    ModelResponse {
        response_id: Some(Uuid7::now()),
        messages: vec![Message::Assistant {
            content: AssistantContent::String(text.to_string()),
            id: None,
        }],
        tool_calls: Vec::new(),
        usage: None,
        model: None,
        ttft: None,
        duration: None,
        provider_cost_usd: None,
    }
}

fn user_message(text: &str) -> Message {
    Message::User {
        content: UserContent::String(text.to_string()),
    }
}
