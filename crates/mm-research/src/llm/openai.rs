use std::time::Duration;

use anyhow::Context;
use serde::Deserialize;
use serde_json::Value;
use tracing::{debug, warn};

use super::types::*;

const OPENAI_API_URL: &str = "https://api.openai.com/v1/chat/completions";
const MAX_RETRIES: u32 = 3;

/// LLM provider for OpenAI's Chat Completions API.
pub struct OpenAiProvider {
    http: reqwest::Client,
    api_key: String,
    model: String,
}

impl OpenAiProvider {
    pub fn new(api_key: String, model: String) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .unwrap_or_default();
        Self {
            http,
            api_key,
            model,
        }
    }

    /// Build the request body for the OpenAI Chat Completions API.
    fn build_request_body(
        &self,
        system: &str,
        messages: &[LlmMessage],
        tools: Option<&[ToolDefinition]>,
        max_tokens: u32,
    ) -> Value {
        let mut api_messages: Vec<Value> = Vec::new();

        // System message
        api_messages.push(serde_json::json!({
            "role": "system",
            "content": system,
        }));

        // Convert LlmMessages to OpenAI format
        for msg in messages {
            match msg.role {
                LlmRole::User => {
                    // User messages: may contain text or tool_result blocks
                    let has_tool_results = msg
                        .content
                        .iter()
                        .any(|b| matches!(b, ContentBlock::ToolResult { .. }));

                    if has_tool_results {
                        // Each tool result becomes a separate "tool" role message
                        for block in &msg.content {
                            if let ContentBlock::ToolResult {
                                tool_use_id,
                                content,
                                ..
                            } = block
                            {
                                api_messages.push(serde_json::json!({
                                    "role": "tool",
                                    "tool_call_id": tool_use_id,
                                    "content": content,
                                }));
                            }
                        }
                    } else {
                        // Regular text user message
                        let text: String = msg
                            .content
                            .iter()
                            .filter_map(|b| match b {
                                ContentBlock::Text { text } => Some(text.as_str()),
                                _ => None,
                            })
                            .collect::<Vec<_>>()
                            .join("");
                        api_messages.push(serde_json::json!({
                            "role": "user",
                            "content": text,
                        }));
                    }
                }
                LlmRole::Assistant => {
                    // Assistant messages may contain text and/or tool_calls
                    let text: String = msg
                        .content
                        .iter()
                        .filter_map(|b| match b {
                            ContentBlock::Text { text } => Some(text.as_str()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join("");

                    let tool_calls: Vec<Value> = msg
                        .content
                        .iter()
                        .filter_map(|b| match b {
                            ContentBlock::ToolUse { id, name, input } => Some(serde_json::json!({
                                "id": id,
                                "type": "function",
                                "function": {
                                    "name": name,
                                    "arguments": serde_json::to_string(input).unwrap_or_default(),
                                }
                            })),
                            _ => None,
                        })
                        .collect();

                    let mut assistant_msg = serde_json::json!({
                        "role": "assistant",
                    });

                    if !text.is_empty() {
                        assistant_msg["content"] = Value::String(text);
                    } else {
                        assistant_msg["content"] = Value::Null;
                    }

                    if !tool_calls.is_empty() {
                        assistant_msg["tool_calls"] = Value::Array(tool_calls);
                    }

                    api_messages.push(assistant_msg);
                }
                LlmRole::System | LlmRole::Tool => {
                    // System already handled above; Tool handled via User+ToolResult
                }
            }
        }

        let mut body = serde_json::json!({
            "model": self.model,
            "max_tokens": max_tokens,
            "messages": api_messages,
        });

        if let Some(tools) = tools {
            if !tools.is_empty() {
                let tool_defs: Vec<Value> = tools
                    .iter()
                    .map(|t| {
                        serde_json::json!({
                            "type": "function",
                            "function": {
                                "name": t.name,
                                "description": t.description,
                                "parameters": t.input_schema,
                            }
                        })
                    })
                    .collect();
                body["tools"] = Value::Array(tool_defs);
            }
        }

        body
    }

    /// Send a request with retry logic.
    async fn send_with_retry(&self, body: Value) -> anyhow::Result<OpenAiResponse> {
        let mut last_err = None;

        for attempt in 0..MAX_RETRIES {
            if attempt > 0 {
                let delay = Duration::from_secs(1 << attempt); // 2s, 4s
                debug!(attempt, delay_secs = delay.as_secs(), "Retrying OpenAI API");
                tokio::time::sleep(delay).await;
            }

            match self.send_request(&body).await {
                Ok(response) => return Ok(response),
                Err(e) => {
                    let is_retryable = e
                        .downcast_ref::<ApiError>()
                        .map(|ae| ae.is_retryable())
                        .unwrap_or(false);

                    if is_retryable && attempt < MAX_RETRIES - 1 {
                        warn!(attempt, error = %e, "Retryable OpenAI API error");
                        last_err = Some(e);
                        continue;
                    }
                    return Err(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("All retry attempts exhausted")))
    }

    /// Send a single request to the OpenAI API.
    async fn send_request(&self, body: &Value) -> anyhow::Result<OpenAiResponse> {
        let response = self
            .http
            .post(OPENAI_API_URL)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(body)
            .send()
            .await
            .context("Failed to send OpenAI API request")?;

        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(ApiError {
                status: status.as_u16(),
                body: text,
            }
            .into());
        }

        let resp: OpenAiResponse = response
            .json()
            .await
            .context("Failed to parse OpenAI API response")?;

        Ok(resp)
    }
}

#[async_trait::async_trait]
impl super::LlmProvider for OpenAiProvider {
    fn name(&self) -> &str {
        "openai"
    }

    async fn complete(
        &self,
        system: &str,
        messages: &[LlmMessage],
        tools: &[ToolDefinition],
        max_tokens: u32,
    ) -> anyhow::Result<LlmResponse> {
        let body = self.build_request_body(system, messages, Some(tools), max_tokens);
        let response = self.send_with_retry(body).await?;
        Ok(response.into_llm_response())
    }

    async fn complete_no_tools(
        &self,
        system: &str,
        messages: &[LlmMessage],
        max_tokens: u32,
    ) -> anyhow::Result<LlmResponse> {
        let body = self.build_request_body(system, messages, None, max_tokens);
        let response = self.send_with_retry(body).await?;
        Ok(response.into_llm_response())
    }
}

// ---------------------------------------------------------------------------
// OpenAI API response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct OpenAiResponse {
    choices: Vec<OpenAiChoice>,
    usage: OpenAiUsage,
}

#[derive(Debug, Deserialize)]
struct OpenAiChoice {
    message: OpenAiMessage,
    finish_reason: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiMessage {
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<OpenAiToolCall>,
}

#[derive(Debug, Deserialize)]
struct OpenAiToolCall {
    id: String,
    function: OpenAiFunction,
}

#[derive(Debug, Deserialize)]
struct OpenAiFunction {
    name: String,
    arguments: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiUsage {
    prompt_tokens: u32,
    completion_tokens: u32,
}

impl OpenAiResponse {
    fn into_llm_response(self) -> LlmResponse {
        let choice = self
            .choices
            .into_iter()
            .next()
            .expect("OpenAI response should have at least one choice");

        let mut content = Vec::new();

        // Add text content if present
        if let Some(text) = choice.message.content {
            if !text.is_empty() {
                content.push(ContentBlock::Text { text });
            }
        }

        // Add tool calls
        for tool_call in choice.message.tool_calls {
            let input: Value = serde_json::from_str(&tool_call.function.arguments)
                .unwrap_or(Value::Object(serde_json::Map::new()));
            content.push(ContentBlock::ToolUse {
                id: tool_call.id,
                name: tool_call.function.name,
                input,
            });
        }

        let stop_reason = match choice.finish_reason.as_str() {
            "tool_calls" => StopReason::ToolUse,
            "length" => StopReason::MaxTokens,
            _ => StopReason::EndTurn,
        };

        LlmResponse {
            content,
            stop_reason,
            usage: Usage {
                input_tokens: self.usage.prompt_tokens,
                output_tokens: self.usage.completion_tokens,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Error type for retryable API errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
#[error("OpenAI API error {status}: {body}")]
struct ApiError {
    status: u16,
    body: String,
}

impl ApiError {
    fn is_retryable(&self) -> bool {
        matches!(self.status, 429 | 500 | 502 | 503)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::ToolSchema;

    #[test]
    fn test_build_request_body_with_tools() {
        let provider = OpenAiProvider::new("test-key".into(), "gpt-4o".into());

        let messages = vec![LlmMessage::user("What is AAPL's P/E?")];

        let tools = vec![ToolDefinition {
            name: "get_ratios".into(),
            description: "Get key financial ratios".into(),
            input_schema: ToolSchema {
                schema_type: "object".into(),
                properties: serde_json::json!({
                    "ticker": {"type": "string"}
                }),
                required: vec!["ticker".into()],
            },
        }];

        let body = provider.build_request_body("You are helpful.", &messages, Some(&tools), 4096);

        assert_eq!(body["model"], "gpt-4o");
        assert_eq!(body["max_tokens"], 4096);
        assert!(body["tools"].is_array());
        assert_eq!(body["tools"].as_array().unwrap().len(), 1);
        assert_eq!(body["tools"][0]["type"], "function");
        assert_eq!(body["tools"][0]["function"]["name"], "get_ratios");

        // System message should be the first message
        let msgs = body["messages"].as_array().unwrap();
        assert_eq!(msgs[0]["role"], "system");
        assert_eq!(msgs[0]["content"], "You are helpful.");
        assert_eq!(msgs[1]["role"], "user");
    }

    #[test]
    fn test_build_request_body_without_tools() {
        let provider = OpenAiProvider::new("test-key".into(), "gpt-4o".into());
        let messages = vec![LlmMessage::user("Summarize the research.")];

        let body = provider.build_request_body("You are helpful.", &messages, None, 4096);

        assert!(body.get("tools").is_none());
    }

    #[test]
    fn test_parse_openai_response_text() {
        let json = r#"{
            "choices": [{
                "message": {"role": "assistant", "content": "The P/E ratio is 28.5", "tool_calls": []},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120}
        }"#;

        let resp: OpenAiResponse = serde_json::from_str(json).unwrap();
        let llm_resp = resp.into_llm_response();

        assert_eq!(llm_resp.text(), "The P/E ratio is 28.5");
        assert_eq!(llm_resp.stop_reason, StopReason::EndTurn);
        assert_eq!(llm_resp.usage.input_tokens, 100);
        assert_eq!(llm_resp.usage.output_tokens, 20);
    }

    #[test]
    fn test_parse_openai_response_tool_use() {
        let json = r#"{
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "Let me look that up.",
                    "tool_calls": [{
                        "id": "call_01",
                        "type": "function",
                        "function": {
                            "name": "get_ratios",
                            "arguments": "{\"ticker\": \"AAPL\"}"
                        }
                    }]
                },
                "finish_reason": "tool_calls"
            }],
            "usage": {"prompt_tokens": 150, "completion_tokens": 40, "total_tokens": 190}
        }"#;

        let resp: OpenAiResponse = serde_json::from_str(json).unwrap();
        let llm_resp = resp.into_llm_response();

        assert_eq!(llm_resp.stop_reason, StopReason::ToolUse);
        assert!(llm_resp.has_tool_use());
        let uses = llm_resp.tool_uses();
        assert_eq!(uses.len(), 1);
        assert_eq!(uses[0].1, "get_ratios");
    }

    #[test]
    fn test_api_error_retryable() {
        assert!(ApiError {
            status: 429,
            body: "rate limited".into()
        }
        .is_retryable());
        assert!(ApiError {
            status: 500,
            body: "server error".into()
        }
        .is_retryable());
        assert!(!ApiError {
            status: 400,
            body: "bad request".into()
        }
        .is_retryable());
        assert!(!ApiError {
            status: 401,
            body: "unauthorized".into()
        }
        .is_retryable());
    }

    #[test]
    fn test_message_with_tool_results() {
        let provider = OpenAiProvider::new("test-key".into(), "gpt-4o".into());

        let messages = vec![
            LlmMessage::user("What is AAPL?"),
            LlmMessage::assistant(vec![ContentBlock::tool_use(
                "call_1",
                "get_ratios",
                serde_json::json!({"ticker": "AAPL"}),
            )]),
            LlmMessage::tool_results(vec![ContentBlock::tool_result(
                "call_1",
                r#"{"pe_ratio": 28.5}"#,
            )]),
        ];

        let body = provider.build_request_body("System.", &messages, None, 2048);
        let api_msgs = body["messages"].as_array().unwrap();

        // system + user + assistant + tool = 4 messages
        assert_eq!(api_msgs.len(), 4);

        // The tool result message should have role "tool"
        let tool_msg = &api_msgs[3];
        assert_eq!(tool_msg["role"], "tool");
        assert_eq!(tool_msg["tool_call_id"], "call_1");

        // The assistant message should have tool_calls
        let assistant_msg = &api_msgs[2];
        assert_eq!(assistant_msg["role"], "assistant");
        assert!(assistant_msg["tool_calls"].is_array());
    }
}
