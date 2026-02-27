use std::time::Duration;

use anyhow::Context;
use serde::Deserialize;
use serde_json::Value;
use tracing::{debug, warn};

use super::types::*;

const ANTHROPIC_API_URL: &str = "https://api.anthropic.com/v1/messages";
const ANTHROPIC_VERSION: &str = "2023-06-01";
const MAX_RETRIES: u32 = 3;

/// LLM provider for Anthropic's Messages API.
pub struct AnthropicProvider {
    http: reqwest::Client,
    api_key: String,
    model: String,
}

impl AnthropicProvider {
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

    /// Build the request body for the Anthropic Messages API.
    fn build_request_body(
        &self,
        system: &str,
        messages: &[LlmMessage],
        tools: Option<&[ToolDefinition]>,
        max_tokens: u32,
    ) -> Value {
        let api_messages: Vec<Value> = messages
            .iter()
            .map(|msg| {
                let content: Vec<Value> = msg
                    .content
                    .iter()
                    .map(|block| match block {
                        ContentBlock::Text { text } => {
                            serde_json::json!({"type": "text", "text": text})
                        }
                        ContentBlock::ToolUse { id, name, input } => {
                            serde_json::json!({"type": "tool_use", "id": id, "name": name, "input": input})
                        }
                        ContentBlock::ToolResult {
                            tool_use_id,
                            content,
                            is_error,
                        } => {
                            serde_json::json!({
                                "type": "tool_result",
                                "tool_use_id": tool_use_id,
                                "content": content,
                                "is_error": is_error,
                            })
                        }
                    })
                    .collect();

                serde_json::json!({
                    "role": msg.role,
                    "content": content,
                })
            })
            .collect();

        let mut body = serde_json::json!({
            "model": self.model,
            "max_tokens": max_tokens,
            "system": [{
                "type": "text",
                "text": system,
                "cache_control": { "type": "ephemeral" }
            }],
            "messages": api_messages,
        });

        if let Some(tools) = tools {
            if !tools.is_empty() {
                let tool_defs: Vec<Value> = tools
                    .iter()
                    .map(|t| {
                        serde_json::json!({
                            "name": t.name,
                            "description": t.description,
                            "input_schema": t.input_schema,
                        })
                    })
                    .collect();
                body["tools"] = Value::Array(tool_defs);
            }
        }

        body
    }

    /// Send a request with retry logic.
    async fn send_with_retry(&self, body: Value) -> anyhow::Result<AnthropicResponse> {
        let mut last_err = None;

        for attempt in 0..MAX_RETRIES {
            if attempt > 0 {
                let delay = Duration::from_secs(1 << attempt); // 2s, 4s
                debug!(
                    attempt,
                    delay_secs = delay.as_secs(),
                    "Retrying Anthropic API"
                );
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
                        warn!(attempt, error = %e, "Retryable Anthropic API error");
                        last_err = Some(e);
                        continue;
                    }
                    return Err(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("All retry attempts exhausted")))
    }

    /// Send a single request to the Anthropic API.
    async fn send_request(&self, body: &Value) -> anyhow::Result<AnthropicResponse> {
        let response = self
            .http
            .post(ANTHROPIC_API_URL)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .header("content-type", "application/json")
            .json(body)
            .send()
            .await
            .context("Failed to send Anthropic API request")?;

        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(ApiError {
                status: status.as_u16(),
                body: text,
            }
            .into());
        }

        let resp: AnthropicResponse = response
            .json()
            .await
            .context("Failed to parse Anthropic API response")?;

        Ok(resp)
    }
}

#[async_trait::async_trait]
impl super::LlmProvider for AnthropicProvider {
    fn name(&self) -> &str {
        "anthropic"
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
// Anthropic API response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct AnthropicResponse {
    content: Vec<AnthropicContentBlock>,
    stop_reason: String,
    usage: AnthropicUsage,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum AnthropicContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
}

#[derive(Debug, Deserialize)]
struct AnthropicUsage {
    input_tokens: u32,
    output_tokens: u32,
}

impl AnthropicResponse {
    fn into_llm_response(self) -> LlmResponse {
        let content = self
            .content
            .into_iter()
            .map(|block| match block {
                AnthropicContentBlock::Text { text } => ContentBlock::Text { text },
                AnthropicContentBlock::ToolUse { id, name, input } => {
                    ContentBlock::ToolUse { id, name, input }
                }
            })
            .collect();

        let stop_reason = match self.stop_reason.as_str() {
            "tool_use" => StopReason::ToolUse,
            "max_tokens" => StopReason::MaxTokens,
            _ => StopReason::EndTurn,
        };

        LlmResponse {
            content,
            stop_reason,
            usage: Usage {
                input_tokens: self.usage.input_tokens,
                output_tokens: self.usage.output_tokens,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Error type for retryable API errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
#[error("Anthropic API error {status}: {body}")]
struct ApiError {
    status: u16,
    body: String,
}

impl ApiError {
    fn is_retryable(&self) -> bool {
        matches!(self.status, 429 | 500 | 502 | 503 | 529)
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
        let provider = AnthropicProvider::new("test-key".into(), "claude-sonnet-4-20250514".into());

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

        assert_eq!(body["model"], "claude-sonnet-4-20250514");
        assert_eq!(body["max_tokens"], 4096);
        assert!(body["tools"].is_array());
        assert_eq!(body["tools"].as_array().unwrap().len(), 1);
        assert_eq!(body["tools"][0]["name"], "get_ratios");

        // System uses cache_control
        assert!(body["system"][0]["cache_control"].is_object());
    }

    #[test]
    fn test_build_request_body_without_tools() {
        let provider = AnthropicProvider::new("test-key".into(), "claude-sonnet-4-20250514".into());
        let messages = vec![LlmMessage::user("Summarize the research.")];

        let body = provider.build_request_body("You are helpful.", &messages, None, 4096);

        assert!(body.get("tools").is_none());
    }

    #[test]
    fn test_parse_anthropic_response_text() {
        let json = r#"{
            "content": [{"type": "text", "text": "The P/E ratio is 28.5"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 100, "output_tokens": 20}
        }"#;

        let resp: AnthropicResponse = serde_json::from_str(json).unwrap();
        let llm_resp = resp.into_llm_response();

        assert_eq!(llm_resp.text(), "The P/E ratio is 28.5");
        assert_eq!(llm_resp.stop_reason, StopReason::EndTurn);
        assert_eq!(llm_resp.usage.input_tokens, 100);
        assert_eq!(llm_resp.usage.output_tokens, 20);
    }

    #[test]
    fn test_parse_anthropic_response_tool_use() {
        let json = r#"{
            "content": [
                {"type": "text", "text": "Let me look that up."},
                {"type": "tool_use", "id": "toolu_01", "name": "get_ratios", "input": {"ticker": "AAPL"}}
            ],
            "stop_reason": "tool_use",
            "usage": {"input_tokens": 150, "output_tokens": 40}
        }"#;

        let resp: AnthropicResponse = serde_json::from_str(json).unwrap();
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
        assert!(ApiError {
            status: 529,
            body: "overloaded".into()
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
        let provider = AnthropicProvider::new("test-key".into(), "claude-sonnet-4-20250514".into());

        let messages = vec![
            LlmMessage::user("What is AAPL?"),
            LlmMessage::assistant(vec![ContentBlock::tool_use(
                "tc_1",
                "get_ratios",
                serde_json::json!({"ticker": "AAPL"}),
            )]),
            LlmMessage::tool_results(vec![ContentBlock::tool_result(
                "tc_1",
                r#"{"pe_ratio": 28.5}"#,
            )]),
        ];

        let body = provider.build_request_body("System.", &messages, None, 2048);
        let api_msgs = body["messages"].as_array().unwrap();
        assert_eq!(api_msgs.len(), 3);

        // The tool result message
        let tool_msg = &api_msgs[2];
        assert_eq!(tool_msg["role"], "user");
        assert_eq!(tool_msg["content"][0]["type"], "tool_result");
        assert_eq!(tool_msg["content"][0]["tool_use_id"], "tc_1");
    }
}
