use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::rest::ApiState;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A chat request sent by the client to the assistant endpoint.
#[derive(Debug, Serialize, Deserialize)]
pub struct ChatRequest {
    pub messages: Vec<ChatMessage>,
}

/// A single message in the conversation history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String, // "user" or "assistant"
    pub content: String,
}

/// Configuration for the Claude AI assistant.
#[derive(Debug, Clone)]
pub struct AssistantConfig {
    pub api_key: String,
    pub model: String,
}

impl AssistantConfig {
    /// Try to load from environment variables.
    /// Returns `None` if the API key is not configured.
    pub fn from_env() -> Option<Self> {
        let api_key = std::env::var("MM_ASSISTANT__ANTHROPIC_API_KEY").ok()?;
        let model = std::env::var("MM_ASSISTANT__MODEL")
            .unwrap_or_else(|_| "claude-sonnet-4-20250514".to_string());
        Some(Self { api_key, model })
    }
}

// ---------------------------------------------------------------------------
// Claude API client
// ---------------------------------------------------------------------------

/// HTTP client for streaming chat completions from the Anthropic Messages API.
pub struct ClaudeClient {
    http: reqwest::Client,
    config: AssistantConfig,
}

impl ClaudeClient {
    pub fn new(config: AssistantConfig) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
            config,
        }
    }

    /// Stream a chat response from the Claude API.
    ///
    /// Returns a stream of text chunks extracted from SSE `content_block_delta`
    /// events.
    pub async fn stream_chat(
        &self,
        system_prompt: &str,
        messages: &[ChatMessage],
    ) -> Result<impl futures::Stream<Item = Result<String, anyhow::Error>>, anyhow::Error> {
        let body = serde_json::json!({
            "model": self.config.model,
            "max_tokens": 2048,
            "stream": true,
            "system": system_prompt,
            "messages": messages.iter().map(|m| {
                serde_json::json!({
                    "role": m.role,
                    "content": m.content,
                })
            }).collect::<Vec<_>>(),
        });

        let response = self
            .http
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &self.config.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("Claude API error {}: {}", status, text);
        }

        let stream = response.bytes_stream();
        Ok(stream_from_sse(stream))
    }
}

// ---------------------------------------------------------------------------
// SSE stream parser
// ---------------------------------------------------------------------------

/// Parse an SSE byte stream from the Anthropic API into a stream of text
/// chunks.
///
/// Each SSE event of type `content_block_delta` carries a JSON payload like:
///
/// ```json
/// {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}
/// ```
///
/// We extract the `delta.text` field and yield it.
fn stream_from_sse(
    byte_stream: impl futures::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send + 'static,
) -> impl futures::Stream<Item = Result<String, anyhow::Error>> + Send {
    async_stream::stream! {
        let mut buffer = String::new();

        futures::pin_mut!(byte_stream);

        while let Some(chunk) = byte_stream.next().await {
            let chunk = chunk?;
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            // Process complete lines.
            while let Some(line_end) = buffer.find('\n') {
                let line = buffer[..line_end].trim().to_string();
                buffer = buffer[line_end + 1..].to_string();

                if let Some(json_str) = line.strip_prefix("data: ") {
                    if json_str == "[DONE]" {
                        return;
                    }
                    if let Ok(event) = serde_json::from_str::<serde_json::Value>(json_str) {
                        if event["type"] == "content_block_delta" {
                            if let Some(text) = event["delta"]["text"].as_str() {
                                yield Ok(text.to_string());
                            }
                        }
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// System context builder
// ---------------------------------------------------------------------------

/// Build a system prompt that includes live trading context for the assistant.
pub async fn build_system_context(state: &Arc<ApiState>) -> String {
    let mut ctx = String::new();

    ctx.push_str("You are an AI trading assistant for an automated market maker platform.\n\n");

    // Current positions
    let positions = state.positions.read().await;
    ctx.push_str(&format!("## Current Positions ({})\n", positions.len()));
    for pos in positions.iter() {
        ctx.push_str(&format!(
            "- {} {}: qty={}, avg_entry={}, unrealized_pnl={}\n",
            pos.exchange, pos.symbol, pos.quantity, pos.average_entry_price, pos.unrealized_pnl
        ));
    }

    // Active strategies
    let strategies = state.strategies.read().await;
    ctx.push_str(&format!("\n## Strategies ({})\n", strategies.len()));
    for s in strategies.iter() {
        ctx.push_str(&format!(
            "- {} ({}): type={}, exchange={}, symbol={}, running={}\n",
            s.config.strategy_id,
            s.config.strategy_type,
            s.config.strategy_type,
            s.config.exchange,
            s.config.symbol,
            s.running
        ));
    }

    // Risk state
    let ks = state.kill_switch_active.lock().await;
    ctx.push_str(&format!("\n## Risk State\nKill switch active: {}\n", *ks));

    // Available strategy types
    ctx.push_str("\n## Available Strategy Types\n");
    ctx.push_str("- spread: Symmetric bid/ask market making around mid price\n");
    ctx.push_str("- avellaneda: Inventory-aware market making (Avellaneda-Stoikov model)\n");
    ctx.push_str("- grid: Arithmetic/geometric grid trading\n");
    ctx.push_str("- twap: Time-weighted average price execution\n");
    ctx.push_str("- vwap: Volume-weighted average price execution\n");
    ctx.push_str("- momentum: Trend-following with trailing stops\n");
    ctx.push_str("- mean_reversion: Bollinger Band-style mean reversion\n");
    ctx.push_str("- stat_arb: Cross-exchange statistical arbitrage\n");

    ctx.push_str("\n## Instructions\n");
    ctx.push_str("- Explain strategies and parameters in plain language\n");
    ctx.push_str("- Suggest parameter values based on market conditions and user goals\n");
    ctx.push_str("- Help interpret metrics and performance data\n");
    ctx.push_str("- Warn about risk considerations\n");
    ctx.push_str(
        "- When suggesting config changes, format as JSON with strategy_type and params\n",
    );

    ctx
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assistant_config_returns_none_without_env() {
        // Ensure the env var is not set for this test.
        std::env::remove_var("MM_ASSISTANT__ANTHROPIC_API_KEY");
        assert!(AssistantConfig::from_env().is_none());
    }

    #[tokio::test]
    async fn system_context_includes_strategy_types() {
        let state = ApiState::new();
        let ctx = build_system_context(&state).await;
        assert!(ctx.contains("spread"));
        assert!(ctx.contains("avellaneda"));
        assert!(ctx.contains("grid"));
        assert!(ctx.contains("twap"));
        assert!(ctx.contains("vwap"));
        assert!(ctx.contains("momentum"));
        assert!(ctx.contains("mean_reversion"));
        assert!(ctx.contains("stat_arb"));
    }

    #[tokio::test]
    async fn system_context_includes_positions_section() {
        let state = ApiState::new();
        let ctx = build_system_context(&state).await;
        assert!(ctx.contains("## Current Positions (0)"));
    }

    #[tokio::test]
    async fn system_context_includes_risk_state() {
        let state = ApiState::new();
        let ctx = build_system_context(&state).await;
        assert!(ctx.contains("Kill switch active: false"));
    }

    #[test]
    fn chat_request_deserializes() {
        let json = r#"{"messages":[{"role":"user","content":"Hello"}]}"#;
        let req: ChatRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.messages.len(), 1);
        assert_eq!(req.messages[0].role, "user");
        assert_eq!(req.messages[0].content, "Hello");
    }

    #[test]
    fn chat_message_serializes() {
        let msg = ChatMessage {
            role: "assistant".to_string(),
            content: "Hi there".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("assistant"));
        assert!(json.contains("Hi there"));
    }
}
