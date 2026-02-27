use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::tools::ToolSchema;

/// Role of a message in the LLM conversation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LlmRole {
    System,
    User,
    Assistant,
    Tool,
}

/// A block of content within an LLM message.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
        #[serde(default)]
        is_error: bool,
    },
}

impl ContentBlock {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }

    pub fn tool_use(id: impl Into<String>, name: impl Into<String>, input: Value) -> Self {
        Self::ToolUse {
            id: id.into(),
            name: name.into(),
            input,
        }
    }

    pub fn tool_result(tool_use_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self::ToolResult {
            tool_use_id: tool_use_id.into(),
            content: content.into(),
            is_error: false,
        }
    }

    pub fn tool_error(tool_use_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self::ToolResult {
            tool_use_id: tool_use_id.into(),
            content: content.into(),
            is_error: true,
        }
    }
}

/// A message in the LLM conversation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmMessage {
    pub role: LlmRole,
    pub content: Vec<ContentBlock>,
}

impl LlmMessage {
    pub fn user(text: impl Into<String>) -> Self {
        Self {
            role: LlmRole::User,
            content: vec![ContentBlock::text(text)],
        }
    }

    pub fn assistant(blocks: Vec<ContentBlock>) -> Self {
        Self {
            role: LlmRole::Assistant,
            content: blocks,
        }
    }

    pub fn tool_results(results: Vec<ContentBlock>) -> Self {
        Self {
            role: LlmRole::User,
            content: results,
        }
    }
}

/// Tool definition for LLM function calling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: ToolSchema,
}

/// Reason the LLM stopped generating.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    EndTurn,
    ToolUse,
    MaxTokens,
}

/// Token usage from an LLM response.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u32,
    pub output_tokens: u32,
}

/// Response from an LLM provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmResponse {
    pub content: Vec<ContentBlock>,
    pub stop_reason: StopReason,
    pub usage: Usage,
}

impl LlmResponse {
    /// Extract all text blocks concatenated.
    pub fn text(&self) -> String {
        self.content
            .iter()
            .filter_map(|b| match b {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("")
    }

    /// Extract all tool use blocks.
    pub fn tool_uses(&self) -> Vec<(&str, &str, &Value)> {
        self.content
            .iter()
            .filter_map(|b| match b {
                ContentBlock::ToolUse { id, name, input } => {
                    Some((id.as_str(), name.as_str(), input))
                }
                _ => None,
            })
            .collect()
    }

    /// Whether the LLM wants to call tools.
    pub fn has_tool_use(&self) -> bool {
        self.stop_reason == StopReason::ToolUse
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_block_serde_roundtrip() {
        let blocks = vec![
            ContentBlock::text("Hello world"),
            ContentBlock::tool_use("tc_1", "get_price", serde_json::json!({"ticker": "AAPL"})),
            ContentBlock::tool_result("tc_1", "150.25"),
        ];

        for block in &blocks {
            let json = serde_json::to_string(block).unwrap();
            let roundtrip: ContentBlock = serde_json::from_str(&json).unwrap();
            let json2 = serde_json::to_string(&roundtrip).unwrap();
            assert_eq!(json, json2);
        }
    }

    #[test]
    fn test_llm_message_user() {
        let msg = LlmMessage::user("What is AAPL's P/E ratio?");
        assert_eq!(msg.role, LlmRole::User);
        assert_eq!(msg.content.len(), 1);
        match &msg.content[0] {
            ContentBlock::Text { text } => assert_eq!(text, "What is AAPL's P/E ratio?"),
            _ => panic!("Expected text block"),
        }
    }

    #[test]
    fn test_llm_response_text_extraction() {
        let response = LlmResponse {
            content: vec![
                ContentBlock::text("The P/E ratio "),
                ContentBlock::text("is 28.5"),
            ],
            stop_reason: StopReason::EndTurn,
            usage: Usage {
                input_tokens: 100,
                output_tokens: 50,
            },
        };

        assert_eq!(response.text(), "The P/E ratio is 28.5");
        assert!(!response.has_tool_use());
        assert!(response.tool_uses().is_empty());
    }

    #[test]
    fn test_llm_response_tool_uses() {
        let response = LlmResponse {
            content: vec![
                ContentBlock::text("Let me look that up."),
                ContentBlock::tool_use(
                    "tc_1",
                    "get_fundamentals",
                    serde_json::json!({"ticker": "AAPL"}),
                ),
            ],
            stop_reason: StopReason::ToolUse,
            usage: Usage::default(),
        };

        assert!(response.has_tool_use());
        let uses = response.tool_uses();
        assert_eq!(uses.len(), 1);
        assert_eq!(uses[0].0, "tc_1");
        assert_eq!(uses[0].1, "get_fundamentals");
    }

    #[test]
    fn test_stop_reason_serde() {
        let reasons = vec![
            (StopReason::EndTurn, "\"end_turn\""),
            (StopReason::ToolUse, "\"tool_use\""),
            (StopReason::MaxTokens, "\"max_tokens\""),
        ];

        for (reason, expected) in reasons {
            assert_eq!(serde_json::to_string(&reason).unwrap(), expected);
        }
    }

    #[test]
    fn test_role_serde() {
        assert_eq!(
            serde_json::to_string(&LlmRole::Assistant).unwrap(),
            "\"assistant\""
        );
        assert_eq!(serde_json::to_string(&LlmRole::User).unwrap(), "\"user\"");
    }
}
