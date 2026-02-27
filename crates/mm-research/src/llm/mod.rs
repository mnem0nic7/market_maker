pub mod anthropic;
pub mod openai;
pub mod types;

pub use anthropic::AnthropicProvider;
pub use openai::OpenAiProvider;
pub use types::*;

use async_trait::async_trait;

/// Trait for LLM providers (Anthropic, OpenAI, etc.).
#[async_trait]
pub trait LlmProvider: Send + Sync {
    /// Provider name (e.g., "anthropic", "openai").
    fn name(&self) -> &str;

    /// Send a completion request with tool definitions.
    async fn complete(
        &self,
        system: &str,
        messages: &[LlmMessage],
        tools: &[ToolDefinition],
        max_tokens: u32,
    ) -> anyhow::Result<LlmResponse>;

    /// Send a completion request without tools (for final answer generation).
    async fn complete_no_tools(
        &self,
        system: &str,
        messages: &[LlmMessage],
        max_tokens: u32,
    ) -> anyhow::Result<LlmResponse>;
}
