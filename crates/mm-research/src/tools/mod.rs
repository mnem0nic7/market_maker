pub mod registry;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// JSON Schema describing tool parameters for LLM function calling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSchema {
    #[serde(rename = "type")]
    pub schema_type: String,
    pub properties: Value,
    #[serde(default)]
    pub required: Vec<String>,
}

/// Result returned by a tool invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolOutput {
    pub name: String,
    pub content: String,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ToolOutput {
    pub fn success(name: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            content: content.into(),
            success: true,
            error: None,
        }
    }

    pub fn error(name: impl Into<String>, error: impl Into<String>) -> Self {
        let err = error.into();
        Self {
            name: name.into(),
            content: String::new(),
            success: false,
            error: Some(err),
        }
    }
}

/// Trait that all research agent tools must implement.
#[async_trait]
pub trait Tool: Send + Sync {
    /// Unique name for this tool (used in LLM function calling).
    fn name(&self) -> &str;

    /// Human-readable description shown to the LLM.
    fn description(&self) -> &str;

    /// JSON Schema for the tool's parameters.
    fn parameters_schema(&self) -> ToolSchema;

    /// Execute the tool with the given parameters.
    async fn invoke(&self, params: Value) -> anyhow::Result<ToolOutput>;
}
