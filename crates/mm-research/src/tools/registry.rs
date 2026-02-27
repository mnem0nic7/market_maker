use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{Tool, ToolOutput, ToolSchema};

/// Definition of a tool suitable for passing to an LLM provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: ToolSchema,
}

/// Registry holding all available tools, keyed by name.
pub struct ToolRegistry {
    tools: HashMap<String, Arc<dyn Tool>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            tools: HashMap::new(),
        }
    }

    /// Register a tool. Overwrites any existing tool with the same name.
    pub fn register(&mut self, tool: Arc<dyn Tool>) {
        self.tools.insert(tool.name().to_string(), tool);
    }

    /// Look up a tool by name.
    pub fn get(&self, name: &str) -> Option<&Arc<dyn Tool>> {
        self.tools.get(name)
    }

    /// Invoke a tool by name with the given parameters.
    pub async fn invoke(&self, name: &str, params: Value) -> anyhow::Result<ToolOutput> {
        match self.tools.get(name) {
            Some(tool) => tool.invoke(params).await,
            None => Ok(ToolOutput::error(name, format!("Unknown tool: {name}"))),
        }
    }

    /// Return tool definitions suitable for passing to an LLM.
    pub fn as_tool_definitions(&self) -> Vec<ToolDefinition> {
        self.tools
            .values()
            .map(|tool| ToolDefinition {
                name: tool.name().to_string(),
                description: tool.description().to_string(),
                input_schema: tool.parameters_schema(),
            })
            .collect()
    }

    /// Number of registered tools.
    pub fn len(&self) -> usize {
        self.tools.len()
    }

    /// Whether the registry has no tools.
    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    struct MockTool {
        tool_name: String,
    }

    #[async_trait]
    impl Tool for MockTool {
        fn name(&self) -> &str {
            &self.tool_name
        }

        fn description(&self) -> &str {
            "A mock tool for testing"
        }

        fn parameters_schema(&self) -> ToolSchema {
            ToolSchema {
                schema_type: "object".to_string(),
                properties: serde_json::json!({
                    "query": { "type": "string", "description": "Test query" }
                }),
                required: vec!["query".to_string()],
            }
        }

        async fn invoke(&self, params: Value) -> anyhow::Result<ToolOutput> {
            let query = params
                .get("query")
                .and_then(|v| v.as_str())
                .unwrap_or("none");
            Ok(ToolOutput::success(&self.tool_name, format!("Result for: {query}")))
        }
    }

    #[tokio::test]
    async fn test_register_and_invoke() {
        let mut registry = ToolRegistry::new();
        registry.register(Arc::new(MockTool {
            tool_name: "test_tool".to_string(),
        }));

        assert_eq!(registry.len(), 1);
        assert!(!registry.is_empty());
        assert!(registry.get("test_tool").is_some());

        let output = registry
            .invoke("test_tool", serde_json::json!({"query": "hello"}))
            .await
            .unwrap();

        assert!(output.success);
        assert_eq!(output.name, "test_tool");
        assert_eq!(output.content, "Result for: hello");
    }

    #[tokio::test]
    async fn test_invoke_missing_tool() {
        let registry = ToolRegistry::new();
        let output = registry
            .invoke("nonexistent", serde_json::json!({}))
            .await
            .unwrap();

        assert!(!output.success);
        assert_eq!(output.error.as_deref(), Some("Unknown tool: nonexistent"));
    }

    #[test]
    fn test_as_tool_definitions() {
        let mut registry = ToolRegistry::new();
        registry.register(Arc::new(MockTool {
            tool_name: "tool_a".to_string(),
        }));
        registry.register(Arc::new(MockTool {
            tool_name: "tool_b".to_string(),
        }));

        let defs = registry.as_tool_definitions();
        assert_eq!(defs.len(), 2);

        let names: Vec<&str> = defs.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"tool_a"));
        assert!(names.contains(&"tool_b"));
    }

    #[test]
    fn test_tool_output_constructors() {
        let success = ToolOutput::success("my_tool", "some result");
        assert!(success.success);
        assert_eq!(success.content, "some result");
        assert!(success.error.is_none());

        let err = ToolOutput::error("my_tool", "something went wrong");
        assert!(!err.success);
        assert!(err.content.is_empty());
        assert_eq!(err.error.as_deref(), Some("something went wrong"));
    }
}
