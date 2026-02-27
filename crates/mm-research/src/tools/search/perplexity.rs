use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use crate::tools::{Tool, ToolOutput, ToolSchema};

const PERPLEXITY_API_URL: &str = "https://api.perplexity.ai/chat/completions";

pub struct PerplexitySearchTool {
    http: reqwest::Client,
    api_key: String,
}

impl PerplexitySearchTool {
    pub fn new(api_key: String) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
            api_key,
        }
    }
}

#[async_trait]
impl Tool for PerplexitySearchTool {
    fn name(&self) -> &str {
        "web_search"
    }

    fn description(&self) -> &str {
        "Search the web using Perplexity for recent news, analysis, and information. Returns a synthesized answer with citations."
    }

    fn parameters_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "object".into(),
            properties: serde_json::json!({
                "query": {"type": "string", "description": "Search query"},
                "num_results": {"type": "integer", "description": "Not used by Perplexity (included for API compatibility)"}
            }),
            required: vec!["query".into()],
        }
    }

    async fn invoke(&self, params: Value) -> anyhow::Result<ToolOutput> {
        let query = params
            .get("query")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if query.is_empty() {
            return Ok(ToolOutput::error(self.name(), "Missing required parameter: query"));
        }

        let body = serde_json::json!({
            "model": "sonar",
            "messages": [
                {"role": "user", "content": query}
            ]
        });

        let response = self
            .http
            .post(PERPLEXITY_API_URL)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let text = response.text().await.unwrap_or_default();
            return Ok(ToolOutput::error(
                self.name(),
                format!("Perplexity API error: {text}"),
            ));
        }

        let data: Value = response.json().await?;

        // Extract the answer from the chat completion response
        let answer = data["choices"][0]["message"]["content"]
            .as_str()
            .unwrap_or("No response");

        Ok(ToolOutput::success(self.name(), answer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_perplexity_tool_metadata() {
        let tool = PerplexitySearchTool::new("test-key".into());
        assert_eq!(tool.name(), "web_search");
        assert!(tool.description().contains("Perplexity"));
    }

    #[tokio::test]
    async fn test_perplexity_missing_query() {
        let tool = PerplexitySearchTool::new("test-key".into());
        let output = tool.invoke(serde_json::json!({})).await.unwrap();
        assert!(!output.success);
    }
}
