use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use crate::tools::{Tool, ToolOutput, ToolSchema};

const TAVILY_API_URL: &str = "https://api.tavily.com/search";

pub struct TavilySearchTool {
    http: reqwest::Client,
    api_key: String,
}

impl TavilySearchTool {
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
impl Tool for TavilySearchTool {
    fn name(&self) -> &str {
        "web_search"
    }

    fn description(&self) -> &str {
        "Search the web using Tavily for recent news, analysis, and information. Returns titles, URLs, and content snippets."
    }

    fn parameters_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "object".into(),
            properties: serde_json::json!({
                "query": {"type": "string", "description": "Search query"},
                "num_results": {"type": "integer", "description": "Maximum results (default: 5)"}
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

        let num_results = params
            .get("num_results")
            .and_then(|v| v.as_u64())
            .unwrap_or(5);

        let body = serde_json::json!({
            "api_key": self.api_key,
            "query": query,
            "max_results": num_results,
            "include_answer": true,
            "include_raw_content": false
        });

        let response = self
            .http
            .post(TAVILY_API_URL)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let text = response.text().await.unwrap_or_default();
            return Ok(ToolOutput::error(
                self.name(),
                format!("Tavily API error: {text}"),
            ));
        }

        let data: Value = response.json().await?;
        Ok(ToolOutput::success(self.name(), serde_json::to_string(&data)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tavily_tool_metadata() {
        let tool = TavilySearchTool::new("test-key".into());
        assert_eq!(tool.name(), "web_search");
        assert!(tool.description().contains("Tavily"));
    }

    #[tokio::test]
    async fn test_tavily_missing_query() {
        let tool = TavilySearchTool::new("test-key".into());
        let output = tool.invoke(serde_json::json!({})).await.unwrap();
        assert!(!output.success);
    }
}
