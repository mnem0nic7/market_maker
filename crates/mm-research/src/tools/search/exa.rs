use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use crate::tools::{Tool, ToolOutput, ToolSchema};

const EXA_API_URL: &str = "https://api.exa.ai/search";

pub struct ExaSearchTool {
    http: reqwest::Client,
    api_key: String,
}

impl ExaSearchTool {
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
impl Tool for ExaSearchTool {
    fn name(&self) -> &str {
        "web_search"
    }

    fn description(&self) -> &str {
        "Search the web using Exa for recent news, analysis, and information. Returns titles, URLs, and text snippets."
    }

    fn parameters_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "object".into(),
            properties: serde_json::json!({
                "query": {"type": "string", "description": "Search query"},
                "num_results": {"type": "integer", "description": "Number of results (default: 5)"}
            }),
            required: vec!["query".into()],
        }
    }

    async fn invoke(&self, params: Value) -> anyhow::Result<ToolOutput> {
        let query = params.get("query").and_then(|v| v.as_str()).unwrap_or("");
        if query.is_empty() {
            return Ok(ToolOutput::error(
                self.name(),
                "Missing required parameter: query",
            ));
        }

        let num_results = params
            .get("num_results")
            .and_then(|v| v.as_u64())
            .unwrap_or(5);

        let body = serde_json::json!({
            "query": query,
            "numResults": num_results,
            "type": "auto",
            "contents": {
                "text": true
            }
        });

        let response = self
            .http
            .post(EXA_API_URL)
            .header("x-api-key", &self.api_key)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let text = response.text().await.unwrap_or_default();
            return Ok(ToolOutput::error(
                self.name(),
                format!("Exa API error: {text}"),
            ));
        }

        let data: Value = response.json().await?;
        Ok(ToolOutput::success(
            self.name(),
            serde_json::to_string(&data)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exa_tool_metadata() {
        let tool = ExaSearchTool::new("test-key".into());
        assert_eq!(tool.name(), "web_search");
        assert!(tool.description().contains("Exa"));
        let schema = tool.parameters_schema();
        assert!(schema.required.contains(&"query".to_string()));
    }

    #[tokio::test]
    async fn test_exa_missing_query() {
        let tool = ExaSearchTool::new("test-key".into());
        let output = tool.invoke(serde_json::json!({})).await.unwrap();
        assert!(!output.success);
        assert!(output.error.unwrap().contains("query"));
    }
}
