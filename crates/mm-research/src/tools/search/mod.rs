mod exa;
mod perplexity;
mod tavily;

use std::sync::Arc;

use crate::config::ResearchConfig;
use crate::tools::Tool;

/// Build the best available search tool based on configured API keys.
/// Priority: Exa > Perplexity > Tavily.
pub fn build_search_tool(config: &ResearchConfig) -> Option<Arc<dyn Tool>> {
    if let Some(ref key) = config.exa_api_key {
        if !key.is_empty() {
            return Some(Arc::new(exa::ExaSearchTool::new(key.clone())));
        }
    }
    if let Some(ref key) = config.perplexity_api_key {
        if !key.is_empty() {
            return Some(Arc::new(perplexity::PerplexitySearchTool::new(key.clone())));
        }
    }
    if let Some(ref key) = config.tavily_api_key {
        if !key.is_empty() {
            return Some(Arc::new(tavily::TavilySearchTool::new(key.clone())));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_search_tool_no_keys() {
        let config = ResearchConfig::default();
        assert!(build_search_tool(&config).is_none());
    }

    #[test]
    fn test_build_search_tool_exa_priority() {
        let config = ResearchConfig {
            exa_api_key: Some("exa-key".into()),
            perplexity_api_key: Some("pplx-key".into()),
            ..ResearchConfig::default()
        };
        let tool = build_search_tool(&config).unwrap();
        assert_eq!(tool.name(), "web_search");
        // Exa should win based on priority
        assert!(tool.description().contains("Exa"));
    }

    #[test]
    fn test_build_search_tool_perplexity_fallback() {
        let config = ResearchConfig {
            perplexity_api_key: Some("pplx-key".into()),
            ..ResearchConfig::default()
        };
        let tool = build_search_tool(&config).unwrap();
        assert_eq!(tool.name(), "web_search");
        assert!(tool.description().contains("Perplexity"));
    }

    #[test]
    fn test_build_search_tool_tavily_fallback() {
        let config = ResearchConfig {
            tavily_api_key: Some("tvly-key".into()),
            ..ResearchConfig::default()
        };
        let tool = build_search_tool(&config).unwrap();
        assert_eq!(tool.name(), "web_search");
        assert!(tool.description().contains("Tavily"));
    }

    #[test]
    fn test_empty_key_skipped() {
        let config = ResearchConfig {
            exa_api_key: Some(String::new()),
            tavily_api_key: Some("tvly-key".into()),
            ..ResearchConfig::default()
        };
        let tool = build_search_tool(&config).unwrap();
        assert!(tool.description().contains("Tavily"));
    }
}
