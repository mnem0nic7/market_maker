use serde::{Deserialize, Serialize};

/// Configuration for the research agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchConfig {
    /// Whether the research agent is enabled.
    pub enabled: bool,
    /// Maximum iterations of the agent loop.
    pub max_iterations: u32,
    /// Maximum tokens per LLM turn.
    pub max_tokens_per_turn: u32,
    /// Token threshold before context compaction triggers.
    pub context_token_threshold: u32,
    /// LLM provider name ("anthropic" or "openai").
    pub llm_provider: String,
    /// LLM model identifier.
    pub llm_model: String,
    /// Anthropic API key.
    pub anthropic_api_key: Option<String>,
    /// OpenAI API key.
    pub openai_api_key: Option<String>,
    /// Financial data API key (financialdatasets.ai).
    pub financial_data_api_key: Option<String>,
    /// Exa search API key.
    pub exa_api_key: Option<String>,
    /// Perplexity search API key.
    pub perplexity_api_key: Option<String>,
    /// Tavily search API key.
    pub tavily_api_key: Option<String>,
    /// Directories to scan for skill files.
    pub skill_dirs: Vec<String>,
}

impl Default for ResearchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_iterations: 10,
            max_tokens_per_turn: 4096,
            context_token_threshold: 80_000,
            llm_provider: "anthropic".to_string(),
            llm_model: "claude-sonnet-4-20250514".to_string(),
            anthropic_api_key: None,
            openai_api_key: None,
            financial_data_api_key: None,
            exa_api_key: None,
            perplexity_api_key: None,
            tavily_api_key: None,
            skill_dirs: vec!["skills".to_string()],
        }
    }
}

impl ResearchConfig {
    /// Load configuration from `MM_RESEARCH__*` environment variables,
    /// falling back to defaults for any missing values.
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(v) = std::env::var("MM_RESEARCH__ENABLED") {
            config.enabled = v.parse().unwrap_or(config.enabled);
        }
        if let Ok(v) = std::env::var("MM_RESEARCH__MAX_ITERATIONS") {
            config.max_iterations = v.parse().unwrap_or(config.max_iterations);
        }
        if let Ok(v) = std::env::var("MM_RESEARCH__MAX_TOKENS_PER_TURN") {
            config.max_tokens_per_turn = v.parse().unwrap_or(config.max_tokens_per_turn);
        }
        if let Ok(v) = std::env::var("MM_RESEARCH__CONTEXT_TOKEN_THRESHOLD") {
            config.context_token_threshold = v.parse().unwrap_or(config.context_token_threshold);
        }
        if let Ok(v) = std::env::var("MM_RESEARCH__LLM_PROVIDER") {
            config.llm_provider = v;
        }
        if let Ok(v) = std::env::var("MM_RESEARCH__LLM_MODEL") {
            config.llm_model = v;
        }
        if let Ok(v) = std::env::var("MM_RESEARCH__ANTHROPIC_API_KEY") {
            config.anthropic_api_key = Some(v);
        }
        if let Ok(v) = std::env::var("MM_RESEARCH__OPENAI_API_KEY") {
            config.openai_api_key = Some(v);
        }
        if let Ok(v) = std::env::var("MM_RESEARCH__FINANCIAL_DATA_API_KEY") {
            config.financial_data_api_key = Some(v);
        }
        if let Ok(v) = std::env::var("MM_RESEARCH__EXA_API_KEY") {
            config.exa_api_key = Some(v);
        }
        if let Ok(v) = std::env::var("MM_RESEARCH__PERPLEXITY_API_KEY") {
            config.perplexity_api_key = Some(v);
        }
        if let Ok(v) = std::env::var("MM_RESEARCH__TAVILY_API_KEY") {
            config.tavily_api_key = Some(v);
        }
        if let Ok(v) = std::env::var("MM_RESEARCH__SKILL_DIRS") {
            config.skill_dirs = v.split(',').map(|s| s.trim().to_string()).collect();
        }

        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = ResearchConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_iterations, 10);
        assert_eq!(config.max_tokens_per_turn, 4096);
        assert_eq!(config.context_token_threshold, 80_000);
        assert_eq!(config.llm_provider, "anthropic");
        assert_eq!(config.llm_model, "claude-sonnet-4-20250514");
        assert!(config.anthropic_api_key.is_none());
        assert!(config.openai_api_key.is_none());
        assert!(config.financial_data_api_key.is_none());
        assert_eq!(config.skill_dirs, vec!["skills"]);
    }

    #[test]
    fn test_from_env_overrides() {
        std::env::set_var("MM_RESEARCH__MAX_ITERATIONS", "20");
        std::env::set_var("MM_RESEARCH__LLM_PROVIDER", "openai");
        std::env::set_var("MM_RESEARCH__ANTHROPIC_API_KEY", "sk-test-key");
        std::env::set_var("MM_RESEARCH__SKILL_DIRS", "skills,custom_skills");

        let config = ResearchConfig::from_env();

        assert_eq!(config.max_iterations, 20);
        assert_eq!(config.llm_provider, "openai");
        assert_eq!(config.anthropic_api_key.as_deref(), Some("sk-test-key"));
        assert_eq!(config.skill_dirs, vec!["skills", "custom_skills"]);

        std::env::remove_var("MM_RESEARCH__MAX_ITERATIONS");
        std::env::remove_var("MM_RESEARCH__LLM_PROVIDER");
        std::env::remove_var("MM_RESEARCH__ANTHROPIC_API_KEY");
        std::env::remove_var("MM_RESEARCH__SKILL_DIRS");
    }

    #[test]
    fn test_from_env_invalid_values_use_defaults() {
        std::env::set_var("MM_RESEARCH__MAX_ITERATIONS", "not_a_number");
        std::env::set_var("MM_RESEARCH__ENABLED", "not_a_bool");

        let config = ResearchConfig::from_env();

        assert_eq!(config.max_iterations, 10);
        assert!(config.enabled);

        std::env::remove_var("MM_RESEARCH__MAX_ITERATIONS");
        std::env::remove_var("MM_RESEARCH__ENABLED");
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = ResearchConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let roundtrip: ResearchConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(roundtrip.max_iterations, config.max_iterations);
        assert_eq!(roundtrip.llm_provider, config.llm_provider);
    }
}
