use serde::{Deserialize, Serialize};

/// A research skill — a named set of instructions the agent can follow
/// for specialized analysis (e.g., DCF valuation, comparable analysis).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Skill {
    /// Skill name (from YAML frontmatter).
    pub name: String,
    /// Short description of what the skill does.
    pub description: String,
    /// The full markdown body with instructions.
    pub body: String,
    /// Path the skill was loaded from, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
}
