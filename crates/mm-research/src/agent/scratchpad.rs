use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A single entry in the agent's scratchpad.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScratchpadEntry {
    /// Free-form thinking from the LLM.
    Thinking { text: String },
    /// A tool call and its result.
    ToolCall {
        name: String,
        params: Value,
        result: String,
        timestamp: DateTime<Utc>,
    },
    /// A skill that was loaded into context.
    Skill { name: String, body: String },
}

impl ScratchpadEntry {
    /// Rough token estimate: character count / 4.
    fn estimated_tokens(&self) -> u32 {
        let chars = match self {
            Self::Thinking { text } => text.len(),
            Self::ToolCall {
                name,
                params,
                result,
                ..
            } => name.len() + params.to_string().len() + result.len(),
            Self::Skill { name, body } => name.len() + body.len(),
        };
        (chars / 4) as u32
    }

    fn is_tool_call(&self) -> bool {
        matches!(self, Self::ToolCall { .. })
    }
}

/// Working memory for the agent across iterations.
///
/// Tracks thinking, tool calls, and skills. Supports compaction to stay
/// within the LLM context window.
pub struct Scratchpad {
    entries: Vec<ScratchpadEntry>,
}

impl Scratchpad {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn append_thinking(&mut self, text: String) {
        self.entries.push(ScratchpadEntry::Thinking { text });
    }

    pub fn append_tool_call(&mut self, name: String, params: Value, result: String) {
        self.entries.push(ScratchpadEntry::ToolCall {
            name,
            params,
            result,
            timestamp: Utc::now(),
        });
    }

    pub fn append_skill(&mut self, name: String, body: String) {
        self.entries.push(ScratchpadEntry::Skill { name, body });
    }

    /// Total estimated tokens across all entries.
    pub fn estimated_tokens(&self) -> u32 {
        self.entries.iter().map(|e| e.estimated_tokens()).sum()
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Compact the scratchpad by dropping oldest tool call entries,
    /// keeping the `keep_recent` most recent tool calls plus all
    /// thinking and skill entries.
    ///
    /// Returns `(retained, dropped)` counts.
    pub fn compact(&mut self, keep_recent: usize) -> (usize, usize) {
        let original_len = self.entries.len();

        // Count tool calls from the end to find which to keep
        let mut tool_call_count = 0;

        // Walk backwards to find which tool calls to keep
        let mut kept_tool_indices: Vec<usize> = Vec::new();
        for (i, entry) in self.entries.iter().enumerate().rev() {
            if entry.is_tool_call() {
                tool_call_count += 1;
                if tool_call_count <= keep_recent {
                    kept_tool_indices.push(i);
                }
            }
        }

        if tool_call_count <= keep_recent {
            // Nothing to compact
            return (original_len, 0);
        }

        let kept_tool_indices_set: std::collections::HashSet<usize> =
            kept_tool_indices.into_iter().collect();

        // Keep: all non-tool-call entries + recent tool calls
        let new_entries: Vec<ScratchpadEntry> = self
            .entries
            .drain(..)
            .enumerate()
            .filter(|(i, entry)| !entry.is_tool_call() || kept_tool_indices_set.contains(i))
            .map(|(_, entry)| entry)
            .collect();

        let retained = new_entries.len();
        let dropped = original_len - retained;
        self.entries = new_entries;

        (retained, dropped)
    }

    /// Serialize scratchpad entries to a context string for the final answer prompt.
    pub fn to_context_string(&self) -> String {
        self.entries
            .iter()
            .map(|entry| serde_json::to_string(entry).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Get the last tool call's name and params (for loop detection).
    pub fn last_tool_call(&self) -> Option<(&str, &Value)> {
        self.entries.iter().rev().find_map(|e| match e {
            ScratchpadEntry::ToolCall { name, params, .. } => Some((name.as_str(), params)),
            _ => None,
        })
    }
}

impl Default for Scratchpad {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_and_count() {
        let mut sp = Scratchpad::new();
        assert!(sp.is_empty());

        sp.append_thinking("Let me analyze this.".into());
        sp.append_tool_call(
            "get_price".into(),
            serde_json::json!({"ticker": "AAPL"}),
            "150.25".into(),
        );
        sp.append_skill("dcf_valuation".into(), "# DCF steps...".into());

        assert_eq!(sp.len(), 3);
        assert!(!sp.is_empty());
    }

    #[test]
    fn test_token_estimation() {
        let mut sp = Scratchpad::new();

        // 20 chars / 4 = 5 tokens
        sp.append_thinking("12345678901234567890".into());
        assert_eq!(sp.estimated_tokens(), 5);

        // Adding more increases the total
        sp.append_thinking("1234567890".into());
        assert!(sp.estimated_tokens() > 5);
    }

    #[test]
    fn test_compact_keeps_recent_tool_calls() {
        let mut sp = Scratchpad::new();

        sp.append_thinking("Start analysis".into());

        for i in 0..10 {
            sp.append_tool_call(
                format!("tool_{i}"),
                serde_json::json!({"i": i}),
                format!("result_{i}"),
            );
        }

        assert_eq!(sp.len(), 11); // 1 thinking + 10 tool calls

        let (retained, dropped) = sp.compact(3);

        // Should keep: 1 thinking + 3 recent tool calls = 4
        assert_eq!(retained, 4);
        assert_eq!(dropped, 7);
        assert_eq!(sp.len(), 4);

        // Verify the kept tool calls are the most recent ones
        let last = sp.last_tool_call().unwrap();
        assert_eq!(last.0, "tool_9");
    }

    #[test]
    fn test_compact_no_op_when_under_threshold() {
        let mut sp = Scratchpad::new();
        sp.append_tool_call("t1".into(), serde_json::json!({}), "r1".into());
        sp.append_tool_call("t2".into(), serde_json::json!({}), "r2".into());

        let (retained, dropped) = sp.compact(5);
        assert_eq!(retained, 2);
        assert_eq!(dropped, 0);
    }

    #[test]
    fn test_compact_preserves_skills() {
        let mut sp = Scratchpad::new();

        sp.append_skill("dcf".into(), "# DCF steps".into());
        for i in 0..5 {
            sp.append_tool_call(
                format!("tool_{i}"),
                serde_json::json!({}),
                format!("result_{i}"),
            );
        }
        sp.append_thinking("Intermediate thought".into());

        assert_eq!(sp.len(), 7); // 1 skill + 5 tools + 1 thinking

        let (retained, dropped) = sp.compact(2);

        // Should keep: 1 skill + 1 thinking + 2 recent tools = 4
        assert_eq!(retained, 4);
        assert_eq!(dropped, 3);
    }

    #[test]
    fn test_to_context_string() {
        let mut sp = Scratchpad::new();
        sp.append_thinking("Let me check.".into());
        sp.append_tool_call(
            "get_price".into(),
            serde_json::json!({"ticker": "AAPL"}),
            "150.25".into(),
        );

        let ctx = sp.to_context_string();
        let lines: Vec<&str> = ctx.lines().collect();
        assert_eq!(lines.len(), 2);

        // Each line should be valid JSON
        for line in &lines {
            let _: serde_json::Value = serde_json::from_str(line).unwrap();
        }
    }

    #[test]
    fn test_last_tool_call() {
        let mut sp = Scratchpad::new();
        assert!(sp.last_tool_call().is_none());

        sp.append_thinking("Thinking...".into());
        assert!(sp.last_tool_call().is_none());

        sp.append_tool_call(
            "tool_a".into(),
            serde_json::json!({"q": "hello"}),
            "result_a".into(),
        );
        sp.append_thinking("More thinking".into());

        let (name, params) = sp.last_tool_call().unwrap();
        assert_eq!(name, "tool_a");
        assert_eq!(params["q"], "hello");
    }
}
