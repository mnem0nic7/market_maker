use serde::{Deserialize, Serialize};

/// Events emitted by the research agent during execution.
///
/// These are streamed to clients via SSE for real-time progress display.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentEvent {
    /// A new iteration of the agent loop has started.
    IterationStart { iteration: u32, max_iterations: u32 },

    /// The agent is thinking (text from the LLM before tool calls).
    Thinking { text: String },

    /// A tool invocation has started.
    ToolStart {
        tool_name: String,
        params: serde_json::Value,
    },

    /// A tool invocation has completed.
    ToolEnd {
        tool_name: String,
        output: String,
        duration_ms: u64,
    },

    /// A skill was invoked during reasoning.
    SkillInvoked { skill_name: String },

    /// Loop detection triggered — consecutive tool calls are too similar.
    LoopDetected { similarity: f64 },

    /// The scratchpad was compacted to stay under the token threshold.
    ContextCompacted {
        retained_entries: usize,
        dropped_entries: usize,
    },

    /// The agent is starting to produce its final answer.
    AnswerStart,

    /// A chunk of the final answer text.
    AnswerChunk { text: String },

    /// The agent run is complete.
    Done {
        total_iterations: u32,
        total_tool_calls: u32,
        total_tokens: u64,
    },

    /// An error occurred during the agent run.
    Error { message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iteration_start_serde() {
        let event = AgentEvent::IterationStart {
            iteration: 1,
            max_iterations: 10,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"type\":\"iteration_start\""));
        assert!(json.contains("\"iteration\":1"));
        let roundtrip: AgentEvent = serde_json::from_str(&json).unwrap();
        match roundtrip {
            AgentEvent::IterationStart {
                iteration,
                max_iterations,
            } => {
                assert_eq!(iteration, 1);
                assert_eq!(max_iterations, 10);
            }
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_thinking_serde() {
        let event = AgentEvent::Thinking {
            text: "Let me analyze the financials.".into(),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"type\":\"thinking\""));
        let _: AgentEvent = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn test_tool_start_serde() {
        let event = AgentEvent::ToolStart {
            tool_name: "get_fundamentals".into(),
            params: serde_json::json!({"ticker": "AAPL"}),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"type\":\"tool_start\""));
        assert!(json.contains("\"tool_name\":\"get_fundamentals\""));
        let _: AgentEvent = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn test_tool_end_serde() {
        let event = AgentEvent::ToolEnd {
            tool_name: "get_fundamentals".into(),
            output: r#"{"pe_ratio": 28.5}"#.into(),
            duration_ms: 342,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"duration_ms\":342"));
        let _: AgentEvent = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn test_done_serde() {
        let event = AgentEvent::Done {
            total_iterations: 5,
            total_tool_calls: 12,
            total_tokens: 45000,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"type\":\"done\""));
        let _: AgentEvent = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn test_error_serde() {
        let event = AgentEvent::Error {
            message: "API key not configured".into(),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"type\":\"error\""));
        let _: AgentEvent = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn test_loop_detected_serde() {
        let event = AgentEvent::LoopDetected { similarity: 0.92 };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"similarity\":0.92"));
        let _: AgentEvent = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn test_context_compacted_serde() {
        let event = AgentEvent::ContextCompacted {
            retained_entries: 5,
            dropped_entries: 8,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"retained_entries\":5"));
        let _: AgentEvent = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn test_answer_events_serde() {
        let start = AgentEvent::AnswerStart;
        let json = serde_json::to_string(&start).unwrap();
        assert!(json.contains("\"type\":\"answer_start\""));
        let _: AgentEvent = serde_json::from_str(&json).unwrap();

        let chunk = AgentEvent::AnswerChunk {
            text: "Based on my analysis...".into(),
        };
        let json = serde_json::to_string(&chunk).unwrap();
        assert!(json.contains("\"type\":\"answer_chunk\""));
        let _: AgentEvent = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn test_skill_invoked_serde() {
        let event = AgentEvent::SkillInvoked {
            skill_name: "dcf_valuation".into(),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"skill_name\":\"dcf_valuation\""));
        let _: AgentEvent = serde_json::from_str(&json).unwrap();
    }
}
