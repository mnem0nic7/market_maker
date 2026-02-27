use std::sync::Arc;
use std::time::Instant;

use futures::Stream;
use tracing::{debug, info, warn};

use super::events::AgentEvent;
use super::loop_detector::{jaccard_similarity, LOOP_THRESHOLD};
use super::scratchpad::Scratchpad;
use crate::config::ResearchConfig;
use crate::llm::{ContentBlock, LlmMessage, LlmProvider};
use crate::skills::Skill;
use crate::tools::registry::ToolRegistry;

/// The core research agent runner.
///
/// Executes an iterative tool-calling loop: sends a query to the LLM,
/// invokes any requested tools, feeds results back, and repeats until
/// the LLM produces a final answer or max iterations are reached.
pub struct AgentRunner {
    llm: Arc<dyn LlmProvider>,
    tools: Arc<ToolRegistry>,
    skills: Vec<Skill>,
    config: ResearchConfig,
}

/// Number of recent tool calls to keep during compaction.
const COMPACT_KEEP_RECENT: usize = 3;

impl AgentRunner {
    pub fn new(
        llm: Arc<dyn LlmProvider>,
        tools: Arc<ToolRegistry>,
        skills: Vec<Skill>,
        config: ResearchConfig,
    ) -> Self {
        Self {
            llm,
            tools,
            skills,
            config,
        }
    }

    /// Run the agent loop, returning a stream of events.
    pub fn run(
        self,
        query: String,
        trading_context: Option<String>,
    ) -> impl Stream<Item = AgentEvent> + Send + 'static {
        let llm = Arc::clone(&self.llm);
        let tools = Arc::clone(&self.tools);
        let skills = self.skills.clone();
        let config = self.config.clone();

        async_stream::stream! {
            let system = build_system_message(&skills, &tools, trading_context.as_deref());
            let tool_defs = tools.as_tool_definitions();

            // Convert ToolDefinitions to LLM ToolDefinitions
            let llm_tools: Vec<crate::llm::ToolDefinition> = tool_defs
                .iter()
                .map(|td| crate::llm::ToolDefinition {
                    name: td.name.clone(),
                    description: td.description.clone(),
                    input_schema: td.input_schema.clone(),
                })
                .collect();

            let mut messages: Vec<LlmMessage> = vec![LlmMessage::user(&query)];
            let mut scratchpad = Scratchpad::new();
            let mut total_tool_calls: u32 = 0;
            let mut total_tokens: u64 = 0;
            let mut prev_tool_call_str: Option<String> = None;

            let max_iterations = config.max_iterations;

            for iteration in 1..=max_iterations {
                yield AgentEvent::IterationStart {
                    iteration,
                    max_iterations,
                };

                // Check if we need to compact the scratchpad
                if scratchpad.estimated_tokens() > config.context_token_threshold {
                    let (retained, dropped) = scratchpad.compact(COMPACT_KEEP_RECENT);
                    yield AgentEvent::ContextCompacted {
                        retained_entries: retained,
                        dropped_entries: dropped,
                    };
                    debug!(retained, dropped, "Compacted scratchpad");
                }

                // Call the LLM
                let response = match llm.complete(&system, &messages, &llm_tools, config.max_tokens_per_turn).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        yield AgentEvent::Error {
                            message: format!("LLM error: {e}"),
                        };
                        return;
                    }
                };

                total_tokens += response.usage.input_tokens as u64
                    + response.usage.output_tokens as u64;

                // Process text blocks (thinking)
                let text = response.text();
                if !text.is_empty() {
                    scratchpad.append_thinking(text.clone());
                    yield AgentEvent::Thinking { text };
                }

                // If no tool use, we're done — generate final answer
                if !response.has_tool_use() {
                    break;
                }

                // Process tool calls
                let tool_uses = response.tool_uses();
                let mut tool_result_blocks: Vec<ContentBlock> = Vec::new();

                // Add assistant message with the full response content
                messages.push(LlmMessage::assistant(response.content.clone()));

                for (tool_id, tool_name, tool_input) in &tool_uses {
                    // Loop detection
                    let call_str = format!("{tool_name} {tool_input}");
                    if let Some(ref prev) = prev_tool_call_str {
                        let sim = jaccard_similarity(prev, &call_str);
                        if sim > LOOP_THRESHOLD {
                            yield AgentEvent::LoopDetected { similarity: sim };
                            warn!(similarity = sim, tool = tool_name, "Loop detected, breaking");
                            // Push a tool result telling the LLM to stop looping
                            tool_result_blocks.push(ContentBlock::tool_error(
                                *tool_id,
                                "Loop detected: you called this tool with very similar parameters. Please try a different approach or provide your final answer.",
                            ));
                            prev_tool_call_str = Some(call_str);
                            continue;
                        }
                    }
                    prev_tool_call_str = Some(call_str);

                    yield AgentEvent::ToolStart {
                        tool_name: tool_name.to_string(),
                        params: (*tool_input).clone(),
                    };

                    let start = Instant::now();
                    let output = tools.invoke(tool_name, (*tool_input).clone()).await;
                    let duration_ms = start.elapsed().as_millis() as u64;

                    match output {
                        Ok(tool_output) => {
                            let content = if tool_output.success {
                                tool_output.content.clone()
                            } else {
                                tool_output
                                    .error
                                    .clone()
                                    .unwrap_or_else(|| "Tool failed".to_string())
                            };

                            scratchpad.append_tool_call(
                                tool_name.to_string(),
                                (*tool_input).clone(),
                                content.clone(),
                            );

                            yield AgentEvent::ToolEnd {
                                tool_name: tool_name.to_string(),
                                output: content.clone(),
                                duration_ms,
                            };

                            if tool_output.success {
                                tool_result_blocks.push(ContentBlock::tool_result(*tool_id, content));
                            } else {
                                tool_result_blocks.push(ContentBlock::tool_error(*tool_id, content));
                            }
                        }
                        Err(e) => {
                            let err_msg = format!("Tool error: {e}");
                            yield AgentEvent::ToolEnd {
                                tool_name: tool_name.to_string(),
                                output: err_msg.clone(),
                                duration_ms,
                            };
                            tool_result_blocks.push(ContentBlock::tool_error(*tool_id, err_msg));
                        }
                    }

                    total_tool_calls += 1;
                }

                // Add tool results as a user message
                if !tool_result_blocks.is_empty() {
                    messages.push(LlmMessage::tool_results(tool_result_blocks));
                }
            }

            // Generate final answer using scratchpad context
            yield AgentEvent::AnswerStart;

            let answer_prompt = format!(
                "Based on your research, provide a comprehensive answer to the user's question.\n\n\
                 ## Research Context\n{}\n\n\
                 ## Original Question\n{}",
                scratchpad.to_context_string(),
                query
            );

            let answer_messages = vec![LlmMessage::user(answer_prompt)];

            match llm.complete_no_tools(&system, &answer_messages, config.max_tokens_per_turn).await {
                Ok(answer_resp) => {
                    total_tokens += answer_resp.usage.input_tokens as u64
                        + answer_resp.usage.output_tokens as u64;

                    let answer_text = answer_resp.text();
                    if !answer_text.is_empty() {
                        yield AgentEvent::AnswerChunk { text: answer_text };
                    }
                }
                Err(e) => {
                    yield AgentEvent::Error {
                        message: format!("Final answer error: {e}"),
                    };
                    return;
                }
            }

            info!(
                iterations = total_tool_calls,
                tool_calls = total_tool_calls,
                tokens = total_tokens,
                "Agent run complete"
            );

            yield AgentEvent::Done {
                total_iterations: std::cmp::min(
                    total_tool_calls.max(1),
                    config.max_iterations,
                ),
                total_tool_calls,
                total_tokens,
            };
        }
    }
}

/// Build the system message with personality, skills, and optional trading context.
fn build_system_message(
    skills: &[Skill],
    tools: &ToolRegistry,
    trading_context: Option<&str>,
) -> String {
    let mut parts = Vec::new();

    parts.push(
        "You are a financial research agent. Your job is to autonomously gather data, \
         analyze financials, and produce comprehensive research reports.\n\n\
         ## Capabilities\n\
         - Use your tools to gather financial data (income statements, balance sheets, ratios, etc.)\n\
         - Search the web for recent news and analysis\n\
         - Access live trading platform state (positions, strategies, risk)\n\
         - Perform multi-step research by calling tools iteratively\n\n\
         ## Guidelines\n\
         - Be thorough: use multiple data sources before forming conclusions\n\
         - Be specific: cite numbers, dates, and sources\n\
         - Be concise in tool calls but comprehensive in your final answer\n\
         - If a tool fails, try an alternative approach\n\
         - Do NOT call the same tool with identical parameters repeatedly"
            .to_string(),
    );

    // Add skills metadata
    if !skills.is_empty() {
        let mut skills_section = String::from("\n\n## Available Skills\n");
        for skill in skills {
            skills_section.push_str(&format!("- **{}**: {}\n", skill.name, skill.description));
        }
        skills_section
            .push_str("\nYou can reference these skills when performing specialized analysis.");
        parts.push(skills_section);
    }

    // Add tool summary
    let tool_count = tools.len();
    if tool_count > 0 {
        parts.push(format!(
            "\n\nYou have access to {tool_count} tools. Use them to gather data before answering."
        ));
    }

    // Add trading context if available
    if let Some(ctx) = trading_context {
        parts.push(format!("\n\n## Live Trading Context\n{ctx}"));
    }

    parts.join("")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::types::*;
    use crate::tools::{Tool, ToolOutput, ToolSchema};
    use async_trait::async_trait;
    use futures::StreamExt;
    use std::sync::Mutex;

    // --- Mock LLM ---

    struct MockLlm {
        responses: Mutex<Vec<LlmResponse>>,
    }

    impl MockLlm {
        fn new(responses: Vec<LlmResponse>) -> Self {
            Self {
                responses: Mutex::new(responses),
            }
        }
    }

    #[async_trait]
    impl LlmProvider for MockLlm {
        fn name(&self) -> &str {
            "mock"
        }

        async fn complete(
            &self,
            _system: &str,
            _messages: &[LlmMessage],
            _tools: &[ToolDefinition],
            _max_tokens: u32,
        ) -> anyhow::Result<LlmResponse> {
            let mut responses = self.responses.lock().unwrap();
            if responses.is_empty() {
                Ok(LlmResponse {
                    content: vec![ContentBlock::text("Fallback answer")],
                    stop_reason: StopReason::EndTurn,
                    usage: Usage::default(),
                })
            } else {
                Ok(responses.remove(0))
            }
        }

        async fn complete_no_tools(
            &self,
            _system: &str,
            _messages: &[LlmMessage],
            _max_tokens: u32,
        ) -> anyhow::Result<LlmResponse> {
            Ok(LlmResponse {
                content: vec![ContentBlock::text("Final answer from mock LLM.")],
                stop_reason: StopReason::EndTurn,
                usage: Usage {
                    input_tokens: 50,
                    output_tokens: 20,
                },
            })
        }
    }

    // --- Mock Tool ---

    struct MockTool;

    #[async_trait]
    impl Tool for MockTool {
        fn name(&self) -> &str {
            "mock_tool"
        }
        fn description(&self) -> &str {
            "A mock tool"
        }
        fn parameters_schema(&self) -> ToolSchema {
            ToolSchema {
                schema_type: "object".into(),
                properties: serde_json::json!({"q": {"type": "string"}}),
                required: vec![],
            }
        }
        async fn invoke(&self, _params: serde_json::Value) -> anyhow::Result<ToolOutput> {
            Ok(ToolOutput::success(
                "mock_tool",
                r#"{"result": "mock data"}"#,
            ))
        }
    }

    fn test_config() -> ResearchConfig {
        ResearchConfig {
            max_iterations: 5,
            max_tokens_per_turn: 1024,
            context_token_threshold: 50_000,
            ..ResearchConfig::default()
        }
    }

    #[tokio::test]
    async fn test_single_turn_no_tools() {
        let llm = Arc::new(MockLlm::new(vec![LlmResponse {
            content: vec![ContentBlock::text("The P/E ratio is 28.5")],
            stop_reason: StopReason::EndTurn,
            usage: Usage {
                input_tokens: 100,
                output_tokens: 20,
            },
        }]));

        let tools = Arc::new(ToolRegistry::new());
        let runner = AgentRunner::new(llm, tools, vec![], test_config());
        let events: Vec<AgentEvent> = runner
            .run("What is AAPL's P/E?".into(), None)
            .collect()
            .await;

        // Should see: IterationStart, Thinking, AnswerStart, AnswerChunk, Done
        let types: Vec<&str> = events
            .iter()
            .map(|e| match e {
                AgentEvent::IterationStart { .. } => "iteration_start",
                AgentEvent::Thinking { .. } => "thinking",
                AgentEvent::AnswerStart => "answer_start",
                AgentEvent::AnswerChunk { .. } => "answer_chunk",
                AgentEvent::Done { .. } => "done",
                _ => "other",
            })
            .collect();

        assert!(types.contains(&"iteration_start"));
        assert!(types.contains(&"thinking"));
        assert!(types.contains(&"answer_start"));
        assert!(types.contains(&"answer_chunk"));
        assert!(types.contains(&"done"));
    }

    #[tokio::test]
    async fn test_multi_turn_with_tool() {
        // Turn 1: LLM requests a tool call
        let turn1 = LlmResponse {
            content: vec![
                ContentBlock::text("Let me look that up."),
                ContentBlock::tool_use("tc_1", "mock_tool", serde_json::json!({"q": "AAPL"})),
            ],
            stop_reason: StopReason::ToolUse,
            usage: Usage {
                input_tokens: 100,
                output_tokens: 30,
            },
        };

        // Turn 2: LLM gives final answer
        let turn2 = LlmResponse {
            content: vec![ContentBlock::text("Based on the data, the answer is 28.5")],
            stop_reason: StopReason::EndTurn,
            usage: Usage {
                input_tokens: 200,
                output_tokens: 40,
            },
        };

        let llm = Arc::new(MockLlm::new(vec![turn1, turn2]));

        let mut registry = ToolRegistry::new();
        registry.register(Arc::new(MockTool));
        let tools = Arc::new(registry);

        let runner = AgentRunner::new(llm, tools, vec![], test_config());
        let events: Vec<AgentEvent> = runner
            .run("What is AAPL's P/E?".into(), None)
            .collect()
            .await;

        let has_tool_start = events.iter().any(
            |e| matches!(e, AgentEvent::ToolStart { tool_name, .. } if tool_name == "mock_tool"),
        );
        let has_tool_end = events.iter().any(
            |e| matches!(e, AgentEvent::ToolEnd { tool_name, .. } if tool_name == "mock_tool"),
        );

        assert!(has_tool_start);
        assert!(has_tool_end);
    }

    #[tokio::test]
    async fn test_max_iterations_reached() {
        // LLM always requests tools — should stop at max_iterations
        let tool_response = LlmResponse {
            content: vec![ContentBlock::tool_use(
                "tc_1",
                "mock_tool",
                serde_json::json!({"q": "data"}),
            )],
            stop_reason: StopReason::ToolUse,
            usage: Usage::default(),
        };

        // 5 iterations worth of responses (matching test_config max_iterations)
        let llm = Arc::new(MockLlm::new(vec![
            tool_response.clone(),
            tool_response.clone(),
            tool_response.clone(),
            tool_response.clone(),
            tool_response,
        ]));

        let mut registry = ToolRegistry::new();
        registry.register(Arc::new(MockTool));
        let tools = Arc::new(registry);

        let runner = AgentRunner::new(llm, tools, vec![], test_config());
        let events: Vec<AgentEvent> = runner.run("Analyze AAPL".into(), None).collect().await;

        // Should still get a final answer even when max iterations hit
        let has_answer = events
            .iter()
            .any(|e| matches!(e, AgentEvent::AnswerChunk { .. }));
        let has_done = events.iter().any(|e| matches!(e, AgentEvent::Done { .. }));

        assert!(has_answer);
        assert!(has_done);
    }

    #[test]
    fn test_build_system_message_basic() {
        let tools = ToolRegistry::new();
        let msg = build_system_message(&[], &tools, None);

        assert!(msg.contains("financial research agent"));
        assert!(msg.contains("Guidelines"));
    }

    #[test]
    fn test_build_system_message_with_skills() {
        let tools = ToolRegistry::new();
        let skills = vec![Skill {
            name: "dcf_valuation".into(),
            description: "Run a DCF analysis".into(),
            body: "# Steps...".into(),
            source_path: None,
        }];
        let msg = build_system_message(&skills, &tools, None);

        assert!(msg.contains("Available Skills"));
        assert!(msg.contains("dcf_valuation"));
    }

    #[test]
    fn test_build_system_message_with_trading_context() {
        let tools = ToolRegistry::new();
        let msg = build_system_message(&[], &tools, Some("Kill switch: inactive\nPositions: 0"));

        assert!(msg.contains("Live Trading Context"));
        assert!(msg.contains("Kill switch: inactive"));
    }
}
