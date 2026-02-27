"use client";

import { useState, useRef, useCallback } from "react";
import ReactMarkdown from "react-markdown";
import { api } from "@/lib/api";

interface AgentEvent {
  type: string;
  [key: string]: unknown;
}

interface ToolCallEntry {
  name: string;
  params: unknown;
  output?: string;
  duration_ms?: number;
  expanded: boolean;
}

export function ResearchAgentPanel() {
  const [query, setQuery] = useState("");
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [available, setAvailable] = useState(true);
  const [includeTradingCtx, setIncludeTradingCtx] = useState(true);

  // Agent state
  const [iteration, setIteration] = useState(0);
  const [maxIterations, setMaxIterations] = useState(0);
  const [thinking, setThinking] = useState("");
  const [toolCalls, setToolCalls] = useState<ToolCallEntry[]>([]);
  const [answer, setAnswer] = useState("");
  const [stats, setStats] = useState<{
    total_iterations: number;
    total_tool_calls: number;
    total_tokens: number;
  } | null>(null);

  const scrollRef = useRef<HTMLDivElement>(null);
  const scrollToBottom = () =>
    scrollRef.current?.scrollIntoView({ behavior: "smooth" });

  const handleRun = useCallback(async () => {
    if (!query.trim() || running) return;

    setRunning(true);
    setError(null);
    setIteration(0);
    setMaxIterations(0);
    setThinking("");
    setToolCalls([]);
    setAnswer("");
    setStats(null);

    try {
      const stream = api.researchQuery(query.trim(), includeTradingCtx);
      for await (const event of stream) {
        switch (event.type) {
          case "IterationStart":
            setIteration(event.iteration as number);
            setMaxIterations(event.max_iterations as number);
            break;
          case "Thinking":
            setThinking(event.text as string);
            break;
          case "ToolStart":
            setToolCalls((prev) => [
              ...prev,
              {
                name: event.tool_name as string,
                params: event.params,
                expanded: false,
              },
            ]);
            break;
          case "ToolEnd":
            setToolCalls((prev) => {
              const updated = [...prev];
              const idx = updated.findLastIndex(
                (tc) => tc.name === (event.tool_name as string) && !tc.output
              );
              if (idx >= 0) {
                updated[idx] = {
                  ...updated[idx],
                  output: event.output as string,
                  duration_ms: event.duration_ms as number,
                };
              }
              return updated;
            });
            break;
          case "AnswerStart":
            setThinking("");
            break;
          case "AnswerChunk":
            setAnswer((prev) => prev + (event.text as string));
            scrollToBottom();
            break;
          case "Done":
            setStats({
              total_iterations: event.total_iterations as number,
              total_tool_calls: event.total_tool_calls as number,
              total_tokens: event.total_tokens as number,
            });
            break;
          case "Error":
            setError(event.message as string);
            break;
          case "LoopDetected":
            setThinking("Loop detected, breaking...");
            break;
          case "ContextCompacted":
            setThinking("Compacting context...");
            break;
        }
      }
    } catch (e) {
      const errMsg = e instanceof Error ? e.message : String(e);
      if (errMsg.includes("not configured") || errMsg.includes("disabled")) {
        setAvailable(false);
      }
      setError(errMsg);
    } finally {
      setRunning(false);
      scrollToBottom();
    }
  }, [query, running, includeTradingCtx]);

  const toggleToolCall = (idx: number) => {
    setToolCalls((prev) => {
      const updated = [...prev];
      updated[idx] = { ...updated[idx], expanded: !updated[idx].expanded };
      return updated;
    });
  };

  if (!available) {
    return (
      <div className="flex items-center justify-center h-full text-text-muted text-sm">
        Research agent not configured. Set MM_RESEARCH__ANTHROPIC_API_KEY.
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      {/* Header with iteration progress */}
      <div className="flex items-center justify-between text-[10px] text-text-muted px-2 py-1 border-b border-border-panel">
        <span>Autonomous research agent with tool calling</span>
        {running && maxIterations > 0 && (
          <span className="text-accent-cyan">
            Iteration {iteration}/{maxIterations}
          </span>
        )}
      </div>

      {/* Content area */}
      <div className="flex-1 overflow-y-auto p-2 space-y-2">
        {/* Thinking indicator */}
        {thinking && (
          <div className="text-accent-amber text-xs italic px-2 py-1 bg-accent-amber/5 rounded">
            {thinking}
          </div>
        )}

        {/* Tool calls */}
        {toolCalls.length > 0 && (
          <div className="space-y-1">
            <div className="text-[10px] text-text-muted uppercase tracking-wider">
              Tool Calls ({toolCalls.length})
            </div>
            {toolCalls.map((tc, i) => (
              <div
                key={i}
                className="border border-border-panel rounded text-xs"
              >
                <button
                  type="button"
                  onClick={() => toggleToolCall(i)}
                  className="w-full flex items-center justify-between px-2 py-1 hover:bg-bg-primary/50 text-left"
                >
                  <span className="flex items-center gap-2">
                    <span
                      className={`w-1.5 h-1.5 rounded-full ${
                        tc.output
                          ? "bg-accent-green"
                          : "bg-accent-amber animate-pulse"
                      }`}
                    />
                    <span className="font-mono text-accent-cyan">
                      {tc.name}
                    </span>
                  </span>
                  {tc.duration_ms !== undefined && (
                    <span className="text-text-muted">
                      {tc.duration_ms}ms
                    </span>
                  )}
                </button>
                {tc.expanded && (
                  <div className="px-2 py-1 border-t border-border-panel bg-bg-primary/30">
                    <div className="text-[10px] text-text-muted mb-1">
                      Params:
                    </div>
                    <pre className="text-[10px] text-text-secondary whitespace-pre-wrap break-all max-h-24 overflow-y-auto">
                      {JSON.stringify(tc.params, null, 2)}
                    </pre>
                    {tc.output && (
                      <>
                        <div className="text-[10px] text-text-muted mt-1 mb-1">
                          Result:
                        </div>
                        <pre className="text-[10px] text-text-secondary whitespace-pre-wrap break-all max-h-32 overflow-y-auto">
                          {tc.output.length > 500
                            ? tc.output.slice(0, 500) + "..."
                            : tc.output}
                        </pre>
                      </>
                    )}
                  </div>
                )}
              </div>
            ))}
          </div>
        )}

        {/* Answer */}
        {answer && (
          <div className="border-t border-border-panel pt-2">
            <div className="text-[10px] text-text-muted uppercase tracking-wider mb-1">
              Research Report
            </div>
            <div className="ai-markdown text-xs">
              <ReactMarkdown>{answer}</ReactMarkdown>
            </div>
          </div>
        )}

        {/* Stats */}
        {stats && (
          <div className="flex gap-4 text-[10px] text-text-muted pt-1 border-t border-border-panel">
            <span>Iterations: {stats.total_iterations}</span>
            <span>Tool calls: {stats.total_tool_calls}</span>
            <span>Tokens: {stats.total_tokens.toLocaleString()}</span>
          </div>
        )}

        {/* Empty state */}
        {!running && !answer && toolCalls.length === 0 && (
          <p className="text-text-muted text-xs text-center mt-4">
            Ask a research question. The agent will autonomously gather
            financial data, search the web, and produce a report.
          </p>
        )}

        <div ref={scrollRef} />
      </div>

      {/* Error */}
      {error && <p className="text-accent-red text-xs px-2 py-1">{error}</p>}

      {/* Input */}
      <div className="border-t border-border-panel p-2 space-y-2">
        <div className="flex items-center gap-2">
          <label className="flex items-center gap-1 text-[10px] text-text-muted cursor-pointer">
            <input
              type="checkbox"
              checked={includeTradingCtx}
              onChange={(e) => setIncludeTradingCtx(e.target.checked)}
              disabled={running}
              className="rounded"
            />
            Include trading context
          </label>
        </div>
        <div className="flex gap-2">
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) =>
              e.key === "Enter" && !e.shiftKey && handleRun()
            }
            placeholder="e.g., What is AAPL's intrinsic value using DCF?"
            disabled={running}
            className="flex-1 bg-bg-primary border border-border-panel rounded px-2 py-1 text-xs text-text-primary font-mono focus:outline-hidden focus:border-accent-cyan disabled:opacity-50"
          />
          <button
            onClick={handleRun}
            disabled={running || !query.trim()}
            className="px-3 py-1 rounded text-xs bg-accent-cyan/20 text-accent-cyan hover:bg-accent-cyan/30 disabled:opacity-50"
          >
            {running ? "Running..." : "Research"}
          </button>
        </div>
      </div>
    </div>
  );
}
