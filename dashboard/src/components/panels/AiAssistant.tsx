"use client";

import { useState, useRef, useCallback } from "react";
import ReactMarkdown from "react-markdown";
import { api } from "@/lib/api";

interface ChatMsg {
  role: "user" | "assistant";
  content: string;
}

export function AiAssistantPanel() {
  const [messages, setMessages] = useState<ChatMsg[]>([]);
  const [input, setInput] = useState("");
  const [streaming, setStreaming] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [available, setAvailable] = useState(true);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  const handleSend = useCallback(async () => {
    if (!input.trim() || streaming) return;

    const userMsg: ChatMsg = { role: "user", content: input.trim() };
    const newMessages = [...messages, userMsg];
    setMessages(newMessages);
    setInput("");
    setStreaming(true);
    setError(null);

    // Add empty assistant message that we'll stream into
    const assistantMsg: ChatMsg = { role: "assistant", content: "" };
    setMessages([...newMessages, assistantMsg]);

    try {
      const stream = api.assistantChat(newMessages);
      let fullContent = "";
      for await (const chunk of stream) {
        fullContent += chunk;
        setMessages((prev) => {
          const updated = [...prev];
          updated[updated.length - 1] = {
            role: "assistant",
            content: fullContent,
          };
          return updated;
        });
        scrollToBottom();
      }
    } catch (e) {
      const errMsg = e instanceof Error ? e.message : String(e);
      if (errMsg.includes("503") || errMsg.includes("not configured")) {
        setAvailable(false);
      }
      setError(errMsg);
    } finally {
      setStreaming(false);
      scrollToBottom();
    }
  }, [input, messages, streaming]);

  if (!available) {
    return (
      <div className="flex items-center justify-center h-full text-text-muted text-sm">
        AI Assistant not configured. Set MM_ASSISTANT__ANTHROPIC_API_KEY.
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      {/* Context indicator */}
      <div className="text-[10px] text-text-muted px-2 py-1 border-b border-border-panel">
        Context: positions, strategies, market data, risk state
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto p-2 space-y-3">
        {messages.length === 0 && (
          <p className="text-text-muted text-xs text-center mt-4">
            Ask about strategies, parameters, or market conditions.
          </p>
        )}
        {messages.map((msg, i) => (
          <div
            key={i}
            className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
          >
            <div
              className={`max-w-[85%] rounded px-3 py-2 text-xs ${
                msg.role === "user"
                  ? "bg-accent-cyan/20 text-text-primary"
                  : "bg-bg-primary text-text-primary"
              }`}
            >
              {msg.role === "assistant" ? (
                <div className="ai-markdown">
                  <ReactMarkdown>{msg.content}</ReactMarkdown>
                </div>
              ) : (
                <p>{msg.content}</p>
              )}
            </div>
          </div>
        ))}
        {streaming && (
          <div className="text-accent-cyan text-xs animate-pulse">
            Thinking...
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      {/* Error */}
      {error && <p className="text-accent-red text-xs px-2">{error}</p>}

      {/* Input */}
      <div className="border-t border-border-panel p-2 flex gap-2">
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && handleSend()}
          placeholder="Ask the AI assistant..."
          disabled={streaming}
          className="flex-1 bg-bg-primary border border-border-panel rounded px-2 py-1 text-xs text-text-primary font-mono focus:outline-hidden focus:border-accent-cyan disabled:opacity-50"
        />
        <button
          onClick={handleSend}
          disabled={streaming || !input.trim()}
          className="px-3 py-1 rounded text-xs bg-accent-cyan/20 text-accent-cyan hover:bg-accent-cyan/30 disabled:opacity-50"
        >
          Send
        </button>
      </div>
    </div>
  );
}
