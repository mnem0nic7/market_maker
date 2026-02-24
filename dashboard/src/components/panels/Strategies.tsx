"use client";

import { useState, useCallback } from "react";
import { useDashboard, useDashboardDispatch } from "@/lib/store";
import { api } from "@/lib/api";

export function StrategiesPanel() {
  const { strategies } = useDashboard();
  const dispatch = useDashboardDispatch();
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [loadingIds, setLoadingIds] = useState<Set<string>>(new Set());

  const toggleExpanded = useCallback((id: string) => {
    setExpandedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  const handleToggle = useCallback(
    async (strategyId: string, running: boolean) => {
      setLoadingIds((prev) => new Set(prev).add(strategyId));
      try {
        if (running) {
          await api.stopStrategy(strategyId);
        } else {
          await api.startStrategy(strategyId);
        }
        const updated = await api.strategies();
        dispatch({ type: "SET_STRATEGIES", strategies: updated });
      } catch (e) {
        console.error("Strategy toggle failed:", e);
      } finally {
        setLoadingIds((prev) => {
          const next = new Set(prev);
          next.delete(strategyId);
          return next;
        });
      }
    },
    [dispatch]
  );

  if (strategies.length === 0) {
    return <p className="text-text-muted text-sm">No strategies configured</p>;
  }

  return (
    <div className="flex flex-col gap-2">
      {strategies.map((strat) => {
        const id = strat.config.strategy_id;
        const isExpanded = expandedIds.has(id);
        const isLoading = loadingIds.has(id);

        return (
          <div
            key={id}
            className="border border-border-panel rounded-lg bg-bg-primary/50 overflow-hidden"
          >
            <div className="flex items-center gap-3 px-3 py-2">
              {/* Status dot */}
              <span
                className={`w-2.5 h-2.5 rounded-full shrink-0 ${
                  strat.running
                    ? "bg-accent-green animate-pulse"
                    : "bg-text-muted"
                }`}
              />

              {/* Info */}
              <div className="flex-1 min-w-0">
                <div className="text-xs font-mono text-text-primary truncate">{id}</div>
                <div className="text-[10px] text-text-secondary">
                  {strat.config.strategy_type} | {strat.config.exchange} |{" "}
                  {strat.config.symbol.base}/{strat.config.symbol.quote}
                </div>
              </div>

              {/* Actions */}
              <button
                type="button"
                onClick={() => toggleExpanded(id)}
                className="text-text-muted hover:text-text-secondary text-xs px-1"
              >
                {isExpanded ? "[-]" : "[+]"}
              </button>
              <button
                type="button"
                disabled={isLoading}
                onClick={() => handleToggle(id, strat.running)}
                className={`text-xs font-semibold px-3 py-1 rounded transition-colors ${
                  strat.running
                    ? "bg-accent-red/20 text-accent-red hover:bg-accent-red/30"
                    : "bg-accent-green/20 text-accent-green hover:bg-accent-green/30"
                } ${isLoading ? "opacity-50 cursor-not-allowed" : ""}`}
              >
                {isLoading ? "..." : strat.running ? "STOP" : "START"}
              </button>
            </div>

            {/* Expanded params */}
            {isExpanded && (
              <div className="px-3 pb-2 border-t border-border-panel">
                <div className="text-[10px] text-text-muted mt-2 mb-1">Parameters</div>
                <pre className="text-[10px] font-mono text-text-secondary bg-bg-primary rounded p-2 overflow-auto max-h-32">
                  {JSON.stringify(strat.config.params, null, 2)}
                </pre>
                {strat.started_at && (
                  <div className="text-[10px] text-text-muted mt-1">
                    Started: {strat.started_at}
                  </div>
                )}
                {strat.stopped_at && (
                  <div className="text-[10px] text-text-muted">
                    Stopped: {strat.stopped_at}
                  </div>
                )}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
