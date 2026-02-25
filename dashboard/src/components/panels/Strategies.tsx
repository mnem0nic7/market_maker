"use client";

import { useState, useCallback } from "react";
import { useDashboard, useDashboardDispatch } from "@/lib/store";
import { api } from "@/lib/api";
import type { StrategyMetrics, StrategyStatus } from "@/lib/types";
import { OptimizeModal } from "./OptimizeModal";

function formatUptime(secs: number): string {
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = Math.floor(secs % 60);
  return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

function pnlColor(value: string): string {
  const n = parseFloat(value);
  if (Number.isNaN(n) || n === 0) return "text-text-secondary";
  return n > 0 ? "text-accent-green" : "text-accent-red";
}

function formatPnl(value: string): string {
  const n = parseFloat(value);
  if (Number.isNaN(n)) return value;
  const sign = n >= 0 ? "+" : "";
  return `${sign}${n.toFixed(4)}`;
}

function MetricsRow({ m }: { m: StrategyMetrics }) {
  const realizedN = parseFloat(m.realized_pnl) || 0;
  const unrealizedN = parseFloat(m.unrealized_pnl) || 0;
  const totalPnl = (realizedN + unrealizedN).toFixed(4);

  return (
    <div className="flex items-center gap-3 px-3 py-1.5 text-xs border-t border-border-panel bg-bg-primary/30">
      <div className="flex items-center gap-1">
        <span className="text-text-secondary">PnL:</span>
        <span className={`font-mono ${pnlColor(totalPnl)}`}>{formatPnl(totalPnl)}</span>
      </div>
      <div className="flex items-center gap-1">
        <span className="text-text-secondary">Fills:</span>
        <span className="font-mono text-text-primary">{m.total_fills}</span>
      </div>
      <div className="flex items-center gap-1">
        <span className="text-text-secondary">Vol:</span>
        <span className="font-mono text-text-primary">{parseFloat(m.volume_traded).toFixed(4)}</span>
      </div>
      <div className="flex items-center gap-1">
        <span className="text-text-secondary">Up:</span>
        <span className="font-mono text-text-primary">{formatUptime(m.uptime_secs)}</span>
      </div>
    </div>
  );
}

function MetricsDetail({ m }: { m: StrategyMetrics }) {
  return (
    <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-[10px] mt-2">
      <div>
        <span className="text-text-muted">Realized PnL:</span>{" "}
        <span className={`font-mono ${pnlColor(m.realized_pnl)}`}>{formatPnl(m.realized_pnl)}</span>
      </div>
      <div>
        <span className="text-text-muted">Unrealized PnL:</span>{" "}
        <span className={`font-mono ${pnlColor(m.unrealized_pnl)}`}>{formatPnl(m.unrealized_pnl)}</span>
      </div>
      <div>
        <span className="text-text-muted">Orders Placed:</span>{" "}
        <span className="font-mono text-text-secondary">{m.total_orders_placed}</span>
      </div>
      <div>
        <span className="text-text-muted">Orders Cancelled:</span>{" "}
        <span className="font-mono text-text-secondary">{m.total_orders_cancelled}</span>
      </div>
      <div>
        <span className="text-text-muted">Orders Rejected:</span>{" "}
        <span className="font-mono text-text-secondary">{m.total_orders_rejected}</span>
      </div>
      <div>
        <span className="text-text-muted">Avg Spread (bps):</span>{" "}
        <span className="font-mono text-text-secondary">{parseFloat(m.avg_spread_captured_bps).toFixed(2)}</span>
      </div>
      <div>
        <span className="text-text-muted">Volume Traded:</span>{" "}
        <span className="font-mono text-text-secondary">{parseFloat(m.volume_traded).toFixed(6)}</span>
      </div>
      <div>
        <span className="text-text-muted">Uptime:</span>{" "}
        <span className="font-mono text-text-secondary">{formatUptime(m.uptime_secs)}</span>
      </div>
      {m.last_fill_at && (
        <div className="col-span-2">
          <span className="text-text-muted">Last Fill:</span>{" "}
          <span className="font-mono text-text-secondary">{m.last_fill_at}</span>
        </div>
      )}
    </div>
  );
}

export function StrategiesPanel() {
  const { strategies, metrics } = useDashboard();
  const dispatch = useDashboardDispatch();
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [loadingIds, setLoadingIds] = useState<Set<string>>(new Set());
  const [optimizeTarget, setOptimizeTarget] = useState<StrategyStatus | null>(null);

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

  const handleOptimizeApply = useCallback(
    async (params: Record<string, unknown>) => {
      if (!optimizeTarget) return;
      try {
        const updatedConfig = {
          ...optimizeTarget.config,
          params: { ...optimizeTarget.config.params, ...params },
        };
        await api.updateStrategy(optimizeTarget.config.strategy_id, updatedConfig);
        const updated = await api.strategies();
        dispatch({ type: "SET_STRATEGIES", strategies: updated });
      } catch (e) {
        console.error("Failed to apply optimized params:", e);
      } finally {
        setOptimizeTarget(null);
      }
    },
    [optimizeTarget, dispatch]
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
        const m = metrics[id];

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
                onClick={() => setOptimizeTarget(strat)}
                className="text-xs font-semibold px-3 py-1 rounded transition-colors bg-accent-blue/20 text-accent-blue hover:bg-accent-blue/30"
              >
                OPT
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

            {/* Live metrics row */}
            {m && <MetricsRow m={m} />}

            {/* Expanded params + metrics detail */}
            {isExpanded && (
              <div className="px-3 pb-2 border-t border-border-panel">
                {m && (
                  <>
                    <div className="text-[10px] text-text-muted mt-2 mb-1">Metrics Detail</div>
                    <MetricsDetail m={m} />
                  </>
                )}
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

      {optimizeTarget && (
        <OptimizeModal
          strategyType={optimizeTarget.config.strategy_type}
          currentParams={optimizeTarget.config.params}
          onClose={() => setOptimizeTarget(null)}
          onApply={handleOptimizeApply}
        />
      )}
    </div>
  );
}
