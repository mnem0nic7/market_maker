"use client";

import { useState, useCallback } from "react";
import { useDashboard, useDashboardDispatch } from "@/lib/store";
import { api } from "@/lib/api";
import { getStrategyTypeInfo } from "@/lib/types";
import type { StrategyMetrics, StrategyStatus } from "@/lib/types";
import { OptimizeModal } from "./OptimizeModal";
import { StrategyDrawer } from "./StrategyDrawer";

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

function formatSymbolValue(value: unknown): string | null {
  if (!value) return null;
  if (typeof value === "string") {
    return value;
  }
  if (
    typeof value === "object" &&
    value !== null &&
    "base" in value &&
    "quote" in value &&
    typeof value.base === "string" &&
    typeof value.quote === "string"
  ) {
    return `${value.base}/${value.quote}`;
  }
  return null;
}

function formatStrategyVenue(strategy: StrategyStatus): string {
  if (strategy.config.strategy_type !== "stat_arb") {
    return `${strategy.config.exchange} | ${strategy.config.symbol.base}/${strategy.config.symbol.quote}`;
  }

  const legA = formatSymbolValue(strategy.config.params.leg_a_symbol);
  const legB = formatSymbolValue(strategy.config.params.leg_b_symbol);

  if (legA && legB) {
    return `Pair | ${legA} vs ${legB}`;
  }

  return `${strategy.config.exchange} | ${strategy.config.symbol.base}/${strategy.config.symbol.quote}`;
}

async function refreshStrategies(dispatch: ReturnType<typeof useDashboardDispatch>) {
  const updated = await api.strategies();
  dispatch({ type: "SET_STRATEGIES", strategies: updated });
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

export function StrategiesPanel() {
  const { strategies, metrics, loading: storeLoading } = useDashboard();
  const dispatch = useDashboardDispatch();
  const [loadingIds, setLoadingIds] = useState<Set<string>>(new Set());
  const [optimizeTarget, setOptimizeTarget] = useState<StrategyStatus | null>(null);
  const [selectedStrategyId, setSelectedStrategyId] = useState<string | null>(null);

  const selectedStrategy = strategies.find((strategy) => strategy.config.strategy_id === selectedStrategyId) ?? null;

  const handleToggle = useCallback(
    async (strategyId: string, running: boolean) => {
      setLoadingIds((prev) => new Set(prev).add(strategyId));
      try {
        if (running) {
          await api.stopStrategy(strategyId);
        } else {
          await api.startStrategy(strategyId);
        }
        await refreshStrategies(dispatch);
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
        await refreshStrategies(dispatch);
      } catch (e) {
        console.error("Failed to apply optimized params:", e);
      } finally {
        setOptimizeTarget(null);
      }
    },
    [optimizeTarget, dispatch]
  );

  if (storeLoading) {
    return (
      <div className="flex flex-col gap-2 animate-pulse">
        <div className="h-10 w-full bg-bg-secondary rounded-lg" />
        <div className="h-10 w-full bg-bg-secondary rounded-lg" />
      </div>
    );
  }

  if (strategies.length === 0) {
    return <p className="text-text-muted text-sm">No strategies configured</p>;
  }

  return (
    <div className="flex flex-col gap-2">
      {strategies.map((strat) => {
        const id = strat.config.strategy_id;
        const isLoading = loadingIds.has(id);
        const m = metrics[id];
        const strategyInfo = getStrategyTypeInfo(strat.config.strategy_type);
        const strategyLabel = strategyInfo?.label ?? strat.config.strategy_type;
        const strategyCategory = strategyInfo?.category ?? "Strategy";
        const strategyVenue = formatStrategyVenue(strat);

        return (
          <div
            key={id}
            className="overflow-hidden rounded-lg border border-border-panel bg-bg-primary/50 transition-colors hover:border-accent-cyan/40"
          >
            <div
              role="button"
              tabIndex={0}
              onClick={() => setSelectedStrategyId(id)}
              onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  setSelectedStrategyId(id);
                }
              }}
              className="flex w-full items-center gap-3 px-3 py-2 text-left"
            >
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
                  {strategyLabel} | {strategyCategory} | {strategyVenue}
                </div>
              </div>

              {/* Actions */}
              <div className="flex shrink-0 items-center gap-2">
                <button
                  type="button"
                  onClick={(event) => {
                    event.stopPropagation();
                    setSelectedStrategyId(id);
                  }}
                  className="rounded border border-border-panel px-2 py-1 text-[11px] font-semibold text-text-secondary transition-colors hover:text-text-primary"
                >
                  Open
                </button>
                <button
                  type="button"
                  onClick={(event) => {
                    event.stopPropagation();
                    setOptimizeTarget(strat);
                  }}
                  className="rounded bg-accent-blue/20 px-3 py-1 text-xs font-semibold text-accent-blue transition-colors hover:bg-accent-blue/30"
                >
                  OPT
                </button>
                <button
                  type="button"
                  disabled={isLoading}
                  onClick={(event) => {
                    event.stopPropagation();
                    void handleToggle(id, strat.running);
                  }}
                  className={`rounded px-3 py-1 text-xs font-semibold transition-colors ${
                    strat.running
                      ? "bg-accent-red/20 text-accent-red hover:bg-accent-red/30"
                      : "bg-accent-green/20 text-accent-green hover:bg-accent-green/30"
                  } ${isLoading ? "cursor-not-allowed opacity-50" : ""}`}
                >
                  {isLoading ? "..." : strat.running ? "STOP" : "START"}
                </button>
              </div>
            </div>

            {/* Live metrics row */}
            {m && <MetricsRow m={m} />}
          </div>
        );
      })}

      {selectedStrategy && (
        <StrategyDrawer
          strategy={selectedStrategy}
          metrics={metrics[selectedStrategy.config.strategy_id]}
          onClose={() => setSelectedStrategyId(null)}
          onRefresh={() => refreshStrategies(dispatch)}
          onOptimize={(strategy) => setOptimizeTarget(strategy)}
        />
      )}

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
