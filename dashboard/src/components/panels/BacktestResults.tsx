"use client";

import { useState, useCallback, useEffect } from "react";
import { api } from "@/lib/api";
import { STRATEGY_TYPES } from "@/lib/types";
import type { BacktestResult } from "@/lib/types";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

function formatPercent(value: string): string {
  const n = parseFloat(value);
  if (Number.isNaN(n)) return value;
  return `${(n * 100).toFixed(2)}%`;
}

function formatDuration(d: { secs: number; nanos: number }): string {
  const totalSecs = d.secs;
  if (totalSecs < 60) return `${totalSecs}s`;
  const m = Math.floor(totalSecs / 60);
  const s = totalSecs % 60;
  if (m < 60) return `${m}m ${s}s`;
  const h = Math.floor(m / 60);
  const rm = m % 60;
  return `${h}h ${rm}m`;
}

function formatDateTime(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  } catch {
    return iso;
  }
}

const inputClass =
  "bg-bg-primary border border-border-panel rounded px-2 py-1.5 text-xs text-text-primary font-mono focus:outline-hidden focus:border-accent-cyan w-full";

// ---------------------------------------------------------------------------
// Run Backtest Form (inline)
// ---------------------------------------------------------------------------

function RunBacktestForm({
  onComplete,
  onCancel,
}: {
  onComplete: (result: BacktestResult) => void;
  onCancel: () => void;
}) {
  const [strategyType, setStrategyType] = useState("");
  const [symbol, setSymbol] = useState("BTC/USDT");
  const [initialCapital, setInitialCapital] = useState("100000");
  const [numEvents, setNumEvents] = useState("500");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = useCallback(async () => {
    if (!strategyType || !symbol) return;

    setSubmitting(true);
    setError(null);

    try {
      const result = await api.runBacktest({
        strategy_type: strategyType,
        symbol,
        initial_capital: initialCapital,
        num_events: parseInt(numEvents, 10) || 500,
      });
      onComplete(result);
    } catch (e) {
      setError(String(e));
    } finally {
      setSubmitting(false);
    }
  }, [strategyType, symbol, initialCapital, numEvents, onComplete]);

  return (
    <div className="flex flex-col gap-2 border border-border-panel rounded-lg p-3 bg-bg-primary/50">
      <div className="text-xs font-semibold text-text-secondary">Run Backtest</div>

      <div className="flex flex-col gap-1">
        <label className="text-text-secondary text-[10px]">Strategy Type</label>
        <select
          value={strategyType}
          onChange={(e) => setStrategyType(e.target.value)}
          className={inputClass}
        >
          <option value="">Select a strategy type...</option>
          {STRATEGY_TYPES.map((t) => (
            <option key={t.type} value={t.type}>
              {t.label}
            </option>
          ))}
        </select>
      </div>

      <div className="flex flex-col gap-1">
        <label className="text-text-secondary text-[10px]">Symbol</label>
        <input
          type="text"
          value={symbol}
          onChange={(e) => setSymbol(e.target.value)}
          placeholder="BTC/USDT"
          className={inputClass}
        />
      </div>

      <div className="grid grid-cols-2 gap-2">
        <div className="flex flex-col gap-1">
          <label className="text-text-secondary text-[10px]">Initial Capital</label>
          <input
            type="text"
            value={initialCapital}
            onChange={(e) => setInitialCapital(e.target.value)}
            className={inputClass}
          />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-text-secondary text-[10px]">Num Events</label>
          <input
            type="number"
            value={numEvents}
            onChange={(e) => setNumEvents(e.target.value)}
            min={100}
            max={10000}
            step={100}
            className={inputClass}
          />
        </div>
      </div>

      {error && (
        <div className="px-2 py-1.5 rounded bg-accent-red/10 border border-accent-red/30 text-accent-red text-[10px]">
          {error}
        </div>
      )}

      <div className="flex gap-2 mt-1">
        <button
          type="button"
          onClick={onCancel}
          className="flex-1 py-1.5 rounded text-xs font-semibold bg-bg-primary border border-border-panel text-text-secondary hover:bg-bg-secondary transition-colors"
        >
          Cancel
        </button>
        <button
          type="button"
          disabled={submitting || !strategyType || !symbol}
          onClick={handleSubmit}
          className={`flex-1 py-1.5 rounded text-xs font-semibold transition-colors ${
            submitting || !strategyType || !symbol
              ? "bg-accent-cyan/10 text-text-muted cursor-not-allowed"
              : "bg-accent-cyan/20 text-accent-cyan hover:bg-accent-cyan/30"
          }`}
        >
          {submitting ? "Running..." : "Run"}
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Result detail (expanded row)
// ---------------------------------------------------------------------------

function ResultDetail({ result }: { result: BacktestResult }) {
  return (
    <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-[10px] px-3 pb-2">
      <div>
        <span className="text-text-muted">Gross PnL:</span>{" "}
        <span className={`font-mono ${pnlColor(result.total_pnl)}`}>{formatPnl(result.total_pnl)}</span>
      </div>
      <div>
        <span className="text-text-muted">Total Fees:</span>{" "}
        <span className="font-mono text-accent-amber">{parseFloat(result.total_fees).toFixed(4)}</span>
      </div>
      <div>
        <span className="text-text-muted">Sortino Ratio:</span>{" "}
        <span className="font-mono text-text-secondary">{parseFloat(result.sortino_ratio).toFixed(4)}</span>
      </div>
      <div>
        <span className="text-text-muted">Profit Factor:</span>{" "}
        <span className="font-mono text-text-secondary">{parseFloat(result.profit_factor).toFixed(4)}</span>
      </div>
      <div>
        <span className="text-text-muted">Avg Trade PnL:</span>{" "}
        <span className={`font-mono ${pnlColor(result.avg_trade_pnl)}`}>{formatPnl(result.avg_trade_pnl)}</span>
      </div>
      <div>
        <span className="text-text-muted">Total Volume:</span>{" "}
        <span className="font-mono text-text-secondary">{parseFloat(result.total_volume).toFixed(4)}</span>
      </div>
      <div>
        <span className="text-text-muted">Num Fills:</span>{" "}
        <span className="font-mono text-text-secondary">{result.num_fills}</span>
      </div>
      <div>
        <span className="text-text-muted">Duration:</span>{" "}
        <span className="font-mono text-text-secondary">{formatDuration(result.duration)}</span>
      </div>
      {result.start_time && (
        <div className="col-span-2">
          <span className="text-text-muted">Period:</span>{" "}
          <span className="font-mono text-text-secondary">
            {formatDateTime(result.start_time)} - {formatDateTime(result.end_time)}
          </span>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main panel
// ---------------------------------------------------------------------------

export function BacktestResultsPanel() {
  const [results, setResults] = useState<BacktestResult[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showForm, setShowForm] = useState(false);
  const [expandedIdx, setExpandedIdx] = useState<Set<number>>(new Set());

  // Fetch results on mount
  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const data = await api.backtestResults();
        if (!cancelled) setResults(data);
      } catch (e) {
        if (!cancelled) setError(String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, []);

  const toggleExpanded = useCallback((idx: number) => {
    setExpandedIdx((prev) => {
      const next = new Set(prev);
      if (next.has(idx)) next.delete(idx);
      else next.add(idx);
      return next;
    });
  }, []);

  const handleBacktestComplete = useCallback((result: BacktestResult) => {
    setResults((prev) => [result, ...prev]);
    setShowForm(false);
  }, []);

  // Loading skeleton
  if (loading) {
    return (
      <div className="flex flex-col gap-3 animate-pulse">
        <div className="h-3 w-48 bg-bg-secondary rounded" />
        <div className="h-3 w-64 bg-bg-secondary rounded" />
        <div className="h-3 w-40 bg-bg-secondary rounded" />
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      {/* Header bar */}
      <div className="flex items-center justify-between px-1">
        <span className="text-xs text-text-secondary">
          Results: <span className="text-text-primary font-mono">{results.length}</span>
        </span>
        <button
          type="button"
          onClick={() => setShowForm((prev) => !prev)}
          className={`text-xs font-semibold px-3 py-1 rounded transition-colors ${
            showForm
              ? "bg-accent-red/20 text-accent-red hover:bg-accent-red/30"
              : "bg-accent-cyan/20 text-accent-cyan hover:bg-accent-cyan/30"
          }`}
        >
          {showForm ? "CANCEL" : "RUN BACKTEST"}
        </button>
      </div>

      {/* Run form */}
      {showForm && (
        <RunBacktestForm
          onComplete={handleBacktestComplete}
          onCancel={() => setShowForm(false)}
        />
      )}

      {/* Error banner */}
      {error && (
        <div className="px-2 py-1.5 rounded bg-accent-red/10 border border-accent-red/30 text-accent-red text-[10px]">
          {error}
        </div>
      )}

      {/* Empty state */}
      {results.length === 0 && !showForm ? (
        <p className="text-text-muted text-sm">No backtest results yet</p>
      ) : results.length > 0 ? (
        <div className="flex flex-col gap-2">
          {results.map((result, idx) => {
            const isExpanded = expandedIdx.has(idx);
            const netN = parseFloat(result.net_pnl);
            const sharpeN = parseFloat(result.sharpe_ratio);

            return (
              <div
                key={idx}
                className="border border-border-panel rounded-lg bg-bg-primary/50 overflow-hidden"
              >
                {/* Summary row */}
                <div className="flex items-center gap-3 px-3 py-2">
                  {/* PnL indicator dot */}
                  <span
                    className={`w-2.5 h-2.5 rounded-full shrink-0 ${
                      netN > 0 ? "bg-accent-green" : netN < 0 ? "bg-accent-red" : "bg-text-muted"
                    }`}
                  />

                  {/* Key metrics */}
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-3 text-xs">
                      <span className="text-text-secondary">
                        Net PnL:{" "}
                        <span className={`font-mono font-semibold ${pnlColor(result.net_pnl)}`}>
                          {formatPnl(result.net_pnl)}
                        </span>
                      </span>
                      <span className="text-text-secondary">
                        Sharpe:{" "}
                        <span className={`font-mono ${sharpeN >= 1 ? "text-accent-green" : sharpeN >= 0 ? "text-text-primary" : "text-accent-red"}`}>
                          {sharpeN.toFixed(2)}
                        </span>
                      </span>
                    </div>
                    <div className="flex items-center gap-3 text-[10px] text-text-muted mt-0.5">
                      <span>
                        Win: <span className="font-mono text-text-secondary">{formatPercent(result.win_rate)}</span>
                      </span>
                      <span>
                        DD: <span className="font-mono text-accent-red">{formatPercent(result.max_drawdown_pct)}</span>
                      </span>
                      <span>
                        Trades: <span className="font-mono text-text-secondary">{result.num_trades}</span>
                      </span>
                      <span>
                        {formatDuration(result.duration)}
                      </span>
                    </div>
                  </div>

                  {/* Expand toggle */}
                  <button
                    type="button"
                    onClick={() => toggleExpanded(idx)}
                    className="text-text-muted hover:text-text-secondary text-xs px-1"
                  >
                    {isExpanded ? "[-]" : "[+]"}
                  </button>
                </div>

                {/* Expanded detail */}
                {isExpanded && (
                  <div className="border-t border-border-panel pt-2">
                    <ResultDetail result={result} />
                  </div>
                )}
              </div>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}
