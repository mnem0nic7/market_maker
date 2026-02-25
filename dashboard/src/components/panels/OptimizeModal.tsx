"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import { api } from "@/lib/api";
import { STRATEGY_TYPES, type OptimizationResult, type StrategyField } from "@/lib/types";

interface OptimizeModalProps {
  strategyType: string;
  currentParams: Record<string, unknown>;
  onClose: () => void;
  onApply: (params: Record<string, unknown>) => void;
}

interface ParamRange {
  name: string;
  from: number;
  to: number;
  step: number;
}

type SortKey = "rank" | "metric_value" | "total_pnl" | "sharpe_ratio" | "win_rate" | "max_drawdown_pct";

const METRICS = [
  { label: "Sharpe Ratio", value: "Sharpe" },
  { label: "Sortino Ratio", value: "Sortino" },
  { label: "Total PnL", value: "TotalPnl" },
  { label: "Max Drawdown", value: "MaxDrawdown" },
  { label: "Profit Factor", value: "ProfitFactor" },
  { label: "Win Rate", value: "WinRate" },
];

function generateValues(from: number, to: number, step: number): number[] {
  const values: number[] = [];
  if (step <= 0 || from > to) return [from];
  for (let v = from; v <= to + step * 0.001; v += step) {
    values.push(parseFloat(v.toFixed(10)));
  }
  return values;
}

function getNumericFields(strategyType: string): StrategyField[] {
  const info = STRATEGY_TYPES.find((s) => s.type === strategyType);
  if (!info) return [];
  return info.fields.filter((f) => f.type === "number");
}

export function OptimizeModal({ strategyType, currentParams, onClose, onApply }: OptimizeModalProps) {
  const numericFields = getNumericFields(strategyType);

  // Parameter ranges state - initialize from current params
  const [ranges, setRanges] = useState<ParamRange[]>(() =>
    numericFields.map((f) => {
      const current = parseFloat(String(currentParams[f.name] ?? f.default));
      const fieldStep = f.step ?? 1;
      return {
        name: f.name,
        from: current,
        to: current,
        step: fieldStep,
      };
    })
  );

  // Optimization settings
  const [metric, setMetric] = useState("Sharpe");
  const [topN, setTopN] = useState(10);
  const [numEvents, setNumEvents] = useState(500);
  const [initialCapital, setInitialCapital] = useState("10000");
  const [feeRate, setFeeRate] = useState("0.001");
  const [slippageBps, setSlippageBps] = useState("5");

  // Job state
  const [jobId, setJobId] = useState<string | null>(null);
  const [status, setStatus] = useState<"idle" | "running" | "completed" | "error">("idle");
  const [progress, setProgress] = useState<{ completed: number; total: number } | null>(null);
  const [results, setResults] = useState<OptimizationResult[]>([]);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Results sorting
  const [sortKey, setSortKey] = useState<SortKey>("rank");
  const [sortAsc, setSortAsc] = useState(true);

  // Count total combinations
  const totalCombinations = ranges.reduce((acc, r) => {
    const vals = generateValues(r.from, r.to, r.step);
    return acc * vals.length;
  }, 1);

  const updateRange = useCallback((index: number, field: keyof ParamRange, value: number) => {
    setRanges((prev) => {
      const next = [...prev];
      next[index] = { ...next[index], [field]: value };
      return next;
    });
  }, []);

  // Polling cleanup
  useEffect(() => {
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, []);

  const handleSubmit = useCallback(async () => {
    setErrorMsg(null);
    setStatus("running");
    setResults([]);
    setProgress(null);

    const paramRanges = ranges
      .filter((r) => {
        const vals = generateValues(r.from, r.to, r.step);
        return vals.length > 1;
      })
      .map((r) => ({
        name: r.name,
        values: generateValues(r.from, r.to, r.step),
      }));

    if (paramRanges.length === 0) {
      setErrorMsg("Select at least one parameter range with more than one value.");
      setStatus("idle");
      return;
    }

    try {
      const res = await api.startOptimization({
        strategy_type: strategyType,
        base_params: currentParams,
        param_ranges: paramRanges,
        backtest_config: {
          initial_capital: initialCapital,
          fee_rate: feeRate,
          slippage_bps: slippageBps,
        },
        metric,
        top_n: topN,
        num_events: numEvents,
      });

      setJobId(res.job_id);

      // Start polling
      pollRef.current = setInterval(async () => {
        try {
          const result = await api.getOptimizationResult(res.job_id);
          if (result.progress) {
            setProgress(result.progress);
          }
          if (result.status === "completed" && result.results) {
            setResults(result.results);
            setStatus("completed");
            if (pollRef.current) clearInterval(pollRef.current);
          } else if (result.status === "failed") {
            setErrorMsg("Optimization job failed on the server.");
            setStatus("error");
            if (pollRef.current) clearInterval(pollRef.current);
          }
        } catch (e) {
          setErrorMsg(String(e));
          setStatus("error");
          if (pollRef.current) clearInterval(pollRef.current);
        }
      }, 2000);
    } catch (e) {
      setErrorMsg(String(e));
      setStatus("error");
    }
  }, [ranges, strategyType, currentParams, metric, topN, numEvents, initialCapital, feeRate, slippageBps]);

  const handleSort = useCallback((key: SortKey) => {
    setSortKey((prev) => {
      if (prev === key) {
        setSortAsc((a) => !a);
        return key;
      }
      setSortAsc(true);
      return key;
    });
  }, []);

  const sortedResults = [...results].map((r, i) => ({ ...r, rank: i + 1 }));
  if (sortKey !== "rank") {
    sortedResults.sort((a, b) => {
      let av: number, bv: number;
      if (sortKey === "metric_value") {
        av = a.metric_value;
        bv = b.metric_value;
      } else {
        av = a.backtest_result[sortKey as keyof typeof a.backtest_result] as number;
        bv = b.backtest_result[sortKey as keyof typeof b.backtest_result] as number;
      }
      return sortAsc ? av - bv : bv - av;
    });
  } else if (!sortAsc) {
    sortedResults.reverse();
  }

  const progressPct = progress ? Math.round((progress.completed / Math.max(progress.total, 1)) * 100) : 0;

  const strategyInfo = STRATEGY_TYPES.find((s) => s.type === strategyType);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm"
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
    >
      <div className="bg-bg-secondary border border-border-panel rounded-xl shadow-2xl w-full max-w-3xl max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-border-panel">
          <div>
            <h2 className="text-sm font-semibold text-text-primary">Parameter Optimization</h2>
            <p className="text-xs text-text-secondary mt-0.5">
              {strategyInfo?.label ?? strategyType} — sweep parameters to find optimal configuration
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="text-text-muted hover:text-text-primary text-lg leading-none px-2"
          >
            x
          </button>
        </div>

        <div className="px-5 py-4 space-y-5">
          {/* Parameter Ranges */}
          <section>
            <h3 className="text-xs font-semibold text-text-secondary mb-2 uppercase tracking-wide">
              Parameter Ranges
            </h3>
            <div className="space-y-2">
              {numericFields.map((field, idx) => {
                const range = ranges[idx];
                const fieldLabel = field.label;
                return (
                  <div key={field.name} className="grid grid-cols-[1fr_auto_auto_auto] gap-2 items-center">
                    <label className="text-xs text-text-secondary truncate" title={field.tooltip}>
                      {fieldLabel}
                    </label>
                    <div className="flex items-center gap-1">
                      <span className="text-[10px] text-text-muted w-8 text-right">From</span>
                      <input
                        type="number"
                        value={range.from}
                        onChange={(e) => updateRange(idx, "from", parseFloat(e.target.value) || 0)}
                        className="w-24 px-2 py-1 text-xs font-mono bg-bg-primary border border-border-panel rounded text-text-primary focus:outline-none focus:border-accent-blue"
                        step={field.step}
                        disabled={status === "running"}
                      />
                    </div>
                    <div className="flex items-center gap-1">
                      <span className="text-[10px] text-text-muted w-6 text-right">To</span>
                      <input
                        type="number"
                        value={range.to}
                        onChange={(e) => updateRange(idx, "to", parseFloat(e.target.value) || 0)}
                        className="w-24 px-2 py-1 text-xs font-mono bg-bg-primary border border-border-panel rounded text-text-primary focus:outline-none focus:border-accent-blue"
                        step={field.step}
                        disabled={status === "running"}
                      />
                    </div>
                    <div className="flex items-center gap-1">
                      <span className="text-[10px] text-text-muted w-8 text-right">Step</span>
                      <input
                        type="number"
                        value={range.step}
                        onChange={(e) => updateRange(idx, "step", parseFloat(e.target.value) || 1)}
                        className="w-24 px-2 py-1 text-xs font-mono bg-bg-primary border border-border-panel rounded text-text-primary focus:outline-none focus:border-accent-blue"
                        step={field.step}
                        min={0.0001}
                        disabled={status === "running"}
                      />
                    </div>
                  </div>
                );
              })}
            </div>
            <p className="text-[10px] text-text-muted mt-2">
              Total combinations: <span className="font-mono text-text-secondary">{totalCombinations.toLocaleString()}</span>
              {" "}(parameters with from = to are held constant)
            </p>
          </section>

          {/* Optimization Settings */}
          <section>
            <h3 className="text-xs font-semibold text-text-secondary mb-2 uppercase tracking-wide">
              Optimization Settings
            </h3>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="text-[10px] text-text-muted block mb-1">Optimization Metric</label>
                <select
                  value={metric}
                  onChange={(e) => setMetric(e.target.value)}
                  disabled={status === "running"}
                  className="w-full px-2 py-1.5 text-xs bg-bg-primary border border-border-panel rounded text-text-primary focus:outline-none focus:border-accent-blue"
                >
                  {METRICS.map((m) => (
                    <option key={m.value} value={m.value}>{m.label}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className="text-[10px] text-text-muted block mb-1">Top N Results</label>
                <input
                  type="number"
                  value={topN}
                  onChange={(e) => setTopN(parseInt(e.target.value) || 10)}
                  min={1}
                  max={100}
                  disabled={status === "running"}
                  className="w-full px-2 py-1.5 text-xs font-mono bg-bg-primary border border-border-panel rounded text-text-primary focus:outline-none focus:border-accent-blue"
                />
              </div>
              <div>
                <label className="text-[10px] text-text-muted block mb-1">Num Events (synthetic data)</label>
                <input
                  type="number"
                  value={numEvents}
                  onChange={(e) => setNumEvents(parseInt(e.target.value) || 500)}
                  min={50}
                  max={100000}
                  disabled={status === "running"}
                  className="w-full px-2 py-1.5 text-xs font-mono bg-bg-primary border border-border-panel rounded text-text-primary focus:outline-none focus:border-accent-blue"
                />
              </div>
              <div>
                <label className="text-[10px] text-text-muted block mb-1">Initial Capital</label>
                <input
                  type="text"
                  value={initialCapital}
                  onChange={(e) => setInitialCapital(e.target.value)}
                  disabled={status === "running"}
                  className="w-full px-2 py-1.5 text-xs font-mono bg-bg-primary border border-border-panel rounded text-text-primary focus:outline-none focus:border-accent-blue"
                />
              </div>
              <div>
                <label className="text-[10px] text-text-muted block mb-1">Fee Rate</label>
                <input
                  type="text"
                  value={feeRate}
                  onChange={(e) => setFeeRate(e.target.value)}
                  disabled={status === "running"}
                  className="w-full px-2 py-1.5 text-xs font-mono bg-bg-primary border border-border-panel rounded text-text-primary focus:outline-none focus:border-accent-blue"
                />
              </div>
              <div>
                <label className="text-[10px] text-text-muted block mb-1">Slippage (bps)</label>
                <input
                  type="text"
                  value={slippageBps}
                  onChange={(e) => setSlippageBps(e.target.value)}
                  disabled={status === "running"}
                  className="w-full px-2 py-1.5 text-xs font-mono bg-bg-primary border border-border-panel rounded text-text-primary focus:outline-none focus:border-accent-blue"
                />
              </div>
            </div>
          </section>

          {/* Progress */}
          {status === "running" && (
            <section>
              <h3 className="text-xs font-semibold text-text-secondary mb-2 uppercase tracking-wide">
                Progress
              </h3>
              <div className="w-full bg-bg-primary rounded-full h-3 border border-border-panel overflow-hidden">
                <div
                  className="h-full bg-accent-blue rounded-full transition-all duration-300"
                  style={{ width: `${progressPct}%` }}
                />
              </div>
              <p className="text-[10px] text-text-muted mt-1 text-center font-mono">
                {progress ? `${progress.completed} / ${progress.total}` : "Starting..."} ({progressPct}%)
              </p>
              {jobId && (
                <p className="text-[10px] text-text-muted text-center mt-0.5">
                  Job ID: <span className="font-mono">{jobId}</span>
                </p>
              )}
            </section>
          )}

          {/* Error */}
          {errorMsg && (
            <div className="bg-accent-red/10 border border-accent-red/30 rounded px-3 py-2">
              <p className="text-xs text-accent-red">{errorMsg}</p>
            </div>
          )}

          {/* Results Table */}
          {status === "completed" && sortedResults.length > 0 && (
            <section>
              <h3 className="text-xs font-semibold text-text-secondary mb-2 uppercase tracking-wide">
                Results ({sortedResults.length})
              </h3>
              <div className="overflow-x-auto border border-border-panel rounded">
                <table className="w-full text-[11px]">
                  <thead>
                    <tr className="bg-bg-primary text-text-muted">
                      {([
                        ["rank", "#"],
                        ["metric_value", "Metric"],
                        ["total_pnl", "Net PnL"],
                        ["sharpe_ratio", "Sharpe"],
                        ["win_rate", "Win Rate"],
                        ["max_drawdown_pct", "Max DD"],
                      ] as [SortKey, string][]).map(([key, label]) => (
                        <th
                          key={key}
                          onClick={() => handleSort(key)}
                          className="px-2 py-1.5 text-left cursor-pointer hover:text-text-secondary select-none whitespace-nowrap"
                        >
                          {label} {sortKey === key ? (sortAsc ? "^" : "v") : ""}
                        </th>
                      ))}
                      <th className="px-2 py-1.5 text-left whitespace-nowrap">Key Params</th>
                      <th className="px-2 py-1.5 text-right whitespace-nowrap">Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedResults.map((r, idx) => (
                      <tr
                        key={idx}
                        className="border-t border-border-panel hover:bg-bg-primary/50 transition-colors"
                      >
                        <td className="px-2 py-1.5 font-mono text-text-secondary">{r.rank}</td>
                        <td className="px-2 py-1.5 font-mono text-accent-blue font-semibold">
                          {r.metric_value.toFixed(4)}
                        </td>
                        <td className={`px-2 py-1.5 font-mono ${r.backtest_result.total_pnl >= 0 ? "text-accent-green" : "text-accent-red"}`}>
                          {r.backtest_result.total_pnl >= 0 ? "+" : ""}{r.backtest_result.total_pnl.toFixed(4)}
                        </td>
                        <td className="px-2 py-1.5 font-mono text-text-primary">
                          {r.backtest_result.sharpe_ratio.toFixed(3)}
                        </td>
                        <td className="px-2 py-1.5 font-mono text-text-primary">
                          {(r.backtest_result.win_rate * 100).toFixed(1)}%
                        </td>
                        <td className="px-2 py-1.5 font-mono text-accent-red">
                          {(r.backtest_result.max_drawdown_pct * 100).toFixed(2)}%
                        </td>
                        <td className="px-2 py-1.5 font-mono text-text-secondary">
                          <span className="truncate max-w-[200px] inline-block" title={JSON.stringify(r.params)}>
                            {Object.entries(r.params)
                              .slice(0, 3)
                              .map(([k, v]) => `${k}=${v}`)
                              .join(", ")}
                            {Object.keys(r.params).length > 3 ? "..." : ""}
                          </span>
                        </td>
                        <td className="px-2 py-1.5 text-right">
                          <button
                            type="button"
                            onClick={() => onApply(r.params)}
                            className="px-2.5 py-0.5 text-[10px] font-semibold bg-accent-blue/20 text-accent-blue hover:bg-accent-blue/30 rounded transition-colors"
                          >
                            Apply
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          )}

          {status === "completed" && sortedResults.length === 0 && (
            <div className="text-center py-4">
              <p className="text-xs text-text-muted">No results returned. Try adjusting parameter ranges.</p>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between px-5 py-3 border-t border-border-panel">
          <button
            type="button"
            onClick={onClose}
            className="px-4 py-1.5 text-xs text-text-secondary hover:text-text-primary border border-border-panel rounded transition-colors"
          >
            {status === "completed" ? "Close" : "Cancel"}
          </button>
          {status !== "running" && status !== "completed" && (
            <button
              type="button"
              onClick={handleSubmit}
              className="px-4 py-1.5 text-xs font-semibold bg-accent-blue/20 text-accent-blue hover:bg-accent-blue/30 rounded transition-colors"
            >
              Run Optimization ({totalCombinations.toLocaleString()} combinations)
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
