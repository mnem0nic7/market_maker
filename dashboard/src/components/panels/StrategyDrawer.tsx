"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { api } from "@/lib/api";
import {
  getStrategyTypeInfo,
  type StrategyDetail,
  type StrategyField,
  type StrategyMetrics,
  type StrategyStatus,
} from "@/lib/types";
import { StrategyGuide } from "./StrategyGuide";

interface StrategyDrawerProps {
  strategy: StrategyStatus | null;
  metrics?: StrategyMetrics;
  onClose: () => void;
  onRefresh: () => Promise<void>;
  onOptimize: (strategy: StrategyStatus) => void;
}

type DraftValues = Record<string, string | boolean>;

function formatTimestamp(value: string | null): string {
  if (!value) return "Not yet recorded";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function formatPnl(value: string): string {
  const number = Number.parseFloat(value);
  if (Number.isNaN(number)) return value;
  return `${number >= 0 ? "+" : ""}${number.toFixed(4)}`;
}

function pnlColor(value: string): string {
  const number = Number.parseFloat(value);
  if (Number.isNaN(number) || number === 0) return "text-text-secondary";
  return number > 0 ? "text-accent-green" : "text-accent-red";
}

function formatMetricValue(value: string, digits = 2): string {
  const number = Number.parseFloat(value);
  if (Number.isNaN(number)) return value;
  return number.toFixed(digits);
}

function formatUptime(secs: number): string {
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = Math.floor(secs % 60);
  return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

function isIntegerLike(field: StrategyField): boolean {
  if (field.step === undefined) return false;
  return Number.isInteger(field.step) && Number.isInteger(field.default as number);
}

function buildDraftValues(strategy: StrategyStatus): DraftValues {
  const info = getStrategyTypeInfo(strategy.config.strategy_type);
  if (!info) return {};

  return Object.fromEntries(
    info.fields.map((field) => {
      const rawValue = strategy.config.params[field.name];
      if (field.type === "boolean") {
        return [field.name, Boolean(rawValue ?? field.default)];
      }
      if (field.type === "json") {
        return [field.name, JSON.stringify(rawValue ?? field.default, null, 2)];
      }
      return [field.name, String(rawValue ?? field.default ?? "")];
    }),
  );
}

function parseDraftValue(field: StrategyField, value: string | boolean, currentValue: unknown): unknown {
  if (field.type === "boolean") {
    return Boolean(value);
  }

  if (field.type === "json") {
    return JSON.parse(String(value));
  }

  if (field.type === "select") {
    return String(value);
  }

  const nextValue = String(value).trim();
  if (nextValue.length === 0) {
    throw new Error(`${field.label} is required`);
  }

  const numericValue = Number(nextValue);
  if (Number.isNaN(numericValue)) {
    throw new Error(`${field.label} must be a number`);
  }

  if (field.min !== undefined && numericValue < field.min) {
    throw new Error(`${field.label} must be at least ${field.min}`);
  }

  if (field.max !== undefined && numericValue > field.max) {
    throw new Error(`${field.label} must be at most ${field.max}`);
  }

  if (typeof currentValue === "string" || typeof field.default === "string") {
    return nextValue;
  }

  if (isIntegerLike(field)) {
    return Math.trunc(numericValue);
  }

  return numericValue;
}

function formatVenue(strategy: StrategyStatus): string {
  const legA = strategy.config.params.leg_a_symbol;
  const legB = strategy.config.params.leg_b_symbol;

  if (
    strategy.config.strategy_type === "stat_arb"
    && legA
    && legB
    && typeof legA === "object"
    && typeof legB === "object"
    && legA !== null
    && legB !== null
    && "base" in legA
    && "quote" in legA
    && "base" in legB
    && "quote" in legB
  ) {
    return `${String(legA.base)}/${String(legA.quote)} vs ${String(legB.base)}/${String(legB.quote)}`;
  }

  return `${strategy.config.exchange} | ${strategy.config.symbol.base}/${strategy.config.symbol.quote}`;
}

function inferRiskPosture(strategy: StrategyStatus): { label: string; reason: string } {
  const params = strategy.config.params;
  const type = strategy.config.strategy_type;
  const orderSize = Number(params.order_size ?? 0);
  const maxPosition = Number(params.max_position ?? 0);

  if (type === "spread") {
    const spreadBps = Number(params.spread_bps ?? 0);
    const numLevels = Number(params.num_levels ?? 0);
    if (spreadBps <= 8 || numLevels >= 2) {
      return {
        label: "Aggressive",
        reason: "Tighter spreads or more levels increase fill rate but raise adverse-selection and churn risk.",
      };
    }
  }

  if (type === "momentum") {
    const lookback = Number(params.lookback_periods ?? 0);
    const entry = Number(params.entry_threshold_bps ?? 0);
    if (lookback <= 10 || entry <= 30) {
      return {
        label: "Aggressive",
        reason: "Short lookbacks and low entry thresholds make the strategy react faster but increase whipsaw risk.",
      };
    }
  }

  if (type === "mean_reversion") {
    const windowPeriods = Number(params.window_periods ?? 0);
    const stdDev = Number(params.num_std_dev ?? 0);
    if (windowPeriods <= 10 || stdDev <= 1.5) {
      return {
        label: "Aggressive",
        reason: "Shorter windows and tighter bands increase trade frequency and the chance of fading a real trend too early.",
      };
    }
  }

  if (maxPosition > 0 && orderSize > maxPosition * 0.5) {
    return {
      label: "Elevated",
      reason: "Large order size relative to max position means each signal can move the strategy near its inventory limit quickly.",
    };
  }

  return {
    label: "Balanced",
    reason: "The current settings look closer to the default operating envelope for this strategy type.",
  };
}

function buildRiskSummary(strategy: StrategyStatus, metrics?: StrategyMetrics) {
  const info = getStrategyTypeInfo(strategy.config.strategy_type);
  const posture = inferRiskPosture(strategy);
  const totalPnl = metrics
    ? (Number.parseFloat(metrics.realized_pnl) + Number.parseFloat(metrics.unrealized_pnl || "0")).toFixed(4)
    : null;

  const upside = [
    info?.bestFor ?? "Designed for a specific market regime.",
    metrics && metrics.total_fills > 0
      ? `Live fills are coming through (${metrics.total_fills}), so the current market is giving the strategy some chance to express its edge.`
      : "No live fills yet, so the strategy has not had enough market interaction to prove its edge in this session.",
    metrics && Number.parseFloat(metrics.avg_spread_captured_bps) > 0
      ? `Average spread capture is ${formatMetricValue(metrics.avg_spread_captured_bps)} bps, which is a useful sign that execution quality is not purely reactive.`
      : "Execution edge will depend mostly on market regime and how tightly the parameters are tuned.",
  ];

  const downside = [
    info?.watchFor ?? "This strategy can underperform when market structure shifts away from the conditions it expects.",
    posture.reason,
    metrics && Number.parseFloat(metrics.max_drawdown) > 0
      ? `Measured max drawdown is ${formatMetricValue(metrics.max_drawdown, 4)}, so this session has already seen a live PnL retracement.`
      : "No meaningful live drawdown is visible yet, so current session stress has been limited from the available metrics.",
    metrics && Number.parseInt(String(metrics.total_orders_rejected), 10) > 0
      ? `The strategy has already seen ${metrics.total_orders_rejected} rejected orders, which can indicate parameter aggressiveness or venue friction.`
      : "No order rejections are visible yet, so there is no obvious operational friction from the current session data.",
    totalPnl !== null && Number.parseFloat(totalPnl) < 0
      ? `Current total PnL is ${formatPnl(totalPnl)}, so the live session is presently moving against the strategy.`
      : "Current session PnL is not showing immediate live stress from the available metrics.",
  ];

  return { posture, upside, downside };
}

function normalizeRiskSummary(
  detail: StrategyDetail | null,
  strategy: StrategyStatus,
  metrics?: StrategyMetrics | null,
) {
  if (detail?.risk_summary) {
    return {
      posture: {
        label: detail.risk_summary.posture,
        reason: detail.risk_summary.downside[0] ?? "Risk summary unavailable.",
      },
      upside: detail.risk_summary.upside,
      downside: detail.risk_summary.downside,
    };
  }

  return buildRiskSummary(strategy, metrics ?? undefined);
}

function knownFieldNames(fields: StrategyField[]): Set<string> {
  return new Set(fields.map((field) => field.name));
}

export function StrategyDrawer({
  strategy,
  metrics,
  onClose,
  onRefresh,
  onOptimize,
}: StrategyDrawerProps) {
  const drawerRef = useRef<HTMLElement | null>(null);
  const [detail, setDetail] = useState<StrategyDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [draftValues, setDraftValues] = useState<DraftValues>({});
  const [saveState, setSaveState] = useState<"idle" | "saving" | "saved">("idle");
  const [actionState, setActionState] = useState<"idle" | "starting" | "stopping">("idle");
  const [error, setError] = useState<string | null>(null);
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
    return () => {
      setMounted(false);
    };
  }, []);

  useEffect(() => {
    if (!mounted || !strategy) return;

    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        onClose();
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    drawerRef.current?.focus();

    return () => {
      document.body.style.overflow = previousOverflow;
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [mounted, onClose, strategy]);

  useEffect(() => {
    if (!strategy) return;
    setDraftValues(buildDraftValues(strategy));
    setSaveState("idle");
    setActionState("idle");
    setError(null);
  }, [strategy]);

  useEffect(() => {
    if (!strategy) return;

    let active = true;
    setDetailLoading(true);

    void api.strategyDetail(strategy.config.strategy_id)
      .then((response) => {
        if (!active) return;
        setDetail(response);
      })
      .catch(() => {
        if (!active) return;
        setDetail(null);
      })
      .finally(() => {
        if (active) {
          setDetailLoading(false);
        }
      });

    return () => {
      active = false;
    };
  }, [strategy]);

  const activeStatus = detail?.status ?? strategy;
  const activeMetrics = detail?.metrics ?? metrics;

  const strategyInfo = useMemo(
    () => (activeStatus ? getStrategyTypeInfo(activeStatus.config.strategy_type) : undefined),
    [activeStatus],
  );

  const riskSummary = useMemo(
    () => (activeStatus ? normalizeRiskSummary(detail, activeStatus, activeMetrics) : null),
    [activeMetrics, activeStatus, detail],
  );

  if (!activeStatus || !strategyInfo) {
    return null;
  }

  if (!mounted) {
    return null;
  }

  const knownParams = knownFieldNames(strategyInfo.fields);
  const advancedParams = Object.fromEntries(
    Object.entries(activeStatus.config.params).filter(([key]) => !knownParams.has(key)),
  );

  const refreshDetail = async () => {
    const response = await api.strategyDetail(activeStatus.config.strategy_id);
    setDetail(response);
  };

  const handleFieldChange = (fieldName: string, value: string | boolean) => {
    setDraftValues((prev) => ({ ...prev, [fieldName]: value }));
    setSaveState("idle");
  };

  const handleSave = async () => {
    setError(null);
    setSaveState("saving");

    try {
      const patch = Object.fromEntries(
        strategyInfo.fields.map((field) => [
          field.name,
          parseDraftValue(field, draftValues[field.name], activeStatus.config.params[field.name]),
        ]),
      );

      await api.patchStrategyParams(activeStatus.config.strategy_id, patch);
      await onRefresh();
      await refreshDetail();
      setSaveState("saved");
    } catch (saveError) {
      setSaveState("idle");
      setError(saveError instanceof Error ? saveError.message : String(saveError));
    }
  };

  const handleToggle = async () => {
    setError(null);
    setActionState(activeStatus.running ? "stopping" : "starting");

    try {
      if (activeStatus.running) {
        await api.stopStrategy(activeStatus.config.strategy_id);
      } else {
        await api.startStrategy(activeStatus.config.strategy_id);
      }
      await onRefresh();
      await refreshDetail();
    } catch (toggleError) {
      setError(toggleError instanceof Error ? toggleError.message : String(toggleError));
    } finally {
      setActionState("idle");
    }
  };

  const totalPnl = activeMetrics
    ? (Number.parseFloat(activeMetrics.realized_pnl) + Number.parseFloat(activeMetrics.unrealized_pnl)).toFixed(4)
    : null;

  const drawer = (
    <>
      <div className="fixed inset-0 z-[200] bg-slate-950/70 backdrop-blur-md" onClick={onClose} />
      <aside
        ref={drawerRef}
        role="dialog"
        aria-modal="true"
        aria-label="Strategy details"
        tabIndex={-1}
        onClick={(event) => event.stopPropagation()}
        className="fixed inset-y-0 right-0 z-[210] w-full max-w-2xl overflow-y-auto border-l border-border-panel bg-bg-panel shadow-2xl outline-none"
      >
        <div className="sticky top-0 z-10 border-b border-border-panel bg-bg-panel/95 px-5 py-4 backdrop-blur">
          <div className="flex items-start justify-between gap-4">
            <div className="min-w-0">
              <div className="flex items-center gap-2">
                <span
                  className={`h-2.5 w-2.5 shrink-0 rounded-full ${activeStatus.running ? "bg-accent-green animate-pulse" : "bg-text-muted"}`}
                />
                <h2 className="truncate text-sm font-semibold text-text-primary">{activeStatus.config.strategy_id}</h2>
              </div>
              <p className="mt-1 text-[11px] uppercase tracking-[0.2em] text-accent-cyan">
                {strategyInfo.label} · {strategyInfo.category}
              </p>
              <p className="mt-1 text-xs text-text-secondary">{formatVenue(activeStatus)}</p>
            </div>

            <div className="flex shrink-0 items-center gap-2">
              <button
                type="button"
                onClick={() => onOptimize(activeStatus)}
                className="rounded border border-accent-blue/30 bg-accent-blue/15 px-3 py-1.5 text-xs font-semibold text-accent-blue transition-colors hover:bg-accent-blue/25"
              >
                Optimize
              </button>
              <button
                type="button"
                onClick={handleToggle}
                disabled={actionState !== "idle" || detailLoading}
                className={`rounded px-3 py-1.5 text-xs font-semibold transition-colors ${activeStatus.running ? "bg-accent-red/20 text-accent-red hover:bg-accent-red/30" : "bg-accent-green/20 text-accent-green hover:bg-accent-green/30"} ${actionState !== "idle" || detailLoading ? "cursor-not-allowed opacity-60" : ""}`}
              >
                {actionState === "starting" ? "Starting..." : actionState === "stopping" ? "Stopping..." : activeStatus.running ? "Stop" : "Start"}
              </button>
              <button
                type="button"
                onClick={onClose}
                className="rounded border border-border-panel px-3 py-1.5 text-xs font-semibold text-text-secondary transition-colors hover:text-text-primary"
              >
                Close
              </button>
            </div>
          </div>
        </div>

        <div className="flex flex-col gap-5 px-5 py-5">
          {error && (
            <div className="rounded-lg border border-accent-red/30 bg-accent-red/10 px-3 py-2 text-sm text-accent-red">
              {error}
            </div>
          )}

          <section className="grid gap-3 md:grid-cols-3">
            <div className="rounded-xl border border-border-panel bg-bg-primary/60 p-3">
              <div className="text-[10px] uppercase tracking-[0.18em] text-text-muted">State</div>
              <div className="mt-2 text-sm font-semibold text-text-primary">
                {activeStatus.running ? "Running" : detailLoading ? "Loading..." : "Stopped"}
              </div>
              <div className="mt-2 text-[11px] text-text-secondary">
                Started: {formatTimestamp(activeStatus.started_at)}
              </div>
              <div className="mt-1 text-[11px] text-text-secondary">
                Stopped: {formatTimestamp(activeStatus.stopped_at)}
              </div>
            </div>

            <div className="rounded-xl border border-border-panel bg-bg-primary/60 p-3">
              <div className="text-[10px] uppercase tracking-[0.18em] text-text-muted">PnL</div>
              <div className={`mt-2 text-sm font-semibold ${pnlColor(totalPnl ?? "0")}`}>
                {totalPnl ? formatPnl(totalPnl) : "No fills yet"}
              </div>
              <div className="mt-2 text-[11px] text-text-secondary">
                Realized: {activeMetrics ? formatPnl(activeMetrics.realized_pnl) : "-"}
              </div>
              <div className="mt-1 text-[11px] text-text-secondary">
                Unrealized: {activeMetrics ? formatPnl(activeMetrics.unrealized_pnl) : "-"}
              </div>
            </div>

            <div className="rounded-xl border border-border-panel bg-bg-primary/60 p-3">
              <div className="text-[10px] uppercase tracking-[0.18em] text-text-muted">Risk Posture</div>
              <div className="mt-2 text-sm font-semibold text-text-primary">
                {riskSummary?.posture.label ?? "Balanced"}
              </div>
              <p className="mt-2 text-[11px] leading-snug text-text-secondary">
                {riskSummary?.posture.reason ?? "Risk summary unavailable."}
              </p>
            </div>
          </section>

          <StrategyGuide selectedType={activeStatus.config.strategy_type} />

          <section className="grid gap-3 md:grid-cols-2">
            <div className="rounded-xl border border-border-panel bg-bg-primary/60 p-4">
              <div className="text-[10px] uppercase tracking-[0.18em] text-accent-green">Upside</div>
              <div className="mt-3 flex flex-col gap-2 text-sm text-text-secondary">
                {riskSummary?.upside.map((item) => (
                  <p key={item}>{item}</p>
                ))}
              </div>
            </div>

            <div className="rounded-xl border border-border-panel bg-bg-primary/60 p-4">
              <div className="text-[10px] uppercase tracking-[0.18em] text-accent-red">Downside</div>
              <div className="mt-3 flex flex-col gap-2 text-sm text-text-secondary">
                {riskSummary?.downside.map((item) => (
                  <p key={item}>{item}</p>
                ))}
              </div>
            </div>
          </section>

          {activeMetrics && (
            <section className="rounded-xl border border-border-panel bg-bg-primary/60 p-4">
              <div className="text-[10px] uppercase tracking-[0.18em] text-text-muted">Live Metrics</div>
              <div className="mt-3 grid gap-3 text-sm md:grid-cols-2 xl:grid-cols-4">
                <div>
                  <div className="text-text-muted">Fills</div>
                  <div className="mt-1 font-mono text-text-primary">{activeMetrics.total_fills}</div>
                </div>
                <div>
                  <div className="text-text-muted">Current Position</div>
                  <div className="mt-1 font-mono text-text-primary">{formatMetricValue(activeMetrics.current_position, 4)}</div>
                </div>
                <div>
                  <div className="text-text-muted">Volume</div>
                  <div className="mt-1 font-mono text-text-primary">{formatMetricValue(activeMetrics.volume_traded, 4)}</div>
                </div>
                <div>
                  <div className="text-text-muted">Spread Captured</div>
                  <div className="mt-1 font-mono text-text-primary">{formatMetricValue(activeMetrics.avg_spread_captured_bps)} bps</div>
                </div>
                <div>
                  <div className="text-text-muted">Uptime</div>
                  <div className="mt-1 font-mono text-text-primary">{formatUptime(activeMetrics.uptime_secs)}</div>
                </div>
                <div>
                  <div className="text-text-muted">Peak Total PnL</div>
                  <div className={`mt-1 font-mono ${pnlColor(activeMetrics.peak_total_pnl)}`}>
                    {formatPnl(activeMetrics.peak_total_pnl)}
                  </div>
                </div>
                <div>
                  <div className="text-text-muted">Max Drawdown</div>
                  <div className="mt-1 font-mono text-text-primary">{formatMetricValue(activeMetrics.max_drawdown, 4)}</div>
                </div>
                <div>
                  <div className="text-text-muted">Orders Placed</div>
                  <div className="mt-1 font-mono text-text-primary">{activeMetrics.total_orders_placed}</div>
                </div>
                <div>
                  <div className="text-text-muted">Orders Cancelled</div>
                  <div className="mt-1 font-mono text-text-primary">{activeMetrics.total_orders_cancelled}</div>
                </div>
                <div>
                  <div className="text-text-muted">Orders Rejected</div>
                  <div className="mt-1 font-mono text-text-primary">{activeMetrics.total_orders_rejected}</div>
                </div>
                <div>
                  <div className="text-text-muted">Last Fill</div>
                  <div className="mt-1 font-mono text-text-primary">{formatTimestamp(activeMetrics.last_fill_at)}</div>
                </div>
              </div>
            </section>
          )}

          <section className="rounded-xl border border-border-panel bg-bg-primary/60 p-4">
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="text-[10px] uppercase tracking-[0.18em] text-text-muted">Settings</div>
                <p className="mt-1 text-sm text-text-secondary">
                  {detail?.lifecycle.save_mode === "hot_apply"
                    ? "Save updates here. Running strategies hot-apply these parameter changes through the gateway update path."
                    : "Save updates here. Stopped strategies update their stored config and will use the new parameters on the next start."}
                </p>
              </div>
              <button
                type="button"
                onClick={handleSave}
                disabled={saveState === "saving" || detailLoading}
                className={`rounded px-3 py-2 text-xs font-semibold transition-colors ${saveState === "saved" ? "bg-accent-green/20 text-accent-green" : "bg-accent-cyan/20 text-accent-cyan hover:bg-accent-cyan/30"} ${saveState === "saving" || detailLoading ? "cursor-not-allowed opacity-60" : ""}`}
              >
                {saveState === "saving" ? "Saving..." : saveState === "saved" ? "Saved" : "Save Settings"}
              </button>
            </div>

            <div className="mt-4 grid gap-4 md:grid-cols-2">
              {strategyInfo.fields.map((field) => {
                const value = draftValues[field.name];

                return (
                  <label key={field.name} className="flex flex-col gap-1">
                    <span className="text-xs font-semibold text-text-primary">{field.label}</span>
                    {field.type === "boolean" ? (
                      <label className="flex items-center gap-2 rounded border border-border-panel bg-bg-panel/50 px-3 py-2 text-sm text-text-secondary">
                        <input
                          type="checkbox"
                          checked={Boolean(value)}
                          onChange={(event) => handleFieldChange(field.name, event.target.checked)}
                        />
                        Enable this behavior
                      </label>
                    ) : field.type === "select" ? (
                      <select
                        value={String(value ?? field.default ?? "")}
                        onChange={(event) => handleFieldChange(field.name, event.target.value)}
                        className="rounded border border-border-panel bg-bg-panel/50 px-3 py-2 text-sm text-text-primary focus:outline-none focus:border-accent-cyan"
                      >
                        {(field.options ?? []).map((option) => (
                          <option key={option.value} value={option.value}>
                            {option.label}
                          </option>
                        ))}
                      </select>
                    ) : field.type === "json" ? (
                      <textarea
                        value={String(value ?? "")}
                        onChange={(event) => handleFieldChange(field.name, event.target.value)}
                        rows={5}
                        className="rounded border border-border-panel bg-bg-panel/50 px-3 py-2 font-mono text-sm text-text-primary focus:outline-none focus:border-accent-cyan"
                      />
                    ) : (
                      <input
                        type="number"
                        value={String(value ?? "")}
                        min={field.min}
                        max={field.max}
                        step={field.step ?? "any"}
                        onChange={(event) => handleFieldChange(field.name, event.target.value)}
                        className="rounded border border-border-panel bg-bg-panel/50 px-3 py-2 text-sm text-text-primary focus:outline-none focus:border-accent-cyan"
                      />
                    )}
                    <span className="text-[11px] leading-snug text-text-muted">{field.tooltip ?? "Strategy parameter"}</span>
                  </label>
                );
              })}
            </div>

            <div className="mt-4 rounded-lg border border-border-panel bg-bg-panel/50 p-3">
              <div className="text-[10px] uppercase tracking-[0.18em] text-text-muted">Advanced Parameters</div>
              <pre className="mt-2 max-h-40 overflow-auto text-[11px] text-text-secondary">{JSON.stringify(advancedParams, null, 2)}</pre>
            </div>
          </section>

          <section className="rounded-xl border border-border-panel bg-bg-primary/60 p-4">
            <div className="text-[10px] uppercase tracking-[0.18em] text-text-muted">What Start, Stop, And Save Do</div>
            <div className="mt-3 grid gap-3 md:grid-cols-3">
              <div className="rounded-lg border border-border-panel bg-bg-panel/50 p-3">
                <div className="text-xs font-semibold text-text-primary">Start</div>
                <p className="mt-2 text-sm leading-snug text-text-secondary">
                    {detail?.lifecycle.start_behavior ?? "Start validates the current config, builds the strategy instance, registers it with the engine, marks the strategy as running, and begins collecting fresh runtime metrics."}
                </p>
              </div>
              <div className="rounded-lg border border-border-panel bg-bg-panel/50 p-3">
                <div className="text-xs font-semibold text-text-primary">Stop</div>
                <p className="mt-2 text-sm leading-snug text-text-secondary">
                    {detail?.lifecycle.stop_behavior ?? "Stop removes the strategy from the engine and prevents new strategy decisions. It records a stop timestamp, but it should be treated as halting strategy logic rather than guaranteeing a full position unwind."}
                </p>
              </div>
              <div className="rounded-lg border border-border-panel bg-bg-panel/50 p-3">
                <div className="text-xs font-semibold text-text-primary">Save</div>
                <p className="mt-2 text-sm leading-snug text-text-secondary">
                  Save patches the strategy parameters through the gateway. On a running strategy, the current backend applies those changes without requiring a stop and restart.
                </p>
              </div>
            </div>
          </section>
        </div>
      </aside>
    </>
  );

  return createPortal(drawer, document.body);
}