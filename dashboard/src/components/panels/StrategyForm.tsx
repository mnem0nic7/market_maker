"use client";

import { useState, useMemo, useCallback } from "react";
import { useDashboard, useDashboardDispatch } from "@/lib/store";
import { api } from "@/lib/api";
import { STRATEGY_TYPES, STRATEGY_PRESETS } from "@/lib/types";
import type { StrategyConfig, Exchange, StrategyTypeInfo, StrategyField } from "@/lib/types";

const inputClass =
  "bg-bg-primary border border-border-panel rounded px-2 py-1.5 text-xs text-text-primary font-mono focus:outline-hidden focus:border-accent-cyan w-full";

function generateId(prefix: string): string {
  const rand = Math.random().toString(36).slice(2, 8);
  return `${prefix}-${rand}`;
}

// ---------------------------------------------------------------------------
// Field renderer
// ---------------------------------------------------------------------------

function FieldInput({
  field,
  value,
  onChange,
}: {
  field: StrategyField;
  value: unknown;
  onChange: (name: string, value: unknown) => void;
}) {
  switch (field.type) {
    case "number":
      return (
        <input
          type="number"
          value={value as number | string}
          min={field.min}
          max={field.max}
          step={field.step}
          onChange={(e) => onChange(field.name, e.target.value)}
          className={inputClass}
        />
      );
    case "select":
      return (
        <select
          value={value as string}
          onChange={(e) => onChange(field.name, e.target.value)}
          className={inputClass}
        >
          {field.options?.map((opt) => (
            <option key={opt.value} value={opt.value}>
              {opt.label}
            </option>
          ))}
        </select>
      );
    case "boolean":
      return (
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => onChange(field.name, true)}
            className={`flex-1 py-1 rounded text-xs font-semibold transition-colors ${
              value === true
                ? "bg-accent-cyan/20 text-accent-cyan"
                : "bg-bg-primary border border-border-panel text-text-secondary hover:bg-bg-secondary"
            }`}
          >
            Yes
          </button>
          <button
            type="button"
            onClick={() => onChange(field.name, false)}
            className={`flex-1 py-1 rounded text-xs font-semibold transition-colors ${
              value === false
                ? "bg-accent-cyan/20 text-accent-cyan"
                : "bg-bg-primary border border-border-panel text-text-secondary hover:bg-bg-secondary"
            }`}
          >
            No
          </button>
        </div>
      );
    case "json":
      return (
        <input
          type="text"
          value={typeof value === "string" ? value : JSON.stringify(value)}
          onChange={(e) => {
            try {
              onChange(field.name, JSON.parse(e.target.value));
            } catch {
              onChange(field.name, e.target.value);
            }
          }}
          className={inputClass}
        />
      );
    default:
      return null;
  }
}

// ---------------------------------------------------------------------------
// Step indicator
// ---------------------------------------------------------------------------

function StepIndicator({ current, total }: { current: number; total: number }) {
  return (
    <div className="flex items-center gap-1 mb-2">
      {Array.from({ length: total }, (_, i) => {
        const step = i + 1;
        const active = step === current;
        const done = step < current;
        return (
          <div key={step} className="flex items-center gap-1">
            <span
              className={`w-5 h-5 rounded-full text-[10px] font-bold flex items-center justify-center ${
                active
                  ? "bg-accent-cyan text-bg-primary"
                  : done
                    ? "bg-accent-green/30 text-accent-green"
                    : "bg-bg-primary border border-border-panel text-text-muted"
              }`}
            >
              {done ? "\u2713" : step}
            </span>
            {step < total && (
              <span className={`w-4 h-px ${done ? "bg-accent-green/50" : "bg-border-panel"}`} />
            )}
          </div>
        );
      })}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main form component
// ---------------------------------------------------------------------------

export function StrategyFormPanel() {
  const [step, setStep] = useState(1);
  const [strategyType, setStrategyType] = useState("");
  const [exchange, setExchange] = useState<string>("");
  const [symbol, setSymbol] = useState<string>("");
  const [params, setParams] = useState<Record<string, unknown>>({});
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const { ticks } = useDashboard();
  const dispatch = useDashboardDispatch();

  // Derive available exchanges and symbols from live ticks
  const tickKeys = Object.keys(ticks);
  const availableExchanges = useMemo(() => {
    const set = new Set<string>();
    for (const key of tickKeys) {
      const tick = ticks[key];
      if (tick) set.add(tick.exchange);
    }
    return Array.from(set).sort();
  }, [tickKeys, ticks]);

  const availableSymbols = useMemo(() => {
    const set = new Set<string>();
    for (const key of tickKeys) {
      const tick = ticks[key];
      if (tick && (!exchange || tick.exchange === exchange)) {
        set.add(`${tick.symbol.base}/${tick.symbol.quote}`);
      }
    }
    return Array.from(set).sort();
  }, [tickKeys, ticks, exchange]);

  // Currently selected strategy type info
  const typeInfo: StrategyTypeInfo | undefined = useMemo(
    () => STRATEGY_TYPES.find((t) => t.type === strategyType),
    [strategyType],
  );

  const presets = useMemo(
    () => (strategyType ? STRATEGY_PRESETS[strategyType] ?? [] : []),
    [strategyType],
  );

  const handleParamChange = useCallback((name: string, value: unknown) => {
    setParams((prev) => ({ ...prev, [name]: value }));
  }, []);

  const applyPreset = useCallback(
    (presetParams: Record<string, unknown>) => {
      setParams(presetParams);
    },
    [],
  );

  const handleTypeSelect = useCallback(
    (type: string) => {
      setStrategyType(type);
      const info = STRATEGY_TYPES.find((t) => t.type === type);
      if (info) setParams({ ...info.defaultParams });
      setError(null);
      setSuccess(null);
    },
    [],
  );

  const handleSubmit = useCallback(async () => {
    if (!typeInfo || !exchange || !symbol) return;

    setSubmitting(true);
    setError(null);
    setSuccess(null);

    const [base, quote] = symbol.split("/");
    const strategyId = generateId(strategyType);

    const config: StrategyConfig = {
      strategy_id: strategyId,
      strategy_type: strategyType,
      exchange: exchange as Exchange,
      symbol: { base, quote },
      enabled: true,
      params,
    };

    try {
      await api.createStrategy(config);
      const updated = await api.strategies();
      dispatch({ type: "SET_STRATEGIES", strategies: updated });
      setSuccess(`Strategy "${strategyId}" created successfully`);
      // Reset form
      setStep(1);
      setStrategyType("");
      setExchange("");
      setSymbol("");
      setParams({});
    } catch (e) {
      setError(String(e));
    } finally {
      setSubmitting(false);
    }
  }, [typeInfo, exchange, symbol, strategyType, params, dispatch]);

  const canAdvanceToStep2 = strategyType !== "";
  const canAdvanceToStep3 = exchange !== "" && symbol !== "";

  return (
    <div className="flex flex-col gap-3 h-full overflow-y-auto text-xs">
      <StepIndicator current={step} total={3} />

      {/* ----------------------------------------------------------------- */}
      {/* Step 1: Strategy type selection                                    */}
      {/* ----------------------------------------------------------------- */}
      {step === 1 && (
        <div className="flex flex-col gap-2">
          <label className="text-text-secondary font-semibold">Strategy Type</label>
          <select
            value={strategyType}
            onChange={(e) => handleTypeSelect(e.target.value)}
            className={inputClass}
          >
            <option value="">Select a strategy type...</option>
            {STRATEGY_TYPES.map((t) => (
              <option key={t.type} value={t.type}>
                {t.label}
              </option>
            ))}
          </select>
          {typeInfo && (
            <p className="text-text-muted text-[10px] leading-snug">{typeInfo.description}</p>
          )}
          <button
            type="button"
            disabled={!canAdvanceToStep2}
            onClick={() => setStep(2)}
            className={`mt-1 py-1.5 rounded font-semibold transition-colors ${
              canAdvanceToStep2
                ? "bg-accent-cyan/20 text-accent-cyan hover:bg-accent-cyan/30"
                : "bg-bg-primary border border-border-panel text-text-muted cursor-not-allowed"
            }`}
          >
            Next
          </button>
        </div>
      )}

      {/* ----------------------------------------------------------------- */}
      {/* Step 2: Exchange + Symbol                                         */}
      {/* ----------------------------------------------------------------- */}
      {step === 2 && (
        <div className="flex flex-col gap-2">
          <label className="text-text-secondary font-semibold">Exchange</label>
          {availableExchanges.length > 0 ? (
            <select
              value={exchange}
              onChange={(e) => {
                setExchange(e.target.value);
                setSymbol("");
              }}
              className={inputClass}
            >
              <option value="">Select exchange...</option>
              {availableExchanges.map((ex) => (
                <option key={ex} value={ex}>
                  {ex}
                </option>
              ))}
            </select>
          ) : (
            <input
              type="text"
              value={exchange}
              onChange={(e) => setExchange(e.target.value)}
              placeholder="e.g. Binance"
              className={inputClass}
            />
          )}

          <label className="text-text-secondary font-semibold mt-1">Symbol</label>
          {availableSymbols.length > 0 ? (
            <select
              value={symbol}
              onChange={(e) => setSymbol(e.target.value)}
              className={inputClass}
            >
              <option value="">Select symbol...</option>
              {availableSymbols.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          ) : (
            <input
              type="text"
              value={symbol}
              onChange={(e) => setSymbol(e.target.value)}
              placeholder="BTC/USDT"
              className={inputClass}
            />
          )}

          <div className="flex gap-2 mt-1">
            <button
              type="button"
              onClick={() => setStep(1)}
              className="flex-1 py-1.5 rounded font-semibold bg-bg-primary border border-border-panel text-text-secondary hover:bg-bg-secondary transition-colors"
            >
              Back
            </button>
            <button
              type="button"
              disabled={!canAdvanceToStep3}
              onClick={() => setStep(3)}
              className={`flex-1 py-1.5 rounded font-semibold transition-colors ${
                canAdvanceToStep3
                  ? "bg-accent-cyan/20 text-accent-cyan hover:bg-accent-cyan/30"
                  : "bg-bg-primary border border-border-panel text-text-muted cursor-not-allowed"
              }`}
            >
              Next
            </button>
          </div>
        </div>
      )}

      {/* ----------------------------------------------------------------- */}
      {/* Step 3: Parameters + Presets                                      */}
      {/* ----------------------------------------------------------------- */}
      {step === 3 && typeInfo && (
        <div className="flex flex-col gap-2">
          {/* Presets */}
          {presets.length > 0 && (
            <div className="flex flex-col gap-1">
              <label className="text-text-secondary font-semibold">Presets</label>
              <div className="flex gap-1">
                {presets.map((p) => (
                  <button
                    key={p.label}
                    type="button"
                    onClick={() => applyPreset(p.params)}
                    className="flex-1 py-1 rounded text-[10px] font-semibold bg-bg-primary border border-border-panel text-text-secondary hover:bg-accent-cyan/10 hover:text-accent-cyan hover:border-accent-cyan/30 transition-colors"
                  >
                    {p.label}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Parameter fields */}
          <div className="flex flex-col gap-2 mt-1">
            {typeInfo.fields.map((field) => (
              <div key={field.name} className="flex flex-col gap-0.5">
                <div className="flex items-center gap-1">
                  <label className="text-text-secondary">{field.label}</label>
                  {field.tooltip && (
                    <span
                      className="text-text-muted text-[10px] cursor-help"
                      title={field.tooltip}
                    >
                      [?]
                    </span>
                  )}
                </div>
                <FieldInput
                  field={field}
                  value={params[field.name] ?? field.default}
                  onChange={handleParamChange}
                />
              </div>
            ))}
          </div>

          {/* Summary */}
          <div className="mt-2 border-t border-border-panel pt-2">
            <div className="text-[10px] text-text-muted mb-1">Summary</div>
            <div className="text-[10px] font-mono text-text-secondary space-y-0.5">
              <div>
                <span className="text-text-muted">Type: </span>
                {typeInfo.label}
              </div>
              <div>
                <span className="text-text-muted">Exchange: </span>
                {exchange}
              </div>
              <div>
                <span className="text-text-muted">Symbol: </span>
                {symbol}
              </div>
            </div>
          </div>

          {/* Actions */}
          <div className="flex gap-2 mt-1">
            <button
              type="button"
              onClick={() => setStep(2)}
              className="flex-1 py-1.5 rounded font-semibold bg-bg-primary border border-border-panel text-text-secondary hover:bg-bg-secondary transition-colors"
            >
              Back
            </button>
            <button
              type="button"
              disabled={submitting}
              onClick={handleSubmit}
              className={`flex-1 py-1.5 rounded font-semibold transition-colors ${
                submitting
                  ? "bg-accent-cyan/10 text-text-muted cursor-not-allowed"
                  : "bg-accent-green/20 text-accent-green hover:bg-accent-green/30"
              }`}
            >
              {submitting ? "Creating..." : "Create Strategy"}
            </button>
          </div>
        </div>
      )}

      {/* ----------------------------------------------------------------- */}
      {/* Status messages                                                    */}
      {/* ----------------------------------------------------------------- */}
      {error && (
        <div className="mt-1 px-2 py-1.5 rounded bg-accent-red/10 border border-accent-red/30 text-accent-red text-[10px]">
          {error}
        </div>
      )}
      {success && (
        <div className="mt-1 px-2 py-1.5 rounded bg-accent-green/10 border border-accent-green/30 text-accent-green text-[10px]">
          {success}
        </div>
      )}
    </div>
  );
}
