"use client";

import { getStrategyTypeInfo, STRATEGY_TYPES, type StrategyField } from "@/lib/types";

interface StrategyGuideProps {
  selectedType: string;
  onSelectType?: (strategyType: string) => void;
  showSelector?: boolean;
  compact?: boolean;
}

function keyFields(fields: StrategyField[]): StrategyField[] {
  return fields.slice(0, 4);
}

export function StrategyGuide({
  selectedType,
  onSelectType,
  showSelector = false,
  compact = false,
}: StrategyGuideProps) {
  const strategyInfo = getStrategyTypeInfo(selectedType) ?? STRATEGY_TYPES[0];

  if (!strategyInfo) {
    return null;
  }

  const fields = keyFields(strategyInfo.fields);
  const containerClass = compact
    ? "mt-2 rounded-lg border border-border-panel bg-bg-primary/70 p-3"
    : "rounded-lg border border-border-panel bg-bg-primary/70 p-3";
  const detailGridClass = compact
    ? "grid gap-2 text-[10px]"
    : "grid gap-2 text-[11px]";

  return (
    <div className={containerClass}>
      {showSelector && (
        <div className="mb-3 flex flex-wrap gap-1.5">
          {STRATEGY_TYPES.map((strategy) => {
            const active = strategy.type === strategyInfo.type;
            return (
              <button
                key={strategy.type}
                type="button"
                onClick={() => onSelectType?.(strategy.type)}
                className={`rounded-full border px-2 py-1 text-[10px] font-semibold transition-colors ${
                  active
                    ? "border-accent-cyan/50 bg-accent-cyan/15 text-accent-cyan"
                    : strategy.creationMode === "template_only"
                      ? "border-accent-amber/30 bg-accent-amber/10 text-accent-amber hover:border-accent-amber/50"
                      : "border-border-panel bg-bg-panel/60 text-text-secondary hover:border-accent-cyan/30 hover:text-text-primary"
                }`}
              >
                {strategy.label}
              </button>
            );
          })}
        </div>
      )}

      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-xs font-semibold text-text-primary">{strategyInfo.label}</div>
          <div className="mt-1 text-[10px] uppercase tracking-[0.18em] text-accent-cyan">
            {strategyInfo.category}
          </div>
          {strategyInfo.creationMode === "template_only" && (
            <div className="mt-2 inline-flex rounded-full border border-accent-amber/30 bg-accent-amber/10 px-2 py-1 text-[9px] font-semibold uppercase tracking-[0.18em] text-accent-amber">
              Template Only
            </div>
          )}
        </div>
        <div className="max-w-44 text-right text-[10px] text-text-secondary">
          {strategyInfo.paperFit}
        </div>
      </div>

      <p className="mt-2 text-xs leading-snug text-text-secondary">
        {strategyInfo.description}
      </p>
      {strategyInfo.creationHint && (
        <p className="mt-2 rounded border border-border-panel bg-bg-panel/60 px-2 py-1.5 text-[10px] leading-snug text-text-secondary">
          {strategyInfo.creationHint}
        </p>
      )}

      <div className={`mt-3 ${detailGridClass}`}>
        <div>
          <div className="mb-1 text-[10px] uppercase tracking-[0.16em] text-text-muted">
            How It Trades
          </div>
          <p className="leading-snug text-text-secondary">{strategyInfo.howItWorks}</p>
        </div>

        <div>
          <div className="mb-1 text-[10px] uppercase tracking-[0.16em] text-text-muted">
            Best For
          </div>
          <p className="leading-snug text-text-secondary">{strategyInfo.bestFor}</p>
        </div>

        <div>
          <div className="mb-1 text-[10px] uppercase tracking-[0.16em] text-text-muted">
            Watch For
          </div>
          <p className="leading-snug text-text-secondary">{strategyInfo.watchFor}</p>
        </div>
      </div>

      <div className="mt-3 border-t border-border-panel pt-3">
        <div className="mb-2 text-[10px] uppercase tracking-[0.16em] text-text-muted">
          Key Knobs
        </div>
        <div className="grid gap-2 md:grid-cols-2">
          {fields.map((field) => (
            <div key={field.name} className="rounded border border-border-panel bg-bg-panel/60 px-2 py-1.5">
              <div className="text-[10px] font-semibold text-text-primary">{field.label}</div>
              <div className="mt-0.5 text-[10px] leading-snug text-text-secondary">
                {field.tooltip ?? "Strategy parameter"}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
