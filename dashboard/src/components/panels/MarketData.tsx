"use client";

import { useDashboard } from "@/lib/store";

export function MarketDataPanel() {
  const { ticks } = useDashboard();
  const entries = Object.entries(ticks);

  if (entries.length === 0) {
    return <p className="text-text-muted text-sm">Waiting for market data ticks...</p>;
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="text-text-secondary border-b border-border-panel">
            <th className="text-left py-1.5 px-2">Symbol</th>
            <th className="text-left py-1.5 px-2">Exchange</th>
            <th className="text-right py-1.5 px-2">Bid</th>
            <th className="text-right py-1.5 px-2">Ask</th>
            <th className="text-right py-1.5 px-2">Mid</th>
            <th className="text-right py-1.5 px-2">Spread</th>
          </tr>
        </thead>
        <tbody>
          {entries.map(([key, tick]) => {
            const spreadBps = parseFloat(tick.spread_bps);
            const spreadColor =
              spreadBps < 5
                ? "text-accent-green"
                : spreadBps < 20
                  ? "text-accent-amber"
                  : "text-accent-red";

            return (
              <tr
                key={key}
                className="border-b border-border-panel/50 hover:bg-bg-hover transition-colors"
              >
                <td className="py-1.5 px-2 font-mono text-text-primary">
                  {tick.symbol.base}/{tick.symbol.quote}
                </td>
                <td className="py-1.5 px-2 text-text-secondary">{tick.exchange}</td>
                <td className="py-1.5 px-2 font-mono text-right text-accent-green">
                  {tick.best_bid}
                </td>
                <td className="py-1.5 px-2 font-mono text-right text-accent-red">
                  {tick.best_ask}
                </td>
                <td className="py-1.5 px-2 font-mono text-right text-text-primary">
                  {tick.mid_price}
                </td>
                <td className={`py-1.5 px-2 font-mono text-right ${spreadColor}`}>
                  {spreadBps.toFixed(1)} bps
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
