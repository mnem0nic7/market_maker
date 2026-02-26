"use client";

import { useDashboard } from "@/lib/store";

function pnlColor(value: number): string {
  if (value > 0) return "text-accent-green";
  if (value < 0) return "text-accent-red";
  return "text-text-muted";
}

function formatPnl(value: number): string {
  const prefix = value > 0 ? "+" : "";
  return `${prefix}${value.toFixed(4)}`;
}

function sideLabel(qty: number): { text: string; color: string } {
  if (qty > 0) return { text: "LONG", color: "text-accent-green" };
  if (qty < 0) return { text: "SHORT", color: "text-accent-red" };
  return { text: "FLAT", color: "text-text-muted" };
}

export function PositionsPanel() {
  const { positions, loading } = useDashboard();

  if (loading) {
    return (
      <div className="flex flex-col gap-3 animate-pulse">
        <div className="h-3 w-48 bg-bg-secondary rounded" />
        <div className="h-3 w-64 bg-bg-secondary rounded" />
        <div className="h-3 w-40 bg-bg-secondary rounded" />
      </div>
    );
  }

  const totalUnrealized = positions.reduce((sum, p) => sum + parseFloat(p.unrealized_pnl), 0);
  const totalRealized = positions.reduce((sum, p) => sum + parseFloat(p.realized_pnl), 0);

  return (
    <div className="flex flex-col gap-3">
      {/* Summary bar */}
      <div className="flex items-center gap-4 text-xs px-1">
        <span className="text-text-secondary">
          Positions: <span className="text-text-primary font-mono">{positions.length}</span>
        </span>
        <span className="text-text-secondary">
          Unrealized:{" "}
          <span className={`font-mono ${pnlColor(totalUnrealized)}`}>
            {formatPnl(totalUnrealized)}
          </span>
        </span>
        <span className="text-text-secondary">
          Realized:{" "}
          <span className={`font-mono ${pnlColor(totalRealized)}`}>
            {formatPnl(totalRealized)}
          </span>
        </span>
      </div>

      {positions.length === 0 ? (
        <p className="text-text-muted text-sm">No open positions</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-text-secondary border-b border-border-panel">
                <th className="text-left py-1.5 px-2">Symbol</th>
                <th className="text-left py-1.5 px-2">Exchange</th>
                <th className="text-left py-1.5 px-2">Side</th>
                <th className="text-right py-1.5 px-2">Qty</th>
                <th className="text-right py-1.5 px-2">Entry Price</th>
                <th className="text-right py-1.5 px-2">Unrealized</th>
                <th className="text-right py-1.5 px-2">Realized</th>
              </tr>
            </thead>
            <tbody>
              {positions.map((pos) => {
                const qty = parseFloat(pos.quantity);
                const unrealized = parseFloat(pos.unrealized_pnl);
                const realized = parseFloat(pos.realized_pnl);
                const side = sideLabel(qty);
                return (
                  <tr
                    key={`${pos.exchange}:${pos.symbol.base}/${pos.symbol.quote}`}
                    className="border-b border-border-panel/50 hover:bg-bg-hover transition-colors"
                  >
                    <td className="py-1.5 px-2 font-mono text-text-primary">
                      {pos.symbol.base}/{pos.symbol.quote}
                    </td>
                    <td className="py-1.5 px-2 text-text-secondary">{pos.exchange}</td>
                    <td className={`py-1.5 px-2 font-mono font-semibold ${side.color}`}>
                      {side.text}
                    </td>
                    <td className="py-1.5 px-2 font-mono text-right text-text-primary">
                      {Math.abs(qty).toFixed(4)}
                    </td>
                    <td className="py-1.5 px-2 font-mono text-right text-text-primary">
                      {pos.average_entry_price}
                    </td>
                    <td className={`py-1.5 px-2 font-mono text-right ${pnlColor(unrealized)}`}>
                      {formatPnl(unrealized)}
                    </td>
                    <td className={`py-1.5 px-2 font-mono text-right ${pnlColor(realized)}`}>
                      {formatPnl(realized)}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
