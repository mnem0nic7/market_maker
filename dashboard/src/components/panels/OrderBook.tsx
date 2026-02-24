"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import { useDashboard } from "@/lib/store";
import { api } from "@/lib/api";
import type { OrderBook, PriceLevel } from "@/lib/types";

export function OrderBookPanel() {
  const { ticks } = useDashboard();
  const [selectedKey, setSelectedKey] = useState<string>("");
  const [book, setBook] = useState<OrderBook | null>(null);
  const [error, setError] = useState<string | null>(null);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const tickKeys = Object.keys(ticks);

  // Auto-select first key if nothing selected
  useEffect(() => {
    if (!selectedKey && tickKeys.length > 0) {
      setSelectedKey(tickKeys[0]);
    }
  }, [selectedKey, tickKeys]);

  const fetchBook = useCallback(async () => {
    if (!selectedKey) return;
    const tick = ticks[selectedKey];
    if (!tick) return;
    try {
      const data = await api.orderbook(
        tick.exchange,
        `${tick.symbol.base}/${tick.symbol.quote}`
      );
      setBook(data);
      setError(null);
    } catch (e) {
      setError(String(e));
    }
  }, [selectedKey, ticks]);

  useEffect(() => {
    fetchBook();
    intervalRef.current = setInterval(fetchBook, 2000);
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [fetchBook]);

  const maxQty = (levels: PriceLevel[]) =>
    Math.max(...levels.map((l) => parseFloat(l.quantity)), 0.0001);

  const renderLevel = (
    level: PriceLevel,
    side: "bid" | "ask",
    max: number
  ) => {
    const qty = parseFloat(level.quantity);
    const pct = (qty / max) * 100;
    const barColor = side === "bid" ? "bg-accent-green/20" : "bg-accent-red/20";
    const textColor = side === "bid" ? "text-accent-green" : "text-accent-red";

    return (
      <div key={`${side}-${level.price}`} className="relative flex items-center text-xs py-0.5 px-2">
        <div
          className={`absolute inset-y-0 ${side === "bid" ? "right-0" : "left-0"} ${barColor}`}
          style={{ width: `${pct}%` }}
        />
        <span className={`relative z-10 font-mono flex-1 ${side === "bid" ? "text-left" : "text-right"} ${textColor}`}>
          {level.price}
        </span>
        <span className="relative z-10 font-mono flex-1 text-right text-text-secondary">
          {qty.toFixed(4)}
        </span>
      </div>
    );
  };

  return (
    <div className="flex flex-col gap-2 h-full">
      {/* Symbol selector */}
      <select
        value={selectedKey}
        onChange={(e) => setSelectedKey(e.target.value)}
        className="bg-bg-primary border border-border-panel rounded px-2 py-1 text-xs text-text-primary font-mono focus:outline-none focus:border-accent-cyan"
      >
        {tickKeys.length === 0 && <option value="">No symbols available</option>}
        {tickKeys.map((key) => (
          <option key={key} value={key}>
            {key}
          </option>
        ))}
      </select>

      {error && <p className="text-accent-red text-xs">{error}</p>}

      {!book ? (
        <p className="text-text-muted text-sm">
          {selectedKey ? "Loading order book..." : "Select a symbol"}
        </p>
      ) : (
        <div className="flex flex-col flex-1 overflow-hidden">
          {/* Asks (reversed: lowest at bottom) */}
          <div className="flex-1 overflow-auto flex flex-col justify-end">
            <div className="text-[10px] text-text-muted px-2 pb-0.5">ASKS</div>
            {[...book.asks]
              .slice(0, 15)
              .reverse()
              .map((level) => renderLevel(level, "ask", maxQty(book.asks)))}
          </div>

          {/* Spread */}
          <div className="flex items-center justify-center py-1.5 border-y border-border-panel">
            <span className="text-xs font-mono text-accent-cyan">
              Spread:{" "}
              {book.asks.length > 0 && book.bids.length > 0
                ? (parseFloat(book.asks[0].price) - parseFloat(book.bids[0].price)).toFixed(4)
                : "N/A"}
            </span>
          </div>

          {/* Bids */}
          <div className="flex-1 overflow-auto">
            <div className="text-[10px] text-text-muted px-2 pt-0.5">BIDS</div>
            {book.bids
              .slice(0, 15)
              .map((level) => renderLevel(level, "bid", maxQty(book.bids)))}
          </div>
        </div>
      )}
    </div>
  );
}
