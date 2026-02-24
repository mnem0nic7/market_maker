"use client";

import { useState, useCallback } from "react";
import type { Exchange, Side, OrderType } from "@/lib/types";

const EXCHANGES: Exchange[] = ["Binance", "Coinbase", "Kraken", "Alpaca", "Simulated"];

export function OrderEntryPanel() {
  const [exchange, setExchange] = useState<Exchange>("Binance");
  const [symbol, setSymbol] = useState("");
  const [side, setSide] = useState<Side>("Buy");
  const [orderType, setOrderType] = useState<OrderType>("Limit");
  const [price, setPrice] = useState("");
  const [quantity, setQuantity] = useState("");
  const [submitted, setSubmitted] = useState(false);

  const handleSubmit = useCallback(
    (e: React.FormEvent) => {
      e.preventDefault();
      const payload = {
        exchange,
        symbol,
        side,
        order_type: orderType,
        price: orderType === "Limit" ? price : null,
        quantity,
      };
      console.log("Order submitted:", payload);
      setSubmitted(true);
      setTimeout(() => setSubmitted(false), 2000);
    },
    [exchange, symbol, side, orderType, price, quantity]
  );

  return (
    <form onSubmit={handleSubmit} className="flex flex-col gap-3 text-xs">
      {/* Exchange */}
      <div className="flex flex-col gap-1">
        <label className="text-text-secondary">Exchange</label>
        <select
          value={exchange}
          onChange={(e) => setExchange(e.target.value as Exchange)}
          className="bg-bg-primary border border-border-panel rounded px-2 py-1.5 text-text-primary font-mono focus:outline-none focus:border-accent-cyan"
        >
          {EXCHANGES.map((ex) => (
            <option key={ex} value={ex}>
              {ex}
            </option>
          ))}
        </select>
      </div>

      {/* Symbol */}
      <div className="flex flex-col gap-1">
        <label className="text-text-secondary">Symbol</label>
        <input
          type="text"
          value={symbol}
          onChange={(e) => setSymbol(e.target.value)}
          placeholder="BTC/USDT"
          className="bg-bg-primary border border-border-panel rounded px-2 py-1.5 text-text-primary font-mono placeholder:text-text-muted focus:outline-none focus:border-accent-cyan"
        />
      </div>

      {/* Side toggle */}
      <div className="flex flex-col gap-1">
        <label className="text-text-secondary">Side</label>
        <div className="flex gap-1">
          <button
            type="button"
            onClick={() => setSide("Buy")}
            className={`flex-1 py-1.5 rounded font-semibold transition-colors ${
              side === "Buy"
                ? "bg-accent-green text-bg-primary"
                : "bg-bg-primary border border-border-panel text-text-secondary hover:bg-bg-hover"
            }`}
          >
            BUY
          </button>
          <button
            type="button"
            onClick={() => setSide("Sell")}
            className={`flex-1 py-1.5 rounded font-semibold transition-colors ${
              side === "Sell"
                ? "bg-accent-red text-bg-primary"
                : "bg-bg-primary border border-border-panel text-text-secondary hover:bg-bg-hover"
            }`}
          >
            SELL
          </button>
        </div>
      </div>

      {/* Order Type toggle */}
      <div className="flex flex-col gap-1">
        <label className="text-text-secondary">Order Type</label>
        <div className="flex gap-1">
          <button
            type="button"
            onClick={() => setOrderType("Limit")}
            className={`flex-1 py-1.5 rounded font-semibold transition-colors ${
              orderType === "Limit"
                ? "bg-accent-cyan text-bg-primary"
                : "bg-bg-primary border border-border-panel text-text-secondary hover:bg-bg-hover"
            }`}
          >
            LIMIT
          </button>
          <button
            type="button"
            onClick={() => setOrderType("Market")}
            className={`flex-1 py-1.5 rounded font-semibold transition-colors ${
              orderType === "Market"
                ? "bg-accent-cyan text-bg-primary"
                : "bg-bg-primary border border-border-panel text-text-secondary hover:bg-bg-hover"
            }`}
          >
            MARKET
          </button>
        </div>
      </div>

      {/* Price (only for Limit) */}
      {orderType === "Limit" && (
        <div className="flex flex-col gap-1">
          <label className="text-text-secondary">Price</label>
          <input
            type="text"
            value={price}
            onChange={(e) => setPrice(e.target.value)}
            placeholder="0.00"
            className="bg-bg-primary border border-border-panel rounded px-2 py-1.5 text-text-primary font-mono placeholder:text-text-muted focus:outline-none focus:border-accent-cyan"
          />
        </div>
      )}

      {/* Quantity */}
      <div className="flex flex-col gap-1">
        <label className="text-text-secondary">Quantity</label>
        <input
          type="text"
          value={quantity}
          onChange={(e) => setQuantity(e.target.value)}
          placeholder="0.00"
          className="bg-bg-primary border border-border-panel rounded px-2 py-1.5 text-text-primary font-mono placeholder:text-text-muted focus:outline-none focus:border-accent-cyan"
        />
      </div>

      {/* Submit */}
      <button
        type="submit"
        className={`py-2 rounded font-semibold transition-colors ${
          side === "Buy"
            ? "bg-accent-green text-bg-primary hover:bg-accent-green/80"
            : "bg-accent-red text-bg-primary hover:bg-accent-red/80"
        }`}
      >
        {side === "Buy" ? "BUY" : "SELL"} {symbol || "..."}
      </button>

      {submitted && (
        <p className="text-accent-green text-center">
          Order logged to console (gateway POST not available)
        </p>
      )}
    </form>
  );
}
