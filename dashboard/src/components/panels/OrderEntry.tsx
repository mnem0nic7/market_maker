"use client";

import { useState, useCallback, useEffect, useMemo } from "react";
import type { Exchange, Side, OrderType } from "@/lib/types";
import { api } from "@/lib/api";
import { useDashboard, useDashboardDispatch } from "@/lib/store";

export function OrderEntryPanel() {
  const { ticks, orders } = useDashboard();
  const dispatch = useDashboardDispatch();
  const [exchange, setExchange] = useState<Exchange>("Alpaca");
  const [symbol, setSymbol] = useState("");
  const [side, setSide] = useState<Side>("Buy");
  const [orderType, setOrderType] = useState<OrderType>("Limit");
  const [price, setPrice] = useState("");
  const [quantity, setQuantity] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [feedback, setFeedback] = useState<{
    type: "success" | "error";
    message: string;
  } | null>(null);

  const tickKeys = Object.keys(ticks);
  const liveExchanges = useMemo(() => {
    const values = new Set<Exchange>();
    for (const key of tickKeys) {
      const tick = ticks[key];
      if (tick) values.add(tick.exchange);
    }
    return Array.from(values).sort();
  }, [tickKeys, ticks]);

  const availableExchanges = useMemo<Exchange[]>(
    () => (liveExchanges.length > 0 ? liveExchanges : ["Alpaca"]),
    [liveExchanges],
  );

  const preferredExchange = useMemo<Exchange>(
    () => (
      availableExchanges.includes("Alpaca")
        ? "Alpaca"
        : availableExchanges[0]
    ),
    [availableExchanges],
  );

  const liveSymbols = useMemo(() => {
    const values = new Set<string>();
    for (const key of tickKeys) {
      const tick = ticks[key];
      if (tick && tick.exchange === exchange) {
        values.add(`${tick.symbol.base}/${tick.symbol.quote}`);
      }
    }
    return Array.from(values).sort();
  }, [exchange, tickKeys, ticks]);

  const availableSymbols = useMemo(
    () => (liveSymbols.length > 0 ? liveSymbols : ["AAPL/USD"]),
    [liveSymbols],
  );

  useEffect(() => {
    if (!availableExchanges.includes(exchange)) {
      setExchange(preferredExchange);
    }
  }, [availableExchanges, exchange, preferredExchange]);

  useEffect(() => {
    if (!symbol || (liveSymbols.length > 0 && !availableSymbols.includes(symbol))) {
      setSymbol(availableSymbols[0] ?? "AAPL/USD");
    }
  }, [availableSymbols, liveSymbols.length, symbol]);

  const handleSubmit = useCallback(
    async (e: React.FormEvent) => {
      e.preventDefault();
      setSubmitting(true);
      setFeedback(null);

      try {
        const order = await api.placeOrder({
          exchange: exchange.toLowerCase(),
          symbol,
          side,
          order_type: orderType,
          price: orderType === "Limit" ? price : null,
          quantity,
        });

        const nextOrders = orders.some((existingOrder) => existingOrder.client_order_id === order.client_order_id)
          ? orders.map((existingOrder) =>
            existingOrder.client_order_id === order.client_order_id ? order : existingOrder,
          )
          : [...orders, order];
        dispatch({ type: "SET_ORDERS", orders: nextOrders });

        setFeedback({
          type: "success",
          message: `Order placed: ${order.client_order_id}`,
        });
        // Reset quantity after successful submission
        setQuantity("");
        setPrice("");
      } catch (err) {
        setFeedback({
          type: "error",
          message: err instanceof Error ? err.message : "Failed to place order",
        });
      } finally {
        setSubmitting(false);
        setTimeout(() => setFeedback(null), 4000);
      }
    },
    [dispatch, exchange, orders, symbol, side, orderType, price, quantity]
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
          {availableExchanges.map((ex) => (
            <option key={ex} value={ex}>
              {ex}
            </option>
          ))}
        </select>
      </div>

      {/* Symbol */}
      <div className="flex flex-col gap-1">
        <label className="text-text-secondary">Symbol</label>
        {liveSymbols.length > 0 ? (
          <select
            value={symbol}
            onChange={(e) => setSymbol(e.target.value)}
            className="bg-bg-primary border border-border-panel rounded px-2 py-1.5 text-text-primary font-mono focus:outline-none focus:border-accent-cyan"
          >
            {availableSymbols.map((availableSymbol) => (
              <option key={availableSymbol} value={availableSymbol}>
                {availableSymbol}
              </option>
            ))}
          </select>
        ) : (
          <input
            type="text"
            value={symbol}
            onChange={(e) => setSymbol(e.target.value)}
            placeholder="AAPL/USD"
            className="bg-bg-primary border border-border-panel rounded px-2 py-1.5 text-text-primary font-mono placeholder:text-text-muted focus:outline-none focus:border-accent-cyan"
          />
        )}
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
        disabled={submitting}
        className={`py-2 rounded font-semibold transition-colors ${
          submitting
            ? "bg-bg-hover text-text-muted cursor-not-allowed"
            : side === "Buy"
              ? "bg-accent-green text-bg-primary hover:bg-accent-green/80"
              : "bg-accent-red text-bg-primary hover:bg-accent-red/80"
        }`}
      >
        {submitting
          ? "Submitting..."
          : `${side === "Buy" ? "BUY" : "SELL"} ${symbol || "..."}`}
      </button>

      {feedback && (
        <p
          className={`text-center ${
            feedback.type === "success"
              ? "text-accent-green"
              : "text-accent-red"
          }`}
        >
          {feedback.message}
        </p>
      )}
    </form>
  );
}
