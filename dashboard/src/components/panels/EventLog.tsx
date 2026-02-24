"use client";

import { useState, useRef, useCallback, useEffect } from "react";
import { useDashboard } from "@/lib/store";
import type { ServerMessage } from "@/lib/types";

type FilterType = "ALL" | "MarketDataTick" | "OrderUpdate" | "PositionUpdate" | "RiskUpdate" | "Error";

const FILTERS: FilterType[] = ["ALL", "MarketDataTick", "OrderUpdate", "PositionUpdate", "RiskUpdate", "Error"];

function typeColor(type: string): string {
  switch (type) {
    case "MarketDataTick":
      return "text-accent-cyan";
    case "PositionUpdate":
      return "text-accent-amber";
    case "OrderUpdate":
      return "text-accent-green";
    case "RiskUpdate":
      return "text-accent-red";
    case "Error":
      return "text-accent-red";
    default:
      return "text-text-secondary";
  }
}

function formatEvent(msg: ServerMessage): string {
  switch (msg.type) {
    case "MarketDataTick":
      return `${msg.exchange} ${msg.symbol.base}/${msg.symbol.quote} bid=${msg.best_bid} ask=${msg.best_ask} spread=${msg.spread_bps}bps`;
    case "PositionUpdate":
      return `${msg.exchange} ${msg.symbol.base}/${msg.symbol.quote} qty=${msg.quantity} uPnL=${msg.unrealized_pnl}`;
    case "OrderUpdate":
      return `${msg.exchange} ${msg.symbol.base}/${msg.symbol.quote} ${msg.status} filled=${msg.filled_quantity} id=${msg.client_order_id.substring(0, 8)}`;
    case "RiskUpdate":
      return `net=${msg.net_exposure} gross=${msg.gross_exposure} pnl=${msg.daily_pnl} kill=${msg.kill_switch_active}`;
    case "Error":
      return msg.message;
    case "Subscribed":
      return `Subscribed to ${msg.channel}`;
    case "Unsubscribed":
      return `Unsubscribed from ${msg.channel}`;
    default:
      return JSON.stringify(msg);
  }
}

export function EventLogPanel() {
  const { events } = useDashboard();
  const [filter, setFilter] = useState<FilterType>("ALL");
  const [hovering, setHovering] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const isAtTopRef = useRef(true);

  const filtered = filter === "ALL" ? events : events.filter((e) => e.type === filter);

  const handleScroll = useCallback(() => {
    const el = scrollRef.current;
    if (el) {
      isAtTopRef.current = el.scrollTop <= 5;
    }
  }, []);

  // Auto-scroll to top when new events arrive (if at top and not hovering)
  useEffect(() => {
    if (isAtTopRef.current && !hovering && scrollRef.current) {
      scrollRef.current.scrollTop = 0;
    }
  }, [events, hovering]);

  return (
    <div className="flex flex-col gap-2 h-full">
      {/* Filter bar */}
      <div className="flex flex-wrap gap-1 shrink-0">
        {FILTERS.map((f) => (
          <button
            key={f}
            type="button"
            onClick={() => setFilter(f)}
            className={`text-[10px] px-2 py-0.5 rounded transition-colors ${
              filter === f
                ? "bg-accent-cyan text-bg-primary"
                : "bg-bg-primary text-text-muted hover:text-text-secondary border border-border-panel"
            }`}
          >
            {f === "ALL" ? "ALL" : f.replace("Update", "").replace("Msg", "")}
          </button>
        ))}
      </div>

      {/* Event list */}
      <div
        ref={scrollRef}
        onScroll={handleScroll}
        onMouseEnter={() => setHovering(true)}
        onMouseLeave={() => setHovering(false)}
        className="flex-1 overflow-auto"
      >
        {filtered.length === 0 ? (
          <p className="text-text-muted text-sm">No events</p>
        ) : (
          <div className="flex flex-col gap-0.5">
            {filtered.map((event, idx) => (
              <div
                key={idx}
                className="text-[10px] font-mono py-0.5 px-1 hover:bg-bg-hover rounded transition-colors"
              >
                <span className={`font-semibold ${typeColor(event.type)}`}>
                  [{event.type}]
                </span>{" "}
                <span className="text-text-secondary">{formatEvent(event)}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
