"use client";

import { DashboardGrid } from "@/components/layout/DashboardGrid";
import { Panel } from "@/components/ui/Panel";
import { useDashboard } from "@/lib/store";

export default function DashboardPage() {
  const { connected, loading } = useDashboard();

  return (
    <main className="min-h-screen">
      <div className="flex items-center justify-between px-4 py-2 border-b border-border-panel bg-bg-panel/50">
        <h1 className="text-sm font-bold text-accent-cyan font-mono tracking-wider">MARKET MAKER</h1>
        <div className="flex items-center gap-3 text-xs">
          <span className={`flex items-center gap-1.5 ${connected ? "text-accent-green" : "text-accent-red"}`}>
            <span className={`w-2 h-2 rounded-full ${connected ? "bg-accent-green" : "bg-accent-red"} ${connected ? "animate-pulse" : ""}`} />
            {connected ? "CONNECTED" : "DISCONNECTED"}
          </span>
          {loading && <span className="text-accent-amber">Loading...</span>}
        </div>
      </div>

      <DashboardGrid>
        <div key="market-data"><Panel title="Market Data"><p className="text-text-muted text-sm">Waiting for ticks...</p></Panel></div>
        <div key="positions"><Panel title="Positions & PnL"><p className="text-text-muted text-sm">No positions</p></Panel></div>
        <div key="order-book"><Panel title="Order Book"><p className="text-text-muted text-sm">Select a symbol</p></Panel></div>
        <div key="order-entry"><Panel title="Order Entry"><p className="text-text-muted text-sm">Form here</p></Panel></div>
        <div key="strategies"><Panel title="Strategies"><p className="text-text-muted text-sm">No strategies</p></Panel></div>
        <div key="price-chart"><Panel title="Price Chart"><p className="text-text-muted text-sm">Chart here</p></Panel></div>
        <div key="risk"><Panel title="Risk Dashboard"><p className="text-text-muted text-sm">No risk data</p></Panel></div>
        <div key="orders"><Panel title="Active Orders"><p className="text-text-muted text-sm">No orders</p></Panel></div>
        <div key="event-log"><Panel title="Event Log"><p className="text-text-muted text-sm">No events</p></Panel></div>
      </DashboardGrid>
    </main>
  );
}
