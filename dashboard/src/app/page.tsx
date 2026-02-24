"use client";

import { DashboardGrid } from "@/components/layout/DashboardGrid";
import { Panel } from "@/components/ui/Panel";
import { useDashboard } from "@/lib/store";
import { MarketDataPanel } from "@/components/panels/MarketData";
import { PositionsPanel } from "@/components/panels/Positions";
import { OrderBookPanel } from "@/components/panels/OrderBook";
import { OrderEntryPanel } from "@/components/panels/OrderEntry";
import { StrategiesPanel } from "@/components/panels/Strategies";
import { PriceChartPanel } from "@/components/panels/PriceChart";
import { RiskDashboardPanel } from "@/components/panels/RiskDashboard";
import { ActiveOrdersPanel } from "@/components/panels/ActiveOrders";
import { EventLogPanel } from "@/components/panels/EventLog";

export default function DashboardPage() {
  const { connected, loading } = useDashboard();
  return (
    <main className="min-h-screen">
      {/* status bar stays the same */}
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
        <div key="market-data"><Panel title="Market Data"><MarketDataPanel /></Panel></div>
        <div key="positions"><Panel title="Positions & PnL"><PositionsPanel /></Panel></div>
        <div key="order-book"><Panel title="Order Book"><OrderBookPanel /></Panel></div>
        <div key="order-entry"><Panel title="Order Entry"><OrderEntryPanel /></Panel></div>
        <div key="strategies"><Panel title="Strategies"><StrategiesPanel /></Panel></div>
        <div key="price-chart"><Panel title="Price Chart"><PriceChartPanel /></Panel></div>
        <div key="risk"><Panel title="Risk Dashboard"><RiskDashboardPanel /></Panel></div>
        <div key="orders"><Panel title="Active Orders"><ActiveOrdersPanel /></Panel></div>
        <div key="event-log"><Panel title="Event Log"><EventLogPanel /></Panel></div>
      </DashboardGrid>
    </main>
  );
}
