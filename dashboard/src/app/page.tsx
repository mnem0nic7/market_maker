"use client";

import { DashboardGrid } from "@/components/layout/DashboardGrid";
import { Panel } from "@/components/ui/Panel";
import { useDashboard, useDashboardDispatch } from "@/lib/store";
import { MarketDataPanel } from "@/components/panels/MarketData";
import { PositionsPanel } from "@/components/panels/Positions";
import { OrderBookPanel } from "@/components/panels/OrderBook";
import { OrderEntryPanel } from "@/components/panels/OrderEntry";
import { StrategiesPanel } from "@/components/panels/Strategies";
import { PriceChartPanel } from "@/components/panels/PriceChart";
import { RiskDashboardPanel } from "@/components/panels/RiskDashboard";
import { ActiveOrdersPanel } from "@/components/panels/ActiveOrders";
import { EventLogPanel } from "@/components/panels/EventLog";
import { StrategyFormPanel } from "@/components/panels/StrategyForm";
import { AiAssistantPanel } from "@/components/panels/AiAssistant";
import { BacktestResultsPanel } from "@/components/panels/BacktestResults";

export default function DashboardPage() {
  const { connected, loading, error } = useDashboard();
  const dispatch = useDashboardDispatch();
  return (
    <main className="min-h-screen">
      {/* status bar */}
      <div className="flex items-center justify-between px-4 py-2 border-b border-border-panel bg-bg-panel/50">
        <h1 className="text-sm font-bold text-accent-cyan font-mono tracking-wider">MARKET MAKER</h1>
        <div className="flex items-center gap-3 text-xs">
          {connected ? (
            <span className="flex items-center gap-1.5 text-accent-green">
              <span className="w-2 h-2 rounded-full bg-accent-green animate-pulse" />
              CONNECTED
            </span>
          ) : loading ? (
            <span className="flex items-center gap-1.5 text-accent-amber">
              <span className="w-2 h-2 rounded-full bg-accent-amber animate-pulse" />
              CONNECTING...
            </span>
          ) : (
            <span className="flex items-center gap-1.5 text-accent-red">
              <span className="w-2 h-2 rounded-full bg-accent-red animate-pulse" />
              RECONNECTING...
            </span>
          )}
          {loading && <span className="text-accent-amber animate-pulse">Loading...</span>}
        </div>
      </div>

      {/* Global error banner */}
      {error && (
        <div className="flex items-center justify-between px-4 py-2 bg-accent-red/10 border-b border-accent-red/30 text-accent-red text-xs">
          <span className="font-mono">{error}</span>
          <button
            type="button"
            onClick={() => dispatch({ type: "SET_ERROR", error: null })}
            className="ml-4 px-2 py-0.5 rounded hover:bg-accent-red/20 transition-colors font-semibold"
          >
            Dismiss
          </button>
        </div>
      )}

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
        <div key="strategy-form"><Panel title="New Strategy"><StrategyFormPanel /></Panel></div>
        <div key="ai-assistant"><Panel title="AI Assistant"><AiAssistantPanel /></Panel></div>
        <div key="backtest-results"><Panel title="Backtest Results"><BacktestResultsPanel /></Panel></div>
      </DashboardGrid>
    </main>
  );
}
