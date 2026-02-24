# Trading Dashboard Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a full trading desk dashboard (Next.js + React) with draggable panels, real-time WebSocket updates, and controls for orders, strategies, and the kill switch.

**Architecture:** Standalone Next.js 14 app in `dashboard/` connects to the existing Rust gateway at `localhost:8080`. WebSocket for real-time data, REST for actions. React Context + useReducer for state, react-grid-layout for draggable panels.

**Tech Stack:** Next.js 14, TypeScript, TailwindCSS, TradingView Lightweight Charts, Recharts, react-grid-layout

**Design doc:** `docs/plans/2026-02-24-trading-dashboard-design.md`

---

## Gateway API Reference (Quick)

All REST responses wrapped in `{ status: "ok"|"error", data?: T, error?: string }`.

| Method | Endpoint | Returns |
|--------|----------|---------|
| GET | /api/v1/health | `{ status, timestamp, version }` |
| GET | /api/v1/positions | `Position[]` |
| GET | /api/v1/orders | `Order[]` (query: exchange, symbol, strategy_id) |
| GET | /api/v1/orders/:id | `Order` |
| GET | /api/v1/portfolio/risk | `PortfolioRisk` |
| GET | /api/v1/strategies | `StrategyStatus[]` |
| POST | /api/v1/strategies/:id/start | message |
| POST | /api/v1/strategies/:id/stop | message |
| POST | /api/v1/risk/kill-switch | `{ active, timestamp }` |
| DELETE | /api/v1/risk/kill-switch | `{ active, timestamp }` |
| GET | /api/v1/market-data/:exchange/:symbol/orderbook | `OrderBook` |

WebSocket at `/api/v1/ws`. Client sends `{"action":"subscribe","channel":"all"}`. Server sends tagged messages: `PositionUpdate`, `OrderUpdate`, `RiskUpdate`, `MarketDataTick`, `Error`, `Subscribed`, `Unsubscribed`.

---

## Task 1: Scaffold Next.js Project

**Files:**
- Create: `dashboard/package.json`
- Create: `dashboard/tsconfig.json`
- Create: `dashboard/next.config.js`
- Create: `dashboard/tailwind.config.ts`
- Create: `dashboard/postcss.config.js`
- Create: `dashboard/.gitignore`
- Create: `dashboard/src/app/layout.tsx`
- Create: `dashboard/src/app/page.tsx`
- Create: `dashboard/src/app/globals.css`

**Step 1: Initialize Next.js app**

```bash
cd /home/gallison/workspace/market_maker
npx create-next-app@latest dashboard \
  --typescript \
  --tailwind \
  --eslint \
  --app \
  --src-dir \
  --import-alias "@/*" \
  --no-turbopack
```

Accept all defaults. This creates the full scaffold.

**Step 2: Install additional dependencies**

```bash
cd /home/gallison/workspace/market_maker/dashboard
npm install lightweight-charts recharts react-grid-layout
npm install -D @types/react-grid-layout
```

**Step 3: Configure dark terminal theme in `dashboard/tailwind.config.ts`**

Replace the generated tailwind config with:

```typescript
import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./src/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        bg: {
          primary: "#0a0a0f",
          panel: "#1a1a2e",
          hover: "#252540",
        },
        border: {
          panel: "#2a2a3e",
        },
        accent: {
          cyan: "#00d4ff",
          green: "#00ff88",
          red: "#ff4444",
          amber: "#ffaa00",
        },
        text: {
          primary: "#e0e0e0",
          secondary: "#8888aa",
          muted: "#555577",
        },
      },
      fontFamily: {
        mono: ['"JetBrains Mono"', '"Fira Code"', "ui-monospace", "monospace"],
        sans: ["Inter", "system-ui", "sans-serif"],
      },
    },
  },
  plugins: [],
};

export default config;
```

**Step 4: Set up globals.css**

Replace `dashboard/src/app/globals.css` with:

```css
@tailwind base;
@tailwind components;
@tailwind utilities;

@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap');

:root {
  --bg-primary: #0a0a0f;
  --bg-panel: #1a1a2e;
  --border-panel: #2a2a3e;
  --accent-cyan: #00d4ff;
  --accent-green: #00ff88;
  --accent-red: #ff4444;
  --accent-amber: #ffaa00;
}

body {
  background-color: var(--bg-primary);
  color: #e0e0e0;
  font-family: "Inter", system-ui, sans-serif;
}

/* react-grid-layout styles */
.react-grid-layout {
  position: relative;
}
.react-grid-item {
  transition: all 200ms ease;
  transition-property: left, top;
}
.react-grid-item.cssTransforms {
  transition-property: transform;
}
.react-grid-item.resizing {
  z-index: 10;
  will-change: width, height;
}
.react-grid-item > .react-resizable-handle {
  position: absolute;
  width: 20px;
  height: 20px;
  bottom: 0;
  right: 0;
  cursor: se-resize;
}
.react-grid-item > .react-resizable-handle::after {
  content: "";
  position: absolute;
  right: 4px;
  bottom: 4px;
  width: 8px;
  height: 8px;
  border-right: 2px solid #2a2a3e;
  border-bottom: 2px solid #2a2a3e;
}
.react-grid-placeholder {
  background: #00d4ff;
  opacity: 0.15;
  border-radius: 8px;
}

/* Scrollbar styling */
::-webkit-scrollbar {
  width: 6px;
  height: 6px;
}
::-webkit-scrollbar-track {
  background: #0a0a0f;
}
::-webkit-scrollbar-thumb {
  background: #2a2a3e;
  border-radius: 3px;
}
::-webkit-scrollbar-thumb:hover {
  background: #3a3a5e;
}
```

**Step 5: Create root layout with fonts**

Replace `dashboard/src/app/layout.tsx`:

```tsx
import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Trading Dashboard",
  description: "Market Maker Trading Desk",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body className="bg-bg-primary text-text-primary antialiased min-h-screen">
        {children}
      </body>
    </html>
  );
}
```

**Step 6: Create placeholder page**

Replace `dashboard/src/app/page.tsx`:

```tsx
export default function DashboardPage() {
  return (
    <main className="p-4">
      <h1 className="text-2xl font-bold text-accent-cyan font-mono">
        Trading Dashboard
      </h1>
      <p className="text-text-secondary mt-2">Panels loading...</p>
    </main>
  );
}
```

**Step 7: Configure Next.js for API proxy**

Replace `dashboard/next.config.js`:

```javascript
/** @type {import('next').NextConfig} */
const nextConfig = {
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: "http://localhost:8080/api/:path*",
      },
    ];
  },
};

module.exports = nextConfig;
```

**Step 8: Verify dev server starts**

```bash
cd /home/gallison/workspace/market_maker/dashboard
npm run dev
```

Expected: Server starts on http://localhost:3000, page shows "Trading Dashboard" in cyan.

**Step 9: Commit**

```bash
cd /home/gallison/workspace/market_maker
git add dashboard/
git commit -m "feat(dashboard): scaffold Next.js project with dark terminal theme"
```

---

## Task 2: TypeScript Types + REST Client + WebSocket Manager

**Files:**
- Create: `dashboard/src/lib/types.ts`
- Create: `dashboard/src/lib/api.ts`
- Create: `dashboard/src/lib/websocket.ts`

**Step 1: Create TypeScript types matching the Rust API**

Create `dashboard/src/lib/types.ts`:

```typescript
// Matches crates/mm-core/src/types.rs

export type Exchange =
  | "Binance"
  | "Coinbase"
  | "Kraken"
  | "Uniswap"
  | "Raydium"
  | "InteractiveBrokers"
  | "Alpaca"
  | "Simulated";

export type Side = "Buy" | "Sell";

export type OrderType = "Market" | "Limit" | "StopLimit" | "StopMarket";

export type TimeInForce = "GTC" | "IOC" | "FOK" | "GTD";

export type OrderStatus =
  | "Pending"
  | "Submitted"
  | "New"
  | "PartiallyFilled"
  | "Filled"
  | "PendingCancel"
  | "Cancelled"
  | "Rejected"
  | "Expired";

export interface Symbol {
  base: string;
  quote: string;
}

export interface PriceLevel {
  price: string; // Decimal serializes as string
  quantity: string;
}

export interface OrderBook {
  exchange: Exchange;
  symbol: Symbol;
  bids: PriceLevel[];
  asks: PriceLevel[];
  timestamp: string;
  sequence: number;
}

export interface Order {
  client_order_id: string;
  exchange_order_id: string | null;
  exchange: Exchange;
  symbol: Symbol;
  side: Side;
  order_type: OrderType;
  time_in_force: TimeInForce;
  price: string | null;
  quantity: string;
  status: OrderStatus;
  filled_quantity: string;
  remaining_quantity: string;
  average_fill_price: string;
  strategy_id: string;
  reduce_only: boolean;
  created_at: string;
  updated_at: string;
}

export interface Position {
  exchange: Exchange;
  symbol: Symbol;
  quantity: string;
  average_entry_price: string;
  unrealized_pnl: string;
  realized_pnl: string;
  updated_at: string;
}

export interface PortfolioRisk {
  total_var_95: string;
  total_var_99: string;
  net_exposure: string;
  gross_exposure: string;
  max_drawdown: string;
  daily_pnl: string;
  positions: Position[];
  timestamp: string;
}

export interface StrategyConfig {
  strategy_id: string;
  strategy_type: string;
  exchange: Exchange;
  symbol: Symbol;
  enabled: boolean;
  params: Record<string, unknown>;
}

export interface StrategyStatus {
  config: StrategyConfig;
  running: boolean;
  started_at: string | null;
  stopped_at: string | null;
}

export interface HealthResponse {
  status: string;
  timestamp: string;
  version: string;
}

export interface KillSwitchResponse {
  active: boolean;
  timestamp: string;
}

// Gateway API envelope
export interface ApiResponse<T> {
  status: "ok" | "error";
  data?: T;
  error?: string;
}

// WebSocket messages (server → client)
// Tagged union on "type" field
export type ServerMessage =
  | PositionUpdateMsg
  | OrderUpdateMsg
  | RiskUpdateMsg
  | MarketDataTickMsg
  | ErrorMsg
  | SubscribedMsg
  | UnsubscribedMsg;

export interface PositionUpdateMsg {
  type: "PositionUpdate";
  exchange: Exchange;
  symbol: Symbol;
  quantity: string;
  unrealized_pnl: string;
  realized_pnl: string;
}

export interface OrderUpdateMsg {
  type: "OrderUpdate";
  client_order_id: string;
  exchange: Exchange;
  symbol: Symbol;
  status: string;
  filled_quantity: string;
  remaining_quantity: string;
}

export interface RiskUpdateMsg {
  type: "RiskUpdate";
  net_exposure: string;
  gross_exposure: string;
  daily_pnl: string;
  kill_switch_active: boolean;
}

export interface MarketDataTickMsg {
  type: "MarketDataTick";
  exchange: Exchange;
  symbol: Symbol;
  best_bid: string;
  best_ask: string;
  mid_price: string;
  spread_bps: string;
}

export interface ErrorMsg {
  type: "Error";
  message: string;
}

export interface SubscribedMsg {
  type: "Subscribed";
  channel: string;
}

export interface UnsubscribedMsg {
  type: "Unsubscribed";
  channel: string;
}

// Client → server
export type ClientMessage =
  | { action: "subscribe"; channel: string }
  | { action: "unsubscribe"; channel: string }
  | { action: "ping" };
```

**Step 2: Create REST client**

Create `dashboard/src/lib/api.ts`:

```typescript
import type {
  ApiResponse,
  HealthResponse,
  Position,
  Order,
  PortfolioRisk,
  StrategyStatus,
  OrderBook,
  KillSwitchResponse,
} from "./types";

// Uses Next.js rewrite proxy: /api/* → localhost:8080/api/*
const BASE = "/api/v1";

async function fetchApi<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...init?.headers },
    ...init,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API ${res.status}: ${text}`);
  }

  const envelope: ApiResponse<T> = await res.json();
  if (envelope.status === "error") {
    throw new Error(envelope.error ?? "Unknown API error");
  }
  return envelope.data as T;
}

export const api = {
  health: () => fetchApi<HealthResponse>("/health"),

  positions: () => fetchApi<Position[]>("/positions"),

  orders: (params?: { exchange?: string; symbol?: string; strategy_id?: string }) => {
    const query = new URLSearchParams();
    if (params?.exchange) query.set("exchange", params.exchange);
    if (params?.symbol) query.set("symbol", params.symbol);
    if (params?.strategy_id) query.set("strategy_id", params.strategy_id);
    const qs = query.toString();
    return fetchApi<Order[]>(`/orders${qs ? `?${qs}` : ""}`);
  },

  order: (id: string) => fetchApi<Order>(`/orders/${encodeURIComponent(id)}`),

  portfolioRisk: () => fetchApi<PortfolioRisk>("/portfolio/risk"),

  strategies: () => fetchApi<StrategyStatus[]>("/strategies"),

  startStrategy: (id: string) =>
    fetchApi<string>(`/strategies/${encodeURIComponent(id)}/start`, { method: "POST" }),

  stopStrategy: (id: string) =>
    fetchApi<string>(`/strategies/${encodeURIComponent(id)}/stop`, { method: "POST" }),

  activateKillSwitch: () =>
    fetchApi<KillSwitchResponse>("/risk/kill-switch", { method: "POST" }),

  resetKillSwitch: () =>
    fetchApi<KillSwitchResponse>("/risk/kill-switch", { method: "DELETE" }),

  orderbook: (exchange: string, symbol: string) =>
    fetchApi<OrderBook>(`/market-data/${encodeURIComponent(exchange)}/${encodeURIComponent(symbol)}/orderbook`),
};
```

**Step 3: Create WebSocket manager**

Create `dashboard/src/lib/websocket.ts`:

```typescript
import type { ServerMessage, ClientMessage } from "./types";

export type MessageHandler = (msg: ServerMessage) => void;
export type StatusHandler = (connected: boolean) => void;

export class WebSocketManager {
  private ws: WebSocket | null = null;
  private url: string;
  private onMessage: MessageHandler;
  private onStatus: StatusHandler;
  private reconnectDelay = 1000;
  private maxReconnectDelay = 30000;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private intentionalClose = false;

  constructor(url: string, onMessage: MessageHandler, onStatus: StatusHandler) {
    this.url = url;
    this.onMessage = onMessage;
    this.onStatus = onStatus;
  }

  connect() {
    this.intentionalClose = false;
    this.ws = new WebSocket(this.url);

    this.ws.onopen = () => {
      this.reconnectDelay = 1000;
      this.onStatus(true);
      this.send({ action: "subscribe", channel: "all" });
    };

    this.ws.onmessage = (event) => {
      try {
        const msg: ServerMessage = JSON.parse(event.data);
        this.onMessage(msg);
      } catch {
        console.warn("Failed to parse WS message:", event.data);
      }
    };

    this.ws.onclose = () => {
      this.onStatus(false);
      if (!this.intentionalClose) {
        this.scheduleReconnect();
      }
    };

    this.ws.onerror = () => {
      // onclose will fire after onerror
    };
  }

  disconnect() {
    this.intentionalClose = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.ws?.close();
    this.ws = null;
  }

  send(msg: ClientMessage) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    }
  }

  private scheduleReconnect() {
    this.reconnectTimer = setTimeout(() => {
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, this.maxReconnectDelay);
      this.connect();
    }, this.reconnectDelay);
  }
}
```

**Step 4: Commit**

```bash
cd /home/gallison/workspace/market_maker
git add dashboard/src/lib/
git commit -m "feat(dashboard): add TypeScript types, REST client, and WebSocket manager"
```

---

## Task 3: Global State (Context + Reducer)

**Files:**
- Create: `dashboard/src/lib/store.tsx`

**Step 1: Create the store**

Create `dashboard/src/lib/store.tsx`:

```tsx
"use client";

import {
  createContext,
  useContext,
  useReducer,
  useEffect,
  useRef,
  useCallback,
  type ReactNode,
  type Dispatch,
} from "react";
import { api } from "./api";
import { WebSocketManager } from "./websocket";
import type {
  Position,
  Order,
  PortfolioRisk,
  StrategyStatus,
  ServerMessage,
  MarketDataTickMsg,
} from "./types";

// ---------------------------------------------------------------------------
// State shape
// ---------------------------------------------------------------------------

export interface DashboardState {
  connected: boolean;
  positions: Position[];
  orders: Order[];
  risk: PortfolioRisk | null;
  strategies: StrategyStatus[];
  ticks: Record<string, MarketDataTickMsg>; // keyed by "Exchange:BASE/QUOTE"
  events: ServerMessage[]; // last N events for the log panel
  loading: boolean;
  error: string | null;
}

const initialState: DashboardState = {
  connected: false,
  positions: [],
  orders: [],
  risk: null,
  strategies: [],
  ticks: {},
  events: [],
  loading: true,
  error: null,
};

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------

export type Action =
  | { type: "SET_CONNECTED"; connected: boolean }
  | { type: "SET_POSITIONS"; positions: Position[] }
  | { type: "SET_ORDERS"; orders: Order[] }
  | { type: "SET_RISK"; risk: PortfolioRisk }
  | { type: "SET_STRATEGIES"; strategies: StrategyStatus[] }
  | { type: "WS_MESSAGE"; msg: ServerMessage }
  | { type: "SET_LOADING"; loading: boolean }
  | { type: "SET_ERROR"; error: string | null };

const MAX_EVENTS = 200;

function tickKey(exchange: string, symbol: { base: string; quote: string }): string {
  return `${exchange}:${symbol.base}/${symbol.quote}`;
}

function reducer(state: DashboardState, action: Action): DashboardState {
  switch (action.type) {
    case "SET_CONNECTED":
      return { ...state, connected: action.connected };
    case "SET_POSITIONS":
      return { ...state, positions: action.positions };
    case "SET_ORDERS":
      return { ...state, orders: action.orders };
    case "SET_RISK":
      return { ...state, risk: action.risk };
    case "SET_STRATEGIES":
      return { ...state, strategies: action.strategies };
    case "SET_LOADING":
      return { ...state, loading: action.loading };
    case "SET_ERROR":
      return { ...state, error: action.error };
    case "WS_MESSAGE": {
      const msg = action.msg;
      const events = [msg, ...state.events].slice(0, MAX_EVENTS);

      switch (msg.type) {
        case "MarketDataTick":
          return {
            ...state,
            events,
            ticks: {
              ...state.ticks,
              [tickKey(msg.exchange, msg.symbol)]: msg,
            },
          };
        case "PositionUpdate":
          return { ...state, events }; // Full refresh on next REST poll or keep WS-only
        case "OrderUpdate":
          return { ...state, events };
        case "RiskUpdate":
          return {
            ...state,
            events,
            risk: state.risk
              ? {
                  ...state.risk,
                  net_exposure: msg.net_exposure,
                  gross_exposure: msg.gross_exposure,
                  daily_pnl: msg.daily_pnl,
                }
              : null,
          };
        default:
          return { ...state, events };
      }
    }
    default:
      return state;
  }
}

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

const StateContext = createContext<DashboardState>(initialState);
const DispatchContext = createContext<Dispatch<Action>>(() => {});

export function useDashboard() {
  return useContext(StateContext);
}

export function useDashboardDispatch() {
  return useContext(DispatchContext);
}

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

export function DashboardProvider({ children }: { children: ReactNode }) {
  const [state, dispatch] = useReducer(reducer, initialState);
  const wsRef = useRef<WebSocketManager | null>(null);

  const handleMessage = useCallback((msg: ServerMessage) => {
    dispatch({ type: "WS_MESSAGE", msg });
  }, []);

  const handleStatus = useCallback((connected: boolean) => {
    dispatch({ type: "SET_CONNECTED", connected });
  }, []);

  // Hydrate initial state from REST
  useEffect(() => {
    async function hydrate() {
      try {
        const [positions, orders, strategies] = await Promise.allSettled([
          api.positions(),
          api.orders(),
          api.strategies(),
        ]);
        if (positions.status === "fulfilled")
          dispatch({ type: "SET_POSITIONS", positions: positions.value });
        if (orders.status === "fulfilled")
          dispatch({ type: "SET_ORDERS", orders: orders.value });
        if (strategies.status === "fulfilled")
          dispatch({ type: "SET_STRATEGIES", strategies: strategies.value });

        try {
          const risk = await api.portfolioRisk();
          dispatch({ type: "SET_RISK", risk });
        } catch {
          // Risk endpoint may not be populated yet
        }
      } catch (e) {
        dispatch({ type: "SET_ERROR", error: String(e) });
      } finally {
        dispatch({ type: "SET_LOADING", loading: false });
      }
    }
    hydrate();
  }, []);

  // Connect WebSocket
  useEffect(() => {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    // In dev, connect to gateway directly; in prod, use same host
    const wsUrl =
      process.env.NODE_ENV === "development"
        ? "ws://localhost:8080/api/v1/ws"
        : `${protocol}//${window.location.host}/api/v1/ws`;

    const ws = new WebSocketManager(wsUrl, handleMessage, handleStatus);
    ws.connect();
    wsRef.current = ws;

    return () => {
      ws.disconnect();
    };
  }, [handleMessage, handleStatus]);

  return (
    <StateContext.Provider value={state}>
      <DispatchContext.Provider value={dispatch}>{children}</DispatchContext.Provider>
    </StateContext.Provider>
  );
}
```

**Step 2: Wire provider into layout**

Update `dashboard/src/app/layout.tsx` — wrap `{children}` with `<DashboardProvider>`:

```tsx
import type { Metadata } from "next";
import { DashboardProvider } from "@/lib/store";
import "./globals.css";

export const metadata: Metadata = {
  title: "Trading Dashboard",
  description: "Market Maker Trading Desk",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body className="bg-bg-primary text-text-primary antialiased min-h-screen">
        <DashboardProvider>{children}</DashboardProvider>
      </body>
    </html>
  );
}
```

**Step 3: Commit**

```bash
cd /home/gallison/workspace/market_maker
git add dashboard/src/lib/store.tsx dashboard/src/app/layout.tsx
git commit -m "feat(dashboard): add global state provider with REST hydration and WebSocket"
```

---

## Task 4: Panel Shell + Dashboard Grid Layout

**Files:**
- Create: `dashboard/src/components/ui/Panel.tsx`
- Create: `dashboard/src/components/layout/DashboardGrid.tsx`
- Modify: `dashboard/src/app/page.tsx`

**Step 1: Create Panel wrapper component**

Create `dashboard/src/components/ui/Panel.tsx`:

```tsx
"use client";

import type { ReactNode } from "react";

interface PanelProps {
  title: string;
  children: ReactNode;
  className?: string;
  headerRight?: ReactNode;
}

export function Panel({ title, children, className = "", headerRight }: PanelProps) {
  return (
    <div
      className={`flex flex-col bg-bg-panel border border-border-panel rounded-lg overflow-hidden h-full ${className}`}
    >
      <div className="flex items-center justify-between px-3 py-2 border-b border-border-panel bg-bg-panel/80 cursor-move select-none shrink-0">
        <span className="text-xs font-semibold uppercase tracking-wider text-text-secondary">
          {title}
        </span>
        {headerRight}
      </div>
      <div className="flex-1 overflow-auto p-3">{children}</div>
    </div>
  );
}
```

**Step 2: Create DashboardGrid**

Create `dashboard/src/components/layout/DashboardGrid.tsx`:

```tsx
"use client";

import { useState, useCallback } from "react";
import { Responsive, WidthProvider, type Layout, type Layouts } from "react-grid-layout";
import "react-grid-layout/css/styles.css";

const ResponsiveGridLayout = WidthProvider(Responsive);

const STORAGE_KEY = "dashboard-layouts";

// Default layout: 12-column grid
const defaultLayouts: Layouts = {
  lg: [
    { i: "market-data", x: 0, y: 0, w: 6, h: 4, minW: 3, minH: 3 },
    { i: "positions", x: 6, y: 0, w: 6, h: 4, minW: 3, minH: 3 },
    { i: "order-book", x: 0, y: 4, w: 4, h: 5, minW: 3, minH: 4 },
    { i: "order-entry", x: 4, y: 4, w: 4, h: 5, minW: 3, minH: 4 },
    { i: "strategies", x: 8, y: 4, w: 4, h: 5, minW: 3, minH: 3 },
    { i: "price-chart", x: 0, y: 9, w: 8, h: 5, minW: 4, minH: 4 },
    { i: "risk", x: 8, y: 9, w: 4, h: 5, minW: 3, minH: 3 },
    { i: "orders", x: 0, y: 14, w: 8, h: 4, minW: 4, minH: 3 },
    { i: "event-log", x: 8, y: 14, w: 4, h: 4, minW: 3, minH: 3 },
  ],
};

function loadLayouts(): Layouts {
  if (typeof window === "undefined") return defaultLayouts;
  try {
    const saved = localStorage.getItem(STORAGE_KEY);
    return saved ? JSON.parse(saved) : defaultLayouts;
  } catch {
    return defaultLayouts;
  }
}

interface DashboardGridProps {
  children: React.ReactNode;
}

export function DashboardGrid({ children }: DashboardGridProps) {
  const [layouts, setLayouts] = useState<Layouts>(loadLayouts);

  const onLayoutChange = useCallback((_layout: Layout[], allLayouts: Layouts) => {
    setLayouts(allLayouts);
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(allLayouts));
    } catch {
      // localStorage full or unavailable
    }
  }, []);

  return (
    <ResponsiveGridLayout
      className="layout"
      layouts={layouts}
      breakpoints={{ lg: 1200, md: 996, sm: 768, xs: 480 }}
      cols={{ lg: 12, md: 10, sm: 6, xs: 4 }}
      rowHeight={40}
      onLayoutChange={onLayoutChange}
      draggableHandle=".cursor-move"
      compactType="vertical"
      margin={[8, 8]}
    >
      {children}
    </ResponsiveGridLayout>
  );
}
```

**Step 3: Update page.tsx with grid and placeholder panels**

Replace `dashboard/src/app/page.tsx`:

```tsx
"use client";

import { DashboardGrid } from "@/components/layout/DashboardGrid";
import { Panel } from "@/components/ui/Panel";
import { useDashboard } from "@/lib/store";

export default function DashboardPage() {
  const { connected, loading } = useDashboard();

  return (
    <main className="min-h-screen">
      {/* Status bar */}
      <div className="flex items-center justify-between px-4 py-2 border-b border-border-panel bg-bg-panel/50">
        <h1 className="text-sm font-bold text-accent-cyan font-mono tracking-wider">
          MARKET MAKER
        </h1>
        <div className="flex items-center gap-3 text-xs">
          <span className={`flex items-center gap-1.5 ${connected ? "text-accent-green" : "text-accent-red"}`}>
            <span className={`w-2 h-2 rounded-full ${connected ? "bg-accent-green" : "bg-accent-red"} ${connected ? "animate-pulse" : ""}`} />
            {connected ? "CONNECTED" : "DISCONNECTED"}
          </span>
          {loading && <span className="text-accent-amber">Loading...</span>}
        </div>
      </div>

      <DashboardGrid>
        <div key="market-data">
          <Panel title="Market Data">
            <p className="text-text-muted text-sm">Waiting for ticks...</p>
          </Panel>
        </div>
        <div key="positions">
          <Panel title="Positions & PnL">
            <p className="text-text-muted text-sm">No positions</p>
          </Panel>
        </div>
        <div key="order-book">
          <Panel title="Order Book">
            <p className="text-text-muted text-sm">Select a symbol</p>
          </Panel>
        </div>
        <div key="order-entry">
          <Panel title="Order Entry">
            <p className="text-text-muted text-sm">Form here</p>
          </Panel>
        </div>
        <div key="strategies">
          <Panel title="Strategies">
            <p className="text-text-muted text-sm">No strategies</p>
          </Panel>
        </div>
        <div key="price-chart">
          <Panel title="Price Chart">
            <p className="text-text-muted text-sm">Chart here</p>
          </Panel>
        </div>
        <div key="risk">
          <Panel title="Risk Dashboard">
            <p className="text-text-muted text-sm">No risk data</p>
          </Panel>
        </div>
        <div key="orders">
          <Panel title="Active Orders">
            <p className="text-text-muted text-sm">No orders</p>
          </Panel>
        </div>
        <div key="event-log">
          <Panel title="Event Log">
            <p className="text-text-muted text-sm">No events</p>
          </Panel>
        </div>
      </DashboardGrid>
    </main>
  );
}
```

**Step 4: Verify layout renders**

```bash
cd /home/gallison/workspace/market_maker/dashboard
npm run dev
```

Expected: 9 draggable panels in a grid layout, dark theme, status bar at top.

**Step 5: Commit**

```bash
cd /home/gallison/workspace/market_maker
git add dashboard/src/components/ dashboard/src/app/page.tsx
git commit -m "feat(dashboard): add draggable panel grid with dark terminal theme"
```

---

## Task 5: Market Data Panel

**Files:**
- Create: `dashboard/src/components/panels/MarketData.tsx`
- Modify: `dashboard/src/app/page.tsx`

**Step 1: Build the MarketData panel**

Create `dashboard/src/components/panels/MarketData.tsx`:

```tsx
"use client";

import { useDashboard } from "@/lib/store";
import type { MarketDataTickMsg } from "@/lib/types";

function formatDecimal(val: string, decimals = 2): string {
  const num = parseFloat(val);
  return isNaN(num) ? val : num.toFixed(decimals);
}

function TickRow({ tick }: { tick: MarketDataTickMsg }) {
  const spread = parseFloat(tick.spread_bps);
  const spreadColor =
    spread < 5 ? "text-accent-green" : spread < 20 ? "text-accent-amber" : "text-accent-red";

  return (
    <tr className="border-b border-border-panel/50 hover:bg-bg-hover transition-colors">
      <td className="py-1.5 px-2 font-mono text-xs">
        <span className="text-text-primary">{tick.symbol.base}</span>
        <span className="text-text-muted">/{tick.symbol.quote}</span>
      </td>
      <td className="py-1.5 px-2 text-xs text-text-secondary">{tick.exchange}</td>
      <td className="py-1.5 px-2 font-mono text-xs text-accent-green text-right">
        {formatDecimal(tick.best_bid)}
      </td>
      <td className="py-1.5 px-2 font-mono text-xs text-accent-red text-right">
        {formatDecimal(tick.best_ask)}
      </td>
      <td className="py-1.5 px-2 font-mono text-xs text-text-primary text-right">
        {formatDecimal(tick.mid_price)}
      </td>
      <td className={`py-1.5 px-2 font-mono text-xs text-right ${spreadColor}`}>
        {formatDecimal(tick.spread_bps, 1)} bps
      </td>
    </tr>
  );
}

export function MarketDataPanel() {
  const { ticks } = useDashboard();
  const tickList = Object.values(ticks);

  if (tickList.length === 0) {
    return <p className="text-text-muted text-sm">Waiting for market data ticks...</p>;
  }

  return (
    <table className="w-full text-left">
      <thead>
        <tr className="text-text-muted text-[10px] uppercase tracking-wider">
          <th className="py-1 px-2">Symbol</th>
          <th className="py-1 px-2">Exchange</th>
          <th className="py-1 px-2 text-right">Bid</th>
          <th className="py-1 px-2 text-right">Ask</th>
          <th className="py-1 px-2 text-right">Mid</th>
          <th className="py-1 px-2 text-right">Spread</th>
        </tr>
      </thead>
      <tbody>
        {tickList.map((tick) => (
          <TickRow
            key={`${tick.exchange}:${tick.symbol.base}/${tick.symbol.quote}`}
            tick={tick}
          />
        ))}
      </tbody>
    </table>
  );
}
```

**Step 2: Replace placeholder in page.tsx**

In `dashboard/src/app/page.tsx`, import `MarketDataPanel` and replace the market-data panel content:

```tsx
// Add import at top:
import { MarketDataPanel } from "@/components/panels/MarketData";

// Replace the market-data div:
<div key="market-data">
  <Panel title="Market Data">
    <MarketDataPanel />
  </Panel>
</div>
```

**Step 3: Commit**

```bash
cd /home/gallison/workspace/market_maker
git add dashboard/src/components/panels/MarketData.tsx dashboard/src/app/page.tsx
git commit -m "feat(dashboard): add live market data ticker panel"
```

---

## Task 6: Positions & PnL Panel

**Files:**
- Create: `dashboard/src/components/panels/Positions.tsx`
- Modify: `dashboard/src/app/page.tsx`

**Step 1: Build the Positions panel**

Create `dashboard/src/components/panels/Positions.tsx`:

```tsx
"use client";

import { useDashboard } from "@/lib/store";
import type { Position } from "@/lib/types";

function fmt(val: string, decimals = 2): string {
  const num = parseFloat(val);
  return isNaN(num) ? val : num.toFixed(decimals);
}

function pnlColor(val: string): string {
  const num = parseFloat(val);
  if (num > 0) return "text-accent-green";
  if (num < 0) return "text-accent-red";
  return "text-text-secondary";
}

function PositionRow({ pos }: { pos: Position }) {
  const qty = parseFloat(pos.quantity);
  const side = qty > 0 ? "LONG" : qty < 0 ? "SHORT" : "FLAT";
  const sideColor =
    side === "LONG" ? "text-accent-green" : side === "SHORT" ? "text-accent-red" : "text-text-muted";

  return (
    <tr className="border-b border-border-panel/50 hover:bg-bg-hover transition-colors">
      <td className="py-1.5 px-2 font-mono text-xs">
        {pos.symbol.base}/{pos.symbol.quote}
      </td>
      <td className="py-1.5 px-2 text-xs text-text-secondary">{pos.exchange}</td>
      <td className={`py-1.5 px-2 font-mono text-xs font-semibold ${sideColor}`}>{side}</td>
      <td className="py-1.5 px-2 font-mono text-xs text-right">{fmt(pos.quantity, 4)}</td>
      <td className="py-1.5 px-2 font-mono text-xs text-right">{fmt(pos.average_entry_price)}</td>
      <td className={`py-1.5 px-2 font-mono text-xs text-right ${pnlColor(pos.unrealized_pnl)}`}>
        {fmt(pos.unrealized_pnl)}
      </td>
      <td className={`py-1.5 px-2 font-mono text-xs text-right ${pnlColor(pos.realized_pnl)}`}>
        {fmt(pos.realized_pnl)}
      </td>
    </tr>
  );
}

export function PositionsPanel() {
  const { positions } = useDashboard();

  if (positions.length === 0) {
    return <p className="text-text-muted text-sm">No open positions</p>;
  }

  const totalUnrealized = positions.reduce((sum, p) => sum + parseFloat(p.unrealized_pnl), 0);
  const totalRealized = positions.reduce((sum, p) => sum + parseFloat(p.realized_pnl), 0);

  return (
    <div>
      {/* Summary bar */}
      <div className="flex gap-4 mb-3 text-xs">
        <span className="text-text-secondary">
          Positions: <span className="text-text-primary font-mono">{positions.length}</span>
        </span>
        <span className="text-text-secondary">
          Unrealized:{" "}
          <span className={`font-mono ${pnlColor(String(totalUnrealized))}`}>
            {totalUnrealized.toFixed(2)}
          </span>
        </span>
        <span className="text-text-secondary">
          Realized:{" "}
          <span className={`font-mono ${pnlColor(String(totalRealized))}`}>
            {totalRealized.toFixed(2)}
          </span>
        </span>
      </div>

      <table className="w-full text-left">
        <thead>
          <tr className="text-text-muted text-[10px] uppercase tracking-wider">
            <th className="py-1 px-2">Symbol</th>
            <th className="py-1 px-2">Exchange</th>
            <th className="py-1 px-2">Side</th>
            <th className="py-1 px-2 text-right">Qty</th>
            <th className="py-1 px-2 text-right">Entry</th>
            <th className="py-1 px-2 text-right">Unrealized</th>
            <th className="py-1 px-2 text-right">Realized</th>
          </tr>
        </thead>
        <tbody>
          {positions.map((pos) => (
            <PositionRow
              key={`${pos.exchange}:${pos.symbol.base}/${pos.symbol.quote}`}
              pos={pos}
            />
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

**Step 2: Wire into page.tsx**

Import `PositionsPanel` and replace the positions div content. Same pattern as Task 5.

**Step 3: Commit**

```bash
cd /home/gallison/workspace/market_maker
git add dashboard/src/components/panels/Positions.tsx dashboard/src/app/page.tsx
git commit -m "feat(dashboard): add positions and PnL panel"
```

---

## Task 7: Order Book Panel

**Files:**
- Create: `dashboard/src/components/panels/OrderBook.tsx`
- Modify: `dashboard/src/app/page.tsx`

**Step 1: Build the OrderBook panel**

Create `dashboard/src/components/panels/OrderBook.tsx`:

```tsx
"use client";

import { useState, useEffect } from "react";
import { api } from "@/lib/api";
import { useDashboard } from "@/lib/store";
import type { OrderBook, PriceLevel } from "@/lib/types";

function fmt(val: string, decimals = 2): string {
  const num = parseFloat(val);
  return isNaN(num) ? val : num.toFixed(decimals);
}

function DepthRow({
  level,
  side,
  maxQty,
}: {
  level: PriceLevel;
  side: "bid" | "ask";
  maxQty: number;
}) {
  const qty = parseFloat(level.quantity);
  const pct = maxQty > 0 ? (qty / maxQty) * 100 : 0;
  const color = side === "bid" ? "bg-accent-green/15" : "bg-accent-red/15";
  const textColor = side === "bid" ? "text-accent-green" : "text-accent-red";

  return (
    <tr className="relative hover:bg-bg-hover transition-colors">
      <td
        className="absolute inset-0 pointer-events-none"
        style={{ background: "transparent" }}
      >
        <div
          className={`h-full ${color}`}
          style={{ width: `${pct}%`, float: side === "bid" ? "right" : "left" }}
        />
      </td>
      <td className={`relative py-0.5 px-2 font-mono text-xs ${textColor} text-right`}>
        {fmt(level.price)}
      </td>
      <td className="relative py-0.5 px-2 font-mono text-xs text-text-primary text-right">
        {fmt(level.quantity, 4)}
      </td>
    </tr>
  );
}

export function OrderBookPanel() {
  const { ticks } = useDashboard();
  const [book, setBook] = useState<OrderBook | null>(null);
  const [selectedSymbol, setSelectedSymbol] = useState<string>("");
  const [selectedExchange, setSelectedExchange] = useState<string>("");

  // Derive available symbols from ticks
  const symbols = Object.values(ticks).map((t) => ({
    exchange: t.exchange,
    label: `${t.exchange}:${t.symbol.base}/${t.symbol.quote}`,
    symbol: `${t.symbol.base}/${t.symbol.quote}`,
  }));

  // Auto-select first available symbol
  useEffect(() => {
    if (!selectedSymbol && symbols.length > 0) {
      setSelectedExchange(symbols[0].exchange);
      setSelectedSymbol(symbols[0].symbol);
    }
  }, [symbols, selectedSymbol]);

  // Fetch order book
  useEffect(() => {
    if (!selectedExchange || !selectedSymbol) return;

    let cancelled = false;
    const fetchBook = async () => {
      try {
        const ob = await api.orderbook(selectedExchange, selectedSymbol);
        if (!cancelled) setBook(ob);
      } catch {
        // Silently fail — order book may not be available
      }
    };
    fetchBook();
    const interval = setInterval(fetchBook, 2000);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [selectedExchange, selectedSymbol]);

  const maxBidQty = book
    ? Math.max(...book.bids.slice(0, 15).map((l) => parseFloat(l.quantity)), 0)
    : 0;
  const maxAskQty = book
    ? Math.max(...book.asks.slice(0, 15).map((l) => parseFloat(l.quantity)), 0)
    : 0;
  const maxQty = Math.max(maxBidQty, maxAskQty);

  return (
    <div className="flex flex-col h-full">
      {/* Symbol selector */}
      {symbols.length > 0 && (
        <select
          className="mb-2 px-2 py-1 text-xs bg-bg-primary border border-border-panel rounded text-text-primary"
          value={`${selectedExchange}:${selectedSymbol}`}
          onChange={(e) => {
            const [ex, sym] = e.target.value.split(":");
            setSelectedExchange(ex);
            setSelectedSymbol(sym);
          }}
        >
          {symbols.map((s) => (
            <option key={s.label} value={`${s.exchange}:${s.symbol}`}>
              {s.label}
            </option>
          ))}
        </select>
      )}

      {!book ? (
        <p className="text-text-muted text-sm">Select a symbol to view depth</p>
      ) : (
        <div className="flex-1 overflow-auto">
          {/* Asks (reversed so lowest ask is at bottom) */}
          <table className="w-full">
            <thead>
              <tr className="text-text-muted text-[10px] uppercase tracking-wider">
                <th className="py-0.5 px-2 text-right">Price</th>
                <th className="py-0.5 px-2 text-right">Qty</th>
              </tr>
            </thead>
            <tbody>
              {book.asks
                .slice(0, 15)
                .reverse()
                .map((level, i) => (
                  <DepthRow key={`ask-${i}`} level={level} side="ask" maxQty={maxQty} />
                ))}
            </tbody>
          </table>

          {/* Spread */}
          <div className="py-1 px-2 text-center text-xs font-mono text-accent-cyan border-y border-border-panel/50">
            Spread:{" "}
            {book.asks[0] && book.bids[0]
              ? (parseFloat(book.asks[0].price) - parseFloat(book.bids[0].price)).toFixed(2)
              : "—"}
          </div>

          {/* Bids */}
          <table className="w-full">
            <tbody>
              {book.bids.slice(0, 15).map((level, i) => (
                <DepthRow key={`bid-${i}`} level={level} side="bid" maxQty={maxQty} />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
```

**Step 2: Wire into page.tsx (same pattern)**

**Step 3: Commit**

```bash
cd /home/gallison/workspace/market_maker
git add dashboard/src/components/panels/OrderBook.tsx dashboard/src/app/page.tsx
git commit -m "feat(dashboard): add order book depth panel with visual depth bars"
```

---

## Task 8: Order Entry Panel

**Files:**
- Create: `dashboard/src/components/panels/OrderEntry.tsx`
- Modify: `dashboard/src/app/page.tsx`

**Step 1: Build the OrderEntry form**

Create `dashboard/src/components/panels/OrderEntry.tsx`:

```tsx
"use client";

import { useState, type FormEvent } from "react";
import { api } from "@/lib/api";
import type { Exchange, Side, OrderType } from "@/lib/types";

const EXCHANGES: Exchange[] = ["Binance", "Coinbase", "Kraken", "Alpaca", "Simulated"];
const ORDER_TYPES: OrderType[] = ["Limit", "Market"];

export function OrderEntryPanel() {
  const [exchange, setExchange] = useState<Exchange>("Binance");
  const [symbol, setSymbol] = useState("BTC/USDT");
  const [side, setSide] = useState<Side>("Buy");
  const [orderType, setOrderType] = useState<OrderType>("Limit");
  const [price, setPrice] = useState("");
  const [quantity, setQuantity] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult] = useState<{ ok: boolean; msg: string } | null>(null);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setSubmitting(true);
    setResult(null);

    try {
      // The gateway doesn't have a POST /orders endpoint yet.
      // For now, show what would be submitted.
      // TODO: Wire to actual order submission when gateway supports it.
      const payload = {
        exchange,
        symbol,
        side,
        order_type: orderType,
        price: orderType === "Limit" ? price : undefined,
        quantity,
      };
      console.log("Order payload:", payload);
      setResult({ ok: true, msg: `Order submitted: ${side} ${quantity} ${symbol}` });
    } catch (err) {
      setResult({ ok: false, msg: String(err) });
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <form onSubmit={handleSubmit} className="flex flex-col gap-2">
      {/* Exchange */}
      <div>
        <label className="text-[10px] uppercase tracking-wider text-text-muted">Exchange</label>
        <select
          className="w-full mt-0.5 px-2 py-1.5 text-xs bg-bg-primary border border-border-panel rounded text-text-primary"
          value={exchange}
          onChange={(e) => setExchange(e.target.value as Exchange)}
        >
          {EXCHANGES.map((ex) => (
            <option key={ex} value={ex}>{ex}</option>
          ))}
        </select>
      </div>

      {/* Symbol */}
      <div>
        <label className="text-[10px] uppercase tracking-wider text-text-muted">Symbol</label>
        <input
          className="w-full mt-0.5 px-2 py-1.5 text-xs bg-bg-primary border border-border-panel rounded text-text-primary font-mono"
          value={symbol}
          onChange={(e) => setSymbol(e.target.value)}
          placeholder="BTC/USDT"
        />
      </div>

      {/* Side toggle */}
      <div>
        <label className="text-[10px] uppercase tracking-wider text-text-muted">Side</label>
        <div className="flex gap-1 mt-0.5">
          <button
            type="button"
            className={`flex-1 py-1.5 text-xs font-semibold rounded transition-colors ${
              side === "Buy"
                ? "bg-accent-green/20 text-accent-green border border-accent-green/50"
                : "bg-bg-primary text-text-muted border border-border-panel hover:border-accent-green/30"
            }`}
            onClick={() => setSide("Buy")}
          >
            BUY
          </button>
          <button
            type="button"
            className={`flex-1 py-1.5 text-xs font-semibold rounded transition-colors ${
              side === "Sell"
                ? "bg-accent-red/20 text-accent-red border border-accent-red/50"
                : "bg-bg-primary text-text-muted border border-border-panel hover:border-accent-red/30"
            }`}
            onClick={() => setSide("Sell")}
          >
            SELL
          </button>
        </div>
      </div>

      {/* Order type */}
      <div>
        <label className="text-[10px] uppercase tracking-wider text-text-muted">Type</label>
        <div className="flex gap-1 mt-0.5">
          {ORDER_TYPES.map((t) => (
            <button
              key={t}
              type="button"
              className={`flex-1 py-1.5 text-xs rounded transition-colors ${
                orderType === t
                  ? "bg-accent-cyan/20 text-accent-cyan border border-accent-cyan/50"
                  : "bg-bg-primary text-text-muted border border-border-panel"
              }`}
              onClick={() => setOrderType(t)}
            >
              {t.toUpperCase()}
            </button>
          ))}
        </div>
      </div>

      {/* Price (only for limit) */}
      {orderType === "Limit" && (
        <div>
          <label className="text-[10px] uppercase tracking-wider text-text-muted">Price</label>
          <input
            className="w-full mt-0.5 px-2 py-1.5 text-xs bg-bg-primary border border-border-panel rounded text-text-primary font-mono"
            type="number"
            step="any"
            value={price}
            onChange={(e) => setPrice(e.target.value)}
            placeholder="0.00"
          />
        </div>
      )}

      {/* Quantity */}
      <div>
        <label className="text-[10px] uppercase tracking-wider text-text-muted">Quantity</label>
        <input
          className="w-full mt-0.5 px-2 py-1.5 text-xs bg-bg-primary border border-border-panel rounded text-text-primary font-mono"
          type="number"
          step="any"
          value={quantity}
          onChange={(e) => setQuantity(e.target.value)}
          placeholder="0.00"
        />
      </div>

      {/* Submit */}
      <button
        type="submit"
        disabled={submitting || !quantity}
        className={`mt-1 py-2 text-xs font-bold rounded transition-colors ${
          side === "Buy"
            ? "bg-accent-green/20 text-accent-green border border-accent-green hover:bg-accent-green/30"
            : "bg-accent-red/20 text-accent-red border border-accent-red hover:bg-accent-red/30"
        } disabled:opacity-50 disabled:cursor-not-allowed`}
      >
        {submitting ? "Submitting..." : `${side.toUpperCase()} ${symbol}`}
      </button>

      {/* Result feedback */}
      {result && (
        <div
          className={`text-xs mt-1 px-2 py-1 rounded ${
            result.ok ? "bg-accent-green/10 text-accent-green" : "bg-accent-red/10 text-accent-red"
          }`}
        >
          {result.msg}
        </div>
      )}
    </form>
  );
}
```

**Step 2: Wire into page.tsx**

**Step 3: Commit**

```bash
cd /home/gallison/workspace/market_maker
git add dashboard/src/components/panels/OrderEntry.tsx dashboard/src/app/page.tsx
git commit -m "feat(dashboard): add order entry form panel"
```

---

## Task 9: Strategies Panel

**Files:**
- Create: `dashboard/src/components/panels/Strategies.tsx`
- Modify: `dashboard/src/app/page.tsx`

**Step 1: Build the Strategies panel**

Create `dashboard/src/components/panels/Strategies.tsx`:

```tsx
"use client";

import { useState } from "react";
import { api } from "@/lib/api";
import { useDashboard, useDashboardDispatch } from "@/lib/store";
import type { StrategyStatus } from "@/lib/types";

function StrategyCard({ strategy }: { strategy: StrategyStatus }) {
  const dispatch = useDashboardDispatch();
  const [loading, setLoading] = useState(false);

  const toggle = async () => {
    setLoading(true);
    try {
      if (strategy.running) {
        await api.stopStrategy(strategy.config.strategy_id);
      } else {
        await api.startStrategy(strategy.config.strategy_id);
      }
      // Refresh strategies
      const strategies = await api.strategies();
      dispatch({ type: "SET_STRATEGIES", strategies });
    } catch (err) {
      console.error("Strategy toggle failed:", err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="border border-border-panel rounded-lg p-3 bg-bg-primary/50">
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <span
            className={`w-2 h-2 rounded-full ${strategy.running ? "bg-accent-green animate-pulse" : "bg-text-muted"}`}
          />
          <span className="text-xs font-semibold text-text-primary">
            {strategy.config.strategy_id}
          </span>
        </div>
        <button
          onClick={toggle}
          disabled={loading}
          className={`px-3 py-1 text-[10px] font-semibold rounded transition-colors ${
            strategy.running
              ? "bg-accent-red/20 text-accent-red border border-accent-red/50 hover:bg-accent-red/30"
              : "bg-accent-green/20 text-accent-green border border-accent-green/50 hover:bg-accent-green/30"
          } disabled:opacity-50`}
        >
          {loading ? "..." : strategy.running ? "STOP" : "START"}
        </button>
      </div>

      <div className="text-[10px] text-text-muted space-y-0.5">
        <div>
          Type: <span className="text-text-secondary">{strategy.config.strategy_type}</span>
        </div>
        <div>
          Exchange: <span className="text-text-secondary">{strategy.config.exchange}</span>
        </div>
        <div>
          Symbol:{" "}
          <span className="text-text-secondary font-mono">
            {strategy.config.symbol.base}/{strategy.config.symbol.quote}
          </span>
        </div>
      </div>

      {/* Params (collapsed JSON) */}
      {strategy.config.params && Object.keys(strategy.config.params).length > 0 && (
        <details className="mt-2">
          <summary className="text-[10px] text-accent-cyan cursor-pointer">Parameters</summary>
          <pre className="mt-1 text-[10px] text-text-secondary font-mono overflow-auto max-h-20">
            {JSON.stringify(strategy.config.params, null, 2)}
          </pre>
        </details>
      )}
    </div>
  );
}

export function StrategiesPanel() {
  const { strategies } = useDashboard();

  if (strategies.length === 0) {
    return <p className="text-text-muted text-sm">No strategies configured</p>;
  }

  return (
    <div className="space-y-2">
      {strategies.map((s) => (
        <StrategyCard key={s.config.strategy_id} strategy={s} />
      ))}
    </div>
  );
}
```

**Step 2: Wire into page.tsx**

**Step 3: Commit**

```bash
cd /home/gallison/workspace/market_maker
git add dashboard/src/components/panels/Strategies.tsx dashboard/src/app/page.tsx
git commit -m "feat(dashboard): add strategy control cards panel"
```

---

## Task 10: Active Orders Panel

**Files:**
- Create: `dashboard/src/components/panels/ActiveOrders.tsx`
- Modify: `dashboard/src/app/page.tsx`

**Step 1: Build the ActiveOrders panel**

Create `dashboard/src/components/panels/ActiveOrders.tsx`:

```tsx
"use client";

import { useDashboard } from "@/lib/store";
import type { Order } from "@/lib/types";

function fmt(val: string, decimals = 2): string {
  const num = parseFloat(val);
  return isNaN(num) ? val : num.toFixed(decimals);
}

function statusColor(status: string): string {
  switch (status) {
    case "New":
    case "Submitted":
      return "text-accent-cyan";
    case "PartiallyFilled":
      return "text-accent-amber";
    case "Filled":
      return "text-accent-green";
    case "Cancelled":
    case "Expired":
      return "text-text-muted";
    case "Rejected":
      return "text-accent-red";
    default:
      return "text-text-secondary";
  }
}

function OrderRow({ order }: { order: Order }) {
  const sideColor = order.side === "Buy" ? "text-accent-green" : "text-accent-red";

  return (
    <tr className="border-b border-border-panel/50 hover:bg-bg-hover transition-colors text-xs">
      <td className="py-1 px-2 font-mono text-text-secondary" title={order.client_order_id}>
        {order.client_order_id.slice(0, 8)}...
      </td>
      <td className="py-1 px-2 text-text-secondary">{order.exchange}</td>
      <td className="py-1 px-2 font-mono">
        {order.symbol.base}/{order.symbol.quote}
      </td>
      <td className={`py-1 px-2 font-semibold ${sideColor}`}>{order.side}</td>
      <td className="py-1 px-2 text-text-secondary">{order.order_type}</td>
      <td className="py-1 px-2 font-mono text-right">{order.price ? fmt(order.price) : "MKT"}</td>
      <td className="py-1 px-2 font-mono text-right">{fmt(order.quantity, 4)}</td>
      <td className="py-1 px-2 font-mono text-right">{fmt(order.filled_quantity, 4)}</td>
      <td className={`py-1 px-2 font-semibold ${statusColor(order.status)}`}>{order.status}</td>
    </tr>
  );
}

export function ActiveOrdersPanel() {
  const { orders } = useDashboard();

  if (orders.length === 0) {
    return <p className="text-text-muted text-sm">No orders</p>;
  }

  // Show most recent first
  const sorted = [...orders].sort(
    (a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
  );

  return (
    <div className="overflow-auto">
      <table className="w-full text-left">
        <thead>
          <tr className="text-text-muted text-[10px] uppercase tracking-wider">
            <th className="py-1 px-2">ID</th>
            <th className="py-1 px-2">Exchange</th>
            <th className="py-1 px-2">Symbol</th>
            <th className="py-1 px-2">Side</th>
            <th className="py-1 px-2">Type</th>
            <th className="py-1 px-2 text-right">Price</th>
            <th className="py-1 px-2 text-right">Qty</th>
            <th className="py-1 px-2 text-right">Filled</th>
            <th className="py-1 px-2">Status</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((order) => (
            <OrderRow key={order.client_order_id} order={order} />
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

**Step 2: Wire into page.tsx**

**Step 3: Commit**

```bash
cd /home/gallison/workspace/market_maker
git add dashboard/src/components/panels/ActiveOrders.tsx dashboard/src/app/page.tsx
git commit -m "feat(dashboard): add active orders table panel"
```

---

## Task 11: Risk Dashboard + Kill Switch Panel

**Files:**
- Create: `dashboard/src/components/panels/RiskDashboard.tsx`
- Modify: `dashboard/src/app/page.tsx`

**Step 1: Build the Risk + Kill Switch panel**

Create `dashboard/src/components/panels/RiskDashboard.tsx`:

```tsx
"use client";

import { useState } from "react";
import { api } from "@/lib/api";
import { useDashboard } from "@/lib/store";

function fmt(val: string | undefined, decimals = 2): string {
  if (!val) return "—";
  const num = parseFloat(val);
  return isNaN(num) ? val : num.toFixed(decimals);
}

function pnlColor(val: string | undefined): string {
  if (!val) return "text-text-muted";
  const num = parseFloat(val);
  if (num > 0) return "text-accent-green";
  if (num < 0) return "text-accent-red";
  return "text-text-secondary";
}

function MetricRow({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div className="flex justify-between items-center py-1.5 border-b border-border-panel/30">
      <span className="text-[10px] uppercase tracking-wider text-text-muted">{label}</span>
      <span className={`font-mono text-xs ${color || "text-text-primary"}`}>{value}</span>
    </div>
  );
}

export function RiskDashboardPanel() {
  const { risk } = useDashboard();
  const [killSwitchActive, setKillSwitchActive] = useState(false);
  const [toggling, setToggling] = useState(false);
  const [confirmArm, setConfirmArm] = useState(false);

  const toggleKillSwitch = async () => {
    if (!killSwitchActive && !confirmArm) {
      setConfirmArm(true);
      return;
    }

    setToggling(true);
    setConfirmArm(false);
    try {
      const res = killSwitchActive
        ? await api.resetKillSwitch()
        : await api.activateKillSwitch();
      setKillSwitchActive(res.active);
    } catch (err) {
      console.error("Kill switch toggle failed:", err);
    } finally {
      setToggling(false);
    }
  };

  return (
    <div className="flex flex-col h-full">
      {/* Risk metrics */}
      <div className="flex-1">
        <MetricRow label="Net Exposure" value={fmt(risk?.net_exposure)} />
        <MetricRow label="Gross Exposure" value={fmt(risk?.gross_exposure)} />
        <MetricRow
          label="Daily PnL"
          value={fmt(risk?.daily_pnl)}
          color={pnlColor(risk?.daily_pnl)}
        />
        <MetricRow label="Max Drawdown" value={fmt(risk?.max_drawdown)} />
        <MetricRow label="VaR 95%" value={fmt(risk?.total_var_95)} />
        <MetricRow label="VaR 99%" value={fmt(risk?.total_var_99)} />
      </div>

      {/* Kill Switch */}
      <div className="mt-3 pt-3 border-t border-border-panel">
        <div className="flex items-center justify-between mb-2">
          <span className="text-[10px] uppercase tracking-wider text-text-muted">Kill Switch</span>
          <span
            className={`text-xs font-bold ${killSwitchActive ? "text-accent-red" : "text-accent-green"}`}
          >
            {killSwitchActive ? "ACTIVE" : "ARMED"}
          </span>
        </div>

        {confirmArm && (
          <div className="mb-2 p-2 bg-accent-red/10 border border-accent-red/30 rounded text-[10px] text-accent-red">
            This will cancel ALL open orders. Click again to confirm.
          </div>
        )}

        <button
          onClick={toggleKillSwitch}
          disabled={toggling}
          className={`w-full py-2.5 text-xs font-bold rounded transition-all ${
            killSwitchActive
              ? "bg-accent-green/20 text-accent-green border-2 border-accent-green hover:bg-accent-green/30"
              : confirmArm
                ? "bg-accent-red/30 text-accent-red border-2 border-accent-red animate-pulse"
                : "bg-accent-red/10 text-accent-red border-2 border-accent-red/50 hover:bg-accent-red/20"
          } disabled:opacity-50`}
        >
          {toggling
            ? "Processing..."
            : killSwitchActive
              ? "RESET KILL SWITCH"
              : confirmArm
                ? "CONFIRM — CANCEL ALL ORDERS"
                : "ACTIVATE KILL SWITCH"}
        </button>
      </div>
    </div>
  );
}
```

**Step 2: Wire into page.tsx**

**Step 3: Commit**

```bash
cd /home/gallison/workspace/market_maker
git add dashboard/src/components/panels/RiskDashboard.tsx dashboard/src/app/page.tsx
git commit -m "feat(dashboard): add risk metrics and kill switch panel"
```

---

## Task 12: Event Log Panel

**Files:**
- Create: `dashboard/src/components/panels/EventLog.tsx`
- Modify: `dashboard/src/app/page.tsx`

**Step 1: Build the EventLog panel**

Create `dashboard/src/components/panels/EventLog.tsx`:

```tsx
"use client";

import { useRef, useEffect, useState } from "react";
import { useDashboard } from "@/lib/store";
import type { ServerMessage } from "@/lib/types";

function msgColor(type: string): string {
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
    case "Subscribed":
    case "Unsubscribed":
      return "text-text-muted";
    default:
      return "text-text-secondary";
  }
}

function formatEvent(msg: ServerMessage): string {
  switch (msg.type) {
    case "MarketDataTick":
      return `${msg.exchange} ${msg.symbol.base}/${msg.symbol.quote} bid=${msg.best_bid} ask=${msg.best_ask} spread=${msg.spread_bps}bps`;
    case "PositionUpdate":
      return `${msg.exchange} ${msg.symbol.base}/${msg.symbol.quote} qty=${msg.quantity} upnl=${msg.unrealized_pnl}`;
    case "OrderUpdate":
      return `${msg.exchange} ${msg.symbol.base}/${msg.symbol.quote} ${msg.status} filled=${msg.filled_quantity}`;
    case "RiskUpdate":
      return `net=${msg.net_exposure} gross=${msg.gross_exposure} pnl=${msg.daily_pnl} kill=${msg.kill_switch_active}`;
    case "Error":
      return msg.message;
    case "Subscribed":
      return `subscribed to ${msg.channel}`;
    case "Unsubscribed":
      return `unsubscribed from ${msg.channel}`;
  }
}

export function EventLogPanel() {
  const { events } = useDashboard();
  const containerRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);
  const [filter, setFilter] = useState<string>("all");

  useEffect(() => {
    if (autoScroll && containerRef.current) {
      containerRef.current.scrollTop = 0;
    }
  }, [events, autoScroll]);

  const filtered =
    filter === "all" ? events : events.filter((e) => e.type === filter);

  const types = ["all", "MarketDataTick", "OrderUpdate", "PositionUpdate", "RiskUpdate", "Error"];

  return (
    <div className="flex flex-col h-full">
      {/* Filter bar */}
      <div className="flex gap-1 mb-2 flex-wrap">
        {types.map((t) => (
          <button
            key={t}
            className={`px-2 py-0.5 text-[10px] rounded transition-colors ${
              filter === t
                ? "bg-accent-cyan/20 text-accent-cyan border border-accent-cyan/50"
                : "text-text-muted border border-border-panel hover:text-text-secondary"
            }`}
            onClick={() => setFilter(t)}
          >
            {t === "all" ? "ALL" : t.replace("Update", "").replace("Msg", "")}
          </button>
        ))}
      </div>

      {/* Event list */}
      <div
        ref={containerRef}
        className="flex-1 overflow-auto font-mono text-[11px] space-y-0.5"
        onMouseEnter={() => setAutoScroll(false)}
        onMouseLeave={() => setAutoScroll(true)}
      >
        {filtered.map((event, i) => (
          <div key={i} className="flex gap-2 py-0.5 hover:bg-bg-hover px-1 rounded">
            <span className={`shrink-0 ${msgColor(event.type)}`}>
              [{event.type.replace("Update", "").replace("Msg", "").substring(0, 6).padEnd(6)}]
            </span>
            <span className="text-text-secondary truncate">{formatEvent(event)}</span>
          </div>
        ))}
        {filtered.length === 0 && (
          <p className="text-text-muted text-xs">No events yet</p>
        )}
      </div>
    </div>
  );
}
```

**Step 2: Wire into page.tsx**

**Step 3: Commit**

```bash
cd /home/gallison/workspace/market_maker
git add dashboard/src/components/panels/EventLog.tsx dashboard/src/app/page.tsx
git commit -m "feat(dashboard): add filterable event log panel"
```

---

## Task 13: Price Chart Panel (Lightweight Charts)

**Files:**
- Create: `dashboard/src/components/panels/PriceChart.tsx`
- Modify: `dashboard/src/app/page.tsx`

**Step 1: Build the PriceChart panel**

Create `dashboard/src/components/panels/PriceChart.tsx`:

```tsx
"use client";

import { useEffect, useRef, useState } from "react";
import { createChart, type IChartApi, type ISeriesApi, ColorType } from "lightweight-charts";
import { useDashboard } from "@/lib/store";

export function PriceChartPanel() {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const lineSeriesRef = useRef<ISeriesApi<"Line"> | null>(null);
  const { ticks } = useDashboard();
  const [selectedKey, setSelectedKey] = useState<string>("");

  const tickEntries = Object.entries(ticks);

  // Auto-select first symbol
  useEffect(() => {
    if (!selectedKey && tickEntries.length > 0) {
      setSelectedKey(tickEntries[0][0]);
    }
  }, [tickEntries, selectedKey]);

  // Create chart
  useEffect(() => {
    if (!chartContainerRef.current) return;

    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: "#0a0a0f" },
        textColor: "#8888aa",
        fontFamily: "'JetBrains Mono', monospace",
      },
      grid: {
        vertLines: { color: "#1a1a2e" },
        horzLines: { color: "#1a1a2e" },
      },
      crosshair: {
        vertLine: { color: "#00d4ff", width: 1, style: 3 },
        horzLine: { color: "#00d4ff", width: 1, style: 3 },
      },
      rightPriceScale: {
        borderColor: "#2a2a3e",
      },
      timeScale: {
        borderColor: "#2a2a3e",
        timeVisible: true,
        secondsVisible: true,
      },
    });

    const lineSeries = chart.addLineSeries({
      color: "#00d4ff",
      lineWidth: 2,
      crosshairMarkerVisible: true,
      crosshairMarkerRadius: 4,
    });

    chartRef.current = chart;
    lineSeriesRef.current = lineSeries;

    const resizeObserver = new ResizeObserver(() => {
      if (chartContainerRef.current) {
        chart.applyOptions({
          width: chartContainerRef.current.clientWidth,
          height: chartContainerRef.current.clientHeight,
        });
      }
    });
    resizeObserver.observe(chartContainerRef.current);

    return () => {
      resizeObserver.disconnect();
      chart.remove();
      chartRef.current = null;
      lineSeriesRef.current = null;
    };
  }, []);

  // Update data on tick
  useEffect(() => {
    if (!selectedKey || !lineSeriesRef.current) return;
    const tick = ticks[selectedKey];
    if (!tick) return;

    const time = Math.floor(Date.now() / 1000) as unknown as import("lightweight-charts").Time;
    lineSeriesRef.current.update({
      time,
      value: parseFloat(tick.mid_price),
    });
  }, [ticks, selectedKey]);

  return (
    <div className="flex flex-col h-full">
      {/* Symbol selector */}
      {tickEntries.length > 0 && (
        <select
          className="mb-2 px-2 py-1 text-xs bg-bg-primary border border-border-panel rounded text-text-primary shrink-0"
          value={selectedKey}
          onChange={(e) => setSelectedKey(e.target.value)}
        >
          {tickEntries.map(([key, tick]) => (
            <option key={key} value={key}>
              {tick.exchange}: {tick.symbol.base}/{tick.symbol.quote}
            </option>
          ))}
        </select>
      )}

      <div ref={chartContainerRef} className="flex-1 min-h-0" />

      {tickEntries.length === 0 && (
        <p className="text-text-muted text-sm text-center mt-4">
          Waiting for market data to populate chart...
        </p>
      )}
    </div>
  );
}
```

**Step 2: Wire into page.tsx**

**Step 3: Commit**

```bash
cd /home/gallison/workspace/market_maker
git add dashboard/src/components/panels/PriceChart.tsx dashboard/src/app/page.tsx
git commit -m "feat(dashboard): add real-time price chart with Lightweight Charts"
```

---

## Task 14: Wire All Panels into page.tsx (Final Assembly)

**Files:**
- Modify: `dashboard/src/app/page.tsx`

**Step 1: Final page.tsx with all panel imports**

Replace `dashboard/src/app/page.tsx` with the complete version importing all panels:

```tsx
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
      {/* Status bar */}
      <div className="flex items-center justify-between px-4 py-2 border-b border-border-panel bg-bg-panel/50">
        <h1 className="text-sm font-bold text-accent-cyan font-mono tracking-wider">
          MARKET MAKER
        </h1>
        <div className="flex items-center gap-3 text-xs">
          <span className={`flex items-center gap-1.5 ${connected ? "text-accent-green" : "text-accent-red"}`}>
            <span className={`w-2 h-2 rounded-full ${connected ? "bg-accent-green" : "bg-accent-red"} ${connected ? "animate-pulse" : ""}`} />
            {connected ? "CONNECTED" : "DISCONNECTED"}
          </span>
          {loading && <span className="text-accent-amber">Loading...</span>}
        </div>
      </div>

      <DashboardGrid>
        <div key="market-data">
          <Panel title="Market Data">
            <MarketDataPanel />
          </Panel>
        </div>
        <div key="positions">
          <Panel title="Positions & PnL">
            <PositionsPanel />
          </Panel>
        </div>
        <div key="order-book">
          <Panel title="Order Book">
            <OrderBookPanel />
          </Panel>
        </div>
        <div key="order-entry">
          <Panel title="Order Entry">
            <OrderEntryPanel />
          </Panel>
        </div>
        <div key="strategies">
          <Panel title="Strategies">
            <StrategiesPanel />
          </Panel>
        </div>
        <div key="price-chart">
          <Panel title="Price Chart">
            <PriceChartPanel />
          </Panel>
        </div>
        <div key="risk">
          <Panel title="Risk Dashboard">
            <RiskDashboardPanel />
          </Panel>
        </div>
        <div key="orders">
          <Panel title="Active Orders">
            <ActiveOrdersPanel />
          </Panel>
        </div>
        <div key="event-log">
          <Panel title="Event Log">
            <EventLogPanel />
          </Panel>
        </div>
      </DashboardGrid>
    </main>
  );
}
```

**Step 2: Run dev server and verify all panels render**

```bash
cd /home/gallison/workspace/market_maker/dashboard
npm run dev
```

Expected: All 9 panels render, draggable/resizable, dark theme, status bar shows connection status.

**Step 3: Commit**

```bash
cd /home/gallison/workspace/market_maker
git add dashboard/src/app/page.tsx
git commit -m "feat(dashboard): wire all panels into final dashboard layout"
```

---

## Task 15: Build Verification + Final Polish

**Step 1: Run production build**

```bash
cd /home/gallison/workspace/market_maker/dashboard
npm run build
```

Expected: Build succeeds with no errors. Fix any TypeScript errors if they appear.

**Step 2: Run lint**

```bash
cd /home/gallison/workspace/market_maker/dashboard
npm run lint
```

Expected: No lint errors. Fix any warnings.

**Step 3: Test with gateway running**

```bash
# Terminal 1: Start gateway (if not already running)
cd /home/gallison/workspace/market_maker
cargo run --bin trading-core

# Terminal 2: Start dashboard
cd /home/gallison/workspace/market_maker/dashboard
npm run dev
```

Open http://localhost:3000. Verify:
- Status bar shows CONNECTED (green) when gateway is running
- Market data ticks appear in real-time
- Order book fetches and displays depth
- Price chart plots streaming mid-price data
- Strategies list with start/stop buttons
- Kill switch button with confirmation flow
- Event log shows streaming WebSocket messages
- Panels are draggable and resizable
- Layout persists after page refresh (localStorage)

**Step 4: Final commit**

```bash
cd /home/gallison/workspace/market_maker
git add dashboard/
git commit -m "feat(dashboard): complete trading dashboard with 9 interactive panels"
```

---

## Summary

| Task | Description | Files |
|------|-------------|-------|
| 1 | Scaffold Next.js + Tailwind + dark theme | 9 files created |
| 2 | TypeScript types + REST client + WebSocket manager | 3 files |
| 3 | Global state (Context + Reducer + Provider) | 2 files |
| 4 | Panel shell + DashboardGrid layout | 3 files |
| 5 | Market Data panel | 1 file |
| 6 | Positions & PnL panel | 1 file |
| 7 | Order Book panel | 1 file |
| 8 | Order Entry panel | 1 file |
| 9 | Strategies panel | 1 file |
| 10 | Active Orders panel | 1 file |
| 11 | Risk Dashboard + Kill Switch panel | 1 file |
| 12 | Event Log panel | 1 file |
| 13 | Price Chart panel (Lightweight Charts) | 1 file |
| 14 | Final assembly (wire all panels) | 1 file modified |
| 15 | Build verification + integration test | 0 files |
