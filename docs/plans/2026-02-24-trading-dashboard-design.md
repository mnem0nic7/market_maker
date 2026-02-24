# Trading Dashboard Design

**Date**: 2026-02-24
**Status**: Approved

## Overview

Full trading desk dashboard — monitor positions, manage orders, control strategies, view charts, and operate the kill switch. Standalone Next.js application that connects to the existing Rust gateway API.

## Tech Stack

| Layer | Choice | Rationale |
|-------|--------|-----------|
| Framework | Next.js 14 (App Router) + TypeScript | File-based routing, large ecosystem, SSR-capable |
| Styling | TailwindCSS | Dark terminal theme, utility-first, fast iteration |
| Financial Charts | TradingView Lightweight Charts | Candlesticks, streaming updates, industry standard |
| General Charts | Recharts | Equity curves, PnL bars, composable React components |
| Panel Layout | react-grid-layout | Draggable + resizable grid with localStorage persistence |
| State | React Context + useReducer | Simple, no extra dependencies, sufficient for this scope |

## Visual Style

Dark terminal aesthetic optimized for extended trading sessions.

- Background: `#0a0a0f` (near-black)
- Panel background: `#1a1a2e` with `#2a2a3e` borders
- Profit / Bids: `#00ff88` (green)
- Loss / Asks: `#ff4444` (red)
- Accent / Links: `#00d4ff` (cyan)
- Warning: `#ffaa00` (amber)
- Numbers: monospace font (JetBrains Mono or system monospace)
- Text: Inter or system sans-serif

## Layout

Draggable/resizable floating panels via react-grid-layout. Users can rearrange panels freely. Layout state persists to localStorage.

### Panels

1. **Market Data** — Live tickers: symbol, bid, ask, last, spread, 24h change. WebSocket-driven updates.
2. **Price Chart** — Lightweight Charts candlestick chart for the selected symbol. Real-time streaming via WebSocket market_data channel.
3. **Order Book** — Bid/ask depth ladder for the selected symbol. Visual depth bars.
4. **Positions & PnL** — Table of open positions (symbol, side, qty, entry, unrealized PnL). Equity curve via Recharts line chart.
5. **Order Entry** — Form: exchange, symbol, side (buy/sell), order type (limit/market), price, quantity. Submit via REST POST /api/v1/orders.
6. **Strategies** — Cards per strategy: name, status indicator (running/stopped), start/stop buttons, parameter editing (inline JSON or form fields). REST calls to /api/v1/strategies/{id}/start|stop.
7. **Active Orders** — Sortable table: order ID, exchange, symbol, side, type, price, qty, status, filled. Cancel buttons per order.
8. **Risk Dashboard** — Current risk metrics (gross exposure, daily PnL, drawdown). Kill switch: prominent red button, arm/disarm via REST POST/DELETE /api/v1/risk/kill-switch.
9. **Event Log** — Scrolling feed of all WebSocket messages. Filterable by type. Auto-scroll with pause-on-hover.

## Data Flow

### WebSocket (real-time)
- Connect to `ws://localhost:8080/api/v1/ws`
- Send: `{"action": "subscribe", "channel": "all"}`
- Receive: PositionUpdate, OrderUpdate, RiskUpdate, MarketDataTick messages
- Auto-reconnect with exponential backoff

### REST (actions + initial state)
- GET /api/v1/health — connection status
- GET /api/v1/positions — initial positions
- GET /api/v1/orders — initial orders
- GET /api/v1/strategies — strategy list
- GET /api/v1/portfolio/risk — risk metrics
- GET /api/v1/market-data/{exchange}/{symbol}/orderbook — order book snapshot
- POST /api/v1/orders — place order (JSON body: exchange, symbol, side, order_type, price, quantity)
- POST /api/v1/strategies/{id}/start — start strategy
- POST /api/v1/strategies/{id}/stop — stop strategy
- POST /api/v1/risk/kill-switch — arm kill switch
- DELETE /api/v1/risk/kill-switch — disarm kill switch

### State Management
- React Context + useReducer at root level
- WebSocket messages dispatched as reducer actions
- REST responses used for initial hydration
- Optimistic updates for user actions (order placement, strategy toggle)

## Project Structure

```
dashboard/
├── src/
│   ├── app/
│   │   ├── layout.tsx              # Root layout, fonts, providers
│   │   ├── page.tsx                # Main dashboard (DashboardGrid)
│   │   └── globals.css             # Tailwind imports + dark theme vars
│   ├── components/
│   │   ├── panels/
│   │   │   ├── MarketData.tsx      # Live ticker table
│   │   │   ├── PriceChart.tsx      # Lightweight Charts candlestick
│   │   │   ├── OrderBook.tsx       # Bid/ask depth ladder
│   │   │   ├── Positions.tsx       # Position table + equity curve
│   │   │   ├── OrderEntry.tsx      # Order submission form
│   │   │   ├── Strategies.tsx      # Strategy cards with controls
│   │   │   ├── ActiveOrders.tsx    # Order table with cancel
│   │   │   ├── RiskDashboard.tsx   # Risk metrics + kill switch
│   │   │   └── EventLog.tsx        # Scrolling event feed
│   │   ├── layout/
│   │   │   └── DashboardGrid.tsx   # react-grid-layout wrapper
│   │   └── ui/                     # Shared primitives (Badge, Button, etc.)
│   └── lib/
│       ├── api.ts                  # REST client (fetch wrapper)
│       ├── websocket.ts            # WebSocket manager (connect, reconnect, parse)
│       ├── types.ts                # TypeScript types matching Rust API
│       └── store.ts                # Context + reducer + provider
├── public/
├── package.json
├── tailwind.config.ts
├── tsconfig.json
└── next.config.js
```

## Location

`dashboard/` at repository root, alongside `bins/` and `crates/`.

## Running

```bash
cd dashboard
npm install
npm run dev        # Development server on localhost:3000
                   # Connects to gateway at localhost:8080
```

## Gateway CORS

The existing gateway already has permissive CORS (any origin). The Next.js dev server on :3000 can call the gateway on :8080 without issues.
