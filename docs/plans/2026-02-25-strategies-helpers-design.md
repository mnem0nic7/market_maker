# Strategies, Helpers & AI Assistant — Design Document

**Goal:** Expand the market maker platform with 5 new trading strategies, guided dashboard setup with live metrics, parameter optimization tools, and an AI chat assistant powered by Claude.

**Architecture:** Extends the existing `Strategy` trait system with new implementations, adds CRUD REST endpoints for runtime strategy management, introduces per-strategy metrics tracking with WebSocket broadcast, builds a backtester parameter sweep tool, and adds a Claude-powered AI chat panel to the dashboard.

**Tech Stack:** Rust (strategies, metrics, optimization, Claude API client), Next.js/React (dashboard panels), Anthropic Claude API (AI assistant), SSE for streaming responses.

---

## 1. New Strategies

Five new strategies added to `crates/mm-strategy/`, all implementing the existing `Strategy` trait.

### TWAP (Time-Weighted Average Price)
- **Purpose:** Execute a target quantity over a fixed time window in equal-sized slices.
- **Parameters:** `target_quantity`, `duration_secs`, `slice_interval_secs`, `side` (buy/sell), `max_deviation_bps` (price limit vs. mid).
- **Logic:** On each tick, if current slice interval has elapsed and remaining quantity > 0, place a limit order at mid +/- max_deviation_bps. Track fills, adjust remaining quantity.

### VWAP (Volume-Weighted Average Price)
- **Purpose:** Like TWAP but sizes slices proportional to a historical volume profile.
- **Parameters:** `target_quantity`, `duration_secs`, `side`, `volume_profile` (array of hourly volume weights or "auto" to derive from recent trades), `max_deviation_bps`.
- **Logic:** Pre-compute slice sizes from volume profile. Heavier slices during historically high-volume periods. Otherwise same execution logic as TWAP.

### Momentum
- **Purpose:** Trade in the direction of recent price trends.
- **Parameters:** `lookback_periods`, `entry_threshold_bps` (momentum above this → enter long, below negative → enter short), `exit_threshold_bps`, `order_size`, `trailing_stop_bps`, `max_position`.
- **Logic:** Track rolling returns over lookback window. Enter when momentum exceeds threshold. Exit on mean reversion or trailing stop hit. One position at a time.

### Mean Reversion
- **Purpose:** Trade against price extremes using Bollinger Band-style logic.
- **Parameters:** `window_periods`, `num_std_dev` (band width), `order_size`, `max_position`, `exit_at_mean` (bool).
- **Logic:** Compute rolling mean and standard deviation. Buy when price < lower band, sell when price > upper band. Exit when price reverts to mean (or opposite band).

### Statistical Arbitrage
- **Purpose:** Trade the spread between two correlated instruments across exchanges.
- **Parameters:** `leg_a` (exchange + symbol), `leg_b` (exchange + symbol), `lookback_periods`, `entry_z_score`, `exit_z_score`, `order_size`, `max_position`.
- **Logic:** Track price ratio or spread between leg_a and leg_b. Compute rolling z-score. Enter when |z| > entry threshold (buy underpriced, sell overpriced). Exit when |z| < exit threshold. Requires market data from both instruments.

---

## 2. Guided Strategy Setup (Dashboard)

### Strategy Creation Form
- "New Strategy" button at top of Strategies panel.
- **Step 1:** Strategy type dropdown (all 8 types with brief description).
- **Step 2:** Exchange + symbol selection (populated from live market data ticks).
- **Step 3:** Auto-generated parameter form based on strategy type. Each field has: label, input, default value, min/max validation, tooltip.
- **Presets:** "Conservative", "Moderate", "Aggressive" buttons that populate parameters.
- Submit calls `POST /api/v1/strategies` to create and optionally start.

### Strategy CRUD
- **Edit:** Modify parameters of a stopped strategy (inline form).
- **Clone:** Duplicate strategy with a new auto-generated ID.
- **Delete:** Remove with confirmation dialog.

### REST Endpoints
- `POST /api/v1/strategies` — Create strategy from JSON config body. Instantiates and registers with StrategyEngine.
- `PUT /api/v1/strategies/{id}` — Update parameters (strategy must be stopped).
- `DELETE /api/v1/strategies/{id}` — Remove strategy (must be stopped first).

### Runtime Management
- `StrategyEngine` gets new methods: `add_strategy_from_config()`, `update_strategy_config()`, `remove_strategy()`.
- Strategy configs persisted to a `strategies.json` file in the data directory so they survive restarts.

---

## 3. Live Strategy Metrics

### Metrics Struct
```
StrategyMetrics {
    strategy_id: String,
    realized_pnl: Decimal,
    unrealized_pnl: Decimal,
    total_fills: u64,
    total_orders_placed: u64,
    total_orders_cancelled: u64,
    total_orders_rejected: u64,
    avg_spread_captured_bps: Decimal,  // market-making strategies only
    volume_traded: Decimal,
    uptime_secs: u64,
    last_fill_at: Option<DateTime>,
}
```

### Collection
- `StrategyMetricsTracker` in `mm-core`: one tracker per strategy, updated by the main loop.
- On fill: update realized_pnl, total_fills, volume_traded, avg_spread_captured.
- On order placed/cancelled/rejected: increment counters.
- Unrealized PnL computed from current position * (mark_price - avg_entry).

### Broadcast
- New `ServerMessage::StrategyMetrics { strategy_id, metrics }` variant.
- Published every tick interval alongside market data ticks.
- Dashboard reducer handles `StrategyMetrics` messages, stores in state keyed by strategy_id.

### Dashboard Display
- Each strategy card shows a live metrics row: PnL (green/red), fill count, volume, uptime.
- Expandable detail view with all metrics.

### REST
- `GET /api/v1/strategies/{id}/metrics` — Snapshot of current metrics for one strategy.

---

## 4. Parameter Optimization

### Backend: ParameterOptimizer
- New module in `crates/mm-backtest/optimizer.rs`.
- Takes: strategy type, parameter ranges (each as a list of values), historical data config, optimization metric (Sharpe, Sortino, PnL, etc.).
- Runs grid search: iterate all combinations, run `ReplayEngine` for each, collect `BacktestResult`.
- Returns sorted results with top N parameter sets.

### CLI Extension
- `bins/backtester` gets `--optimize` mode.
- Flags: `--param-ranges` (JSON of param name → [values]), `--metric` (default: sharpe), `--top-n` (default: 10).
- Outputs ranked results table.

### REST Endpoint
- `POST /api/v1/backtest/optimize` — Accepts strategy type, parameter ranges, data source config.
- Runs asynchronously (tokio::spawn). Returns a job ID.
- `GET /api/v1/backtest/optimize/{job_id}` — Poll for results (or stream via WebSocket).
- Progress updates broadcast via `ServerMessage::OptimizationProgress { job_id, completed, total, best_so_far }`.

### Dashboard Integration
- "Optimize" button on each strategy card → opens modal.
- Parameter range inputs (from/to/step for each parameter).
- Data source selection (synthetic, or date range for historical).
- Progress bar during sweep.
- Results table: sortable by any metric. "Apply" button copies best params to strategy config.

---

## 5. AI Chat Assistant

### Dashboard Panel
- New "AI Assistant" panel in the dashboard grid.
- Chat interface: scrollable message list (user + assistant), markdown rendering, input box at bottom.
- Context indicator: shows what data the assistant can see (positions, strategies, market data).
- Action buttons: when the assistant suggests a config change, it includes an "Apply" button that calls the appropriate REST endpoint.

### Backend
- New module `crates/mm-gateway/src/assistant.rs`.
- `POST /api/v1/assistant/chat` — Accepts `{ messages: [{ role, content }] }`.
- Collects system context:
  - Current positions and PnL
  - Active strategy configs, status, and metrics
  - Recent market data (latest ticks, spreads)
  - Risk state (exposure, kill switch status)
  - Available strategy types and their parameter descriptions
- Builds Claude API request with system prompt + context + user messages.
- Streams response back via SSE (`text/event-stream`).

### Claude Integration
- Uses Anthropic Messages API directly (HTTP client, no SDK needed in Rust).
- Model: `claude-sonnet-4-20250514` (fast, capable, cost-effective for assistant use).
- System prompt describes the platform, all strategy types and parameters, and instructs Claude to:
  - Explain strategies and parameters in plain language
  - Suggest parameter values based on market conditions and user goals
  - Help interpret metrics and performance data
  - Warn about risk considerations
  - Format suggestions as structured JSON when proposing config changes

### Configuration
- `MM_ASSISTANT__ANTHROPIC_API_KEY` env var (required to enable).
- `MM_ASSISTANT__MODEL` env var (optional, defaults to claude-sonnet).
- Panel hidden in dashboard if assistant endpoint returns 503 (no API key configured).

---

## Implementation Order

Given the scope, recommended build sequence:

1. **New strategies** (Rust only, no dashboard changes) — foundation
2. **Strategy CRUD endpoints** + runtime management — enables dashboard setup
3. **Guided strategy setup** (dashboard forms, presets) — user-facing setup
4. **Strategy metrics tracking** + WebSocket broadcast — live data
5. **Dashboard metrics display** — user-facing metrics
6. **Parameter optimizer** (backend + CLI) — tooling
7. **Dashboard optimization modal** — user-facing optimization
8. **AI assistant backend** (Claude API integration) — chat backend
9. **AI assistant panel** (dashboard) — user-facing chat
