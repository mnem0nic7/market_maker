# Strategies, Helpers & AI Assistant — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add 5 new trading strategies, guided dashboard setup with CRUD, live per-strategy metrics, parameter optimization, and a Claude-powered AI chat assistant.

**Architecture:** Extends the existing `Strategy` trait system with 5 new implementations (TWAP, VWAP, Momentum, Mean Reversion, Statistical Arbitrage). Adds CRUD REST endpoints for runtime strategy management. Introduces per-strategy metrics tracking with WebSocket broadcast. Builds a backtester parameter sweep tool. Adds a Claude-powered AI chat panel to the dashboard with SSE streaming.

**Tech Stack:** Rust (strategies, metrics, optimization, Claude API client), Next.js/React (dashboard panels), Anthropic Claude API (AI assistant), SSE for streaming responses.

---

## Task 1: TWAP Strategy

**Files:**
- Create: `crates/mm-strategy/src/twap.rs`
- Modify: `crates/mm-strategy/src/lib.rs`
- Modify: `crates/mm-strategy/Cargo.toml` (if new deps needed)

**Step 1: Write the TWAP config and struct**

```rust
// crates/mm-strategy/src/twap.rs

use chrono::{DateTime, Utc};
use mm_core::events::OrderUpdate;
use mm_core::traits::Strategy;
use mm_core::types::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TwapConfig {
    pub strategy_id: String,
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub target_quantity: Decimal,
    pub duration_secs: u64,
    pub slice_interval_secs: u64,
    pub side: Side,
    pub max_deviation_bps: Decimal,
}

pub struct TwapStrategy {
    config: TwapConfig,
    active: bool,
    start_time: Option<DateTime<Utc>>,
    last_slice_time: Option<DateTime<Utc>>,
    filled_quantity: Decimal,
    slices_sent: u64,
    total_slices: u64,
    slice_size: Decimal,
}
```

**Step 2: Implement the Strategy trait**

```rust
impl TwapStrategy {
    pub fn new(config: TwapConfig) -> Self {
        let total_slices = config.duration_secs / config.slice_interval_secs;
        let slice_size = if total_slices > 0 {
            config.target_quantity / Decimal::from(total_slices)
        } else {
            config.target_quantity
        };
        Self {
            config, active: true, start_time: None, last_slice_time: None,
            filled_quantity: Decimal::ZERO, slices_sent: 0, total_slices, slice_size,
        }
    }
}

impl Strategy for TwapStrategy {
    fn name(&self) -> &str { "twap" }
    fn strategy_id(&self) -> &str { &self.config.strategy_id }
    fn is_active(&self) -> bool { self.active }
    fn stop(&mut self) -> Vec<OrderAction> {
        self.active = false;
        vec![OrderAction::CancelAll(CancelAllOrders {
            exchange: self.config.exchange,
            symbol: self.config.symbol.clone(),
            strategy_id: self.config.strategy_id.clone(),
        })]
    }

    fn on_tick(&mut self, snapshot: &MarketSnapshot) -> Vec<OrderAction> {
        // Check if time to send next slice
        // Compute limit price = mid +/- max_deviation_bps
        // Place limit order for slice_size (or remaining if last slice)
        // Return vec![OrderAction::Place(order)]
    }

    fn on_fill(&mut self, fill: &Fill) {
        self.filled_quantity += fill.quantity;
        if self.filled_quantity >= self.config.target_quantity {
            self.active = false;
        }
    }

    fn on_order_update(&mut self, _update: &OrderUpdate) {}
}
```

**Step 3: Register module in lib.rs**

Add to `crates/mm-strategy/src/lib.rs`:
```rust
pub mod twap;
pub use twap::{TwapConfig, TwapStrategy};
```

**Step 4: Write tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    // Test: new() computes correct slice_size and total_slices
    // Test: on_tick places order when interval elapsed
    // Test: on_tick does nothing when interval not elapsed
    // Test: on_fill tracks filled_quantity, deactivates when complete
    // Test: stop() cancels all and deactivates
}
```

**Step 5: Run tests**

Run: `cargo test -p mm-strategy -- twap`
Expected: All PASS

**Step 6: Commit**

```bash
git add crates/mm-strategy/src/twap.rs crates/mm-strategy/src/lib.rs
git commit -m "feat(strategy): add TWAP strategy"
```

---

## Task 2: VWAP Strategy

**Files:**
- Create: `crates/mm-strategy/src/vwap.rs`
- Modify: `crates/mm-strategy/src/lib.rs`

**Step 1: Write the VWAP config and struct**

```rust
// crates/mm-strategy/src/vwap.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VwapConfig {
    pub strategy_id: String,
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub target_quantity: Decimal,
    pub duration_secs: u64,
    pub side: Side,
    pub volume_profile: Vec<Decimal>,  // hourly weights, normalized to sum=1
    pub max_deviation_bps: Decimal,
}

pub struct VwapStrategy {
    config: VwapConfig,
    active: bool,
    start_time: Option<DateTime<Utc>>,
    filled_quantity: Decimal,
    current_bucket: usize,
    bucket_sizes: Vec<Decimal>,  // pre-computed from volume_profile * target_quantity
    last_bucket_time: Option<DateTime<Utc>>,
    bucket_interval_secs: u64,
}
```

**Step 2: Implement Strategy trait**

Same pattern as TWAP, but:
- `new()` pre-computes `bucket_sizes` from volume profile weights
- `on_tick()` determines current time bucket, computes slice size from `bucket_sizes[current_bucket]`
- Heavier slices during high-volume periods

**Step 3: Register module, write tests, commit**

Same pattern as Task 1.

Run: `cargo test -p mm-strategy -- vwap`

```bash
git add crates/mm-strategy/src/vwap.rs crates/mm-strategy/src/lib.rs
git commit -m "feat(strategy): add VWAP strategy"
```

---

## Task 3: Momentum Strategy

**Files:**
- Create: `crates/mm-strategy/src/momentum.rs`
- Modify: `crates/mm-strategy/src/lib.rs`

**Step 1: Write config and struct**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MomentumConfig {
    pub strategy_id: String,
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub lookback_periods: usize,
    pub entry_threshold_bps: Decimal,
    pub exit_threshold_bps: Decimal,
    pub order_size: Decimal,
    pub trailing_stop_bps: Decimal,
    pub max_position: Decimal,
}

pub struct MomentumStrategy {
    config: MomentumConfig,
    active: bool,
    price_history: VecDeque<Decimal>,
    current_position_side: Option<Side>,
    entry_price: Option<Decimal>,
    highest_since_entry: Option<Decimal>,
    lowest_since_entry: Option<Decimal>,
    live_order_ids: HashSet<String>,
}
```

**Step 2: Implement Strategy trait**

- `on_tick()`: Maintain rolling price window. Compute momentum = (current - oldest) / oldest * 10000 bps. Enter long if momentum > entry_threshold, short if < -entry_threshold. Exit via trailing stop or mean reversion.
- `on_fill()`: Update position tracking, set entry_price and trailing-stop anchors.

**Step 3: Register, test, commit**

Run: `cargo test -p mm-strategy -- momentum`

```bash
git add crates/mm-strategy/src/momentum.rs crates/mm-strategy/src/lib.rs
git commit -m "feat(strategy): add Momentum strategy"
```

---

## Task 4: Mean Reversion Strategy

**Files:**
- Create: `crates/mm-strategy/src/mean_reversion.rs`
- Modify: `crates/mm-strategy/src/lib.rs`

**Step 1: Write config and struct**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeanReversionConfig {
    pub strategy_id: String,
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub window_periods: usize,
    pub num_std_dev: Decimal,
    pub order_size: Decimal,
    pub max_position: Decimal,
    pub exit_at_mean: bool,
}

pub struct MeanReversionStrategy {
    config: MeanReversionConfig,
    active: bool,
    price_history: VecDeque<Decimal>,
    current_position_side: Option<Side>,
    live_order_ids: HashSet<String>,
}
```

**Step 2: Implement Strategy trait**

- `on_tick()`: Maintain rolling window. Compute mean and std_dev. Lower band = mean - num_std_dev * std. Upper band = mean + num_std_dev * std. Buy when price < lower band, sell when price > upper band. Exit at mean (if configured) or opposite band.
- Helper: `fn rolling_stats(prices: &VecDeque<Decimal>) -> (Decimal, Decimal)` computes mean and standard deviation.

**Step 3: Register, test, commit**

Run: `cargo test -p mm-strategy -- mean_reversion`

```bash
git add crates/mm-strategy/src/mean_reversion.rs crates/mm-strategy/src/lib.rs
git commit -m "feat(strategy): add Mean Reversion strategy"
```

---

## Task 5: Statistical Arbitrage Strategy

**Files:**
- Create: `crates/mm-strategy/src/stat_arb.rs`
- Modify: `crates/mm-strategy/src/lib.rs`

**Step 1: Write config and struct**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatArbConfig {
    pub strategy_id: String,
    pub leg_a_exchange: Exchange,
    pub leg_a_symbol: Symbol,
    pub leg_b_exchange: Exchange,
    pub leg_b_symbol: Symbol,
    pub lookback_periods: usize,
    pub entry_z_score: Decimal,
    pub exit_z_score: Decimal,
    pub order_size: Decimal,
    pub max_position: Decimal,
}

pub struct StatArbStrategy {
    config: StatArbConfig,
    active: bool,
    spread_history: VecDeque<Decimal>,
    leg_a_price: Option<Decimal>,
    leg_b_price: Option<Decimal>,
    position_open: bool,
    position_side: Option<Side>,  // side of leg_a
    live_order_ids: HashSet<String>,
}
```

**Step 2: Implement Strategy trait**

- `on_tick()`: Update price for matching leg. When both legs have prices, compute spread = leg_a - leg_b. Compute z-score from rolling mean/std of spread. Enter when |z| > entry_z_score (buy underpriced leg, sell overpriced). Exit when |z| < exit_z_score.
- Note: This strategy needs ticks from two instruments. On each tick, check which leg matches and update accordingly. Only trade when both legs have recent data.

**Step 3: Register, test, commit**

Run: `cargo test -p mm-strategy -- stat_arb`

```bash
git add crates/mm-strategy/src/stat_arb.rs crates/mm-strategy/src/lib.rs
git commit -m "feat(strategy): add Statistical Arbitrage strategy"
```

---

## Task 6: Wire New Strategies into Trading Core

**Files:**
- Modify: `bins/trading-core/src/main.rs` (strategy factory section, ~lines 271-400)
- Modify: `bins/backtester/src/main.rs` (strategy factory)

**Step 1: Update trading-core strategy factory**

In the match block that constructs strategies from config, add arms for the 5 new types:

```rust
"twap" => {
    let cfg: TwapConfig = serde_json::from_value(strategy_cfg.params.clone())?;
    Box::new(TwapStrategy::new(cfg))
}
"vwap" => { /* similar */ }
"momentum" => { /* similar */ }
"mean_reversion" => { /* similar */ }
"stat_arb" => { /* similar */ }
```

**Step 2: Update backtester strategy factory**

Same pattern in `bins/backtester/src/main.rs` `create_strategy()` function.

**Step 3: Run tests**

Run: `cargo check --workspace` (type-check)
Run: `cargo test --workspace` (all tests pass)

**Step 4: Commit**

```bash
git add bins/trading-core/src/main.rs bins/backtester/src/main.rs
git commit -m "feat: wire 5 new strategies into trading-core and backtester"
```

---

## Task 7: Strategy CRUD REST Endpoints

**Files:**
- Modify: `crates/mm-gateway/src/rest.rs`
- Modify: `crates/mm-core/src/types.rs` (if StrategyConfig needs changes)

**Step 1: Add POST /api/v1/strategies endpoint**

```rust
async fn create_strategy(
    State(state): State<Arc<ApiState>>,
    Json(config): Json<StrategyConfig>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiResponse<()>>)> {
    // Validate strategy_type is known
    // Check for duplicate strategy_id
    // Create StrategyStatus { config, running: false, started_at: None, stopped_at: None }
    // Push to state.strategies
    // Return 201 Created
}
```

**Step 2: Add PUT /api/v1/strategies/{id} endpoint**

```rust
async fn update_strategy(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    Json(config): Json<StrategyConfig>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiResponse<()>>)> {
    // Find strategy by id
    // Reject if running (must stop first)
    // Replace config
    // Return ok
}
```

**Step 3: Add DELETE /api/v1/strategies/{id} endpoint**

```rust
async fn delete_strategy(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiResponse<()>>)> {
    // Find strategy by id
    // Reject if running
    // Remove from vec
    // Return ok
}
```

**Step 4: Register routes in rest_router()**

```rust
.route("/api/v1/strategies", post(create_strategy))
.route("/api/v1/strategies/{id}", axum::routing::put(update_strategy))
.route("/api/v1/strategies/{id}", delete(delete_strategy))
```

**Step 5: Write tests**

```rust
#[tokio::test]
async fn create_strategy_returns_201() { /* POST with valid config */ }

#[tokio::test]
async fn update_stopped_strategy_returns_ok() { /* PUT on stopped strategy */ }

#[tokio::test]
async fn update_running_strategy_returns_409() { /* PUT on running → conflict */ }

#[tokio::test]
async fn delete_strategy_returns_ok() { /* DELETE on stopped strategy */ }
```

**Step 6: Run tests, commit**

Run: `cargo test -p mm-gateway`

```bash
git add crates/mm-gateway/src/rest.rs
git commit -m "feat(gateway): add strategy CRUD endpoints (POST, PUT, DELETE)"
```

---

## Task 8: Dashboard Strategy Creation Form

**Files:**
- Create: `dashboard/src/components/panels/StrategyForm.tsx`
- Modify: `dashboard/src/lib/api.ts` (add CRUD methods)
- Modify: `dashboard/src/lib/types.ts` (add strategy param types)
- Modify: `dashboard/src/components/Dashboard.tsx` (add panel to grid)

**Step 1: Add API methods**

In `dashboard/src/lib/api.ts`:
```typescript
createStrategy: (config: StrategyConfig) =>
  fetchApi<string>("/strategies", { method: "POST", body: JSON.stringify(config) }),
updateStrategy: (id: string, config: StrategyConfig) =>
  fetchApi<string>(`/strategies/${id}`, { method: "PUT", body: JSON.stringify(config) }),
deleteStrategy: (id: string) =>
  fetchApi<string>(`/strategies/${id}`, { method: "DELETE" }),
```

**Step 2: Add strategy type definitions**

In `dashboard/src/lib/types.ts`, add parameter type definitions for each strategy type with their fields, defaults, and validation ranges.

**Step 3: Create StrategyForm component**

Build a multi-step form component:
1. Strategy type dropdown (8 types with descriptions)
2. Exchange + symbol selector (from live ticks)
3. Auto-generated parameter form based on type
4. Preset buttons: Conservative / Moderate / Aggressive
5. Submit calls `api.createStrategy()`

Use controlled form state with validation.

**Step 4: Add to Dashboard grid**

Register the StrategyForm as a new panel in the dashboard grid layout.

**Step 5: Test manually via Playwright**

Start trading-core + dashboard, navigate to form, verify rendering and submission.

**Step 6: Commit**

```bash
git add dashboard/src/components/panels/StrategyForm.tsx dashboard/src/lib/api.ts dashboard/src/lib/types.ts dashboard/src/components/Dashboard.tsx
git commit -m "feat(dashboard): add strategy creation form with presets"
```

---

## Task 9: Strategy Metrics Backend

**Files:**
- Create: `crates/mm-core/src/metrics_tracker.rs` (or add to existing)
- Modify: `crates/mm-gateway/src/ws.rs` (new ServerMessage variant)
- Modify: `crates/mm-gateway/src/rest.rs` (metrics endpoint)
- Modify: `bins/trading-core/src/main.rs` (metrics collection in main loop)

**Step 1: Define StrategyMetrics struct**

In `crates/mm-core/`:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyMetrics {
    pub strategy_id: String,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub total_fills: u64,
    pub total_orders_placed: u64,
    pub total_orders_cancelled: u64,
    pub total_orders_rejected: u64,
    pub avg_spread_captured_bps: Decimal,
    pub volume_traded: Decimal,
    pub uptime_secs: u64,
    pub last_fill_at: Option<DateTime<Utc>>,
}
```

**Step 2: Create StrategyMetricsTracker**

```rust
pub struct StrategyMetricsTracker {
    metrics: HashMap<String, StrategyMetrics>,
}

impl StrategyMetricsTracker {
    pub fn new() -> Self { ... }
    pub fn on_fill(&mut self, strategy_id: &str, fill: &Fill) { ... }
    pub fn on_order_placed(&mut self, strategy_id: &str) { ... }
    pub fn on_order_cancelled(&mut self, strategy_id: &str) { ... }
    pub fn on_order_rejected(&mut self, strategy_id: &str) { ... }
    pub fn update_unrealized_pnl(&mut self, strategy_id: &str, pnl: Decimal) { ... }
    pub fn get(&self, strategy_id: &str) -> Option<&StrategyMetrics> { ... }
    pub fn all(&self) -> &HashMap<String, StrategyMetrics> { ... }
}
```

**Step 3: Add ServerMessage::StrategyMetrics variant**

In `crates/mm-gateway/src/ws.rs`:
```rust
StrategyMetrics {
    strategy_id: String,
    metrics: mm_core::StrategyMetrics,
}
```

**Step 4: Add REST endpoint**

`GET /api/v1/strategies/{id}/metrics` — returns current metrics snapshot for one strategy.

**Step 5: Wire into main loop**

In `bins/trading-core/src/main.rs`, create a `StrategyMetricsTracker`, update on fills/orders, broadcast metrics on each tick.

**Step 6: Tests and commit**

Run: `cargo test --workspace`

```bash
git add crates/mm-core/ crates/mm-gateway/ bins/trading-core/
git commit -m "feat: add per-strategy metrics tracking with WebSocket broadcast"
```

---

## Task 10: Dashboard Metrics Display

**Files:**
- Modify: `dashboard/src/lib/types.ts` (StrategyMetrics type)
- Modify: `dashboard/src/lib/store.tsx` (handle StrategyMetrics WS message)
- Modify: `dashboard/src/components/panels/Strategies.tsx` (display metrics)

**Step 1: Add StrategyMetrics TypeScript type**

```typescript
interface StrategyMetrics {
  strategy_id: string;
  realized_pnl: string;
  unrealized_pnl: string;
  total_fills: number;
  total_orders_placed: number;
  total_orders_cancelled: number;
  total_orders_rejected: number;
  avg_spread_captured_bps: string;
  volume_traded: string;
  uptime_secs: number;
  last_fill_at: string | null;
}
```

**Step 2: Add WS_MESSAGE handler for StrategyMetrics**

In store reducer, handle `StrategyMetrics` message type: store in state keyed by strategy_id.

Add `metrics: Record<string, StrategyMetrics>` to `DashboardState`.

**Step 3: Update Strategies panel**

Each strategy card shows a live metrics row: PnL (green/red), fill count, volume, uptime. Expandable detail view with all metrics fields.

**Step 4: Test, commit**

```bash
git add dashboard/src/lib/ dashboard/src/components/
git commit -m "feat(dashboard): display live per-strategy metrics"
```

---

## Task 11: Parameter Optimizer Backend

**Files:**
- Create: `crates/mm-backtest/src/optimizer.rs`
- Modify: `crates/mm-backtest/src/lib.rs`
- Modify: `bins/backtester/src/main.rs` (add --optimize mode)
- Modify: `crates/mm-gateway/src/rest.rs` (optimizer endpoints)

**Step 1: Define optimizer types**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParameterRange {
    pub name: String,
    pub values: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationConfig {
    pub strategy_type: String,
    pub base_params: serde_json::Value,
    pub param_ranges: Vec<ParameterRange>,
    pub backtest_config: BacktestConfig,
    pub metric: OptimizationMetric,  // Sharpe, Sortino, PnL, etc.
    pub top_n: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationResult {
    pub params: serde_json::Value,
    pub backtest_result: BacktestResult,
    pub metric_value: Decimal,
}
```

**Step 2: Implement ParameterOptimizer**

```rust
pub struct ParameterOptimizer;

impl ParameterOptimizer {
    pub fn run(
        config: &OptimizationConfig,
        events: Vec<MarketEvent>,
        progress_tx: Option<tokio::sync::mpsc::Sender<OptimizationProgress>>,
    ) -> Vec<OptimizationResult> {
        // Generate all parameter combinations (cartesian product)
        // For each combination:
        //   - Merge into base_params
        //   - Create strategy from merged params
        //   - Run ReplayEngine
        //   - Collect BacktestResult
        //   - Send progress update if tx provided
        // Sort by metric, return top_n
    }
}
```

**Step 3: Add CLI --optimize flag**

In `bins/backtester/src/main.rs`, add `--optimize`, `--param-ranges` (JSON), `--metric`, `--top-n` flags. When `--optimize` is set, run grid search instead of single backtest.

**Step 4: Add REST endpoints**

- `POST /api/v1/backtest/optimize` — starts async job, returns job_id
- `GET /api/v1/backtest/optimize/{job_id}` — poll for results

**Step 5: Tests and commit**

```bash
git add crates/mm-backtest/ bins/backtester/ crates/mm-gateway/
git commit -m "feat(backtest): add parameter optimization with grid search"
```

---

## Task 12: Dashboard Optimization Modal

**Files:**
- Create: `dashboard/src/components/panels/OptimizeModal.tsx`
- Modify: `dashboard/src/lib/api.ts` (optimizer endpoints)
- Modify: `dashboard/src/components/panels/Strategies.tsx` (add Optimize button)

**Step 1: Add API methods**

```typescript
startOptimization: (config: OptimizationConfig) =>
  fetchApi<{ job_id: string }>("/backtest/optimize", {
    method: "POST", body: JSON.stringify(config)
  }),
getOptimizationResult: (jobId: string) =>
  fetchApi<OptimizationResult[]>(`/backtest/optimize/${jobId}`),
```

**Step 2: Build OptimizeModal component**

- Parameter range inputs (from/to/step for each param)
- Data source selection (synthetic or date range)
- Progress bar during sweep (poll job status)
- Results table: sortable by any metric
- "Apply" button copies best params to strategy config (calls PUT endpoint)

**Step 3: Wire into Strategies panel**

Add "Optimize" button on each strategy card that opens the modal pre-filled with that strategy's type and current params.

**Step 4: Test, commit**

```bash
git add dashboard/src/components/ dashboard/src/lib/api.ts
git commit -m "feat(dashboard): add parameter optimization modal"
```

---

## Task 13: AI Assistant Backend

**Files:**
- Create: `crates/mm-gateway/src/assistant.rs`
- Modify: `crates/mm-gateway/src/lib.rs` (re-export)
- Modify: `crates/mm-gateway/src/rest.rs` (SSE endpoint)
- Modify: `crates/mm-gateway/Cargo.toml` (add reqwest for HTTP, add eventsource/SSE deps)

**Step 1: Define assistant types**

```rust
#[derive(Debug, Deserialize)]
pub struct ChatRequest {
    pub messages: Vec<ChatMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,   // "user" or "assistant"
    pub content: String,
}

pub struct AssistantConfig {
    pub api_key: String,
    pub model: String,  // default: "claude-sonnet-4-20250514"
}
```

**Step 2: Build Claude API client**

```rust
pub struct ClaudeClient {
    http: reqwest::Client,
    config: AssistantConfig,
}

impl ClaudeClient {
    pub fn new(config: AssistantConfig) -> Self { ... }

    pub async fn stream_chat(
        &self,
        system_prompt: &str,
        messages: &[ChatMessage],
    ) -> Result<impl Stream<Item = Result<String, Error>>> {
        // Build request to https://api.anthropic.com/v1/messages
        // Set headers: x-api-key, anthropic-version, content-type
        // Enable streaming: "stream": true
        // Parse SSE events, extract text deltas
    }
}
```

**Step 3: Build system context collector**

```rust
fn build_system_context(state: &ApiState) -> String {
    // Collect current positions and PnL
    // Active strategy configs, status, and metrics
    // Recent market data (latest ticks, spreads)
    // Risk state (kill switch status)
    // Available strategy types and parameter descriptions
    // Format as structured text for Claude system prompt
}
```

**Step 4: Add SSE streaming endpoint**

```rust
// POST /api/v1/assistant/chat
async fn assistant_chat(
    State(state): State<Arc<ApiState>>,
    Json(req): Json<ChatRequest>,
) -> impl IntoResponse {
    // Check if assistant is configured (API key present)
    // Build system context from current state
    // Call Claude API with streaming
    // Return SSE response (text/event-stream)
}
```

**Step 5: Configuration**

Read from env vars:
- `MM_ASSISTANT__ANTHROPIC_API_KEY` (required to enable)
- `MM_ASSISTANT__MODEL` (optional, defaults to claude-sonnet-4-20250514)

If no API key, endpoint returns 503 Service Unavailable.

**Step 6: Tests and commit**

```rust
#[tokio::test]
async fn assistant_returns_503_without_api_key() { ... }

#[tokio::test]
async fn system_context_includes_positions() { ... }
```

```bash
git add crates/mm-gateway/
git commit -m "feat(gateway): add Claude AI assistant with SSE streaming"
```

---

## Task 14: AI Assistant Dashboard Panel

**Files:**
- Create: `dashboard/src/components/panels/AiAssistant.tsx`
- Modify: `dashboard/src/lib/api.ts` (add assistant endpoint)
- Modify: `dashboard/src/components/Dashboard.tsx` (add panel to grid)
- Modify: `dashboard/package.json` (add react-markdown dependency)

**Step 1: Add API method for streaming chat**

```typescript
assistantChat: async function* (messages: ChatMessage[]): AsyncGenerator<string> {
  const res = await fetch("/api/v1/assistant/chat", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ messages }),
  });
  if (!res.ok) throw new Error(`Assistant error: ${res.status}`);
  const reader = res.body!.getReader();
  const decoder = new TextDecoder();
  // Parse SSE stream, yield text chunks
}
```

**Step 2: Build AiAssistant panel component**

- Chat interface: scrollable message list with user + assistant messages
- Use `react-markdown` for safe markdown rendering of assistant responses
- Input box at bottom with send button
- Context indicator showing what data the assistant can see
- Auto-scroll to latest message
- Loading indicator while streaming
- When assistant suggests config changes, include an "Apply" button that calls the strategy PUT endpoint

**Step 3: Install react-markdown dependency**

```bash
cd dashboard && npm install react-markdown
```

**Step 4: Add to Dashboard grid**

Register AiAssistant as a new panel. If the assistant endpoint returns 503, hide the panel or show "AI Assistant not configured" message.

**Step 5: Test manually via Playwright**

**Step 6: Commit**

```bash
git add dashboard/
git commit -m "feat(dashboard): add AI chat assistant panel with streaming"
```

---

## Task 15: Integration Testing

**Files:**
- Modify: `tests/` or create integration test files
- No new features, just verification

**Step 1: Strategy round-trip test**

```rust
// Create strategy via POST, verify in GET list
// Start via POST /start, verify running=true
// Stop via POST /stop, verify running=false
// Update via PUT, verify params changed
// Delete via DELETE, verify removed from list
```

**Step 2: Metrics flow test**

```rust
// Create and start a strategy
// Simulate a market data tick
// Verify metrics are produced and broadcast via WebSocket
// Verify GET /strategies/{id}/metrics returns data
```

**Step 3: Optimizer test**

```rust
// POST /backtest/optimize with small param grid
// Poll GET /backtest/optimize/{job_id} until complete
// Verify results are sorted by metric
```

**Step 4: Full workspace test**

Run: `cargo test --workspace`
Run: `cargo clippy --workspace`
Expected: All pass, no warnings

**Step 5: Manual smoke test**

1. Start trading-core + dashboard
2. Create a new strategy via the form
3. Verify it appears in strategies list
4. Start/stop it
5. Check metrics display
6. Run optimization
7. Test AI assistant chat

**Step 6: Final commit**

```bash
git commit -m "test: add integration tests for strategies, metrics, optimizer"
```

---

## Summary

| Task | Component | Estimated Complexity |
|------|-----------|---------------------|
| 1 | TWAP Strategy | Medium |
| 2 | VWAP Strategy | Medium |
| 3 | Momentum Strategy | Medium |
| 4 | Mean Reversion Strategy | Medium |
| 5 | Statistical Arbitrage Strategy | High |
| 6 | Wire into trading-core | Low |
| 7 | Strategy CRUD Endpoints | Medium |
| 8 | Dashboard Strategy Form | High |
| 9 | Strategy Metrics Backend | Medium |
| 10 | Dashboard Metrics Display | Medium |
| 11 | Parameter Optimizer Backend | High |
| 12 | Dashboard Optimization Modal | High |
| 13 | AI Assistant Backend | High |
| 14 | AI Assistant Dashboard Panel | Medium |
| 15 | Integration Testing | Medium |
