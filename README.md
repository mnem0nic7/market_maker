# Market Maker

Multi-market automated trading platform in Rust.

![CI](https://github.com/mnem0nic7/market_maker/actions/workflows/ci.yml/badge.svg)

## Overview

Market Maker is a high-performance automated trading platform written in Rust.
It supports crypto centralized exchanges (Binance, Coinbase, Kraken),
decentralized exchanges (Uniswap on Ethereum, Raydium on Solana), and equities
brokers (Alpaca, Interactive Brokers) through a unified connector interface.

The platform uses a hybrid architecture. The hot trading path -- market data
ingestion, strategy evaluation, risk checks, and order execution -- runs in a
single Tokio process with in-memory channels for minimal latency. Cold-path
services (event persistence, API gateway, backtester) run as separate binaries
and communicate over NATS JetStream for event sourcing and audit.

Eight pluggable strategies ship out of the box, from basic symmetric spread
market making to statistical arbitrage across correlated pairs. All strategies
implement the same `Strategy` trait, so they work identically in live trading
and backtesting. A Next.js dashboard provides real-time monitoring with
WebSocket-driven updates.

## Architecture

### Data Flow (Hot Path)

```
Exchange WebSockets
        |
        v
MarketDataConnector ---mpsc---> Main Loop
        |                           |
        |                    StrategyEngine.tick(snapshot)
        |                           |
        |                     Vec<OrderAction>
        |                           |
        |                    RiskEngine.evaluate()
        |                     /           \
        |               Allow           Reject / KillSwitch
        |                 |
        |          OrderRouter.route()
        |                 |
        v                 v
  OrderManager <--- Exchange REST API
        |
        v
  NATS JetStream ---> TimescaleDB / Audit Log
```

### Inter-Service Communication

| Path | Transport | Use case |
|------|-----------|----------|
| Hot path | `tokio::sync::mpsc` | In-process market data and order flow |
| Cold path | NATS JetStream | Event streaming, persistence, audit trail |
| Dashboard | Axum REST + WebSocket | Queries, real-time updates |

## Quick Start

### Prerequisites

- **Rust** 1.75+ (install via [rustup](https://rustup.rs/))
- **Docker** and **Docker Compose** (for infrastructure services)
- **Node.js** 18+ (optional, for the dashboard)

### Build

```bash
cargo build --workspace
```

### Start Infrastructure

Launch NATS, TimescaleDB, Prometheus, and Grafana:

```bash
docker compose -f docker/docker-compose.yml up -d
```

| Service | Port | Purpose |
|---------|------|---------|
| NATS | 4222 | Event streaming (JetStream) |
| TimescaleDB | 5432 | Time-series persistence |
| Prometheus | 9090 | Metrics collection |
| Grafana | 3000 | Metrics dashboards |

### Configure Exchanges

Copy the default config and add your API keys:

```bash
cp config/default.toml config/local.toml
```

Or set environment variables with the `MM_` prefix:

```bash
export MM_EXCHANGES__BINANCE__API_KEY="your-key"
export MM_EXCHANGES__BINANCE__SECRET_KEY="your-secret"
export MM_EXCHANGES__BINANCE__ENABLED=true
```

### Run

```bash
cargo run --bin trading-core
```

Additional binaries:

```bash
cargo run --bin gateway          # REST/WebSocket API server
cargo run --bin data-pipeline    # Event persistence and audit logging
cargo run --bin backtester       # Historical backtesting
```

## Dashboard

The project includes a Next.js dashboard for real-time monitoring of positions,
orders, P&L, and strategy state. It connects to the gateway over WebSocket.

```bash
cd dashboard
npm install
npm run dev
```

The dashboard starts at `http://localhost:3000` by default. Make sure the
gateway binary is running on port 8080 (the default).

## Strategies

| Strategy | Type | Description |
|----------|------|-------------|
| `spread` | Market Making | Basic symmetric spread quoting around the mid price |
| `avellaneda` | Market Making | Avellaneda-Stoikov inventory-aware model with reservation price adjustment |
| `grid` | Market Making | Grid trading with arithmetic or geometric level spacing |
| `twap` | Execution | Time-weighted average price execution over a fixed window |
| `vwap` | Execution | Volume-weighted average price execution using historical volume profiles |
| `momentum` | Directional | Trend-following with configurable entry/exit thresholds and trailing stops |
| `mean_reversion` | Directional | Bollinger Band-style mean reversion with z-score entry signals |
| `stat_arb` | Relative Value | Statistical arbitrage on the spread between two correlated instruments |

All strategies implement the `Strategy` trait and can be configured in
`config/default.toml` under the `[[strategies]]` array.

## Backtesting

Run the backtester with synthetic data generation:

```bash
cargo run --bin backtester
```

The backtester uses the same `Strategy` trait implementations as live trading,
driven by a `ReplayEngine` that feeds historical snapshots into a
`SimulatedExchange` with configurable fees and slippage. Performance metrics
include Sharpe ratio, Sortino ratio, maximum drawdown, and profit factor.

To load historical data from a file instead of generating synthetic data, place
JSONL-formatted market snapshots in the data directory and pass the path as an
argument.

## Configuration

The primary config file is `config/default.toml`. Every value can be overridden
with environment variables using the `MM_` prefix and double underscores for
nesting.

### Exchanges

```toml
[exchanges.binance]
api_key = "your-api-key"
secret_key = "your-secret-key"
enabled = true
symbols = ["BTC/USDT", "ETH/USDT"]
```

### Strategies

```toml
[[strategies]]
strategy_id = "spread-btc-binance"
strategy_type = "spread"
exchange = "binance"
symbol = "BTC/USDT"
enabled = true

[strategies.params]
spread_bps = "10"
order_size = "0.01"
num_levels = 3
refresh_interval_ms = 1000
```

### Risk Limits

```toml
[risk.position_limits]
max_position_per_symbol = "10"
max_gross_exposure = "500000"

[risk.order_limits]
max_order_quantity = "1"
max_order_notional = "100000"

[risk.pnl_limits]
max_daily_loss = "10000"
max_drawdown_pct = "0.05"

[risk.rate_limits]
max_orders_per_second = 10
max_orders_per_minute = 200
```

### Kill Switch

The kill switch is an `AtomicBool` that any task can check without locking. It
fires automatically when daily loss, position, disconnect, or heartbeat
thresholds are breached.

```toml
[risk.kill_switch]
max_daily_loss = "10000"
max_position_per_symbol = "100"
exchange_disconnect_timeout_secs = 30
heartbeat_timeout_secs = 10
```

## Testing

```bash
cargo test --workspace
```

The test suite contains 303 tests covering the core types, order state machine,
risk checks, circuit breaker logic, strategy calculations, simulated exchange
matching, and performance metrics.

Run a single crate:

```bash
cargo test -p mm-risk
```

Run a specific test:

```bash
cargo test -p mm-backtest -- sim_exchange::tests::test_market_buy_fill
```

## Project Structure

```
market_maker/
  bins/
    trading-core/       Main hot-path trading binary
    data-pipeline/      Event persistence service
    gateway/            REST/WebSocket API server
    backtester/         CLI backtesting tool
  crates/
    mm-core/            Shared types, traits, events, errors
    mm-market-data/     Exchange WebSocket connectors
    mm-strategy/        Strategy engine and implementations
    mm-execution/       Order manager, router, exchange adapters
    mm-risk/            Risk engine, circuit breaker, kill switch, VaR
    mm-data-pipeline/   NATS publisher/subscriber, TimescaleDB, audit log
    mm-backtest/        Replay engine, simulated exchange, performance calc
    mm-gateway/         Axum REST API and WebSocket broadcaster
  config/
    default.toml        Default configuration
  dashboard/            Next.js real-time monitoring UI
  docker/
    docker-compose.yml  NATS, TimescaleDB, Prometheus, Grafana
```

| Crate | Purpose |
|-------|---------|
| `mm-core` | Shared types (`Order`, `OrderBook`, `Fill`, `Position`), traits (`Strategy`, `MarketDataConnector`, `ExecutionConnector`, `RiskCheck`), event sourcing envelope |
| `mm-market-data` | Per-exchange WebSocket connectors, `LocalOrderBook` (BTreeMap), symbol normalizer, `MarketDataManager` orchestrator |
| `mm-strategy` | `StrategyEngine` dispatcher, 8 pluggable strategy implementations |
| `mm-execution` | `OrderManager` (DashMap, dual-indexed, state machine validation), `OrderRouter`, per-exchange REST/WS adapters |
| `mm-risk` | `RiskEngine` pipeline, position/order/PnL/rate limit checks, 5-level `CircuitBreaker`, `KillSwitch` (AtomicBool), parametric and historical VaR |
| `mm-data-pipeline` | NATS JetStream publisher/subscriber, `TimescaleStore` (sqlx + hypertables), hash-chained `AuditLogger` |
| `mm-backtest` | `ReplayEngine`, `SimulatedExchange` (matching with fees/slippage), `PerformanceCalculator` (Sharpe, Sortino, drawdown) |
| `mm-gateway` | Axum REST API (11 endpoints under `/api/v1/`), WebSocket broadcaster, graceful shutdown |

## Key Design Decisions

- All monetary values use `rust_decimal::Decimal` -- never floating point.
- Exchange-specific code lives behind trait abstractions for easy swapping.
- Event sourcing: every state change emits an `Envelope<Event>` to NATS.
- The order state machine in `OrderManager` rejects illegal transitions.
- Symbol normalization is handled per-exchange in the market data layer.

## License

See [LICENSE](LICENSE).
