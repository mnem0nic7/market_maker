# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Multi-market automated trading platform (market maker) in Rust. Supports crypto CEX (Binance, Coinbase, Kraken), DEX (Ethereum/Solana), and equities (Interactive Brokers, Alpaca). Hybrid architecture: hot trading path runs in a single Tokio process; cold-path services (data pipeline, API gateway, backtester) run as separate binaries.

## Build & Test Commands

```bash
cargo check --workspace          # Type-check all crates
cargo build --workspace          # Build all crates (debug)
cargo build --release --workspace # Release build
cargo test --workspace           # Run all 82+ tests
cargo test -p mm-risk            # Test a single crate
cargo test -p mm-backtest -- sim_exchange::tests::test_market_buy_fill  # Single test
cargo run --bin trading-core     # Run the main trading bot
cargo run --bin backtester       # Run backtester with synthetic data
cargo run --bin data-pipeline    # Run data pipeline service
cargo run --bin gateway          # Run standalone API gateway
```

## Architecture

### Crate Layout (Cargo workspace)

- **`crates/mm-core`** — Shared types, traits, events, errors. Everything depends on this. Key traits: `MarketDataConnector`, `ExecutionConnector`, `Strategy`, `RiskCheck`, `EventPublisher`. Key types: `Order`, `OrderBook`, `Fill`, `Position`, `MarketSnapshot`, `OrderAction`, `Envelope`/`Event` (event sourcing).
- **`crates/mm-market-data`** — Exchange WebSocket connectors for market data. Per-exchange modules (binance/coinbase/kraken) + `LocalOrderBook` (BTreeMap-based) + symbol `normalizer` + `MarketDataManager` orchestrator.
- **`crates/mm-strategy`** — `StrategyEngine` + pluggable strategies: `SpreadStrategy` (basic symmetric spread), `AvellanedaStoikovStrategy` (inventory-aware, reservation price model), `GridStrategy` (arithmetic/geometric grid). All implement the `Strategy` trait.
- **`crates/mm-execution`** — `OrderManager` (OMS with DashMap, dual-indexed by client/exchange order ID, state machine validation), `OrderRouter` (smart routing to exchange connectors), per-exchange execution adapters with REST signing + WebSocket user data streams.
- **`crates/mm-risk`** — `RiskEngine` (sequential check pipeline), `PositionLimitCheck`, `OrderSizeCheck`, `PnlLimitCheck`, `RateLimitCheck`, `CircuitBreaker` (5-level: order/rate/PnL/position/system), `KillSwitch` (AtomicBool, thread-safe), `ParametricVar`/`HistoricalVar`.
- **`crates/mm-data-pipeline`** — `NatsEventPublisher`/`NatsEventSubscriber` (JetStream), `TimescaleStore` (sqlx + hypertables), `AuditLogger` (hash-chained JSONL + DB dual-write).
- **`crates/mm-backtest`** — `ReplayEngine` (event-driven historical replay), `SimulatedExchange` (matching engine with fees/slippage), `PerformanceCalculator` (Sharpe, Sortino, drawdown, profit factor).
- **`crates/mm-gateway`** — Axum REST API (11 endpoints under `/api/v1/`), WebSocket broadcaster for live dashboards, graceful shutdown.

### Binary Crates (`bins/`)

- **`trading-core`** — Main hot-path binary. Wires market data → strategy → risk → execution in a single `tokio::select!` loop. Config from `config/default.toml` + `MM_*` env vars.
- **`data-pipeline`** — Consumes events from NATS, persists to TimescaleDB, runs audit logger.
- **`gateway`** — Standalone REST/WebSocket API server.
- **`backtester`** — CLI backtester, can generate synthetic data or load from JSONL files.

### Data Flow (Hot Path)

```
Exchange WebSockets → MarketDataConnector → mpsc → main loop
    → StrategyEngine.tick(MarketSnapshot) → Vec<OrderAction>
    → RiskEngine.evaluate_order() → Allow/Reject/ReduceSize/KillSwitch
    → OrderRouter.route_action() → ExecutionConnector → Exchange REST
    → OrderUpdate via WebSocket → OrderManager → Strategy.on_fill()
    → All events published to NATS JetStream
```

### Inter-Service Communication

- **Hot path**: `tokio::sync::mpsc` channels (in-process, zero network hop)
- **Cold path**: NATS JetStream for event streaming, event sourcing, audit trail
- **Sync queries**: gRPC (tonic) for request/reply (planned), Axum REST for dashboard/API

## Configuration

- Config file: `config/default.toml` (TOML with all sections documented)
- Environment override prefix: `MM_` (double underscore for nesting: `MM_EXCHANGES__BINANCE__API_KEY`)
- Exchange API keys go in config or env vars — never commit real keys

## Infrastructure (Docker)

```bash
docker compose -f docker/docker-compose.yml up -d  # Start NATS, TimescaleDB, Prometheus, Grafana
```

## Key Patterns

- All monetary values use `rust_decimal::Decimal`, never f64
- Exchange-specific code is behind trait abstractions (`MarketDataConnector`, `ExecutionConnector`)
- Strategies implement the `Strategy` trait — same interface for live and backtest
- Event sourcing: all state changes emit `Envelope<Event>` to NATS
- Kill switch uses `AtomicBool` — checkable from any task without locking
- Order state machine validated in `OrderManager` (rejects illegal transitions)
- Symbol normalization handled per-exchange in `mm-market-data::normalizer`
