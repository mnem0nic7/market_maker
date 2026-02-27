//! trading-core — main binary for the automated market making platform.
//!
//! Wires together market data ingestion, strategy execution, risk management,
//! order routing, and the REST/WebSocket gateway into a single process with a
//! tight event-driven main loop.

mod config;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use mm_core::{
    events::{Envelope, Event, OrderUpdate},
    traits::EventPublisher,
    types::{
        CancelAllOrders, CancelOrder, Exchange, Fill, MarketSnapshot, Order, OrderAction,
        OrderBook, Position, RiskAction, Symbol, Trade,
    },
    MetricsRegistry, OrderStatus, StrategyConfig, StrategyMetricsTracker,
};
use mm_data_pipeline::{NatsConfig, NatsEventPublisher};
use mm_execution::{
    AlpacaExecutionConfig, AlpacaExecutionConnector, BinanceExecutionConfig,
    BinanceExecutionConnector, CoinbaseExecutionConfig, CoinbaseExecutionConnector,
    IbExecutionConfig, IbExecutionConnector, KrakenExecutionConfig, KrakenExecutionConnector,
    OrderManager, OrderRouter, PositionReconciliation,
};
use mm_gateway::ws::ServerMessage;
use mm_gateway::{ApiState, DashboardBroadcaster, GatewayConfig, GatewayServer};
use mm_market_data::{
    AlpacaConfig, AlpacaConnector, BinanceConnector, BinanceMarketDataConfig, CoinbaseConnector,
    IbConfig, IbConnector, KrakenConnector, MarketDataManager,
};
use mm_risk::{
    ExchangeState, KillSwitch, KillSwitchConfig, OrderSizeCheck, OrderSizeConfig, PnlLimitCheck,
    PnlLimitConfig, PositionLimitCheck, PositionLimitConfig, RateLimitCheck, RateLimitConfig,
    RiskEngine,
};
use mm_strategy::{build_strategy_from_config, StrategyEngine, StrategyParamUpdate};

use crate::config::{parse_exchange, parse_symbol};

// ---------------------------------------------------------------------------
// Channel capacities
// ---------------------------------------------------------------------------

const BOOK_CHANNEL_CAPACITY: usize = 4096;
const TRADE_CHANNEL_CAPACITY: usize = 4096;
const ORDER_UPDATE_CHANNEL_CAPACITY: usize = 4096;
const PARAM_UPDATE_CHANNEL_CAPACITY: usize = 64;

// ---------------------------------------------------------------------------
// Shared position state
// ---------------------------------------------------------------------------

type PositionMap = HashMap<(Exchange, Symbol), Position>;

// ---------------------------------------------------------------------------
// Shutdown checkpoint
// ---------------------------------------------------------------------------

/// Path to the shutdown checkpoint file. Written on graceful shutdown,
/// checked on next startup for state reconciliation.
const SHUTDOWN_CHECKPOINT_PATH: &str = "data/shutdown_checkpoint.json";

/// Persisted state written during graceful shutdown so the next startup
/// can detect whether there were open positions or orders at the time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownCheckpoint {
    pub positions: Vec<Position>,
    pub open_orders: Vec<Order>,
    pub timestamp: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// ArcPnlCheck wrapper — delegates RiskCheck to Arc<PnlLimitCheck>
// ---------------------------------------------------------------------------

/// Wrapper to allow `Arc<PnlLimitCheck>` to be used as a `Box<dyn RiskCheck>`.
/// This lets the main loop retain a reference to call `record_pnl` while the
/// `RiskEngine` also holds the check behind its `Box<dyn RiskCheck>` pipeline.
struct ArcPnlCheck(Arc<PnlLimitCheck>);

#[async_trait::async_trait]
impl mm_core::traits::RiskCheck for ArcPnlCheck {
    fn name(&self) -> &str {
        "pnl_limit"
    }

    async fn check_order(
        &self,
        order: &mm_core::types::NewOrder,
        portfolio: &mm_core::types::PortfolioRisk,
    ) -> mm_core::types::RiskAction {
        self.0.check_order(order, portfolio).await
    }

    async fn check_portfolio(
        &self,
        portfolio: &mm_core::types::PortfolioRisk,
    ) -> mm_core::types::RiskAction {
        self.0.check_portfolio(portfolio).await
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    // ----- (a) Initialize tracing subscriber with env filter -----
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .init();

    info!("trading-core starting");

    // ----- (b) Load .env file (if present) into process environment -----
    let _ = dotenvy::dotenv();

    // ----- (c) Load configuration -----
    let cfg = config::load_config().context("failed to load configuration")?;
    info!(?cfg.trading_loop, "configuration loaded");

    // ----- (c) Validate configuration before proceeding -----
    cfg.validate().context("startup configuration validation")?;

    // ----- (c2) Check for prior shutdown checkpoint -----
    if Path::new(SHUTDOWN_CHECKPOINT_PATH).exists() {
        match std::fs::read_to_string(SHUTDOWN_CHECKPOINT_PATH) {
            Ok(json) => match serde_json::from_str::<ShutdownCheckpoint>(&json) {
                Ok(checkpoint) => {
                    warn!(
                        timestamp = %checkpoint.timestamp,
                        positions = checkpoint.positions.len(),
                        open_orders = checkpoint.open_orders.len(),
                        "found shutdown checkpoint from previous run — review for reconciliation"
                    );
                    for pos in &checkpoint.positions {
                        warn!(
                            exchange = %pos.exchange,
                            symbol = %pos.symbol,
                            quantity = %pos.quantity,
                            realized_pnl = %pos.realized_pnl,
                            "checkpoint position"
                        );
                    }
                    for order in &checkpoint.open_orders {
                        warn!(
                            client_order_id = %order.client_order_id,
                            exchange = %order.exchange,
                            symbol = %order.symbol,
                            side = %order.side,
                            status = %order.status,
                            "checkpoint open order"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        path = SHUTDOWN_CHECKPOINT_PATH,
                        "failed to parse shutdown checkpoint file"
                    );
                }
            },
            Err(e) => {
                warn!(
                    error = %e,
                    path = SHUTDOWN_CHECKPOINT_PATH,
                    "failed to read shutdown checkpoint file"
                );
            }
        }
    }

    // ----- (d) Initialize shared state -----
    let order_manager = Arc::new(OrderManager::new());
    let positions: Arc<RwLock<PositionMap>> = Arc::new(RwLock::new(HashMap::new()));
    let daily_pnl: Arc<RwLock<Decimal>> = Arc::new(RwLock::new(Decimal::ZERO));

    // Determine which exchanges are enabled so we can configure the kill switch.
    let enabled_exchanges: Vec<Exchange> = cfg
        .exchanges
        .iter()
        .filter(|(_, ec)| ec.enabled)
        .filter_map(|(name, _)| parse_exchange(name))
        .collect();

    let kill_switch = Arc::new(KillSwitch::new(KillSwitchConfig {
        max_daily_loss: cfg.risk.kill_switch.max_daily_loss,
        max_position_per_symbol: cfg.risk.kill_switch.max_position_per_symbol,
        exchange_disconnect_timeout_secs: cfg.risk.kill_switch.exchange_disconnect_timeout_secs,
        heartbeat_timeout_secs: cfg.risk.kill_switch.heartbeat_timeout_secs,
        exchanges: enabled_exchanges.clone(),
    }));

    // ----- (e) Optional NATS event publisher -----
    let event_publisher: Option<Arc<dyn EventPublisher>> = if cfg.nats.enabled {
        let nats_cfg = NatsConfig {
            url: cfg.nats.url.clone(),
            stream_name: cfg.nats.stream_name.clone(),
            max_age_secs: cfg.nats.max_age_secs,
            max_bytes: cfg.nats.max_bytes,
        };
        match NatsEventPublisher::connect(&nats_cfg).await {
            Ok(pub_) => {
                info!("NATS event publisher connected");
                Some(Arc::new(pub_) as Arc<dyn EventPublisher>)
            }
            Err(e) => {
                error!(error = %e, "failed to connect NATS event publisher — continuing without it");
                None
            }
        }
    } else {
        info!("NATS event publisher disabled");
        None
    };

    // ----- (f) Market data channels -----
    let (book_tx, mut book_rx) = mpsc::channel::<OrderBook>(BOOK_CHANNEL_CAPACITY);
    let (trade_tx, mut trade_rx) = mpsc::channel::<Trade>(TRADE_CHANNEL_CAPACITY);

    // ----- (g) Initialize exchange connectors (market data + execution) -----
    let mut md_manager = MarketDataManager::new(book_tx, trade_tx);
    let mut order_router =
        OrderRouter::new(Arc::clone(&order_manager), ORDER_UPDATE_CHANNEL_CAPACITY);

    // We also need a separate order-update channel for the main loop to
    // process order updates (the router has its own internal one for the OMS,
    // but we need to observe updates too).
    let (order_update_tx, mut order_update_rx) =
        mpsc::channel::<OrderUpdate>(ORDER_UPDATE_CHANNEL_CAPACITY);

    // Track which (exchange, symbols) pairs we're subscribed to.
    let mut subscribed_pairs: Vec<(Exchange, Vec<Symbol>)> = Vec::new();

    for (exchange_name, exchange_cfg) in &cfg.exchanges {
        if !exchange_cfg.enabled {
            debug!(exchange = %exchange_name, "exchange disabled, skipping");
            continue;
        }

        let exchange = match parse_exchange(exchange_name) {
            Some(e) => e,
            None => {
                warn!(exchange = %exchange_name, "unknown exchange, skipping");
                continue;
            }
        };

        let symbols: Vec<Symbol> = exchange_cfg
            .symbols
            .iter()
            .filter_map(|s| parse_symbol(s))
            .collect();

        if symbols.is_empty() {
            warn!(exchange = %exchange_name, "no valid symbols configured, skipping");
            continue;
        }

        // Register market data connector.
        match exchange {
            Exchange::Binance => {
                md_manager.register_connector(Box::new(BinanceConnector::new(
                    BinanceMarketDataConfig {
                        rest_url: "https://api.binance.us".into(),
                        ws_url: "wss://stream.binance.us:9443".into(),
                    },
                )));
                order_router.register_connector(Box::new(BinanceExecutionConnector::new(
                    BinanceExecutionConfig {
                        api_key: exchange_cfg.api_key.clone(),
                        secret_key: exchange_cfg.secret_key.clone(),
                        base_url: "https://api.binance.us".into(),
                        ws_url: "wss://stream.binance.us:9443".into(),
                    },
                )));
            }
            Exchange::Coinbase => {
                md_manager.register_connector(Box::new(CoinbaseConnector::new()));
                order_router.register_connector(Box::new(CoinbaseExecutionConnector::new(
                    CoinbaseExecutionConfig {
                        api_key: exchange_cfg.api_key.clone(),
                        secret_key: exchange_cfg.secret_key.clone(),
                        base_url: "https://api.coinbase.com".into(),
                        ws_url: "wss://advanced-trade-ws.coinbase.com".into(),
                    },
                )));
            }
            Exchange::Kraken => {
                md_manager.register_connector(Box::new(KrakenConnector::new()));
                order_router.register_connector(Box::new(KrakenExecutionConnector::new(
                    KrakenExecutionConfig {
                        api_key: exchange_cfg.api_key.clone(),
                        secret_key: exchange_cfg.secret_key.clone(),
                        base_url: "https://api.kraken.com".into(),
                        ws_url: "wss://ws-auth.kraken.com/v2".into(),
                    },
                )));
            }
            Exchange::Alpaca => {
                md_manager.register_connector(Box::new(AlpacaConnector::new(AlpacaConfig {
                    api_key: exchange_cfg.api_key.clone(),
                    secret_key: exchange_cfg.secret_key.clone(),
                    data_ws_url: "wss://stream.data.alpaca.markets/v2/iex".into(),
                })));
                order_router.register_connector(Box::new(AlpacaExecutionConnector::new(
                    AlpacaExecutionConfig {
                        api_key: exchange_cfg.api_key.clone(),
                        secret_key: exchange_cfg.secret_key.clone(),
                        base_url: "https://paper-api.alpaca.markets".into(),
                        ws_url: "wss://paper-api.alpaca.markets/stream".into(),
                    },
                )));
            }
            Exchange::InteractiveBrokers => {
                md_manager.register_connector(Box::new(IbConnector::new(IbConfig {
                    base_url: "https://localhost:5000/v1/api".into(),
                    ws_url: "wss://localhost:5000/v1/api/ws".into(),
                    account_id: String::new(),
                    conid_map: std::collections::HashMap::new(),
                })));
                order_router.register_connector(Box::new(IbExecutionConnector::new(
                    IbExecutionConfig {
                        base_url: "https://localhost:5000/v1/api".into(),
                        account_id: String::new(),
                        conid_map: std::collections::HashMap::new(),
                    },
                )));
            }
            _ => {
                warn!(%exchange, "exchange not supported for live trading yet, skipping");
                continue;
            }
        }

        // Subscribe to market data.
        if let Err(e) = md_manager.subscribe(exchange, &symbols).await {
            error!(%exchange, error = %e, "failed to subscribe to market data");
        } else {
            info!(%exchange, symbol_count = symbols.len(), "subscribed to market data");
            subscribed_pairs.push((exchange, symbols));
        }
    }

    // Wire the forwarding channel so the router's update loop sends order
    // updates (fills, cancellations, etc.) to the main event loop.
    order_router.set_forward_channel(order_update_tx);

    // Connect all execution connectors (starts their WebSocket user-data
    // streams and the internal update loop).
    if let Err(e) = order_router.connect_all().await {
        error!(error = %e, "failed to connect execution connectors");
    }

    // ----- (h) Initialize strategy engine -----
    let strategy_engine_shared = Arc::new(tokio::sync::Mutex::new(StrategyEngine::new()));

    {
        let mut strategy_engine = strategy_engine_shared.lock().await;
        for strat_cfg in &cfg.strategies {
            if !strat_cfg.enabled {
                debug!(strategy_id = %strat_cfg.strategy_id, "strategy disabled, skipping");
                continue;
            }

            let exchange = match parse_exchange(&strat_cfg.exchange) {
                Some(e) => e,
                None => {
                    warn!(
                        strategy_id = %strat_cfg.strategy_id,
                        exchange = %strat_cfg.exchange,
                        "unknown exchange for strategy, skipping"
                    );
                    continue;
                }
            };

            let symbol = match parse_symbol(&strat_cfg.symbol) {
                Some(s) => s,
                None => {
                    warn!(
                        strategy_id = %strat_cfg.strategy_id,
                        symbol = %strat_cfg.symbol,
                        "invalid symbol for strategy, skipping"
                    );
                    continue;
                }
            };

            let strategy_config = StrategyConfig {
                strategy_id: strat_cfg.strategy_id.clone(),
                exchange,
                symbol: symbol.clone(),
                strategy_type: strat_cfg.strategy_type.clone(),
                enabled: strat_cfg.enabled,
                params: strat_cfg.params.clone(),
            };

            let strategy = match build_strategy_from_config(&strategy_config) {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        strategy_id = %strat_cfg.strategy_id,
                        error = %e,
                        "failed to build strategy, skipping"
                    );
                    continue;
                }
            };

            match strategy_engine.add_strategy(strategy) {
                Ok(()) => info!(
                    strategy_id = %strat_cfg.strategy_id,
                    strategy_type = %strat_cfg.strategy_type,
                    "strategy registered"
                ),
                Err(e) => error!(
                    strategy_id = %strat_cfg.strategy_id,
                    error = %e,
                    "failed to register strategy"
                ),
            }
        }

        info!(
            strategy_count = strategy_engine.strategy_count(),
            active_count = strategy_engine.active_count(),
            "strategy engine initialized"
        );
    } // drop lock on strategy_engine_shared

    // ----- (h1) Create parameter update channel for hot-updating strategies -----
    let (param_update_tx, param_update_rx) =
        mpsc::channel::<StrategyParamUpdate>(PARAM_UPDATE_CHANNEL_CAPACITY);

    // Attach the receiver to the strategy engine so it can poll for updates.
    {
        let mut engine = strategy_engine_shared.lock().await;
        engine.set_param_update_receiver(param_update_rx);
    }

    // ----- (h2) Initialize strategy metrics tracker -----
    let mut strategy_metrics_tracker = StrategyMetricsTracker::new();
    for strat_cfg in &cfg.strategies {
        if strat_cfg.enabled {
            strategy_metrics_tracker.register_strategy(&strat_cfg.strategy_id);
        }
    }
    info!(
        tracked_strategies = strategy_metrics_tracker.all().len(),
        "strategy metrics tracker initialized"
    );

    // ----- (i) Initialize risk engine -----
    let mut risk_engine = RiskEngine::new();

    risk_engine.add_check(Box::new(PositionLimitCheck::new(PositionLimitConfig {
        max_position_per_symbol: cfg.risk.position_limits.max_position_per_symbol,
        max_gross_exposure: cfg.risk.position_limits.max_gross_exposure,
    })));

    risk_engine.add_check(Box::new(OrderSizeCheck::new(OrderSizeConfig {
        max_order_quantity: cfg.risk.order_limits.max_order_quantity,
        max_order_notional: cfg.risk.order_limits.max_order_notional,
    })));

    let pnl_limit_check = Arc::new(PnlLimitCheck::new(PnlLimitConfig {
        max_daily_loss: cfg.risk.pnl_limits.max_daily_loss,
        max_drawdown_pct: cfg.risk.pnl_limits.max_drawdown_pct,
    }));
    risk_engine.add_check(Box::new(ArcPnlCheck(Arc::clone(&pnl_limit_check))));

    risk_engine.add_check(Box::new(RateLimitCheck::new(RateLimitConfig {
        max_orders_per_second: cfg.risk.rate_limits.max_orders_per_second,
        max_orders_per_minute: cfg.risk.rate_limits.max_orders_per_minute,
    })));

    info!(
        check_count = risk_engine.check_count(),
        "risk engine initialized"
    );

    // ----- (j) Initialize gateway (REST API + WebSocket) -----
    let metrics = Arc::new(MetricsRegistry::new().expect("Failed to initialize metrics"));
    let api_state = ApiState::with_param_updates(
        Arc::clone(&metrics),
        Arc::clone(&strategy_engine_shared),
        Arc::clone(&kill_switch),
        param_update_tx,
    );

    // Create the broadcaster externally so the main loop can publish events.
    let broadcaster = DashboardBroadcaster::new(4096, 1024);
    let broadcast_tx = broadcaster.sender();

    if cfg.gateway.enabled {
        let gw_config = GatewayConfig {
            host: cfg.gateway.host.clone(),
            port: cfg.gateway.port,
            cors_origins: cfg.gateway.cors_origins.clone(),
            api_token: cfg.gateway.api_token.clone(),
            rate_limit_rps: Some(100),
        };
        let gw_state = Arc::clone(&api_state);
        let gw_broadcaster = Arc::clone(&broadcaster);

        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            let server = GatewayServer::with_broadcaster(gw_config, gw_state, gw_broadcaster)
                .with_ready_signal(ready_tx);
            if let Err(e) = server.start().await {
                error!(error = %e, "gateway server exited with error");
            }
        });

        // Wait for the gateway to signal readiness (with a timeout).
        match tokio::time::timeout(Duration::from_secs(5), ready_rx).await {
            Ok(Ok(())) => {
                info!(host = %cfg.gateway.host, port = cfg.gateway.port, "gateway ready");
            }
            Ok(Err(_)) => {
                warn!("gateway readiness channel closed without signal");
            }
            Err(_) => {
                warn!(
                    host = %cfg.gateway.host,
                    port = cfg.gateway.port,
                    "gateway readiness timeout — continuing without confirmation"
                );
            }
        }
    } else {
        info!("gateway disabled");
    }

    // ----- (k) Main trading loop -----
    let tick_interval = Duration::from_millis(cfg.trading_loop.tick_interval_ms);
    let mut tick_timer = tokio::time::interval(tick_interval);
    tick_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Latest order book and trade snapshots for building MarketSnapshot.
    // Books are wrapped in Arc to avoid deep-cloning BTreeMap/Vec price levels
    // on every strategy tick in the hot path — Arc::clone is just a refcount bump.
    let mut latest_books: HashMap<(Exchange, String), Arc<OrderBook>> = HashMap::new();
    let mut latest_trades: HashMap<(Exchange, String), Trade> = HashMap::new();
    // Track last order book receive time per exchange for disconnect detection.
    let mut last_book_time: HashMap<Exchange, chrono::DateTime<Utc>> = HashMap::new();

    // ----- (k1) Position reconciliation timer -----
    let reconciliation_enabled = cfg.trading_loop.reconciliation_interval_secs > 0;
    let reconciliation_interval = Duration::from_secs(if reconciliation_enabled {
        cfg.trading_loop.reconciliation_interval_secs
    } else {
        // Use a large value — the branch will be skipped anyway.
        3600
    });
    let mut reconciliation_timer = tokio::time::interval(reconciliation_interval);
    reconciliation_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the first immediate tick so reconciliation starts after the
    // configured interval, not at startup.
    reconciliation_timer.tick().await;
    let drift_warn_threshold =
        Decimal::try_from(cfg.trading_loop.reconciliation_drift_threshold_pct)
            .unwrap_or(Decimal::ONE);
    let drift_error_threshold = Decimal::from(5); // 5% is always a severe mismatch
    if reconciliation_enabled {
        info!(
            interval_secs = cfg.trading_loop.reconciliation_interval_secs,
            drift_warn_pct = %drift_warn_threshold,
            "position reconciliation enabled"
        );
    } else {
        info!("position reconciliation disabled");
    }

    // Graceful shutdown signal.
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

    // Spawn a task that waits for SIGINT or SIGTERM and signals the main loop.
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = shutdown_tx.send(()).await;
    });

    info!(
        "entering main trading loop (tick interval: {}ms)",
        cfg.trading_loop.tick_interval_ms
    );

    loop {
        tokio::select! {
            // --- Order book update ---
            Some(book) = book_rx.recv() => {
                let key = (book.exchange, book.symbol.canonical());
                debug!(
                    exchange = %book.exchange,
                    symbol = %book.symbol,
                    best_bid = ?book.best_bid().map(|l| l.price),
                    best_ask = ?book.best_ask().map(|l| l.price),
                    "order book update"
                );

                // Update the market data manager's local book.
                md_manager.update_local_book(&book);

                // Record WebSocket connectivity metric (receiving books = connected).
                metrics.websocket_connected
                    .with_label_values(&[book.exchange.as_str()])
                    .set(1.0);

                // Broadcast market data tick to WebSocket dashboard clients.
                if let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) {
                    let mid = (bid.price + ask.price) / Decimal::from(2);
                    let spread_bps = if mid > Decimal::ZERO {
                        (ask.price - bid.price) / mid * Decimal::from(10_000)
                    } else {
                        Decimal::ZERO
                    };

                    // Record order book spread metric.
                    metrics.order_book_spread_bps
                        .with_label_values(&[book.exchange.as_str(), &book.symbol.canonical()])
                        .set(spread_bps.to_f64().unwrap_or(0.0));

                    let tick_msg = ServerMessage::MarketDataTick {
                        exchange: book.exchange,
                        symbol: book.symbol.clone(),
                        best_bid: bid.price,
                        best_ask: ask.price,
                        mid_price: mid,
                        spread_bps,
                    };
                    let _ = broadcast_tx.try_send(tick_msg);
                }

                last_book_time.insert(book.exchange, Utc::now());
                // Wrap in Arc so the hot-path tick loop can share the book
                // via cheap refcount bumps instead of deep-cloning Vec<PriceLevel>.
                let arc_book = Arc::new(book);
                {
                    let mut obs = api_state.order_books.write().await;
                    obs.insert(key.clone(), Arc::clone(&arc_book));
                }
                latest_books.insert(key, arc_book);
            }

            // --- Trade update ---
            Some(trade) = trade_rx.recv() => {
                let key = (trade.exchange, trade.symbol.canonical());
                debug!(
                    exchange = %trade.exchange,
                    symbol = %trade.symbol,
                    price = %trade.price,
                    quantity = %trade.quantity,
                    "trade update"
                );
                latest_trades.insert(key, trade);
            }

            // --- Order update from execution connectors ---
            Some(update) = order_update_rx.recv() => {
                handle_order_update(
                    &update,
                    &order_manager,
                    &strategy_engine_shared,
                    &positions,
                    &daily_pnl,
                    &api_state,
                    event_publisher.as_ref(),
                    &mut strategy_metrics_tracker,
                    &pnl_limit_check,
                    &metrics,
                    &broadcast_tx,
                ).await;
            }

            // --- Tick interval: run strategy engine ---
            _ = tick_timer.tick() => {
                // Record heartbeat for kill switch.
                kill_switch.record_heartbeat();

                // Check kill switch — if active, do not tick strategies.
                if kill_switch.is_active() {
                    metrics.kill_switch_active.set(1.0);
                    debug!("kill switch active, skipping strategy tick");
                    continue;
                }
                metrics.kill_switch_active.set(0.0);

                // Apply any pending strategy parameter updates (hot-update).
                strategy_engine_shared.lock().await.try_apply_updates();

                // Build portfolio risk snapshot for risk checks.
                let pos_snapshot = positions.read().await;
                let positions_vec: Vec<Position> = pos_snapshot.values().cloned().collect();
                let current_daily_pnl = *daily_pnl.read().await;
                drop(pos_snapshot);

                let portfolio_risk = RiskEngine::build_portfolio_risk(
                    &positions_vec,
                    current_daily_pnl,
                );

                // Build exchange connectivity states from last book timestamps.
                let exchange_states: Vec<ExchangeState> = subscribed_pairs
                    .iter()
                    .map(|(exchange, _)| {
                        let last_msg = last_book_time.get(exchange).copied();
                        ExchangeState {
                            exchange: *exchange,
                            connected: last_msg.is_some(),
                            last_message_at: last_msg,
                        }
                    })
                    .collect();

                // Check kill switch triggers.
                if let Some(activation) = kill_switch.check_triggers(&portfolio_risk, &exchange_states) {
                    error!(
                        reason = %activation.trigger,
                        "kill switch activated — cancelling all orders"
                    );

                    // Cancel all orders on all exchanges.
                    for cancel_msg in &activation.cancel_messages {
                        if let Err(e) = order_router.route_action(OrderAction::CancelAll(cancel_msg.clone())).await {
                            error!(error = %e, "failed to cancel all orders during kill switch");
                        }
                    }

                    // Stop all strategies.
                    let stop_actions = strategy_engine_shared.lock().await.stop_all();
                    for action in stop_actions {
                        if let Err(e) = order_router.route_action(action).await {
                            error!(error = %e, "failed to route stop action during kill switch");
                        }
                    }

                    // Publish kill switch event.
                    if let Some(ref publisher) = event_publisher {
                        let envelope = Envelope::new(
                            "trading-core",
                            "kill-switch",
                            Event::KillSwitchActivated {
                                reason: activation.trigger.to_string(),
                                orders_cancelled: activation.cancel_messages.len() as u32,
                            },
                        );
                        if let Err(e) = publisher.publish(envelope).await {
                            error!(error = %e, "failed to publish kill switch event");
                        }
                    }

                    // Update gateway state.
                    *api_state.kill_switch_active.lock().await = true;
                    metrics.kill_switch_active.set(1.0);

                    continue;
                }

                // For each subscribed (exchange, symbol), build a snapshot and tick.
                for (exchange, symbols) in &subscribed_pairs {
                    for symbol in symbols {
                        let key = (*exchange, symbol.canonical());

                        // We need a valid order book to run the strategy.
                        // Arc::clone is a cheap refcount bump — no deep copy of
                        // the Vec<PriceLevel> bid/ask levels.
                        let book = match latest_books.get(&key) {
                            Some(b) => Arc::clone(b),
                            None => {
                                debug!(
                                    exchange = %exchange,
                                    symbol = %symbol,
                                    "no order book yet, skipping"
                                );
                                continue;
                            }
                        };

                        // Skip stale order books to avoid trading on outdated data.
                        let book_age = Utc::now() - book.timestamp;
                        let stale_threshold = chrono::Duration::milliseconds(cfg.trading_loop.order_book_stale_ms as i64);
                        if book_age > stale_threshold {
                            warn!(
                                exchange = %exchange,
                                symbol = %symbol,
                                age_ms = book_age.num_milliseconds(),
                                "skipping stale order book"
                            );
                            metrics.websocket_connected
                                .with_label_values(&[exchange.as_str()])
                                .set(0.0);
                            continue;
                        }

                        let last_trade = latest_trades.get(&key).cloned();

                        // Position is cloned here; it's a small fixed-size struct
                        // (a few Decimals + Exchange + Symbol + DateTime) so the
                        // copy cost is negligible compared to OrderBook.  Not worth
                        // Arc-wrapping because Position is mutated via update_on_fill.
                        let position = {
                            let pos_map = positions.read().await;
                            pos_map
                                .get(&(*exchange, symbol.clone()))
                                .cloned()
                                .unwrap_or_else(|| Position::new(*exchange, symbol.clone()))
                        };

                        // Filter at the DashMap level to avoid cloning
                        // orders for irrelevant instruments.
                        let relevant_orders =
                            order_manager.get_open_orders_for(exchange, symbol);

                        let snapshot = MarketSnapshot {
                            exchange: *exchange,
                            symbol: symbol.clone(),
                            order_book: book,
                            last_trade,
                            position,
                            open_orders: relevant_orders,
                            timestamp: Utc::now(),
                        };

                        // Run strategies and measure tick duration.
                        let tick_start = Instant::now();
                        let actions = strategy_engine_shared.lock().await.tick(&snapshot);
                        let tick_elapsed = tick_start.elapsed();
                        metrics.strategy_tick_duration
                            .with_label_values(&[&format!("{}:{}", exchange, symbol.canonical())])
                            .observe(tick_elapsed.as_secs_f64());

                        if actions.is_empty() {
                            continue;
                        }

                        debug!(
                            exchange = %exchange,
                            symbol = %symbol,
                            action_count = actions.len(),
                            "strategy produced actions"
                        );

                        // Run each action through risk checks before routing.
                        for action in actions {
                            let approved_action = match &action {
                                OrderAction::Place(new_order) => {
                                    let risk_result = risk_engine
                                        .evaluate_order(new_order, &portfolio_risk)
                                        .await;

                                    let exchange_label = new_order.exchange.as_str();
                                    let side_label = new_order.side.as_str();

                                    match risk_result {
                                        RiskAction::Allow => {
                                            metrics.risk_checks_total
                                                .with_label_values(&["risk_engine", "allow"])
                                                .inc();
                                            metrics.orders_total
                                                .with_label_values(&[exchange_label, side_label, "submitted"])
                                                .inc();
                                            strategy_metrics_tracker.on_order_placed(&new_order.strategy_id);
                                            Some(action)
                                        }
                                        RiskAction::Reject { reason } => {
                                            warn!(
                                                reason = %reason,
                                                "order rejected by risk engine"
                                            );
                                            metrics.risk_checks_total
                                                .with_label_values(&["risk_engine", "reject"])
                                                .inc();
                                            metrics.orders_total
                                                .with_label_values(&[exchange_label, side_label, "rejected"])
                                                .inc();
                                            strategy_metrics_tracker.on_order_rejected(&new_order.strategy_id);
                                            None
                                        }
                                        RiskAction::ReduceSize { max_quantity, reason } => {
                                            if max_quantity <= Decimal::ZERO {
                                                warn!(
                                                    reason = %reason,
                                                    "risk engine reduced order to zero quantity, dropping"
                                                );
                                                metrics.risk_checks_total
                                                    .with_label_values(&["risk_engine", "reject"])
                                                    .inc();
                                                metrics.orders_total
                                                    .with_label_values(&[exchange_label, side_label, "rejected"])
                                                    .inc();
                                                strategy_metrics_tracker.on_order_rejected(&new_order.strategy_id);
                                                None
                                            } else {
                                                info!(
                                                    max_quantity = %max_quantity,
                                                    reason = %reason,
                                                    "risk engine reducing order size"
                                                );
                                                metrics.risk_checks_total
                                                    .with_label_values(&["risk_engine", "reduce_size"])
                                                    .inc();
                                                metrics.orders_total
                                                    .with_label_values(&[exchange_label, side_label, "submitted"])
                                                    .inc();
                                                strategy_metrics_tracker.on_order_placed(&new_order.strategy_id);
                                                let mut reduced = new_order.clone();
                                                reduced.quantity = max_quantity;
                                                Some(OrderAction::Place(reduced))
                                            }
                                        }
                                        RiskAction::KillSwitch { reason } => {
                                            error!(
                                                reason = %reason,
                                                "risk check triggered kill switch"
                                            );
                                            metrics.risk_checks_total
                                                .with_label_values(&["risk_engine", "kill_switch"])
                                                .inc();
                                            metrics.orders_total
                                                .with_label_values(&[exchange_label, side_label, "rejected"])
                                                .inc();
                                            metrics.kill_switch_active.set(1.0);
                                            strategy_metrics_tracker.on_order_rejected(&new_order.strategy_id);
                                            let activation = kill_switch.activate(
                                                mm_risk::KillSwitchTrigger::MaxDailyLoss {
                                                    daily_pnl: current_daily_pnl,
                                                    threshold: cfg.risk.kill_switch.max_daily_loss,
                                                },
                                            );
                                            for cancel_msg in &activation.cancel_messages {
                                                if let Err(e) = order_router
                                                    .route_action(OrderAction::CancelAll(cancel_msg.clone()))
                                                    .await
                                                {
                                                    error!(error = %e, "kill switch cancel failed");
                                                }
                                            }
                                            strategy_engine_shared.lock().await.stop_all();
                                            *api_state.kill_switch_active.lock().await = true;
                                            None
                                        }
                                    }
                                }
                                // Track cancels for strategy metrics.
                                OrderAction::Cancel(cancel) => {
                                    // Look up the strategy_id from the order manager.
                                    if let Some(order) = order_manager.get_order(&cancel.client_order_id) {
                                        strategy_metrics_tracker.on_order_cancelled(&order.strategy_id);
                                    }
                                    Some(action)
                                }
                                // CancelAll and Amend bypass individual tracking.
                                _ => Some(action),
                            };

                            if let Some(approved) = approved_action {
                                if let Err(e) = order_router.route_action(approved).await {
                                    error!(error = %e, "failed to route order action");
                                }
                            }

                            // Break out of the action loop if kill switch was just activated.
                            if kill_switch.is_active() {
                                break;
                            }
                        }

                        // Break out of the symbol loop if kill switch was activated.
                        if kill_switch.is_active() {
                            break;
                        }
                    }

                    // Break out of the exchange loop if kill switch was activated.
                    if kill_switch.is_active() {
                        break;
                    }
                }

                // Update strategy metrics uptime.
                strategy_metrics_tracker.update_uptime(Utc::now());

                // Broadcast risk update to WebSocket dashboard clients.
                let _ = broadcast_tx.try_send(ServerMessage::RiskUpdate {
                    net_exposure: portfolio_risk.net_exposure,
                    gross_exposure: portfolio_risk.gross_exposure,
                    daily_pnl: portfolio_risk.daily_pnl,
                    kill_switch_active: kill_switch.is_active(),
                });

                // Broadcast strategy metrics to WebSocket dashboard clients.
                for m in strategy_metrics_tracker.all() {
                    let msg = ServerMessage::StrategyMetrics {
                        strategy_id: m.strategy_id.clone(),
                        metrics: m,
                    };
                    let _ = broadcast_tx.try_send(msg);
                }

                // Periodically update the gateway state with current orders, positions, and strategy metrics.
                {
                    let open = order_manager.get_open_orders();
                    *api_state.orders.write().await = open;
                }
                {
                    let pos_map = positions.read().await;
                    *api_state.positions.write().await = pos_map.values().cloned().collect();
                }
                {
                    let all_metrics = strategy_metrics_tracker.all();
                    let mut sm = api_state.strategy_metrics.write().await;
                    sm.clear();
                    for m in all_metrics {
                        sm.insert(m.strategy_id.clone(), m);
                    }
                }
            }

            // --- Position reconciliation ---
            _ = reconciliation_timer.tick(), if reconciliation_enabled => {
                debug!("running position reconciliation");

                let pos_snapshot = positions.read().await;
                let active_positions: Vec<_> = pos_snapshot
                    .iter()
                    .filter(|(_, pos)| !pos.is_flat())
                    .map(|(key, pos)| (key.clone(), pos.clone()))
                    .collect();
                drop(pos_snapshot);

                if active_positions.is_empty() {
                    debug!("no active positions to reconcile");
                } else {
                    for ((exchange, _symbol), local_pos) in &active_positions {
                        // Fetch balances from the exchange.
                        let balances = match order_router.fetch_balances(*exchange).await {
                            Ok(b) => b,
                            Err(e) => {
                                warn!(
                                    %exchange,
                                    error = %e,
                                    "reconciliation: failed to fetch balances"
                                );
                                continue;
                            }
                        };

                        // Find the matching balance for the base currency of this position.
                        let base_currency = &local_pos.symbol.base;
                        let exchange_qty = balances
                            .iter()
                            .find(|b| b.currency.eq_ignore_ascii_case(base_currency))
                            .map(|b| b.total)
                            .unwrap_or(Decimal::ZERO);

                        let recon = PositionReconciliation::check(local_pos, exchange_qty);

                        if recon.drift_pct >= drift_error_threshold {
                            error!(
                                exchange = %recon.exchange,
                                symbol = %recon.symbol,
                                local_qty = %recon.local_quantity,
                                exchange_qty = %recon.exchange_quantity,
                                drift = %recon.drift,
                                drift_pct = %recon.drift_pct,
                                "SEVERE position drift detected — local and exchange out of sync"
                            );
                        } else if recon.drift_pct >= drift_warn_threshold {
                            warn!(
                                exchange = %recon.exchange,
                                symbol = %recon.symbol,
                                local_qty = %recon.local_quantity,
                                exchange_qty = %recon.exchange_quantity,
                                drift = %recon.drift,
                                drift_pct = %recon.drift_pct,
                                "position drift detected"
                            );
                        } else {
                            info!(
                                exchange = %recon.exchange,
                                symbol = %recon.symbol,
                                drift_pct = %recon.drift_pct,
                                "position reconciled"
                            );
                        }
                    }
                }
            }

            // --- Shutdown signal ---
            _ = shutdown_rx.recv() => {
                info!("received shutdown signal, initiating graceful shutdown");
                break;
            }
        }
    }

    // ----- (l) Graceful shutdown -----
    info!("initiating graceful shutdown sequence");

    // (l.1) Stop accepting new ticks — strategies will no longer fire.
    let stop_actions = strategy_engine_shared.lock().await.stop_all();
    info!(
        cancel_actions = stop_actions.len(),
        "strategies stopped, routing final cancel actions"
    );
    for action in stop_actions {
        if let Err(e) = order_router.route_action(action).await {
            error!(error = %e, "failed to route shutdown action");
        }
    }

    // (l.2) Drain remaining order updates so OMS state is current.
    let mut drained = 0u64;
    while let Ok(update) = order_update_rx.try_recv() {
        handle_order_update(
            &update,
            &order_manager,
            &strategy_engine_shared,
            &positions,
            &daily_pnl,
            &api_state,
            event_publisher.as_ref(),
            &mut strategy_metrics_tracker,
            &pnl_limit_check,
            &metrics,
            &broadcast_tx,
        )
        .await;
        drained += 1;
    }
    info!(drained, "drained remaining order updates");

    // (l.3) Cancel all open orders individually so each exchange receives
    //       an explicit cancel per order.
    let open_orders = order_manager.get_open_orders();
    info!(
        count = open_orders.len(),
        "cancelling open orders on shutdown"
    );
    for order in &open_orders {
        let cancel = OrderAction::Cancel(CancelOrder {
            exchange: order.exchange,
            symbol: order.symbol.clone(),
            client_order_id: order.client_order_id.clone(),
            exchange_order_id: order.exchange_order_id.clone(),
        });
        if let Err(e) = order_router.route_action(cancel).await {
            warn!(
                order_id = %order.client_order_id,
                error = %e,
                "failed to cancel order during shutdown"
            );
        }
    }

    // Also send a blanket CancelAll as a safety net.
    let cancel_all = CancelAllOrders {
        exchange: None,
        symbol: None,
        strategy_id: None,
    };
    if let Err(e) = order_router
        .route_action(OrderAction::CancelAll(cancel_all))
        .await
    {
        error!(error = %e, "failed to cancel all orders during shutdown");
    }

    // (l.4) Wait for cancel confirmations (up to 5 seconds).
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut confirmations = 0u64;
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), order_update_rx.recv()).await {
            Ok(Some(update)) => {
                handle_order_update(
                    &update,
                    &order_manager,
                    &strategy_engine_shared,
                    &positions,
                    &daily_pnl,
                    &api_state,
                    event_publisher.as_ref(),
                    &mut strategy_metrics_tracker,
                    &pnl_limit_check,
                    &metrics,
                    &broadcast_tx,
                )
                .await;
                confirmations += 1;
            }
            _ => break,
        }
    }
    info!(
        confirmations,
        "received cancel confirmations during shutdown"
    );

    // (l.5) Save state checkpoint — positions and any remaining open orders.
    {
        let pos_snapshot = positions.read().await;
        let final_positions: Vec<Position> = pos_snapshot.values().cloned().collect();
        drop(pos_snapshot);

        let remaining_open = order_manager.get_open_orders();
        let final_daily_pnl = *daily_pnl.read().await;

        let checkpoint = ShutdownCheckpoint {
            positions: final_positions.clone(),
            open_orders: remaining_open.clone(),
            timestamp: Utc::now(),
        };

        // Ensure the data directory exists.
        if let Some(parent) = Path::new(SHUTDOWN_CHECKPOINT_PATH).parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                error!(error = %e, "failed to create checkpoint directory");
            }
        }

        match serde_json::to_string_pretty(&checkpoint) {
            Ok(json) => {
                if let Err(e) = std::fs::write(SHUTDOWN_CHECKPOINT_PATH, json) {
                    error!(
                        error = %e,
                        path = SHUTDOWN_CHECKPOINT_PATH,
                        "failed to save shutdown checkpoint"
                    );
                } else {
                    info!(path = SHUTDOWN_CHECKPOINT_PATH, "saved shutdown checkpoint");
                }
            }
            Err(e) => {
                error!(error = %e, "failed to serialize shutdown checkpoint");
            }
        }

        // (l.6) Log final summary.
        info!(
            positions = final_positions.len(),
            open_orders = remaining_open.len(),
            daily_pnl = %final_daily_pnl,
            "shutdown summary"
        );
        for pos in &final_positions {
            if !pos.is_flat() {
                info!(
                    exchange = %pos.exchange,
                    symbol = %pos.symbol,
                    quantity = %pos.quantity,
                    realized_pnl = %pos.realized_pnl,
                    unrealized_pnl = %pos.unrealized_pnl,
                    "final position"
                );
            }
        }
    }

    // Disconnect execution connectors.
    if let Err(e) = order_router.disconnect_all().await {
        error!(error = %e, "error disconnecting execution connectors");
    }

    // Disconnect market data.
    md_manager.disconnect_all().await;

    info!("trading-core shutdown complete");
    Ok(())
}

// ---------------------------------------------------------------------------
// Shutdown signal
// ---------------------------------------------------------------------------

/// Wait for either SIGINT (Ctrl-C) or SIGTERM, then return.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("received SIGINT");
        }
        _ = terminate => {
            info!("received SIGTERM");
        }
    }
}

// ---------------------------------------------------------------------------
// Order update handler
// ---------------------------------------------------------------------------

/// Process an incoming order update: update the OMS, notify the strategy
/// engine, and update positions on fills.
#[allow(clippy::too_many_arguments)]
#[instrument(
    skip(
        order_manager,
        strategy_engine,
        positions,
        daily_pnl,
        _api_state,
        event_publisher,
        strategy_metrics_tracker,
        pnl_limit_check,
        metrics,
        broadcast_tx
    ),
    fields(
        exchange = %update.exchange,
        symbol = %update.symbol,
        client_order_id = %update.client_order_id,
        status = %update.status
    )
)]
async fn handle_order_update(
    update: &OrderUpdate,
    order_manager: &Arc<OrderManager>,
    strategy_engine: &Arc<tokio::sync::Mutex<StrategyEngine>>,
    positions: &Arc<RwLock<PositionMap>>,
    daily_pnl: &Arc<RwLock<Decimal>>,
    _api_state: &Arc<ApiState>,
    event_publisher: Option<&Arc<dyn EventPublisher>>,
    strategy_metrics_tracker: &mut StrategyMetricsTracker,
    pnl_limit_check: &Arc<PnlLimitCheck>,
    metrics: &Arc<MetricsRegistry>,
    broadcast_tx: &mpsc::Sender<ServerMessage>,
) {
    // Apply the update to the OMS.
    let updated_order = order_manager.update_order(update);

    if let Some(ref order) = updated_order {
        // Route order update to the owning strategy.
        strategy_engine
            .lock()
            .await
            .route_order_update(&order.strategy_id, update);

        // Broadcast order update to WebSocket dashboard clients.
        let _ = broadcast_tx.try_send(ServerMessage::OrderUpdate {
            client_order_id: update.client_order_id.clone(),
            exchange: update.exchange,
            symbol: update.symbol.clone(),
            status: format!("{:?}", update.status),
            filled_quantity: order.filled_quantity,
            remaining_quantity: order.remaining_quantity,
        });

        // Handle fills: update positions.
        if matches!(
            update.status,
            OrderStatus::PartiallyFilled | OrderStatus::Filled
        ) {
            if let (Some(fill_price), Some(fill_qty)) =
                (update.last_fill_price, update.last_fill_quantity)
            {
                let fill = Fill {
                    fill_id: Uuid::new_v4().to_string(),
                    client_order_id: update.client_order_id.clone(),
                    exchange_order_id: update.exchange_order_id.clone().unwrap_or_default(),
                    exchange: update.exchange,
                    symbol: update.symbol.clone(),
                    side: order.side,
                    price: fill_price,
                    quantity: fill_qty,
                    fee: update.fee.unwrap_or(Decimal::ZERO),
                    fee_currency: update.fee_currency.clone().unwrap_or_default(),
                    timestamp: update.timestamp,
                };

                // Record fill metrics.
                {
                    let exchange_label = update.exchange.as_str();
                    let side_label = order.side.as_str();
                    metrics
                        .fills_total
                        .with_label_values(&[exchange_label, side_label])
                        .inc();
                }

                // Update position.
                {
                    let mut pos_map = positions.write().await;
                    let pos = pos_map
                        .entry((update.exchange, update.symbol.clone()))
                        .or_insert_with(|| Position::new(update.exchange, update.symbol.clone()));
                    let prev_realized = pos.realized_pnl;
                    pos.update_on_fill(&fill);

                    // Record position and PnL metrics after the fill.
                    {
                        let exchange_label = update.exchange.as_str();
                        let symbol_label = update.symbol.canonical();
                        metrics
                            .position_quantity
                            .with_label_values(&[exchange_label, &symbol_label])
                            .set(pos.quantity.to_f64().unwrap_or(0.0));
                        metrics
                            .pnl_realized
                            .with_label_values(&[exchange_label, &symbol_label])
                            .set(pos.realized_pnl.to_f64().unwrap_or(0.0));
                        metrics
                            .pnl_unrealized
                            .with_label_values(&[exchange_label, &symbol_label])
                            .set(pos.unrealized_pnl.to_f64().unwrap_or(0.0));
                    }

                    // Broadcast position update to WebSocket dashboard clients.
                    let _ = broadcast_tx.try_send(ServerMessage::PositionUpdate {
                        exchange: pos.exchange,
                        symbol: pos.symbol.clone(),
                        quantity: pos.quantity,
                        unrealized_pnl: pos.unrealized_pnl,
                        realized_pnl: pos.realized_pnl,
                    });

                    // Accumulate the realized PnL delta from this fill.
                    let realized_delta = pos.realized_pnl - prev_realized;
                    drop(pos_map);
                    if realized_delta != Decimal::ZERO {
                        *daily_pnl.write().await += realized_delta;
                    }

                    // Feed realized PnL delta into the PnlLimitCheck's internal
                    // tracker so drawdown / daily-loss checks work correctly.
                    if realized_delta != Decimal::ZERO {
                        pnl_limit_check.record_pnl(realized_delta);
                    }
                }

                // Route fill to strategy.
                strategy_engine
                    .lock()
                    .await
                    .route_fill(&order.strategy_id, &fill);

                // Track fill in strategy metrics.
                strategy_metrics_tracker.on_fill(&order.strategy_id, &fill);

                info!(
                    client_order_id = %update.client_order_id,
                    exchange = %update.exchange,
                    symbol = %update.symbol,
                    side = %order.side,
                    fill_price = %fill_price,
                    fill_qty = %fill_qty,
                    status = %update.status,
                    "order fill processed"
                );

                // Publish fill event.
                if let Some(publisher) = event_publisher {
                    let event = if update.status == OrderStatus::Filled {
                        Event::OrderFilled {
                            client_order_id: update.client_order_id.clone(),
                            exchange_order_id: update.exchange_order_id.clone().unwrap_or_default(),
                            exchange: update.exchange,
                            symbol: update.symbol.clone(),
                            fill_price,
                            fill_quantity: fill_qty,
                            total_quantity: order.quantity,
                            fee: update.fee.unwrap_or(Decimal::ZERO),
                            fee_currency: update.fee_currency.clone().unwrap_or_default(),
                        }
                    } else {
                        Event::OrderPartiallyFilled {
                            client_order_id: update.client_order_id.clone(),
                            exchange_order_id: update.exchange_order_id.clone().unwrap_or_default(),
                            exchange: update.exchange,
                            symbol: update.symbol.clone(),
                            fill_price,
                            fill_quantity: fill_qty,
                            cumulative_quantity: update.filled_quantity,
                            remaining_quantity: update.remaining_quantity,
                            fee: update.fee.unwrap_or(Decimal::ZERO),
                            fee_currency: update.fee_currency.clone().unwrap_or_default(),
                        }
                    };

                    let envelope =
                        Envelope::new("trading-core", update.client_order_id.clone(), event);
                    if let Err(e) = publisher.publish(envelope).await {
                        error!(error = %e, "failed to publish fill event");
                    }
                }
            }
        }
    } else {
        warn!(
            client_order_id = %update.client_order_id,
            "OMS rejected order update"
        );
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use mm_core::types::{Exchange, OrderStatus, OrderType, Side, Symbol, TimeInForce};

    fn sample_position() -> Position {
        Position {
            exchange: Exchange::Binance,
            symbol: Symbol {
                base: "BTC".to_string(),
                quote: "USDT".to_string(),
            },
            quantity: Decimal::new(15, 1), // 1.5
            average_entry_price: Decimal::new(42000, 0),
            unrealized_pnl: Decimal::new(150, 0),
            realized_pnl: Decimal::new(50, 0),
            updated_at: Utc::now(),
        }
    }

    fn sample_order() -> Order {
        Order {
            client_order_id: "test-order-001".to_string(),
            exchange_order_id: Some("ex-123".to_string()),
            exchange: Exchange::Binance,
            symbol: Symbol {
                base: "BTC".to_string(),
                quote: "USDT".to_string(),
            },
            side: Side::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            price: Some(Decimal::new(41500, 0)),
            quantity: Decimal::new(1, 0),
            status: OrderStatus::New,
            filled_quantity: Decimal::ZERO,
            remaining_quantity: Decimal::new(1, 0),
            average_fill_price: Decimal::ZERO,
            strategy_id: "spread_btc".to_string(),
            reduce_only: false,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn test_shutdown_checkpoint_serialization() {
        let checkpoint = ShutdownCheckpoint {
            positions: vec![sample_position()],
            open_orders: vec![sample_order()],
            timestamp: Utc::now(),
        };

        let json = serde_json::to_string_pretty(&checkpoint).expect("serialization failed");

        // Verify the JSON contains expected fields.
        assert!(json.contains("positions"));
        assert!(json.contains("open_orders"));
        assert!(json.contains("timestamp"));
        assert!(json.contains("BTC"));
        assert!(json.contains("USDT"));
        assert!(json.contains("test-order-001"));
    }

    #[test]
    fn test_shutdown_checkpoint_round_trip() {
        let original = ShutdownCheckpoint {
            positions: vec![sample_position()],
            open_orders: vec![sample_order()],
            timestamp: Utc::now(),
        };

        let json = serde_json::to_string_pretty(&original).expect("serialization failed");
        let deserialized: ShutdownCheckpoint =
            serde_json::from_str(&json).expect("deserialization failed");

        // Verify positions round-trip.
        assert_eq!(deserialized.positions.len(), 1);
        assert_eq!(deserialized.positions[0].exchange, Exchange::Binance);
        assert_eq!(deserialized.positions[0].quantity, Decimal::new(15, 1));
        assert_eq!(
            deserialized.positions[0].average_entry_price,
            Decimal::new(42000, 0)
        );
        assert_eq!(deserialized.positions[0].realized_pnl, Decimal::new(50, 0));

        // Verify open orders round-trip.
        assert_eq!(deserialized.open_orders.len(), 1);
        assert_eq!(
            deserialized.open_orders[0].client_order_id,
            "test-order-001"
        );
        assert_eq!(deserialized.open_orders[0].side, Side::Buy);
        assert_eq!(deserialized.open_orders[0].status, OrderStatus::New);
        assert_eq!(
            deserialized.open_orders[0].price,
            Some(Decimal::new(41500, 0))
        );

        // Verify timestamp round-trips (to millisecond precision).
        let delta = (original.timestamp - deserialized.timestamp)
            .num_milliseconds()
            .abs();
        assert!(delta < 1000, "timestamp drift too large: {delta}ms");
    }

    #[test]
    fn test_shutdown_checkpoint_empty_state() {
        let checkpoint = ShutdownCheckpoint {
            positions: vec![],
            open_orders: vec![],
            timestamp: Utc::now(),
        };

        let json = serde_json::to_string(&checkpoint).expect("serialization failed");
        let deserialized: ShutdownCheckpoint =
            serde_json::from_str(&json).expect("deserialization failed");

        assert!(deserialized.positions.is_empty());
        assert!(deserialized.open_orders.is_empty());
    }

    #[test]
    fn test_shutdown_checkpoint_file_round_trip() {
        let checkpoint = ShutdownCheckpoint {
            positions: vec![sample_position()],
            open_orders: vec![sample_order()],
            timestamp: Utc::now(),
        };

        let dir = std::env::temp_dir().join("trading_core_test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test_checkpoint.json");

        // Write checkpoint to file.
        let json = serde_json::to_string_pretty(&checkpoint).expect("serialization failed");
        std::fs::write(&path, &json).expect("write failed");

        // Read it back.
        let read_json = std::fs::read_to_string(&path).expect("read failed");
        let loaded: ShutdownCheckpoint =
            serde_json::from_str(&read_json).expect("deserialization failed");

        assert_eq!(loaded.positions.len(), 1);
        assert_eq!(loaded.open_orders.len(), 1);
        assert_eq!(
            loaded.positions[0].exchange,
            checkpoint.positions[0].exchange
        );
        assert_eq!(
            loaded.open_orders[0].client_order_id,
            checkpoint.open_orders[0].client_order_id
        );

        // Cleanup.
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }
}
