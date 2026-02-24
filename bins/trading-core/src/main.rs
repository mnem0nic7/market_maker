//! trading-core — main binary for the automated market making platform.
//!
//! Wires together market data ingestion, strategy execution, risk management,
//! order routing, and the REST/WebSocket gateway into a single process with a
//! tight event-driven main loop.

mod config;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

use mm_core::{
    events::{Envelope, Event, OrderUpdate},
    traits::EventPublisher,
    types::{
        CancelAllOrders, Exchange, Fill, MarketSnapshot, OrderAction, OrderBook, Position,
        RiskAction, Symbol, Trade,
    },
    OrderStatus,
};
use mm_data_pipeline::{NatsConfig, NatsEventPublisher};
use mm_execution::{
    BinanceExecutionConfig, BinanceExecutionConnector, CoinbaseExecutionConfig,
    CoinbaseExecutionConnector, KrakenExecutionConfig, KrakenExecutionConnector, OrderManager,
    OrderRouter,
};
use mm_gateway::{ApiState, GatewayConfig, GatewayServer};
use mm_market_data::{BinanceConnector, CoinbaseConnector, KrakenConnector, MarketDataManager};
use mm_risk::{
    KillSwitch, KillSwitchConfig, OrderSizeCheck, OrderSizeConfig, PnlLimitCheck, PnlLimitConfig,
    PositionLimitCheck, PositionLimitConfig, RateLimitCheck, RateLimitConfig, RiskEngine,
};
use mm_strategy::{
    AvellanedaStoikovConfig, AvellanedaStoikovStrategy, GridConfig, GridStrategy, SpreadConfig,
    SpreadStrategy, StrategyEngine,
};

use crate::config::{parse_exchange, parse_symbol};

// ---------------------------------------------------------------------------
// Channel capacities
// ---------------------------------------------------------------------------

const BOOK_CHANNEL_CAPACITY: usize = 4096;
const TRADE_CHANNEL_CAPACITY: usize = 4096;
const ORDER_UPDATE_CHANNEL_CAPACITY: usize = 4096;

// ---------------------------------------------------------------------------
// Shared position state
// ---------------------------------------------------------------------------

type PositionMap = HashMap<(Exchange, Symbol), Position>;

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

    // ----- (b) Load configuration -----
    let cfg = config::load_config().context("failed to load configuration")?;
    info!(?cfg.trading_loop, "configuration loaded");

    // ----- (c) TradingConfig is already parsed by the loader -----

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
    let mut order_router = OrderRouter::new(Arc::clone(&order_manager), ORDER_UPDATE_CHANNEL_CAPACITY);

    // We also need a separate order-update channel for the main loop to
    // process order updates (the router has its own internal one for the OMS,
    // but we need to observe updates too).
    let (_order_update_tx, mut order_update_rx) = mpsc::channel::<OrderUpdate>(ORDER_UPDATE_CHANNEL_CAPACITY);

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
                md_manager.register_connector(Box::new(BinanceConnector::new()));
                order_router.register_connector(Box::new(BinanceExecutionConnector::new(
                    BinanceExecutionConfig {
                        api_key: exchange_cfg.api_key.clone(),
                        secret_key: exchange_cfg.secret_key.clone(),
                        base_url: "https://api.binance.com".into(),
                        ws_url: "wss://stream.binance.com:9443".into(),
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
            _ => {
                warn!(%exchange, "exchange not supported for live trading, skipping");
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

    // Connect all execution connectors (starts their WebSocket user-data
    // streams and the internal update loop).
    if let Err(e) = order_router.connect_all().await {
        error!(error = %e, "failed to connect execution connectors");
    }

    // ----- (h) Initialize strategy engine -----
    let mut strategy_engine = StrategyEngine::new();

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

        let result = match strat_cfg.strategy_type.as_str() {
            "spread" => {
                let mut spread_cfg: SpreadConfig =
                    serde_json::from_value(strat_cfg.params.clone()).unwrap_or_else(|e| {
                        warn!(error = %e, "failed to parse spread params, using defaults");
                        serde_json::from_value(serde_json::json!({
                            "strategy_id": strat_cfg.strategy_id,
                            "exchange": exchange,
                            "symbol": symbol,
                            "spread_bps": "10",
                            "order_size": "0.01",
                            "num_levels": 3,
                            "refresh_interval_ms": 1000,
                            "min_spread_bps": "5",
                            "reprice_threshold_bps": "3"
                        }))
                        .expect("default spread config should parse")
                    });
                // Ensure the strategy_id, exchange, and symbol match the
                // outer config.
                spread_cfg.strategy_id = strat_cfg.strategy_id.clone();
                spread_cfg.exchange = exchange;
                spread_cfg.symbol = symbol.clone();
                strategy_engine.add_strategy(Box::new(SpreadStrategy::new(spread_cfg)))
            }
            "avellaneda" => {
                let mut as_cfg: AvellanedaStoikovConfig =
                    serde_json::from_value(strat_cfg.params.clone()).unwrap_or_else(|e| {
                        warn!(error = %e, "failed to parse avellaneda params, using defaults");
                        serde_json::from_value(serde_json::json!({
                            "strategy_id": strat_cfg.strategy_id,
                            "exchange": exchange,
                            "symbol": symbol,
                            "gamma": "0.1",
                            "sigma": "0.02",
                            "kappa": "1.5",
                            "order_size": "0.01",
                            "max_position": "1.0",
                            "session_duration_secs": 28800
                        }))
                        .expect("default avellaneda config should parse")
                    });
                as_cfg.strategy_id = strat_cfg.strategy_id.clone();
                as_cfg.exchange = exchange;
                as_cfg.symbol = symbol.clone();
                strategy_engine.add_strategy(Box::new(AvellanedaStoikovStrategy::new(as_cfg)))
            }
            "grid" => {
                let mut grid_cfg: GridConfig =
                    serde_json::from_value(strat_cfg.params.clone()).unwrap_or_else(|e| {
                        warn!(error = %e, "failed to parse grid params, using defaults");
                        serde_json::from_value(serde_json::json!({
                            "strategy_id": strat_cfg.strategy_id,
                            "exchange": exchange,
                            "symbol": symbol,
                            "upper_price": "70000",
                            "lower_price": "60000",
                            "num_grids": 10,
                            "order_size": "0.01",
                            "grid_type": "Arithmetic"
                        }))
                        .expect("default grid config should parse")
                    });
                grid_cfg.strategy_id = strat_cfg.strategy_id.clone();
                grid_cfg.exchange = exchange;
                grid_cfg.symbol = symbol.clone();
                strategy_engine.add_strategy(Box::new(GridStrategy::new(grid_cfg)))
            }
            unknown => {
                warn!(
                    strategy_id = %strat_cfg.strategy_id,
                    strategy_type = %unknown,
                    "unknown strategy type, skipping"
                );
                continue;
            }
        };

        match result {
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

    risk_engine.add_check(Box::new(PnlLimitCheck::new(PnlLimitConfig {
        max_daily_loss: cfg.risk.pnl_limits.max_daily_loss,
        max_drawdown_pct: cfg.risk.pnl_limits.max_drawdown_pct,
    })));

    risk_engine.add_check(Box::new(RateLimitCheck::new(RateLimitConfig {
        max_orders_per_second: cfg.risk.rate_limits.max_orders_per_second,
        max_orders_per_minute: cfg.risk.rate_limits.max_orders_per_minute,
    })));

    info!(check_count = risk_engine.check_count(), "risk engine initialized");

    // ----- (j) Initialize gateway (REST API + WebSocket) -----
    let api_state = ApiState::new();

    if cfg.gateway.enabled {
        let gw_config = GatewayConfig {
            host: cfg.gateway.host.clone(),
            port: cfg.gateway.port,
        };
        let gw_state = Arc::clone(&api_state);
        tokio::spawn(async move {
            let server = GatewayServer::new(gw_config, gw_state);
            if let Err(e) = server.start().await {
                error!(error = %e, "gateway server exited with error");
            }
        });
        info!(host = %cfg.gateway.host, port = cfg.gateway.port, "gateway started");
    } else {
        info!("gateway disabled");
    }

    // ----- (k) Main trading loop -----
    let tick_interval = Duration::from_millis(cfg.trading_loop.tick_interval_ms);
    let mut tick_timer = tokio::time::interval(tick_interval);
    tick_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Latest order book and trade snapshots for building MarketSnapshot.
    let mut latest_books: HashMap<(Exchange, String), OrderBook> = HashMap::new();
    let mut latest_trades: HashMap<(Exchange, String), Trade> = HashMap::new();

    // Graceful shutdown signal.
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

    // Spawn a task that waits for SIGINT or SIGTERM and signals the main loop.
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = shutdown_tx.send(()).await;
    });

    info!("entering main trading loop (tick interval: {}ms)", cfg.trading_loop.tick_interval_ms);

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

                // Update the gateway state.
                {
                    let mut obs = api_state.order_books.write().await;
                    obs.insert(key.clone(), book.clone());
                }

                latest_books.insert(key, book);
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
                    &mut strategy_engine,
                    &positions,
                    &daily_pnl,
                    &api_state,
                    event_publisher.as_ref(),
                ).await;
            }

            // --- Tick interval: run strategy engine ---
            _ = tick_timer.tick() => {
                // Record heartbeat for kill switch.
                kill_switch.record_heartbeat();

                // Check kill switch — if active, do not tick strategies.
                if kill_switch.is_active() {
                    debug!("kill switch active, skipping strategy tick");
                    continue;
                }

                // Build portfolio risk snapshot for risk checks.
                let pos_snapshot = positions.read().await;
                let positions_vec: Vec<Position> = pos_snapshot.values().cloned().collect();
                let current_daily_pnl = *daily_pnl.read().await;
                drop(pos_snapshot);

                let portfolio_risk = RiskEngine::build_portfolio_risk(
                    &positions_vec,
                    current_daily_pnl,
                );

                // Check kill switch triggers.
                if let Some(activation) = kill_switch.check_triggers(&portfolio_risk, &[]) {
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
                    let stop_actions = strategy_engine.stop_all();
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

                    continue;
                }

                // For each subscribed (exchange, symbol), build a snapshot and tick.
                for (exchange, symbols) in &subscribed_pairs {
                    for symbol in symbols {
                        let key = (*exchange, symbol.canonical());

                        // We need a valid order book to run the strategy.
                        let book = match latest_books.get(&key) {
                            Some(b) => b.clone(),
                            None => {
                                debug!(
                                    exchange = %exchange,
                                    symbol = %symbol,
                                    "no order book yet, skipping"
                                );
                                continue;
                            }
                        };

                        let last_trade = latest_trades.get(&key).cloned();

                        let position = {
                            let pos_map = positions.read().await;
                            pos_map
                                .get(&(*exchange, symbol.clone()))
                                .cloned()
                                .unwrap_or_else(|| Position::new(*exchange, symbol.clone()))
                        };

                        let open_orders = order_manager.get_open_orders();
                        // Filter to orders for this exchange/symbol.
                        let relevant_orders: Vec<_> = open_orders
                            .into_iter()
                            .filter(|o| o.exchange == *exchange && o.symbol == *symbol)
                            .collect();

                        let snapshot = MarketSnapshot {
                            exchange: *exchange,
                            symbol: symbol.clone(),
                            order_book: book,
                            last_trade,
                            position,
                            open_orders: relevant_orders,
                            timestamp: Utc::now(),
                        };

                        // Run strategies.
                        let actions = strategy_engine.tick(&snapshot);

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

                                    match risk_result {
                                        RiskAction::Allow => Some(action),
                                        RiskAction::Reject { reason } => {
                                            warn!(
                                                reason = %reason,
                                                "order rejected by risk engine"
                                            );
                                            None
                                        }
                                        RiskAction::ReduceSize { max_quantity, reason } => {
                                            info!(
                                                max_quantity = %max_quantity,
                                                reason = %reason,
                                                "risk engine reducing order size"
                                            );
                                            let mut reduced = new_order.clone();
                                            reduced.quantity = max_quantity;
                                            Some(OrderAction::Place(reduced))
                                        }
                                        RiskAction::KillSwitch { reason } => {
                                            error!(
                                                reason = %reason,
                                                "risk check triggered kill switch"
                                            );
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
                                            strategy_engine.stop_all();
                                            *api_state.kill_switch_active.lock().await = true;
                                            None
                                        }
                                    }
                                }
                                // Cancel and CancelAll actions bypass risk checks.
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

                // Periodically update the gateway state with current orders and positions.
                {
                    let open = order_manager.get_open_orders();
                    *api_state.orders.write().await = open;
                }
                {
                    let pos_map = positions.read().await;
                    *api_state.positions.write().await = pos_map.values().cloned().collect();
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
    info!("shutting down...");

    // Stop all strategies — they may emit final cancel actions.
    let stop_actions = strategy_engine.stop_all();
    info!(
        cancel_actions = stop_actions.len(),
        "strategies stopped, routing final cancel actions"
    );
    for action in stop_actions {
        if let Err(e) = order_router.route_action(action).await {
            error!(error = %e, "failed to route shutdown action");
        }
    }

    // Cancel any remaining open orders across all exchanges.
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

    // Give the exchange a moment to process cancellations.
    tokio::time::sleep(Duration::from_secs(2)).await;

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
async fn handle_order_update(
    update: &OrderUpdate,
    order_manager: &Arc<OrderManager>,
    strategy_engine: &mut StrategyEngine,
    positions: &Arc<RwLock<PositionMap>>,
    daily_pnl: &Arc<RwLock<Decimal>>,
    _api_state: &Arc<ApiState>,
    event_publisher: Option<&Arc<dyn EventPublisher>>,
) {
    // Apply the update to the OMS.
    let updated_order = order_manager.update_order(update);

    if let Some(ref order) = updated_order {
        // Route order update to the owning strategy.
        strategy_engine.route_order_update(&order.strategy_id, update);

        // Handle fills: update positions.
        if matches!(
            update.status,
            OrderStatus::PartiallyFilled | OrderStatus::Filled
        ) {
            if let (Some(fill_price), Some(fill_qty)) =
                (update.last_fill_price, update.last_fill_quantity)
            {
                let fill = Fill {
                    fill_id: format!("{}-{}", update.client_order_id, Utc::now().timestamp_millis()),
                    client_order_id: update.client_order_id.clone(),
                    exchange_order_id: update
                        .exchange_order_id
                        .clone()
                        .unwrap_or_default(),
                    exchange: update.exchange,
                    symbol: update.symbol.clone(),
                    side: order.side,
                    price: fill_price,
                    quantity: fill_qty,
                    fee: update.fee.unwrap_or(Decimal::ZERO),
                    fee_currency: update.fee_currency.clone().unwrap_or_default(),
                    timestamp: update.timestamp,
                };

                // Update position.
                {
                    let mut pos_map = positions.write().await;
                    let pos = pos_map
                        .entry((update.exchange, update.symbol.clone()))
                        .or_insert_with(|| Position::new(update.exchange, update.symbol.clone()));
                    pos.update_on_fill(&fill);

                    // Track realized PnL.
                    let realized = pos.realized_pnl;
                    drop(pos_map);
                    *daily_pnl.write().await = realized;
                }

                // Route fill to strategy.
                strategy_engine.route_fill(&order.strategy_id, &fill);

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

                    let envelope = Envelope::new(
                        "trading-core",
                        update.client_order_id.clone(),
                        event,
                    );
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
