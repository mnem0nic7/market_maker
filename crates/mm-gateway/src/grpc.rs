//! gRPC service implementations for the market maker gateway.
//!
//! Each service delegates to the shared [`ApiState`] for reads and writes,
//! keeping the gRPC layer thin. The generated proto types live in sub-modules
//! produced by `tonic-build` in `build.rs`.

use std::sync::Arc;

use chrono::Utc;
use tonic::{Request, Response, Status};

use crate::rest::{
    build_strategy, build_strategy_lifecycle_info, build_strategy_risk_summary,
    route_strategy_stop_actions, ApiState, StrategyStatus,
};

// ---------------------------------------------------------------------------
// Generated protobuf modules
// ---------------------------------------------------------------------------

pub mod pb {
    pub mod common {
        tonic::include_proto!("mm.common");
    }
    pub mod market_data {
        tonic::include_proto!("mm.market_data");
    }
    pub mod execution {
        tonic::include_proto!("mm.execution");
    }
    pub mod risk {
        tonic::include_proto!("mm.risk");
    }
    pub mod strategy {
        tonic::include_proto!("mm.strategy");
    }
}

// ---------------------------------------------------------------------------
// MarketDataService
// ---------------------------------------------------------------------------

pub struct MarketDataServiceImpl {
    state: Arc<ApiState>,
}

impl MarketDataServiceImpl {
    pub fn new(state: Arc<ApiState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl pb::market_data::market_data_service_server::MarketDataService for MarketDataServiceImpl {
    async fn get_order_book(
        &self,
        request: Request<pb::market_data::GetOrderBookRequest>,
    ) -> Result<Response<pb::market_data::GetOrderBookResponse>, Status> {
        let req = request.into_inner();
        let symbol = req
            .symbol
            .ok_or_else(|| Status::invalid_argument("symbol is required"))?;

        let exchange_enum = map_exchange(req.exchange)?;
        let canonical = format!(
            "{}/{}",
            symbol.base.to_uppercase(),
            symbol.quote.to_uppercase()
        );

        let books = self.state.order_books.read().await;
        let book = books
            .get(&(exchange_enum, canonical))
            .ok_or_else(|| Status::not_found("order book not found"))?;

        let depth = if req.depth == 0 {
            usize::MAX
        } else {
            req.depth as usize
        };

        let pb_book = pb::common::OrderBook {
            exchange: req.exchange,
            symbol: Some(symbol),
            bids: book
                .bids
                .iter()
                .take(depth)
                .map(|l| pb::common::PriceLevel {
                    price: l.price.to_string(),
                    quantity: l.quantity.to_string(),
                })
                .collect(),
            asks: book
                .asks
                .iter()
                .take(depth)
                .map(|l| pb::common::PriceLevel {
                    price: l.price.to_string(),
                    quantity: l.quantity.to_string(),
                })
                .collect(),
            timestamp_ms: book.timestamp.timestamp_millis(),
            sequence: book.sequence,
        };

        Ok(Response::new(pb::market_data::GetOrderBookResponse {
            order_book: Some(pb_book),
        }))
    }

    type StreamOrderBookStream =
        futures::stream::BoxStream<'static, Result<pb::common::OrderBook, Status>>;

    async fn stream_order_book(
        &self,
        _request: Request<pb::market_data::StreamOrderBookRequest>,
    ) -> Result<Response<Self::StreamOrderBookStream>, Status> {
        // Streaming will be connected to the live market data pipeline.
        // For now, return an empty stream as a placeholder.
        let stream = futures::stream::empty();
        Ok(Response::new(Box::pin(stream)))
    }

    type StreamTradesStream =
        futures::stream::BoxStream<'static, Result<pb::common::Trade, Status>>;

    async fn stream_trades(
        &self,
        _request: Request<pb::market_data::StreamTradesRequest>,
    ) -> Result<Response<Self::StreamTradesStream>, Status> {
        let stream = futures::stream::empty();
        Ok(Response::new(Box::pin(stream)))
    }
}

// ---------------------------------------------------------------------------
// ExecutionService
// ---------------------------------------------------------------------------

pub struct ExecutionServiceImpl {
    state: Arc<ApiState>,
}

impl ExecutionServiceImpl {
    pub fn new(state: Arc<ApiState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl pb::execution::execution_service_server::ExecutionService for ExecutionServiceImpl {
    async fn submit_order(
        &self,
        _request: Request<pb::execution::SubmitOrderRequest>,
    ) -> Result<Response<pb::execution::SubmitOrderResponse>, Status> {
        // Order submission will be wired to the OrderRouter in trading-core.
        // The gateway gRPC layer delegates to the shared state / command channel.
        Err(Status::unimplemented(
            "order submission via gRPC will be connected to the OrderRouter",
        ))
    }

    async fn cancel_order(
        &self,
        _request: Request<pb::execution::CancelOrderRequest>,
    ) -> Result<Response<pb::execution::CancelOrderResponse>, Status> {
        Err(Status::unimplemented(
            "order cancellation via gRPC will be connected to the OrderRouter",
        ))
    }

    async fn cancel_all_orders(
        &self,
        _request: Request<pb::execution::CancelAllOrdersRequest>,
    ) -> Result<Response<pb::execution::CancelAllOrdersResponse>, Status> {
        Err(Status::unimplemented(
            "cancel all via gRPC will be connected to the OrderRouter",
        ))
    }

    async fn get_open_orders(
        &self,
        _request: Request<pb::execution::GetOpenOrdersRequest>,
    ) -> Result<Response<pb::execution::GetOpenOrdersResponse>, Status> {
        let orders = self.state.orders.read().await;

        let pb_orders: Vec<pb::common::Order> = orders
            .iter()
            .map(|o| pb::common::Order {
                client_order_id: o.client_order_id.clone(),
                exchange_order_id: o.exchange_order_id.clone().unwrap_or_default(),
                exchange: exchange_to_pb(o.exchange),
                symbol: Some(pb::common::Symbol {
                    base: o.symbol.base.clone(),
                    quote: o.symbol.quote.clone(),
                }),
                side: side_to_pb(o.side),
                order_type: order_type_to_pb(o.order_type),
                time_in_force: tif_to_pb(o.time_in_force),
                price: o.price.map(|p| p.to_string()).unwrap_or_default(),
                quantity: o.quantity.to_string(),
                status: order_status_to_pb(o.status),
                filled_quantity: o.filled_quantity.to_string(),
                remaining_quantity: o.remaining_quantity.to_string(),
                average_fill_price: o.average_fill_price.to_string(),
                strategy_id: o.strategy_id.clone(),
                created_at_ms: o.created_at.timestamp_millis(),
                updated_at_ms: o.updated_at.timestamp_millis(),
            })
            .collect();

        Ok(Response::new(pb::execution::GetOpenOrdersResponse {
            orders: pb_orders,
        }))
    }

    type StreamOrderUpdatesStream =
        futures::stream::BoxStream<'static, Result<pb::execution::OrderUpdate, Status>>;

    async fn stream_order_updates(
        &self,
        _request: Request<pb::execution::StreamOrderUpdatesRequest>,
    ) -> Result<Response<Self::StreamOrderUpdatesStream>, Status> {
        let stream = futures::stream::empty();
        Ok(Response::new(Box::pin(stream)))
    }
}

// ---------------------------------------------------------------------------
// RiskService
// ---------------------------------------------------------------------------

pub struct RiskServiceImpl {
    state: Arc<ApiState>,
}

impl RiskServiceImpl {
    pub fn new(state: Arc<ApiState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl pb::risk::risk_service_server::RiskService for RiskServiceImpl {
    async fn evaluate_order(
        &self,
        _request: Request<pb::risk::EvaluateOrderRequest>,
    ) -> Result<Response<pb::risk::EvaluateOrderResponse>, Status> {
        // Risk evaluation via gRPC will be connected to the RiskEngine.
        Err(Status::unimplemented(
            "order evaluation via gRPC will be connected to the RiskEngine",
        ))
    }

    async fn get_portfolio_risk(
        &self,
        _request: Request<pb::risk::GetPortfolioRiskRequest>,
    ) -> Result<Response<pb::risk::GetPortfolioRiskResponse>, Status> {
        let risk = self.state.portfolio_risk.read().await;

        match risk.as_ref() {
            Some(r) => {
                let pb_risk = pb::common::PortfolioRisk {
                    total_var_95: r.total_var_95.to_string(),
                    total_var_99: r.total_var_99.to_string(),
                    net_exposure: r.net_exposure.to_string(),
                    gross_exposure: r.gross_exposure.to_string(),
                    max_drawdown: r.max_drawdown.to_string(),
                    daily_pnl: r.daily_pnl.to_string(),
                    positions: r
                        .positions
                        .iter()
                        .map(|p| pb::common::Position {
                            exchange: exchange_to_pb(p.exchange),
                            symbol: Some(pb::common::Symbol {
                                base: p.symbol.base.clone(),
                                quote: p.symbol.quote.clone(),
                            }),
                            quantity: p.quantity.to_string(),
                            average_entry_price: p.average_entry_price.to_string(),
                            unrealized_pnl: p.unrealized_pnl.to_string(),
                            realized_pnl: p.realized_pnl.to_string(),
                            updated_at_ms: p.updated_at.timestamp_millis(),
                        })
                        .collect(),
                    timestamp_ms: r.timestamp.timestamp_millis(),
                };
                Ok(Response::new(pb::risk::GetPortfolioRiskResponse {
                    risk: Some(pb_risk),
                }))
            }
            None => Err(Status::not_found("no portfolio risk data available")),
        }
    }

    async fn activate_kill_switch(
        &self,
        _request: Request<pb::risk::ActivateKillSwitchRequest>,
    ) -> Result<Response<pb::risk::KillSwitchResponse>, Status> {
        let mut ks = self.state.kill_switch_active.lock().await;
        *ks = true;

        // Also stop all strategies.
        let mut strategies = self.state.strategies.write().await;
        let now = Utc::now();
        for s in strategies.iter_mut() {
            if s.running {
                s.running = false;
                s.stopped_at = Some(now);
            }
        }

        Ok(Response::new(pb::risk::KillSwitchResponse {
            active: true,
            timestamp_ms: now.timestamp_millis(),
        }))
    }

    async fn reset_kill_switch(
        &self,
        _request: Request<pb::risk::ResetKillSwitchRequest>,
    ) -> Result<Response<pb::risk::KillSwitchResponse>, Status> {
        let mut ks = self.state.kill_switch_active.lock().await;
        *ks = false;

        Ok(Response::new(pb::risk::KillSwitchResponse {
            active: false,
            timestamp_ms: Utc::now().timestamp_millis(),
        }))
    }

    async fn get_kill_switch_status(
        &self,
        _request: Request<pb::risk::GetKillSwitchStatusRequest>,
    ) -> Result<Response<pb::risk::KillSwitchResponse>, Status> {
        let active = *self.state.kill_switch_active.lock().await;

        Ok(Response::new(pb::risk::KillSwitchResponse {
            active,
            timestamp_ms: Utc::now().timestamp_millis(),
        }))
    }
}

// ---------------------------------------------------------------------------
// StrategyService
// ---------------------------------------------------------------------------

pub struct StrategyServiceImpl {
    state: Arc<ApiState>,
}

impl StrategyServiceImpl {
    pub fn new(state: Arc<ApiState>) -> Self {
        Self { state }
    }
}

async fn emit_strategy_lifecycle_event(state: &Arc<ApiState>, strategy_id: &str, event: &str, running: bool) {
    if let Some(tx) = &state.dashboard_event_tx {
        let _ = tx
            .send(crate::ws::ServerMessage::StrategyLifecycleEvent {
                strategy_id: strategy_id.to_string(),
                event: event.to_string(),
                running,
                timestamp: Utc::now(),
            })
            .await;
    }
}

fn strategy_info_from_status(status: &StrategyStatus) -> pb::strategy::StrategyInfo {
    pb::strategy::StrategyInfo {
        strategy_id: status.config.strategy_id.clone(),
        strategy_type: status.config.strategy_type.clone(),
        exchange: exchange_to_pb(status.config.exchange),
        symbol: Some(pb::common::Symbol {
            base: status.config.symbol.base.clone(),
            quote: status.config.symbol.quote.clone(),
        }),
        running: status.running,
        started_at_ms: status.started_at.map(|t| t.timestamp_millis()).unwrap_or(0),
        stopped_at_ms: status.stopped_at.map(|t| t.timestamp_millis()).unwrap_or(0),
    }
}

fn strategy_detail_from_parts(
    status: &StrategyStatus,
    metrics: Option<&mm_core::strategy_metrics::StrategyMetrics>,
) -> pb::strategy::StrategyDetail {
    let risk_summary = build_strategy_risk_summary(status, metrics);
    let lifecycle = build_strategy_lifecycle_info(status);

    pb::strategy::StrategyDetail {
        info: Some(strategy_info_from_status(status)),
        config_json: serde_json::to_string(&status.config).unwrap_or_default(),
        metrics_json: metrics
            .map(|strategy_metrics| serde_json::to_string(strategy_metrics).unwrap_or_default())
            .unwrap_or_default(),
        risk_summary: Some(pb::strategy::StrategyRiskSummary {
            posture: risk_summary.posture,
            current_total_pnl: risk_summary
                .current_total_pnl
                .map(|pnl| pnl.to_string())
                .unwrap_or_default(),
            upside: risk_summary.upside,
            downside: risk_summary.downside,
        }),
        lifecycle: Some(pb::strategy::StrategyLifecycleInfo {
            can_start: lifecycle.can_start,
            can_stop: lifecycle.can_stop,
            can_edit: lifecycle.can_edit,
            save_mode: lifecycle.save_mode,
            start_behavior: lifecycle.start_behavior,
            stop_behavior: lifecycle.stop_behavior,
        }),
    }
}

#[tonic::async_trait]
impl pb::strategy::strategy_service_server::StrategyService for StrategyServiceImpl {
    async fn list_strategies(
        &self,
        _request: Request<pb::strategy::ListStrategiesRequest>,
    ) -> Result<Response<pb::strategy::ListStrategiesResponse>, Status> {
        let strategies = self.state.strategies.read().await;

        let infos: Vec<pb::strategy::StrategyInfo> = strategies
            .iter()
            .map(strategy_info_from_status)
            .collect();

        Ok(Response::new(pb::strategy::ListStrategiesResponse {
            strategies: infos,
        }))
    }

    async fn start_strategy(
        &self,
        request: Request<pb::strategy::StartStrategyRequest>,
    ) -> Result<Response<pb::strategy::StrategyActionResponse>, Status> {
        let id = request.into_inner().strategy_id;
        let mut strategies = self.state.strategies.write().await;

        let strategy = strategies.iter_mut().find(|s| s.config.strategy_id == id);

        match strategy {
            Some(s) => {
                if s.running {
                    Ok(Response::new(pb::strategy::StrategyActionResponse {
                        success: true,
                        message: format!("strategy {id} is already running"),
                    }))
                } else {
                    if let Some(ref engine) = self.state.strategy_engine {
                        let strategy_instance = build_strategy(&s.config).map_err(|e| {
                            Status::internal(format!("failed to build strategy: {e}"))
                        })?;

                        let mut engine_guard = engine.lock().await;
                        engine_guard.add_strategy(strategy_instance).map_err(|e| {
                            Status::internal(format!("failed to register strategy: {e}"))
                        })?;
                    }

                    s.running = true;
                    s.started_at = Some(Utc::now());
                    s.stopped_at = None;
                    emit_strategy_lifecycle_event(&self.state, &id, "started", true).await;
                    Ok(Response::new(pb::strategy::StrategyActionResponse {
                        success: true,
                        message: format!("strategy {id} started"),
                    }))
                }
            }
            None => Err(Status::not_found(format!("strategy not found: {id}"))),
        }
    }

    async fn stop_strategy(
        &self,
        request: Request<pb::strategy::StopStrategyRequest>,
    ) -> Result<Response<pb::strategy::StrategyActionResponse>, Status> {
        let id = request.into_inner().strategy_id;
        let was_running = {
            let strategies = self.state.strategies.read().await;
            match strategies.iter().find(|s| s.config.strategy_id == id) {
                Some(strategy) => strategy.running,
                None => return Err(Status::not_found(format!("strategy not found: {id}"))),
            }
        };

        if !was_running {
            return Ok(Response::new(pb::strategy::StrategyActionResponse {
                success: true,
                message: format!("strategy {id} is already stopped"),
            }));
        }

        let cancel_actions = if let Some(ref engine) = self.state.strategy_engine {
            let mut engine_guard = engine.lock().await;
            if let Some((_strategy, cancel_actions)) = engine_guard.remove_strategy(&id) {
                cancel_actions
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        let queued_cancel_requests =
            route_strategy_stop_actions(&self.state, &id, cancel_actions).await;

        let mut strategies = self.state.strategies.write().await;
        let strategy = strategies.iter_mut().find(|s| s.config.strategy_id == id);

        match strategy {
            Some(s) => {
                s.running = false;
                s.stopped_at = Some(Utc::now());
                emit_strategy_lifecycle_event(&self.state, &id, "stopped", false).await;
                Ok(Response::new(pb::strategy::StrategyActionResponse {
                    success: true,
                    message: format!(
                        "strategy {id} stopped; queued {queued_cancel_requests} cancel request(s)"
                    ),
                }))
            }
            None => Err(Status::not_found(format!("strategy not found: {id}"))),
        }
    }

    async fn get_strategy(
        &self,
        request: Request<pb::strategy::GetStrategyRequest>,
    ) -> Result<Response<pb::strategy::StrategyDetail>, Status> {
        let id = request.into_inner().strategy_id;
        let status = {
            let strategies = self.state.strategies.read().await;
            strategies
                .iter()
                .find(|s| s.config.strategy_id == id)
                .cloned()
        };

        match status {
            Some(status) => {
                let metrics = self.state.strategy_metrics.read().await;
                let strategy_metrics = metrics.get(&id);
                Ok(Response::new(strategy_detail_from_parts(&status, strategy_metrics)))
            }
            None => Err(Status::not_found(format!("strategy not found: {id}"))),
        }
    }
}

// ---------------------------------------------------------------------------
// Enum conversion helpers
// ---------------------------------------------------------------------------

#[allow(clippy::result_large_err)]
fn map_exchange(pb_exchange: i32) -> Result<mm_core::Exchange, Status> {
    match pb_exchange {
        1 => Ok(mm_core::Exchange::Binance),
        2 => Ok(mm_core::Exchange::Coinbase),
        3 => Ok(mm_core::Exchange::Kraken),
        4 => Ok(mm_core::Exchange::Uniswap),
        5 => Ok(mm_core::Exchange::Raydium),
        6 => Ok(mm_core::Exchange::InteractiveBrokers),
        7 => Ok(mm_core::Exchange::Alpaca),
        8 => Ok(mm_core::Exchange::Simulated),
        _ => Err(Status::invalid_argument("unknown exchange")),
    }
}

fn exchange_to_pb(exchange: mm_core::Exchange) -> i32 {
    match exchange {
        mm_core::Exchange::Binance => 1,
        mm_core::Exchange::Coinbase => 2,
        mm_core::Exchange::Kraken => 3,
        mm_core::Exchange::Uniswap => 4,
        mm_core::Exchange::Raydium => 5,
        mm_core::Exchange::InteractiveBrokers => 6,
        mm_core::Exchange::Alpaca => 7,
        mm_core::Exchange::Simulated => 8,
    }
}

fn side_to_pb(side: mm_core::types::Side) -> i32 {
    match side {
        mm_core::types::Side::Buy => 1,
        mm_core::types::Side::Sell => 2,
    }
}

fn order_type_to_pb(ot: mm_core::types::OrderType) -> i32 {
    match ot {
        mm_core::types::OrderType::Market => 1,
        mm_core::types::OrderType::Limit => 2,
        mm_core::types::OrderType::StopLimit => 3,
        mm_core::types::OrderType::StopMarket => 4,
    }
}

fn tif_to_pb(tif: mm_core::types::TimeInForce) -> i32 {
    match tif {
        mm_core::types::TimeInForce::GTC => 1,
        mm_core::types::TimeInForce::IOC => 2,
        mm_core::types::TimeInForce::FOK => 3,
        mm_core::types::TimeInForce::GTD => 4,
    }
}

fn order_status_to_pb(status: mm_core::types::OrderStatus) -> i32 {
    match status {
        mm_core::types::OrderStatus::Pending => 1,
        mm_core::types::OrderStatus::Submitted => 2,
        mm_core::types::OrderStatus::New => 3,
        mm_core::types::OrderStatus::PartiallyFilled => 4,
        mm_core::types::OrderStatus::Filled => 5,
        mm_core::types::OrderStatus::PendingCancel => 6,
        mm_core::types::OrderStatus::Cancelled => 7,
        mm_core::types::OrderStatus::Rejected => 8,
        mm_core::types::OrderStatus::Expired => 9,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use mm_core::types::{Position, Symbol};
    use mm_core::{Exchange, OrderBook, PortfolioRisk, StrategyConfig};
    use rust_decimal_macros::dec;
    use tonic::Request;

    use pb::execution::execution_service_server::ExecutionService;
    use pb::market_data::market_data_service_server::MarketDataService;
    use pb::risk::risk_service_server::RiskService;
    use pb::strategy::strategy_service_server::StrategyService;

    use crate::rest::{ApiState, StrategyStatus};

    /// Build a shared ApiState suitable for testing (empty state).
    fn test_state() -> Arc<ApiState> {
        ApiState::new()
    }

    // -----------------------------------------------------------------------
    // RiskService: Kill switch tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_get_kill_switch_status() {
        let state = test_state();
        let svc = RiskServiceImpl::new(Arc::clone(&state));

        let resp = svc
            .get_kill_switch_status(Request::new(pb::risk::GetKillSwitchStatusRequest {}))
            .await
            .expect("get_kill_switch_status should succeed");

        let inner = resp.into_inner();
        assert!(!inner.active, "kill switch should be inactive by default");
        assert!(inner.timestamp_ms > 0, "timestamp should be set");
    }

    #[tokio::test]
    async fn test_activate_kill_switch() {
        let state = test_state();
        let svc = RiskServiceImpl::new(Arc::clone(&state));

        let resp = svc
            .activate_kill_switch(Request::new(pb::risk::ActivateKillSwitchRequest {
                reason: "test activation".to_string(),
            }))
            .await
            .expect("activate_kill_switch should succeed");

        let inner = resp.into_inner();
        assert!(
            inner.active,
            "kill switch should be active after activation"
        );
        assert!(inner.timestamp_ms > 0, "timestamp should be set");

        // Verify the underlying state was updated.
        let ks = *state.kill_switch_active.lock().await;
        assert!(ks, "underlying state should reflect active kill switch");
    }

    #[tokio::test]
    async fn test_reset_kill_switch() {
        let state = test_state();
        let svc = RiskServiceImpl::new(Arc::clone(&state));

        // First activate the kill switch.
        svc.activate_kill_switch(Request::new(pb::risk::ActivateKillSwitchRequest {
            reason: "pre-reset activation".to_string(),
        }))
        .await
        .expect("activate should succeed");

        // Confirm it is active.
        let active = *state.kill_switch_active.lock().await;
        assert!(active, "kill switch should be active before reset");

        // Now reset it.
        let resp = svc
            .reset_kill_switch(Request::new(pb::risk::ResetKillSwitchRequest {}))
            .await
            .expect("reset_kill_switch should succeed");

        let inner = resp.into_inner();
        assert!(!inner.active, "kill switch should be inactive after reset");

        // Verify underlying state.
        let ks = *state.kill_switch_active.lock().await;
        assert!(!ks, "underlying state should reflect inactive kill switch");
    }

    // -----------------------------------------------------------------------
    // RiskService: activate_kill_switch stops running strategies
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_activate_kill_switch_stops_strategies() {
        let state = test_state();
        {
            let mut strategies = state.strategies.write().await;
            strategies.push(StrategyStatus {
                config: StrategyConfig {
                    strategy_id: "strat-1".to_string(),
                    strategy_type: "spread".to_string(),
                    exchange: Exchange::Simulated,
                    symbol: Symbol::new("BTC", "USDT"),
                    enabled: true,
                    params: serde_json::json!({}),
                },
                running: true,
                started_at: Some(Utc::now()),
                stopped_at: None,
            });
        }

        let svc = RiskServiceImpl::new(Arc::clone(&state));

        svc.activate_kill_switch(Request::new(pb::risk::ActivateKillSwitchRequest {
            reason: "emergency".to_string(),
        }))
        .await
        .expect("activate should succeed");

        let strategies = state.strategies.read().await;
        assert!(
            !strategies[0].running,
            "strategy should be stopped after kill switch activation"
        );
        assert!(
            strategies[0].stopped_at.is_some(),
            "stopped_at should be set"
        );
    }

    // -----------------------------------------------------------------------
    // MarketDataService: get_order_book not found
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_get_order_book_not_found() {
        let state = test_state();
        let svc = MarketDataServiceImpl::new(Arc::clone(&state));

        let result = svc
            .get_order_book(Request::new(pb::market_data::GetOrderBookRequest {
                exchange: 1, // Binance
                symbol: Some(pb::common::Symbol {
                    base: "BTC".to_string(),
                    quote: "USDT".to_string(),
                }),
                depth: 10,
            }))
            .await;

        let err = result.expect_err("should return not_found for missing order book");
        assert_eq!(
            err.code(),
            tonic::Code::NotFound,
            "expected NOT_FOUND status"
        );
        assert!(
            err.message().contains("order book not found"),
            "error message should mention 'order book not found'"
        );
    }

    // -----------------------------------------------------------------------
    // MarketDataService: get_order_book with populated data
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_get_order_book_returns_data() {
        let state = test_state();

        // Insert a test order book into state.
        {
            let mut books = state.order_books.write().await;
            let book = Arc::new(OrderBook {
                exchange: Exchange::Binance,
                symbol: Symbol::new("ETH", "USDT"),
                bids: vec![mm_core::types::PriceLevel {
                    price: dec!(3000),
                    quantity: dec!(1.5),
                }],
                asks: vec![mm_core::types::PriceLevel {
                    price: dec!(3001),
                    quantity: dec!(2.0),
                }],
                timestamp: Utc::now(),
                sequence: 42,
            });
            books.insert((Exchange::Binance, "ETH/USDT".to_string()), book);
        }

        let svc = MarketDataServiceImpl::new(Arc::clone(&state));

        let resp = svc
            .get_order_book(Request::new(pb::market_data::GetOrderBookRequest {
                exchange: 1, // Binance
                symbol: Some(pb::common::Symbol {
                    base: "ETH".to_string(),
                    quote: "USDT".to_string(),
                }),
                depth: 0, // all levels
            }))
            .await
            .expect("should return order book");

        let book = resp
            .into_inner()
            .order_book
            .expect("order_book should be present");
        assert_eq!(book.bids.len(), 1);
        assert_eq!(book.asks.len(), 1);
        assert_eq!(book.bids[0].price, "3000");
        assert_eq!(book.asks[0].price, "3001");
        assert_eq!(book.sequence, 42);
    }

    // -----------------------------------------------------------------------
    // ExecutionService: get_open_orders with empty state
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_get_open_orders_empty() {
        let state = test_state();
        let svc = ExecutionServiceImpl::new(Arc::clone(&state));

        let resp = svc
            .get_open_orders(Request::new(pb::execution::GetOpenOrdersRequest {
                exchange: 0, // UNSPECIFIED = all
                symbol: None,
            }))
            .await
            .expect("get_open_orders should succeed on empty state");

        let inner = resp.into_inner();
        assert!(
            inner.orders.is_empty(),
            "orders list should be empty when no orders exist"
        );
    }

    // -----------------------------------------------------------------------
    // StrategyService: list_strategies with empty state
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_list_strategies_empty() {
        let state = test_state();
        let svc = StrategyServiceImpl::new(Arc::clone(&state));

        let resp = svc
            .list_strategies(Request::new(pb::strategy::ListStrategiesRequest {}))
            .await
            .expect("list_strategies should succeed on empty state");

        let inner = resp.into_inner();
        assert!(
            inner.strategies.is_empty(),
            "strategies list should be empty"
        );
    }

    // -----------------------------------------------------------------------
    // ExecutionService: submit_order returns unimplemented
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_submit_order_unimplemented() {
        let state = test_state();
        let svc = ExecutionServiceImpl::new(Arc::clone(&state));

        let result = svc
            .submit_order(Request::new(pb::execution::SubmitOrderRequest {
                exchange: 1,
                symbol: Some(pb::common::Symbol {
                    base: "BTC".to_string(),
                    quote: "USDT".to_string(),
                }),
                side: 1,
                order_type: 2,
                time_in_force: 1,
                price: "50000".to_string(),
                quantity: "0.1".to_string(),
                strategy_id: "test-strat".to_string(),
                reduce_only: false,
            }))
            .await;

        let err = result.expect_err("submit_order should return unimplemented");
        assert_eq!(
            err.code(),
            tonic::Code::Unimplemented,
            "expected UNIMPLEMENTED status"
        );
    }

    // -----------------------------------------------------------------------
    // ExecutionService: cancel_order returns unimplemented
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_cancel_order_unimplemented() {
        let state = test_state();
        let svc = ExecutionServiceImpl::new(Arc::clone(&state));

        let result = svc
            .cancel_order(Request::new(pb::execution::CancelOrderRequest {
                exchange: 1,
                client_order_id: "some-order".to_string(),
                exchange_order_id: String::new(),
                symbol: Some(pb::common::Symbol {
                    base: "BTC".to_string(),
                    quote: "USDT".to_string(),
                }),
            }))
            .await;

        let err = result.expect_err("cancel_order should return unimplemented");
        assert_eq!(
            err.code(),
            tonic::Code::Unimplemented,
            "expected UNIMPLEMENTED status"
        );
    }

    // -----------------------------------------------------------------------
    // RiskService: get_portfolio_risk with populated data
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_get_portfolio_risk() {
        let state = test_state();

        // Populate portfolio risk data.
        {
            let mut risk = state.portfolio_risk.write().await;
            *risk = Some(PortfolioRisk {
                total_var_95: dec!(1000),
                total_var_99: dec!(1500),
                net_exposure: dec!(5000),
                gross_exposure: dec!(10000),
                max_drawdown: dec!(200),
                daily_pnl: dec!(350),
                positions: vec![Position {
                    exchange: Exchange::Binance,
                    symbol: Symbol::new("BTC", "USDT"),
                    quantity: dec!(0.5),
                    average_entry_price: dec!(50000),
                    unrealized_pnl: dec!(100),
                    realized_pnl: dec!(50),
                    updated_at: Utc::now(),
                }],
                timestamp: Utc::now(),
            });
        }

        let svc = RiskServiceImpl::new(Arc::clone(&state));

        let resp = svc
            .get_portfolio_risk(Request::new(pb::risk::GetPortfolioRiskRequest {}))
            .await
            .expect("get_portfolio_risk should succeed");

        let risk = resp
            .into_inner()
            .risk
            .expect("risk field should be present");

        assert_eq!(risk.total_var_95, "1000");
        assert_eq!(risk.total_var_99, "1500");
        assert_eq!(risk.net_exposure, "5000");
        assert_eq!(risk.gross_exposure, "10000");
        assert_eq!(risk.max_drawdown, "200");
        assert_eq!(risk.daily_pnl, "350");
        assert_eq!(risk.positions.len(), 1);
        assert_eq!(risk.positions[0].quantity, "0.5");
        assert_eq!(risk.positions[0].average_entry_price, "50000");
    }

    // -----------------------------------------------------------------------
    // RiskService: get_portfolio_risk returns not_found when empty
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_get_portfolio_risk_not_found() {
        let state = test_state();
        let svc = RiskServiceImpl::new(Arc::clone(&state));

        let result = svc
            .get_portfolio_risk(Request::new(pb::risk::GetPortfolioRiskRequest {}))
            .await;

        let err = result.expect_err("should return not_found when no risk data");
        assert_eq!(
            err.code(),
            tonic::Code::NotFound,
            "expected NOT_FOUND status"
        );
    }

    // -----------------------------------------------------------------------
    // Enum conversion helpers
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_map_exchange_valid() {
        assert!(matches!(map_exchange(1), Ok(mm_core::Exchange::Binance)));
        assert!(matches!(map_exchange(2), Ok(mm_core::Exchange::Coinbase)));
        assert!(matches!(map_exchange(3), Ok(mm_core::Exchange::Kraken)));
        assert!(matches!(map_exchange(8), Ok(mm_core::Exchange::Simulated)));
    }

    #[tokio::test]
    async fn test_map_exchange_invalid() {
        let err = map_exchange(0).expect_err("exchange 0 should be invalid");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        let err = map_exchange(99).expect_err("exchange 99 should be invalid");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }
}
