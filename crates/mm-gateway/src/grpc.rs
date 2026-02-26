//! gRPC service implementations for the market maker gateway.
//!
//! Each service delegates to the shared [`ApiState`] for reads and writes,
//! keeping the gRPC layer thin. The generated proto types live in sub-modules
//! produced by `tonic-build` in `build.rs`.

use std::sync::Arc;

use chrono::Utc;
use tonic::{Request, Response, Status};

use crate::rest::ApiState;

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
        let canonical = format!("{}/{}", symbol.base.to_uppercase(), symbol.quote.to_uppercase());

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

    type StreamOrderBookStream = futures::stream::BoxStream<
        'static,
        Result<pb::common::OrderBook, Status>,
    >;

    async fn stream_order_book(
        &self,
        _request: Request<pb::market_data::StreamOrderBookRequest>,
    ) -> Result<Response<Self::StreamOrderBookStream>, Status> {
        // Streaming will be connected to the live market data pipeline.
        // For now, return an empty stream as a placeholder.
        let stream = futures::stream::empty();
        Ok(Response::new(Box::pin(stream)))
    }

    type StreamTradesStream = futures::stream::BoxStream<
        'static,
        Result<pb::common::Trade, Status>,
    >;

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

    type StreamOrderUpdatesStream = futures::stream::BoxStream<
        'static,
        Result<pb::execution::OrderUpdate, Status>,
    >;

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

#[tonic::async_trait]
impl pb::strategy::strategy_service_server::StrategyService for StrategyServiceImpl {
    async fn list_strategies(
        &self,
        _request: Request<pb::strategy::ListStrategiesRequest>,
    ) -> Result<Response<pb::strategy::ListStrategiesResponse>, Status> {
        let strategies = self.state.strategies.read().await;

        let infos: Vec<pb::strategy::StrategyInfo> = strategies
            .iter()
            .map(|s| pb::strategy::StrategyInfo {
                strategy_id: s.config.strategy_id.clone(),
                strategy_type: s.config.strategy_type.clone(),
                exchange: exchange_to_pb(s.config.exchange),
                symbol: Some(pb::common::Symbol {
                    base: s.config.symbol.base.clone(),
                    quote: s.config.symbol.quote.clone(),
                }),
                running: s.running,
                started_at_ms: s.started_at.map(|t| t.timestamp_millis()).unwrap_or(0),
                stopped_at_ms: s.stopped_at.map(|t| t.timestamp_millis()).unwrap_or(0),
            })
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

        let strategy = strategies
            .iter_mut()
            .find(|s| s.config.strategy_id == id);

        match strategy {
            Some(s) => {
                if s.running {
                    Ok(Response::new(pb::strategy::StrategyActionResponse {
                        success: true,
                        message: format!("strategy {id} is already running"),
                    }))
                } else {
                    s.running = true;
                    s.started_at = Some(Utc::now());
                    s.stopped_at = None;
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
        let mut strategies = self.state.strategies.write().await;

        let strategy = strategies
            .iter_mut()
            .find(|s| s.config.strategy_id == id);

        match strategy {
            Some(s) => {
                if !s.running {
                    Ok(Response::new(pb::strategy::StrategyActionResponse {
                        success: true,
                        message: format!("strategy {id} is already stopped"),
                    }))
                } else {
                    s.running = false;
                    s.stopped_at = Some(Utc::now());
                    Ok(Response::new(pb::strategy::StrategyActionResponse {
                        success: true,
                        message: format!("strategy {id} stopped"),
                    }))
                }
            }
            None => Err(Status::not_found(format!("strategy not found: {id}"))),
        }
    }

    async fn get_strategy(
        &self,
        request: Request<pb::strategy::GetStrategyRequest>,
    ) -> Result<Response<pb::strategy::StrategyDetail>, Status> {
        let id = request.into_inner().strategy_id;
        let strategies = self.state.strategies.read().await;

        let strategy = strategies
            .iter()
            .find(|s| s.config.strategy_id == id);

        match strategy {
            Some(s) => Ok(Response::new(pb::strategy::StrategyDetail {
                info: Some(pb::strategy::StrategyInfo {
                    strategy_id: s.config.strategy_id.clone(),
                    strategy_type: s.config.strategy_type.clone(),
                    exchange: exchange_to_pb(s.config.exchange),
                    symbol: Some(pb::common::Symbol {
                        base: s.config.symbol.base.clone(),
                        quote: s.config.symbol.quote.clone(),
                    }),
                    running: s.running,
                    started_at_ms: s.started_at.map(|t| t.timestamp_millis()).unwrap_or(0),
                    stopped_at_ms: s.stopped_at.map(|t| t.timestamp_millis()).unwrap_or(0),
                }),
                config_json: serde_json::to_string(&s.config).unwrap_or_default(),
            })),
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
