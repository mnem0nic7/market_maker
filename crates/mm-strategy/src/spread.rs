use std::collections::HashSet;

use chrono::{DateTime, Utc};
use mm_core::events::OrderUpdate;
use mm_core::traits::Strategy;
use mm_core::types::{
    CancelAllOrders, Exchange, Fill, MarketSnapshot, NewOrder, OrderAction, OrderStatus, OrderType,
    Side, Symbol, TimeInForce,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the basic spread market making strategy.
///
/// The strategy places symmetric bid and ask orders around the mid price,
/// with each successive level offset by an additional `spread_bps` increment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpreadConfig {
    /// Unique identifier for this strategy instance.
    pub strategy_id: String,
    /// Target exchange.
    pub exchange: Exchange,
    /// Trading pair.
    pub symbol: Symbol,
    /// Half-spread in basis points for the first level.
    /// Level N is offset by `spread_bps * N` from the mid price.
    pub spread_bps: Decimal,
    /// Order size (quantity) per level.
    pub order_size: Decimal,
    /// Number of price levels on each side.
    pub num_levels: usize,
    /// Minimum time between order refreshes (milliseconds).
    pub refresh_interval_ms: i64,
    /// Minimum acceptable spread in basis points. If the market spread is
    /// below this, the strategy will widen to this floor.
    pub min_spread_bps: Decimal,
    /// Threshold in basis points for mid-price movement that triggers a full
    /// cancel-and-replace cycle. If the mid price has moved by more than this
    /// many bps since the last quote, all orders are cancelled and re-placed.
    pub reprice_threshold_bps: Decimal,
}

// ---------------------------------------------------------------------------
// Strategy State
// ---------------------------------------------------------------------------

pub struct SpreadStrategy {
    config: SpreadConfig,
    /// Whether the strategy is currently active.
    active: bool,
    /// Set of client_order_ids for orders we have placed that are still live.
    live_order_ids: HashSet<String>,
    /// The mid price at which we last placed quotes.
    last_mid_price: Option<Decimal>,
    /// Timestamp of the last time we placed/refreshed quotes.
    last_refresh_time: Option<DateTime<Utc>>,
}

impl SpreadStrategy {
    pub fn new(config: SpreadConfig) -> Self {
        info!(
            strategy_id = %config.strategy_id,
            exchange = %config.exchange,
            symbol = %config.symbol,
            spread_bps = %config.spread_bps,
            num_levels = config.num_levels,
            order_size = %config.order_size,
            "initializing spread market making strategy"
        );
        Self {
            config,
            active: true,
            live_order_ids: HashSet::new(),
            last_mid_price: None,
            last_refresh_time: None,
        }
    }

    /// Check whether enough time has elapsed since the last refresh.
    fn should_refresh(&self, now: DateTime<Utc>) -> bool {
        match self.last_refresh_time {
            None => true,
            Some(last) => {
                let elapsed_ms = (now - last).num_milliseconds();
                elapsed_ms >= self.config.refresh_interval_ms
            }
        }
    }

    /// Check whether the mid price has moved beyond the reprice threshold
    /// since we last placed quotes.
    fn mid_price_moved_significantly(&self, current_mid: Decimal) -> bool {
        match self.last_mid_price {
            None => true,
            Some(prev_mid) => {
                if prev_mid.is_zero() {
                    return true;
                }
                let change_bps = ((current_mid - prev_mid) / prev_mid).abs() * dec!(10_000);
                change_bps > self.config.reprice_threshold_bps
            }
        }
    }

    /// Build the cancel-all action for this strategy's orders.
    fn cancel_all_action(&self) -> OrderAction {
        OrderAction::CancelAll(CancelAllOrders {
            exchange: Some(self.config.exchange),
            symbol: Some(self.config.symbol.clone()),
            strategy_id: Some(self.config.strategy_id.clone()),
        })
    }

    /// Generate new limit orders around the given mid price.
    fn generate_quotes(&self, mid: Decimal) -> Vec<OrderAction> {
        let mut actions = Vec::with_capacity(self.config.num_levels * 2);

        // Use at least min_spread_bps for the base offset
        let effective_spread_bps = self.config.spread_bps.max(self.config.min_spread_bps);

        for level in 1..=self.config.num_levels {
            let level_dec = Decimal::from(level as u64);
            // Offset in bps for this level: effective_spread_bps * level
            let offset_bps = effective_spread_bps * level_dec;
            let offset_fraction = offset_bps / dec!(10_000);

            let bid_price = mid * (Decimal::ONE - offset_fraction);
            let ask_price = mid * (Decimal::ONE + offset_fraction);

            // Place bid
            actions.push(OrderAction::Place(NewOrder {
                exchange: self.config.exchange,
                symbol: self.config.symbol.clone(),
                side: Side::Buy,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::GTC,
                price: Some(bid_price),
                quantity: self.config.order_size,
                strategy_id: self.config.strategy_id.clone(),
                reduce_only: false,
            }));

            // Place ask
            actions.push(OrderAction::Place(NewOrder {
                exchange: self.config.exchange,
                symbol: self.config.symbol.clone(),
                side: Side::Sell,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::GTC,
                price: Some(ask_price),
                quantity: self.config.order_size,
                strategy_id: self.config.strategy_id.clone(),
                reduce_only: false,
            }));

            debug!(
                strategy_id = %self.config.strategy_id,
                level = level,
                bid_price = %bid_price,
                ask_price = %ask_price,
                offset_bps = %offset_bps,
                "generated quote level"
            );
        }

        actions
    }
}

impl Strategy for SpreadStrategy {
    fn name(&self) -> &str {
        "SpreadMarketMaker"
    }

    fn strategy_id(&self) -> &str {
        &self.config.strategy_id
    }

    fn on_tick(&mut self, snapshot: &MarketSnapshot) -> Vec<OrderAction> {
        if !self.active {
            return Vec::new();
        }

        let mid = match snapshot.order_book.mid_price() {
            Some(m) => m,
            None => {
                warn!(
                    strategy_id = %self.config.strategy_id,
                    "no mid price available, skipping tick"
                );
                return Vec::new();
            }
        };

        let now = snapshot.timestamp;
        let needs_refresh = self.should_refresh(now);
        let price_moved = self.mid_price_moved_significantly(mid);

        if !needs_refresh && !price_moved {
            return Vec::new();
        }

        debug!(
            strategy_id = %self.config.strategy_id,
            mid_price = %mid,
            needs_refresh = needs_refresh,
            price_moved = price_moved,
            "repricing quotes"
        );

        let mut actions = Vec::new();

        // Cancel all existing orders before placing new ones
        if !self.live_order_ids.is_empty() {
            actions.push(self.cancel_all_action());
            self.live_order_ids.clear();
        }

        // Generate fresh quotes
        let quote_actions = self.generate_quotes(mid);

        // Track the client_order_ids we are about to place.
        // Since NewOrder doesn't carry an id yet (it gets one when submitted),
        // we generate deterministic ids here for tracking. The execution layer
        // will use the strategy_id to correlate fills back.
        for action in &quote_actions {
            if let OrderAction::Place(_) = action {
                // We track by generating a UUID that we'll see in order updates
                let tracking_id = Uuid::new_v4().to_string();
                self.live_order_ids.insert(tracking_id);
            }
        }

        actions.extend(quote_actions);

        self.last_mid_price = Some(mid);
        self.last_refresh_time = Some(now);

        actions
    }

    fn on_fill(&mut self, fill: &Fill) {
        info!(
            strategy_id = %self.config.strategy_id,
            fill_id = %fill.fill_id,
            side = %fill.side,
            price = %fill.price,
            quantity = %fill.quantity,
            symbol = %fill.symbol,
            "spread strategy received fill"
        );
        // Remove the filled order from tracking
        self.live_order_ids.remove(&fill.client_order_id);
    }

    fn on_order_update(&mut self, update: &OrderUpdate) {
        debug!(
            strategy_id = %self.config.strategy_id,
            client_order_id = %update.client_order_id,
            status = ?update.status,
            "spread strategy received order update"
        );

        // If the order reached a terminal state, remove it from tracking.
        if update.status.is_terminal() {
            self.live_order_ids.remove(&update.client_order_id);
        }

        // If the order was just acknowledged, add the real order id to tracking.
        if update.status == OrderStatus::New || update.status == OrderStatus::Submitted {
            self.live_order_ids.insert(update.client_order_id.clone());
        }
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn stop(&mut self) -> Vec<OrderAction> {
        info!(
            strategy_id = %self.config.strategy_id,
            "stopping spread market making strategy"
        );
        self.active = false;
        self.live_order_ids.clear();
        vec![self.cancel_all_action()]
    }
}
