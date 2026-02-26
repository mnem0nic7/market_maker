use std::collections::HashSet;

use chrono::{DateTime, Utc};
use mm_core::events::OrderUpdate;
use mm_core::traits::Strategy;
use mm_core::types::{
    CancelAllOrders, Exchange, Fill, MarketSnapshot, NewOrder, OrderAction, OrderType, Side,
    Symbol, TimeInForce,
};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the Avellaneda-Stoikov inventory-aware market making strategy.
///
/// Reference: Avellaneda & Stoikov (2008) — "High-frequency trading in a limit
/// order book."
///
/// The model computes a reservation price that adjusts the mid price based on
/// current inventory and remaining session time, then derives an optimal spread
/// around that reservation price.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvellanedaStoikovConfig {
    /// Unique identifier for this strategy instance.
    pub strategy_id: String,
    /// Target exchange.
    pub exchange: Exchange,
    /// Trading pair.
    pub symbol: Symbol,
    /// Risk aversion parameter (gamma). Higher values penalize inventory more aggressively.
    pub gamma: Decimal,
    /// Volatility estimate (sigma). Annualized or period-matched; the model uses
    /// sigma^2 so the unit must be consistent with session_duration_secs.
    pub sigma: Decimal,
    /// Order book liquidity parameter (kappa). Controls how the optimal spread
    /// responds to fill probability assumptions.
    pub kappa: Decimal,
    /// Order size (quantity) per side.
    pub order_size: Decimal,
    /// Maximum absolute inventory (position) the strategy is allowed to hold.
    pub max_position: Decimal,
    /// Total session duration in seconds. The model uses the fraction of time
    /// remaining (T - t) to scale its risk adjustments.
    pub session_duration_secs: i64,
}

// ---------------------------------------------------------------------------
// Strategy State
// ---------------------------------------------------------------------------

pub struct AvellanedaStoikovStrategy {
    config: AvellanedaStoikovConfig,
    /// Whether the strategy is currently active.
    active: bool,
    /// Current inventory (positive = long, negative = short), tracked from fills.
    inventory: Decimal,
    /// Timestamp when the strategy session started.
    session_start: DateTime<Utc>,
    /// Set of live client_order_ids for tracking.
    live_order_ids: HashSet<String>,
}

impl AvellanedaStoikovStrategy {
    pub fn new(config: AvellanedaStoikovConfig) -> Self {
        info!(
            strategy_id = %config.strategy_id,
            exchange = %config.exchange,
            symbol = %config.symbol,
            gamma = %config.gamma,
            sigma = %config.sigma,
            kappa = %config.kappa,
            max_position = %config.max_position,
            session_duration_secs = config.session_duration_secs,
            "initializing Avellaneda-Stoikov strategy"
        );
        Self {
            config,
            active: true,
            inventory: Decimal::ZERO,
            session_start: Utc::now(),
            live_order_ids: HashSet::new(),
        }
    }

    /// Create a new instance with a specific session start time (useful for testing
    /// and backtesting).
    pub fn with_session_start(mut self, start: DateTime<Utc>) -> Self {
        self.session_start = start;
        self
    }

    /// Create a new instance with a pre-set inventory (useful for resuming).
    pub fn with_inventory(mut self, inventory: Decimal) -> Self {
        self.inventory = inventory;
        self
    }

    /// Fraction of session time remaining, clamped to [0, 1].
    fn time_remaining_fraction(&self, now: DateTime<Utc>) -> Decimal {
        let elapsed_secs = (now - self.session_start).num_seconds();
        let total = self.config.session_duration_secs;
        if total <= 0 {
            return Decimal::ZERO;
        }
        let remaining = total - elapsed_secs;
        let fraction = Decimal::from(remaining.max(0)) / Decimal::from(total);
        fraction.min(Decimal::ONE).max(Decimal::ZERO)
    }

    /// Compute the reservation price.
    ///
    /// r = s - q * gamma * sigma^2 * (T - t)
    ///
    /// where:
    ///   s = mid price
    ///   q = current inventory
    ///   gamma = risk aversion
    ///   sigma = volatility
    ///   T - t = time remaining fraction
    fn reservation_price(&self, mid: Decimal, time_remaining: Decimal) -> Decimal {
        let sigma_sq = self.config.sigma * self.config.sigma;
        mid - self.inventory * self.config.gamma * sigma_sq * time_remaining
    }

    /// Compute the optimal spread.
    ///
    /// delta = gamma * sigma^2 * (T - t) + (2 / gamma) * ln(1 + gamma / kappa)
    ///
    /// The ln() term requires floating-point; we convert to f64 for that computation
    /// and convert back to Decimal.
    fn optimal_spread(&self, time_remaining: Decimal) -> Decimal {
        let gamma = self.config.gamma;
        let sigma_sq = self.config.sigma * self.config.sigma;

        // First term: gamma * sigma^2 * (T - t)
        let term1 = gamma * sigma_sq * time_remaining;

        // Second term: (2 / gamma) * ln(1 + gamma / kappa)
        // We must use f64 for the natural log.
        let gamma_f64 = gamma.to_f64().unwrap_or(1.0);
        let kappa_f64 = self.config.kappa.to_f64().unwrap_or(1.0);

        if kappa_f64 <= 0.0 || gamma_f64 <= 0.0 {
            warn!(strategy_id = %self.config.strategy_id, "gamma or kappa is zero/negative, using minimum spread");
            return term1;
        }
        let ln_arg = 1.0 + gamma_f64 / kappa_f64;
        if ln_arg <= 0.0 {
            warn!(strategy_id = %self.config.strategy_id, "ln_arg non-positive, using minimum spread");
            return term1;
        }
        let ln_value = ln_arg.ln();
        let term2_f64 = (2.0 / gamma_f64) * ln_value;

        let term2 = Decimal::try_from(term2_f64).unwrap_or(Decimal::ZERO);

        term1 + term2
    }

    /// Build the cancel-all action for this strategy.
    fn cancel_all_action(&self) -> OrderAction {
        OrderAction::CancelAll(CancelAllOrders {
            exchange: Some(self.config.exchange),
            symbol: Some(self.config.symbol.clone()),
            strategy_id: Some(self.config.strategy_id.clone()),
        })
    }

    /// Determine whether placing an order on the given side would violate
    /// max position limits.
    fn would_exceed_position(&self, side: Side) -> bool {
        match side {
            Side::Buy => self.inventory + self.config.order_size > self.config.max_position,
            Side::Sell => self.inventory - self.config.order_size < -self.config.max_position,
        }
    }
}

impl Strategy for AvellanedaStoikovStrategy {
    fn name(&self) -> &str {
        "AvellanedaStoikov"
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
        let time_remaining = self.time_remaining_fraction(now);

        // If session is over, stop quoting.
        if time_remaining == Decimal::ZERO {
            info!(
                strategy_id = %self.config.strategy_id,
                "session expired, cancelling all orders"
            );
            self.active = false;
            return vec![self.cancel_all_action()];
        }

        let reservation = self.reservation_price(mid, time_remaining);
        let spread = self.optimal_spread(time_remaining);
        let half_spread = spread / dec!(2);

        let bid_price = reservation - half_spread;
        let ask_price = reservation + half_spread;

        debug!(
            strategy_id = %self.config.strategy_id,
            mid = %mid,
            inventory = %self.inventory,
            time_remaining = %time_remaining,
            reservation_price = %reservation,
            optimal_spread = %spread,
            bid = %bid_price,
            ask = %ask_price,
            "Avellaneda-Stoikov tick"
        );

        let mut actions = Vec::new();

        // Always cancel existing orders before placing new ones to avoid
        // stale quotes.
        if !self.live_order_ids.is_empty() {
            actions.push(self.cancel_all_action());
            self.live_order_ids.clear();
        }

        // Place bid if position limit allows
        if !self.would_exceed_position(Side::Buy) {
            if bid_price > Decimal::ZERO {
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
            }
        } else {
            debug!(
                strategy_id = %self.config.strategy_id,
                inventory = %self.inventory,
                "skipping bid — would exceed max position"
            );
        }

        // Place ask if position limit allows
        if !self.would_exceed_position(Side::Sell) {
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
        } else {
            debug!(
                strategy_id = %self.config.strategy_id,
                inventory = %self.inventory,
                "skipping ask — would exceed max position"
            );
        }

        actions
    }

    fn on_fill(&mut self, fill: &Fill) {
        let signed_qty = match fill.side {
            Side::Buy => fill.quantity,
            Side::Sell => -fill.quantity,
        };
        self.inventory += signed_qty;

        info!(
            strategy_id = %self.config.strategy_id,
            fill_id = %fill.fill_id,
            side = %fill.side,
            price = %fill.price,
            quantity = %fill.quantity,
            new_inventory = %self.inventory,
            "Avellaneda-Stoikov fill processed"
        );

        self.live_order_ids.remove(&fill.client_order_id);
    }

    fn on_order_update(&mut self, update: &OrderUpdate) {
        debug!(
            strategy_id = %self.config.strategy_id,
            client_order_id = %update.client_order_id,
            status = ?update.status,
            "Avellaneda-Stoikov order update"
        );

        if update.status.is_terminal() {
            self.live_order_ids.remove(&update.client_order_id);
        }

        if update.status == mm_core::types::OrderStatus::New
            || update.status == mm_core::types::OrderStatus::Submitted
        {
            self.live_order_ids.insert(update.client_order_id.clone());
        }
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn stop(&mut self) -> Vec<OrderAction> {
        info!(
            strategy_id = %self.config.strategy_id,
            final_inventory = %self.inventory,
            "stopping Avellaneda-Stoikov strategy"
        );
        self.active = false;
        self.live_order_ids.clear();
        vec![self.cancel_all_action()]
    }
}
