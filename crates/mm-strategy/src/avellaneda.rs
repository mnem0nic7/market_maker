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

impl AvellanedaStoikovConfig {
    /// Validate logical constraints on the configuration parameters.
    pub fn validate(&self) -> Result<(), String> {
        if self.order_size <= Decimal::ZERO {
            return Err("order_size must be positive".to_string());
        }
        if self.max_position <= Decimal::ZERO {
            return Err("max_position must be positive".to_string());
        }
        if self.gamma <= Decimal::ZERO {
            return Err("gamma (risk aversion) must be positive".to_string());
        }
        if self.sigma <= Decimal::ZERO {
            return Err("sigma (volatility) must be positive".to_string());
        }
        if self.kappa <= Decimal::ZERO {
            return Err("kappa (liquidity parameter) must be positive".to_string());
        }
        if self.session_duration_secs <= 0 {
            return Err("session_duration_secs must be positive".to_string());
        }
        Ok(())
    }
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

    fn exchange(&self) -> Exchange {
        self.config.exchange
    }

    fn symbol(&self) -> Symbol {
        self.config.symbol.clone()
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use mm_core::traits::Strategy;
    use mm_core::types::{
        Exchange, Fill, MarketSnapshot, OrderAction, OrderBook, Position, PriceLevel, Side, Symbol,
    };
    use rust_decimal_macros::dec;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_config() -> AvellanedaStoikovConfig {
        AvellanedaStoikovConfig {
            strategy_id: "test-as".to_string(),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            gamma: dec!(0.1),
            sigma: dec!(0.02),
            kappa: dec!(1.5),
            order_size: dec!(0.01),
            max_position: dec!(1.0),
            session_duration_secs: 3600, // 1 hour
        }
    }

    /// Build a MarketSnapshot with the given mid price and timestamp.
    /// The order book is set with bid = mid - 1, ask = mid + 1.
    fn make_snapshot_at(mid: Decimal, ts: DateTime<Utc>) -> MarketSnapshot {
        let bid = mid - dec!(1);
        let ask = mid + dec!(1);
        MarketSnapshot {
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            order_book: std::sync::Arc::new(OrderBook {
                exchange: Exchange::Simulated,
                symbol: Symbol::new("BTC", "USDT"),
                bids: vec![PriceLevel {
                    price: bid,
                    quantity: dec!(10),
                }],
                asks: vec![PriceLevel {
                    price: ask,
                    quantity: dec!(10),
                }],
                timestamp: ts,
                sequence: 1,
            }),
            last_trade: None,
            position: Position::new(Exchange::Simulated, Symbol::new("BTC", "USDT")),
            open_orders: vec![],
            timestamp: ts,
        }
    }

    /// Build a Fill for the given side, price, and quantity.
    fn make_fill(side: Side, price: Decimal, quantity: Decimal) -> Fill {
        Fill {
            fill_id: "fill-1".to_string(),
            client_order_id: "order-1".to_string(),
            exchange_order_id: "exch-1".to_string(),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            side,
            price,
            quantity,
            fee: Decimal::ZERO,
            fee_currency: "USDT".to_string(),
            timestamp: Utc::now(),
        }
    }

    /// Create a strategy whose session started at `start` and has zero inventory.
    fn make_strategy(start: DateTime<Utc>) -> AvellanedaStoikovStrategy {
        AvellanedaStoikovStrategy::new(make_config()).with_session_start(start)
    }

    /// Create a strategy whose session started at `start` with a given inventory.
    fn make_strategy_with_inventory(
        start: DateTime<Utc>,
        inventory: Decimal,
    ) -> AvellanedaStoikovStrategy {
        AvellanedaStoikovStrategy::new(make_config())
            .with_session_start(start)
            .with_inventory(inventory)
    }

    // -----------------------------------------------------------------------
    // 1. Initialization
    // -----------------------------------------------------------------------

    #[test]
    fn test_new_initializes_correctly() {
        let strategy = AvellanedaStoikovStrategy::new(make_config());

        assert_eq!(strategy.name(), "AvellanedaStoikov");
        assert_eq!(strategy.strategy_id(), "test-as");
        assert!(strategy.is_active());
        assert_eq!(strategy.inventory, Decimal::ZERO);
        assert!(strategy.live_order_ids.is_empty());
    }

    // -----------------------------------------------------------------------
    // 2. Reservation price with zero inventory
    // -----------------------------------------------------------------------

    #[test]
    fn test_reservation_price_zero_inventory() {
        // r = mid - q * gamma * sigma^2 * (T-t)
        // With q = 0 the adjustment term vanishes, so r = mid.
        let start = Utc::now();
        let strategy = make_strategy(start);

        let mid = dec!(50000);
        let time_remaining = dec!(0.5); // half the session left

        let reservation = strategy.reservation_price(mid, time_remaining);
        assert_eq!(reservation, mid);
    }

    // -----------------------------------------------------------------------
    // 3. Reservation price with positive (long) inventory
    // -----------------------------------------------------------------------

    #[test]
    fn test_reservation_price_long_inventory() {
        // With q > 0, the reservation price should be BELOW mid (incentivize selling).
        // r = mid - q * gamma * sigma^2 * (T-t)
        // q=2, gamma=0.1, sigma=0.02, T-t=0.5
        // adjustment = 2 * 0.1 * 0.0004 * 0.5 = 0.00004
        // r = 50000 - 0.00004 = 49999.99996
        let start = Utc::now();
        let strategy = make_strategy_with_inventory(start, dec!(2));

        let mid = dec!(50000);
        let time_remaining = dec!(0.5);
        let reservation = strategy.reservation_price(mid, time_remaining);

        assert!(
            reservation < mid,
            "With long inventory, reservation price ({}) should be below mid ({})",
            reservation,
            mid
        );

        // Verify the exact math:
        // q * gamma * sigma^2 * (T-t) = 2 * 0.1 * 0.0004 * 0.5 = 0.00004
        let expected = mid - dec!(2) * dec!(0.1) * dec!(0.0004) * dec!(0.5);
        assert_eq!(reservation, expected);
    }

    // -----------------------------------------------------------------------
    // 4. Reservation price with negative (short) inventory
    // -----------------------------------------------------------------------

    #[test]
    fn test_reservation_price_short_inventory() {
        // With q < 0, the reservation price should be ABOVE mid (incentivize buying).
        // r = mid - (-2) * gamma * sigma^2 * (T-t) = mid + 2 * gamma * sigma^2 * (T-t)
        let start = Utc::now();
        let strategy = make_strategy_with_inventory(start, dec!(-2));

        let mid = dec!(50000);
        let time_remaining = dec!(0.5);
        let reservation = strategy.reservation_price(mid, time_remaining);

        assert!(
            reservation > mid,
            "With short inventory, reservation price ({}) should be above mid ({})",
            reservation,
            mid
        );
    }

    // -----------------------------------------------------------------------
    // 5. Optimal spread is always positive
    // -----------------------------------------------------------------------

    #[test]
    fn test_optimal_spread_positive() {
        // delta = gamma * sigma^2 * (T-t) + (2/gamma) * ln(1 + gamma/kappa)
        // Both terms should be positive for reasonable params, so delta > 0.
        let start = Utc::now();
        let strategy = make_strategy(start);

        // Test at various time_remaining fractions
        for tr in [dec!(0.01), dec!(0.1), dec!(0.5), dec!(1.0)] {
            let spread = strategy.optimal_spread(tr);
            assert!(
                spread > Decimal::ZERO,
                "Optimal spread should be positive at time_remaining={}, got {}",
                tr,
                spread
            );
        }
    }

    // -----------------------------------------------------------------------
    // 6. Places both bid and ask on first tick with zero inventory
    // -----------------------------------------------------------------------

    #[test]
    fn test_places_bid_and_ask() {
        let start = Utc::now();
        let mut strategy = make_strategy(start);

        // Tick 30 minutes into the session (half-way)
        let ts = start + Duration::minutes(30);
        let mid = dec!(50000);
        let snapshot = make_snapshot_at(mid, ts);
        let actions = strategy.on_tick(&snapshot);

        // Should have exactly 2 Place actions (bid + ask), no CancelAll since no
        // live orders exist yet.
        let places: Vec<&NewOrder> = actions
            .iter()
            .filter_map(|a| match a {
                OrderAction::Place(o) => Some(o),
                _ => None,
            })
            .collect();

        assert_eq!(
            places.len(),
            2,
            "Expected 2 Place actions, got {}",
            places.len()
        );

        let bid_order = places.iter().find(|o| o.side == Side::Buy).unwrap();
        let ask_order = places.iter().find(|o| o.side == Side::Sell).unwrap();

        assert!(
            bid_order.price.unwrap() < mid,
            "Bid price ({}) should be below mid ({})",
            bid_order.price.unwrap(),
            mid
        );
        assert!(
            ask_order.price.unwrap() > mid,
            "Ask price ({}) should be above mid ({})",
            ask_order.price.unwrap(),
            mid
        );

        // Verify order sizes
        assert_eq!(bid_order.quantity, dec!(0.01));
        assert_eq!(ask_order.quantity, dec!(0.01));
    }

    // -----------------------------------------------------------------------
    // 7. Max position blocks buy when at max long
    // -----------------------------------------------------------------------

    #[test]
    fn test_max_position_blocks_buy() {
        // max_position = 1.0, order_size = 0.01
        // inventory = 1.0, buying 0.01 more would make 1.01 > 1.0, so bid blocked.
        let start = Utc::now();
        let mut strategy = make_strategy_with_inventory(start, dec!(1.0));

        let ts = start + Duration::minutes(30);
        let snapshot = make_snapshot_at(dec!(50000), ts);
        let actions = strategy.on_tick(&snapshot);

        let places: Vec<&NewOrder> = actions
            .iter()
            .filter_map(|a| match a {
                OrderAction::Place(o) => Some(o),
                _ => None,
            })
            .collect();

        // Should only have the ask (sell side)
        assert_eq!(
            places.len(),
            1,
            "Expected 1 Place action (ask only), got {}",
            places.len()
        );
        assert_eq!(
            places[0].side,
            Side::Sell,
            "The only order should be a sell"
        );
    }

    // -----------------------------------------------------------------------
    // 8. Max position blocks sell when at max short
    // -----------------------------------------------------------------------

    #[test]
    fn test_max_position_blocks_sell() {
        // inventory = -1.0, selling 0.01 more would make -1.01 < -1.0, so ask blocked.
        let start = Utc::now();
        let mut strategy = make_strategy_with_inventory(start, dec!(-1.0));

        let ts = start + Duration::minutes(30);
        let snapshot = make_snapshot_at(dec!(50000), ts);
        let actions = strategy.on_tick(&snapshot);

        let places: Vec<&NewOrder> = actions
            .iter()
            .filter_map(|a| match a {
                OrderAction::Place(o) => Some(o),
                _ => None,
            })
            .collect();

        // Should only have the bid (buy side)
        assert_eq!(
            places.len(),
            1,
            "Expected 1 Place action (bid only), got {}",
            places.len()
        );
        assert_eq!(places[0].side, Side::Buy, "The only order should be a buy");
    }

    // -----------------------------------------------------------------------
    // 9. Session expiry cancels all and deactivates
    // -----------------------------------------------------------------------

    #[test]
    fn test_session_expiry_cancels_all() {
        let start = Utc::now() - Duration::hours(2); // session started 2 hours ago
        let mut strategy = make_strategy(start);

        // Tick at now (well past the 1-hour session)
        let ts = Utc::now();
        let snapshot = make_snapshot_at(dec!(50000), ts);
        let actions = strategy.on_tick(&snapshot);

        // Should return a single CancelAll action
        assert_eq!(
            actions.len(),
            1,
            "Expected exactly 1 action on session expiry"
        );
        assert!(
            matches!(&actions[0], OrderAction::CancelAll(_)),
            "Action should be CancelAll, got {:?}",
            actions[0]
        );

        // Strategy should now be inactive
        assert!(
            !strategy.is_active(),
            "Strategy should be inactive after session expiry"
        );

        // Further ticks should return empty
        let actions2 = strategy.on_tick(&snapshot);
        assert!(
            actions2.is_empty(),
            "Ticks after session expiry should return empty"
        );
    }

    // -----------------------------------------------------------------------
    // 10. on_fill updates inventory
    // -----------------------------------------------------------------------

    #[test]
    fn test_on_fill_updates_inventory() {
        let start = Utc::now();
        let mut strategy = make_strategy(start);

        assert_eq!(strategy.inventory, Decimal::ZERO);

        // Buy fill increases inventory
        strategy.on_fill(&make_fill(Side::Buy, dec!(50000), dec!(0.5)));
        assert_eq!(
            strategy.inventory,
            dec!(0.5),
            "Buy fill should increase inventory"
        );

        // Another buy
        strategy.on_fill(&make_fill(Side::Buy, dec!(50100), dec!(0.3)));
        assert_eq!(
            strategy.inventory,
            dec!(0.8),
            "Second buy fill should further increase inventory"
        );

        // Sell fill decreases inventory
        strategy.on_fill(&make_fill(Side::Sell, dec!(50200), dec!(1.0)));
        assert_eq!(
            strategy.inventory,
            dec!(-0.2),
            "Sell fill should decrease inventory"
        );
    }

    // -----------------------------------------------------------------------
    // 11. stop() deactivates and returns CancelAll
    // -----------------------------------------------------------------------

    #[test]
    fn test_stop_deactivates_and_cancels() {
        let start = Utc::now();
        let mut strategy = make_strategy(start);
        assert!(strategy.is_active());

        let actions = strategy.stop();

        // Should be inactive
        assert!(
            !strategy.is_active(),
            "Strategy should be inactive after stop"
        );

        // Should return a single CancelAll
        assert_eq!(actions.len(), 1, "stop() should return exactly 1 action");
        match &actions[0] {
            OrderAction::CancelAll(cancel) => {
                assert_eq!(cancel.exchange, Some(Exchange::Simulated));
                assert_eq!(cancel.symbol, Some(Symbol::new("BTC", "USDT")));
                assert_eq!(cancel.strategy_id, Some("test-as".to_string()));
            }
            other => panic!("Expected CancelAll, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // 12. Inactive strategy returns empty on tick
    // -----------------------------------------------------------------------

    #[test]
    fn test_inactive_returns_empty() {
        let start = Utc::now();
        let mut strategy = make_strategy(start);

        // Stop the strategy
        let _ = strategy.stop();
        assert!(!strategy.is_active());

        // Subsequent ticks should return empty
        let ts = start + Duration::minutes(10);
        let snapshot = make_snapshot_at(dec!(50000), ts);
        let actions = strategy.on_tick(&snapshot);

        assert!(
            actions.is_empty(),
            "Inactive strategy should return empty actions, got {} actions",
            actions.len()
        );
    }

    // -----------------------------------------------------------------------
    // Validation tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_validate_valid_config() {
        let config = make_config();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_zero_order_size() {
        let mut config = make_config();
        config.order_size = Decimal::ZERO;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("order_size must be positive"));
    }

    #[test]
    fn test_validate_zero_gamma() {
        let mut config = make_config();
        config.gamma = Decimal::ZERO;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("gamma"));
    }

    #[test]
    fn test_validate_negative_session_duration() {
        let mut config = make_config();
        config.session_duration_secs = -1;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("session_duration_secs must be positive"));
    }
}
