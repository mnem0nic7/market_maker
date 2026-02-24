use chrono::Utc;
use rust_decimal::Decimal;
use tracing::{debug, info, warn};

use mm_core::{
    traits::RiskCheck, CancelAllOrders, Exchange, NewOrder, Position, PortfolioRisk, RiskAction,
};

/// Central risk engine that evaluates orders and portfolio state against a
/// configurable chain of [`RiskCheck`] implementations.
///
/// Checks execute in insertion order. The first `Reject` or `KillSwitch` result
/// short-circuits the chain. If all checks return `Allow`, the order is allowed.
/// If any check returns `ReduceSize`, the most restrictive (smallest
/// `max_quantity`) wins.
pub struct RiskEngine {
    checks: Vec<Box<dyn RiskCheck>>,
}

impl RiskEngine {
    /// Create a new, empty `RiskEngine`.
    pub fn new() -> Self {
        Self { checks: Vec::new() }
    }

    /// Append a risk check to the evaluation chain.
    pub fn add_check(&mut self, check: Box<dyn RiskCheck>) {
        info!(check = check.name(), "risk engine: added check");
        self.checks.push(check);
    }

    /// Evaluate a proposed order against every registered check.
    ///
    /// Semantics:
    /// - First `Reject` or `KillSwitch` encountered is returned immediately.
    /// - If one or more checks return `ReduceSize`, the most restrictive
    ///   (smallest `max_quantity`) is returned.
    /// - If every check returns `Allow`, the order is allowed.
    pub async fn evaluate_order(
        &self,
        order: &NewOrder,
        portfolio: &PortfolioRisk,
    ) -> RiskAction {
        let mut most_restrictive_reduce: Option<(Decimal, String)> = None;

        for check in &self.checks {
            let action = check.check_order(order, portfolio).await;

            match &action {
                RiskAction::Allow => {
                    debug!(check = check.name(), "order allowed");
                }
                RiskAction::Reject { reason } => {
                    warn!(
                        check = check.name(),
                        reason = %reason,
                        symbol = %order.symbol,
                        "order rejected by risk check"
                    );
                    return action;
                }
                RiskAction::KillSwitch { reason } => {
                    warn!(
                        check = check.name(),
                        reason = %reason,
                        "kill switch triggered by risk check"
                    );
                    return action;
                }
                RiskAction::ReduceSize {
                    max_quantity,
                    reason,
                } => {
                    debug!(
                        check = check.name(),
                        max_quantity = %max_quantity,
                        reason = %reason,
                        "risk check requests size reduction"
                    );
                    match &most_restrictive_reduce {
                        Some((current_max, _)) if max_quantity < current_max => {
                            most_restrictive_reduce =
                                Some((*max_quantity, reason.clone()));
                        }
                        None => {
                            most_restrictive_reduce =
                                Some((*max_quantity, reason.clone()));
                        }
                        _ => {}
                    }
                }
            }
        }

        if let Some((max_quantity, reason)) = most_restrictive_reduce {
            RiskAction::ReduceSize {
                max_quantity,
                reason,
            }
        } else {
            RiskAction::Allow
        }
    }

    /// Evaluate the overall portfolio against every registered check.
    ///
    /// Same short-circuit semantics as [`evaluate_order`].
    pub async fn evaluate_portfolio(&self, portfolio: &PortfolioRisk) -> RiskAction {
        let mut most_restrictive_reduce: Option<(Decimal, String)> = None;

        for check in &self.checks {
            let action = check.check_portfolio(portfolio).await;

            match &action {
                RiskAction::Allow => {
                    debug!(check = check.name(), "portfolio check passed");
                }
                RiskAction::Reject { reason } => {
                    warn!(
                        check = check.name(),
                        reason = %reason,
                        "portfolio rejected by risk check"
                    );
                    return action;
                }
                RiskAction::KillSwitch { reason } => {
                    warn!(
                        check = check.name(),
                        reason = %reason,
                        "kill switch triggered by portfolio check"
                    );
                    return action;
                }
                RiskAction::ReduceSize {
                    max_quantity,
                    reason,
                } => {
                    match &most_restrictive_reduce {
                        Some((current_max, _)) if max_quantity < current_max => {
                            most_restrictive_reduce =
                                Some((*max_quantity, reason.clone()));
                        }
                        None => {
                            most_restrictive_reduce =
                                Some((*max_quantity, reason.clone()));
                        }
                        _ => {}
                    }
                }
            }
        }

        if let Some((max_quantity, reason)) = most_restrictive_reduce {
            RiskAction::ReduceSize {
                max_quantity,
                reason,
            }
        } else {
            RiskAction::Allow
        }
    }

    /// Build a [`PortfolioRisk`] snapshot from a set of live positions.
    ///
    /// VaR fields are left at zero; callers should fill them via the VaR
    /// calculators in [`crate::var`].
    pub fn build_portfolio_risk(positions: &[Position], daily_pnl: Decimal) -> PortfolioRisk {
        let mut net_exposure = Decimal::ZERO;
        let mut gross_exposure = Decimal::ZERO;
        let mut max_drawdown = Decimal::ZERO;

        for pos in positions {
            // Use average entry price as a proxy when current price is unavailable.
            let notional = pos.quantity * pos.average_entry_price;
            net_exposure += notional;
            gross_exposure += notional.abs();

            // Track unrealized drawdown per position.
            if pos.unrealized_pnl < max_drawdown {
                max_drawdown = pos.unrealized_pnl;
            }
        }

        PortfolioRisk {
            total_var_95: Decimal::ZERO,
            total_var_99: Decimal::ZERO,
            net_exposure,
            gross_exposure,
            max_drawdown,
            daily_pnl,
            positions: positions.to_vec(),
            timestamp: Utc::now(),
        }
    }

    /// Build a [`CancelAllOrders`] message for a given exchange (convenience
    /// helper used by the kill-switch path).
    pub fn cancel_all_for_exchange(exchange: Exchange) -> CancelAllOrders {
        CancelAllOrders {
            exchange: Some(exchange),
            symbol: None,
            strategy_id: None,
        }
    }

    /// Return the number of registered risk checks.
    pub fn check_count(&self) -> usize {
        self.checks.len()
    }
}

impl Default for RiskEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use mm_core::{OrderType, Side, Symbol, TimeInForce};
    use rust_decimal_macros::dec;

    /// Stub risk check that always allows.
    struct AlwaysAllow;

    #[async_trait]
    impl RiskCheck for AlwaysAllow {
        fn name(&self) -> &str {
            "always_allow"
        }
        async fn check_order(&self, _: &NewOrder, _: &PortfolioRisk) -> RiskAction {
            RiskAction::Allow
        }
        async fn check_portfolio(&self, _: &PortfolioRisk) -> RiskAction {
            RiskAction::Allow
        }
    }

    /// Stub risk check that always rejects.
    struct AlwaysReject;

    #[async_trait]
    impl RiskCheck for AlwaysReject {
        fn name(&self) -> &str {
            "always_reject"
        }
        async fn check_order(&self, _: &NewOrder, _: &PortfolioRisk) -> RiskAction {
            RiskAction::Reject {
                reason: "test rejection".into(),
            }
        }
        async fn check_portfolio(&self, _: &PortfolioRisk) -> RiskAction {
            RiskAction::Reject {
                reason: "test rejection".into(),
            }
        }
    }

    /// Stub that requests reducing to a specific quantity.
    struct ReduceTo(Decimal);

    #[async_trait]
    impl RiskCheck for ReduceTo {
        fn name(&self) -> &str {
            "reduce_to"
        }
        async fn check_order(&self, _: &NewOrder, _: &PortfolioRisk) -> RiskAction {
            RiskAction::ReduceSize {
                max_quantity: self.0,
                reason: format!("reduce to {}", self.0),
            }
        }
        async fn check_portfolio(&self, _: &PortfolioRisk) -> RiskAction {
            RiskAction::Allow
        }
    }

    fn sample_order() -> NewOrder {
        NewOrder {
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            side: Side::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            price: Some(dec!(50000)),
            quantity: dec!(1),
            strategy_id: "test".into(),
            reduce_only: false,
        }
    }

    fn empty_portfolio() -> PortfolioRisk {
        PortfolioRisk {
            total_var_95: Decimal::ZERO,
            total_var_99: Decimal::ZERO,
            net_exposure: Decimal::ZERO,
            gross_exposure: Decimal::ZERO,
            max_drawdown: Decimal::ZERO,
            daily_pnl: Decimal::ZERO,
            positions: vec![],
            timestamp: Utc::now(),
        }
    }

    #[tokio::test]
    async fn all_allow_yields_allow() {
        let mut engine = RiskEngine::new();
        engine.add_check(Box::new(AlwaysAllow));
        engine.add_check(Box::new(AlwaysAllow));

        let result = engine.evaluate_order(&sample_order(), &empty_portfolio()).await;
        assert!(matches!(result, RiskAction::Allow));
    }

    #[tokio::test]
    async fn reject_short_circuits() {
        let mut engine = RiskEngine::new();
        engine.add_check(Box::new(AlwaysAllow));
        engine.add_check(Box::new(AlwaysReject));
        engine.add_check(Box::new(AlwaysAllow));

        let result = engine.evaluate_order(&sample_order(), &empty_portfolio()).await;
        assert!(matches!(result, RiskAction::Reject { .. }));
    }

    #[tokio::test]
    async fn most_restrictive_reduce_wins() {
        let mut engine = RiskEngine::new();
        engine.add_check(Box::new(ReduceTo(dec!(5))));
        engine.add_check(Box::new(ReduceTo(dec!(2))));
        engine.add_check(Box::new(ReduceTo(dec!(10))));

        let result = engine.evaluate_order(&sample_order(), &empty_portfolio()).await;
        match result {
            RiskAction::ReduceSize { max_quantity, .. } => {
                assert_eq!(max_quantity, dec!(2));
            }
            other => panic!("expected ReduceSize, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn empty_engine_allows_everything() {
        let engine = RiskEngine::new();
        let result = engine.evaluate_order(&sample_order(), &empty_portfolio()).await;
        assert!(matches!(result, RiskAction::Allow));
    }
}
