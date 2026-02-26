//! Integration test: kill switch activation cancels all orders.

use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use mm_core::{
    types::{Exchange, Position, Symbol},
    PortfolioRisk,
};
use mm_risk::{KillSwitch, KillSwitchConfig, KillSwitchTrigger};

fn make_symbol() -> Symbol {
    Symbol::new("BTC", "USDT")
}

#[test]
fn kill_switch_activates_on_max_daily_loss() {
    let exchanges = vec![Exchange::Binance, Exchange::Coinbase];
    let kill_switch = KillSwitch::new(KillSwitchConfig {
        max_daily_loss: dec!(1000),
        max_position_per_symbol: dec!(100),
        exchange_disconnect_timeout_secs: 30,
        heartbeat_timeout_secs: 10,
        exchanges: exchanges.clone(),
    });

    // Not active initially.
    assert!(!kill_switch.is_active());

    // Build a portfolio with daily PnL exceeding the threshold.
    let portfolio = PortfolioRisk {
        total_var_95: Decimal::ZERO,
        total_var_99: Decimal::ZERO,
        net_exposure: Decimal::ZERO,
        gross_exposure: Decimal::ZERO,
        max_drawdown: Decimal::ZERO,
        daily_pnl: dec!(-1500), // Exceeds -1000 threshold
        positions: Vec::new(),
        timestamp: Utc::now(),
    };

    let result = kill_switch.check_triggers(&portfolio, &[]);

    // The kill switch should trigger.
    assert!(result.is_some(), "kill switch should have triggered");

    // Now it should be active.
    assert!(kill_switch.is_active());

    let activation = result.unwrap();
    assert!(!activation.cancel_messages.is_empty());
}

#[test]
fn kill_switch_activates_on_position_limit() {
    let exchanges = vec![Exchange::Simulated];
    let kill_switch = KillSwitch::new(KillSwitchConfig {
        max_daily_loss: dec!(100_000), // high enough to not trigger
        max_position_per_symbol: dec!(5),
        exchange_disconnect_timeout_secs: 30,
        heartbeat_timeout_secs: 10,
        exchanges: exchanges.clone(),
    });

    let portfolio = PortfolioRisk {
        total_var_95: Decimal::ZERO,
        total_var_99: Decimal::ZERO,
        net_exposure: dec!(60_000),
        gross_exposure: dec!(60_000),
        max_drawdown: Decimal::ZERO,
        daily_pnl: Decimal::ZERO,
        positions: vec![Position {
            exchange: Exchange::Simulated,
            symbol: make_symbol(),
            quantity: dec!(10), // Exceeds 5 limit
            average_entry_price: dec!(50000),
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            updated_at: Utc::now(),
        }],
        timestamp: Utc::now(),
    };

    let result = kill_switch.check_triggers(&portfolio, &[]);
    assert!(result.is_some(), "kill switch should trigger on oversized position");
    assert!(kill_switch.is_active());
}

#[test]
fn kill_switch_can_be_manually_activated_and_checked() {
    let kill_switch = KillSwitch::new(KillSwitchConfig {
        max_daily_loss: dec!(100_000),
        max_position_per_symbol: dec!(100),
        exchange_disconnect_timeout_secs: 30,
        heartbeat_timeout_secs: 10,
        exchanges: vec![Exchange::Simulated],
    });

    assert!(!kill_switch.is_active());

    // Manually activate.
    let activation = kill_switch.activate(KillSwitchTrigger::ManualActivation {
        operator: "test_operator".to_string(),
    });
    assert!(kill_switch.is_active());
    assert!(!activation.cancel_messages.is_empty());
}

#[test]
fn kill_switch_heartbeat_keeps_alive() {
    let kill_switch = KillSwitch::new(KillSwitchConfig {
        max_daily_loss: dec!(100_000),
        max_position_per_symbol: dec!(100),
        exchange_disconnect_timeout_secs: 30,
        heartbeat_timeout_secs: 10,
        exchanges: vec![Exchange::Simulated],
    });

    // Record a heartbeat — should succeed without activating.
    kill_switch.record_heartbeat();
    assert!(!kill_switch.is_active());
}
