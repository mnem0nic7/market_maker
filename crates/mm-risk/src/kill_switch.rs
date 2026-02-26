use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::{error, warn};

use mm_core::{CancelAllOrders, Exchange, PortfolioRisk};

// ---------------------------------------------------------------------------
// Kill-switch trigger reasons
// ---------------------------------------------------------------------------

/// Enumeration of events that can activate the kill switch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KillSwitchTrigger {
    /// Daily loss exceeded the configured maximum.
    MaxDailyLoss {
        daily_pnl: Decimal,
        threshold: Decimal,
    },
    /// A single-symbol position exceeded the configured maximum.
    MaxPositionSize {
        symbol: String,
        position: Decimal,
        threshold: Decimal,
    },
    /// An exchange connection has been down longer than the allowed timeout.
    ExchangeDisconnect {
        exchange: Exchange,
        disconnected_for: Duration,
        timeout: Duration,
    },
    /// Internal heartbeat was not received within the expected interval.
    HeartbeatFailure {
        last_heartbeat: Option<DateTime<Utc>>,
        timeout: Duration,
    },
    /// A human operator activated the kill switch manually.
    ManualActivation { operator: String },
}

impl std::fmt::Display for KillSwitchTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KillSwitchTrigger::MaxDailyLoss {
                daily_pnl,
                threshold,
            } => write!(
                f,
                "max daily loss: PnL {} exceeded threshold -{}",
                daily_pnl, threshold,
            ),
            KillSwitchTrigger::MaxPositionSize {
                symbol,
                position,
                threshold,
            } => write!(
                f,
                "max position size: {} position {} exceeded threshold {}",
                symbol, position, threshold,
            ),
            KillSwitchTrigger::ExchangeDisconnect {
                exchange,
                disconnected_for,
                timeout,
            } => write!(
                f,
                "exchange disconnect: {} down for {}s (timeout {}s)",
                exchange,
                disconnected_for.num_seconds(),
                timeout.num_seconds(),
            ),
            KillSwitchTrigger::HeartbeatFailure {
                last_heartbeat,
                timeout,
            } => {
                let hb = last_heartbeat
                    .map(|t| t.to_rfc3339())
                    .unwrap_or_else(|| "never".into());
                write!(
                    f,
                    "heartbeat failure: last heartbeat at {}, timeout {}s",
                    hb,
                    timeout.num_seconds(),
                )
            }
            KillSwitchTrigger::ManualActivation { operator } => {
                write!(f, "manual activation by {}", operator)
            }
        }
    }
}

/// Record of a kill-switch activation, for audit purposes.
#[derive(Debug, Clone)]
pub struct KillSwitchActivation {
    pub trigger: KillSwitchTrigger,
    pub activated_at: DateTime<Utc>,
    /// Cancel-all messages to send to each exchange.
    pub cancel_messages: Vec<CancelAllOrders>,
}

// ---------------------------------------------------------------------------
// Exchange health state (input to the kill switch checker)
// ---------------------------------------------------------------------------

/// Snapshot of exchange connectivity health, provided by the caller.
#[derive(Debug, Clone)]
pub struct ExchangeState {
    pub exchange: Exchange,
    pub connected: bool,
    pub last_message_at: Option<DateTime<Utc>>,
}

// ---------------------------------------------------------------------------
// Kill-switch configuration
// ---------------------------------------------------------------------------

/// Configuration for the kill-switch trigger thresholds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KillSwitchConfig {
    /// Maximum daily loss (positive value).
    pub max_daily_loss: Decimal,
    /// Maximum position size per symbol (positive value).
    pub max_position_per_symbol: Decimal,
    /// Maximum allowed exchange disconnection time.
    pub exchange_disconnect_timeout_secs: u64,
    /// Maximum time between internal heartbeats.
    pub heartbeat_timeout_secs: u64,
    /// List of exchanges the kill switch should cancel orders on when
    /// activated.
    pub exchanges: Vec<Exchange>,
}

// ---------------------------------------------------------------------------
// KillSwitch
// ---------------------------------------------------------------------------

/// Global kill switch using an `AtomicBool` for lock-free status checks.
///
/// When activated, the kill switch generates `CancelAllOrders` messages for
/// every configured exchange and sets an atomic flag that can be cheaply
/// polled from any async task.
///
/// Resetting the kill switch requires an explicit [`KillSwitch::reset`] call
/// and is logged at `WARN` level for audit purposes.
pub struct KillSwitch {
    /// Atomic flag: `true` when the kill switch is active.
    active: Arc<AtomicBool>,
    config: KillSwitchConfig,
    /// Mutable state protected by a parking_lot mutex.
    inner: Mutex<KillSwitchInner>,
}

struct KillSwitchInner {
    /// History of activations (append-only for audit).
    activations: Vec<KillSwitchActivation>,
    /// Timestamp of the last internal heartbeat.
    last_heartbeat: Option<DateTime<Utc>>,
}

impl KillSwitch {
    /// Create a new kill switch in the inactive state.
    pub fn new(config: KillSwitchConfig) -> Self {
        Self {
            active: Arc::new(AtomicBool::new(false)),
            config,
            inner: Mutex::new(KillSwitchInner {
                activations: Vec::new(),
                last_heartbeat: Some(Utc::now()),
            }),
        }
    }

    // ------------------------------------------------------------------
    // Activation
    // ------------------------------------------------------------------

    /// Activate the kill switch for the given reason.
    ///
    /// Returns a `KillSwitchActivation` containing `CancelAllOrders` messages
    /// for every configured exchange.
    pub fn activate(&self, trigger: KillSwitchTrigger) -> KillSwitchActivation {
        error!(reason = %trigger, "KILL SWITCH ACTIVATED");

        self.active.store(true, Ordering::SeqCst);

        let cancel_messages: Vec<CancelAllOrders> = self
            .config
            .exchanges
            .iter()
            .map(|ex| CancelAllOrders {
                exchange: Some(*ex),
                symbol: None,
                strategy_id: None,
            })
            .collect();

        let activation = KillSwitchActivation {
            trigger,
            activated_at: Utc::now(),
            cancel_messages: cancel_messages.clone(),
        };

        {
            let mut inner = self.inner.lock();
            inner.activations.push(activation.clone());
        }

        KillSwitchActivation {
            trigger: activation.trigger,
            activated_at: activation.activated_at,
            cancel_messages,
        }
    }

    // ------------------------------------------------------------------
    // Status queries (lock-free)
    // ------------------------------------------------------------------

    /// Check if the kill switch is currently active.
    ///
    /// This is an atomic read -- no lock required, safe to call from any
    /// thread or async task on the hot path.
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    /// Return a clone of the `Arc<AtomicBool>` so other components can cheaply
    /// poll kill-switch state without holding a reference to the `KillSwitch`.
    pub fn active_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.active)
    }

    // ------------------------------------------------------------------
    // Reset
    // ------------------------------------------------------------------

    /// Reset the kill switch to inactive. This requires an explicit call and
    /// is logged at `WARN` level.
    pub fn reset(&self) {
        warn!("KILL SWITCH RESET -- trading can resume");
        self.active.store(false, Ordering::SeqCst);
    }

    // ------------------------------------------------------------------
    // Heartbeat
    // ------------------------------------------------------------------

    /// Record an internal heartbeat. Call this periodically from the main
    /// event loop to prove the system is alive.
    pub fn record_heartbeat(&self) {
        let mut inner = self.inner.lock();
        inner.last_heartbeat = Some(Utc::now());
    }

    // ------------------------------------------------------------------
    // Trigger evaluation
    // ------------------------------------------------------------------

    /// Evaluate all automatic triggers against the current portfolio and
    /// exchange states.
    ///
    /// Returns `Some(KillSwitchActivation)` if any trigger fires (and the kill
    /// switch was not already active). Returns `None` if everything is within
    /// bounds or the kill switch is already active.
    pub fn check_triggers(
        &self,
        portfolio: &PortfolioRisk,
        exchange_states: &[ExchangeState],
    ) -> Option<KillSwitchActivation> {
        // If already active, nothing more to do.
        if self.is_active() {
            return None;
        }

        let now = Utc::now();

        // --- Max daily loss ---
        if portfolio.daily_pnl <= -self.config.max_daily_loss {
            return Some(self.activate(KillSwitchTrigger::MaxDailyLoss {
                daily_pnl: portfolio.daily_pnl,
                threshold: self.config.max_daily_loss,
            }));
        }

        // --- Max position size per symbol ---
        for pos in &portfolio.positions {
            if pos.quantity.abs() > self.config.max_position_per_symbol {
                return Some(self.activate(KillSwitchTrigger::MaxPositionSize {
                    symbol: pos.symbol.canonical(),
                    position: pos.quantity.abs(),
                    threshold: self.config.max_position_per_symbol,
                }));
            }
        }

        // --- Exchange disconnect ---
        let disconnect_timeout =
            Duration::seconds(self.config.exchange_disconnect_timeout_secs as i64);
        for es in exchange_states {
            if !es.connected {
                let disconnected_for = match es.last_message_at {
                    Some(last) => now - last,
                    None => disconnect_timeout + Duration::seconds(1), // treat "never connected" as timed out
                };
                if disconnected_for >= disconnect_timeout {
                    return Some(self.activate(KillSwitchTrigger::ExchangeDisconnect {
                        exchange: es.exchange,
                        disconnected_for,
                        timeout: disconnect_timeout,
                    }));
                }
            }
        }

        // --- Heartbeat failure ---
        let heartbeat_timeout = Duration::seconds(self.config.heartbeat_timeout_secs as i64);
        {
            let inner = self.inner.lock();
            if let Some(last_hb) = inner.last_heartbeat {
                if now - last_hb >= heartbeat_timeout {
                    // Drop the lock before calling activate (which takes the lock).
                    let last = inner.last_heartbeat;
                    drop(inner);
                    return Some(self.activate(KillSwitchTrigger::HeartbeatFailure {
                        last_heartbeat: last,
                        timeout: heartbeat_timeout,
                    }));
                }
            }
        }

        None
    }

    // ------------------------------------------------------------------
    // Audit
    // ------------------------------------------------------------------

    /// Return a copy of the activation history.
    pub fn activation_history(&self) -> Vec<KillSwitchActivation> {
        self.inner.lock().activations.clone()
    }

    /// Return the number of times the kill switch has been activated.
    pub fn activation_count(&self) -> usize {
        self.inner.lock().activations.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use mm_core::{Position, Symbol};
    use rust_decimal_macros::dec;

    fn test_config() -> KillSwitchConfig {
        KillSwitchConfig {
            max_daily_loss: dec!(10_000),
            max_position_per_symbol: dec!(100),
            exchange_disconnect_timeout_secs: 30,
            heartbeat_timeout_secs: 10,
            exchanges: vec![Exchange::Binance, Exchange::Coinbase],
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

    fn healthy_exchanges() -> Vec<ExchangeState> {
        vec![
            ExchangeState {
                exchange: Exchange::Binance,
                connected: true,
                last_message_at: Some(Utc::now()),
            },
            ExchangeState {
                exchange: Exchange::Coinbase,
                connected: true,
                last_message_at: Some(Utc::now()),
            },
        ]
    }

    #[test]
    fn starts_inactive() {
        let ks = KillSwitch::new(test_config());
        assert!(!ks.is_active());
    }

    #[test]
    fn manual_activation() {
        let ks = KillSwitch::new(test_config());
        let activation = ks.activate(KillSwitchTrigger::ManualActivation {
            operator: "test_operator".into(),
        });

        assert!(ks.is_active());
        assert_eq!(activation.cancel_messages.len(), 2);
        assert_eq!(ks.activation_count(), 1);
    }

    #[test]
    fn reset_clears_active_flag() {
        let ks = KillSwitch::new(test_config());
        ks.activate(KillSwitchTrigger::ManualActivation {
            operator: "test".into(),
        });
        assert!(ks.is_active());

        ks.reset();
        assert!(!ks.is_active());
        // Activation history is preserved even after reset.
        assert_eq!(ks.activation_count(), 1);
    }

    #[test]
    fn triggers_on_daily_loss() {
        let ks = KillSwitch::new(test_config());
        let mut portfolio = empty_portfolio();
        portfolio.daily_pnl = dec!(-15_000);

        let result = ks.check_triggers(&portfolio, &healthy_exchanges());
        assert!(result.is_some());
        assert!(ks.is_active());
    }

    #[test]
    fn triggers_on_position_size() {
        let ks = KillSwitch::new(test_config());
        let mut portfolio = empty_portfolio();
        portfolio.positions.push(Position {
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            quantity: dec!(150),
            average_entry_price: dec!(50000),
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            updated_at: Utc::now(),
        });

        let result = ks.check_triggers(&portfolio, &healthy_exchanges());
        assert!(result.is_some());
        assert!(ks.is_active());
    }

    #[test]
    fn triggers_on_exchange_disconnect() {
        let ks = KillSwitch::new(test_config());
        let exchanges = vec![
            ExchangeState {
                exchange: Exchange::Binance,
                connected: false,
                last_message_at: Some(Utc::now() - Duration::seconds(60)),
            },
            ExchangeState {
                exchange: Exchange::Coinbase,
                connected: true,
                last_message_at: Some(Utc::now()),
            },
        ];

        let result = ks.check_triggers(&empty_portfolio(), &exchanges);
        assert!(result.is_some());
        assert!(ks.is_active());
    }

    #[test]
    fn no_trigger_when_healthy() {
        let ks = KillSwitch::new(test_config());
        let result = ks.check_triggers(&empty_portfolio(), &healthy_exchanges());
        assert!(result.is_none());
        assert!(!ks.is_active());
    }

    #[test]
    fn does_not_double_trigger() {
        let ks = KillSwitch::new(test_config());
        let mut portfolio = empty_portfolio();
        portfolio.daily_pnl = dec!(-15_000);

        let r1 = ks.check_triggers(&portfolio, &healthy_exchanges());
        assert!(r1.is_some());

        // Already active -- check_triggers should return None.
        let r2 = ks.check_triggers(&portfolio, &healthy_exchanges());
        assert!(r2.is_none());
        assert_eq!(ks.activation_count(), 1);
    }

    #[test]
    fn cancel_messages_target_configured_exchanges() {
        let ks = KillSwitch::new(test_config());
        let activation = ks.activate(KillSwitchTrigger::ManualActivation {
            operator: "ops".into(),
        });

        let exchanges: Vec<Option<Exchange>> = activation
            .cancel_messages
            .iter()
            .map(|c| c.exchange)
            .collect();
        assert_eq!(
            exchanges,
            vec![Some(Exchange::Binance), Some(Exchange::Coinbase)]
        );
    }

    #[test]
    fn active_flag_is_shareable() {
        let ks = KillSwitch::new(test_config());
        let flag = ks.active_flag();

        assert!(!flag.load(Ordering::SeqCst));
        ks.activate(KillSwitchTrigger::ManualActivation {
            operator: "test".into(),
        });
        assert!(flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_kill_switch_exchange_disconnect_trigger() {
        let ks = KillSwitch::new(test_config());
        // Exchange disconnected for longer than the timeout (30s).
        let exchanges = vec![ExchangeState {
            exchange: Exchange::Binance,
            connected: false,
            last_message_at: Some(Utc::now() - Duration::seconds(60)),
        }];

        let activation = ks.check_triggers(&empty_portfolio(), &exchanges);
        assert!(activation.is_some());
        let activation = activation.unwrap();

        // Verify the trigger is ExchangeDisconnect.
        match &activation.trigger {
            KillSwitchTrigger::ExchangeDisconnect {
                exchange, timeout, ..
            } => {
                assert_eq!(*exchange, Exchange::Binance);
                assert_eq!(timeout.num_seconds(), 30);
            }
            other => panic!("expected ExchangeDisconnect trigger, got {:?}", other),
        }
        assert!(ks.is_active());
    }

    #[test]
    fn test_kill_switch_exchange_never_connected_triggers() {
        let ks = KillSwitch::new(test_config());
        // Exchange that has never sent a message (last_message_at = None).
        let exchanges = vec![ExchangeState {
            exchange: Exchange::Coinbase,
            connected: false,
            last_message_at: None,
        }];

        let activation = ks.check_triggers(&empty_portfolio(), &exchanges);
        assert!(
            activation.is_some(),
            "exchange that never connected should trigger kill switch"
        );
        assert!(ks.is_active());
    }

    #[test]
    fn test_kill_switch_exchange_disconnect_within_timeout_no_trigger() {
        let ks = KillSwitch::new(test_config());
        // Exchange disconnected but within the 30s timeout.
        let exchanges = vec![ExchangeState {
            exchange: Exchange::Binance,
            connected: false,
            last_message_at: Some(Utc::now() - Duration::seconds(10)),
        }];

        let result = ks.check_triggers(&empty_portfolio(), &exchanges);
        assert!(
            result.is_none(),
            "disconnect within timeout should not trigger"
        );
        assert!(!ks.is_active());
    }

    #[test]
    fn test_kill_switch_heartbeat_timeout_trigger() {
        let mut config = test_config();
        config.heartbeat_timeout_secs = 2;
        let ks = KillSwitch::new(config);

        // Manually set last heartbeat to far in the past by not recording any
        // new heartbeat and using a very short timeout.
        // The constructor sets last_heartbeat to Utc::now(). We need to
        // simulate time passing. Set last heartbeat to 5 seconds ago.
        {
            let mut inner = ks.inner.lock();
            inner.last_heartbeat = Some(Utc::now() - Duration::seconds(5));
        }

        let activation = ks.check_triggers(&empty_portfolio(), &healthy_exchanges());
        assert!(
            activation.is_some(),
            "stale heartbeat should trigger kill switch"
        );

        let activation = activation.unwrap();
        match &activation.trigger {
            KillSwitchTrigger::HeartbeatFailure { timeout, .. } => {
                assert_eq!(timeout.num_seconds(), 2);
            }
            other => panic!("expected HeartbeatFailure trigger, got {:?}", other),
        }
        assert!(ks.is_active());
    }

    #[test]
    fn test_kill_switch_heartbeat_refresh_prevents_trigger() {
        let mut config = test_config();
        config.heartbeat_timeout_secs = 2;
        let ks = KillSwitch::new(config);

        // Record a fresh heartbeat — should prevent timeout trigger.
        ks.record_heartbeat();

        let result = ks.check_triggers(&empty_portfolio(), &healthy_exchanges());
        assert!(
            result.is_none(),
            "fresh heartbeat should not trigger kill switch"
        );
        assert!(!ks.is_active());
    }

    #[test]
    fn test_kill_switch_position_limit_trigger() {
        let ks = KillSwitch::new(test_config());
        let mut portfolio = empty_portfolio();
        portfolio.positions.push(Position {
            exchange: Exchange::Binance,
            symbol: Symbol::new("ETH", "USDT"),
            quantity: dec!(200), // exceeds max_position_per_symbol of 100
            average_entry_price: dec!(3000),
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            updated_at: Utc::now(),
        });

        let activation = ks.check_triggers(&portfolio, &healthy_exchanges());
        assert!(activation.is_some());

        let activation = activation.unwrap();
        match &activation.trigger {
            KillSwitchTrigger::MaxPositionSize {
                symbol,
                position,
                threshold,
            } => {
                assert_eq!(symbol, "ETH/USDT");
                assert_eq!(*position, dec!(200));
                assert_eq!(*threshold, dec!(100));
            }
            other => panic!("expected MaxPositionSize trigger, got {:?}", other),
        }
    }

    #[test]
    fn test_kill_switch_reset() {
        let ks = KillSwitch::new(test_config());

        // Activate the kill switch.
        ks.activate(KillSwitchTrigger::ManualActivation {
            operator: "test".into(),
        });
        assert!(ks.is_active());
        assert_eq!(ks.activation_count(), 1);

        // Reset clears the active state.
        ks.reset();
        assert!(!ks.is_active());

        // The shared flag also reflects the reset.
        let flag = ks.active_flag();
        assert!(!flag.load(Ordering::SeqCst));

        // Activation history is preserved after reset.
        assert_eq!(ks.activation_count(), 1);

        // Can be re-triggered after reset.
        let mut portfolio = empty_portfolio();
        portfolio.daily_pnl = dec!(-15_000);
        let result = ks.check_triggers(&portfolio, &healthy_exchanges());
        assert!(result.is_some());
        assert!(ks.is_active());
        assert_eq!(ks.activation_count(), 2);
    }

    #[test]
    fn test_kill_switch_active_flag_is_thread_safe() {
        use std::thread;

        let ks = KillSwitch::new(test_config());
        let flag = ks.active_flag();

        // Spawn a thread that polls the flag.
        let flag_clone = Arc::clone(&flag);
        let handle = thread::spawn(move || {
            // Busy-wait until flag becomes true (with a safety limit).
            for _ in 0..1_000_000 {
                if flag_clone.load(Ordering::SeqCst) {
                    return true;
                }
                std::hint::spin_loop();
            }
            false
        });

        // Activate from the main thread.
        ks.activate(KillSwitchTrigger::ManualActivation {
            operator: "thread_test".into(),
        });

        let seen_active = handle.join().expect("thread panicked");
        assert!(
            seen_active,
            "background thread should have seen the kill switch activation"
        );
    }

    #[test]
    fn test_kill_switch_trigger_display_messages() {
        // Verify Display implementations produce reasonable messages.
        let trigger = KillSwitchTrigger::MaxDailyLoss {
            daily_pnl: dec!(-15000),
            threshold: dec!(10000),
        };
        let msg = format!("{}", trigger);
        assert!(msg.contains("max daily loss"), "got: {}", msg);

        let trigger = KillSwitchTrigger::ManualActivation {
            operator: "admin".into(),
        };
        let msg = format!("{}", trigger);
        assert!(msg.contains("manual activation"), "got: {}", msg);
        assert!(msg.contains("admin"), "got: {}", msg);

        let trigger = KillSwitchTrigger::ExchangeDisconnect {
            exchange: Exchange::Binance,
            disconnected_for: Duration::seconds(60),
            timeout: Duration::seconds(30),
        };
        let msg = format!("{}", trigger);
        assert!(msg.contains("exchange disconnect"), "got: {}", msg);
        assert!(msg.contains("60"), "got: {}", msg);

        let trigger = KillSwitchTrigger::HeartbeatFailure {
            last_heartbeat: None,
            timeout: Duration::seconds(10),
        };
        let msg = format!("{}", trigger);
        assert!(msg.contains("heartbeat failure"), "got: {}", msg);
        assert!(msg.contains("never"), "got: {}", msg);
    }
}
