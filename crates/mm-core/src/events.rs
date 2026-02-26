use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::types::{Exchange, OrderStatus, OrderType, Side, Symbol, TimeInForce};

/// Every state change in the system is captured as an immutable event.
/// Events are published to NATS JetStream for persistence, audit, and replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub event_id: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub source: String,
    pub payload: Event,
}

impl Envelope {
    pub fn new(
        source: impl Into<String>,
        correlation_id: impl Into<String>,
        payload: Event,
    ) -> Self {
        Self {
            event_id: Uuid::new_v4().to_string(),
            correlation_id: correlation_id.into(),
            causation_id: None,
            timestamp: Utc::now(),
            source: source.into(),
            payload,
        }
    }

    pub fn with_cause(mut self, causation_id: impl Into<String>) -> Self {
        self.causation_id = Some(causation_id.into());
        self
    }

    pub fn nats_subject(&self) -> String {
        match &self.payload {
            Event::OrderSubmitted {
                exchange, symbol, ..
            } => {
                format!("events.order.submitted.{}.{}", exchange, symbol)
            }
            Event::OrderAcknowledged {
                exchange, symbol, ..
            } => {
                format!("events.order.ack.{}.{}", exchange, symbol)
            }
            Event::OrderFilled {
                exchange, symbol, ..
            }
            | Event::OrderPartiallyFilled {
                exchange, symbol, ..
            } => {
                format!("events.order.fill.{}.{}", exchange, symbol)
            }
            Event::OrderCancelled {
                exchange, symbol, ..
            } => {
                format!("events.order.cancelled.{}.{}", exchange, symbol)
            }
            Event::OrderRejected {
                exchange, symbol, ..
            } => {
                format!("events.order.rejected.{}.{}", exchange, symbol)
            }
            Event::PositionUpdated {
                exchange, symbol, ..
            } => {
                format!("events.position.{}.{}", exchange, symbol)
            }
            Event::RiskLimitBreached { .. } => "events.risk.breach".to_string(),
            Event::KillSwitchActivated { .. } => "events.risk.killswitch".to_string(),
            Event::CircuitBreakerTriggered { .. } => "events.risk.circuitbreaker".to_string(),
            Event::StrategySignal { strategy_id, .. } => {
                format!("events.strategy.signal.{}", strategy_id)
            }
            Event::MarketDataUpdate {
                exchange, symbol, ..
            } => {
                format!("events.marketdata.{}.{}", exchange, symbol)
            }
            Event::SystemHealthCheck { .. } => "events.system.health".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Event {
    // Order lifecycle
    OrderSubmitted {
        client_order_id: String,
        strategy_id: String,
        exchange: Exchange,
        symbol: Symbol,
        side: Side,
        order_type: OrderType,
        time_in_force: TimeInForce,
        price: Option<Decimal>,
        quantity: Decimal,
    },
    OrderAcknowledged {
        client_order_id: String,
        exchange_order_id: String,
        exchange: Exchange,
        symbol: Symbol,
    },
    OrderPartiallyFilled {
        client_order_id: String,
        exchange_order_id: String,
        exchange: Exchange,
        symbol: Symbol,
        fill_price: Decimal,
        fill_quantity: Decimal,
        cumulative_quantity: Decimal,
        remaining_quantity: Decimal,
        fee: Decimal,
        fee_currency: String,
    },
    OrderFilled {
        client_order_id: String,
        exchange_order_id: String,
        exchange: Exchange,
        symbol: Symbol,
        fill_price: Decimal,
        fill_quantity: Decimal,
        total_quantity: Decimal,
        fee: Decimal,
        fee_currency: String,
    },
    OrderCancelled {
        client_order_id: String,
        exchange_order_id: Option<String>,
        exchange: Exchange,
        symbol: Symbol,
        reason: String,
    },
    OrderRejected {
        client_order_id: String,
        exchange: Exchange,
        symbol: Symbol,
        reason: String,
    },

    // Position
    PositionUpdated {
        exchange: Exchange,
        symbol: Symbol,
        quantity: Decimal,
        average_entry_price: Decimal,
        unrealized_pnl: Decimal,
        realized_pnl: Decimal,
    },

    // Risk
    RiskLimitBreached {
        limit_type: String,
        current_value: Decimal,
        threshold: Decimal,
        action_taken: String,
    },
    KillSwitchActivated {
        reason: String,
        orders_cancelled: u32,
    },
    CircuitBreakerTriggered {
        level: String,
        trigger: String,
        action: String,
    },

    // Strategy
    StrategySignal {
        strategy_id: String,
        signal_type: String,
        details: serde_json::Value,
    },

    // Market data (sampled, not every tick)
    MarketDataUpdate {
        exchange: Exchange,
        symbol: Symbol,
        best_bid: Decimal,
        best_ask: Decimal,
        mid_price: Decimal,
        spread_bps: Decimal,
    },

    // System
    SystemHealthCheck {
        component: String,
        status: String,
        details: serde_json::Value,
    },
}

/// Lightweight struct for passing order status updates through channels
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderUpdate {
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub status: OrderStatus,
    pub filled_quantity: Decimal,
    pub remaining_quantity: Decimal,
    pub average_fill_price: Decimal,
    pub last_fill_price: Option<Decimal>,
    pub last_fill_quantity: Option<Decimal>,
    pub fee: Option<Decimal>,
    pub fee_currency: Option<String>,
    pub timestamp: DateTime<Utc>,
}
