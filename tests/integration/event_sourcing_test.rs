//! Integration test: events are serializable and structurally correct.

use rust_decimal_macros::dec;

use mm_core::{
    events::{Envelope, Event},
    types::{Exchange, OrderType, Side, Symbol, TimeInForce},
};

#[test]
fn envelope_serializes_and_deserializes() {
    let envelope = Envelope::new(
        "test-source",
        "correlation-123",
        Event::OrderSubmitted {
            client_order_id: "ord-001".into(),
            strategy_id: "spread-1".into(),
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            side: Side::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::GTC,
            price: Some(dec!(50000)),
            quantity: dec!(0.1),
        },
    );

    // Serialize to JSON.
    let json = serde_json::to_string(&envelope).expect("should serialize");

    // Deserialize back.
    let deserialized: Envelope = serde_json::from_str(&json).expect("should deserialize");

    assert_eq!(deserialized.source, "test-source");
    assert_eq!(deserialized.correlation_id, "correlation-123");
    assert_eq!(deserialized.event_id, envelope.event_id);

    match &deserialized.payload {
        Event::OrderSubmitted {
            client_order_id,
            exchange,
            symbol,
            side,
            price,
            quantity,
            ..
        } => {
            assert_eq!(client_order_id, "ord-001");
            assert_eq!(*exchange, Exchange::Binance);
            assert_eq!(symbol.base, "BTC");
            assert_eq!(symbol.quote, "USDT");
            assert_eq!(*side, Side::Buy);
            assert_eq!(*price, Some(dec!(50000)));
            assert_eq!(*quantity, dec!(0.1));
        }
        other => panic!("unexpected event type: {other:?}"),
    }
}

#[test]
fn envelope_nats_subject_is_correct() {
    let envelope = Envelope::new(
        "test",
        "corr",
        Event::OrderFilled {
            client_order_id: "ord-001".into(),
            exchange_order_id: "exch-001".into(),
            exchange: Exchange::Binance,
            symbol: Symbol::new("ETH", "USDT"),
            fill_price: dec!(3000),
            fill_quantity: dec!(1),
            total_quantity: dec!(1),
            fee: dec!(0.01),
            fee_currency: "USDT".into(),
        },
    );

    let subject = envelope.nats_subject();
    assert!(
        subject.starts_with("events.order.fill."),
        "subject should be events.order.fill.*: {subject}"
    );
}

#[test]
fn kill_switch_event_serializes() {
    let envelope = Envelope::new(
        "risk-engine",
        "ks-activation",
        Event::KillSwitchActivated {
            reason: "max daily loss exceeded".into(),
            orders_cancelled: 15,
        },
    );

    let json = serde_json::to_string(&envelope).expect("should serialize");
    assert!(json.contains("KillSwitchActivated"));
    assert!(json.contains("max daily loss exceeded"));

    let subject = envelope.nats_subject();
    assert_eq!(subject, "events.risk.killswitch");
}

#[test]
fn envelope_with_cause_preserves_causation_id() {
    let envelope = Envelope::new(
        "oms",
        "corr-1",
        Event::OrderCancelled {
            client_order_id: "ord-1".into(),
            exchange_order_id: Some("exch-1".into()),
            exchange: Exchange::Coinbase,
            symbol: Symbol::new("BTC", "USD"),
            reason: "user requested".into(),
        },
    )
    .with_cause("cause-abc");

    assert_eq!(envelope.causation_id.as_deref(), Some("cause-abc"));

    let json = serde_json::to_string(&envelope).unwrap();
    let back: Envelope = serde_json::from_str(&json).unwrap();
    assert_eq!(back.causation_id.as_deref(), Some("cause-abc"));
}

#[test]
fn all_event_variants_serialize() {
    // Ensure every Event variant can be serialized without panicking.
    let events = vec![
        Event::OrderSubmitted {
            client_order_id: "a".into(),
            strategy_id: "s".into(),
            exchange: Exchange::Kraken,
            symbol: Symbol::new("ETH", "USD"),
            side: Side::Sell,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::IOC,
            price: Some(dec!(3000)),
            quantity: dec!(1),
        },
        Event::OrderAcknowledged {
            client_order_id: "a".into(),
            exchange_order_id: "e".into(),
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
        },
        Event::OrderFilled {
            client_order_id: "a".into(),
            exchange_order_id: "e".into(),
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            fill_price: dec!(50000),
            fill_quantity: dec!(0.1),
            total_quantity: dec!(0.1),
            fee: dec!(0.005),
            fee_currency: "BTC".into(),
        },
        Event::OrderPartiallyFilled {
            client_order_id: "a".into(),
            exchange_order_id: "e".into(),
            exchange: Exchange::Coinbase,
            symbol: Symbol::new("BTC", "USD"),
            fill_price: dec!(50000),
            fill_quantity: dec!(0.05),
            cumulative_quantity: dec!(0.05),
            remaining_quantity: dec!(0.05),
            fee: dec!(0.001),
            fee_currency: "USD".into(),
        },
        Event::OrderCancelled {
            client_order_id: "a".into(),
            exchange_order_id: Some("e".into()),
            exchange: Exchange::Kraken,
            symbol: Symbol::new("ETH", "USD"),
            reason: "cancelled".into(),
        },
        Event::OrderRejected {
            client_order_id: "a".into(),
            exchange: Exchange::Binance,
            symbol: Symbol::new("BTC", "USDT"),
            reason: "insufficient funds".into(),
        },
        Event::KillSwitchActivated {
            reason: "test".into(),
            orders_cancelled: 0,
        },
    ];

    for event in events {
        let env = Envelope::new("test", "corr", event);
        let json = serde_json::to_string(&env).expect("all events should serialize");
        let _back: Envelope = serde_json::from_str(&json).expect("all events should deserialize");
    }
}
