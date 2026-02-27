use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::tools::registry::ToolRegistry;
use crate::tools::{Tool, ToolOutput, ToolSchema};

/// Trait providing access to live trading state.
///
/// Implemented by the gateway's ApiState to avoid circular dependencies
/// between mm-research and mm-gateway.
#[async_trait]
pub trait TradingContext: Send + Sync {
    async fn get_positions(&self) -> Vec<Value>;
    async fn get_order_books(&self) -> Vec<Value>;
    async fn get_strategies(&self) -> Vec<Value>;
    async fn get_risk_state(&self) -> Value;
}

// ---------------------------------------------------------------------------
// Tool: get_positions
// ---------------------------------------------------------------------------

struct PositionsTool(Arc<dyn TradingContext>);

#[async_trait]
impl Tool for PositionsTool {
    fn name(&self) -> &str {
        "get_trading_positions"
    }
    fn description(&self) -> &str {
        "Returns current trading positions from the live platform, including exchange, symbol, quantity, entry price, and unrealized PnL."
    }
    fn parameters_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "object".into(),
            properties: serde_json::json!({}),
            required: vec![],
        }
    }
    async fn invoke(&self, _params: Value) -> anyhow::Result<ToolOutput> {
        let positions = self.0.get_positions().await;
        Ok(ToolOutput::success(
            self.name(),
            serde_json::to_string(&positions)?,
        ))
    }
}

// ---------------------------------------------------------------------------
// Tool: get_order_book
// ---------------------------------------------------------------------------

struct OrderBookTool(Arc<dyn TradingContext>);

#[async_trait]
impl Tool for OrderBookTool {
    fn name(&self) -> &str {
        "get_trading_order_books"
    }
    fn description(&self) -> &str {
        "Returns current order book snapshots from the live platform, showing top bid/ask levels for active trading pairs."
    }
    fn parameters_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "object".into(),
            properties: serde_json::json!({}),
            required: vec![],
        }
    }
    async fn invoke(&self, _params: Value) -> anyhow::Result<ToolOutput> {
        let books = self.0.get_order_books().await;
        Ok(ToolOutput::success(
            self.name(),
            serde_json::to_string(&books)?,
        ))
    }
}

// ---------------------------------------------------------------------------
// Tool: get_strategies
// ---------------------------------------------------------------------------

struct StrategiesTool(Arc<dyn TradingContext>);

#[async_trait]
impl Tool for StrategiesTool {
    fn name(&self) -> &str {
        "get_trading_strategies"
    }
    fn description(&self) -> &str {
        "Returns active trading strategies and their configurations from the live platform."
    }
    fn parameters_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "object".into(),
            properties: serde_json::json!({}),
            required: vec![],
        }
    }
    async fn invoke(&self, _params: Value) -> anyhow::Result<ToolOutput> {
        let strategies = self.0.get_strategies().await;
        Ok(ToolOutput::success(
            self.name(),
            serde_json::to_string(&strategies)?,
        ))
    }
}

// ---------------------------------------------------------------------------
// Tool: get_risk_state
// ---------------------------------------------------------------------------

struct RiskStateTool(Arc<dyn TradingContext>);

#[async_trait]
impl Tool for RiskStateTool {
    fn name(&self) -> &str {
        "get_trading_risk_state"
    }
    fn description(&self) -> &str {
        "Returns current risk state from the live platform, including kill switch status, exposure levels, and PnL limits."
    }
    fn parameters_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "object".into(),
            properties: serde_json::json!({}),
            required: vec![],
        }
    }
    async fn invoke(&self, _params: Value) -> anyhow::Result<ToolOutput> {
        let risk = self.0.get_risk_state().await;
        Ok(ToolOutput::success(
            self.name(),
            serde_json::to_string(&risk)?,
        ))
    }
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/// Register all trading context tools into the given registry.
pub fn register_trading_tools(registry: &mut ToolRegistry, context: Arc<dyn TradingContext>) {
    registry.register(Arc::new(PositionsTool(Arc::clone(&context))));
    registry.register(Arc::new(OrderBookTool(Arc::clone(&context))));
    registry.register(Arc::new(StrategiesTool(Arc::clone(&context))));
    registry.register(Arc::new(RiskStateTool(context)));
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockTradingContext;

    #[async_trait]
    impl TradingContext for MockTradingContext {
        async fn get_positions(&self) -> Vec<Value> {
            vec![serde_json::json!({
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "quantity": "0.5",
                "unrealized_pnl": "125.50"
            })]
        }
        async fn get_order_books(&self) -> Vec<Value> {
            vec![serde_json::json!({
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "best_bid": "50000.00",
                "best_ask": "50001.00"
            })]
        }
        async fn get_strategies(&self) -> Vec<Value> {
            vec![serde_json::json!({
                "strategy_id": "spread-1",
                "strategy_type": "spread",
                "running": true
            })]
        }
        async fn get_risk_state(&self) -> Value {
            serde_json::json!({
                "kill_switch_active": false,
                "net_exposure": "0.5",
                "daily_pnl": "125.50"
            })
        }
    }

    #[test]
    fn test_register_all_trading_tools() {
        let ctx: Arc<dyn TradingContext> = Arc::new(MockTradingContext);
        let mut registry = ToolRegistry::new();
        register_trading_tools(&mut registry, ctx);

        assert_eq!(registry.len(), 4);
        assert!(registry.get("get_trading_positions").is_some());
        assert!(registry.get("get_trading_order_books").is_some());
        assert!(registry.get("get_trading_strategies").is_some());
        assert!(registry.get("get_trading_risk_state").is_some());
    }

    #[tokio::test]
    async fn test_positions_tool() {
        let ctx: Arc<dyn TradingContext> = Arc::new(MockTradingContext);
        let tool = PositionsTool(ctx);
        let output = tool.invoke(serde_json::json!({})).await.unwrap();
        assert!(output.success);
        assert!(output.content.contains("BTC/USDT"));
        assert!(output.content.contains("0.5"));
    }

    #[tokio::test]
    async fn test_order_book_tool() {
        let ctx: Arc<dyn TradingContext> = Arc::new(MockTradingContext);
        let tool = OrderBookTool(ctx);
        let output = tool.invoke(serde_json::json!({})).await.unwrap();
        assert!(output.success);
        assert!(output.content.contains("50000"));
    }

    #[tokio::test]
    async fn test_strategies_tool() {
        let ctx: Arc<dyn TradingContext> = Arc::new(MockTradingContext);
        let tool = StrategiesTool(ctx);
        let output = tool.invoke(serde_json::json!({})).await.unwrap();
        assert!(output.success);
        assert!(output.content.contains("spread-1"));
    }

    #[tokio::test]
    async fn test_risk_state_tool() {
        let ctx: Arc<dyn TradingContext> = Arc::new(MockTradingContext);
        let tool = RiskStateTool(ctx);
        let output = tool.invoke(serde_json::json!({})).await.unwrap();
        assert!(output.success);
        assert!(output.content.contains("kill_switch_active"));
    }
}
