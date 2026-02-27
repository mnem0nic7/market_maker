use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use super::client::FinancialDataClient;
use crate::tools::registry::ToolRegistry;
use crate::tools::{Tool, ToolOutput, ToolSchema};

// ---------------------------------------------------------------------------
// Helper: extract a string param with a default
// ---------------------------------------------------------------------------

fn param_str<'a>(params: &'a Value, key: &str, default: &'a str) -> &'a str {
    params.get(key).and_then(|v| v.as_str()).unwrap_or(default)
}

fn param_u32(params: &Value, key: &str, default: u32) -> u32 {
    params
        .get(key)
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .unwrap_or(default)
}

// ---------------------------------------------------------------------------
// Tool: get_income_statements
// ---------------------------------------------------------------------------

struct IncomeStatementsTool(Arc<FinancialDataClient>);

#[async_trait]
impl Tool for IncomeStatementsTool {
    fn name(&self) -> &str {
        "get_income_statements"
    }
    fn description(&self) -> &str {
        "Fetches a company's income statements, detailing revenues, expenses, and net income over a reporting period."
    }
    fn parameters_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "object".into(),
            properties: serde_json::json!({
                "ticker": {"type": "string", "description": "Stock ticker symbol (e.g., 'AAPL')"},
                "period": {"type": "string", "enum": ["annual", "quarterly", "ttm"], "description": "Reporting period"},
                "limit": {"type": "integer", "description": "Number of statements to return (default: 4)"}
            }),
            required: vec!["ticker".into()],
        }
    }
    async fn invoke(&self, params: Value) -> anyhow::Result<ToolOutput> {
        let ticker = param_str(&params, "ticker", "");
        if ticker.is_empty() {
            return Ok(ToolOutput::error(
                self.name(),
                "Missing required parameter: ticker",
            ));
        }
        let period = param_str(&params, "period", "annual");
        let limit = param_u32(&params, "limit", 4);
        let data = self.0.income_statements(ticker, period, limit).await?;
        Ok(ToolOutput::success(
            self.name(),
            serde_json::to_string(&data)?,
        ))
    }
}

// ---------------------------------------------------------------------------
// Tool: get_balance_sheets
// ---------------------------------------------------------------------------

struct BalanceSheetsTool(Arc<FinancialDataClient>);

#[async_trait]
impl Tool for BalanceSheetsTool {
    fn name(&self) -> &str {
        "get_balance_sheets"
    }
    fn description(&self) -> &str {
        "Retrieves a company's balance sheets, showing assets, liabilities, and shareholders' equity."
    }
    fn parameters_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "object".into(),
            properties: serde_json::json!({
                "ticker": {"type": "string", "description": "Stock ticker symbol"},
                "period": {"type": "string", "enum": ["annual", "quarterly", "ttm"], "description": "Reporting period"},
                "limit": {"type": "integer", "description": "Number of statements (default: 4)"}
            }),
            required: vec!["ticker".into()],
        }
    }
    async fn invoke(&self, params: Value) -> anyhow::Result<ToolOutput> {
        let ticker = param_str(&params, "ticker", "");
        if ticker.is_empty() {
            return Ok(ToolOutput::error(
                self.name(),
                "Missing required parameter: ticker",
            ));
        }
        let data = self
            .0
            .balance_sheets(
                ticker,
                param_str(&params, "period", "annual"),
                param_u32(&params, "limit", 4),
            )
            .await?;
        Ok(ToolOutput::success(
            self.name(),
            serde_json::to_string(&data)?,
        ))
    }
}

// ---------------------------------------------------------------------------
// Tool: get_cash_flow_statements
// ---------------------------------------------------------------------------

struct CashFlowStatementsTool(Arc<FinancialDataClient>);

#[async_trait]
impl Tool for CashFlowStatementsTool {
    fn name(&self) -> &str {
        "get_cash_flow_statements"
    }
    fn description(&self) -> &str {
        "Retrieves cash flow statements, showing cash generated and used across operating, investing, and financing activities."
    }
    fn parameters_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "object".into(),
            properties: serde_json::json!({
                "ticker": {"type": "string", "description": "Stock ticker symbol"},
                "period": {"type": "string", "enum": ["annual", "quarterly", "ttm"], "description": "Reporting period"},
                "limit": {"type": "integer", "description": "Number of statements (default: 4)"}
            }),
            required: vec!["ticker".into()],
        }
    }
    async fn invoke(&self, params: Value) -> anyhow::Result<ToolOutput> {
        let ticker = param_str(&params, "ticker", "");
        if ticker.is_empty() {
            return Ok(ToolOutput::error(
                self.name(),
                "Missing required parameter: ticker",
            ));
        }
        let data = self
            .0
            .cash_flow_statements(
                ticker,
                param_str(&params, "period", "annual"),
                param_u32(&params, "limit", 4),
            )
            .await?;
        Ok(ToolOutput::success(
            self.name(),
            serde_json::to_string(&data)?,
        ))
    }
}

// ---------------------------------------------------------------------------
// Tool: get_key_ratios
// ---------------------------------------------------------------------------

struct KeyRatiosTool(Arc<FinancialDataClient>);

#[async_trait]
impl Tool for KeyRatiosTool {
    fn name(&self) -> &str {
        "get_key_ratios"
    }
    fn description(&self) -> &str {
        "Retrieves key financial ratios and metrics including P/E, EV/EBITDA, ROE, revenue per share, and enterprise value for trend analysis."
    }
    fn parameters_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "object".into(),
            properties: serde_json::json!({
                "ticker": {"type": "string", "description": "Stock ticker symbol"},
                "period": {"type": "string", "enum": ["annual", "quarterly", "ttm"], "description": "Reporting period (default: ttm)"},
                "limit": {"type": "integer", "description": "Number of periods (default: 4)"}
            }),
            required: vec!["ticker".into()],
        }
    }
    async fn invoke(&self, params: Value) -> anyhow::Result<ToolOutput> {
        let ticker = param_str(&params, "ticker", "");
        if ticker.is_empty() {
            return Ok(ToolOutput::error(
                self.name(),
                "Missing required parameter: ticker",
            ));
        }
        let data = self
            .0
            .financial_metrics(
                ticker,
                param_str(&params, "period", "ttm"),
                param_u32(&params, "limit", 4),
            )
            .await?;
        Ok(ToolOutput::success(
            self.name(),
            serde_json::to_string(&data)?,
        ))
    }
}

// ---------------------------------------------------------------------------
// Tool: get_filings
// ---------------------------------------------------------------------------

struct FilingsTool(Arc<FinancialDataClient>);

#[async_trait]
impl Tool for FilingsTool {
    fn name(&self) -> &str {
        "get_filings"
    }
    fn description(&self) -> &str {
        "Retrieves SEC filings (10-K, 10-Q, 8-K) for a company, including filing dates and accession numbers."
    }
    fn parameters_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "object".into(),
            properties: serde_json::json!({
                "ticker": {"type": "string", "description": "Stock ticker symbol"},
                "filing_type": {"type": "string", "enum": ["10-K", "10-Q", "8-K"], "description": "Type of filing to filter by"},
                "limit": {"type": "integer", "description": "Maximum filings to return (default: 10)"}
            }),
            required: vec!["ticker".into()],
        }
    }
    async fn invoke(&self, params: Value) -> anyhow::Result<ToolOutput> {
        let ticker = param_str(&params, "ticker", "");
        if ticker.is_empty() {
            return Ok(ToolOutput::error(
                self.name(),
                "Missing required parameter: ticker",
            ));
        }
        let filing_type = params.get("filing_type").and_then(|v| v.as_str());
        let data = self
            .0
            .filings(ticker, filing_type, param_u32(&params, "limit", 10))
            .await?;
        Ok(ToolOutput::success(
            self.name(),
            serde_json::to_string(&data)?,
        ))
    }
}

// ---------------------------------------------------------------------------
// Tool: get_insider_trades
// ---------------------------------------------------------------------------

struct InsiderTradesTool(Arc<FinancialDataClient>);

#[async_trait]
impl Tool for InsiderTradesTool {
    fn name(&self) -> &str {
        "get_insider_trades"
    }
    fn description(&self) -> &str {
        "Retrieves insider trading transactions (SEC Form 4) for a company, showing buys/sells by officers and directors."
    }
    fn parameters_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "object".into(),
            properties: serde_json::json!({
                "ticker": {"type": "string", "description": "Stock ticker symbol"},
                "limit": {"type": "integer", "description": "Maximum trades to return (default: 10)"}
            }),
            required: vec!["ticker".into()],
        }
    }
    async fn invoke(&self, params: Value) -> anyhow::Result<ToolOutput> {
        let ticker = param_str(&params, "ticker", "");
        if ticker.is_empty() {
            return Ok(ToolOutput::error(
                self.name(),
                "Missing required parameter: ticker",
            ));
        }
        let data = self
            .0
            .insider_trades(ticker, param_u32(&params, "limit", 10))
            .await?;
        Ok(ToolOutput::success(
            self.name(),
            serde_json::to_string(&data)?,
        ))
    }
}

// ---------------------------------------------------------------------------
// Tool: get_company_news
// ---------------------------------------------------------------------------

struct NewsTool(Arc<FinancialDataClient>);

#[async_trait]
impl Tool for NewsTool {
    fn name(&self) -> &str {
        "get_company_news"
    }
    fn description(&self) -> &str {
        "Retrieves recent news articles and headlines for a company, including title, source, date, and URL."
    }
    fn parameters_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "object".into(),
            properties: serde_json::json!({
                "ticker": {"type": "string", "description": "Stock ticker symbol"},
                "limit": {"type": "integer", "description": "Number of articles (default: 5, max: 10)"}
            }),
            required: vec!["ticker".into()],
        }
    }
    async fn invoke(&self, params: Value) -> anyhow::Result<ToolOutput> {
        let ticker = param_str(&params, "ticker", "");
        if ticker.is_empty() {
            return Ok(ToolOutput::error(
                self.name(),
                "Missing required parameter: ticker",
            ));
        }
        let limit = param_u32(&params, "limit", 5).min(10);
        let data = self.0.news(ticker, limit).await?;
        Ok(ToolOutput::success(
            self.name(),
            serde_json::to_string(&data)?,
        ))
    }
}

// ---------------------------------------------------------------------------
// Tool: get_analyst_estimates
// ---------------------------------------------------------------------------

struct AnalystEstimatesTool(Arc<FinancialDataClient>);

#[async_trait]
impl Tool for AnalystEstimatesTool {
    fn name(&self) -> &str {
        "get_analyst_estimates"
    }
    fn description(&self) -> &str {
        "Retrieves analyst estimates including EPS forecasts, revenue estimates, and consensus recommendations."
    }
    fn parameters_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "object".into(),
            properties: serde_json::json!({
                "ticker": {"type": "string", "description": "Stock ticker symbol"},
                "period": {"type": "string", "enum": ["annual", "quarterly"], "description": "Estimate period (default: annual)"}
            }),
            required: vec!["ticker".into()],
        }
    }
    async fn invoke(&self, params: Value) -> anyhow::Result<ToolOutput> {
        let ticker = param_str(&params, "ticker", "");
        if ticker.is_empty() {
            return Ok(ToolOutput::error(
                self.name(),
                "Missing required parameter: ticker",
            ));
        }
        let data = self
            .0
            .analyst_estimates(ticker, param_str(&params, "period", "annual"))
            .await?;
        Ok(ToolOutput::success(
            self.name(),
            serde_json::to_string(&data)?,
        ))
    }
}

// ---------------------------------------------------------------------------
// Tool: get_segmented_revenues
// ---------------------------------------------------------------------------

struct SegmentedRevenuesTool(Arc<FinancialDataClient>);

#[async_trait]
impl Tool for SegmentedRevenuesTool {
    fn name(&self) -> &str {
        "get_segmented_revenues"
    }
    fn description(&self) -> &str {
        "Retrieves revenue breakdown by business segment (products, services, geographic regions) for analyzing revenue composition."
    }
    fn parameters_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "object".into(),
            properties: serde_json::json!({
                "ticker": {"type": "string", "description": "Stock ticker symbol"},
                "period": {"type": "string", "enum": ["annual", "quarterly"], "description": "Reporting period"},
                "limit": {"type": "integer", "description": "Number of periods (default: 4)"}
            }),
            required: vec!["ticker".into()],
        }
    }
    async fn invoke(&self, params: Value) -> anyhow::Result<ToolOutput> {
        let ticker = param_str(&params, "ticker", "");
        if ticker.is_empty() {
            return Ok(ToolOutput::error(
                self.name(),
                "Missing required parameter: ticker",
            ));
        }
        let data = self
            .0
            .segmented_revenues(
                ticker,
                param_str(&params, "period", "annual"),
                param_u32(&params, "limit", 4),
            )
            .await?;
        Ok(ToolOutput::success(
            self.name(),
            serde_json::to_string(&data)?,
        ))
    }
}

// ---------------------------------------------------------------------------
// Tool: get_crypto_price_snapshot
// ---------------------------------------------------------------------------

struct CryptoSnapshotTool(Arc<FinancialDataClient>);

#[async_trait]
impl Tool for CryptoSnapshotTool {
    fn name(&self) -> &str {
        "get_crypto_price_snapshot"
    }
    fn description(&self) -> &str {
        "Retrieves the latest price snapshot for a cryptocurrency including price, volume, and OHLC data."
    }
    fn parameters_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "object".into(),
            properties: serde_json::json!({
                "ticker": {"type": "string", "description": "Crypto ticker (e.g., 'BTC-USD')"}
            }),
            required: vec!["ticker".into()],
        }
    }
    async fn invoke(&self, params: Value) -> anyhow::Result<ToolOutput> {
        let ticker = param_str(&params, "ticker", "");
        if ticker.is_empty() {
            return Ok(ToolOutput::error(
                self.name(),
                "Missing required parameter: ticker",
            ));
        }
        let data = self.0.crypto_snapshot(ticker).await?;
        Ok(ToolOutput::success(
            self.name(),
            serde_json::to_string(&data)?,
        ))
    }
}

// ---------------------------------------------------------------------------
// Tool: get_crypto_prices
// ---------------------------------------------------------------------------

struct CryptoPricesTool(Arc<FinancialDataClient>);

#[async_trait]
impl Tool for CryptoPricesTool {
    fn name(&self) -> &str {
        "get_crypto_prices"
    }
    fn description(&self) -> &str {
        "Retrieves historical cryptocurrency price data (OHLCV) over a date range."
    }
    fn parameters_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "object".into(),
            properties: serde_json::json!({
                "ticker": {"type": "string", "description": "Crypto ticker (e.g., 'BTC-USD')"},
                "interval": {"type": "string", "enum": ["minute", "day", "week", "month", "year"], "description": "Price interval (default: day)"},
                "start_date": {"type": "string", "description": "Start date (YYYY-MM-DD)"},
                "end_date": {"type": "string", "description": "End date (YYYY-MM-DD)"}
            }),
            required: vec!["ticker".into(), "start_date".into(), "end_date".into()],
        }
    }
    async fn invoke(&self, params: Value) -> anyhow::Result<ToolOutput> {
        let ticker = param_str(&params, "ticker", "");
        let start = param_str(&params, "start_date", "");
        let end = param_str(&params, "end_date", "");
        if ticker.is_empty() || start.is_empty() || end.is_empty() {
            return Ok(ToolOutput::error(
                self.name(),
                "Missing required parameters: ticker, start_date, end_date",
            ));
        }
        let data = self
            .0
            .crypto_prices(ticker, param_str(&params, "interval", "day"), start, end)
            .await?;
        Ok(ToolOutput::success(
            self.name(),
            serde_json::to_string(&data)?,
        ))
    }
}

// ---------------------------------------------------------------------------
// Tool: get_stock_prices
// ---------------------------------------------------------------------------

struct StockPricesTool(Arc<FinancialDataClient>);

#[async_trait]
impl Tool for StockPricesTool {
    fn name(&self) -> &str {
        "get_stock_prices"
    }
    fn description(&self) -> &str {
        "Retrieves historical stock price data (OHLCV) for a given ticker and date range."
    }
    fn parameters_schema(&self) -> ToolSchema {
        ToolSchema {
            schema_type: "object".into(),
            properties: serde_json::json!({
                "ticker": {"type": "string", "description": "Stock ticker symbol"},
                "interval": {"type": "string", "enum": ["minute", "day", "week", "month", "year"], "description": "Price interval (default: day)"},
                "start_date": {"type": "string", "description": "Start date (YYYY-MM-DD)"},
                "end_date": {"type": "string", "description": "End date (YYYY-MM-DD)"}
            }),
            required: vec!["ticker".into(), "start_date".into(), "end_date".into()],
        }
    }
    async fn invoke(&self, params: Value) -> anyhow::Result<ToolOutput> {
        let ticker = param_str(&params, "ticker", "");
        let start = param_str(&params, "start_date", "");
        let end = param_str(&params, "end_date", "");
        if ticker.is_empty() || start.is_empty() || end.is_empty() {
            return Ok(ToolOutput::error(
                self.name(),
                "Missing required parameters: ticker, start_date, end_date",
            ));
        }
        let data = self
            .0
            .stock_prices(ticker, param_str(&params, "interval", "day"), start, end)
            .await?;
        Ok(ToolOutput::success(
            self.name(),
            serde_json::to_string(&data)?,
        ))
    }
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/// Register all financial data tools into the given registry.
pub fn register_finance_tools(registry: &mut ToolRegistry, client: Arc<FinancialDataClient>) {
    registry.register(Arc::new(IncomeStatementsTool(Arc::clone(&client))));
    registry.register(Arc::new(BalanceSheetsTool(Arc::clone(&client))));
    registry.register(Arc::new(CashFlowStatementsTool(Arc::clone(&client))));
    registry.register(Arc::new(KeyRatiosTool(Arc::clone(&client))));
    registry.register(Arc::new(FilingsTool(Arc::clone(&client))));
    registry.register(Arc::new(InsiderTradesTool(Arc::clone(&client))));
    registry.register(Arc::new(NewsTool(Arc::clone(&client))));
    registry.register(Arc::new(AnalystEstimatesTool(Arc::clone(&client))));
    registry.register(Arc::new(SegmentedRevenuesTool(Arc::clone(&client))));
    registry.register(Arc::new(CryptoSnapshotTool(Arc::clone(&client))));
    registry.register(Arc::new(CryptoPricesTool(Arc::clone(&client))));
    registry.register(Arc::new(StockPricesTool(client)));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_all_finance_tools() {
        let client = Arc::new(FinancialDataClient::new("test-key".into()));
        let mut registry = ToolRegistry::new();
        register_finance_tools(&mut registry, client);

        assert_eq!(registry.len(), 12);

        // Verify all expected tools are registered
        let expected = [
            "get_income_statements",
            "get_balance_sheets",
            "get_cash_flow_statements",
            "get_key_ratios",
            "get_filings",
            "get_insider_trades",
            "get_company_news",
            "get_analyst_estimates",
            "get_segmented_revenues",
            "get_crypto_price_snapshot",
            "get_crypto_prices",
            "get_stock_prices",
        ];

        for name in &expected {
            assert!(registry.get(name).is_some(), "Missing tool: {name}");
        }
    }

    #[test]
    fn test_tool_schemas_valid() {
        let client = Arc::new(FinancialDataClient::new("test-key".into()));
        let mut registry = ToolRegistry::new();
        register_finance_tools(&mut registry, client);

        let defs = registry.as_tool_definitions();
        for def in &defs {
            assert!(!def.name.is_empty());
            assert!(!def.description.is_empty());
            assert_eq!(def.input_schema.schema_type, "object");
            // properties should be a JSON object
            assert!(def.input_schema.properties.is_object());
        }
    }

    #[tokio::test]
    async fn test_missing_ticker_returns_error() {
        let client = Arc::new(FinancialDataClient::new("test-key".into()));
        let tool = IncomeStatementsTool(client);
        let output = tool.invoke(serde_json::json!({})).await.unwrap();
        assert!(!output.success);
        assert!(output.error.unwrap().contains("ticker"));
    }

    #[test]
    fn test_param_helpers() {
        let params = serde_json::json!({"ticker": "AAPL", "limit": 10});
        assert_eq!(param_str(&params, "ticker", ""), "AAPL");
        assert_eq!(param_str(&params, "missing", "default"), "default");
        assert_eq!(param_u32(&params, "limit", 0), 10);
        assert_eq!(param_u32(&params, "missing", 5), 5);
    }
}
