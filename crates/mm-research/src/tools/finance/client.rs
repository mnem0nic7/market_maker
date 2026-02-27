use std::time::Duration;

use anyhow::Context;
use serde_json::Value;

const BASE_URL: &str = "https://api.financialdatasets.ai";

/// HTTP client for the financialdatasets.ai API.
#[derive(Clone)]
pub struct FinancialDataClient {
    http: reqwest::Client,
    api_key: String,
}

impl FinancialDataClient {
    pub fn new(api_key: String) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        Self { http, api_key }
    }

    /// Generic GET request to the financial data API.
    async fn get(&self, path: &str, params: &[(&str, &str)]) -> anyhow::Result<Value> {
        let url = format!("{BASE_URL}{path}");
        let response = self
            .http
            .get(&url)
            .header("X-API-Key", &self.api_key)
            .query(params)
            .send()
            .await
            .with_context(|| format!("Failed to call {path}"))?;

        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("Financial data API error {status}: {text}");
        }

        response
            .json()
            .await
            .with_context(|| format!("Failed to parse response from {path}"))
    }

    pub async fn income_statements(
        &self,
        ticker: &str,
        period: &str,
        limit: u32,
    ) -> anyhow::Result<Value> {
        self.get(
            "/financials/income-statements/",
            &[
                ("ticker", ticker),
                ("period", period),
                ("limit", &limit.to_string()),
            ],
        )
        .await
    }

    pub async fn balance_sheets(
        &self,
        ticker: &str,
        period: &str,
        limit: u32,
    ) -> anyhow::Result<Value> {
        self.get(
            "/financials/balance-sheets/",
            &[
                ("ticker", ticker),
                ("period", period),
                ("limit", &limit.to_string()),
            ],
        )
        .await
    }

    pub async fn cash_flow_statements(
        &self,
        ticker: &str,
        period: &str,
        limit: u32,
    ) -> anyhow::Result<Value> {
        self.get(
            "/financials/cash-flow-statements/",
            &[
                ("ticker", ticker),
                ("period", period),
                ("limit", &limit.to_string()),
            ],
        )
        .await
    }

    pub async fn financial_metrics(
        &self,
        ticker: &str,
        period: &str,
        limit: u32,
    ) -> anyhow::Result<Value> {
        self.get(
            "/financial-metrics/",
            &[
                ("ticker", ticker),
                ("period", period),
                ("limit", &limit.to_string()),
            ],
        )
        .await
    }

    pub async fn filings(
        &self,
        ticker: &str,
        filing_type: Option<&str>,
        limit: u32,
    ) -> anyhow::Result<Value> {
        let limit_str = limit.to_string();
        let mut params: Vec<(&str, &str)> = vec![("ticker", ticker), ("limit", &limit_str)];
        if let Some(ft) = filing_type {
            params.push(("filing_type", ft));
        }
        self.get("/filings/", &params).await
    }

    pub async fn insider_trades(&self, ticker: &str, limit: u32) -> anyhow::Result<Value> {
        let limit_str = limit.to_string();
        self.get(
            "/insider-trades/",
            &[("ticker", ticker), ("limit", &limit_str)],
        )
        .await
    }

    pub async fn news(&self, ticker: &str, limit: u32) -> anyhow::Result<Value> {
        let limit_str = limit.to_string();
        self.get("/news", &[("ticker", ticker), ("limit", &limit_str)])
            .await
    }

    pub async fn analyst_estimates(&self, ticker: &str, period: &str) -> anyhow::Result<Value> {
        self.get(
            "/analyst-estimates/",
            &[("ticker", ticker), ("period", period)],
        )
        .await
    }

    pub async fn segmented_revenues(
        &self,
        ticker: &str,
        period: &str,
        limit: u32,
    ) -> anyhow::Result<Value> {
        let limit_str = limit.to_string();
        self.get(
            "/financials/segmented-revenues/",
            &[
                ("ticker", ticker),
                ("period", period),
                ("limit", &limit_str),
            ],
        )
        .await
    }

    pub async fn crypto_snapshot(&self, ticker: &str) -> anyhow::Result<Value> {
        self.get("/crypto/prices/snapshot/", &[("ticker", ticker)])
            .await
    }

    pub async fn crypto_prices(
        &self,
        ticker: &str,
        interval: &str,
        start_date: &str,
        end_date: &str,
    ) -> anyhow::Result<Value> {
        self.get(
            "/crypto/prices/",
            &[
                ("ticker", ticker),
                ("interval", interval),
                ("start_date", start_date),
                ("end_date", end_date),
            ],
        )
        .await
    }

    pub async fn crypto_tickers(&self) -> anyhow::Result<Value> {
        self.get("/crypto/prices/tickers/", &[]).await
    }

    pub async fn stock_prices(
        &self,
        ticker: &str,
        interval: &str,
        start_date: &str,
        end_date: &str,
    ) -> anyhow::Result<Value> {
        let limit_str = "5000";
        self.get(
            "/prices/",
            &[
                ("ticker", ticker),
                ("interval", interval),
                ("start_date", start_date),
                ("end_date", end_date),
                ("limit", limit_str),
            ],
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_construction() {
        let client = FinancialDataClient::new("test-key".into());
        // Just verify it doesn't panic
        assert!(!client.api_key.is_empty());
    }

    #[test]
    fn test_base_url() {
        assert_eq!(BASE_URL, "https://api.financialdatasets.ai");
    }
}
