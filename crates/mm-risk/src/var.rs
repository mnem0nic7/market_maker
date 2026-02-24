use std::collections::VecDeque;

use parking_lot::Mutex;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::debug;

use mm_core::PortfolioRisk;

// ---------------------------------------------------------------------------
// Z-score lookup
// ---------------------------------------------------------------------------

/// Return the Z-score for a given confidence level.
///
/// Supported levels: 0.90, 0.95, 0.975, 0.99, 0.995, 0.999.
/// Falls back to 1.645 (95%) for unsupported values.
fn z_score(confidence: f64) -> f64 {
    match confidence {
        c if (c - 0.90).abs() < 1e-9 => 1.282,
        c if (c - 0.95).abs() < 1e-9 => 1.645,
        c if (c - 0.975).abs() < 1e-9 => 1.960,
        c if (c - 0.99).abs() < 1e-9 => 2.326,
        c if (c - 0.995).abs() < 1e-9 => 2.576,
        c if (c - 0.999).abs() < 1e-9 => 3.090,
        _ => {
            debug!(
                confidence,
                "unsupported confidence level, defaulting to 95% z-score"
            );
            1.645
        }
    }
}

// ---------------------------------------------------------------------------
// Parametric (Variance-Covariance) VaR
// ---------------------------------------------------------------------------

/// Computes Value-at-Risk using the variance-covariance (parametric) method.
///
/// ```text
/// VaR = |portfolio_value| * volatility * z_score * sqrt(holding_period)
/// ```
///
/// The volatility estimate can be supplied directly or computed from a price
/// series via [`RollingVolatility`].
pub struct ParametricVar {
    /// Annualized volatility estimate (e.g. 0.60 for 60%).
    volatility: Decimal,
}

impl ParametricVar {
    /// Create with a fixed annualized volatility.
    pub fn new(volatility: Decimal) -> Self {
        Self { volatility }
    }

    /// Update the volatility estimate (e.g. from a rolling calculator).
    pub fn set_volatility(&mut self, volatility: Decimal) {
        self.volatility = volatility;
    }

    /// Compute VaR for the given portfolio.
    ///
    /// # Arguments
    /// * `portfolio` - Current portfolio risk snapshot.
    /// * `confidence` - Confidence level (e.g. 0.95, 0.99).
    /// * `holding_period_days` - Holding period in trading days.
    pub fn compute_var(
        &self,
        portfolio: &PortfolioRisk,
        confidence: f64,
        holding_period_days: f64,
    ) -> Decimal {
        let z = z_score(confidence);
        let sqrt_t = holding_period_days.sqrt();

        // Convert f64 intermediaries to Decimal for the final result.
        let z_dec = Decimal::try_from(z).unwrap_or(dec!(1.645));
        let sqrt_t_dec = Decimal::try_from(sqrt_t).unwrap_or(dec!(1));

        let var = portfolio.gross_exposure.abs() * self.volatility * z_dec * sqrt_t_dec;

        debug!(
            gross_exposure = %portfolio.gross_exposure,
            volatility = %self.volatility,
            confidence,
            holding_period_days,
            var = %var,
            "parametric VaR computed"
        );

        var
    }
}

// ---------------------------------------------------------------------------
// Historical Simulation VaR
// ---------------------------------------------------------------------------

/// Computes Value-at-Risk from a rolling window of historical returns using
/// the historical simulation method.
///
/// Returns are sorted and the VaR is taken as the loss at the appropriate
/// percentile.
pub struct HistoricalVar {
    /// Rolling window of returns (most recent at the back).
    returns: Mutex<VecDeque<Decimal>>,
    /// Maximum number of returns to keep.
    window_size: usize,
}

impl HistoricalVar {
    /// Create with a fixed rolling window size.
    pub fn new(window_size: usize) -> Self {
        Self {
            returns: Mutex::new(VecDeque::with_capacity(window_size)),
            window_size,
        }
    }

    /// Add a return observation (e.g. daily return as a decimal fraction).
    pub fn add_return(&self, ret: Decimal) {
        let mut returns = self.returns.lock();
        if returns.len() >= self.window_size {
            returns.pop_front();
        }
        returns.push_back(ret);
    }

    /// Compute VaR at the given confidence level.
    ///
    /// Returns `None` if there are not enough data points (fewer than 2).
    ///
    /// The result is a positive number representing the loss amount at the
    /// given confidence level. For example, a 95% VaR of 0.03 means "there is
    /// a 5% chance of losing more than 3% of portfolio value."
    pub fn compute_var(&self, confidence: f64) -> Option<Decimal> {
        let returns = self.returns.lock();
        let n = returns.len();
        if n < 2 {
            return None;
        }

        // Sort returns ascending (worst losses first).
        let mut sorted: Vec<Decimal> = returns.iter().copied().collect();
        sorted.sort();

        // The VaR index: for 95% confidence with 100 observations, we want
        // the 5th worst return (index 4, i.e. floor(100 * 0.05) - 1 clamped).
        let tail_pct = 1.0 - confidence;
        let idx = ((n as f64) * tail_pct).floor() as usize;
        let idx = idx.min(n - 1);

        // VaR is the absolute value of the loss at this percentile.
        let var = -sorted[idx];

        debug!(
            n,
            confidence,
            percentile_idx = idx,
            var = %var,
            "historical VaR computed"
        );

        Some(var)
    }

    /// Return the number of stored observations.
    pub fn observation_count(&self) -> usize {
        self.returns.lock().len()
    }

    /// Clear all stored returns.
    pub fn clear(&self) {
        self.returns.lock().clear();
    }
}

// ---------------------------------------------------------------------------
// Rolling Volatility
// ---------------------------------------------------------------------------

/// Computes annualized volatility from a rolling window of prices using
/// log-returns and sample standard deviation.
pub struct RollingVolatility {
    /// Rolling window of prices (most recent at the back).
    prices: Mutex<VecDeque<Decimal>>,
    /// Maximum number of prices to keep.
    window_size: usize,
    /// Number of trading periods per year (e.g. 252 for daily, 365 for crypto
    /// 24/7 markets).
    annualization_factor: f64,
}

impl RollingVolatility {
    /// Create with a window size and annualization factor.
    pub fn new(window_size: usize, annualization_factor: f64) -> Self {
        Self {
            prices: Mutex::new(VecDeque::with_capacity(window_size)),
            window_size,
            annualization_factor,
        }
    }

    /// Add a price observation.
    pub fn add_price(&self, price: Decimal) {
        let mut prices = self.prices.lock();
        if prices.len() >= self.window_size {
            prices.pop_front();
        }
        prices.push_back(price);
    }

    /// Compute annualized volatility from the stored price series.
    ///
    /// Returns `None` if there are fewer than 3 prices (need at least 2
    /// returns for a meaningful standard deviation).
    pub fn compute(&self) -> Option<Decimal> {
        let prices = self.prices.lock();
        if prices.len() < 3 {
            return None;
        }

        // Compute log-returns.
        let mut log_returns: Vec<f64> = Vec::with_capacity(prices.len() - 1);
        for i in 1..prices.len() {
            let prev = prices[i - 1];
            let curr = prices[i];
            if prev <= Decimal::ZERO {
                continue;
            }
            // Use f64 for log computation.
            let prev_f: f64 = decimal_to_f64(prev);
            let curr_f: f64 = decimal_to_f64(curr);
            if prev_f > 0.0 {
                log_returns.push((curr_f / prev_f).ln());
            }
        }

        if log_returns.len() < 2 {
            return None;
        }

        let n = log_returns.len() as f64;
        let mean = log_returns.iter().sum::<f64>() / n;
        let variance = log_returns
            .iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>()
            / (n - 1.0);
        let daily_vol = variance.sqrt();
        let annualized_vol = daily_vol * self.annualization_factor.sqrt();

        Decimal::try_from(annualized_vol).ok()
    }

    /// Return the number of stored prices.
    pub fn price_count(&self) -> usize {
        self.prices.lock().len()
    }
}

/// Convert a `Decimal` to `f64`. Uses `to_string` + parse as a fallback-safe
/// approach that avoids precision issues in `TryInto` for extreme values.
fn decimal_to_f64(d: Decimal) -> f64 {
    use std::str::FromStr;
    f64::from_str(&d.to_string()).unwrap_or(0.0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use mm_core::PortfolioRisk;
    use rust_decimal_macros::dec;

    fn empty_portfolio_with_exposure(gross: Decimal) -> PortfolioRisk {
        PortfolioRisk {
            total_var_95: Decimal::ZERO,
            total_var_99: Decimal::ZERO,
            net_exposure: gross,
            gross_exposure: gross,
            max_drawdown: Decimal::ZERO,
            daily_pnl: Decimal::ZERO,
            positions: vec![],
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn parametric_var_basic() {
        let var_calc = ParametricVar::new(dec!(0.60)); // 60% annualized vol
        let portfolio = empty_portfolio_with_exposure(dec!(100_000));

        let var_95 = var_calc.compute_var(&portfolio, 0.95, 1.0);
        // Expected: 100_000 * 0.60 * 1.645 * 1.0 = 98_700
        assert!(var_95 > dec!(98_000));
        assert!(var_95 < dec!(100_000));

        let var_99 = var_calc.compute_var(&portfolio, 0.99, 1.0);
        // 99% should be larger than 95%
        assert!(var_99 > var_95);
    }

    #[test]
    fn parametric_var_multi_day_holding() {
        let var_calc = ParametricVar::new(dec!(0.60));
        let portfolio = empty_portfolio_with_exposure(dec!(100_000));

        let var_1d = var_calc.compute_var(&portfolio, 0.95, 1.0);
        let var_10d = var_calc.compute_var(&portfolio, 0.95, 10.0);

        // 10-day VaR should be ~sqrt(10) times 1-day VaR.
        let ratio = decimal_to_f64(var_10d) / decimal_to_f64(var_1d);
        let expected_ratio = 10.0_f64.sqrt();
        assert!((ratio - expected_ratio).abs() < 0.01);
    }

    #[test]
    fn historical_var_basic() {
        let hist = HistoricalVar::new(100);

        // Add 100 returns: -0.05, -0.04, ..., 0.04 (uniformly spaced).
        for i in 0..100 {
            let ret = Decimal::from(-50 + i as i64) / Decimal::from(1000);
            hist.add_return(ret);
        }

        let var_95 = hist.compute_var(0.95).unwrap();
        // At 95%, we expect the 5th percentile loss ~ 0.045.
        assert!(var_95 > dec!(0.04));
        assert!(var_95 < dec!(0.06));
    }

    #[test]
    fn historical_var_returns_none_with_insufficient_data() {
        let hist = HistoricalVar::new(100);
        assert!(hist.compute_var(0.95).is_none());

        hist.add_return(dec!(-0.01));
        assert!(hist.compute_var(0.95).is_none());
    }

    #[test]
    fn historical_var_respects_window_size() {
        let hist = HistoricalVar::new(5);
        for i in 0..10 {
            hist.add_return(Decimal::from(i));
        }
        assert_eq!(hist.observation_count(), 5);
    }

    #[test]
    fn rolling_volatility_basic() {
        let vol = RollingVolatility::new(30, 252.0);

        // Generate a simple price series with known characteristics.
        let base_price = 100.0_f64;
        for i in 0..20 {
            let price = base_price + (i as f64) * 0.5; // trending up
            vol.add_price(Decimal::try_from(price).unwrap());
        }

        let result = vol.compute();
        assert!(result.is_some());
        let annualized = result.unwrap();
        // Should be a positive, non-zero value.
        assert!(annualized > Decimal::ZERO);
    }

    #[test]
    fn rolling_volatility_insufficient_data() {
        let vol = RollingVolatility::new(30, 252.0);
        vol.add_price(dec!(100));
        vol.add_price(dec!(101));
        assert!(vol.compute().is_none());
    }

    #[test]
    fn z_score_lookup() {
        assert!((z_score(0.95) - 1.645).abs() < 1e-6);
        assert!((z_score(0.99) - 2.326).abs() < 1e-6);
        assert!((z_score(0.90) - 1.282).abs() < 1e-6);
    }
}
