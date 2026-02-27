---
name: dcf_valuation
description: Run a discounted cash flow (DCF) analysis to estimate intrinsic value
---

# DCF Valuation Workflow

## Step 1: Gather Historical Data

Use financial data tools to collect:
- **Free Cash Flow (FCF)**: Use `get_cash_flow_statements` for the last 5 years
- **Revenue Growth**: Use `get_income_statements` for revenue trends
- **Margins**: Use `get_key_ratios` for operating and net margins
- **Capital Structure**: Use `get_balance_sheets` for debt and equity

## Step 2: Estimate Growth Rate

1. Calculate historical revenue CAGR (3-year and 5-year)
2. Compare with analyst estimates using `get_analyst_estimates`
3. Consider segment trends using `get_segmented_revenues`
4. Use the lower of: historical growth, analyst consensus, or industry average

## Step 3: Project Future Cash Flows

- Project FCF for 5-10 years using estimated growth rate
- Apply a fade rate: growth should converge toward GDP growth (2-3%) over time
- Account for margin expansion/compression based on historical trends

## Step 4: Calculate WACC (Weighted Average Cost of Capital)

- **Cost of Equity**: Use CAPM (Risk-free rate + Beta × Equity Risk Premium)
  - Risk-free rate: Use 10-year Treasury yield (~4-5%)
  - Beta: From `get_key_ratios`
  - Equity Risk Premium: 5-6%
- **Cost of Debt**: From interest expense / total debt
- **WACC** = (E/V × Re) + (D/V × Rd × (1-T))

## Step 5: Calculate Terminal Value

Use **Gordon Growth Model**:
- Terminal Value = FCF_final × (1 + g) / (WACC - g)
- Where g = long-term growth rate (2-3%)

## Step 6: Discount to Present Value

- Discount all projected FCFs and terminal value back to present
- PV = FCF_t / (1 + WACC)^t
- Enterprise Value = Sum of all PVs

## Step 7: Calculate Equity Value Per Share

1. Enterprise Value
2. Subtract: Net Debt (total debt - cash)
3. Divide by: Shares Outstanding
4. = **Intrinsic Value Per Share**

## Step 8: Sensitivity Analysis

Create a matrix varying:
- WACC: ±1% in 0.5% increments
- Terminal growth rate: 1.5% to 3.5%
- Show the range of intrinsic values

## Output Format

Present results as:
1. Key assumptions table
2. DCF model summary (projected FCFs, terminal value, enterprise value)
3. Intrinsic value per share vs. current market price
4. Sensitivity matrix
5. Bull/Base/Bear case scenarios
