# Analytical methodology

## Revenue and margin

Gross revenue is `quantity × unit price`. A returned line reverses net revenue. Gross profit uses the same sign convention and subtracts product unit cost. This simplified return treatment is appropriate for the demo but a production model should distinguish order, shipment, return initiation, refund, restocking, and write-off events.

Unit prices already include the recorded discount; do not apply `discount_pct` again.
Daily reconciliation uses decimal arithmetic for revenue and checks Gold floating-point
aggregates to within half a cent. Units include quantities on both sale and return lines;
the stock calculation separately reverses the returned units.

## Product velocity

Units per active day normalize sales by the interval between first and last sale. A product is flagged as a slow mover when its velocity is below 55% of the median for its own category. The comparison within category matters: the natural purchase cadence for accessories differs from outerwear.

The flag is a review queue, not an automatic markdown instruction. Inventory on hand, weeks of supply, launch age, campaign exposure, margin, and assortment strategy should be included before taking action.

## Customer RFM

Recency, frequency, and monetary values are ranked into quartiles. Rules then map those values into Champions, Loyal, At Risk, New, and Developing segments. Ranking avoids pretending that one universal dollar threshold works across retailers; production thresholds should be versioned, monitored, and tested for stability.

## Demand forecast

The baseline predicts each future date using mean observed units for the matching weekday over the trailing 84 days. A nominal, uncalibrated 80% band uses 1.28 times weekday-level historical standard deviation and clips negative values to zero.

Rolling-origin backtesting now compares this baseline with last-week demand and a trailing 28-day mean. Each fold uses only earlier history, followed by a disjoint 28-day holdout; missing category-days are zero demand. Reports contain per-category and pooled category-day MAE, RMSE, WAPE, signed bias, and band coverage. The pooled score is not an error on summed total demand. See [measured results](validation.md); the weekday model did not beat the 28-day mean on the synthetic dataset. Promotions, price, holidays, inventory availability, and launch calendars are valuable future covariates.

## Company inventory stress

For each company-quarter, MERCHMIND calculates year-over-year revenue growth, inventory growth, and gross-margin change. The stress score is:

```text
inventory growth YoY − revenue growth YoY − gross-margin change YoY
```

Bands are Low (≤0), Watch (0–8), and Elevated (>8). This is an explainable screening heuristic. It is not causal, does not account for acquisitions or accounting-policy changes, and is not investment advice.

## Data quality

Contract failures are additive: a row can contain more than one rejection reason. Duplicate transaction IDs, required-ID nulls, invalid quantities, invalid prices, invalid timestamps, and broken product/customer references are measured separately. The pipeline passes only valid rows to Silver and publishes both the quarantined rows and aggregate report for auditability.

Validation checks contracts before reserving a transaction ID, so a bad first arrival
does not suppress a later valid record. Replayed valid IDs do not add serving revenue.
For the daily close, the default 5% invalid-rate gate covers the whole pinned source
history, not just the requested date. Duplicate-only rejections are counted separately;
malformed JSON counts as invalid. A successful reconciliation establishes agreement
between the pinned sources and serving records, not completeness of events still queued
upstream or future arrivals.

## Product inventory

On-hand stock is opening balance + receipts + signed adjustments − sold units +
restockable returned units. Count only transactions at or after the opening timestamp.
The synthetic demo assumes every return is restockable, seeds 500 opening units per
product, and flags balances at or below 20. Negative balances are marked oversold.
Products with no inventory events have unknown stock. This product-level calculation
is separate from the quarterly company inventory-stress heuristic.

## Daily closes and historical forecasts

The Airflow timetable explicitly defines UTC daily intervals. Midnight closes the
preceding date; historical reruns use all committed arrivals known at execution time.
They do not reconstruct what was known at the historical processing date. A pinned
snapshot isolates reconciliation from concurrent new stream batches, and failed tasks
leave the last successful daily report available.

Close forecasts use only event dates through that close. Their artifacts are retained
with the attempt; the live dashboard forecast continues to use the live analytical
snapshot. Refreshing a forecast does not establish improved predictive accuracy.
