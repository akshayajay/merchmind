# Analytical methodology

## Revenue and margin

Gross revenue is `quantity × unit price`. A returned line reverses net revenue. Gross profit uses the same sign convention and subtracts product unit cost. This simplified return treatment is appropriate for the demo but a production model should distinguish order, shipment, return initiation, refund, restocking, and write-off events.

## Product velocity

Units per active day normalize sales by the interval between first and last sale. A product is flagged as a slow mover when its velocity is below 55% of the median for its own category. The comparison within category matters: the natural purchase cadence for accessories differs from outerwear.

The flag is a review queue, not an automatic markdown instruction. Inventory on hand, weeks of supply, launch age, campaign exposure, margin, and assortment strategy should be included before taking action.

## Customer RFM

Recency, frequency, and monetary values are ranked into quartiles. Rules then map those values into Champions, Loyal, At Risk, New, and Developing segments. Ranking avoids pretending that one universal dollar threshold works across retailers; production thresholds should be versioned, monitored, and tested for stability.

## Demand forecast

The baseline predicts each future date using mean observed units for the matching weekday over the trailing 84 days. An 80% interval uses 1.28 times weekday-level historical standard deviation and clips negative values to zero.

This baseline is deliberately interpretable and leakage-resistant. The correct next step is rolling-origin backtesting against alternatives, evaluated with weighted absolute percentage error and bias at category and total levels. Promotions, price, holidays, inventory availability, and launch calendars are valuable future covariates.

## Company inventory stress

For each company-quarter, MERCHMIND calculates year-over-year revenue growth, inventory growth, and gross-margin change. The stress score is:

```text
inventory growth YoY − revenue growth YoY − gross-margin change YoY
```

Bands are Low (≤0), Watch (0–8), and Elevated (>8). This is an explainable screening heuristic. It is not causal, does not account for acquisitions or accounting-policy changes, and is not investment advice.

## Data quality

Contract failures are additive: a row can contain more than one rejection reason. Duplicate transaction IDs, required-ID nulls, invalid quantities, invalid prices, invalid timestamps, and broken product/customer references are measured separately. The pipeline passes only valid rows to Silver and publishes both the quarantined rows and aggregate report for auditability.
