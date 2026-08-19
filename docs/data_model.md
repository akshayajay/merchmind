# Data model

MERCHMIND uses a compact dimensional model. Source-shaped Bronze objects are immutable; rejected records are preserved separately; Silver holds trusted facts and dimensions; Gold holds purpose-built decision tables.

## Core model

```mermaid
erDiagram
    DIM_CUSTOMERS ||--o{ FACT_TRANSACTIONS : places
    DIM_PRODUCTS ||--o{ FACT_TRANSACTIONS : contains
    FACT_TRANSACTIONS {
        string transaction_id PK
        timestamp transaction_ts
        string customer_id FK
        string product_id FK
        int quantity
        decimal unit_price
        decimal discount_pct
        string sales_channel
        boolean returned
    }
    DIM_PRODUCTS {
        string product_id PK
        string product_name
        string category
        string department
        string color
        string aesthetic
        string season
        decimal list_price
        decimal unit_cost
        date launch_date
    }
    DIM_CUSTOMERS {
        string customer_id PK
        string age_band
        string region
        string acquisition_channel
        date joined_date
    }
```

## Grain and invariants

`fact_transactions` has one row per transaction line. Transaction ID is unique in this demo. Quantity must be positive, price nonnegative, event time parseable, required IDs present, and product/customer references valid. Because the model includes returns on the same grain, net revenue and gross profit reverse when `returned=true`.

The synthetic customer table deliberately uses only broad age bands and regions. It does not create names, email addresses, street addresses, or other direct identifiers.

## Gold tables

- `daily_category_performance`: one row per date and category.
- `product_performance`: one row per product across the observed period.
- `customer_rfm`: one row per purchasing customer at the pipeline reference date.
- `market_pulse`: one row per company-quarter, enriched with the latest available monthly macro observation.
- `category_forecast`: one row per future date and category for a 28-day horizon.

The Gold tables are denormalized intentionally. They optimize stable API and dashboard reads while the conformed Silver layer remains the reusable source of truth.
