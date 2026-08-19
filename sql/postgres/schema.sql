CREATE TABLE IF NOT EXISTS dim_products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    department TEXT NOT NULL,
    color TEXT NOT NULL,
    aesthetic TEXT NOT NULL,
    season TEXT NOT NULL,
    list_price NUMERIC(12, 2) NOT NULL CHECK (list_price >= 0),
    unit_cost NUMERIC(12, 2) NOT NULL CHECK (unit_cost >= 0),
    launch_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id TEXT PRIMARY KEY,
    age_band TEXT NOT NULL,
    region TEXT NOT NULL,
    acquisition_channel TEXT NOT NULL,
    joined_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id TEXT PRIMARY KEY,
    transaction_ts TIMESTAMPTZ NOT NULL,
    customer_id TEXT NOT NULL REFERENCES dim_customers(customer_id),
    product_id TEXT NOT NULL REFERENCES dim_products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    discount_pct NUMERIC(5, 4) NOT NULL CHECK (discount_pct BETWEEN 0 AND 1),
    sales_channel TEXT NOT NULL CHECK (sales_channel IN ('Online', 'Store')),
    returned BOOLEAN NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_transactions_ts
    ON fact_transactions (transaction_ts);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_product_ts
    ON fact_transactions (product_id, transaction_ts DESC);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_customer_ts
    ON fact_transactions (customer_id, transaction_ts DESC);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_channel_ts
    ON fact_transactions (sales_channel, transaction_ts DESC);
