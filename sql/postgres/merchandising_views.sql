CREATE OR REPLACE VIEW gold_daily_category_performance AS
SELECT
    transaction_ts::date AS sale_date,
    p.category,
    COUNT(DISTINCT transaction_id) AS orders,
    SUM(quantity) AS units,
    SUM(
        CASE WHEN returned THEN -quantity * unit_price ELSE quantity * unit_price END
    ) AS net_revenue,
    AVG(discount_pct) AS average_discount,
    AVG(returned::int) AS return_rate
FROM fact_transactions AS t
JOIN dim_products AS p USING (product_id)
GROUP BY transaction_ts::date, p.category;

CREATE OR REPLACE VIEW gold_product_velocity AS
WITH daily AS (
    SELECT
        transaction_ts::date AS sale_date,
        product_id,
        SUM(quantity) AS units
    FROM fact_transactions
    GROUP BY transaction_ts::date, product_id
),
rolling AS (
    SELECT
        sale_date,
        product_id,
        SUM(units) OVER (
            PARTITION BY product_id
            ORDER BY sale_date
            ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
        ) AS rolling_28d_units
    FROM daily
),
ranked AS (
    SELECT
        r.sale_date,
        r.product_id,
        p.product_name,
        p.category,
        r.rolling_28d_units,
        PERCENT_RANK() OVER (
            PARTITION BY p.category, r.sale_date
            ORDER BY r.rolling_28d_units
        ) AS category_velocity_percentile
    FROM rolling AS r
    JOIN dim_products AS p USING (product_id)
)
SELECT
    *,
    category_velocity_percentile < 0.20 AS slow_mover_flag
FROM ranked;

CREATE OR REPLACE VIEW gold_customer_rfm AS
WITH reference AS (
    SELECT MAX(transaction_ts)::date + 1 AS reference_date
    FROM fact_transactions
),
metrics AS (
    SELECT
        customer_id,
        (MAX(reference_date) - MAX(transaction_ts)::date) AS recency_days,
        COUNT(DISTINCT transaction_id) AS frequency,
        SUM(CASE WHEN returned THEN -quantity * unit_price ELSE quantity * unit_price END) AS monetary
    FROM fact_transactions
    CROSS JOIN reference
    GROUP BY customer_id
),
scored AS (
    SELECT
        *,
        5 - NTILE(4) OVER (ORDER BY recency_days) AS r_score,
        NTILE(4) OVER (ORDER BY frequency) AS f_score,
        NTILE(4) OVER (ORDER BY monetary) AS m_score
    FROM metrics
)
SELECT
    *,
    CASE
        WHEN r_score >= 3 AND f_score >= 3 THEN 'Champions'
        WHEN f_score >= 3 THEN 'Loyal'
        WHEN r_score <= 2 AND f_score >= 2 THEN 'At Risk'
        WHEN r_score >= 3 AND f_score <= 2 THEN 'New'
        ELSE 'Developing'
    END AS segment
FROM scored;
