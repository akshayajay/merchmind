from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

CATEGORY_CONFIG = {
    "Dresses": (96.0, "Womenswear"),
    "Tops": (48.0, "Womenswear"),
    "Trousers": (72.0, "Womenswear"),
    "Outerwear": (145.0, "Unisex"),
    "Knitwear": (82.0, "Unisex"),
    "Activewear": (64.0, "Unisex"),
    "Shoes": (118.0, "Unisex"),
    "Accessories": (42.0, "Unisex"),
}

COLORS = ["Black", "Ivory", "Navy", "Red", "Sage", "Chocolate", "Silver", "Pink"]
AESTHETICS = [
    "Minimalist",
    "Streetwear",
    "Romantic",
    "Athleisure",
    "Quiet Luxury",
    "Vintage",
]
SEASONS = ["Spring", "Summer", "Autumn", "Winter", "Core"]
REGIONS = ["Northeast", "South", "Midwest", "West", "International"]
CHANNELS = ["Online", "Store"]
COMPANIES = [
    "Aster & Row",
    "Northstar Apparel",
    "Maison Vale",
    "Common Thread",
    "Lumen Active",
]


@dataclass(frozen=True)
class SyntheticConfig:
    seed: int = 42
    customers: int = 2_500
    products: int = 500
    transactions: int = 50_000
    start_date: str = "2024-01-01"
    days: int = 730
    quality_noise: float = 0.012


def _rng(config: SyntheticConfig) -> np.random.Generator:
    return np.random.default_rng(config.seed)


def generate_products(config: SyntheticConfig) -> pd.DataFrame:
    rng = _rng(config)
    categories = rng.choice(
        list(CATEGORY_CONFIG),
        size=config.products,
        p=[0.13, 0.20, 0.13, 0.10, 0.12, 0.11, 0.12, 0.09],
    )
    colors = rng.choice(COLORS, size=config.products)
    aesthetics = rng.choice(AESTHETICS, size=config.products)
    seasons = rng.choice(SEASONS, size=config.products, p=[0.17, 0.18, 0.18, 0.17, 0.30])
    prices = np.array([CATEGORY_CONFIG[category][0] for category in categories])
    prices *= rng.lognormal(mean=0.0, sigma=0.24, size=config.products)
    launch_offsets = rng.integers(0, config.days - 30, size=config.products)

    return pd.DataFrame(
        {
            "product_id": [f"P{i:06d}" for i in range(1, config.products + 1)],
            "product_name": [
                f"{color} {aesthetic} {category}"
                for color, aesthetic, category in zip(colors, aesthetics, categories, strict=True)
            ],
            "category": categories,
            "department": [CATEGORY_CONFIG[category][1] for category in categories],
            "color": colors,
            "aesthetic": aesthetics,
            "season": seasons,
            "list_price": np.round(prices, 2),
            "unit_cost": np.round(prices * rng.uniform(0.28, 0.48, size=config.products), 2),
            "launch_date": pd.Timestamp(config.start_date)
            + pd.to_timedelta(launch_offsets, unit="D"),
        }
    )


def generate_customers(config: SyntheticConfig) -> pd.DataFrame:
    rng = np.random.default_rng(config.seed + 1)
    joined_offsets = rng.integers(0, config.days - 14, size=config.customers)
    return pd.DataFrame(
        {
            "customer_id": [f"C{i:07d}" for i in range(1, config.customers + 1)],
            "age_band": rng.choice(
                ["18-24", "25-34", "35-44", "45-54", "55+"],
                size=config.customers,
                p=[0.18, 0.34, 0.24, 0.15, 0.09],
            ),
            "region": rng.choice(REGIONS, size=config.customers, p=[0.20, 0.24, 0.18, 0.25, 0.13]),
            "acquisition_channel": rng.choice(
                ["Organic", "Paid Social", "Email", "Referral", "Store"],
                size=config.customers,
            ),
            "joined_date": pd.Timestamp(config.start_date)
            + pd.to_timedelta(joined_offsets, unit="D"),
        }
    )


def generate_transactions(
    config: SyntheticConfig, products: pd.DataFrame, customers: pd.DataFrame
) -> pd.DataFrame:
    rng = np.random.default_rng(config.seed + 2)
    product_popularity = rng.pareto(1.8, len(products)) + 0.2
    product_popularity /= product_popularity.sum()
    customer_activity = rng.pareto(2.2, len(customers)) + 0.3
    customer_activity /= customer_activity.sum()

    product_idx = rng.choice(len(products), config.transactions, p=product_popularity)
    customer_idx = rng.choice(len(customers), config.transactions, p=customer_activity)
    day_offsets = rng.integers(0, config.days, size=config.transactions)
    hour_offsets = rng.integers(8, 23, size=config.transactions)
    minute_offsets = rng.integers(0, 60, size=config.transactions)
    discount = rng.choice(
        [0.0, 0.10, 0.20, 0.35, 0.50], config.transactions, p=[0.47, 0.18, 0.17, 0.12, 0.06]
    )
    selected_prices = products.iloc[product_idx]["list_price"].to_numpy()
    transaction_time = (
        pd.Timestamp(config.start_date)
        + pd.to_timedelta(day_offsets, unit="D")
        + pd.to_timedelta(hour_offsets, unit="h")
        + pd.to_timedelta(minute_offsets, unit="m")
    )

    transactions = pd.DataFrame(
        {
            "transaction_id": [f"T{i:09d}" for i in range(1, config.transactions + 1)],
            "transaction_ts": transaction_time,
            "customer_id": customers.iloc[customer_idx]["customer_id"].to_numpy(),
            "product_id": products.iloc[product_idx]["product_id"].to_numpy(),
            "quantity": rng.choice([1, 2, 3], config.transactions, p=[0.88, 0.10, 0.02]),
            "unit_price": np.round(selected_prices * (1 - discount), 2),
            "discount_pct": discount,
            "sales_channel": rng.choice(CHANNELS, config.transactions, p=[0.68, 0.32]),
            "returned": rng.random(config.transactions) < 0.115,
        }
    )

    noise_rows = max(1, int(config.transactions * config.quality_noise))
    invalid_idx = rng.choice(transactions.index, noise_rows, replace=False)
    thirds = np.array_split(invalid_idx, 3)
    transactions.loc[thirds[0], "customer_id"] = None
    transactions.loc[thirds[1], "quantity"] = 0
    transactions.loc[thirds[2], "unit_price"] = -1.0

    duplicate_count = max(1, noise_rows // 3)
    duplicates = transactions.sample(duplicate_count, random_state=config.seed)
    return pd.concat([transactions, duplicates], ignore_index=True)


def generate_company_financials(config: SyntheticConfig) -> pd.DataFrame:
    rng = np.random.default_rng(config.seed + 3)
    periods = pd.date_range(config.start_date, periods=8, freq="QE")
    rows: list[dict[str, object]] = []
    for company_idx, company in enumerate(COMPANIES):
        base_revenue = 360 + company_idx * 115
        inventory_ratio = 0.24 + company_idx * 0.025
        for quarter_idx, period in enumerate(periods):
            seasonality = [0.88, 0.94, 1.00, 1.28][quarter_idx % 4]
            growth = 1 + (0.018 + company_idx * 0.004) * quarter_idx
            revenue = base_revenue * seasonality * growth * rng.normal(1, 0.025)
            inventory_pressure = 1 + max(0, quarter_idx - 4) * (0.018 * company_idx)
            rows.append(
                {
                    "company": company,
                    "period": period,
                    "revenue_m": round(revenue, 2),
                    "inventory_m": round(
                        base_revenue * inventory_ratio * inventory_pressure * rng.normal(1, 0.035),
                        2,
                    ),
                    "gross_margin_pct": round(
                        42.5 + company_idx * 1.7 - max(0, quarter_idx - 4) * 0.35 * company_idx,
                        2,
                    ),
                    "operating_expense_m": round(revenue * (0.27 + company_idx * 0.008), 2),
                }
            )
    return pd.DataFrame(rows)


def generate_macro_indicators(config: SyntheticConfig) -> pd.DataFrame:
    rng = np.random.default_rng(config.seed + 4)
    periods = pd.date_range(config.start_date, periods=24, freq="MS")
    trend = np.arange(len(periods))
    return pd.DataFrame(
        {
            "period": periods,
            "apparel_cpi": np.round(100 + trend * 0.18 + rng.normal(0, 0.22, len(periods)), 2),
            "retail_sales_index": np.round(
                100
                + trend * 0.34
                + 3.2 * np.sin(2 * np.pi * trend / 12)
                + rng.normal(0, 0.5, len(periods)),
                2,
            ),
            "consumer_sentiment": np.round(
                76 + 4.5 * np.sin(2 * np.pi * (trend + 2) / 12) + rng.normal(0, 1.2, len(periods)),
                2,
            ),
        }
    )


def generate_all(config: SyntheticConfig) -> dict[str, pd.DataFrame]:
    products = generate_products(config)
    customers = generate_customers(config)
    return {
        "products": products,
        "customers": customers,
        "transactions": generate_transactions(config, products, customers),
        "company_financials": generate_company_financials(config),
        "macro_indicators": generate_macro_indicators(config),
    }
