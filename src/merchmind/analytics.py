from __future__ import annotations

import numpy as np
import pandas as pd


def enrich_transactions(
    transactions: pd.DataFrame,
    products: pd.DataFrame,
    customers: pd.DataFrame,
) -> pd.DataFrame:
    frame = transactions.merge(products, on="product_id", how="left", validate="many_to_one")
    frame = frame.merge(customers, on="customer_id", how="left", validate="many_to_one")
    frame["date"] = frame["transaction_ts"].dt.tz_convert(None).dt.normalize()
    frame["gross_revenue"] = frame["quantity"] * frame["unit_price"]
    frame["net_revenue"] = np.where(
        frame["returned"], -frame["gross_revenue"], frame["gross_revenue"]
    )
    frame["gross_profit"] = np.where(
        frame["returned"],
        -frame["quantity"] * (frame["unit_price"] - frame["unit_cost"]),
        frame["quantity"] * (frame["unit_price"] - frame["unit_cost"]),
    )
    return frame


def build_daily_category(enriched: pd.DataFrame) -> pd.DataFrame:
    return (
        enriched.groupby(["date", "category"], as_index=False)
        .agg(
            net_revenue=("net_revenue", "sum"),
            gross_revenue=("gross_revenue", "sum"),
            units=("quantity", "sum"),
            orders=("transaction_id", "nunique"),
            customers=("customer_id", "nunique"),
            average_unit_price=("unit_price", "mean"),
            return_rate=("returned", "mean"),
            average_discount=("discount_pct", "mean"),
        )
        .sort_values(["date", "category"])
        .reset_index(drop=True)
    )


def build_product_performance(enriched: pd.DataFrame) -> pd.DataFrame:
    metrics = (
        enriched.groupby(
            ["product_id", "product_name", "category", "aesthetic", "color"], as_index=False
        )
        .agg(
            net_revenue=("net_revenue", "sum"),
            gross_profit=("gross_profit", "sum"),
            units=("quantity", "sum"),
            orders=("transaction_id", "nunique"),
            customers=("customer_id", "nunique"),
            average_price=("unit_price", "mean"),
            average_discount=("discount_pct", "mean"),
            return_rate=("returned", "mean"),
            first_sale=("date", "min"),
            last_sale=("date", "max"),
        )
        .reset_index(drop=True)
    )
    active_days = (metrics["last_sale"] - metrics["first_sale"]).dt.days.clip(lower=1)
    metrics["units_per_active_day"] = metrics["units"] / active_days
    category_velocity = metrics.groupby("category")["units_per_active_day"].transform("median")
    metrics["slow_mover_flag"] = metrics["units_per_active_day"] < category_velocity * 0.55
    metrics["gross_margin_pct"] = np.where(
        metrics["net_revenue"] != 0,
        100 * metrics["gross_profit"] / metrics["net_revenue"],
        np.nan,
    )
    return metrics.sort_values("net_revenue", ascending=False).reset_index(drop=True)


def _quartile_score(series: pd.Series, higher_is_better: bool = True) -> pd.Series:
    ranked = series.rank(method="first", ascending=True)
    labels = [1, 2, 3, 4] if higher_is_better else [4, 3, 2, 1]
    return pd.qcut(ranked, 4, labels=labels).astype(int)


def build_customer_rfm(enriched: pd.DataFrame) -> pd.DataFrame:
    reference_date = enriched["date"].max() + pd.Timedelta(days=1)
    rfm = (
        enriched.groupby("customer_id", as_index=False)
        .agg(
            last_purchase=("date", "max"),
            frequency=("transaction_id", "nunique"),
            monetary=("net_revenue", "sum"),
            return_rate=("returned", "mean"),
            preferred_channel=("sales_channel", lambda values: values.mode().iat[0]),
        )
        .reset_index(drop=True)
    )
    rfm["recency_days"] = (reference_date - rfm["last_purchase"]).dt.days
    rfm["r_score"] = _quartile_score(rfm["recency_days"], higher_is_better=False)
    rfm["f_score"] = _quartile_score(rfm["frequency"], higher_is_better=True)
    rfm["m_score"] = _quartile_score(rfm["monetary"], higher_is_better=True)

    conditions = [
        (rfm["r_score"] >= 3) & (rfm["f_score"] >= 3),
        rfm["f_score"] >= 3,
        (rfm["r_score"] <= 2) & (rfm["f_score"] >= 2),
        (rfm["r_score"] >= 3) & (rfm["f_score"] <= 2),
    ]
    rfm["segment"] = np.select(
        conditions,
        ["Champions", "Loyal", "At Risk", "New"],
        default="Developing",
    )
    return rfm.sort_values(["segment", "monetary"], ascending=[True, False]).reset_index(drop=True)


def build_market_pulse(
    company_financials: pd.DataFrame, macro_indicators: pd.DataFrame
) -> pd.DataFrame:
    frame = company_financials.copy().sort_values(["company", "period"])
    frame["revenue_growth_yoy_pct"] = frame.groupby("company")["revenue_m"].pct_change(4) * 100
    frame["inventory_growth_yoy_pct"] = frame.groupby("company")["inventory_m"].pct_change(4) * 100
    frame["gross_margin_change_yoy_pp"] = frame.groupby("company")["gross_margin_pct"].diff(4)
    frame["inventory_to_sales"] = frame["inventory_m"] / frame["revenue_m"]
    frame["inventory_stress_score"] = (
        frame["inventory_growth_yoy_pct"]
        - frame["revenue_growth_yoy_pct"]
        - frame["gross_margin_change_yoy_pp"]
    )
    frame["risk_band"] = pd.cut(
        frame["inventory_stress_score"],
        bins=[-np.inf, 0, 8, np.inf],
        labels=["Low", "Watch", "Elevated"],
    ).astype("string")

    macro = macro_indicators.copy().sort_values("period")
    frame["month"] = frame["period"].dt.to_period("M").dt.to_timestamp()
    frame = pd.merge_asof(
        frame.sort_values("month"),
        macro.sort_values("period"),
        left_on="month",
        right_on="period",
        direction="backward",
        suffixes=("", "_macro"),
    )
    return frame.sort_values(["company", "period"]).reset_index(drop=True)


def build_category_forecast(daily_category: pd.DataFrame, horizon: int = 28) -> pd.DataFrame:
    """Create a transparent weekday seasonal-naive forecast with uncertainty bands."""
    forecasts: list[pd.DataFrame] = []
    calendar = pd.date_range(daily_category.date.min(), daily_category.date.max(), freq="D")
    for category, category_frame in daily_category.groupby("category"):
        indexed = (
            category_frame.set_index("date")["units"].reindex(calendar, fill_value=0).astype(float)
        )
        last_date = indexed.index.max()
        future_dates = pd.date_range(last_date + pd.Timedelta(days=1), periods=horizon, freq="D")
        history = indexed.tail(84)
        weekday_means = history.groupby(history.index.dayofweek).mean()
        weekday_std = history.groupby(history.index.dayofweek).std().fillna(0)
        prediction = np.array(
            [weekday_means.get(day.dayofweek, history.mean()) for day in future_dates]
        )
        uncertainty = np.array(
            [weekday_std.get(day.dayofweek, history.std()) for day in future_dates]
        )
        forecasts.append(
            pd.DataFrame(
                {
                    "forecast_date": future_dates,
                    "category": category,
                    "predicted_units": np.round(np.clip(prediction, 0, None), 2),
                    "lower_units": np.round(np.clip(prediction - 1.28 * uncertainty, 0, None), 2),
                    "upper_units": np.round(np.clip(prediction + 1.28 * uncertainty, 0, None), 2),
                    "model": "weekday_seasonal_naive_80pct_interval",
                }
            )
        )
    return pd.concat(forecasts, ignore_index=True)


def build_executive_kpis(
    enriched: pd.DataFrame, quality_report: dict[str, object]
) -> dict[str, object]:
    category_revenue = (
        enriched.groupby("category")["net_revenue"].sum().sort_values(ascending=False)
    )
    total_orders = int(enriched["transaction_id"].nunique())
    return {
        "net_revenue": round(float(enriched["net_revenue"].sum()), 2),
        "gross_profit": round(float(enriched["gross_profit"].sum()), 2),
        "orders": total_orders,
        "customers": int(enriched["customer_id"].nunique()),
        "products": int(enriched["product_id"].nunique()),
        "average_order_value": round(float(enriched["net_revenue"].sum() / total_orders), 2),
        "return_rate": round(float(enriched["returned"].mean()), 4),
        "top_category": str(category_revenue.index[0]),
        "data_start": str(enriched["date"].min().date()),
        "data_end": str(enriched["date"].max().date()),
        "data_quality_pass_rate": quality_report["pass_rate"],
    }
