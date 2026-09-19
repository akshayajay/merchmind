from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from merchmind.config import DataPaths, default_data_dir
from merchmind.live import serving_paths
from merchmind.pipeline import run_pipeline

st.set_page_config(page_title="MERCHMIND", page_icon="🧵", layout="wide")
st.markdown(
    """
    <style>
    .stApp { background: #f8f4ec; color: #201b17; }
    [data-testid="stMetric"] { background: #fffdf8; border: 1px solid #ddcfbe;
      padding: 1rem; border-radius: 0.75rem; }
    h1, h2, h3 { font-family: Georgia, serif; }
    </style>
    """,
    unsafe_allow_html=True,
)


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _money(value: float) -> str:
    return f"${value:,.0f}"


@st.cache_resource(show_spinner="Preparing the MERCHMIND demo dataset…")
def _ensure_demo_data() -> DataPaths:
    """Build a reproducible hosted demo when no persisted Gold layer is available."""
    paths = DataPaths(default_data_dir())
    if not (paths.gold / "executive_kpis.json").exists():
        run_pipeline(paths.root, transactions=50_000, customers=2_500, products=500)
    return paths


@st.fragment(run_every="5s")
def render() -> None:
    base = _ensure_demo_data()
    paths = serving_paths(base.root)
    kpi_path = paths.gold / "executive_kpis.json"
    st.title("MERCHMIND")
    st.caption("Fashion retail market intelligence and merchandising analytics")

    if not kpi_path.exists():
        st.error("The demo dataset could not be prepared. Please refresh the app.")
        return

    live_report = paths.reports / "live_refresh.json"
    if live_report.exists():
        status = _load_json(live_report)
        st.caption(
            f"Live data · Updated {status['published_at_utc']} · "
            f"{status['new_transactions']:,} new transactions · Refreshes every 5 seconds"
        )
    else:
        st.caption("Batch demo · Waiting for streaming updates")
    kpis = _load_json(kpi_path)
    daily = pd.read_parquet(paths.gold / "daily_category_performance.parquet")
    products = pd.read_parquet(paths.gold / "product_performance.parquet")
    customers = pd.read_parquet(paths.gold / "customer_rfm.parquet")
    market = pd.read_parquet(paths.gold / "market_pulse.parquet")
    forecast = pd.read_parquet(paths.gold / "category_forecast.parquet")

    metric_columns = st.columns(5)
    metric_columns[0].metric("Net revenue", _money(float(kpis["net_revenue"])))
    metric_columns[1].metric("Gross profit", _money(float(kpis["gross_profit"])))
    metric_columns[2].metric("Orders", f"{int(kpis['orders']):,}")
    metric_columns[3].metric("Customers", f"{int(kpis['customers']):,}")
    metric_columns[4].metric(
        "Stream quality" if live_report.exists() else "Quality pass rate",
        f"{100 * float(kpis['data_quality_pass_rate']):.1f}%",
    )

    st.subheader("Market pulse")
    left, right = st.columns([1.6, 1])
    category_totals = daily.groupby("category", as_index=False)["net_revenue"].sum()
    left.plotly_chart(
        px.bar(
            category_totals.sort_values("net_revenue"),
            x="net_revenue",
            y="category",
            orientation="h",
            color="net_revenue",
            color_continuous_scale=["#d9c5b2", "#8c3b2a"],
            labels={"net_revenue": "Net revenue", "category": ""},
        ),
        width="stretch",
    )
    segment_summary = customers.groupby("segment", as_index=False)["customer_id"].nunique()
    right.plotly_chart(
        px.pie(
            segment_summary,
            names="segment",
            values="customer_id",
            hole=0.58,
            color_discrete_sequence=["#8c3b2a", "#d58f72", "#315c55", "#c9a44c", "#86766b"],
        ),
        width="stretch",
    )

    st.subheader("Demand and merchandising")
    selected_category = st.selectbox("Category", sorted(daily["category"].unique()))
    category_history = daily.loc[daily["category"] == selected_category]
    category_forecast = forecast.loc[forecast["category"] == selected_category]
    history_chart = px.line(
        category_history, x="date", y="units", title=f"{selected_category} unit demand"
    )
    history_chart.add_scatter(
        x=category_forecast["forecast_date"],
        y=category_forecast["predicted_units"],
        name="Forecast",
        line={"dash": "dash", "color": "#8c3b2a"},
    )
    history_chart.add_scatter(
        x=category_forecast["forecast_date"],
        y=category_forecast["upper_units"],
        mode="lines",
        line={"width": 0},
        name="Upper band",
        showlegend=False,
    )
    history_chart.add_scatter(
        x=category_forecast["forecast_date"],
        y=category_forecast["lower_units"],
        mode="lines",
        line={"width": 0},
        fill="tonexty",
        fillcolor="rgba(140,59,42,0.15)",
        name="Nominal 80% range",
    )
    st.plotly_chart(history_chart, width="stretch")
    backtest_path = base.reports / "forecast_backtest.json"
    if backtest_path.exists():
        evaluation = _load_json(backtest_path)
        st.markdown("#### Forecast validation")
        st.caption(
            f"Rolling holdouts through {evaluation['data_end']} · "
            f"{evaluation['folds']} folds · Synthetic data; historical evaluation"
        )
        st.dataframe(pd.DataFrame(evaluation["overall"]), hide_index=True, width="stretch")

    product_left, risk_right = st.columns(2)
    product_left.markdown("#### Slow-moving product watchlist")
    product_left.dataframe(
        products.loc[products["slow_mover_flag"]]
        .sort_values("net_revenue")[
            ["product_name", "category", "units_per_active_day", "average_discount", "return_rate"]
        ]
        .head(12),
        width="stretch",
        hide_index=True,
    )
    risk_right.markdown("#### Company inventory stress")
    latest_market = market.sort_values("period").groupby("company", as_index=False).tail(1)
    risk_right.dataframe(
        latest_market[
            [
                "company",
                "inventory_stress_score",
                "risk_band",
                "revenue_growth_yoy_pct",
                "inventory_growth_yoy_pct",
            ]
        ].sort_values("inventory_stress_score", ascending=False),
        width="stretch",
        hide_index=True,
    )

    st.caption(
        "Synthetic data powers the default demo. Optional SEC EDGAR and BLS adapters keep "
        "public market data separate from the reproducible retail dataset."
    )


render()
