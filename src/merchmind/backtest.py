"""Rolling-origin evaluation; every forecast uses history strictly before its test window."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from merchmind.analytics import build_category_forecast


def evaluate_forecasts(
    daily: pd.DataFrame,
    horizon: int = 28,
    min_train_days: int = 84,
    step: int = 28,
) -> tuple[pd.DataFrame, dict]:
    if horizon < 1 or min_train_days < 7 or step < horizon:
        raise ValueError("Use a positive horizon, at least 7 training days, and step >= horizon")
    daily = daily.copy()
    daily["date"] = pd.to_datetime(daily["date"])
    calendar = pd.date_range(daily.date.min(), daily.date.max(), freq="D")
    if len(calendar) < min_train_days + horizon:
        raise ValueError("Not enough history for one complete holdout window")
    predictions = []
    for offset in range(min_train_days, len(calendar) - horizon + 1, step):
        cutoff = calendar[offset - 1]
        train = daily.loc[daily.date <= cutoff]
        forecast = build_category_forecast(train, horizon=horizon)
        forecast["origin"] = cutoff
        forecast["model"] = "weekday_mean_84d"
        # Build actuals only after forecasts; absent category-days represent zero sales.
        actuals = daily.rename(columns={"date": "forecast_date", "units": "actual_units"})
        forecast = forecast.merge(
            actuals[["forecast_date", "category", "actual_units"]],
            on=["forecast_date", "category"],
            how="left",
            validate="one_to_one",
        )
        forecast["actual_units"] = forecast.actual_units.fillna(0)
        predictions.append(forecast)
        for model in ("last_week", "trailing_mean_28d"):
            baseline = forecast.copy()
            baseline["model"] = model
            baseline[["lower_units", "upper_units"]] = np.nan
            for category in forecast.category.unique():
                history = train.loc[train.category.eq(category)].set_index("date").units
                history = history.reindex(pd.date_range(calendar[0], cutoff), fill_value=0)
                mask = baseline.category.eq(category)
                if model == "last_week":
                    values = history.tail(7).to_numpy()
                    baseline.loc[mask, "predicted_units"] = np.resize(values, horizon)
                else:
                    baseline.loc[mask, "predicted_units"] = history.tail(28).mean()
            predictions.append(baseline)
    frame = pd.concat(predictions, ignore_index=True)
    metrics = []
    for keys, group in frame.groupby(["model", "category"]):
        metrics.append({"model": keys[0], "category": keys[1], **_metrics(group)})
    overall = [{"model": model, **_metrics(group)} for model, group in frame.groupby("model")]
    report = {
        "evaluated_at_utc": datetime.now(UTC).isoformat(),
        "data_start": str(calendar[0].date()),
        "data_end": str(calendar[-1].date()),
        "horizon_days": horizon,
        "min_train_days": min_train_days,
        "step_days": step,
        "folds": int(frame.origin.nunique()),
        "categories": int(frame.category.nunique()),
        "overall": overall,
        "by_category": metrics,
        "interpretation": "Synthetic data evaluation; not evidence of real-retailer accuracy. "
        "WAPE uses pooled category-day errors. Coverage measures the existing nominal 80% bands; "
        "the bands have not been recalibrated on these holdouts.",
    }
    return frame, report


def _metrics(frame: pd.DataFrame) -> dict:
    error = frame.predicted_units - frame.actual_units
    denominator = float(frame.actual_units.abs().sum())
    bands = frame.lower_units.notna()
    return {
        "observations": len(frame),
        "mae": round(float(error.abs().mean()), 4),
        "rmse": round(float(np.sqrt((error**2).mean())), 4),
        "wape_pct": round(100 * float(error.abs().sum()) / denominator, 4) if denominator else None,
        "bias_pct": round(100 * float(error.sum()) / denominator, 4) if denominator else None,
        "interval_coverage_pct": round(
            100
            * float(
                frame.loc[bands]
                .actual_units.between(frame.loc[bands].lower_units, frame.loc[bands].upper_units)
                .mean()
            ),
            4,
        )
        if bands.any()
        else None,
    }


def run_backtest(data_dir: Path, **kwargs) -> dict:
    from merchmind.config import DataPaths

    paths = DataPaths(data_dir)
    frame, report = evaluate_forecasts(
        pd.read_parquet(paths.gold / "daily_category_performance.parquet"), **kwargs
    )
    paths.reports.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(paths.reports / "forecast_backtest.parquet", index=False)
    (paths.reports / "forecast_backtest.json").write_text(json.dumps(report, indent=2) + "\n")
    return report
