import numpy as np
import pandas as pd
import pytest

from merchmind.backtest import evaluate_forecasts, run_backtest


def weekly_data():
    dates = pd.date_range("2025-01-01", periods=168)
    return pd.DataFrame({"date": dates, "category": "A", "units": 10 + dates.dayofweek})


def test_rolling_forecasts_do_not_use_heldout_values(tmp_path):
    daily = weekly_data()
    first, report = evaluate_forecasts(daily)
    changed = daily.copy()
    changed.loc[changed.date > first.origin.min(), "units"] = 99999
    altered, _ = evaluate_forecasts(changed)
    columns = ["model", "forecast_date", "predicted_units", "lower_units", "upper_units"]
    pd.testing.assert_frame_equal(
        first.loc[first.origin.eq(first.origin.min()), columns].reset_index(drop=True),
        altered.loc[altered.origin.eq(altered.origin.min()), columns].reset_index(drop=True),
    )
    assert report["folds"] == 3
    metrics = {row["model"]: row for row in report["overall"]}
    assert metrics["last_week"]["mae"] == 0
    assert metrics["weekday_mean_84d"]["mae"] == 0
    assert metrics["weekday_mean_84d"]["interval_coverage_pct"] == 100
    assert metrics["trailing_mean_28d"]["mae"] > 0
    (tmp_path / "gold").mkdir()
    daily.to_parquet(tmp_path / "gold/daily_category_performance.parquet")
    assert run_backtest(tmp_path)["folds"] == 3
    assert (tmp_path / "reports/forecast_backtest.parquet").exists()


def test_zero_demand_and_insufficient_history():
    daily = weekly_data().assign(units=0)
    _, report = evaluate_forecasts(daily)
    assert all(row["wape_pct"] is None for row in report["overall"])
    with pytest.raises(ValueError, match="Not enough"):
        evaluate_forecasts(daily.head(10))
    with pytest.raises(ValueError, match="step"):
        evaluate_forecasts(daily, step=1)
    sparse = daily.copy()
    sparse.loc[sparse.index % 2 == 0, "category"] = "B"
    predictions, _ = evaluate_forecasts(sparse)
    assert predictions.groupby(["origin", "category", "model"]).size().eq(28).all()
    assert np.isfinite(predictions.predicted_units).all()
