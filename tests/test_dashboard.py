import json
from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest

from merchmind.backtest import run_backtest
from merchmind.live import refresh_from_stream
from merchmind.pipeline import run_pipeline


def test_dashboard_categories_and_forecast_validation(tmp_path, monkeypatch):
    run_pipeline(tmp_path, transactions=1000, customers=100, products=30)
    run_backtest(tmp_path)
    monkeypatch.setenv("MERCHMIND_DATA_DIR", str(tmp_path))
    path = Path(__file__).resolve().parents[1] / "src/merchmind/dashboard.py"
    app = AppTest.from_file(str(path)).run(timeout=30)
    assert not app.exception
    assert len(app.metric) == 5
    assert len(app.dataframe) == 3
    categories = sorted(
        pd.read_parquet(tmp_path / "gold/daily_category_performance.parquet").category.unique()
    )
    assert app.selectbox[0].options == categories
    for category in categories:
        app.selectbox[0].select(category).run()
        assert not app.exception
        assert app.selectbox[0].value == category
    before = int(app.metric[2].value.replace(",", ""))
    raw = tmp_path / "raw"
    (raw / "_spark_metadata").mkdir(parents=True)
    row = pd.read_parquet(tmp_path / "silver/fact_transactions.parquet").iloc[0].to_dict()
    row["transaction_ts"] = row["transaction_ts"].isoformat()
    row["transaction_id"] = "dashboard-live-test"
    pd.DataFrame(
        [{"topic": "test", "partition": 0, "offset": 0, "value": json.dumps(row)}]
    ).to_parquet(raw / "part.parquet")
    (raw / "_spark_metadata/0").write_text(
        "v1\n" + json.dumps({"path": "file:///raw/part.parquet", "action": "add"})
    )
    refresh_from_stream(tmp_path, raw)
    app.run()
    assert not app.exception
    assert int(app.metric[2].value.replace(",", "")) == before + 1
    assert app.selectbox[0].value == categories[-1]
