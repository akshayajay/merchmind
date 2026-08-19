from __future__ import annotations

import json

import pandas as pd

from merchmind.config import DataPaths
from merchmind.pipeline import load_kpis, run_pipeline


def test_pipeline_builds_auditable_medallion_layers(tmp_path) -> None:
    result = run_pipeline(
        tmp_path / "warehouse",
        transactions=1_200,
        customers=140,
        products=45,
        seed=23,
    )
    paths = DataPaths(tmp_path / "warehouse")

    assert result.clean_rows + result.quarantine_rows == result.source_rows
    assert result.gold_tables == 6
    assert (paths.bronze / "transactions.parquet").exists()
    assert (paths.silver / "fact_transactions.parquet").exists()
    assert (paths.gold / "category_forecast.parquet").exists()

    kpis = load_kpis(paths.root)
    assert kpis["orders"] == result.clean_rows
    assert 0 < kpis["data_quality_pass_rate"] < 1

    quality = json.loads((paths.reports / "quality_report.json").read_text())
    manifest = json.loads((paths.reports / "pipeline_manifest.json").read_text())
    forecast = pd.read_parquet(paths.gold / "category_forecast.parquet")
    assert quality["quarantine_rows"] == result.quarantine_rows
    assert manifest["pipeline"]["gold_tables"] == 6
    assert forecast.groupby("category").size().eq(28).all()
