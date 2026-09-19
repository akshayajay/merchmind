from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter

import pandas as pd

from merchmind.analytics import (
    build_category_forecast,
    build_customer_rfm,
    build_daily_category,
    build_executive_kpis,
    build_market_pulse,
    build_product_performance,
    enrich_transactions,
)
from merchmind.config import DataPaths
from merchmind.quality import validate_transactions
from merchmind.synthetic import SyntheticConfig, generate_all


@dataclass(frozen=True)
class PipelineResult:
    data_dir: str
    source_rows: int
    clean_rows: int
    quarantine_rows: int
    gold_tables: int
    runtime_seconds: float


def _write_json(payload: dict[str, object], path: Path) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )


def _write_table(frame: pd.DataFrame, path: Path) -> None:
    frame.to_parquet(path, index=False, compression="snappy")


def build_gold(
    paths: DataPaths,
    clean: pd.DataFrame,
    products: pd.DataFrame,
    customers: pd.DataFrame,
    company_financials: pd.DataFrame,
    macro_indicators: pd.DataFrame,
    quality_report: dict,
) -> int:
    enriched = enrich_transactions(clean, products, customers)
    daily_category = build_daily_category(enriched)
    product_performance = build_product_performance(enriched)
    customer_rfm = build_customer_rfm(enriched)
    market_pulse = build_market_pulse(company_financials, macro_indicators)
    category_forecast = build_category_forecast(daily_category)
    executive_kpis = build_executive_kpis(enriched, quality_report)

    gold_frames = {
        "daily_category_performance": daily_category,
        "product_performance": product_performance,
        "customer_rfm": customer_rfm,
        "market_pulse": market_pulse,
        "category_forecast": category_forecast,
    }
    for name, frame in gold_frames.items():
        _write_table(frame, paths.gold / f"{name}.parquet")
    _write_json(executive_kpis, paths.gold / "executive_kpis.json")

    return len(gold_frames) + 1


def run_pipeline(
    data_dir: Path,
    transactions: int = 50_000,
    customers: int = 2_500,
    products: int = 500,
    seed: int = 42,
) -> PipelineResult:
    started = perf_counter()
    paths = DataPaths(data_dir).create()
    synthetic_config = SyntheticConfig(
        seed=seed,
        customers=customers,
        products=products,
        transactions=transactions,
    )
    datasets = generate_all(synthetic_config)

    for name, frame in datasets.items():
        _write_table(frame, paths.bronze / f"{name}.parquet")

    validation = validate_transactions(
        datasets["transactions"], datasets["products"], datasets["customers"]
    )
    _write_table(validation.clean, paths.silver / "fact_transactions.parquet")
    _write_table(validation.quarantine, paths.reports / "quarantined_transactions.parquet")
    _write_table(datasets["products"], paths.silver / "dim_products.parquet")
    _write_table(datasets["customers"], paths.silver / "dim_customers.parquet")
    _write_table(datasets["company_financials"], paths.silver / "company_financials.parquet")
    _write_table(datasets["macro_indicators"], paths.silver / "macro_indicators.parquet")
    _write_json(validation.report, paths.reports / "quality_report.json")

    gold_tables = build_gold(
        paths,
        validation.clean,
        datasets["products"],
        datasets["customers"],
        datasets["company_financials"],
        datasets["macro_indicators"],
        validation.report,
    )

    result = PipelineResult(
        data_dir=str(paths.root),
        source_rows=int(validation.report["source_rows"]),
        clean_rows=int(validation.report["clean_rows"]),
        quarantine_rows=int(validation.report["quarantine_rows"]),
        gold_tables=gold_tables,
        runtime_seconds=round(perf_counter() - started, 3),
    )
    manifest = {
        "pipeline": asdict(result),
        "synthetic_config": asdict(synthetic_config),
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "layers": {
            "bronze": sorted(path.name for path in paths.bronze.glob("*.parquet")),
            "silver": sorted(path.name for path in paths.silver.glob("*.parquet")),
            "gold": sorted(path.name for path in paths.gold.iterdir()),
            "reports": sorted(path.name for path in paths.reports.iterdir()),
        },
    }
    _write_json(manifest, paths.reports / "pipeline_manifest.json")
    return result


def load_kpis(data_dir: Path) -> dict[str, object]:
    path = DataPaths(data_dir).gold / "executive_kpis.json"
    if not path.exists():
        raise FileNotFoundError(f"Gold outputs not found at {path}. Run `merchmind run` first.")
    return json.loads(path.read_text(encoding="utf-8"))
