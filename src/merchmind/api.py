from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, Query

from merchmind import __version__
from merchmind.config import default_data_dir
from merchmind.inventory import stock_path
from merchmind.live import serving_paths


def _records(path: Path, limit: int | None = None) -> list[dict[str, object]]:
    if not path.exists():
        raise HTTPException(status_code=404, detail="Gold table not found; run the pipeline first")
    frame = pd.read_parquet(path)
    if limit is not None:
        frame = frame.head(limit)
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def create_app(data_dir: Path | None = None) -> FastAPI:
    resolved_data_dir = Path(
        data_dir or os.getenv("MERCHMIND_DATA_DIR", str(default_data_dir()))
    ).resolve()

    def paths():
        return serving_paths(resolved_data_dir)

    application = FastAPI(
        title="MERCHMIND API",
        description="Retail analytics, live product stock, and reconciled daily reports",
        version=__version__,
    )

    @application.get("/health")
    def health() -> dict[str, object]:
        return {
            "status": "ok",
            "version": __version__,
            "data_ready": (paths().gold / "executive_kpis.json").exists(),
        }

    @application.get("/v1/kpis")
    def kpis() -> dict[str, object]:
        path = paths().gold / "executive_kpis.json"
        if not path.exists():
            raise HTTPException(status_code=404, detail="KPIs not found; run the pipeline first")
        return json.loads(path.read_text(encoding="utf-8"))

    @application.get("/v1/categories")
    def category_performance(limit: int = Query(500, ge=1, le=5_000)) -> list[dict[str, object]]:
        return _records(paths().gold / "daily_category_performance.parquet", limit)

    @application.get("/v1/products")
    def product_performance(limit: int = Query(100, ge=1, le=2_000)) -> list[dict[str, object]]:
        return _records(paths().gold / "product_performance.parquet", limit)

    @application.get("/v1/inventory-risk")
    def inventory_risk(limit: int = Query(100, ge=1, le=1_000)) -> list[dict[str, object]]:
        return _records(paths().gold / "market_pulse.parquet", limit)

    @application.get("/v1/customer-segments")
    def customer_segments() -> list[dict[str, object]]:
        path = paths().gold / "customer_rfm.parquet"
        if not path.exists():
            raise HTTPException(status_code=404, detail="Customer table not found")
        frame = pd.read_parquet(path)
        summary = (
            frame.groupby("segment", as_index=False)
            .agg(customers=("customer_id", "nunique"), revenue=("monetary", "sum"))
            .sort_values("revenue", ascending=False)
        )
        return json.loads(summary.to_json(orient="records"))

    @application.get("/v1/live-status")
    def live_status() -> dict[str, object]:
        report = paths().reports / "live_refresh.json"
        return json.loads(report.read_text()) if report.exists() else {"mode": "batch"}

    @application.get("/v1/stock")
    def stock(limit: int = Query(100, ge=1, le=2000)) -> list[dict[str, object]]:
        path = stock_path(resolved_data_dir)
        if path is None:
            raise HTTPException(status_code=404, detail="No inventory projection available")
        return _records(path, limit)

    @application.get("/v1/daily-close/{business_date}")
    def daily_close(business_date: str) -> dict[str, object]:
        from datetime import date

        try:
            date.fromisoformat(business_date)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="Use a YYYY-MM-DD date") from exc
        path = resolved_data_dir / "closes" / business_date / "current.json"
        if not path.exists():
            raise HTTPException(status_code=404, detail="No successful close for this date")
        return json.loads(path.read_text())

    return application


app = create_app()
