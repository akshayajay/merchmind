from __future__ import annotations

from fastapi.testclient import TestClient

from merchmind.api import create_app
from merchmind.pipeline import run_pipeline


def test_api_serves_gold_metrics(tmp_path) -> None:
    data_dir = tmp_path / "data"
    run_pipeline(data_dir, transactions=800, customers=100, products=35, seed=4)
    client = TestClient(create_app(data_dir))

    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["data_ready"] is True

    kpis = client.get("/v1/kpis")
    products = client.get("/v1/products", params={"limit": 7})
    segments = client.get("/v1/customer-segments")
    assert kpis.status_code == 200
    assert kpis.json()["orders"] > 0
    assert products.status_code == 200
    assert len(products.json()) == 7
    assert segments.status_code == 200
    assert sum(item["customers"] for item in segments.json()) > 0
