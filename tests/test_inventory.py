import json

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from merchmind.api import create_app
from merchmind.inventory import project_inventory, refresh_inventory, stock_path
from merchmind.live import refresh_from_stream
from merchmind.pipeline import run_pipeline


def commit(raw, batch, values):
    raw.mkdir(exist_ok=True)
    (raw / "_spark_metadata").mkdir(exist_ok=True)
    filename = f"part-{batch}.parquet"
    pd.DataFrame(
        [
            {
                "topic": "inventory",
                "partition": 0,
                "offset": batch * 100 + i,
                "value": json.dumps(value),
            }
            for i, value in enumerate(values)
        ]
    ).to_parquet(raw / filename, index=False)
    (raw / "_spark_metadata" / str(batch)).write_text(
        "v1\n" + json.dumps({"path": f"file:///raw/{filename}", "action": "add"}) + "\n"
    )


def test_inventory_kafka_envelopes_sales_returns_duplicates_and_api(tmp_path):
    root, raw, sales_raw = tmp_path / "data", tmp_path / "raw", tmp_path / "sales"
    run_pipeline(root, transactions=1000, products=30, customers=100)
    row = pd.read_parquet(root / "silver/fact_transactions.parquet").iloc[0].to_dict()
    opening = {
        "event_id": "opening",
        "product_id": row["product_id"],
        "event_ts": "2026-01-01T00:00:00Z",
        "kind": "opening",
        "quantity_delta": 10,
    }
    commit(
        raw,
        0,
        [
            opening,
            opening,
            {**opening, "event_id": "delivery", "kind": "receipt", "quantity_delta": 5},
        ],
    )
    sale = {
        **row,
        "transaction_id": "new-sale",
        "transaction_ts": "2026-01-01T12:00:00Z",
        "quantity": 3,
        "returned": False,
    }
    commit(
        sales_raw,
        0,
        [sale, {**sale, "transaction_id": "new-return", "quantity": 1, "returned": True}],
    )
    refresh_from_stream(root, sales_raw)
    report = refresh_inventory(root, raw)
    assert report["products"] == 1
    assert report["low_stock_products"] == 1
    assert refresh_inventory(root, raw) is None
    stock = pd.read_parquet(stock_path(root))
    assert stock.iloc[0].on_hand == 13
    client = TestClient(create_app(root))
    assert client.get("/v1/stock").json()[0]["on_hand"] == 13
    prior = (root / "inventory/current.json").read_bytes()
    commit(raw, 1, [{**opening, "quantity_delta": 999}])
    with pytest.raises(ValueError, match="Conflicting"):
        refresh_inventory(root, raw)
    assert (root / "inventory/current.json").read_bytes() == prior


@pytest.mark.parametrize(
    "change",
    [
        {"quantity_delta": float("inf")},
        {"quantity_delta": True},
        {"quantity_delta": 1.5},
        {"kind": "unknown"},
        {"event_ts": None},
        {"product_id": "unknown"},
        {"quantity_delta": -1},
        {"event_id": ""},
    ],
)
def test_invalid_inventory_rejected(change):
    products = pd.DataFrame([{"product_id": "P1", "product_name": "Product", "category": "Tops"}])
    sales = pd.DataFrame(
        columns=["transaction_id", "transaction_ts", "product_id", "returned", "quantity"]
    )
    event = {
        "event_id": "e1",
        "event_ts": "2026-01-01",
        "product_id": "P1",
        "kind": "opening",
        "quantity_delta": 10,
        **change,
    }
    with pytest.raises((ValueError, TypeError)):
        project_inventory(pd.DataFrame([event]), sales, products)
