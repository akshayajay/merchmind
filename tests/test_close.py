import json

import pandas as pd
import pytest

from merchmind.close import (
    check_quality,
    prepare_close,
    publish_close,
    reconcile_close,
    refresh_forecast,
    run_close,
)
from merchmind.live import serving_paths
from merchmind.pipeline import run_pipeline
from merchmind.quality import validate_transactions


def commit(raw, batch, records):
    raw.mkdir(exist_ok=True)
    (raw / "_spark_metadata").mkdir(exist_ok=True)
    filename = f"part-{batch}.parquet"
    pd.DataFrame(
        [
            {
                "topic": "close-test",
                "partition": 0,
                "offset": batch * 100 + i,
                "value": json.dumps(row) if isinstance(row, dict) else row,
            }
            for i, row in enumerate(records)
        ]
    ).to_parquet(raw / filename, index=False)
    (raw / "_spark_metadata" / str(batch)).write_text(
        "v1\n" + json.dumps({"path": f"file:///raw/{filename}", "action": "add"}) + "\n"
    )


@pytest.fixture
def retail(tmp_path):
    root, raw = tmp_path / "data", tmp_path / "raw"
    run_pipeline(root, transactions=1000, customers=100, products=30)
    row = pd.read_parquet(root / "silver/fact_transactions.parquet").iloc[0].to_dict()
    row.update(
        transaction_id="new",
        transaction_ts="2026-01-01T12:00:00+00:00",
        quantity=2,
        unit_price=10.0,
        returned=False,
    )
    return root, raw, row


def test_daily_close_duplicates_returns_late_events_and_backfill(retail):
    root, raw, row = retail
    commit(raw, 0, [row, row, {**row, "transaction_id": "return", "returned": True, "quantity": 1}])
    first = run_close(root, raw, "2026-01-01")
    assert first["expected"] == {"transactions": 2, "units": 3, "net_revenue": "10.00"}
    assert first["quality"]["duplicate_rows"] >= 1
    assert run_close(root, raw, "2026-01-01")["actual"] == first["actual"]
    commit(raw, 1, [row, {**row, "transaction_id": "late", "unit_price": 7.5}])
    later = run_close(root, raw, "2026-01-01")
    assert later["expected"] == {"transactions": 3, "units": 5, "net_revenue": "25.00"}
    assert run_close(root, raw, "2026-01-02")["actual"]["transactions"] == 0


def test_failed_publish_preserves_previous_close_and_retries(retail, monkeypatch):
    root, raw, row = retail
    commit(raw, 0, [row])
    run_close(root, raw, "2026-01-01")
    pointer = root / "closes/2026-01-01/current.json"
    before = pointer.read_bytes()
    commit(raw, 1, [{**row, "transaction_id": "late"}])
    with monkeypatch.context() as patch:
        patch.setattr(
            "merchmind.close._atomic_json", lambda *a: (_ for _ in ()).throw(OSError("crash"))
        )
        with pytest.raises(OSError, match="crash"):
            run_close(root, raw, "2026-01-01")
    assert pointer.read_bytes() == before
    assert run_close(root, raw, "2026-01-01")["actual"]["transactions"] == 2


def test_detects_serving_and_gold_corruption(retail):
    root, raw, row = retail
    commit(raw, 0, [row])
    attempt = prepare_close(root, raw, "2026-01-01")
    serving = serving_paths(root)
    path = serving.silver / "fact_transactions.parquet"
    frame = pd.read_parquet(path)
    frame.loc[frame.transaction_id.eq("new"), "quantity"] = 9
    frame.to_parquet(path, index=False)
    with pytest.raises(ValueError, match="reconciliation failed"):
        reconcile_close(prepare_close(root, raw, "2026-01-01"))
    # A previously pinned attempt is unaffected by later mutations.
    assert reconcile_close(attempt)["passed"]
    gold = serving.gold / "daily_category_performance.parquet"
    frame = pd.read_parquet(gold)
    frame["net_revenue"] += 1
    frame.to_parquet(gold, index=False)
    # Restore Silver so this failure specifically exercises the Gold comparison.
    import pathlib

    saved = pd.read_parquet(pathlib.Path(attempt) / "actual.parquet")
    frame = pd.read_parquet(path)
    frame.loc[frame.transaction_id.eq("new"), "quantity"] = saved.iloc[0].quantity
    frame.to_parquet(path, index=False)
    with pytest.raises(ValueError, match="reconciliation failed"):
        reconcile_close(prepare_close(root, raw, "2026-01-01"))


def test_quality_gate_and_invalid_first_arrival(retail):
    root, raw, row = retail
    commit(raw, 0, [{**row, "quantity": -1}, row, "bad-json"])
    attempt = prepare_close(root, raw, "2026-01-01")
    with pytest.raises(ValueError, match="quality gate"):
        check_quality(attempt, max_invalid_rate=0)
    assert reconcile_close(attempt)["expected"]["transactions"] == 1
    refresh_forecast(attempt)
    with pytest.raises(ValueError, match="failed close"):
        publish_close(attempt)
    assert not (root / "closes/2026-01-01/current.json").exists()


def test_source_cut_empty_dates_and_no_forecast_future_leakage(retail):
    root, raw, row = retail
    commit(raw, 0, [row])
    attempt = prepare_close(root, raw, "2023-12-31")
    commit(raw, 1, [{**row, "transaction_id": "later"}])
    assert reconcile_close(attempt)["stream_events"] == 1
    assert refresh_forecast(attempt) == {"training_rows": 0, "forecast_rows": 0}
    check_quality(attempt)
    assert publish_close(attempt)["actual"]["transactions"] == 0
    with pytest.raises(ValueError):
        prepare_close(root, raw, "../../oops")


def test_invalid_event_does_not_reserve_business_id(retail):
    root, _, row = retail
    result = validate_transactions(
        pd.DataFrame([{**row, "quantity": 0}, row, row]),
        pd.read_parquet(root / "silver/dim_products.parquet"),
        pd.read_parquet(root / "silver/dim_customers.parquet"),
    )
    assert len(result.clean) == 1
    assert result.report["duplicate_rows"] == 1


def test_older_close_cannot_replace_a_newer_success(retail):
    root, raw, row = retail
    commit(raw, 0, [row])
    old = prepare_close(root, raw, "2026-01-01")
    check_quality(old)
    reconcile_close(old)
    refresh_forecast(old)
    commit(raw, 1, [{**row, "transaction_id": "late"}])
    new = run_close(root, raw, "2026-01-01")
    assert publish_close(old)["attempt"] == new["attempt"]
