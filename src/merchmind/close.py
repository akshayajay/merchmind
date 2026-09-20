"""Auditable UTC daily closes from Bronze plus committed Kafka envelopes.

Close attempts are immutable once published. A failed attempt never replaces the
last successful close. Backfills recompute event dates using all data known now.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pandas as pd

from merchmind.analytics import build_category_forecast, build_daily_category, enrich_transactions
from merchmind.config import DataPaths
from merchmind.live import TRANSACTION_COLUMNS, _atomic_json, refresh_from_stream, serving_paths
from merchmind.pipeline import _write_json
from merchmind.quality import validate_transactions


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text())


def _day(value: str) -> pd.Timestamp:
    return pd.Timestamp(date.fromisoformat(value), tz="UTC")


def _canonical(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame[TRANSACTION_COLUMNS].copy()
    frame["transaction_ts"] = pd.to_datetime(frame.transaction_ts, utc=True)
    return frame.sort_values("transaction_id").reset_index(drop=True)


def _totals(frame: pd.DataFrame) -> dict:
    # unit_price is already discounted. Returns are negative revenue.
    revenue = sum(
        (
            Decimal(str(row.unit_price)) * int(row.quantity) * (-1 if row.returned else 1)
            for row in frame.itertuples()
        ),
        Decimal(0),
    )
    return {
        "transactions": len(frame),
        "units": int(frame.quantity.sum()),
        "net_revenue": str(revenue.quantize(Decimal("0.01"))),
    }


def prepare_close(root: Path, raw_dir: Path, business_date: str) -> str:
    """Pin the serving revision and independently rebuild expected source records."""
    start = _day(business_date)
    end = start + pd.Timedelta(days=1)
    root, raw_dir = root.resolve(), raw_dir.resolve()
    refresh_from_stream(root, raw_dir)
    # The publisher uses this same lock. Capture its exact source cut, even if new
    # Spark batches arrive while this close is running.
    with (root / "live/refresh.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        serving = serving_paths(root)
        base = DataPaths(root)
        live_report = serving.reports / "live_refresh.json"
        live = _read_json(live_report) if live_report.exists() else {}
        if live and "committed_raw_files" not in live:
            raise ValueError("Refresh with the current publisher before closing a legacy snapshot")
        files = [raw_dir / name for name in live.get("committed_raw_files", [])]
        products = pd.read_parquet(base.silver / "dim_products.parquet")
        customers = pd.read_parquet(base.silver / "dim_customers.parquet")
        bronze = pd.read_parquet(base.bronze / "transactions.parquet")
        baseline = validate_transactions(bronze, products, customers)
        rows, malformed, envelope_count = [], 0, 0
        if files:
            envelopes = pd.concat([pd.read_parquet(p) for p in files], ignore_index=True)
            envelopes = envelopes.sort_values(["topic", "partition", "offset"]).drop_duplicates(
                ["topic", "partition", "offset"]
            )
            envelope_count = len(envelopes)
            for value in envelopes.value:
                try:
                    record = json.loads(value)
                    if not isinstance(record, dict):
                        raise ValueError("Expected an object")
                    rows.append({key: record.get(key) for key in TRANSACTION_COLUMNS})
                except (TypeError, ValueError):
                    malformed += 1
        stream = validate_transactions(
            pd.DataFrame(rows, columns=TRANSACTION_COLUMNS), products, customers
        )
        combined = pd.concat([baseline.clean, stream.clean], ignore_index=True)
        expected = combined.drop_duplicates("transaction_id", keep="first").copy()
        expected["transaction_ts"] = pd.to_datetime(expected.transaction_ts, utc=True)
        actual = pd.read_parquet(serving.silver / "fact_transactions.parquet")
        actual["transaction_ts"] = pd.to_datetime(actual.transaction_ts, utc=True)
        gold = pd.read_parquet(serving.gold / "daily_category_performance.parquet")
        gold_date = pd.to_datetime(gold.date, utc=True)
        attempt = root / "closes" / business_date / "attempts" / uuid.uuid4().hex
        attempt.mkdir(parents=True)
        expected.loc[expected.transaction_ts.between(start, end, inclusive="left")].to_parquet(
            attempt / "expected.parquet", index=False
        )
        actual.loc[actual.transaction_ts.between(start, end, inclusive="left")].to_parquet(
            attempt / "actual.parquet", index=False
        )
        gold.loc[gold_date.between(start, end, inclusive="left")].to_parquet(
            attempt / "actual_categories.parquet", index=False
        )
        # Forecasts may use earlier dates, never transactions after the close date.
        history = expected.loc[expected.transaction_ts < end]
        history.to_parquet(attempt / "history.parquet", index=False)
        products.to_parquet(attempt / "products.parquet", index=False)
        customers.to_parquet(attempt / "customers.parquet", index=False)
        source_count = len(bronze) + envelope_count
        quarantines = [baseline.quarantine, stream.quarantine]
        duplicates = sum(
            int(q.rejection_reason.eq("duplicate_transaction_id").sum()) for q in quarantines
        )
        rejected = malformed + sum(len(q) for q in quarantines) - duplicates
        quality = {
            "source_rows": source_count,
            "invalid_rows": rejected,
            "invalid_rate": rejected / source_count if source_count else 0.0,
            "duplicate_rows": duplicates + int(combined.duplicated("transaction_id").sum()),
            "malformed_json_rows": malformed,
            "scope": "entire pinned source history; exact duplicates excluded from invalid rate",
        }
        _write_json(quality, attempt / "quality.json")
        _write_json(
            {
                "business_date": business_date,
                "prepared_at_utc": datetime.now(UTC).isoformat(),
                "serving_revision": live.get("revision", "batch"),
                "committed_raw_files": [p.name for p in files],
                "stream_events": envelope_count,
                "semantics": "UTC event date, first valid business ID wins; all arrivals known now",
            },
            attempt / "manifest.json",
        )
    return str(attempt)


def check_quality(attempt: str, max_invalid_rate: float = 0.05) -> dict:
    if not 0 <= max_invalid_rate <= 1:
        raise ValueError("max_invalid_rate must be between 0 and 1")
    path = Path(attempt)
    report = _read_json(path / "quality.json")
    report.update(
        max_invalid_rate=max_invalid_rate,
        passed=report["source_rows"] > 0 and report["invalid_rate"] <= max_invalid_rate,
    )
    _write_json(report, path / "quality_check.json")
    if not report["passed"]:
        raise ValueError(f"Data quality gate failed: {report}")
    return report


def reconcile_close(attempt: str) -> dict:
    path = Path(attempt)
    expected, actual = [
        pd.read_parquet(path / f"{name}.parquet") for name in ("expected", "actual")
    ]
    gold = pd.read_parquet(path / "actual_categories.parquet")
    expected_totals, actual_totals = _totals(expected), _totals(actual)
    mismatch = None
    try:
        pd.testing.assert_frame_equal(
            _canonical(expected), _canonical(actual), check_dtype=False, check_exact=True
        )
        products = pd.read_parquet(path / "products.parquet")
        expected_categories = expected.merge(products[["product_id", "category"]], on="product_id")
        category_names = set(expected_categories.category) | set(gold.category)
        for category in category_names:
            totals = _totals(expected_categories.loc[expected_categories.category.eq(category)])
            observed = gold.loc[gold.category.eq(category)]
            if len(observed) != 1:
                raise AssertionError(f"Missing or duplicate Gold category: {category}")
            if int(observed.orders.sum()) != totals["transactions"]:
                raise AssertionError(f"Order count differs for {category}")
            if int(observed.units.sum()) != totals["units"]:
                raise AssertionError(f"Units differ for {category}")
            if not abs(float(observed.net_revenue.sum()) - float(totals["net_revenue"])) < 0.005:
                raise AssertionError(f"Revenue differs for {category}")
    except AssertionError as exc:
        mismatch = str(exc)[:2000]
    report = {
        **_read_json(path / "manifest.json"),
        "expected": expected_totals,
        "actual": actual_totals,
        "passed": mismatch is None and expected_totals == actual_totals,
        "record_or_category_mismatch": mismatch,
    }
    _write_json(report, path / "reconciliation.json")
    if not report["passed"]:
        raise ValueError(f"Daily reconciliation failed; evidence: {path / 'reconciliation.json'}")
    return report


def refresh_forecast(attempt: str) -> dict:
    path = Path(attempt)
    history, products, customers = [
        pd.read_parquet(path / f"{name}.parquet") for name in ("history", "products", "customers")
    ]
    if history.empty:
        forecast = pd.DataFrame()
    else:
        forecast = build_category_forecast(
            build_daily_category(enrich_transactions(history, products, customers))
        )
    forecast.to_parquet(path / "category_forecast.parquet", index=False)
    report = {"training_rows": len(history), "forecast_rows": len(forecast)}
    _write_json(report, path / "forecast.json")
    return report


def publish_close(attempt: str) -> dict:
    path = Path(attempt)
    quality = _read_json(path / "quality_check.json")
    reconciliation = _read_json(path / "reconciliation.json")
    forecast = _read_json(path / "forecast.json")
    if not quality["passed"] or not reconciliation["passed"]:
        raise ValueError("Cannot publish a failed close")
    day_dir = path.parent.parent
    with (day_dir / "publish.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        pointer = day_dir / "current.json"
        if pointer.exists():
            current = _read_json(pointer)
            if current["prepared_at_utc"] > reconciliation["prepared_at_utc"]:
                return current  # An older attempt must never overwrite a newer close.
        report = {
            **reconciliation,
            "quality": quality,
            "forecast": forecast,
            "attempt": path.name,
            "published_at_utc": datetime.now(UTC).isoformat(),
        }
        _atomic_json(report, pointer)
        return report


def run_close(
    root: Path, raw_dir: Path, business_date: str, max_invalid_rate: float = 0.05
) -> dict:
    attempt = prepare_close(root, raw_dir, business_date)
    check_quality(attempt, max_invalid_rate)
    reconcile_close(attempt)
    refresh_forecast(attempt)
    return publish_close(attempt)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--raw-dir", type=Path, required=True)
    parser.add_argument("--start-date", required=True, help="First UTC event date, inclusive")
    parser.add_argument("--end-date", help="Last UTC event date, inclusive")
    parser.add_argument("--max-invalid-rate", type=float, default=0.05)
    args = parser.parse_args()
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date or args.start_date)
    if end < start:
        parser.error("end-date must be on or after start-date")
    while start <= end:
        print(json.dumps(run_close(args.data_dir, args.raw_dir, str(start), args.max_invalid_rate)))
        start += timedelta(days=1)


if __name__ == "__main__":
    main()
