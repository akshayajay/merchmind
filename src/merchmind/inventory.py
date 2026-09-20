"""Project stock from committed inventory events plus deduplicated retail transactions."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import time
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from merchmind.live import _atomic_json, committed_files, serving_paths
from merchmind.pipeline import _write_json


def project_inventory(
    events: pd.DataFrame,
    transactions: pd.DataFrame,
    products: pd.DataFrame,
    low_stock_threshold: int = 20,
) -> pd.DataFrame:
    """One opening balance per product; receipts/adjustments and returns add stock.

    Returns are assumed restockable. Unknown stock is omitted, never treated as zero.
    Conflicting inventory IDs fail the projection, preserving the previous snapshot.
    """
    if low_stock_threshold < 0:
        raise ValueError("low_stock_threshold must be nonnegative")
    fields = ["event_id", "event_ts", "product_id", "kind", "quantity_delta"]
    events = events[fields].copy()
    events["event_ts"] = pd.to_datetime(events.event_ts, utc=True, errors="raise", format="mixed")
    for field in ("event_id", "product_id"):
        if not events[field].map(lambda x: isinstance(x, str) and bool(x.strip())).all():
            raise ValueError(f"Invalid inventory {field}")
    if events.event_ts.isna().any() or not events.product_id.isin(products.product_id).all():
        raise ValueError("Invalid inventory timestamp or unknown product")

    def valid_quantity(value):
        return (
            not isinstance(value, bool)
            and isinstance(value, (int, float))
            and math.isfinite(value)
            and value == int(value)
        )

    if not events.quantity_delta.map(valid_quantity).all():
        raise ValueError("Inventory quantities must be finite integers")
    if not events.kind.isin(["opening", "receipt", "adjustment"]).all():
        raise ValueError("Unknown inventory event kind")
    if (events.loc[events.kind.isin(["opening", "receipt"]), "quantity_delta"] < 0).any():
        raise ValueError("Opening balances and receipts must be nonnegative")
    unique_payloads = events.drop_duplicates(fields)
    if unique_payloads.event_id.duplicated().any():
        raise ValueError("Conflicting inventory event ID")
    events = unique_payloads
    transactions = transactions.copy()
    transactions["transaction_ts"] = pd.to_datetime(transactions.transaction_ts, utc=True)
    if transactions.transaction_id.duplicated().any():
        raise ValueError("Inventory requires deduplicated transactions")
    records = []
    for product_id, changes in events.groupby("product_id"):
        opening = changes.loc[changes.kind.eq("opening")]
        if len(opening) != 1:
            raise ValueError(f"Expected one opening balance for {product_id}")
        start = opening.event_ts.iloc[0]
        if (changes.event_ts < start).any():
            raise ValueError("Inventory event precedes opening balance")
        sales = transactions.loc[
            transactions.product_id.eq(product_id) & transactions.transaction_ts.ge(start)
        ]
        sold = int(sales.loc[~sales.returned, "quantity"].sum())
        returned = int(sales.loc[sales.returned, "quantity"].sum())
        on_hand = int(changes.quantity_delta.sum()) - sold + returned
        records.append(
            {
                "product_id": product_id,
                "on_hand": on_hand,
                "sold_units": sold,
                "restocked_return_units": returned,
                "low_stock": on_hand <= low_stock_threshold,
                "stock_status": "Oversold"
                if on_hand < 0
                else "Low"
                if on_hand <= low_stock_threshold
                else "Available",
            }
        )
    return (
        pd.DataFrame(records)
        .merge(
            products[["product_id", "product_name", "category"]],
            on="product_id",
            validate="one_to_one",
        )
        .sort_values(["on_hand", "product_id"])
        .reset_index(drop=True)
    )


def refresh_inventory(root: Path, raw_dir: Path, low_stock_threshold: int = 20) -> dict | None:
    inventory = root / "inventory"
    inventory.mkdir(parents=True, exist_ok=True)
    with (inventory / "refresh.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        return _refresh_inventory(root, raw_dir, low_stock_threshold)


def _refresh_inventory(root: Path, raw_dir: Path, low_stock_threshold: int) -> dict | None:
    files = committed_files(raw_dir)
    if not files:
        return None
    source = serving_paths(root)
    transaction_file = source.silver / "fact_transactions.parquet"
    product_file = root / "silver/dim_products.parquet"
    fingerprint = [
        (str(p), p.stat().st_size, p.stat().st_mtime_ns)
        for p in [*files, transaction_file, product_file]
    ]
    revision = hashlib.sha256(json.dumps([fingerprint, low_stock_threshold]).encode()).hexdigest()
    pointer = root / "inventory/current.json"
    if pointer.exists() and json.loads(pointer.read_text())["snapshot"] == revision:
        return None
    raw = pd.concat([pd.read_parquet(p) for p in files], ignore_index=True)
    raw = raw.sort_values(["topic", "partition", "offset"]).drop_duplicates(
        ["topic", "partition", "offset"]
    )
    events = pd.DataFrame([json.loads(value) for value in raw.value])
    transactions = pd.read_parquet(transaction_file)
    products = pd.read_parquet(product_file)
    projected = project_inventory(events, transactions, products, low_stock_threshold)
    inventory = root / "inventory"
    snapshot = inventory / "snapshots" / revision
    snapshot.mkdir(parents=True, exist_ok=True)
    projected.to_parquet(snapshot / "stock.parquet", index=False)
    report = {
        "snapshot": snapshot.name,
        "products": len(projected),
        "low_stock_products": int(projected.low_stock.sum()),
        "inventory_events": len(raw),
        "serving_revision": source.root.name,
        "low_stock_threshold": low_stock_threshold,
        "published_at_utc": datetime.now(UTC).isoformat(),
        "return_policy": "All returned units are assumed restockable",
    }
    _write_json(report, snapshot / "report.json")
    _atomic_json(report, inventory / "current.json")
    return report


def stock_path(root: Path) -> Path | None:
    pointer = root / "inventory/current.json"
    if not pointer.exists():
        return None
    revision = json.loads(pointer.read_text())["snapshot"]
    if len(revision) != 64 or any(c not in "0123456789abcdef" for c in revision):
        raise ValueError("Invalid inventory snapshot")
    return root / "inventory/snapshots" / revision / "stock.parquet"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--raw-dir", type=Path)
    parser.add_argument("--seed-demo", action="store_true")
    parser.add_argument("--watch", action="store_true")
    args = parser.parse_args()
    if args.seed_demo:
        products = pd.read_parquet(args.data_dir / "silver/dim_products.parquet")
        opening = pd.DataFrame(
            {
                "event_id": "opening-" + products.product_id,
                "product_id": products.product_id,
                "event_ts": "2024-01-01T00:00:00Z",
                "kind": "opening",
                "quantity_delta": 500,
            }
        )
        destination = args.data_dir / "bronze/inventory_events.parquet"
        opening.to_parquet(destination, index=False)
        print(destination)
        return
    if args.raw_dir is None:
        parser.error("--raw-dir is required unless seeding the demo")
    while True:
        report = refresh_inventory(args.data_dir, args.raw_dir)
        if report:
            print(json.dumps(report), flush=True)
        if not args.watch:
            return
        time.sleep(5)


if __name__ == "__main__":
    main()
