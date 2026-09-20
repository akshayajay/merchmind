"""Publish consistent serving snapshots from Spark's committed raw event files."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

import pandas as pd

from merchmind.config import DataPaths, default_data_dir
from merchmind.pipeline import _write_json, build_gold
from merchmind.quality import validate_transactions

TRANSACTION_COLUMNS = [
    "transaction_id",
    "transaction_ts",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount_pct",
    "sales_channel",
    "returned",
]


def serving_paths(root: Path) -> DataPaths:
    pointer = root / "live/current.json"
    if not pointer.exists():
        return DataPaths(root)
    revision = json.loads(pointer.read_text())["revision"]
    if len(revision) != 64 or any(c not in "0123456789abcdef" for c in revision):
        raise ValueError("Invalid serving revision")
    return DataPaths(root / "live/snapshots" / revision)


def committed_files(raw_dir: Path) -> list[Path]:
    """Honor Spark's file-sink commit log, including compaction and delete records.

    Only flat Parquet sinks are supported. Relocate filenames across Docker/host mounts;
    never glob Parquet, because a crash can leave files that Spark did not commit.
    """
    metadata = raw_dir / "_spark_metadata"
    logs = []
    for path in metadata.glob("*"):
        number = path.name.removesuffix(".compact")
        if number.isdigit():
            logs.append((int(number), path.name.endswith(".compact"), path))
    compact = max((number for number, is_compact, _ in logs if is_compact), default=-1)
    active = {}
    for number, is_compact, path in sorted(logs):
        if number < compact or (number == compact and not is_compact):
            continue
        lines = path.read_text().splitlines()
        if not lines or lines[0] != "v1":
            raise ValueError(f"Unsupported Spark commit log: {path}")
        for line in lines[1:]:
            record = json.loads(line)
            filename = Path(unquote(urlparse(record["path"]).path)).name
            if not filename.endswith(".parquet"):
                raise ValueError("Expected a flat Parquet file sink")
            if record["action"] == "add":
                active[filename] = raw_dir / filename
            elif record["action"] == "delete":
                active.pop(filename, None)
            else:
                raise ValueError("Unsupported Spark file action")
    return sorted(active.values())


def _atomic_json(payload: dict, destination: Path) -> None:
    temporary = destination.with_name(f".{destination.name}-{uuid.uuid4().hex}")
    _write_json(payload, temporary)
    os.replace(temporary, destination)


def refresh_from_stream(root: Path, raw_dir: Path) -> dict | None:
    root, raw_dir = root.resolve(), raw_dir.resolve()
    live = root / "live"
    live.mkdir(parents=True, exist_ok=True)
    # One publisher at a time; readers never acquire this lock.
    with (live / "refresh.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        files = committed_files(raw_dir)
        if not files:
            return None
        base = DataPaths(root)
        base_files = [
            base.silver / f"{name}.parquet"
            for name in (
                "fact_transactions",
                "dim_products",
                "dim_customers",
                "company_financials",
                "macro_indicators",
            )
        ]
        # Airflow and the publisher mount the same volumes at different paths.
        # Identify files by role/name so identical inputs share one revision.
        fingerprint = [
            (role, p.name, p.stat().st_size, p.stat().st_mtime_ns)
            for role, paths in (("raw", files), ("baseline", base_files))
            for p in paths
        ]
        revision = hashlib.sha256(json.dumps([3, fingerprint]).encode()).hexdigest()
        current = live / "current.json"
        if current.exists() and json.loads(current.read_text())["revision"] == revision:
            return None
        snapshots = live / "snapshots"
        snapshots.mkdir(exist_ok=True)
        destination = snapshots / revision
        if destination.exists():
            _atomic_json({"revision": revision}, current)
            return json.loads((destination / "reports/live_refresh.json").read_text())
        paths = DataPaths(snapshots / f".pending-{uuid.uuid4().hex}").create()
        raw = pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)
        raw = raw.sort_values(["topic", "partition", "offset"]).drop_duplicates(
            ["topic", "partition", "offset"]
        )
        rows, invalid = [], []
        for envelope in raw.to_dict("records"):
            try:
                record = json.loads(envelope["value"])
                if not isinstance(record, dict):
                    raise ValueError("not an object")
                rows.append({name: record.get(name) for name in TRANSACTION_COLUMNS})
            except (TypeError, ValueError):
                invalid.append({**envelope, "rejection_reason": "malformed_json"})
        incoming = pd.DataFrame(rows, columns=TRANSACTION_COLUMNS)
        baseline = pd.read_parquet(base_files[0])
        products, customers, company, macro = [pd.read_parquet(path) for path in base_files[1:]]
        # Validate incoming events before ID deduplication so bad events remain auditable.
        validation = validate_transactions(incoming, products, customers)
        combined = pd.concat([baseline, validation.clean], ignore_index=True)
        duplicate_ids = int(combined.duplicated("transaction_id").sum())
        clean = combined.drop_duplicates("transaction_id", keep="first").copy()
        clean["transaction_ts"] = pd.to_datetime(clean.transaction_ts, utc=True)
        clean.to_parquet(paths.silver / "fact_transactions.parquet", index=False)
        # Rejected fields may contain mixed types that Arrow cannot store together.
        # Raw Kafka values above remain the authoritative, unmodified audit record.
        validation.quarantine.astype("string").to_parquet(
            paths.reports / "quarantined_transactions.parquet", index=False
        )
        pd.DataFrame(invalid).to_parquet(paths.reports / "malformed_events.parquet", index=False)
        quality = {**validation.report, "pass_rate": validation.report["pass_rate"]}
        build_gold(paths, clean, products, customers, company, macro, quality)
        report = {
            "revision": revision,
            "published_at_utc": datetime.now(UTC).isoformat(),
            "committed_raw_events": len(raw),
            "baseline_transactions": len(baseline),
            "served_transactions": len(clean),
            "new_transactions": len(clean) - len(baseline),
            "duplicate_ids_ignored": duplicate_ids,
            "quarantined_events": len(validation.quarantine) + len(invalid),
            "stream_valid_events": len(validation.clean),
            "committed_raw_files": [path.name for path in files],
        }
        # The displayed quality rate refers to all received stream events, including bad JSON.
        report["stream_pass_rate"] = len(validation.clean) / len(raw) if len(raw) else 1.0
        kpis = json.loads((paths.gold / "executive_kpis.json").read_text())
        kpis["data_quality_pass_rate"] = report["stream_pass_rate"]
        _write_json(kpis, paths.gold / "executive_kpis.json")
        _write_json(report, paths.reports / "live_refresh.json")
        os.replace(paths.root, destination)
        _atomic_json({"revision": revision}, current)
        return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=default_data_dir())
    parser.add_argument("--raw-dir", type=Path, required=True)
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=float, default=5)
    args = parser.parse_args()
    if args.interval <= 0:
        parser.error("interval must be positive")
    while True:
        result = refresh_from_stream(args.data_dir, args.raw_dir)
        if result:
            print(json.dumps(result), flush=True)
        if not args.watch:
            return
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
