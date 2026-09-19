"""Replay a Silver Parquet file and reconcile Spark output against batch arithmetic."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import uuid
from pathlib import Path

import pandas as pd
from confluent_kafka.admin import AdminClient, NewTopic

from merchmind.streaming import replay_transactions


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("data/silver/fact_transactions.parquet"))
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--evidence-dir", type=Path, default=Path("data/streaming-evidence"))
    args = parser.parse_args()
    topic = f"merchmind-demo-{uuid.uuid4().hex}"
    directory = (args.evidence_dir / topic).resolve()
    directory.mkdir(parents=True)
    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})
    admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])[topic].result(30)
    try:
        confirmed = replay_transactions(args.input, args.bootstrap_servers, topic, 0)
        launcher = Path(__file__).resolve().with_name("run_stream.py")
        with (directory / "spark.log").open("w") as log:
            subprocess.run(
                [
                    sys.executable,
                    str(launcher),
                    "--bootstrap-servers",
                    args.bootstrap_servers,
                    "--topic",
                    topic,
                    "--output",
                    str(directory / "output"),
                    "--checkpoint",
                    str(directory / "checkpoints"),
                    "--available-now",
                    "--progress-report",
                    str(directory / "progress.json"),
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
                timeout=300,
            )
        raw = pd.read_parquet(directory / "output/raw")
        actual = pd.read_parquet(directory / "output/aggregates")
        source = pd.read_parquet(args.input)
        assert len(raw) == len(source) == confirmed
        assert not raw.duplicated(["topic", "partition", "offset"]).any()
        assert sorted(raw.value.map(lambda value: json.loads(value)["transaction_id"])) == sorted(
            source.transaction_id
        )
        source["window_start"] = pd.to_datetime(source.transaction_ts, utc=True).dt.floor("5min")
        source["net_revenue"] = (
            source.quantity * source.unit_price * source.returned.map({True: -1, False: 1})
        )
        expected = source.groupby(["window_start", "sales_channel"], as_index=False).agg(
            transactions=("transaction_id", "count"),
            units=("quantity", "sum"),
            net_revenue=("net_revenue", "sum"),
        )
        progress = json.loads((directory / "progress.json").read_text())
        batches = progress["queries"]["merchmind-aggregates"]["progress"]
        watermark = pd.Timestamp(batches[-1]["eventTime"]["watermark"])
        expected = expected.loc[expected.window_start + pd.Timedelta(minutes=5) <= watermark]
        actual["window_start"] = pd.to_datetime(
            actual["window"].map(lambda value: value["start"]), utc=True
        )
        columns = ["window_start", "sales_channel", "transactions", "units", "net_revenue"]
        sort_columns = ["window_start", "sales_channel"]
        pd.testing.assert_frame_equal(
            actual[columns].sort_values(sort_columns).reset_index(drop=True),
            expected[columns].sort_values(sort_columns).reset_index(drop=True),
            check_dtype=False,
            check_exact=False,
            rtol=1e-10,
            atol=1e-8,
        )
        report = {
            "spark_version": progress["spark_version"],
            "topic": topic,
            "broker_confirmed_events": confirmed,
            "raw_events": len(raw),
            "partitions": int(raw.partition.nunique()),
            "finalized_windows": len(actual),
            "finalized_transactions": int(actual.transactions.sum()),
            "pending_transactions": int(len(source) - actual.transactions.sum()),
            "watermark": str(watermark),
            "batch_reconciliation_passed": True,
        }
        (directory / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
        print(f"Evidence: {directory}")
    finally:
        # Temporary demo-verification topic only; normal retail-transactions is untouched.
        admin.delete_topics([topic])[topic].result(30)


if __name__ == "__main__":
    main()
