"""Real broker -> Spark -> Parquet test, including a checkpointed restart."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pandas as pd
import pytest

from merchmind.streaming import replay_transactions

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_KAFKA_INTEGRATION") != "1",
        reason="Set RUN_KAFKA_INTEGRATION=1 with a running broker and Spark 4.0.1",
    ),
]
ROOT = Path(__file__).resolve().parents[2]


def transaction(identifier, minute, *, channel="Online", quantity=1, price=10.0, returned=False):
    return {
        "transaction_id": identifier,
        "transaction_ts": f"2026-01-01T12:{minute:02d}:00+00:00",
        "customer_id": "C1",
        "product_id": "P1",
        "quantity": quantity,
        "unit_price": price,
        "discount_pct": 0.0,
        "sales_channel": channel,
        "returned": returned,
    }


def run_spark(broker: str, topic: str, directory: Path, stage: str) -> dict:
    report = directory / f"{stage}-progress.json"
    command = [
        sys.executable,
        str(ROOT / "scripts/run_stream.py"),
        "--bootstrap-servers",
        broker,
        "--topic",
        topic,
        "--output",
        str(directory / "output"),
        "--checkpoint",
        str(directory / "checkpoints"),
        "--available-now",
        "--progress-report",
        str(report),
    ]
    log = directory / f"{stage}-spark.log"
    with log.open("w") as stream:
        result = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT, timeout=240)
    assert result.returncode == 0, log.read_text()[-12000:]
    return json.loads(report.read_text())


def input_rows(report: dict, query: str) -> int:
    return sum(batch["numInputRows"] for batch in report["queries"][query]["progress"])


def test_kafka_spark_delivery_aggregates_and_checkpoint_restart(tmp_path):
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    broker = os.getenv("MERCHMIND_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    topic = f"merchmind-test-{uuid.uuid4().hex}"
    directory = Path(os.getenv("STREAMING_EVIDENCE_DIR", str(tmp_path))) / topic
    directory.mkdir(parents=True)
    admin = AdminClient({"bootstrap.servers": broker})
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result(30)
    try:
        initial = [
            transaction("sale", 0, quantity=2),
            transaction("return", 1, returned=True),
            transaction("store", 2, channel="Store", quantity=3, price=5.0),
            transaction("advance", 20, price=1.0),
        ]
        parquet = directory / "input.parquet"
        pd.DataFrame(initial).to_parquet(parquet, index=False)
        assert replay_transactions(parquet, broker, topic, 0) == 4

        # Malformed JSON, invalid quantity and timestamp must be auditable but not aggregated.
        producer = Producer({"bootstrap.servers": broker, "acks": "all"})
        errors = []
        delivered = []

        def confirm(error, message):
            errors.append(error) if error else delivered.append(message.offset())

        invalid_timestamp = transaction("invalid-time", 3, quantity=1000)
        invalid_timestamp["transaction_ts"] = "not-a-timestamp"
        bad_messages = [
            "not-json",
            json.dumps(transaction("invalid-quantity", 2, quantity=0)),
            json.dumps(invalid_timestamp),
        ]
        for value in bad_messages:
            producer.produce(topic, value=value.encode(), on_delivery=confirm)
        assert producer.flush(30) == 0
        assert not errors
        assert len(delivered) == 3

        first = run_spark(broker, topic, directory, "initial")
        raw = pd.read_parquet(directory / "output/raw")
        aggregates = pd.read_parquet(directory / "output/aggregates")
        assert len(raw) == 7
        assert set(raw["offset"]) == set(range(7))
        assert len(aggregates) == 2
        online = aggregates.loc[aggregates.sales_channel.eq("Online")].iloc[0]
        store = aggregates.loc[aggregates.sales_channel.eq("Store")].iloc[0]
        assert (online.transactions, online.units, online.net_revenue) == (2, 3, 10.0)
        assert (store.transactions, store.units, store.net_revenue) == (1, 3, 15.0)
        assert pd.Timestamp(online["window"]["start"]) == pd.Timestamp("2026-01-01T12:00:00")
        assert input_rows(first, "merchmind-raw") == 7
        assert input_rows(first, "merchmind-aggregates") == 7

        # A too-late event stays in raw but cannot change the already finalized window.
        # A later valid event advances the watermark to close the 12:20 window.
        more = [
            transaction("too-late", 1, quantity=1000, price=999.0),
            transaction("advance-again", 40, price=2.0),
        ]
        pd.DataFrame(more).to_parquet(parquet, index=False)
        assert replay_transactions(parquet, broker, topic, 0) == 2
        second = run_spark(broker, topic, directory, "restart")
        raw = pd.read_parquet(directory / "output/raw")
        aggregates = pd.read_parquet(directory / "output/aggregates")
        assert len(raw) == 9
        assert not raw.duplicated(["topic", "partition", "offset"]).any()
        assert len(aggregates) == 3
        assert aggregates.transactions.sum() == 4
        assert aggregates.net_revenue.sum() == pytest.approx(26.0)
        assert input_rows(second, "merchmind-raw") == 2
        for name in first["queries"]:
            assert first["queries"][name]["id"] == second["queries"][name]["id"]
        dropped = sum(
            operator.get("numRowsDroppedByWatermark", 0)
            for batch in second["queries"]["merchmind-aggregates"]["progress"]
            for operator in batch.get("stateOperators", [])
        )
        assert dropped >= 1

        third = run_spark(broker, topic, directory, "idle-restart")
        assert input_rows(third, "merchmind-raw") == 0
        assert input_rows(third, "merchmind-aggregates") == 0
        assert len(pd.read_parquet(directory / "output/raw")) == 9
        assert len(pd.read_parquet(directory / "output/aggregates")) == 3
        for name in first["queries"]:
            assert second["queries"][name]["id"] == third["queries"][name]["id"]
        evidence = {
            "spark_version": first["spark_version"],
            "topic": topic,
            "broker_confirmed_messages": 9,
            "raw_rows": 9,
            "finalized_aggregate_rows": 3,
            "finalized_net_revenue": 26.0,
            "malformed_or_invalid_messages": 3,
            "late_rows_dropped_from_aggregates": dropped,
            "checkpoint_restart_new_rows": 2,
            "idle_restart_new_rows": 0,
            "checks_passed": True,
        }
        (directory / "summary.json").write_text(json.dumps(evidence, indent=2) + "\n")
        print(json.dumps(evidence, indent=2))
    finally:
        # Only remove this test's unique topic, never the user's demo topic.
        admin.delete_topics([topic])[topic].result(30)
