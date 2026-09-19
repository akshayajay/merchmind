"""Container-side operations for the isolated resilience lab."""

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from merchmind.live import committed_files

BROKERS = "broker-a:9092,broker-b:9092,broker-c:9092"
TOPIC = "resilience-transactions"
ROOT = Path("/app/evidence")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["create", "produce", "verify"])
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--count", type=int, default=10000)
    parser.add_argument("--marker", action="store_true")
    args = parser.parse_args()
    admin = AdminClient({"bootstrap.servers": BROKERS})
    if args.action == "create":
        admin.create_topics([NewTopic(TOPIC, 3, 3, config={"min.insync.replicas": "2"})])[
            TOPIC
        ].result(60)
        metadata = admin.list_topics(TOPIC, timeout=30)
        partitions = metadata.topics[TOPIC].partitions
        assert all(len(part.replicas) == 3 for part in partitions.values())
        # Kill an actual partition leader, not an arbitrary idle node.
        leader = next(iter(partitions.values())).leader
        (ROOT / "leader.json").write_text(json.dumps({"leader": leader}))
    elif args.action == "produce":
        errors, offsets = [], []
        producer = Producer(
            {
                "bootstrap.servers": BROKERS,
                "acks": "all",
                "enable.idempotence": True,
                "delivery.timeout.ms": 90000,
            }
        )

        def delivered(error, message):
            errors.append(str(error)) if error else offsets.append(
                (message.partition(), message.offset())
            )

        for i in range(args.start, args.start + args.count):
            instant = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=i)
            if args.marker:
                instant += timedelta(minutes=20)
            event = {
                "transaction_id": f"load-{i}",
                "transaction_ts": instant.isoformat(),
                "customer_id": "C1",
                "product_id": "P1",
                "quantity": 2,
                "unit_price": 10.0,
                "discount_pct": 0,
                "returned": False,
                "sales_channel": "Online",
            }
            producer.produce(TOPIC, partition=i % 3, value=json.dumps(event), on_delivery=delivered)
            producer.poll(0)
        assert producer.flush(100) == 0
        assert not errors, errors
        assert len(offsets) == args.count
        (ROOT / f"delivery-{args.start}.json").write_text(
            json.dumps(
                {
                    "start": args.start,
                    "acknowledged": len(offsets),
                    "partitions": sorted({part for part, _ in offsets}),
                },
                indent=2,
            )
        )
    else:
        raw = pd.concat([pd.read_parquet(p) for p in committed_files(ROOT / "output/raw")])
        aggregates = pd.concat(
            [pd.read_parquet(p) for p in committed_files(ROOT / "output/aggregates")]
        )
        assert len(raw) == 30001
        assert not raw.duplicated(["topic", "partition", "offset"]).any()
        ids = raw.value.map(lambda value: json.loads(value)["transaction_id"])
        assert set(ids) == {f"load-{i}" for i in range(30001)}
        assert aggregates.transactions.sum() == 30000
        assert aggregates.net_revenue.sum() == 600000
        report = json.loads((ROOT / "degraded-progress.json").read_text())
        assert report["master"] == "spark://spark-master:7077"
        workers = [item for item in report["executors"] if item["id"] != "driver"]
        assert len(workers) == 2, workers
        assert all(item["completed_tasks"] > 0 for item in workers), workers
        result = {
            "raw_events": len(raw),
            "finalized_transactions": 30000,
            "net_revenue": 600000,
            "replicas": 3,
            "min_in_sync_replicas": 2,
            "brokers": 3,
            "worker_executors": len(workers),
            "completed_tasks_per_worker": [item["completed_tasks"] for item in workers],
            "acknowledged_while_leader_down": 10000,
            "scope": "One local Docker host; not a production capacity benchmark",
            "checks_passed": True,
        }
        (ROOT / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
