from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import pandas as pd


def replay_transactions(
    parquet_path: Path,
    bootstrap_servers: str,
    topic: str,
    events_per_second: float,
) -> int:
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise RuntimeError(
            "Install streaming dependencies with `pip install -e '.[streaming]'`"
        ) from exc

    producer = Producer({"bootstrap.servers": bootstrap_servers, "client.id": "merchmind-replay"})
    frame = pd.read_parquet(parquet_path).sort_values("transaction_ts")
    delay = 1 / events_per_second if events_per_second > 0 else 0
    produced = 0
    for record in frame.to_dict(orient="records"):
        record["transaction_ts"] = pd.Timestamp(record["transaction_ts"]).isoformat()
        producer.produce(
            topic,
            key=str(record["transaction_id"]),
            value=json.dumps(record, default=str).encode("utf-8"),
        )
        producer.poll(0)
        produced += 1
        if delay:
            time.sleep(delay)
    producer.flush()
    return produced


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay MERCHMIND transactions through Kafka")
    parser.add_argument("--input", type=Path, default=Path("data/silver/fact_transactions.parquet"))
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("MERCHMIND_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--topic", default=os.getenv("MERCHMIND_KAFKA_TOPIC", "retail-transactions")
    )
    parser.add_argument("--events-per-second", type=float, default=100)
    args = parser.parse_args()
    count = replay_transactions(
        args.input, args.bootstrap_servers, args.topic, args.events_per_second
    )
    print(f"Produced {count:,} events to {args.topic}")


if __name__ == "__main__":
    main()
