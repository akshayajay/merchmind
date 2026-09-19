from __future__ import annotations

import argparse
import json
import math
import os
import time
from pathlib import Path

import pandas as pd


def replay_transactions(
    parquet_path: Path,
    bootstrap_servers: str,
    topic: str,
    events_per_second: float,
    delivery_timeout: float = 30,
) -> int:
    """Replay Silver rows and return broker-confirmed deliveries, or raise on failure."""
    if not math.isfinite(events_per_second) or events_per_second < 0:
        raise ValueError("events_per_second must be finite and nonnegative")
    if not math.isfinite(delivery_timeout) or delivery_timeout <= 0:
        raise ValueError("delivery_timeout must be finite and positive")
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise RuntimeError(
            "Install streaming dependencies with `pip install -e '.[streaming]'`"
        ) from exc

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "merchmind-replay",
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": max(1, int(delivery_timeout * 1000)),
        }
    )
    frame = pd.read_parquet(parquet_path).sort_values("transaction_ts")
    delay = 1 / events_per_second if events_per_second > 0 else 0
    delivered = 0
    failures: list[str] = []

    def on_delivery(error, _message) -> None:
        nonlocal delivered
        if error is None:
            delivered += 1
        elif len(failures) < 5:
            failures.append(str(error))

    for record in frame.to_dict(orient="records"):
        record["transaction_ts"] = pd.Timestamp(record["transaction_ts"]).isoformat()
        deadline = time.monotonic() + delivery_timeout
        while True:
            try:
                producer.produce(
                    topic,
                    key=str(record["transaction_id"]),
                    value=json.dumps(record, default=str).encode("utf-8"),
                    on_delivery=on_delivery,
                )
                break
            except BufferError as exc:
                if time.monotonic() >= deadline:
                    raise RuntimeError("Kafka producer queue remained full") from exc
                producer.poll(0.1)
        producer.poll(0)
        if delay:
            time.sleep(delay)
    pending = producer.flush(delivery_timeout)
    if failures or pending or delivered != len(frame):
        raise RuntimeError(
            f"Kafka delivery incomplete: confirmed={delivered}/{len(frame)}, "
            f"pending={pending}, errors={failures}"
        )
    return delivered


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay MERCHMIND transactions through Kafka")
    parser.add_argument("--input", type=Path, default=Path("data/silver/fact_transactions.parquet"))
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("MERCHMIND_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    )
    parser.add_argument(
        "--topic", default=os.getenv("MERCHMIND_KAFKA_TOPIC", "retail-transactions")
    )
    parser.add_argument("--events-per-second", type=float, default=100)
    parser.add_argument("--delivery-timeout", type=float, default=30)
    args = parser.parse_args()
    count = replay_transactions(
        args.input,
        args.bootstrap_servers,
        args.topic,
        args.events_per_second,
        args.delivery_timeout,
    )
    print(f"Broker confirmed {count:,} events delivered to {args.topic}")


if __name__ == "__main__":
    main()
