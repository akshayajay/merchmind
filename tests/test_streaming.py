from __future__ import annotations

import sys
from types import SimpleNamespace

import pandas as pd
import pytest

from merchmind.streaming import replay_transactions


@pytest.mark.parametrize("outcome", ["delivered", "failed", "pending", "backpressure"])
def test_replay_reports_confirmed_delivery_and_handles_backpressure(tmp_path, monkeypatch, outcome):
    class Producer:
        def __init__(self, config):
            assert config["enable.idempotence"] is True
            assert config["acks"] == "all"
            self.callback = None
            self.attempts = 0

        def produce(self, topic, *, key, value, on_delivery):
            self.attempts += 1
            if outcome == "backpressure" and self.attempts == 1:
                raise BufferError("queue full")
            self.callback = on_delivery

        def poll(self, timeout):
            return 0

        def flush(self, timeout):
            if outcome == "pending":
                return 1
            self.callback("broker rejected message" if outcome == "failed" else None, None)
            return 0

    monkeypatch.setitem(sys.modules, "confluent_kafka", SimpleNamespace(Producer=Producer))
    parquet = tmp_path / "transactions.parquet"
    pd.DataFrame([{"transaction_id": "T1", "transaction_ts": "2026-01-01"}]).to_parquet(parquet)
    if outcome in ("failed", "pending"):
        with pytest.raises(RuntimeError, match="Kafka delivery incomplete"):
            replay_transactions(parquet, "localhost:9092", "test", 0)
    else:
        assert replay_transactions(parquet, "localhost:9092", "test", 0) == 1


@pytest.mark.parametrize("rate, timeout", [(-1, 30), (float("nan"), 30), (0, 0)])
def test_replay_rejects_invalid_rate_or_timeout(tmp_path, rate, timeout):
    with pytest.raises(ValueError):
        replay_transactions(tmp_path / "unused", "localhost:9092", "test", rate, timeout)
