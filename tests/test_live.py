import json
import shutil

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from merchmind.api import create_app
from merchmind.live import committed_files, refresh_from_stream, serving_paths
from merchmind.pipeline import run_pipeline


def commit(raw, batch, values):
    raw.mkdir(exist_ok=True)
    metadata = raw / "_spark_metadata"
    metadata.mkdir(exist_ok=True)
    filename = f"part-{batch}.parquet"
    pd.DataFrame(
        [
            {"topic": "test", "partition": 0, "offset": batch * 100 + i, "value": value}
            for i, value in enumerate(values)
        ]
    ).to_parquet(raw / filename)
    (metadata / str(batch)).write_text(
        "v1\n" + json.dumps({"path": f"file:///docker/raw/{filename}", "action": "add"}) + "\n"
    )


@pytest.fixture
def baseline(tmp_path):
    root = tmp_path / "data"
    run_pipeline(root, transactions=1000, customers=100, products=30)
    row = pd.read_parquet(root / "silver/fact_transactions.parquet").iloc[0].to_dict()
    row["transaction_ts"] = row["transaction_ts"].isoformat()
    return root, tmp_path / "raw", row


def test_publication_deduplicates_updates_api_and_recovers_failure(baseline, monkeypatch):
    root, raw, row = baseline
    client = TestClient(create_app(root))
    before = client.get("/v1/kpis").json()
    assert client.get("/v1/live-status").json() == {"mode": "batch"}
    assert refresh_from_stream(root, raw) is None
    new = {**row, "transaction_id": "new-sale", "quantity": 10, "returned": False}
    commit(
        raw,
        0,
        [
            json.dumps(row),
            json.dumps(new),
            "not-json",
            json.dumps({**new, "transaction_id": "bad", "quantity": "nonsense"}),
        ],
    )
    pd.DataFrame({"orphan": [1]}).to_parquet(raw / "uncommitted.parquet")
    report = refresh_from_stream(root, raw)
    assert report["new_transactions"] == 1
    assert report["quarantined_events"] == 2
    assert report["duplicate_ids_ignored"] == 1
    assert report["stream_pass_rate"] == 0.5
    after = client.get("/v1/kpis").json()
    assert after["orders"] == before["orders"] + 1
    assert after["net_revenue"] == pytest.approx(before["net_revenue"] + 10 * row["unit_price"])
    for endpoint in ["categories", "products", "inventory-risk", "customer-segments"]:
        assert client.get("/v1/" + endpoint).status_code == 200
    old = serving_paths(root)
    assert len(list(old.gold.glob("*"))) == 6
    assert refresh_from_stream(root, raw) is None
    # Replay identical business IDs at new Kafka offsets: no inflated revenue.
    commit(raw, 1, [json.dumps(new)])
    with monkeypatch.context() as patch:
        patch.setattr(
            "merchmind.live.build_gold",
            lambda *args: (_ for _ in ()).throw(RuntimeError("simulated publisher crash")),
        )
        with pytest.raises(RuntimeError, match="crash"):
            refresh_from_stream(root, raw)
    assert serving_paths(root) == old
    assert client.get("/v1/kpis").json() == after
    assert refresh_from_stream(root, raw)["new_transactions"] == 1
    assert client.get("/v1/kpis").json()["net_revenue"] == after["net_revenue"]
    # Recover a completed snapshot whose pointer publication was interrupted.
    (root / "live/current.json").unlink()
    assert refresh_from_stream(root, raw)["new_transactions"] == 1
    assert client.get("/v1/live-status").json()["committed_raw_events"] == 5


def test_all_malformed_and_contract_violations(baseline):
    root, raw, row = baseline
    commit(raw, 0, ["null", "[]", "bad"])
    assert refresh_from_stream(root, raw)["quarantined_events"] == 3
    invalid = [
        dict(row, transaction_id=f"bad-{i}", **change)
        for i, change in enumerate(
            [
                {"quantity": 0.5},
                {"unit_price": float("inf")},
                {"returned": "false"},
                {"discount_pct": 2},
                {"sales_channel": ""},
                {"product_id": "unknown"},
            ]
        )
    ]
    commit(raw, 1, [json.dumps(item) for item in invalid])
    report = refresh_from_stream(root, raw)
    assert report["new_transactions"] == 0
    assert report["quarantined_events"] == 9


def test_compacted_commit_log_respects_deletions(tmp_path):
    meta = tmp_path / "_spark_metadata"
    meta.mkdir()

    def entry(name, action="add"):
        return json.dumps({"path": f"file:///old/{name}.parquet", "action": action})

    (meta / "0").write_text("v1\n" + entry("old"))
    (meta / "9.compact").write_text("v1\n" + entry("new"))
    (meta / "10").write_text("v1\n" + entry("new", "delete") + "\n" + entry("final"))
    assert committed_files(tmp_path) == [tmp_path / "final.parquet"]
    (meta / "11").write_text("v2\n")
    with pytest.raises(ValueError, match="Unsupported"):
        committed_files(tmp_path)


def test_invalid_revision_is_rejected(tmp_path):
    (tmp_path / "live").mkdir()
    (tmp_path / "live/current.json").write_text(json.dumps({"revision": "../../elsewhere"}))
    with pytest.raises(ValueError, match="revision"):
        serving_paths(tmp_path)


def test_same_source_cut_has_same_revision_across_volume_mount_paths(baseline, tmp_path):
    root, raw, row = baseline
    commit(raw, 0, [json.dumps({**row, "transaction_id": "new-sale"})])
    original = refresh_from_stream(root, raw)
    mounted_root, mounted_raw = tmp_path / "other-mount", tmp_path / "other-raw-mount"
    shutil.copytree(root, mounted_root)
    shutil.copytree(raw, mounted_raw)
    assert refresh_from_stream(mounted_root, mounted_raw) is None
    assert serving_paths(mounted_root).root.name == original["revision"]
