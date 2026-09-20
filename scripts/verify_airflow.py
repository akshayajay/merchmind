"""Execute the real Airflow DAG against reproducible retail data and retain evidence."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path

import pandas as pd
import pendulum

from merchmind.pipeline import run_pipeline


def main():
    directory = Path(tempfile.mkdtemp(prefix="retail-airflow-"))
    root, raw = directory / "data", directory / "raw"
    os.environ["MERCHMIND_DATA_DIR"] = str(root)
    os.environ["MERCHMIND_RAW_DIR"] = str(raw)
    run_pipeline(root, transactions=1000, customers=100, products=30)
    raw.mkdir()
    (raw / "_spark_metadata").mkdir()
    row = pd.read_parquet(root / "silver/fact_transactions.parquet").iloc[0].to_dict()
    row.update(
        transaction_id="airflow-sale",
        transaction_ts="2026-01-01T12:00:00Z",
        quantity=2,
        unit_price=10.0,
        returned=False,
    )

    def commit(batch, records):
        filename = f"part-{batch}.parquet"
        pd.DataFrame(
            [
                {
                    "topic": "airflow-test",
                    "partition": 0,
                    "offset": batch * 100 + i,
                    "value": json.dumps(item),
                }
                for i, item in enumerate(records)
            ]
        ).to_parquet(raw / filename, index=False)
        (raw / "_spark_metadata" / str(batch)).write_text(
            "v1\n" + json.dumps({"path": f"file:///raw/{filename}", "action": "add"}) + "\n"
        )

    commit(0, [row, row])
    subprocess.run(["airflow", "db", "migrate"], check=True)
    subprocess.run(["airflow", "dags", "reserialize"], check=True)
    from airflow.models import DagBag

    bag = DagBag(dag_folder=os.environ["AIRFLOW__CORE__DAGS_FOLDER"], include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.get_dag("retail_daily_close")
    assert dag is not None
    interval = dag.timetable.infer_manual_data_interval(
        run_after=pendulum.datetime(2026, 1, 2, tz="UTC")
    )
    assert interval.start == pendulum.datetime(2026, 1, 1, tz="UTC")
    assert interval.end == pendulum.datetime(2026, 1, 2, tz="UTC")
    runs = []
    for index, business_date in enumerate(
        ["2026-01-01", "2026-01-01", "2026-01-02", "2026-01-01", "2026-01-01"]
    ):
        if index == 1:
            commit(1, [{**row, "transaction_id": "late", "quantity": 1, "unit_price": 7.5}])
        pointer = root / "closes" / business_date / "current.json"
        before = pointer.read_bytes() if pointer.exists() else None
        os.environ["MERCHMIND_MAX_INVALID_RATE"] = "0" if index == 3 else "0.05"
        result = dag.test(
            logical_date=pendulum.datetime(2026, 1, 2 + index, tz="UTC"),
            run_conf={} if index == 0 else {"business_date": business_date},
        )
        if index == 3:
            assert result.state == "failed", result.state
            assert pointer.read_bytes() == before
            quality_task = next(ti for ti in result.get_task_instances() if ti.task_id == "quality")
            assert quality_task.try_number == 3
            runs.append(
                {
                    "business_date": business_date,
                    "state": result.state,
                    "expected_failure": "quality threshold",
                    "quality_attempts": 3,
                    "previous_close_preserved": True,
                }
            )
            continue
        assert result.state == "success", result.state
        report = json.loads((root / "closes" / business_date / "current.json").read_text())
        assert report["passed"]
        expected = ["20.00", "27.50", "0.00", None, "27.50"][index]
        assert report["actual"]["net_revenue"] == expected
        runs.append(
            {
                "business_date": business_date,
                "state": result.state,
                "transactions": report["actual"]["transactions"],
                "net_revenue": expected,
            }
        )
    evidence = {
        "airflow_version": __import__("airflow").__version__,
        "verification_mode": "real Airflow dag.test; synthetic Spark file commits",
        "dag_id": dag.dag_id,
        "task_ids": sorted(dag.task_ids),
        "runs": runs,
        "checks_passed": True,
    }
    destination = Path("/evidence")
    destination.mkdir(exist_ok=True)
    (destination / "airflow-verification.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    main()
