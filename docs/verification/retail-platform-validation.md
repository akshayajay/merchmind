# Local verification — 2026-09-20

This records executed local checks, not a cloud deployment or completed remote CI run.

- Application regression suite: **38 passed**, 1 integration test deselected;
  **88.61% coverage**, including identical revisions across different volume mount paths.
- Ruff lint passed. `git diff --check` passed.
- Actual Kafka-compatible Redpanda broker and Spark 4.0.1 integration: **1 passed**.
  The test confirmed 12 transaction messages and 3 inventory messages, exercised
  checkpoint restarts and SIGKILL recovery, reconciled 8 unique transactions including
  a very late event, ignored a duplicate business event, and verified stock through FastAPI.
- Real Airflow 3.1.0 execution: four successful `dag.test` runs and one intentionally
  failed quality-gate run. The failed task made three attempts, preserved the prior
  successful close, and a later run recovered. The first run used the inferred UTC
  interval without a manual date override; midnight closes the preceding day.
- Full Docker stack: real Kafka-compatible ingestion, both continuous Spark consumers,
  transaction and inventory publishers, API, dashboard, and standalone Airflow launched.
  Separate Ivy cache directories resolved a concurrent connector-download collision.
- Airflow scheduler run `retail-smoke-final-20260920` closed **2025-12-30** from a pinned
  **24,700-event** stream snapshot. Expected and actual totals were **33 transactions,
  37 units, $1,564.96**; it generated 224 category forecast rows from 24,700 training rows.
- A synthetic stock adjustment for `P000185` was delivered twice at distinct Kafka
  offsets. The API and rendered dashboard both showed **5 units, Low**. Dashboard health
  returned HTTP 200, and browser inspection showed stock and successful-close sections.

Machine-readable evidence: [Airflow](airflow-daily-close.json),
[Kafka/Spark](retail-streaming.json), and [running demo](retail-runtime.json).
Raw logs and generated data remain in ignored `data/airflow-evidence/` and
`data/streaming-evidence/` directories. No credentials are stored in these summaries.

All retail records and stock adjustments are synthetic. Redpanda supplies the Kafka
protocol in this local stack. Reconciliation and stock rebuilding use pandas; Spark
performs streaming ingestion and window aggregation. No Hadoop cluster or AWS deployment
was performed. Remote CI results are tracked separately in
[GitHub Actions](https://github.com/akshayajay/merchmind/actions/workflows/ci.yml).
