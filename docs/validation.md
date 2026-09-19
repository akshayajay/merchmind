# Local validation, September 19, 2026

All work below ran locally. No AWS resources were created and no cloud deployment is claimed.

## Forecast backtesting

Run `merchmind backtest --data-dir data` after the default 50,000-transaction pipeline. Results are written to `data/reports/forecast_backtest.json` and a prediction-level Parquet file. The dashboard shows the report when present; it is a historical baseline evaluation, not a live model score.

The default data spans January 1, 2024–December 30, 2025. Evaluation begins after 84 training days and uses 23 disjoint, 28-day holdouts for eight categories: 5,152 category-day predictions per model. Every fold constructs forecasts from earlier history before joining holdout actuals. Categories absent from all training history are outside that fold's evaluation. Missing days for known categories are treated as zero sales.

| Model | MAE, units/day | WAPE | Bias | Band coverage |
|---|---:|---:|---:|---:|
| Matching weekday, trailing 84 days | 2.8371 | 29.3991% | −0.0937% | 75.2135% |
| Trailing 28-day mean | 2.7777 | 28.7839% | −0.1468% | — |
| Repeat last week | 3.7873 | 39.2453% | −0.8247% | — |

The 28-day mean performed slightly better than the current forecast. Its simpler fit to this synthetic generator is not evidence it will win on real retail demand. The existing ±1.28-standard-deviation bands are nominal 80% bands; measured coverage is lower and they have not been recalibrated against these holdouts. Do not claim improved retail forecast accuracy. [Full metrics](verification/forecast-backtest.json).

## Crash recovery and consistent serving

`make streaming-test` starts a real Kafka-compatible broker and Spark 4.0.1 in Docker. In addition to initial ingestion, watermark checks, a graceful checkpoint restart and an idle restart, it kills a running Spark driver with SIGKILL after a raw-file commit. Restarting with the same checkpoints retains all 11 broker-confirmed messages exactly once by Kafka topic/partition/offset. Five finalized channel windows reconcile to six transactions and $31 net revenue. Three invalid messages remain auditable. The final marker transaction is still in an open window.

The test then reads those real committed files into the publisher and verifies eight new valid business IDs through FastAPI. This includes a late event: historical Gold accepts it, while an already finalized Spark window does not change. [Crash evidence](verification/crash-recovery.json).

The publisher:

- Reads Spark `_spark_metadata` add/delete/compacted logs, ignoring orphan files after crashes. Only flat local Parquet sinks are supported.
- Validates incoming contracts and customer/product references, retaining rejection reasons. Mixed rejected values are rendered as strings in quarantine Parquet; the original Kafka payload remains in raw storage.
- Deduplicates business IDs using the baseline first, then the first valid event in deterministic topic/partition/offset order. IDs are immutable; corrections and cross-partition event-time ordering need a different upsert model.
- Builds all six Gold products under a new snapshot directory before atomically replacing `live/current.json`. Concurrent publishers use a file lock. A build failure leaves the last good snapshot available.
- Resolves a single snapshot for each dashboard render. API endpoints resolve the current snapshot per request. Separate requests may see different revisions; `/v1/live-status` exposes the active revision.

Streamlit refreshes every five seconds while open. The publisher also polls every five seconds; total latency includes Spark processing and Gold rebuild time. Replaying existing Silver IDs adds no serving revenue. Reference dimensions, company/macro tables and PostgreSQL do not update from this transaction stream. Historical forecast validation remains tied to the original evaluation dataset.

This publisher rereads history and rebuilds pandas Gold, so its cost grows with data size. Snapshots and raw/checkpoint files are retained; configure retention and storage monitoring before extended use. Interrupted `.pending-*` directories are ignored but not automatically removed. POSIX locks/atomic rename require Linux or macOS on one shared filesystem. This is not a distributed transactional database or a power-loss durability guarantee.

## Replicated broker failure and multiple Spark workers

Run `python scripts/verify_resilience.py` with Docker running and approximately 8 GB available. This opt-in lab creates an isolated local Compose project, collects evidence in `data/resilience-evidence`, and removes its own containers/volumes in a `finally` block. Archive that evidence directory before a fresh run. No cloud credentials are used.

Observed run: three Redpanda v24.3.5 brokers, three partitions with replication factor three and minimum two in-sync replicas, and Spark 4.0.1 standalone with two worker executors. The test sends 10,000 messages, identifies and SIGKILLs an actual partition leader, then obtains acknowledgements for another 10,000 with that broker down. Spark consumes the degraded cluster across both workers; the report requires completed task counts greater than zero on each worker. After broker restoration, another 10,000 messages and one event-time marker are published. Restarted Spark reconciles all 30,001 unique raw events, 30,000 finalized transactions and $600,000 revenue. [Summary](verification/local-resilience.json).

This demonstrates a broker outage and multiple worker processes on one Docker host. It does not establish sustained throughput, physical host/availability-zone fault tolerance, Spark master HA, worker-loss recovery, production security, or an availability SLA. Spark master and serving publisher are still single points of interruption. The normal demo remains a single broker; the replicated configuration is the opt-in lab.

## Dashboard checks

Automated Streamlit AppTest exercises every category, metrics, tables and the forecast report. Browser QA at 1280×720 and 390×844 checks chart layout; the desktop workflow checks nominal bands, category selection, live status and updating order totals. [Recorded browser checks](verification/dashboard.json). The real Kafka → Spark → publisher → open-browser check adds synthetic test sales and verifies new totals without manually refreshing the page. This covers the primary workflow, not every browser or every built-in Plotly/table export option.

## AWS

Terraform definitions remain a design artifact. Deployment and cloud validation are intentionally deferred to avoid spending money. They should not be described as a deployed AWS system on a resume.

## Runtime references

- [Spark standalone deployment](https://spark.apache.org/docs/4.0.1/spark-standalone.html)
- [Redpanda three-broker local example](https://docs.redpanda.com/labs/docker-compose/three-brokers/)
- [Streamlit timed fragments](https://docs.streamlit.io/develop/api-reference/execution-flow/st.fragment)
